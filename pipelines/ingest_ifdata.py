from __future__ import annotations

import argparse
import json
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import httpx
from sqlalchemy import text

from core.db import engine

# -----------------------------
# Endpoints
# -----------------------------
ODATA_DEFAULT = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"
CADASTRO_URL = "{base}/IfDataCadastro"
VALORES_FUNCTION_URL = "{base}/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"

# -----------------------------
# Text cleaning / normalization
# -----------------------------
_RE_SPACES = re.compile(r"\s+")
_RE_CTRL = re.compile(r"[\x00-\x1F\x7F]+")

def clean_text(s: str) -> str:
    s = _RE_CTRL.sub(" ", s or "")
    s = s.replace("\r", " ").replace("\n", " ")
    s = _RE_SPACES.sub(" ", s).strip()
    return s

def clean_indicator_name(s: str) -> str:
    s = clean_text(s)
    s = re.sub(r"\s*=\s*", " = ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    return s

def safe_trunc(s: str, max_len: int) -> str:
    s = "" if s is None else str(s)
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"

def to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            x = x.strip()
            if x == "":
                return None
            # tenta normalizar "1.234,56"
            if "," in x and "." in x:
                x = x.replace(".", "").replace(",", ".")
            elif "," in x and "." not in x:
                x = x.replace(",", ".")
        return float(x)
    except Exception:
        return None

def parse_ref_date_from_anomes(anomes: int) -> date:
    y = anomes // 100
    m = anomes % 100
    if m == 12:
        next_month = date(y + 1, 1, 1)
    else:
        next_month = date(y, m + 1, 1)
    return date.fromordinal(next_month.toordinal() - 1)

# -----------------------------
# Resilient HTTP (Olinda)
# -----------------------------
def odata_get(
    url: str,
    params: Dict[str, Any],
    timeout_s: float = 90.0,
    tries: int = 8,
) -> Dict[str, Any]:
    last_err: Exception | None = None

    timeout = httpx.Timeout(
        connect=min(10.0, timeout_s),
        read=max(30.0, timeout_s),
        write=min(10.0, timeout_s),
        pool=min(10.0, timeout_s),
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "risk-bank-ingest/1.1",
    }

    for attempt in range(1, tries + 1):
        try:
            with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
                r = client.get(url, params=params)

                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    sleep_s = float(ra) if (ra and ra.isdigit()) else min(60.0, 2.0 * attempt)
                    time.sleep(sleep_s)
                    continue

                if 500 <= r.status_code <= 599:
                    raise httpx.HTTPStatusError(
                        f"{r.status_code} server error", request=r.request, response=r
                    )

                r.raise_for_status()
                return r.json()

        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.ConnectError) as e:
            last_err = e
        except httpx.HTTPStatusError as e:
            last_err = e
        except Exception as e:
            last_err = e

        base = min(2 ** attempt, 60)
        jitter = random.uniform(0.4, 1.3)
        time.sleep(base * jitter)

    raise RuntimeError(f"Falha no GET {url} params={params}. Último erro: {last_err}") from last_err

# -----------------------------
# Checkpoint / resume
# -----------------------------
@dataclass
class Checkpoint:
    anomes: int
    tipo: int
    rel: str
    top: int
    skip: int
    updated_at: str

def state_key(anomes: int, tipo: int, rel: str) -> str:
    return f"{anomes}:{tipo}:{rel}"

def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_state(path: Path, state: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def get_checkpoint(state: Dict[str, Any], anomes: int, tipo: int, rel: str) -> Optional[Checkpoint]:
    v = state.get(state_key(anomes, tipo, rel))
    if not isinstance(v, dict):
        return None
    try:
        return Checkpoint(
            anomes=int(v["anomes"]),
            tipo=int(v["tipo"]),
            rel=str(v["rel"]),
            top=int(v.get("top", 0)),
            skip=int(v.get("skip", 0)),
            updated_at=str(v.get("updated_at", "")),
        )
    except Exception:
        return None

def set_checkpoint(state: Dict[str, Any], cp: Checkpoint) -> None:
    state[state_key(cp.anomes, cp.tipo, cp.rel)] = {
        "anomes": cp.anomes,
        "tipo": cp.tipo,
        "rel": cp.rel,
        "top": cp.top,
        "skip": cp.skip,
        "updated_at": cp.updated_at,
    }

# -----------------------------
# IF.data: Cadastro com fallback (evita 400)
# -----------------------------
def iter_cadastro(anomes: int, tipo: int, base: str, timeout_s: float) -> Iterator[Dict[str, Any]]:
    """
    Tenta com $filter. Se der 400 no Olinda, faz fallback sem $filter e filtra no Python.
    """
    url = CADASTRO_URL.format(base=base)
    top = 5000

    def _with_filter() -> Iterator[Dict[str, Any]]:
        skip = 0
        while True:
            params = {
                "$format": "json",
                "$top": top,
                "$skip": skip,
                "$select": "AnoMes,CodInst,Nome,TipoInstituicao",
                "$filter": f"AnoMes eq {anomes} and TipoInstituicao eq {tipo}",
            }
            data = odata_get(url, params=params, timeout_s=timeout_s)
            rows = data.get("value", []) or []
            if not rows:
                break
            for r in rows:
                yield r
            skip += len(rows)
            if len(rows) < top:
                break

    def _without_filter() -> Iterator[Dict[str, Any]]:
        skip = 0
        while True:
            params = {
                "$format": "json",
                "$top": top,
                "$skip": skip,
                "$select": "AnoMes,CodInst,Nome,TipoInstituicao",
                "$orderby": "AnoMes desc",
            }
            data = odata_get(url, params=params, timeout_s=timeout_s)
            rows = data.get("value", []) or []
            if not rows:
                break

            for r in rows:
                try:
                    if int(r.get("AnoMes")) == int(anomes) and int(r.get("TipoInstituicao")) == int(tipo):
                        yield r
                except Exception:
                    continue

            skip += len(rows)
            if len(rows) < top:
                break

    try:
        yield from _with_filter()
    except Exception as e:
        msg = str(e)
        if "400" in msg or "Bad Request" in msg:
            print("[WARN] IfDataCadastro rejeitou $filter (400). Fallback sem $filter (filtrando no Python)...")
            yield from _without_filter()
        else:
            raise

def build_cadastro_map(anomes: int, tipo: int, base: str, timeout_s: float) -> Dict[str, str]:
    print("==> Carregando cadastro (CodInst -> Nome)...")
    cmap: Dict[str, str] = {}
    for r in iter_cadastro(anomes=anomes, tipo=tipo, base=base, timeout_s=timeout_s):
        cod = str(r.get("CodInst") or "").strip()
        nome = clean_text(str(r.get("Nome") or ""))
        if cod:
            cmap[cod] = nome
    print(f"    cadastro_map size: {len(cmap)}")
    return cmap

def get_latest_anomes(base: str, timeout_s: float) -> int:
    """
    Detecção tolerante: evita combinações de $select que às vezes dão 400.
    """
    url = CADASTRO_URL.format(base=base)
    params = {"$format": "json", "$top": 1, "$orderby": "AnoMes desc"}
    data = odata_get(url, params=params, timeout_s=timeout_s)
    rows = data.get("value", []) or []
    if not rows:
        raise RuntimeError("Não consegui detectar AnoMes mais recente via IfDataCadastro.")
    return int(rows[0]["AnoMes"])

# -----------------------------
# IF.data: Valores (function)
# -----------------------------
def iter_ifdata_valores_pages(
    anomes: int,
    tipo: int,
    rel: str,
    base: str,
    top: int,
    start_skip: int,
    timeout_s: float,
) -> Iterator[Tuple[int, List[Dict[str, Any]]]]:
    url = VALORES_FUNCTION_URL.format(base=base)

    skip = start_skip
    while True:
        params = {
            "$format": "json",
            "$top": top,
            "$skip": skip,
            "@AnoMes": anomes,
            "@TipoInstituicao": tipo,
            "@Relatorio": f"'{rel}'",
        }
        data = odata_get(url, params=params, timeout_s=timeout_s)
        rows = data.get("value", []) or []
        yield (skip, rows)

        if not rows:
            break
        skip += len(rows)
        if len(rows) < top:
            break

# -----------------------------
# DB: upsert
# -----------------------------
UPSERT_SQL = text("""
  INSERT INTO ifdata_indicators
    (ref_date, institution_id, institution_name, indicator, value)
  VALUES
    (:ref_date, :institution_id, :institution_name, :indicator, :value)
  ON CONFLICT (ref_date, institution_id, indicator)
  DO UPDATE SET
    value = EXCLUDED.value,
    institution_name = EXCLUDED.institution_name
""")

def upsert_batch(batch: List[Dict[str, Any]]) -> int:
    if not batch:
        return 0
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, batch)
    return len(batch)

# -----------------------------
# Ingest de um relatório (com fallback top + checkpoint)
# -----------------------------
def ingest_relatorio(
    anomes: int,
    ref_date: date,
    tipo: int,
    rel: str,
    base: str,
    timeout_s: float,
    top_initial: int,
    cadastro_map: Dict[str, str],
    state_path: Path,
    resume: bool,
    commit_every: int = 10_000,
    indicator_max_len: int = 220,
    name_max_len: int = 160,
) -> int:
    top_candidates = [top_initial, 2000, 1000, 500, 200, 100]
    top_candidates = [t for t in top_candidates if t > 0]
    top_candidates = list(dict.fromkeys(top_candidates))

    state = load_state(state_path)
    cp = get_checkpoint(state, anomes, tipo, rel) if resume else None
    start_skip = cp.skip if cp else 0

    total_upserted = 0
    total_rows_downloaded = 0
    institutions_seen: set[str] = set()

    last_error: Optional[Exception] = None

    for top in top_candidates:
        try:
            batch: List[Dict[str, Any]] = []

            for (skip_at, rows) in iter_ifdata_valores_pages(
                anomes=anomes,
                tipo=tipo,
                rel=rel,
                base=base,
                top=top,
                start_skip=start_skip,
                timeout_s=timeout_s,
            ):
                if not rows:
                    # checkpoint final
                    state = load_state(state_path)
                    set_checkpoint(
                        state,
                        Checkpoint(
                            anomes=anomes,
                            tipo=tipo,
                            rel=rel,
                            top=top,
                            skip=skip_at,
                            updated_at=datetime.utcnow().isoformat(),
                        ),
                    )
                    save_state(state_path, state)
                    break

                total_rows_downloaded += len(rows)

                for r in rows:
                    codinst = str(r.get("CodInst") or "").strip()
                    if not codinst:
                        continue

                    nome_inst = cadastro_map.get(codinst) or clean_text(str(r.get("NomeInstituicao") or "")) or ""
                    nome_coluna = r.get("NomeColuna") or r.get("DescricaoColuna") or ""
                    indicator = f"{rel}::{clean_indicator_name(str(nome_coluna))}"

                    # Evita indicador vazio
                    if indicator.strip() == f"{rel}::":
                        continue

                    val = to_float(r.get("Saldo"))
                    institutions_seen.add(codinst)

                    batch.append(
                        {
                            "ref_date": str(ref_date),
                            "institution_id": codinst,
                            "institution_name": safe_trunc(nome_inst, name_max_len),
                            "indicator": safe_trunc(indicator, indicator_max_len),
                            "value": val,
                        }
                    )

                    if len(batch) >= commit_every:
                        total_upserted += upsert_batch(batch)
                        batch.clear()

                        state = load_state(state_path)
                        set_checkpoint(
                            state,
                            Checkpoint(
                                anomes=anomes,
                                tipo=tipo,
                                rel=rel,
                                top=top,
                                skip=skip_at,
                                updated_at=datetime.utcnow().isoformat(),
                            ),
                        )
                        save_state(state_path, state)

                # checkpoint por página (skip + rows)
                state = load_state(state_path)
                set_checkpoint(
                    state,
                    Checkpoint(
                        anomes=anomes,
                        tipo=tipo,
                        rel=rel,
                        top=top,
                        skip=skip_at + len(rows),
                        updated_at=datetime.utcnow().isoformat(),
                    ),
                )
                save_state(state_path, state)

            if batch:
                total_upserted += upsert_batch(batch)
                batch.clear()

            print(f"  linhas baixadas (raw): {total_rows_downloaded}")
            print(f"  instituições únicas processadas: {len(institutions_seen)}")
            return total_upserted

        except Exception as e:
            last_error = e
            print(f"[WARN] Relatório {rel}: falhou com top={top} a partir de skip={start_skip}. Tentando top menor...")
            time.sleep(2.0)

    raise RuntimeError(f"Relatório {rel}: falhou em todas as tentativas de top. Último erro: {last_error}") from last_error

# -----------------------------
# CLI
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Ingest IF.data (BCB Olinda) -> ifdata_indicators (long format)")
    ap.add_argument("--odata-base", type=str, default=ODATA_DEFAULT)
    ap.add_argument("--anomes", type=int, default=0, help="AnoMes (YYYYMM). Se 0, auto-detect.")
    ap.add_argument("--ref-date", type=str, default="", help="YYYY-MM-DD. Se vazio, último dia do mês do AnoMes.")
    ap.add_argument("--tipo", type=int, default=1, help="TipoInstituicao (ex.: 1)")
    ap.add_argument("--relatorios", type=str, default="1,4,5", help="Relatórios (ex.: 1,4,5)")
    ap.add_argument("--top", type=int, default=1000, help="Page size inicial (recomendado 500–1000)")
    ap.add_argument("--timeout", type=float, default=120.0, help="Timeout base em segundos")
    ap.add_argument("--state-path", type=str, default=".ifdata_ingest_state.json")
    ap.add_argument("--no-resume", action="store_true", help="Ignora checkpoint e começa do zero")
    ap.add_argument("--commit-every", type=int, default=10000)
    ap.add_argument("--indicator-max-len", type=int, default=220)
    ap.add_argument("--name-max-len", type=int, default=160)

    args = ap.parse_args()

    base = args.odata_base.rstrip("/")
    timeout_s = float(args.timeout)
    tipo = int(args.tipo)
    relatorios = [r.strip() for r in args.relatorios.split(",") if r.strip()]

    anomes = int(args.anomes)
    if anomes <= 0:
        anomes = get_latest_anomes(base=base, timeout_s=timeout_s)
        print(f"[INFO] Auto-detect: AnoMes={anomes}, TipoInstituicao={tipo}")

    if args.ref_date.strip():
        ref_date = datetime.strptime(args.ref_date.strip(), "%Y-%m-%d").date()
    else:
        ref_date = parse_ref_date_from_anomes(anomes)

    print(f"==> Ingest IF.data: AnoMes={anomes} ref_date={ref_date} tipo={tipo} relatorios={relatorios}")
    print(f"ODATA_BASE={base}")

    state_path = Path(args.state_path)
    resume = not bool(args.no_resume)

    cadastro_map = build_cadastro_map(anomes=anomes, tipo=tipo, base=base, timeout_s=timeout_s)

    total = 0
    for rel in relatorios:
        print(f"\n--- Relatório {rel} ---")
        n = ingest_relatorio(
            anomes=anomes,
            ref_date=ref_date,
            tipo=tipo,
            rel=rel,
            base=base,
            timeout_s=timeout_s,
            top_initial=int(args.top),
            cadastro_map=cadastro_map,
            state_path=state_path,
            resume=resume,
            commit_every=int(args.commit_every),
            indicator_max_len=int(args.indicator_max_len),
            name_max_len=int(args.name_max_len),
        )
        total += n

    print(f"\nOK. Total de registros upsertados: {total}")

if __name__ == "__main__":
    main()
