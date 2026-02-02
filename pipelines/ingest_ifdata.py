from __future__ import annotations

import argparse
import json
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import httpx
from sqlalchemy import text

from core.db import engine


# -----------------------------
# Config / Endpoints
# -----------------------------
ODATA_DEFAULT = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"

CADASTRO_URL = "{base}/IfDataCadastro"
VALORES_FUNCTION_URL = "{base}/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"


# -----------------------------
# Utils: strings / normalization
# -----------------------------
_RE_SPACES = re.compile(r"\s+")
_RE_CTRL = re.compile(r"[\x00-\x1F\x7F]+")

def clean_text(s: str) -> str:
    """Remove controles, normaliza whitespace e trims."""
    s = _RE_CTRL.sub(" ", s)
    s = s.replace("\r", " ").replace("\n", " ")
    s = _RE_SPACES.sub(" ", s).strip()
    return s

def clean_indicator_name(s: str) -> str:
    """
    Nome do indicador vem em NomeColuna / DescricaoColuna e pode ter fórmulas,
    quebras de linha etc. A gente limpa pra ficar estável.
    """
    s = clean_text(s)
    # remove espaços em torno de "=" e símbolos
    s = re.sub(r"\s*=\s*", " = ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    return s

def safe_trunc(s: str, max_len: int) -> str:
    if s is None:
        return ""
    s = str(s)
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"

def parse_ref_date_from_anomes(anomes: int) -> date:
    """
    IF.data usa AnoMes no formato YYYYMM.
    A ref_date do projeto normalmente é o último dia do mês.
    """
    y = anomes // 100
    m = anomes % 100
    if m == 12:
        next_month = date(y + 1, 1, 1)
    else:
        next_month = date(y, m + 1, 1)
    return next_month.fromordinal(next_month.toordinal() - 1)

def to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        # Olinda às vezes manda número como string
        if isinstance(x, str):
            x = x.strip()
            if x == "":
                return None
            x = x.replace(".", "").replace(",", ".") if "," in x else x
        return float(x)
    except Exception:
        return None


# -----------------------------
# HTTP client with resilience
# -----------------------------
def odata_get(
    url: str,
    params: Dict[str, Any],
    timeout_s: float = 90.0,
    tries: int = 8,
) -> Dict[str, Any]:
    """
    GET resiliente para Olinda/IF.data:
    - timeout separado (connect/read/write/pool)
    - retry com backoff exponencial + jitter
    - trata 429/5xx/timeouts/transientes
    """
    last_err: Exception | None = None

    timeout = httpx.Timeout(
        connect=min(10.0, timeout_s),
        read=max(30.0, timeout_s),   # read é o gargalo
        write=min(10.0, timeout_s),
        pool=min(10.0, timeout_s),
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "risk-bank-ingest/1.0",
    }

    for attempt in range(1, tries + 1):
        try:
            with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
                r = client.get(url, params=params)

                # rate limit
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    sleep_s = float(ra) if (ra and ra.isdigit()) else min(60.0, 2.0 * attempt)
                    time.sleep(sleep_s)
                    continue

                # 5xx transitório
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

        # backoff exponencial + jitter
        base = min(2 ** attempt, 60)
        jitter = random.uniform(0.4, 1.3)
        time.sleep(base * jitter)

    raise RuntimeError(f"Falha no GET {url} params={params}. Último erro: {last_err}") from last_err


# -----------------------------
# Checkpoint (resume)
# -----------------------------
@dataclass
class Checkpoint:
    anomes: int
    tipo: int
    rel: str
    top: int
    skip: int
    updated_at: str

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

def state_key(anomes: int, tipo: int, rel: str) -> str:
    return f"{anomes}:{tipo}:{rel}"

def get_checkpoint(state: Dict[str, Any], anomes: int, tipo: int, rel: str) -> Optional[Checkpoint]:
    k = state_key(anomes, tipo, rel)
    v = state.get(k)
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
    k = state_key(cp.anomes, cp.tipo, cp.rel)
    state[k] = {
        "anomes": cp.anomes,
        "tipo": cp.tipo,
        "rel": cp.rel,
        "top": cp.top,
        "skip": cp.skip,
        "updated_at": cp.updated_at,
    }


# -----------------------------
# IF.data iterators
# -----------------------------
def iter_cadastro(anomes: int, tipo: int, base: str, timeout_s: float) -> Iterator[Dict[str, Any]]:
    """
    Cadastro: queremos CodInst e Nome.
    Observação: o cadastro pode ter mais de um AnoMes; mas na prática
    dá pra filtrar AnoMes e TipoInstituicao.
    """
    url = CADASTRO_URL.format(base=base)
    top = 10000
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


def iter_ifdata_valores_pages(
    anomes: int,
    tipo: int,
    rel: str,
    base: str,
    top: int,
    start_skip: int,
    timeout_s: float,
) -> Iterator[Tuple[int, List[Dict[str, Any]]]]:
    """
    Itera páginas do endpoint function IfDataValores(...) com paginação $skip/$top.
    Retorna (skip_atual, rows_da_pagina).
    """
    url = VALORES_FUNCTION_URL.format(base=base)

    skip = start_skip
    while True:
        params = {
            "$format": "json",
            "$top": top,
            "$skip": skip,
            "@AnoMes": anomes,
            "@TipoInstituicao": tipo,
            "@Relatorio": f"'{rel}'",  # precisa ir com aspas simples
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
# DB upsert
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
# Main ingest logic
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
    """
    Ingest de um relatório com checkpoint + fallback de page size.
    Retorna total de registros upsertados.
    """

    # estratégia recomendada: começar com top menor e estável
    # (1000 costuma ser bom; se falhar, desce)
    top_candidates = [top_initial, 2000, 1000, 500, 200, 100]
    top_candidates = [t for t in top_candidates if t > 0]
    # remove duplicados mantendo ordem
    top_candidates = list(dict.fromkeys(top_candidates))

    # checkpoint
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

            # se estamos mudando top e já tinha checkpoint, continua do skip salvo
            # (skip é “offset de linha”, independente do top)
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
                    # salva checkpoint final (fim)
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
                    # chaves observadas: AnoMes, CodInst, Conta, DescricaoColuna, Grupo, NomeColuna, NomeRelatorio, NumeroRelatorio, Saldo, TipoInstituicao
                    codinst = str(r.get("CodInst") or "").strip()
                    nome_inst = cadastro_map.get(codinst) or clean_text(str(r.get("NomeInstituicao") or "")) or ""

                    # Indicador (prefixo do relatório + nome limpo)
                    nome_coluna = r.get("NomeColuna") or r.get("DescricaoColuna") or ""
                    indicator = f"{rel}::{clean_indicator_name(str(nome_coluna))}"

                    # valor
                    val = to_float(r.get("Saldo"))

                    if not codinst or indicator.strip() == f"{rel}::":
                        continue

                    institutions_seen.add(codinst)

                    item = {
                        "ref_date": str(ref_date),
                        "institution_id": codinst,
                        "institution_name": safe_trunc(nome_inst, name_max_len),
                        "indicator": safe_trunc(indicator, indicator_max_len),
                        "value": val,
                    }
                    batch.append(item)

                    if len(batch) >= commit_every:
                        total_upserted += upsert_batch(batch)
                        batch.clear()

                        # checkpoint: salva “até onde chegamos” (skip_at + rows processadas)
                        # Como estamos executando pagina inteira, salvamos o skip da página atual.
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

                # checkpoint por página (ao final de cada página)
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

            # flush final
            if batch:
                total_upserted += upsert_batch(batch)
                batch.clear()

            print(f"  linhas baixadas (raw): {total_rows_downloaded}")
            print(f"  instituições únicas processadas: {len(institutions_seen)}")
            return total_upserted

        except Exception as e:
            last_error = e
            print(f"[WARN] Relatório {rel}: falhou com top={top} a partir de skip={start_skip}. Tentando top menor...")
            # pequena pausa para aliviar
            time.sleep(2.0)

    raise RuntimeError(f"Relatório {rel}: falhou em todas as tentativas de top. Último erro: {last_error}") from last_error


def get_latest_anomes(base: str, timeout_s: float) -> int:
    # tenta inferir o AnoMes mais recente via cadastro (mais leve)
    url = CADASTRO_URL.format(base=base)
    params = {"$format": "json", "$top": 1, "$orderby": "AnoMes desc", "$select": "AnoMes"}
    data = odata_get(url, params=params, timeout_s=timeout_s)
    rows = data.get("value", []) or []
    if not rows:
        raise RuntimeError("Não consegui detectar AnoMes mais recente via IfDataCadastro.")
    return int(rows[0]["AnoMes"])


def main():
    ap = argparse.ArgumentParser(description="Ingest IF.data (BCB Olinda) -> ifdata_indicators (long format)")

    ap.add_argument("--odata-base", type=str, default=ODATA_DEFAULT, help="Base URL do OData do IF.data")
    ap.add_argument("--anomes", type=int, default=0, help="AnoMes (YYYYMM). Se 0, auto-detect pelo cadastro")
    ap.add_argument("--ref-date", type=str, default="", help="ref_date YYYY-MM-DD. Se vazio, usa último dia do mês do AnoMes")
    ap.add_argument("--tipo", type=int, default=1, help="TipoInstituicao (ex.: 1)")
    ap.add_argument("--relatorios", type=str, default="1,4,5", help="Relatórios (ex.: 1,4,5)")
    ap.add_argument("--top", type=int, default=1000, help="Page size inicial (recomendado 500–1000)")
    ap.add_argument("--timeout", type=float, default=90.0, help="Timeout base (segundos)")

    ap.add_argument("--state-path", type=str, default=".ifdata_ingest_state.json", help="Arquivo de checkpoint/resume")
    ap.add_argument("--no-resume", action="store_true", help="Ignora checkpoint e começa do zero")

    ap.add_argument("--commit-every", type=int, default=10000, help="Upsert a cada N registros")
    ap.add_argument("--indicator-max-len", type=int, default=220, help="Truncagem segura do indicador")
    ap.add_argument("--name-max-len", type=int, default=160, help="Truncagem segura do nome da instituição")

    args = ap.parse_args()

    base = args.odata_base.rstrip("/")
    timeout_s = float(args.timeout)
    tipo = int(args.tipo)

    relatorios = [r.strip() for r in args.relatorios.split(",") if r.strip()]
    if not relatorios:
        raise SystemExit("Nenhum relatório informado.")

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

    # carrega cadastro uma vez por execução
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
