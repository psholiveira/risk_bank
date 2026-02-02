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


def safe_trunc(s: Any, max_len: int) -> str:
    s = "" if s is None else str(s)
    if max_len <= 0:
        return s
    if len(s) <= max_len:
        return s
    # -1 pra manter o "…" e não estourar
    return s[: max_len - 1] + "…"


def to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if v != v:  # NaN
                return None
            return v
        if isinstance(x, str):
            s = x.strip()
            if s == "":
                return None
            # pt-BR: 1.234.567,89
            if "," in s and "." in s:
                s = s.replace(".", "").replace(",", ".")
            elif "," in s and "." not in s:
                s = s.replace(",", ".")
            return float(s)
        return None
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
def _timeout(timeout_s: float) -> httpx.Timeout:
    # leitura pode ser bem lenta na Olinda
    return httpx.Timeout(
        connect=min(15.0, timeout_s),
        read=max(60.0, timeout_s),
        write=min(15.0, timeout_s),
        pool=min(15.0, timeout_s),
    )


def _is_retryable_status(code: int) -> bool:
    return code in (408, 429, 500, 502, 503, 504)


def _resp_snippet(resp: httpx.Response, limit: int = 250) -> str:
    try:
        t = resp.text
        t = clean_text(t)
        return t[:limit]
    except Exception:
        return "<no-body>"


def odata_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_s: float = 150.0,
    tries: int = 7,
) -> Dict[str, Any]:
    """
    GET resiliente para Olinda:
    - retry com backoff exponencial + jitter em timeouts/429/5xx
    - se 400: retorna erro "rápido" com snippet do body (normalmente explica)
    """
    last_err: Exception | None = None

    headers = {
        "Accept": "application/json",
        "User-Agent": "risk-bank-ingest/2.0",
    }

    for attempt in range(1, tries + 1):
        try:
            with httpx.Client(timeout=_timeout(timeout_s), headers=headers, follow_redirects=True) as client:
                r = client.get(url, params=params or {})
                if r.status_code == 400:
                    # Olinda costuma devolver 400 com mensagem útil (ex: URI malformed)
                    snippet = _resp_snippet(r)
                    raise httpx.HTTPStatusError(
                        f"400 Bad Request. Body(snippet)={snippet}",
                        request=r.request,
                        response=r,
                    )

                if r.status_code >= 400:
                    if _is_retryable_status(r.status_code):
                        raise httpx.HTTPStatusError(
                            f"{r.status_code} retryable",
                            request=r.request,
                            response=r,
                        )
                    r.raise_for_status()

                return r.json()

        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
            last_err = e
        except httpx.HTTPStatusError as e:
            last_err = e
            code = e.response.status_code if e.response is not None else None
            # 400: não adianta insistir (query inválida)
            if code == 400:
                break
        except Exception as e:
            last_err = e

        # backoff + jitter
        sleep_s = min(25.0, (2 ** (attempt - 1)) * 0.7) + random.random() * 0.8
        time.sleep(sleep_s)

    raise RuntimeError(f"Falha no GET {url} params={params}. Último erro: {last_err}") from last_err


# -----------------------------
# OData pagination helpers
# -----------------------------
def iter_odata_follow_nextlink(
    url: str,
    params: Optional[Dict[str, Any]],
    timeout_s: float,
) -> Iterator[Dict[str, Any]]:
    """
    Itera em um entity-set seguindo @odata.nextLink quando existir.
    Essa é a forma MAIS compatível (evita $skip/$top/$orderby).
    """
    next_url = url
    next_params = params or {}

    while True:
        data = odata_get(next_url, params=next_params, timeout_s=timeout_s)
        rows = data.get("value", []) or []
        for r in rows:
            yield r

        nl = data.get("@odata.nextLink") or data.get("odata.nextLink") or data.get("nextLink")
        if not nl:
            break

        # nextLink já vem com query string pronta, então params devem ser vazios
        next_url = str(nl)
        next_params = {}


# -----------------------------
# Cadastro (ANTI-400): usar somente $format + nextLink
# -----------------------------
def iter_cadastro_raw(base: str, timeout_s: float) -> Iterator[Dict[str, Any]]:
    url = CADASTRO_URL.format(base=base)

    # Evita $top/$skip/$select/$orderby/$filter (fontes de 400)
    params = {"$format": "json"}
    yield from iter_odata_follow_nextlink(url, params=params, timeout_s=timeout_s)


def build_cadastro_map(anomes: int, tipo: int, base: str, timeout_s: float, name_max_len: int) -> Dict[str, str]:
    """
    CodInst -> Nome filtrando no Python (AnoMes e TipoInstituicao).
    """
    print("==> Carregando cadastro (CodInst -> Nome)...")

    cmap: Dict[str, str] = {}
    matched = 0
    total = 0

    for r in iter_cadastro_raw(base=base, timeout_s=timeout_s):
        total += 1
        try:
            if int(r.get("AnoMes")) != int(anomes):
                continue
            if int(r.get("TipoInstituicao")) != int(tipo):
                continue
        except Exception:
            continue

        cod = str(r.get("CodInst") or "").strip()
        nome = safe_trunc(clean_text(str(r.get("Nome") or "")), name_max_len)

        if cod:
            cmap[cod] = nome
            matched += 1

    print(f"    cadastro_raw rows: {total}")
    print(f"    cadastro_map size: {len(cmap)} (matches={matched})")
    return cmap


def get_latest_anomes(base: str, timeout_s: float) -> int:
    """
    Detecção do AnoMes mais recente sem $orderby:
    varre IfDataCadastro e pega max(AnoMes).
    """
    max_anomes = 0
    for r in iter_cadastro_raw(base=base, timeout_s=timeout_s):
        try:
            a = int(r.get("AnoMes"))
            if a > max_anomes:
                max_anomes = a
        except Exception:
            continue
    if max_anomes <= 0:
        raise RuntimeError("Não consegui detectar AnoMes (max) via IfDataCadastro raw.")
    return max_anomes


# -----------------------------
# IF.data: Valores (function) com paginação $skip/$top
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
    """
    Retorna páginas (skip, rows). Se houver timeout, reduz 'top' automaticamente.
    """
    url = VALORES_FUNCTION_URL.format(base=base)
    rel_clean = str(rel).strip().replace("'", "")

    skip = start_skip
    page_top = max(200, int(top))  # não começa minúsculo

    while True:
        params = {
            "$format": "json",
            "$top": page_top,
            "$skip": skip,
            "@AnoMes": int(anomes),
            "@TipoInstituicao": int(tipo),
            "@Relatorio": f"'{rel_clean}'",
        }

        try:
            data = odata_get(url, params=params, timeout_s=timeout_s)
        except RuntimeError as e:
            msg = str(e)
            # Se foi timeout, reduz page size e tenta de novo
            if "ReadTimeout" in msg or "timed out" in msg:
                new_top = max(200, page_top // 2)
                if new_top == page_top:
                    raise
                print(f"[WARN] Timeout em Valores rel={rel_clean} skip={skip}. Reduzindo $top {page_top} -> {new_top} e retry.")
                page_top = new_top
                continue
            raise

        rows = data.get("value", []) or []
        if not rows:
            break

        yield (skip, rows)

        # se veio menos que page_top, acabou
        if len(rows) < page_top:
            break

        skip += len(rows)


# -----------------------------
# DB upsert (long table)
# -----------------------------
def upsert_batch_long(batch: List[Dict[str, Any]]) -> int:
    if not batch:
        return 0

    stmt = text(
        """
        INSERT INTO ifdata_indicators (ref_date, institution_id, institution_name, indicator, value)
        VALUES (:ref_date, :institution_id, :institution_name, :indicator, :value)
        ON CONFLICT (ref_date, institution_id, indicator)
        DO UPDATE SET
          value = EXCLUDED.value,
          institution_name = EXCLUDED.institution_name
        """
    )
    with engine.begin() as conn:
        conn.execute(stmt, batch)
    return len(batch)


# -----------------------------
# Resume / checkpoint
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
# Main ingest per relatório
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
    commit_every: int,
    indicator_max_len: int,
    name_max_len: int,
) -> int:
    state = load_state(state_path) if resume else {}
    cp = get_checkpoint(state, anomes, tipo, rel) if resume else None

    start_skip = int(cp.skip) if cp else 0
    page_top = int(cp.top) if (cp and cp.top > 0) else int(top_initial)

    total_upserts = 0
    seen_inst = set()

    batch: List[Dict[str, Any]] = []
    last_commit_at = 0

    for skip, rows in iter_ifdata_valores_pages(
        anomes=anomes,
        tipo=tipo,
        rel=rel,
        base=base,
        top=page_top,
        start_skip=start_skip,
        timeout_s=timeout_s,
    ):
        for row in rows:
            # Você mostrou que a row tem:
            # ['AnoMes','CodInst','Conta','DescricaoColuna','Grupo','NomeColuna','NomeRelatorio','NumeroRelatorio','Saldo','TipoInstituicao']
            cod_inst = str(row.get("CodInst") or "").strip()
            if not cod_inst:
                continue

            # nome vem do cadastro_map (mais estável)
            inst_name = cadastro_map.get(cod_inst) or ""
            inst_name = safe_trunc(inst_name, name_max_len)

            # indicador correto:
            num_rel = str(row.get("NumeroRelatorio") or rel).strip()
            nome_col = (
                row.get("DescricaoColuna")
                or row.get("NomeColuna")
                or row.get("Conta")
                or ""
            )
            nome_col = clean_indicator_name(str(nome_col))
            if not nome_col:
                continue

            indicator = safe_trunc(f"{num_rel}::{nome_col}", indicator_max_len)

            value = to_float(row.get("Saldo"))
            if value is None:
                continue

            batch.append(
                {
                    "ref_date": ref_date.isoformat(),
                    "institution_id": cod_inst,
                    "institution_name": inst_name,
                    "indicator": indicator,
                    "value": value,
                }
            )

            seen_inst.add(cod_inst)

            if len(batch) >= commit_every:
                n = upsert_batch_long(batch)
                total_upserts += n
                batch.clear()

                # checkpoint
                now = datetime.utcnow().isoformat()
                set_checkpoint(
                    state,
                    Checkpoint(anomes=anomes, tipo=tipo, rel=rel, top=page_top, skip=skip + len(rows), updated_at=now),
                )
                save_state(state_path, state)
                last_commit_at = total_upserts
                print(f"  upsert +{n} (total={total_upserts}) | checkpoint skip={skip + len(rows)}")

        # checkpoint também ao fim da página (segurança)
        if resume and total_upserts != last_commit_at:
            now = datetime.utcnow().isoformat()
            set_checkpoint(
                state,
                Checkpoint(anomes=anomes, tipo=tipo, rel=rel, top=page_top, skip=skip + len(rows), updated_at=now),
            )
            save_state(state_path, state)
            last_commit_at = total_upserts

    if batch:
        n = upsert_batch_long(batch)
        total_upserts += n
        batch.clear()

    # checkpoint final
    if resume:
        now = datetime.utcnow().isoformat()
        set_checkpoint(
            state,
            Checkpoint(anomes=anomes, tipo=tipo, rel=rel, top=page_top, skip=999_999_999, updated_at=now),
        )
        save_state(state_path, state)

    print(f"  instituições únicas processadas: {len(seen_inst)}")
    return total_upserts


# -----------------------------
# CLI
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Ingest IF.data (BCB Olinda) -> ifdata_indicators (long format)")

    ap.add_argument("--odata-base", type=str, default=ODATA_DEFAULT)
    ap.add_argument("--anomes", type=int, default=0, help="AnoMes (YYYYMM). Se 0, auto-detect (max em cadastro raw).")
    ap.add_argument("--ref-date", type=str, default="", help="YYYY-MM-DD. Se vazio, último dia do mês do AnoMes.")
    ap.add_argument("--tipo", type=int, default=1)
    ap.add_argument("--relatorios", type=str, default="1,4,5")

    ap.add_argument("--top", type=int, default=2000, help="Page size ($top) para IfDataValores função.")
    ap.add_argument("--timeout", type=float, default=180.0)

    ap.add_argument("--state-path", type=str, default=".ifdata_ingest_state.json")
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--commit-every", type=int, default=10000)

    ap.add_argument("--indicator-max-len", type=int, default=220)
    ap.add_argument("--name-max-len", type=int, default=200)

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

    # CADASTRO com nextLink (anti-400)
    cadastro_map = build_cadastro_map(
        anomes=anomes,
        tipo=tipo,
        base=base,
        timeout_s=timeout_s,
        name_max_len=int(args.name_max_len),
    )

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
