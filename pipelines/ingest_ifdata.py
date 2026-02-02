from __future__ import annotations

import argparse
import datetime as dt
import math
import re
import time
from typing import Any, Iterable

import httpx
from sqlalchemy import text

from core.settings import settings
from core.db import engine

ODATA_BASE = settings.ifdata_odata_base.rstrip("/")

VALORES_ENTITYSET_URL = f"{ODATA_BASE}/IfDataValores"
VALORES_FUNCTION_URL = (
    f"{ODATA_BASE}/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
)
CADASTRO_FUNCTION_URL = f"{ODATA_BASE}/IfDataCadastro(AnoMes=@AnoMes)"


def _last_day_of_month(year: int, month: int) -> dt.date:
    if month == 12:
        return dt.date(year, 12, 31)
    return dt.date(year, month + 1, 1) - dt.timedelta(days=1)


def anomes_to_ref_date(anomes: int) -> str:
    year = anomes // 100
    month = anomes % 100
    return _last_day_of_month(year, month).isoformat()


def _is_number(x: Any) -> bool:
    if isinstance(x, bool):
        return False
    if isinstance(x, (int, float)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return False
        return True
    if isinstance(x, str):
        s = x.strip().replace(".", "").replace(",", ".")
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            try:
                v = float(s)
                return not (math.isnan(v) or math.isinf(v))
            except Exception:
                return False
    return False


def _to_float(x: Any) -> float:
    if isinstance(x, bool):
        raise ValueError("bool não é número válido")
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(".", "").replace(",", ".")
        return float(s)
    raise ValueError(f"Não consegui converter para float: {x!r}")


def clean_indicator_name(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def odata_get(url: str, params: dict[str, Any], timeout_s: float = 30.0, tries: int = 4) -> dict:
    last_err: Exception | None = None
    for attempt in range(1, tries + 1):
        try:
            with httpx.Client(timeout=timeout_s, headers={"Accept": "application/json"}) as client:
                r = client.get(url, params=params)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.25 * attempt)
    raise RuntimeError(f"Falha no GET {url} params={params}. Último erro: {last_err}") from last_err


def candidate_anomes_from_today(n_quarters: int = 16) -> list[int]:
    today = dt.date.today()
    quarter_months = [12, 9, 6, 3]
    m = max([qm for qm in quarter_months if qm <= today.month] or [12])
    y = today.year
    if today.month < 3:
        y = today.year - 1
        m = 12

    out: list[int] = []
    cur_y, cur_m = y, m
    for _ in range(n_quarters):
        out.append(cur_y * 100 + cur_m)
        if cur_m == 12:
            cur_m = 9
        elif cur_m == 9:
            cur_m = 6
        elif cur_m == 6:
            cur_m = 3
        else:
            cur_m = 12
            cur_y -= 1
    return out


def iter_valores_function(anomes: int, tipo: int, rel: str, top: int, timeout_s: float) -> Iterable[dict[str, Any]]:
    skip = 0
    rel_clean = str(rel).strip().replace("'", "")
    while True:
        params = {
            "$format": "json",
            "$top": top,
            "$skip": skip,
            "@AnoMes": anomes,
            "@TipoInstituicao": tipo,
            "@Relatorio": f"'{rel_clean}'",
        }
        data = odata_get(VALORES_FUNCTION_URL, params=params, timeout_s=timeout_s)
        rows = data.get("value", [])
        if not rows:
            break
        for row in rows:
            yield row
        if len(rows) < top:
            break
        skip += top


def iter_valores_filter(anomes: int, tipo: int, rel: str, top: int, timeout_s: float) -> Iterable[dict[str, Any]]:
    rel_clean = str(rel).strip().replace("'", "")
    filters = [
        f"AnoMes eq {anomes} and TipoInstituicao eq {tipo} and NumeroRelatorio eq {rel_clean}",
        f"AnoMes eq {anomes} and TipoInstituicao eq {tipo} and NumeroRelatorio eq '{rel_clean}'",
        f"AnoMes eq {anomes} and TipoInstituicao eq {tipo} and Relatorio eq {rel_clean}",
        f"AnoMes eq {anomes} and TipoInstituicao eq {tipo} and Relatorio eq '{rel_clean}'",
    ]
    for fexpr in filters:
        skip = 0
        got_any = False
        while True:
            data = odata_get(
                VALORES_ENTITYSET_URL,
                params={"$format": "json", "$top": top, "$skip": skip, "$filter": fexpr},
                timeout_s=timeout_s,
            )
            rows = data.get("value", [])
            if not rows:
                break
            got_any = True
            for row in rows:
                yield row
            if len(rows) < top:
                break
            skip += top
        if got_any:
            return


def iter_ifdata_valores(anomes: int, tipo: int, rel: str, top: int, timeout_s: float) -> Iterable[dict[str, Any]]:
    any_row = False
    for row in iter_valores_function(anomes, tipo, rel, top, timeout_s):
        any_row = True
        yield row
    if any_row:
        return
    for row in iter_valores_filter(anomes, tipo, rel, top, timeout_s):
        yield row


def load_cadastro_map(anomes: int, timeout_s: float) -> dict[str, str]:
    params = {
        "$format": "json",
        "$top": 200000,
        "@AnoMes": anomes,
    }
    data = odata_get(CADASTRO_FUNCTION_URL, params=params, timeout_s=timeout_s)
    rows = data.get("value", [])

    out: dict[str, str] = {}
    id_keys = ["CodInst", "CodIF", "CodIf", "CodInstituicao", "CodigoInstituicao", "CodConglomerado", "CodCong"]
    name_keys = ["NomeInstituicao", "Nome", "NomeIF", "NomeIf", "NomeConglomerado", "NomeCong"]

    for r in rows:
        cid = None
        for k in id_keys:
            if k in r and r[k] not in (None, ""):
                cid = str(r[k]).strip()
                break
        if not cid:
            continue
        cname = None
        for k in name_keys:
            if k in r and r[k] not in (None, ""):
                cname = str(r[k]).strip()
                break
        if cname:
            out[cid] = cname

    return out


def find_working_anomes(relatorio: str, top: int, timeout_s: float, tipos: list[int] = [1, 2, 3, 4], n_quarters: int = 16):
    for anomes in candidate_anomes_from_today(n_quarters=n_quarters):
        for tipo in tipos:
            got = False
            for _ in iter_valores_function(anomes=anomes, tipo=tipo, rel=relatorio, top=min(5, top), timeout_s=timeout_s):
                got = True
                break
            if got:
                return anomes, tipo
    return None


def upsert_batch_long(batch: list[dict[str, Any]]) -> int:
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


def run(anomes: int, tipo: int, relatorios: list[str], top: int, timeout_s: float, chunk: int, debug_sample: bool) -> None:
    ref_date = anomes_to_ref_date(anomes)
    print(f"==> Ingest IF.data: AnoMes={anomes} ref_date={ref_date} tipo={tipo} relatorios={relatorios}")
    print(f"ODATA_BASE={ODATA_BASE}")

    print("==> Carregando cadastro (CodInst -> Nome)...")
    cadastro_map = load_cadastro_map(anomes=anomes, timeout_s=timeout_s)
    print(f"    cadastro_map size: {len(cadastro_map)}")

    total = 0

    for rel in relatorios:
        print(f"\n--- Relatório {rel} ---")
        batch: list[dict[str, Any]] = []
        row_count = 0
        inst_seen: set[str] = set()
        sample_printed = False

        for row in iter_ifdata_valores(anomes=anomes, tipo=tipo, rel=rel, top=top, timeout_s=timeout_s):
            row_count += 1

            if debug_sample and not sample_printed:
                print("SAMPLE ROW KEYS:", sorted(row.keys()))
                print("SAMPLE ROW:", row)
                sample_printed = True

            inst_id = row.get("CodInst")
            nome_coluna = row.get("NomeColuna")
            saldo = row.get("Saldo")
            num_rel = row.get("NumeroRelatorio") or rel

            if not inst_id or not nome_coluna or saldo is None:
                continue
            if not _is_number(saldo):
                continue

            inst_id = str(inst_id).strip()
            inst_name = cadastro_map.get(inst_id, "")

            indicator = f"{num_rel}::{clean_indicator_name(str(nome_coluna))}"
            value = _to_float(saldo)

            inst_seen.add(inst_id)

            batch.append(
                {
                    "ref_date": ref_date,
                    "institution_id": inst_id,
                    "institution_name": inst_name,
                    "indicator": indicator,
                    "value": value,
                }
            )

            if len(batch) >= chunk:
                n = upsert_batch_long(batch)
                total += n
                print(f"  upsert +{n} (total={total})")
                batch.clear()

        if batch:
            n = upsert_batch_long(batch)
            total += n
            print(f"  upsert +{n} (total={total})")
            batch.clear()

        print(f"  linhas baixadas (raw): {row_count}")
        print(f"  instituições únicas processadas: {len(inst_seen)}")

    print(f"\nOK. Total de registros upsertados: {total}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingestão IF.data (LONG) -> ifdata_indicators")
    ap.add_argument("--anomes", type=int, default=0, help="AAAAMM (ex: 202509). 0 = auto-detect.")
    ap.add_argument("--tipo", type=int, default=1, help="TipoInstituicao")
    ap.add_argument("--relatorios", default="1,4,5", help="Relatórios")
    ap.add_argument("--top", type=int, default=5000, help="Page size")
    ap.add_argument("--timeout", type=float, default=settings.request_timeout_s)
    ap.add_argument("--chunk", type=int, default=10000)
    ap.add_argument("--debug-sample", action="store_true")
    args = ap.parse_args()

    anomes = int(args.anomes)
    tipo = int(args.tipo)
    rels = [x.strip() for x in str(args.relatorios).split(",") if x.strip()]

    if anomes == 0:
        probe = find_working_anomes(relatorio="1", top=int(args.top), timeout_s=float(args.timeout))
        if probe is None:
            raise RuntimeError("Auto-detect falhou. Tente passar --anomes manualmente.")
        anomes, tipo_found = probe
        if tipo == 1:
            tipo = tipo_found
        print(f"[INFO] Auto-detect: AnoMes={anomes}, TipoInstituicao={tipo}")

    run(
        anomes=anomes,
        tipo=tipo,
        relatorios=rels,
        top=int(args.top),
        timeout_s=float(args.timeout),
        chunk=int(args.chunk),
        debug_sample=bool(args.debug_sample),
    )


if __name__ == "__main__":
    main()
