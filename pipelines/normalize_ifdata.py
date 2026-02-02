from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text

from core.db import engine


@dataclass(frozen=True)
class MetricRule:
    metric: str
    patterns: list[str]
    report_preference: list[str]


RULES: list[MetricRule] = [
    MetricRule(
        metric="ativo_total",
        patterns=[r"\bativo\s*total\b", r"\btotal\s*do\s*ativo\b"],
        report_preference=["1", "4"],
    ),
    MetricRule(
        metric="patrimonio_liquido",
        patterns=[r"\bpatrim[oô]nio\s*l[ií]quido\b"],
        report_preference=["1", "4"],
    ),
    MetricRule(
        metric="lucro_liquido",
        patterns=[r"\blucro\s*l[ií]quido\b", r"\bresultado\s*l[ií]quido\b"],
        report_preference=["1", "4"],
    ),
    # ✅ Basileia: SOMENTE índice
    MetricRule(
        metric="basileia",
        patterns=[r"^\s*[íi]ndice\s+de\s+basileia\b"],
        report_preference=["1", "5"],
    ),
    # ✅ Liquidez: SOMENTE índice (evita DRE com “liquidez”)
    MetricRule(
        metric="liquidez",
        patterns=[r"^\s*[íi]ndice\s+de\s+liquidez\b", r"\bLCR\b", r"\bNSFR\b"],
        report_preference=["5", "1"],
    ),
    MetricRule(
        metric="inadimplencia",
        patterns=[r"inadimpl", r"\bnpl\b", r"cr[eé]ditos?\s+em\s+atraso"],
        report_preference=["5", "1", "4"],
    ),
]


def clean_text(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def report_of_indicator(ind: str) -> str:
    return ind.split("::", 1)[0].strip() if "::" in ind else ""


def matches_any(name: str, patterns: list[str]) -> bool:
    for p in patterns:
        if re.search(p, name, flags=re.IGNORECASE):
            return True
    return False


def pick_best(rows: list[dict], rule: MetricRule) -> Optional[float]:
    candidates = []
    for r in rows:
        ind = clean_text(str(r["indicator"]))
        name = ind.split("::", 1)[1] if "::" in ind else ind
        if matches_any(name, rule.patterns):
            candidates.append((report_of_indicator(ind), name, float(r["value"])))

    if not candidates:
        return None

    def sane(metric: str, name: str, v: float) -> bool:
        # Basileia pode vir 0-1 (fração) ou 0-100 (%)
        if metric == "basileia":
            return 0.0 < v < 100.0
        # Liquidez como índice (ajuste se seu indicador real for outro range)
        if metric == "liquidez":
            return 0.0 < v < 10.0
        if metric == "inadimplencia":
            return 0.0 <= v < 100.0
        return True

    candidates = [(rep, name, v) for (rep, name, v) in candidates if sane(rule.metric, name, v)]
    if not candidates:
        return None

    for rep in rule.report_preference:
        rep_vals = [v for (rrep, _, v) in candidates if rrep == rep]
        if rep_vals:
            # para índices, maior magnitude não é ideal, mas com regex cirúrgico já fica OK
            return sorted(rep_vals, key=lambda x: abs(x), reverse=True)[0]

    return sorted([v for (_, _, v) in candidates], key=lambda x: abs(x), reverse=True)[0]


def normalize(ref_date: str) -> None:
    q = text(
        """
        SELECT institution_id, institution_name, indicator, value
        FROM ifdata_indicators
        WHERE ref_date = :ref_date
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(q, {"ref_date": ref_date}).mappings().all()

    by_bank: dict[str, dict] = {}
    for r in rows:
        bid = str(r["institution_id"])
        bname = str(r["institution_name"] or "")
        by_bank.setdefault(bid, {"bank_id": bid, "bank_name": bname, "rows": []})
        if bname and not by_bank[bid]["bank_name"]:
            by_bank[bid]["bank_name"] = bname
        by_bank[bid]["rows"].append({"indicator": r["indicator"], "value": r["value"]})

    out = []
    for bid, info in by_bank.items():
        bank_name = info["bank_name"] or ""
        bank_rows = info["rows"]

        metrics = {}
        for rule in RULES:
            metrics[rule.metric] = pick_best(bank_rows, rule)

        # ✅ Basileia: se vier fração (0<x<1), converte para %
        if metrics.get("basileia") is not None:
            b = metrics["basileia"]
            if 0 < b < 1:
                metrics["basileia"] = b * 100.0

        ativo = metrics.get("ativo_total")
        lucro = metrics.get("lucro_liquido")
        pl = metrics.get("patrimonio_liquido")

        roa = None
        if lucro is not None and ativo not in (None, 0):
            roa = (lucro / ativo) * 100.0

        alav = None
        if ativo is not None and pl not in (None, 0):
            alav = ativo / pl

        out.append(
            {
                "ref_date": ref_date,
                "bank_id": bid,
                "bank_name": bank_name,
                "ativo_total": ativo,
                "patrimonio_liquido": pl,
                "lucro_liquido": lucro,
                "basileia": metrics.get("basileia"),
                "liquidez": metrics.get("liquidez"),
                "inadimplencia": metrics.get("inadimplencia"),
                "roa": roa,
                "alavancagem": alav,
            }
        )

    up = text(
        """
        INSERT INTO mart_bank_metrics
          (ref_date, bank_id, bank_name, ativo_total, patrimonio_liquido, lucro_liquido,
           basileia, liquidez, inadimplencia, roa, alavancagem)
        VALUES
          (:ref_date, :bank_id, :bank_name, :ativo_total, :patrimonio_liquido, :lucro_liquido,
           :basileia, :liquidez, :inadimplencia, :roa, :alavancagem)
        ON CONFLICT (ref_date, bank_id)
        DO UPDATE SET
          bank_name = EXCLUDED.bank_name,
          ativo_total = EXCLUDED.ativo_total,
          patrimonio_liquido = EXCLUDED.patrimonio_liquido,
          lucro_liquido = EXCLUDED.lucro_liquido,
          basileia = EXCLUDED.basileia,
          liquidez = EXCLUDED.liquidez,
          inadimplencia = EXCLUDED.inadimplencia,
          roa = EXCLUDED.roa,
          alavancagem = EXCLUDED.alavancagem,
          updated_at = now()
        """
    )
    with engine.begin() as conn:
        conn.execute(up, out)

    print(f"OK. mart_bank_metrics upserted: {len(out)} bancos (ref_date={ref_date})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    normalize(args.ref_date)


if __name__ == "__main__":
    main()
