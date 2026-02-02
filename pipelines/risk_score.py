from __future__ import annotations

import argparse
import json
from sqlalchemy import text

from core.db import engine


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def score_bank(m: dict) -> tuple[float, str, dict]:
    bas = m.get("basileia")
    liq = m.get("liquidez")
    roa = m.get("roa")
    npl = m.get("inadimplencia")
    lev = m.get("alavancagem")

    drivers = {}

    # Basileia (%)
    s_bas = 8.0 if bas is None else (30 if bas < 8 else 20 if bas < 10 else 10 if bas < 12 else 0)
    drivers["basileia"] = {"value": bas, "score": s_bas}

    # Liquidez (índice)
    s_liq = 6.0 if liq is None else (25 if liq < 0.9 else 18 if liq < 1.0 else 10 if liq < 1.1 else 0)
    drivers["liquidez"] = {"value": liq, "score": s_liq}

    # ROA (%)
    s_roa = 5.0 if roa is None else (20 if roa < -1.0 else 12 if roa < 0.0 else 6 if roa < 0.5 else 0)
    drivers["roa"] = {"value": roa, "score": s_roa}

    # Inadimplência (%)
    s_npl = 4.0 if npl is None else (18 if npl > 10 else 12 if npl > 6 else 6 if npl > 4 else 0)
    drivers["inadimplencia"] = {"value": npl, "score": s_npl}

    # Alavancagem (Ativo/PL)
    s_lev = 4.0 if lev is None else (12 if lev > 20 else 8 if lev > 15 else 4 if lev > 10 else 0)
    drivers["alavancagem"] = {"value": lev, "score": s_lev}

    score = clamp(float(s_bas + s_liq + s_roa + s_npl + s_lev), 0, 100)

    if score >= 70:
        rating = "ALTO"
    elif score >= 40:
        rating = "MEDIO"
    else:
        rating = "BAIXO"

    return score, rating, drivers


def run(ref_date: str) -> None:
    q = text("SELECT * FROM mart_bank_metrics WHERE ref_date = :ref_date")
    with engine.begin() as conn:
        rows = conn.execute(q, {"ref_date": ref_date}).mappings().all()

    out = []
    for r in rows:
        score, rating, drivers = score_bank(dict(r))
        out.append(
            {
                "ref_date": ref_date,
                "bank_id": r["bank_id"],
                "bank_name": r["bank_name"],
                "score": score,
                "rating": rating,
                "drivers": json.dumps(drivers, ensure_ascii=False),
            }
        )

    up = text("""
      INSERT INTO mart_bank_risk (ref_date, bank_id, bank_name, score, rating, drivers)
      VALUES (:ref_date, :bank_id, :bank_name, :score, :rating, CAST(:drivers AS jsonb))
      ON CONFLICT (ref_date, bank_id)
      DO UPDATE SET
        bank_name = EXCLUDED.bank_name,
        score = EXCLUDED.score,
        rating = EXCLUDED.rating,
        drivers = EXCLUDED.drivers,
        created_at = now()
    """)

    with engine.begin() as conn:
        conn.execute(up, out)

    print(f"OK. mart_bank_risk upserted: {len(out)} bancos (ref_date={ref_date})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref-date", required=True)
    args = ap.parse_args()
    run(args.ref_date)


if __name__ == "__main__":
    main()
