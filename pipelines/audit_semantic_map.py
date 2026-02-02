from __future__ import annotations

import argparse
from sqlalchemy import text

from core.db import engine
from pipelines.normalize_ifdata import RULES, clean_text, matches_any


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref-date", required=True)
    ap.add_argument("--limit", type=int, default=400)
    args = ap.parse_args()

    q = text("""
      SELECT indicator, count(*) c
      FROM ifdata_indicators
      WHERE ref_date = :ref_date
      GROUP BY indicator
      ORDER BY c DESC
      LIMIT :limit
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"ref_date": args.ref_date, "limit": args.limit}).fetchall()

    for indicator, c in rows:
        ind = clean_text(indicator)
        name = ind.split("::", 1)[1] if "::" in ind else ind
        hits = [rule.metric for rule in RULES if matches_any(name, rule.patterns)]
        print(f"[{c:>6}] {ind} -> {hits}")


if __name__ == "__main__":
    main()
