#!/usr/bin/env python3
"""
Scrapes ONE trading day (default: today, Nepal time) and writes it as a
single Parquet file under data/<year>/<YYYY-MM-DD>.parquet.

Usage:
    python scripts/scrape_daily.py                # today (Asia/Kathmandu)
    python scripts/scrape_daily.py --date 2026-08-27
"""

import argparse
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scraper.floorsheet_lib import (
    fetch_full_day, check_completeness, records_to_parquet,
    is_nepse_trading_day, OutOfRangeDateError,
)

NEPAL_TZ = timezone(timedelta(hours=5, minutes=45))
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

MAX_ATTEMPTS = 4
RETRY_DELAY_SEC = 30


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (defaults to today in Nepal time)")
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target = datetime.now(NEPAL_TZ).date()

    if not is_nepse_trading_day(target):
        print(f"{target} is a NEPSE weekend (Fri/Sat) — nothing to scrape.")
        return 0

    result = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            result = fetch_full_day(target, workers=args.workers)
        except OutOfRangeDateError as e:
            # Retrying won't help this — it's not a transient failure.
            print(f"[fatal] {e}")
            return 1

        ok, problems = check_completeness(result)
        if ok:
            print(f"[ok] {target}: {len(result['records'])} rows in {result['elapsed']:.1f}s, complete.")
            break
        print(f"[attempt {attempt}/{MAX_ATTEMPTS}] incomplete: {problems}")
        if attempt < MAX_ATTEMPTS:
            import time
            time.sleep(RETRY_DELAY_SEC)
    else:
        print(f"[fail] {target}: could not get complete data after {MAX_ATTEMPTS} attempts.")
        _write(result, target)
        return 1

    _write(result, target)
    return 0


def _write(result, target):
    if not result or not result["records"]:
        print(f"No records for {target}; skipping write.")
        return
    iso = target.isoformat()
    year = iso[:4]
    out_dir = os.path.join(DATA_DIR, year)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{iso}.parquet")
    df = records_to_parquet(result["records"], out_path)
    size_kb = os.path.getsize(out_path) / 1024
    print(f"Wrote {out_path} ({len(df)} rows, {size_kb:.1f} KB)")


if __name__ == "__main__":
    sys.exit(main())
