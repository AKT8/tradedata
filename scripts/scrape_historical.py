#!/usr/bin/env python3
"""
Backfills a date range of NEPSE floorsheet data.

Usage:
    python scripts/scrape_historical.py --start 2024-01-01 --end 2024-12-31
    python scripts/scrape_historical.py --start 2024-01-01 --end 2026-08-27 --skip-existing

Writes one Parquet file per trading day under data/<year>/<YYYY-MM-DD>.parquet.
Run from a bounded GitHub Actions workflow_dispatch (e.g. one year at a time)
to keep the job under the timeout and commits reasonably sized.
"""

import argparse
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scraper.floorsheet_lib import (
    fetch_full_day, check_completeness, records_to_parquet,
    is_nepse_trading_day, daterange, OutOfRangeDateError,
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
FAILURES_LOG = os.path.join(os.path.dirname(__file__), "..", "backfill_failures.txt")

MAX_ATTEMPTS = 3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=10,
                         help="Concurrent requests per day")
    parser.add_argument("--skip-existing", action="store_true",
                         help="Skip dates that already have a parquet file (resume mode)")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    ok_count, fail_count, skip_count, out_of_range_count = 0, 0, 0, 0
    failures = []

    for d in daterange(start, end):
        if not is_nepse_trading_day(d):
            continue

        iso = d.isoformat()
        out_path = os.path.join(DATA_DIR, iso[:4], f"{iso}.parquet")

        if args.skip_existing and os.path.exists(out_path):
            skip_count += 1
            continue

        success = False
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                result = fetch_full_day(d, workers=args.workers)
            except OutOfRangeDateError as e:
                # This date is outside what the API serves — no point retrying.
                print(f"[{iso}] OUT OF RANGE: {e}")
                out_of_range_count += 1
                break
            except Exception as e:
                print(f"[{iso}] request error (attempt {attempt}): {e}")
                time.sleep(5)
                continue

            if not result["records"]:
                print(f"[{iso}] no trades (holiday or non-trading day per exchange calendar)")
                success = True
                break

            good, problems = check_completeness(result)
            if good:
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                df = records_to_parquet(result["records"], out_path)
                print(f"[{iso}] ok — {len(df)} rows in {result['elapsed']:.1f}s")
                ok_count += 1
                success = True
                break
            else:
                print(f"[{iso}] incomplete (attempt {attempt}): {problems}")
                time.sleep(5)

        if not success:
            fail_count += 1
            failures.append(iso)

    print("\n--- Summary ---")
    print(f"OK: {ok_count}  Skipped: {skip_count}  Out-of-range: {out_of_range_count}  Failed: {fail_count}")
    if failures:
        print("Failed dates:", ", ".join(failures))
        with open(FAILURES_LOG, "a") as f:
            for iso in failures:
                f.write(iso + "\n")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
