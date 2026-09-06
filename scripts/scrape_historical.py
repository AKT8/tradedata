#!/usr/bin/env python3

"""
Backfills a date range of NEPSE floorsheet data.

Usage:
    python scripts/scrape_historical.py \
        --start 2024-01-01 \
        --end 2024-12-31

    python scripts/scrape_historical.py \
        --start 2024-01-01 \
        --end 2026-08-27 \
        --skip-existing

Writes one Parquet file per trading day under:

    data/<year>/<YYYY-MM-DD>.parquet
"""

import argparse
import os
import sys
import time
from datetime import datetime

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..")
)

from scraper.floorsheet_lib import (
    fetch_full_day,
    check_completeness,
    records_to_parquet,
    is_nepse_trading_day,
    daterange,
    OutOfRangeDateError,
)


DATA_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "data",
)

FAILURES_LOG = os.path.join(
    os.path.dirname(__file__),
    "..",
    "backfill_failures.txt",
)

MAX_ATTEMPTS = 3


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        required=True,
        help="YYYY-MM-DD",
    )

    parser.add_argument(
        "--end",
        required=True,
        help="YYYY-MM-DD",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Concurrent requests per day",
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip dates that already have a parquet file",
    )

    args = parser.parse_args()

    start = datetime.strptime(
        args.start,
        "%Y-%m-%d",
    ).date()

    end = datetime.strptime(
        args.end,
        "%Y-%m-%d",
    ).date()

    dates = list(daterange(start, end))
    total_dates = len(dates)

    ok_count = 0
    fail_count = 0
    skip_count = 0
    out_of_range_count = 0

    failures = []

    overall_start = time.time()

    print()
    print("=" * 70)
    print("NEPSE HISTORICAL FLOORSHEET BACKFILL")
    print("=" * 70)
    print(f"Start date      : {start}")
    print(f"End date        : {end}")
    print(f"Calendar dates  : {total_dates}")
    print(f"Workers/day     : {args.workers}")
    print(f"Skip existing   : {args.skip_existing}")
    print("=" * 70)
    print(flush=True)

    for date_index, d in enumerate(dates, 1):

        iso = d.isoformat()

        print()
        print("-" * 70)
        print(
            f"DATE {date_index}/{total_dates}: {iso}",
            flush=True,
        )
        print("-" * 70)

        if not is_nepse_trading_day(d):

            print(
                f"[{iso}] skipped by weekly calendar",
                flush=True,
            )

            continue

        out_path = os.path.join(
            DATA_DIR,
            iso[:4],
            f"{iso}.parquet",
        )

        if args.skip_existing and os.path.exists(out_path):

            skip_count += 1

            print(
                f"[{iso}] already exists — skipped",
                flush=True,
            )

            continue

        success = False

        for attempt in range(
            1,
            MAX_ATTEMPTS + 1,
        ):

            try:

                print(
                    f"[{iso}] downloading "
                    f"(attempt {attempt}/{MAX_ATTEMPTS})",
                    flush=True,
                )

                result = fetch_full_day(
                    d,
                    workers=args.workers,
                    verbose=True,
                )

            except OutOfRangeDateError as e:

                print(
                    f"[{iso}] OUT OF RANGE: {e}",
                    flush=True,
                )

                out_of_range_count += 1

                success = True

                break

            except Exception as e:

                print(
                    f"[{iso}] request error "
                    f"(attempt {attempt}): {e}",
                    flush=True,
                )

                if attempt < MAX_ATTEMPTS:

                    print(
                        f"[{iso}] retrying in 5 seconds",
                        flush=True,
                    )

                    time.sleep(5)

                continue

            if not result["records"]:

                print(
                    f"[{iso}] no trades "
                    f"(holiday or non-trading day)",
                    flush=True,
                )

                success = True

                break

            good, problems = check_completeness(
                result
            )

            if good:

                try:

                    os.makedirs(
                        os.path.dirname(out_path),
                        exist_ok=True,
                    )

                    df = records_to_parquet(
                        result["records"],
                        out_path,
                    )

                except Exception as e:

                    print(
                        f"[{iso}] parquet conversion error: {e}",
                        flush=True,
                    )

                    if attempt < MAX_ATTEMPTS:

                        print(
                            f"[{iso}] retrying conversion",
                            flush=True,
                        )

                        time.sleep(5)

                    continue

                ok_count += 1
                success = True

                print(
                    f"[{iso}] COMPLETE — "
                    f"{len(df):,} rows "
                    f"in {result['elapsed']:.1f}s",
                    flush=True,
                )

                break

            else:

                print(
                    f"[{iso}] incomplete "
                    f"(attempt {attempt}):",
                    flush=True,
                )

                for problem in problems:

                    print(
                        f"    {problem}",
                        flush=True,
                    )

                if attempt < MAX_ATTEMPTS:

                    print(
                        f"[{iso}] retrying in 5 seconds",
                        flush=True,
                    )

                    time.sleep(5)

        if not success:

            fail_count += 1

            failures.append(iso)

            print(
                f"[{iso}] FAILED",
                flush=True,
            )

    overall_elapsed = (
        time.time() - overall_start
    )

    print()
    print("=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)

    print(
        f"Successful       : {ok_count:,}",
        flush=True,
    )

    print(
        f"Skipped existing  : {skip_count:,}",
        flush=True,
    )

    print(
        f"Out of range      : {out_of_range_count:,}",
        flush=True,
    )

    print(
        f"Failed            : {fail_count:,}",
        flush=True,
    )

    print(
        f"Elapsed           : {overall_elapsed:.1f}s",
        flush=True,
    )

    print("=" * 70)

    if failures:

        print(
            "Failed dates:",
            flush=True,
        )

        for iso in failures:

            print(
                f"    {iso}",
                flush=True,
            )

        with open(
            FAILURES_LOG,
            "a",
            encoding="utf-8",
        ) as f:

            for iso in failures:

                f.write(
                    iso + "\n"
                )

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
