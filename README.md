# NEPSE Floorsheet Archive

Daily NEPSE floorsheet data from sharehubnepal.com, stored as one
Parquet file per trading day under `data/<year>/<YYYY-MM-DD>.parquet`.

## API facts this scraper relies on (verified by hand)

- `Size` is hard-capped at **100** rows per request regardless of what's requested.
- The pagination query param is **`page`** (lowercase) — every other candidate
  (`Page`, `pageIndex`, `pageNumber`, `offset`, `skip`, ...) has no effect.
- `date` must be **unpadded**: `2026-8-27`, not `2026-08-27`.
- A date outside the available range does **not** error — it silently returns
  a *different* date's data. The scraper checks the returned `businessDate`
  against the requested date on every fetch and raises `OutOfRangeDateError`
  if they don't match, rather than silently saving mislabeled data.
- Weekends (Fri/Sat, NEPSE's weekend) and not-yet-traded dates return
  `totalItems: 0`.

## Setup

1. Push this repo to GitHub (public, so Actions minutes are free/unlimited).
2. Nothing else to configure — both workflows use `GITHUB_TOKEN`, which is
   provided automatically and has `contents: write` via the workflow's
   `permissions:` block.

## Daily workflow (`.github/workflows/daily-scrape.yml`)

Runs automatically Sun–Thu ~15:45 NPT (45 min after NEPSE closes), scrapes
just that day, and commits one Parquet file if the data is complete.
Retries up to 4 times (30s apart) if the completeness check fails.

Manually re-run for a specific day via **Actions → Daily Floorsheet Scrape →
Run workflow**, entering a `date`.

## Historical backfill (`.github/workflows/historical-scrape.yml`)

Manual only. **Actions → Historical Floorsheet Backfill → Run workflow**,
enter a `start_date` and `end_date`.

Recommended: run **one calendar year at a time** rather than multiple years
in one go — keeps each commit's diff small and the job comfortably inside
its 300-minute timeout. Use `skip_existing: true` (default) to safely
resume an interrupted run.

## Querying the data

```python
import duckdb
con = duckdb.connect()
df = con.execute("""
    SELECT * FROM read_parquet('data/*/*.parquet')
    WHERE symbol = 'NABIL' AND businessDate = '2026-08-27'
""").df()
```

Or directly from GitHub without cloning:

```python
df = con.execute("""
    SELECT * FROM read_parquet(
        'https://huggingface.co/datasets/you/nepse-floorsheet/resolve/main/data/*/*.parquet'
    )
""").df()  # if you mirror the repo to Hugging Face for glob support
```
