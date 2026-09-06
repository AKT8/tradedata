"""
Core library for scraping sharehubnepal.com NEPSE floorsheet data.

Confirmed API contract (verified by hand, 2026-09):
  GET https://sharehubnepal.com/live/api/v2/floorsheet?Size=100&date=YYYY-M-D&page=N

  - `Size` is HARD-CAPPED at 100 server-side regardless of what you ask for.
  - Page number param is `page` (lowercase). Every other candidate
    (Page, pageIndex, PageIndex, pageNumber, offset, skip, ...) has no effect.
  - date format is UNPADDED: 2026-8-27, not 2026-08-27.
  - A date outside the available range does NOT error — it silently returns
    a DIFFERENT date's data (confirmed: requesting 2020-1-1 returned
    businessDate=2024-01-01). Always verify the returned businessDate matches
    what you asked for before trusting a response.
  - A weekend or not-yet-traded date returns totalItems=0, content=[].
  - Response shape:
      data: {
        totalAmount, totalQty, totalTrades,
        pageIndex, totalPages, totalItems, pageSize,
        hasPrevious, hasNext,
        content: [{ symbol, name, buyerMemberId, sellerMemberId, contractId,
                     contractQuantity, contractRate, contractAmount,
                     businessDate, buyerBrokerName, sellerBrokerName, tradeTime }]
      }
"""

import time
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_URL = "https://sharehubnepal.com/live/api/v2/floorsheet"
PAGE_PARAM = "page"
PAGE_SIZE = 100  # confirmed hard cap — asking for more does nothing

DEFAULT_WORKERS = 10   # concurrent requests per day; tune via fetch_full_day(workers=...)
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 1.5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (research scraper; contact: you@example.com)",
    "Accept": "application/json",
}


class OutOfRangeDateError(Exception):
    """Raised when the API silently returns a different date than requested."""
    pass


def format_date(d) -> str:
    """API expects unpadded YYYY-M-D, e.g. 2026-8-27 (not 2026-08-27)."""
    if isinstance(d, str):
        return d
    return f"{d.year}-{d.month}-{d.day}"


def to_iso(date_str: str) -> str:
    """'2026-8-27' -> '2026-08-27'"""
    y, m, d = date_str.split("-")
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"


def _get_page(session: requests.Session, date_str: str, page: int) -> dict:
    params = {"Size": PAGE_SIZE, "date": date_str, PAGE_PARAM: page}
    resp = session.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success", True):
        raise RuntimeError(f"API returned success=false: {payload.get('message')}")
    return payload["data"]


def _get_page_with_retry(session: requests.Session, date_str: str, page: int) -> dict:
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return _get_page(session, date_str, page)
        except Exception as e:
            last_err = e
            time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
    raise last_err


def fetch_full_day(target_date, workers: int = DEFAULT_WORKERS, verbose: bool = False) -> dict:
    """Fetch every floorsheet row for one date, using threaded pagination.

    Returns: {"date": "YYYY-M-D", "records": [...], "meta": {...}, "elapsed": float}
    Raises OutOfRangeDateError if the API's businessDate doesn't match what
    was requested (silent substitution for out-of-range dates).
    """
    date_str = format_date(target_date)
    session = requests.Session()
    t0 = time.time()

    first = _get_page_with_retry(session, date_str, 1)
    total_items = first["totalItems"]
    total_pages = first["totalPages"]
    meta = {k: first[k] for k in ("totalAmount", "totalQty", "totalTrades", "totalItems")}

    if total_items == 0:
        return {"date": date_str, "records": [], "meta": meta, "elapsed": time.time() - t0}

    seen_date = first["content"][0]["businessDate"][:10]
    requested_iso = to_iso(date_str)
    if seen_date != requested_iso:
        raise OutOfRangeDateError(
            f"Requested {requested_iso} but API returned businessDate={seen_date} "
            f"— date is outside the available range, or the API substituted a fallback date."
        )

    records = list(first["content"])

    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_get_page_with_retry, session, date_str, p): p
                       for p in range(2, total_pages + 1)}
            done = 0
            for fut in as_completed(futures):
                records.extend(fut.result()["content"])
                done += 1
                if verbose and done % 50 == 0:
                    print(f"  ...{done}/{total_pages - 1} pages")

    return {"date": date_str, "records": records, "meta": meta, "elapsed": time.time() - t0}


def check_completeness(result: dict) -> tuple[bool, list[str]]:
    """Sanity-check a fetch_full_day() result. Returns (ok, problems)."""
    problems = []
    records, meta = result["records"], result["meta"]

    if len(records) != meta["totalItems"]:
        problems.append(f"row count mismatch: got {len(records)}, API says {meta['totalItems']}")

    if meta["totalItems"] > 0 and records:
        summed_qty = sum(r["contractQuantity"] for r in records)
        summed_amt = sum(r["contractAmount"] for r in records)
        if abs(summed_qty - meta["totalQty"]) > 1:
            problems.append(f"quantity sum mismatch: {summed_qty} vs {meta['totalQty']}")
        if abs(summed_amt - meta["totalAmount"]) > 1:
            problems.append(f"amount sum mismatch: {summed_amt} vs {meta['totalAmount']}")

        ids = [r["contractId"] for r in records]
        if len(ids) != len(set(ids)):
            problems.append("duplicate contractIds found")

        requested_iso = to_iso(result["date"])
        wrong_date = {r["businessDate"][:10] for r in records} - {requested_iso}
        if wrong_date:
            problems.append(f"records with unexpected businessDate: {wrong_date}")

    return (len(problems) == 0, problems)


def is_nepse_trading_day(d: date) -> bool:
    """NEPSE trades Sunday-Thursday; Friday and Saturday are the weekend."""
    return d.weekday() != 5  # Saturday


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def records_to_parquet(records: list, out_path: str):
    import pandas as pd

    df = pd.DataFrame(records)
    if not df.empty:
        for col in ("symbol", "name", "buyerBrokerName", "sellerBrokerName"):
            if col in df.columns:
                df[col] = df[col].astype("category")
        for col in ("businessDate", "tradeTime"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="mixed", utc=True)
    df.to_parquet(out_path, compression="zstd", index=False)
    return df
