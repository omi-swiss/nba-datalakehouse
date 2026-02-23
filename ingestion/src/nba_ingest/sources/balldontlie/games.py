from __future__ import annotations

import time
import random
from datetime import date
from typing import Iterator, Optional, Dict, Any

import requests

from nba_ingest.config import BDL_PER_PAGE
from nba_ingest.common.http import get_json


def iter_games(
    base_url: str,
    api_key: str,
    *,
    season: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    per_page: int = BDL_PER_PAGE,
    min_sleep_s: float = 0.25,
) -> Iterator[Dict[str, Any]]:
    """
    Yield BallDontLie games using CURSOR pagination.

    - Uses meta.next_cursor for pagination (NOT page=)
    - Retries on 429 with exponential backoff + jitter
    - Adds a small sleep between requests to be polite
    """
    cursor: Optional[int] = None
    headers = {"Authorization": api_key} if api_key else {}

    while True:
        params: Dict[str, Any] = {"per_page": per_page}
        if cursor is not None:
            params["cursor"] = cursor

        if season is not None:
            params["seasons[]"] = season
        if start_date is not None:
            params["start_date"] = start_date.isoformat()
        if end_date is not None:
            params["end_date"] = end_date.isoformat()

        url = f"{base_url}/games"

        # Retry loop for 429
        attempt = 0
        while True:
            try:
                payload = get_json(url, params=params, headers=headers)
                break
            except requests.HTTPError as e:
                resp = getattr(e, "response", None)
                status = resp.status_code if resp is not None else None

                if status == 429:
                    # exponential backoff with jitter
                    attempt += 1
                    sleep_s = min(60.0, (2 ** min(attempt, 6)) + random.random())
                    print(f"[BDL] 429 rate-limited. Sleeping {sleep_s:.1f}s then retrying...")
                    time.sleep(sleep_s)
                    continue
                raise  # not a rate limit; re-raise

        rows = payload.get("data", [])
        meta = payload.get("meta", {}) or {}

        if not rows:
            break

        for r in rows:
            yield r

        next_cursor = meta.get("next_cursor")
        if next_cursor is None:
            break
        cursor = next_cursor

        # small polite sleep to reduce chances of 429
        time.sleep(min_sleep_s)
