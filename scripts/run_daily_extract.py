from __future__ import annotations

"""
scripts/run_daily_extract.py

Local NBA extraction orchestrator (Bronze landing producer).

Goals:
- Production-style, beginner-friendly
- Batch ingestion (daily), raw JSON Lines (JSONL) landing partitioned by dt=YYYY-MM-DD
- Separate source/entity folders (balldontlie, the_odds_api, etc.)
- Minimal ingestion-time transformation: wrap raw API payload as {"meta":..., "data":...}

Modes:
- daily:
    Pull a small, recent window (lookback days) for games (safe during timezone delays).
    You can override with --start-date/--end-date.
- backfill:
    Pull historical seasons for the last ~25 years (or any season range) once.
    Use --season-start/--season-end to run in chunks and avoid API limits.

Usage examples:
  # daily (default lookback from config)
  python scripts/run_daily_extract.py --mode daily

  # daily with explicit date range
  python scripts/run_daily_extract.py --mode daily --start-date 2025-01-05 --end-date 2025-01-05

  # backfill only 2024 season (sanity test)
  python scripts/run_daily_extract.py --mode backfill --season-start 2024 --season-end 2024

  # backfill last ~25 years in chunks
  python scripts/run_daily_extract.py --mode backfill --season-start 2001 --season-end 2006
"""

# -----------------------------------------------------------------------------
# 0) Bootstrap src-layout imports (ingestion/src)
# -----------------------------------------------------------------------------
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "ingestion" / "src"
sys.path.insert(0, str(SRC_PATH))

# -----------------------------------------------------------------------------
# 1) Load .env reliably from PROJECT ROOT (avoids "wrong directory" problems)
# -----------------------------------------------------------------------------
from dotenv import load_dotenv

ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# -----------------------------------------------------------------------------
# 2) Standard imports
# -----------------------------------------------------------------------------
import argparse
from datetime import date, datetime, timedelta
from typing import Optional

# -----------------------------------------------------------------------------
# 3) Project imports
# -----------------------------------------------------------------------------
from nba_ingest.config import (
    BDL_BASE_URL,
    BDL_API_KEY,
    BDL_BACKFILL_START_SEASON,
    BDL_DAILY_LOOKBACK_DAYS,
    # Odds API (optional; will be skipped if key missing)
    THE_ODDS_BASE_URL,
    THE_ODDS_API_KEY,
)
from nba_ingest.common.io import write_jsonl_partition
from nba_ingest.sources.balldontlie.games import iter_games

# If you already have odds extract implemented, we'll import it safely.
# If not present yet, we won't crash.
try:
    from nba_ingest.sources.the_odds_api.extract import run as run_odds_extract  # expects (dt, run_id) or similar
except Exception:
    run_odds_extract = None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _parse_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _today_iso() -> str:
    return date.today().isoformat()


def _make_run_id() -> str:
    # Simple deterministic-enough run id for local runs (UTC timestamp).
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


# -----------------------------------------------------------------------------
# BallDontLie: Games (Daily)
# -----------------------------------------------------------------------------
def run_games_daily(*, start_override: Optional[str], end_override: Optional[str]) -> None:
    end = _parse_yyyy_mm_dd(end_override) if end_override else date.today()
    start = _parse_yyyy_mm_dd(start_override) if start_override else (end - timedelta(days=BDL_DAILY_LOOKBACK_DAYS))

    print("=== BALLDONTLIE GAMES DAILY ===")
    print(f"Date range: {start} → {end}\n")

    total_rows = 0
    total_files = 0

    d = start
    while d <= end:
        dt_str = d.isoformat()

        rows = list(
            iter_games(
                BDL_BASE_URL,
                BDL_API_KEY,
                start_date=d,
                end_date=d,
            )
        )

        files = write_jsonl_partition(
            source="balldontlie",
            entity="games",
            rows=rows,
            dt=dt_str,
            run_id=_make_run_id(),
        )

        print(f"[daily] dt={dt_str} rows={len(rows)} files_written={len(files)}")
        total_rows += len(rows)
        total_files += len(files)

        d += timedelta(days=1)

    print(f"\nDaily range complete. total_rows={total_rows} total_files_written={total_files}")


# -----------------------------------------------------------------------------
# BallDontLie: Games (Backfill)
# -----------------------------------------------------------------------------
def run_games_backfill(*, season_start: Optional[int], season_end: Optional[int]) -> None:
    """
    One-time historical backfill:
    - default: seasons [BDL_BACKFILL_START_SEASON, current_year]
    - override: --season-start / --season-end for chunked backfills
    """
    today = date.today()
    current_year = today.year
    dt_str = today.isoformat()

    start_season = season_start if season_start is not None else BDL_BACKFILL_START_SEASON
    end_season = season_end if season_end is not None else current_year

    if end_season < start_season:
        raise ValueError(f"season_end ({end_season}) must be >= season_start ({start_season})")

    print("=== BALLDONTLIE GAMES BACKFILL ===")
    print(f"Seasons: {start_season} → {end_season}")
    print(f"Landing dt partition: {dt_str}\n")

    total_rows = 0
    total_files = 0
    run_id = _make_run_id()

    for season in range(start_season, end_season + 1):
        rows = list(iter_games(BDL_BASE_URL, BDL_API_KEY, season=season))

        files = write_jsonl_partition(
            source="balldontlie",
            entity="games",
            rows=rows,
            dt=dt_str,
            run_id=run_id,
            extra_partitions={"season": season},
        )

        print(f"[backfill] season={season} rows={len(rows)} files_written={len(files)}")
        total_rows += len(rows)
        total_files += len(files)

    print(f"\nBackfill complete. total_rows={total_rows} total_files_written={total_files}")


# -----------------------------------------------------------------------------
# The Odds API (Optional)
# -----------------------------------------------------------------------------
def run_odds_if_configured(dt: str, run_id: str) -> None:
    """
    Runs odds extraction if:
      - THE_ODDS_API_KEY is present
      - the odds extractor module exists in your codebase
    Otherwise, prints a skip message.
    """
    if not THE_ODDS_API_KEY:
        print("[odds] Skipping: THE_ODDS_API_KEY not set in .env")
        return
    if run_odds_extract is None:
        print("[odds] Skipping: odds extractor module not available/importable")
        return

    print("=== THE ODDS API EXTRACT ===")
    print(f"Running odds extraction for dt={dt} run_id={run_id}")
    # Your extractor signature may differ; adjust in ONE place here if needed.
    run_odds_extract(dt, run_id)
    print("[odds] Done.")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="NBA local extraction orchestrator")

    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help="daily = pull recent window; backfill = pull seasons range",
    )

    # Daily overrides
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD (optional override for daily)")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD (optional override for daily)")

    # Backfill chunking
    parser.add_argument("--season-start", type=int, default=None, help="Season start year (e.g., 2001)")
    parser.add_argument("--season-end", type=int, default=None, help="Season end year (e.g., 2024)")

    # Optional: run odds in the same run (won't crash if not configured)
    parser.add_argument("--with-odds", action="store_true", help="Also run The Odds API extraction (if configured)")

    args = parser.parse_args()

    dt_str = _today_iso()
    run_id = _make_run_id()

    print(f"Running extraction for {dt_str} (run_id={run_id})")

    if args.mode == "backfill":
        run_games_backfill(season_start=args.season_start, season_end=args.season_end)
    else:
        run_games_daily(start_override=args.start_date, end_override=args.end_date)

    if args.with_odds:
        run_odds_if_configured(dt_str, run_id)


if __name__ == "__main__":
    main()
