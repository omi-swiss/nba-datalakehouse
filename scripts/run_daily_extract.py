import sys
from pathlib import Path
from datetime import date

# Make src importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "ingestion" / "src"))

from nba_ingest.common.time import make_run_id
from nba_ingest.sources.the_odds_api.extract import run as run_odds
from nba_ingest.sources.balldontlie.extract import run as run_bdl


def main(dt: str | None = None):
    if dt is None:
        dt = date.today().isoformat()

    run_id = make_run_id()

    print("Running extraction for", dt)

    run_odds(dt, run_id)
    run_bdl(dt, run_id)

    print("Done.")


if __name__ == "__main__":
    main()
