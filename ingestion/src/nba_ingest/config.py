import os
from pathlib import Path
from dotenv import load_dotenv

# Project root = nba-datalakehouse
ROOT = Path(__file__).resolve().parents[3]

# Always load .env from project root
load_dotenv(ROOT / ".env")

THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY")
BALLDONTLIE_API_KEY = os.getenv("BALLDONTLIE_API_KEY")

LANDING_BASE = Path(
    os.getenv("LANDING_BASE_DIR", ROOT / "data" / "landing")
)
