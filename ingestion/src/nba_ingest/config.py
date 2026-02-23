import os
from dotenv import load_dotenv
from pathlib import Path

# Load project .env reliably (important!)
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # points to nba-datalakehouse/
load_dotenv(PROJECT_ROOT / ".env")

BDL_API_KEY = os.getenv("BDL_API_KEY")
BDL_BASE_URL = os.getenv("BDL_BASE_URL", "https://api.balldontlie.io/v1")

THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY")
THE_ODDS_BASE_URL = os.getenv("THE_ODDS_BASE_URL", "https://api.the-odds-api.com/v4")

BDL_PER_PAGE = 100

# Backfill: last ~5 seasons
BDL_BACKFILL_START_SEASON = 2021

# Daily incremental safety window
BDL_DAILY_LOOKBACK_DAYS = 2
