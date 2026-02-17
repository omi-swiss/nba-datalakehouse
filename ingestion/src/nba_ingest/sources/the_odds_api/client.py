from ...config import THE_ODDS_API_KEY
from ...common.http import get_json

BASE = "https://api.the-odds-api.com/v4"

def fetch_sports():
    return get_json(f"{BASE}/sports", {"apiKey": THE_ODDS_API_KEY})

def fetch_events():
    return get_json(
        f"{BASE}/sports/basketball_nba/events",
        {"apiKey": THE_ODDS_API_KEY},
    )

def fetch_odds():
    return get_json(
        f"{BASE}/sports/basketball_nba/odds",
        {
            "apiKey": THE_ODDS_API_KEY,
            "regions": "au",
            "markets": "h2h",
            "oddsFormat": "decimal",
        },
    )
