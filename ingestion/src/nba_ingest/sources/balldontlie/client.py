from ...config import BALLDONTLIE_API_KEY
from ...common.http import get_json

BASE = "https://api.balldontlie.io/v1"
HEADERS = {"Authorization": BALLDONTLIE_API_KEY}

def fetch_teams():
    return get_json(f"{BASE}/teams", headers=HEADERS)

def fetch_games():
    return get_json(f"{BASE}/games", headers=HEADERS, params={"per_page": 25})
