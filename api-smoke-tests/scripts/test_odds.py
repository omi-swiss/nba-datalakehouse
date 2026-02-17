from dotenv import load_dotenv
import os
import requests

load_dotenv()
KEY = os.getenv("ODDS_API_KEY")

if not KEY:
    raise RuntimeError("ODDS_API_KEY not found. Check .env in this folder.")

url = "https://api.odds-api.io/v3/events"
params = {
    "apiKey": KEY,
    "sport": "basketball",
    "league": "usa-nba",
    "limit": 5,
    "skip": 0,
}

r = requests.get(url, params=params, timeout=30)
print("STATUS:", r.status_code)
print("BODY:", r.text[:500])
