from dotenv import load_dotenv
import os
import requests

load_dotenv()
KEY = os.getenv("BDL_API_KEY")

if not KEY:
    raise RuntimeError("BDL_API_KEY not found. Check .env in this folder.")

headers = {"Authorization": KEY}

# Test endpoint: teams
url = "https://api.balldontlie.io/v1/teams"
params = {"per_page": 10}

r = requests.get(url, headers=headers, params=params, timeout=30)
print("STATUS:", r.status_code)
print("BODY:", r.text[:700])
