import os
import sys
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _print_headers(resp: requests.Response, wanted: list[str]) -> None:
    for h in wanted:
        v = resp.headers.get(h)
        if v is not None:
            print(f"  {h}: {v}")


def _print_json_preview(resp: requests.Response, max_chars: int = 600) -> None:
    try:
        data = resp.json()
        s = json.dumps(data, ensure_ascii=False)[:max_chars]
        print(f"  body(json preview): {s}{'...' if len(s) >= max_chars else ''}")
    except Exception:
        txt = (resp.text or "")[:max_chars]
        print(f"  body(text preview): {txt}{'...' if len(txt) >= max_chars else ''}")


def _get(url: str, params: dict | None = None, headers: dict | None = None) -> requests.Response:
    # keep it simple for smoke tests; no retries yet
    return requests.get(url, params=params, headers=headers, timeout=30)


def smoke_the_odds_api(api_key: str) -> bool:
    print("\n=== The Odds API (api.the-odds-api.com) ===")
    base = "https://api.the-odds-api.com/v4"

    ok = True

    # 1) sports (auth required)
    url = f"{base}/sports"
    params = {"apiKey": api_key}
    r = _get(url, params=params)
    print(f"[{_now_iso()}] GET /v4/sports -> {r.status_code}")
    print(f"  url: {r.url}")
    _print_headers(r, ["x-requests-remaining", "x-requests-used", "x-requests-last"])
    _print_json_preview(r)
    ok &= (r.status_code == 200)

    # 2) NBA events
    url = f"{base}/sports/basketball_nba/events"
    params = {"apiKey": api_key}
    r = _get(url, params=params)
    print(f"[{_now_iso()}] GET /v4/sports/basketball_nba/events -> {r.status_code}")
    print(f"  url: {r.url}")
    _print_headers(r, ["x-requests-remaining", "x-requests-used", "x-requests-last"])
    _print_json_preview(r)
    ok &= (r.status_code == 200)

    # 3) NBA odds (featured markets)
    url = f"{base}/sports/basketball_nba/odds"
    params = {
        "apiKey": api_key,
        "regions": "au",          # change to "us" if you want
        "markets": "h2h",         # keep minimal for smoke test
        "oddsFormat": "decimal",
    }
    r = _get(url, params=params)
    print(f"[{_now_iso()}] GET /v4/sports/basketball_nba/odds -> {r.status_code}")
    print(f"  url: {r.url}")
    _print_headers(r, ["x-requests-remaining", "x-requests-used", "x-requests-last"])
    _print_json_preview(r)
    ok &= (r.status_code == 200)

    return ok


def smoke_balldontlie(api_key: str) -> bool:
    print("\n=== BallDontLie (NBA) ===")
    # BallDontLie has multiple domains/versions. This targets their NBA API domain.
    # If your existing smoke test used a different host, swap this base to match yours.
    base = "https://api.balldontlie.io/v1"
    headers = {"Authorization": api_key}

    ok = True

    # 1) teams
    url = f"{base}/teams"
    r = _get(url, headers=headers)
    print(f"[{_now_iso()}] GET /v1/teams -> {r.status_code}")
    print(f"  url: {r.url}")
    _print_headers(r, ["x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"])
    _print_json_preview(r)
    ok &= (r.status_code == 200)

    # 2) games (limit 1)
    url = f"{base}/games"
    r = _get(url, headers=headers, params={"per_page": 1})
    print(f"[{_now_iso()}] GET /v1/games?per_page=1 -> {r.status_code}")
    print(f"  url: {r.url}")
    _print_headers(r, ["x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"])
    _print_json_preview(r)
    ok &= (r.status_code == 200)

    return ok


def main() -> None:
    load_dotenv()

    the_odds_key = (os.getenv("THE_ODDS_API_KEY") or "").strip()
    bdl_key = (os.getenv("BALLDONTLIE_API_KEY") or "").strip()

    if not the_odds_key:
        print("Missing THE_ODDS_API_KEY in .env", file=sys.stderr)
        sys.exit(1)
    if not bdl_key:
        print("Missing BALLDONTLIE_API_KEY in .env", file=sys.stderr)
        sys.exit(1)

    ok_odds = smoke_the_odds_api(the_odds_key)
    ok_bdl = smoke_balldontlie(bdl_key)

    print("\n=== SUMMARY ===")
    print(f"The Odds API: {'PASS' if ok_odds else 'FAIL'}")
    print(f"BallDontLie: {'PASS' if ok_bdl else 'FAIL'}")

    if not (ok_odds and ok_bdl):
        sys.exit(2)


if __name__ == "__main__":
    main()
