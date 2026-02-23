from __future__ import annotations

from typing import Any, Dict, Optional
import requests


def get_json(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Perform an HTTP GET and return the parsed JSON payload (as a dict).

    Why this exists:
      - Keeps request logic in one place
      - Ensures consistent error handling across all sources
      - Returns Python dict so callers can do payload.get("data")

    Raises:
      - requests.HTTPError on non-2xx responses (via raise_for_status)
      - ValueError if response is not JSON
    """
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()
