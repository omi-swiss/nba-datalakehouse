from datetime import datetime, timezone

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")