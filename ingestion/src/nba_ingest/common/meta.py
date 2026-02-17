from .time import now_utc_iso

def build_meta(source, entity, dt, run_id, url, status):
    return {
        "source": source,
        "entity": entity,
        "dt": dt,
        "ingested_at": now_utc_iso(),
        "run_id": run_id,
        "request_url": url,
        "status_code": status,
    }
