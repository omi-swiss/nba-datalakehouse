from pathlib import Path
from ...config import LANDING_BASE
from ...common.jsonl import write_jsonl
from ...common.meta import build_meta
from .client import fetch_sports, fetch_events, fetch_odds


def _save(entity, resp, dt, run_id):
    records = []
    for obj in resp.json():
        records.append({
            "meta": build_meta(
                "the_odds_api",
                entity,
                dt,
                run_id,
                resp.url,
                resp.status_code,
            ),
            "data": obj,
        })

    path = (
        LANDING_BASE
        / "the_odds_api"
        / entity
        / f"dt={dt}"
        / "part-00000.jsonl"
    )

    write_jsonl(path, records)


def run(dt, run_id):
    _save("sports", fetch_sports(), dt, run_id)
    _save("events", fetch_events(), dt, run_id)
    _save("odds", fetch_odds(), dt, run_id)
