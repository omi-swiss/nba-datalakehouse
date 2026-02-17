from pathlib import Path
from ...config import LANDING_BASE
from ...common.jsonl import write_jsonl
from ...common.meta import build_meta
from .client import fetch_teams, fetch_games


def _save(entity, resp, dt, run_id):
    records = []
    for obj in resp.json()["data"]:
        records.append({
            "meta": build_meta(
                "balldontlie",
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
        / "balldontlie"
        / entity
        / f"dt={dt}"
        / "part-00000.jsonl"
    )

    write_jsonl(path, records)


def run(dt, run_id):
    _save("teams", fetch_teams(), dt, run_id)
    _save("games", fetch_games(), dt, run_id)
