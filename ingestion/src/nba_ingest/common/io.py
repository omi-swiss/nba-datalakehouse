from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _landing_base() -> Path:
    return _project_root() / "data" / "landing"


def _partition_dir(
    *,
    source: str,
    entity: str,
    dt: str,
    extra_partitions: Optional[Dict[str, Any]] = None,
) -> Path:
    p = _landing_base() / source / entity / f"dt={dt}"
    if extra_partitions:
        for k, v in extra_partitions.items():
            p = p / f"{k}={v}"
    return p


def _next_part_path(partition_dir: Path) -> Path:
    partition_dir.mkdir(parents=True, exist_ok=True)
    i = 0
    while True:
        candidate = partition_dir / f"part-{i:05d}.jsonl"
        if not candidate.exists():
            return candidate
        i += 1
        if i > 99999:
            raise RuntimeError(f"Too many part files in {partition_dir}")


def write_jsonl_partition(
    *,
    source: str,
    entity: str,
    rows: Iterable[dict],
    dt: str,
    run_id: str,
    extra_partitions: Optional[Dict[str, Any]] = None,
) -> List[Path]:
    partition_dir = _partition_dir(source=source, entity=entity, dt=dt, extra_partitions=extra_partitions)
    out_path = _next_part_path(partition_dir)

    n = 0
    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")
            n += 1

    if n == 0:
        out_path.unlink(missing_ok=True)
        return []

    return [out_path]
