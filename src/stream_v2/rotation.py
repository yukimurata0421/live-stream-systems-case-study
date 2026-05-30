from __future__ import annotations

from pathlib import Path
from typing import Any

ROTATION_MANIFEST = {
    "schema_version": 1,
    "logs": [
        {
            "path": "logs/subsystems_status.jsonl",
            "rotate": "daily_or_size",
            "compress": True,
            "latest_snapshot": False,
            "min_retention_days": 30,
        },
        {
            "path": "logs/recovery_orchestrator.jsonl",
            "rotate": "daily_or_size",
            "compress": True,
            "latest_snapshot": False,
            "min_retention_days": 90,
        },
        {
            "path": "logs/recovery_action_plan.jsonl",
            "rotate": "daily_or_size",
            "compress": True,
            "latest_snapshot": False,
            "min_retention_days": 90,
        },
        {
            "path": "logs/objective_sli.jsonl",
            "rotate": "daily_or_size",
            "compress": True,
            "latest_snapshot": False,
            "min_retention_days": 90,
        },
        {
            "path": "logs/memory_status.jsonl",
            "rotate": "daily_or_size",
            "compress": True,
            "latest_snapshot": False,
            "min_retention_days": 90,
        },
        {
            "path": "logs/resource_memory.jsonl",
            "rotate": "daily_or_size",
            "compress": True,
            "latest_snapshot": False,
            "min_retention_days": 90,
        },
        {
            "path": "logs/stream_components.jsonl",
            "rotate": "daily_or_size",
            "compress": True,
            "latest_snapshot": False,
            "min_retention_days": 30,
        },
    ],
    "snapshots_not_rotated": [
        "subsystems_status.json",
        "recovery_action_plan.json",
        "objective_sli.json",
        "memory_status.json",
        "resource_memory.json",
        "resource_memory_assessment.json",
        "stream_components.json",
    ],
}


def manifest() -> dict[str, Any]:
    return ROTATION_MANIFEST


def write_manifest(path: Path) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(ROTATION_MANIFEST, f, ensure_ascii=False, indent=2)
        f.write("\n")
