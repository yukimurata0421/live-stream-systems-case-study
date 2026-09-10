#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from snapshot_projection import ProjectionReader


def service_properties(unit: str) -> dict[str, str]:
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "NRestarts",
            "-p",
            "MainPID",
            "-p",
            "MemoryCurrent",
            "-p",
            "TasksCurrent",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode != 0:
        return {"read_status": f"ERROR_{completed.returncode}"}
    return {key: value for line in completed.stdout.splitlines() if "=" in line for key, value in [line.split("=", 1)]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--projection", type=Path, required=True)
    parser.add_argument("--source-projection", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    envelope = json.loads(args.projection.read_text(encoding="utf-8"))
    decision = ProjectionReader(
        path=args.projection,
        expected_producer_id="dell-maintenance-snapshot-projector",
        expected_source_producer_id="arena-maintenance-shadow",
    ).read()
    source_path = args.source_projection or args.projection
    source_stat = subprocess.run(
        ["sudo", "-n", "stat", "-c", "%a %U %G", str(source_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    source_mode, source_owner, source_group = (
        source_stat.stdout.strip().split(maxsplit=2) if source_stat.returncode == 0 else ("UNKNOWN", "UNKNOWN", "UNKNOWN")
    )
    now = datetime.now(UTC)
    payload = {
        "schema_version": "recovery_control.snapshot_projection_live.v1",
        "timestamp_utc": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "timestamp_jst": now.astimezone(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds"),
        "projection": {
            "projection_id": envelope.get("projection_id"),
            "projection_sequence": envelope.get("projection_sequence"),
            "producer_id": envelope.get("producer_id"),
            "producer_instance_id": envelope.get("producer_instance_id"),
            "source_snapshot_id": envelope.get("source_snapshot_id"),
            "source_producer_id": envelope.get("source_producer_id"),
            "maintenance_generation": envelope.get("maintenance_generation"),
            "target_snapshot_status": envelope.get("target_snapshot_status"),
            "target_identity": (envelope.get("payload") or {}).get("target_identity"),
            "observed_at": envelope.get("source_observed_at"),
            "fresh_until": envelope.get("fresh_until"),
            "payload_digest": envelope.get("payload_sha256"),
            "mode": source_mode,
            "owner": source_owner,
            "group": source_group,
        },
        "reader": {
            "available": decision.available,
            "reason_code": decision.reason_code,
            "projection_id": decision.projection_id,
            "projection_sequence": decision.projection_sequence,
        },
        "projector_service": service_properties("maintenance-snapshot-projection-shadow.service"),
        "dell_agent_service": service_properties("dell-recovery-agent-shadow.service"),
        "production_behavior_modified": False,
        "physical_effect_count": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
