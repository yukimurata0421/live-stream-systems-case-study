#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from snapshot_projection import ProjectionProjector, ProjectionReader


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * quantile)))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=5000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    target = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-benchmark",
        "namespace": "stream-v3",
        "pod_uid": "pod-benchmark",
        "container_name": "stream-engine",
        "container_id": "containerd://engine-benchmark",
        "ffmpeg_generation": "ffmpeg-benchmark",
        "ffmpeg_pid": 4100,
    }
    now = datetime.now(UTC)
    source_payload = {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "available": True,
        "producer_id": "arena-maintenance-shadow",
        "producer_instance_id": "benchmark-source",
        "snapshot_id": "snapshot-benchmark",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "maintenance_state": "INACTIVE",
        "maintenance_id": "",
        "maintenance_generation": 2,
        "authority_epoch": 34,
        "authority_session_id": "session-benchmark",
        "target_identity": target,
        "source_target_identity": target,
        "target_snapshot_id": "target-benchmark",
        "target_snapshot_status": "VALID",
        "target_snapshot_age_seconds": 0.1,
        "authorizations": [],
        "physical_effect_count": 0,
        "production_behavior_modified": False,
    }
    values: list[float] = []
    with tempfile.TemporaryDirectory(prefix="projection-benchmark-") as td:
        root = Path(td)
        source = root / "source.json"
        output = root / "projection.json"
        source.write_text(json.dumps(source_payload), encoding="utf-8")
        ProjectionProjector(
            source_path=source,
            output_path=output,
            sequence_path=root / "sequence.json",
            producer_id="dell-maintenance-snapshot-projector",
            producer_instance_id="benchmark-projector",
            expected_source_producer_id="arena-maintenance-shadow",
            ttl_seconds=300,
        ).publish(now=now)
        reader = ProjectionReader(
            path=output,
            expected_producer_id="dell-maintenance-snapshot-projector",
            expected_source_producer_id="arena-maintenance-shadow",
        )
        for _ in range(args.iterations):
            started = time.perf_counter_ns()
            decision = reader.read(now=now + timedelta(seconds=1))
            values.append((time.perf_counter_ns() - started) / 1000.0)
            if not decision.available:
                raise RuntimeError(decision.reason_code)
    payload = {
        "schema_version": "recovery_control.snapshot_projection_benchmark.v1",
        "iterations": args.iterations,
        "median_us": statistics.median(values),
        "p95_us": percentile(values, 0.95),
        "p99_us": percentile(values, 0.99),
        "max_us": max(values),
        "remote_lookup_count": 0,
        "production_behavior_modified": False,
        "physical_effect_count": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
