#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from maintenance_audit import AuditEmitter, AuditObservation, AuditPathRole, AuditPhase


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * quantile)))
    return ordered[index]


def summary(values: list[float]) -> dict[str, float]:
    return {
        "median_us": statistics.median(values),
        "p95_us": percentile(values, 0.95),
        "p99_us": percentile(values, 0.99),
        "max_us": max(values),
    }


def fixture_snapshot() -> dict[str, Any]:
    now = datetime.now(UTC)
    target = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-current",
        "namespace": "stream-v3",
        "pod_uid": "pod-current",
        "container_name": "stream-engine",
        "container_id": "containerd://stream-engine-current",
        "ffmpeg_generation": "ffmpeg-current",
        "ffmpeg_pid": 4100,
    }
    return {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "available": True,
        "snapshot_id": "snapshot-benchmark",
        "producer_id": "benchmark",
        "producer_instance_id": "benchmark-1",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "maintenance_state": "INACTIVE",
        "maintenance_id": "",
        "maintenance_generation": 8,
        "authority_epoch": 34,
        "authority_session_id": "session-benchmark",
        "target_identity": target,
        "source_target_identity": target,
        "target_snapshot_id": "target-benchmark",
        "target_snapshot_status": "VALID",
        "target_snapshot_age_seconds": 0.1,
        "authorizations": [],
    }


def run(emitter: AuditEmitter, observation: AuditObservation, iterations: int) -> tuple[list[float], dict[str, Any]]:
    values: list[float] = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        emitter.emit(observation)
        values.append((time.perf_counter_ns() - started) / 1000.0)
    emitter.wait_until_drained(5.0)
    return values, emitter.health_snapshot()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=5000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    observation = AuditObservation(
        path_id="MP-03",
        phase=AuditPhase.OBSERVATION,
        operation="observe_fast_recovery_loop",
        path_role=AuditPathRole.NORMAL_MUTATOR,
        process_service="fast-recovery-loop",
        correlation_id="benchmark",
        bind_source_target=True,
        p2_disabled_evaluation=True,
        actual_production_decision="LEGACY_LOOP_EVALUATION_CONTINUES",
    )
    disabled = AuditEmitter(enabled=False)
    baseline_values, baseline_health = run(disabled, observation, args.iterations)
    disabled.close()
    enabled = AuditEmitter(
        enabled=True,
        state_supplier=fixture_snapshot,
        event_writer=lambda _event: None,
        queue_capacity=max(1024, args.iterations),
        state_refresh_seconds=0.01,
    )
    time.sleep(0.03)
    enabled_values, enabled_health = run(enabled, observation, args.iterations)
    enabled.close()
    payload = {
        "schema_version": "recovery_control.r1_mp03_benchmark.v1",
        "iterations": args.iterations,
        "audit_disabled": summary(baseline_values),
        "audit_p2_disabled_enabled": summary(enabled_values),
        "overhead_p99_us": percentile(enabled_values, 0.99) - percentile(baseline_values, 0.99),
        "baseline_health": baseline_health,
        "enabled_health": enabled_health,
        "fast_recovery_cadence_seconds": 10,
        "exception_count": enabled_health["exception_count"],
        "event_loss": enabled_health["lost"],
        "production_behavior_modified": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["exception_count"] == 0 and payload["event_loss"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
