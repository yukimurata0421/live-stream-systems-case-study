#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger

TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "benchmark-boot",
    "namespace": "stream-v3",
    "pod_uid": "benchmark-pod",
    "container_name": "stream-engine",
    "container_id": "containerd://benchmark",
    "ffmpeg_generation": "protocol-generation",
    "ffmpeg_pid": 4242,
}


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * quantile)))
    return ordered[index]


def run_sample(count: int, *, audit_enabled: bool) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="runtime-boundary-bench-") as raw_root:
        root = Path(raw_root)
        ledger = EffectLedger(root / "ledger.sqlite3", initial_producer_id="benchmark", initial_producer_generation=1)
        server = EffectExecutorServer(
            socket_path=root / "effect.sock",
            ledger=ledger,
            allowed_peer_uids={os.getuid()},
            current_target=lambda: {
                "target_identity": TARGET,
                "ffmpeg_generation": "native-generation",
                "executor_instance_id": "benchmark-executor",
            },
            perform_effect=lambda _request: {"physical_effect_count": 0, "adapter": "benchmark_fake"},
            before_effect=(lambda _request: None) if audit_enabled else None,
        )
        server.start()
        client = EffectClient(root / "effect.sock", timeout_seconds=2.0)
        durations: list[float] = []
        try:
            for sequence in range(1, count + 1):
                now = datetime.now(UTC)
                request_id = f"benchmark-{uuid.uuid4()}"
                request = {
                    "schema_version": "runtime.effect_request.v1",
                    "request_id": request_id,
                    "producer_id": "benchmark",
                    "producer_generation": 1,
                    "operation": "restart_ffmpeg",
                    "reason": "benchmark fake adapter",
                    "issued_at": now.isoformat(),
                    "expires_at": (now + timedelta(seconds=3)).isoformat(),
                    "target_identity": TARGET,
                    "expected_ffmpeg_generation": "native-generation",
                    "idempotency_key": request_id,
                    "correlation_id": request_id,
                    "target_snapshot_id": "benchmark-target",
                    "runtime_observation_id": "benchmark-observation",
                    "expected_executor_instance_id": "benchmark-executor",
                    "maintenance_evidence_status": "AVAILABLE",
                    "projection_id": "benchmark-projection",
                    "projection_sequence": sequence,
                }
                started = time.perf_counter_ns()
                response = client.execute(request)
                durations.append((time.perf_counter_ns() - started) / 1_000_000)
                if not response.get("ok"):
                    raise RuntimeError(str(response))
        finally:
            server.close()
            ledger.close()
    return {
        "count": count,
        "median_ms": statistics.median(durations),
        "p95_ms": percentile(durations, 0.95),
        "p99_ms": percentile(durations, 0.99),
        "max_ms": max(durations),
        "exception_count": 0,
        "real_physical_effect_count": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()
    baseline = run_sample(args.count, audit_enabled=False)
    enabled = run_sample(args.count, audit_enabled=True)
    result = {
        "schema_version": "runtime_boundary.performance.v1",
        "baseline_audit_disabled": baseline,
        "audit_enabled": enabled,
        "median_overhead_ms": enabled["median_ms"] - baseline["median_ms"],
        "p99_overhead_ms": enabled["p99_ms"] - baseline["p99_ms"],
        "controller_loop_budget_ms": 10_000,
        "p99_within_loop_budget": enabled["p99_ms"] < 10_000,
        "fake_adapter_only": True,
        "complete": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
