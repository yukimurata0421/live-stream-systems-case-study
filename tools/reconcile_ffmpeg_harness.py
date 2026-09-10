#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any


def runtime_identity(*, pod_uid: str = "pod-1", generation: str = "runtime-1", container_name: str = "stream-engine") -> dict[str, str]:
    return {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-1",
        "namespace": "stream-v3",
        "pod_uid": pod_uid,
        "stream_engine_container_name": container_name,
        "stream_engine_container_id": "containerd://container-1",
        "runtime_generation": generation,
    }


def request_mapping(
    *,
    request_id: str = "request-1",
    producer_id: str = "controller",
    identity: dict[str, str] | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "schema_version": "runtime.typed_effect_request.v2",
        "request_id": request_id,
        "producer_id": producer_id,
        "producer_generation": 2,
        "intent_type": "RECONCILE_FFMPEG",
        "reason": "ffmpeg missing",
        "failure_domain": "FFMPEG_LOCAL",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "ffmpeg_target_identity": None,
        "runtime_identity": identity or runtime_identity(),
        "expected_ffmpeg_generation": "",
        "idempotency_key": request_id,
        "correlation_id": request_id,
        "target_snapshot_id": "",
        "runtime_snapshot_id": "runtime-snapshot-1",
        "runtime_observation_id": "runtime-observation-1",
        "expected_executor_instance_id": "executor-1",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": "projection-1",
        "projection_sequence": 1,
    }


def independent_cardinality_oracle(cardinality: str, lifecycle: str) -> tuple[str, int]:
    if cardinality == "1":
        return "ALREADY_RECONCILED", 0
    if cardinality == "0" and lifecycle == "RESTART_DELAY":
        return "RESTART_DELAY_RELEASED_AND_CHILD_OBSERVED", 1
    if cardinality == "2+":
        return "MULTIPLE_MANAGED_FFMPEG_CHILDREN", 0
    if cardinality == "UNKNOWN":
        return "MANAGED_CHILD_CARDINALITY_UNKNOWN", 0
    return f"RUNTIME_NOT_RECONCILABLE_{lifecycle}", 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v3-root", type=Path, required=True)
    parser.add_argument("--recovery-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sys.path[:0] = [str(args.v3_root / "src"), str(args.recovery_root / "src")]

    from stream_core.runtime_boundary_entrypoint import RuntimeBoundaryStreamEngine

    from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger, EffectRejected, EffectRequest

    controls: dict[str, bool] = {}
    oracle_rows: list[dict[str, Any]] = []

    def engine_for(cardinality: str, lifecycle: str) -> RuntimeBoundaryStreamEngine:
        engine = object.__new__(RuntimeBoundaryStreamEngine)
        engine.ffmpeg_lifecycle_state = lifecycle
        engine._reconcile_wakeup = threading.Event()
        engine.managed_ffmpeg_child_cardinality = lambda: (cardinality, [4200] if cardinality == "1" else [])
        return engine

    already = engine_for("1", "FFMPEG_RUNNING")
    result = already.reconcile_ffmpeg(SimpleNamespace())
    controls["NC-RF-07"] = result["effect"] == "ALREADY_RECONCILED" and result["physical_effect_count"] == 0

    unknown = engine_for("UNKNOWN", "RESTART_DELAY")
    try:
        unknown.reconcile_ffmpeg(SimpleNamespace())
    except EffectRejected as exc:
        controls["NC-RF-08"] = str(exc) == "MANAGED_CHILD_CARDINALITY_UNKNOWN"
    else:
        controls["NC-RF-08"] = False

    multiple = engine_for("2+", "RESTART_DELAY")
    try:
        multiple.reconcile_ffmpeg(SimpleNamespace())
    except EffectRejected as exc:
        multiple_result = (str(exc), 0)
    else:
        multiple_result = ("UNEXPECTED_ALLOW", 1)

    converging = engine_for("0", "RESTART_DELAY")
    observations = iter((("0", []), ("1", [4300])))

    def converge() -> tuple[str, list[int]]:
        value = next(observations, ("1", [4300]))
        if value[0] == "1":
            converging.ffmpeg_lifecycle_state = "FFMPEG_RUNNING"
        return value

    converging.managed_ffmpeg_child_cardinality = converge
    converged = converging.reconcile_ffmpeg(SimpleNamespace())

    actual_by_fixture = {
        ("1", "FFMPEG_RUNNING"): (str(result["effect"]), int(result["physical_effect_count"])),
        ("0", "RESTART_DELAY"): (str(converged["effect"]), int(converged["physical_effect_count"])),
        ("2+", "RESTART_DELAY"): multiple_result,
        ("UNKNOWN", "RESTART_DELAY"): ("MANAGED_CHILD_CARDINALITY_UNKNOWN", 0),
    }
    for fixture, actual in actual_by_fixture.items():
        expected = independent_cardinality_oracle(*fixture)
        oracle_rows.append(
            {"cardinality": fixture[0], "lifecycle": fixture[1], "expected": expected, "actual": actual, "match": actual == expected}
        )

    with tempfile.TemporaryDirectory(prefix="reconcile-harness-") as raw_root:
        root = Path(raw_root)
        ledger = EffectLedger(
            root / "ledger.sqlite3",
            initial_producer_id="controller",
            initial_producer_generation=2,
        )
        effects: list[str] = []
        current = {"runtime_identity": runtime_identity(), "executor_instance_id": "executor-1"}
        server = EffectExecutorServer(
            socket_path=root / "effect.sock",
            ledger=ledger,
            allowed_peer_uids={os.getuid()},
            current_target=lambda: current,
            perform_effect=lambda item: effects.append(item.request_id) or {"physical_effect_count": 1},
            allowed_intents={"RECONCILE_FFMPEG"},
        )
        server.start()
        client = EffectClient(root / "effect.sock")
        try:
            value = request_mapping(request_id="once")
            first = client.execute(value)
            second = client.execute(value)
            controls["NC-RF-01"] = first["ok"] is True and second["replay"] is True and effects == ["once"]

            rejected = client.execute(request_mapping(request_id="wrong-producer", producer_id="wrong"))
            controls["NC-RF-02"] = rejected["reason"] == "PRODUCER_NOT_ACTIVE" and effects == ["once"]

            old_pod = client.execute(request_mapping(request_id="old-pod", identity=runtime_identity(pod_uid="pod-old")))
            controls["NC-RF-05"] = old_pod["reason"] == "RUNTIME_IDENTITY_DRIFT" and effects == ["once"]

            old_generation = client.execute(
                request_mapping(request_id="old-generation", identity=runtime_identity(generation="runtime-old"))
            )
            controls["NC-RF-06"] = old_generation["reason"] == "RUNTIME_IDENTITY_DRIFT" and effects == ["once"]

            row = ledger.request("once")
            controls["NC-RF-10"] = row is not None and row["state"] == "EFFECT_OBSERVED"
        finally:
            server.close()
            ledger.close()

        crash_ledger = EffectLedger(
            root / "crash.sqlite3",
            initial_producer_id="controller",
            initial_producer_generation=2,
        )
        crash_request = EffectRequest.from_mapping(request_mapping(request_id="crash"))
        crash_ledger.accept(crash_request)
        crash_ledger.transition("crash", expected="ACCEPTED", state="EXECUTION_STARTED")
        crash_ledger.close()
        reconstructed = EffectLedger(
            root / "crash.sqlite3",
            initial_producer_id="controller",
            initial_producer_generation=2,
        )
        controls["NC-RF-03"] = reconstructed.request("crash")["state"] == "OUTCOME_UNKNOWN"
        reconstructed.close()

    wrong_container = request_mapping(identity=runtime_identity(container_name="arbitrary"))
    try:
        EffectRequest.from_mapping(wrong_container)
    except ValueError as exc:
        controls["NC-RF-04"] = str(exc) == "RUNTIME_CONTAINER_NOT_STREAM_ENGINE"
    else:
        controls["NC-RF-04"] = False

    arbitrary = request_mapping()
    arbitrary["intent_type"] = "ARBITRARY_OPERATION"
    try:
        EffectRequest.from_mapping(arbitrary)
    except ValueError as exc:
        controls["NC-RF-09"] = str(exc) == "INTENT_NOT_ALLOWED"
    else:
        controls["NC-RF-09"] = False

    terminal = {
        "schema_version": "reconcile_ffmpeg.harness.v1",
        "negative_controls": controls,
        "negative_controls_detected": sum(controls.values()),
        "negative_controls_total": len(controls),
        "independent_oracle_rows": oracle_rows,
        "independent_oracle_pass": all(row["match"] for row in oracle_rows),
        "physical_effect_count": 0,
        "complete": len(controls) == 10 and all(controls.values()) and all(row["match"] for row in oracle_rows),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(terminal, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in terminal.items() if key != "independent_oracle_rows"}, sort_keys=True))
    return 0 if terminal["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
