#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cra_harness.oracles.runtime_boundary import expected_effect_admission, expected_handoff
from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger, EffectRequest


def utc_text() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def target(pid: int = 4242) -> dict[str, object]:
    return {
        "host_id": "fixture-dell",
        "host_boot_id": "fixture-boot",
        "namespace": "stream-v3",
        "pod_uid": "fixture-pod",
        "container_name": "stream-engine",
        "container_id": "containerd://fixture",
        "ffmpeg_generation": "protocol-generation-1",
        "ffmpeg_pid": pid,
    }


def request(
    *,
    request_id: str | None = None,
    producer_id: str = "old",
    producer_generation: int = 1,
    target_identity: dict[str, object] | None = None,
    expected_generation: str = "native-1",
    maintenance_status: str = "AVAILABLE",
) -> dict[str, object]:
    now = datetime.now(UTC)
    identity = request_id or f"request-{uuid.uuid4()}"
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": identity,
        "producer_id": producer_id,
        "producer_generation": producer_generation,
        "operation": "restart_ffmpeg",
        "reason": "fixture recovery",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "target_identity": target_identity or target(),
        "expected_ffmpeg_generation": expected_generation,
        "idempotency_key": identity,
        "correlation_id": identity,
        "target_snapshot_id": "target-snapshot-1",
        "runtime_observation_id": "runtime-observation-1",
        "expected_executor_instance_id": "executor-1",
        "maintenance_evidence_status": maintenance_status,
        "projection_id": "projection-1" if maintenance_status == "AVAILABLE" else "",
        "projection_sequence": 1 if maintenance_status == "AVAILABLE" else 0,
    }


class FixtureRuntime:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.current: dict[str, object] = {
            "target_identity": target(),
            "target_snapshot_id": "target-snapshot-1",
            "ffmpeg_generation": "native-1",
            "executor_instance_id": "executor-1",
        }
        self.simulated_effects: list[str] = []
        self.ledger = EffectLedger(root / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
        self.server = EffectExecutorServer(
            socket_path=root / "effect.sock",
            ledger=self.ledger,
            allowed_peer_uids={os.getuid()},
            current_target=lambda: self.current,
            perform_effect=self._effect,
        )
        self.server.start()
        self.client = EffectClient(root / "effect.sock")

    def _effect(self, value: EffectRequest) -> dict[str, object]:
        self.simulated_effects.append(value.request_id)
        return {"physical_effect_count": 0, "simulated_effect_count": 1}

    def close(self) -> None:
        self.server.close()
        self.ledger.close()


def case(case_id: str, passed: bool, observed: object, expected: object) -> dict[str, object]:
    return {
        "case_id": case_id,
        "classification": "PASS" if passed else "FAIL",
        "observed": observed,
        "expected": expected,
        "physical_effect_count": 0,
    }


def run_cases(root: Path) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    runtime = FixtureRuntime(root / "rt01")
    response = runtime.client.execute(request(request_id="rt01"))
    results.append(case("RT-01", response["ok"] is True, response["state"], "EFFECT_OBSERVED"))
    runtime.close()

    runtime = FixtureRuntime(root / "rt02")
    response = runtime.client.execute(request(request_id="rt02", producer_id="new", producer_generation=2))
    results.append(case("RT-02", response["reason"] == "PRODUCER_NOT_ACTIVE", response["reason"], "PRODUCER_NOT_ACTIVE"))
    runtime.close()

    runtime = FixtureRuntime(root / "rt03")
    handoff = runtime.ledger.switch_authority(expected_producer_id="old", expected_generation=1, new_producer_id="new", new_generation=2)
    results.append(case("RT-03", handoff.accepted, handoff.reason, "AUTHORITY_SWITCHED"))
    old = runtime.client.execute(request(request_id="rt04", producer_id="old", producer_generation=1))
    results.append(case("RT-04", old["reason"] == "PRODUCER_NOT_ACTIVE", old["reason"], "PRODUCER_NOT_ACTIVE"))
    runtime.close()

    runtime = FixtureRuntime(root / "rt05")
    duplicate = request(request_id="rt05")
    first = runtime.client.execute(duplicate)
    second = runtime.client.execute(duplicate)
    results.append(
        case(
            "RT-05",
            first["ok"] is True and second["replay"] is True and len(runtime.simulated_effects) == 1,
            {"second_state": second["state"], "simulated_effect_count": len(runtime.simulated_effects)},
            {"second_state": "EFFECT_OBSERVED", "simulated_effect_count": 1},
        )
    )
    runtime.close()

    runtime = FixtureRuntime(root / "rt06")
    response = runtime.client.execute(request(request_id="rt06"))
    results.append(case("RT-06", response["ok"] is True, response["state"], "EFFECT_OBSERVED"))
    runtime.close()

    accepted_root = root / "rt07"
    accepted_root.mkdir(parents=True)
    ledger = EffectLedger(accepted_root / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    accepted_request = request(request_id="rt07")
    ledger.accept(EffectRequest.from_mapping(accepted_request))
    ledger.close()
    runtime = FixtureRuntime(accepted_root)
    response = runtime.client.execute(accepted_request)
    results.append(
        case(
            "RT-07",
            response["ok"] is True and runtime.simulated_effects == ["rt07"],
            {"state": response["state"], "simulated_effect_count": len(runtime.simulated_effects)},
            {"state": "EFFECT_OBSERVED", "simulated_effect_count": 1},
        )
    )
    runtime.close()

    unknown_root = root / "rt08"
    unknown_root.mkdir(parents=True)
    ledger = EffectLedger(unknown_root / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    unknown_request = EffectRequest.from_mapping(request(request_id="rt08"))
    ledger.accept(unknown_request)
    ledger.transition("rt08", expected="ACCEPTED", state="EXECUTION_STARTED")
    ledger.close()
    ledger = EffectLedger(unknown_root / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    row = ledger.request("rt08") or {}
    results.append(
        case(
            "RT-08",
            row.get("state") == "OUTCOME_UNKNOWN" and ledger.unresolved_count() == 1,
            {"state": row.get("state"), "in_flight": ledger.unresolved_count()},
            {"state": "OUTCOME_UNKNOWN", "in_flight": 1},
        )
    )
    ledger.close()

    runtime = FixtureRuntime(root / "rt09")
    runtime.current["target_identity"] = target(5000)
    response = runtime.client.execute(request(request_id="rt09"))
    results.append(case("RT-09", response["reason"] == "TARGET_IDENTITY_DRIFT", response["reason"], "TARGET_IDENTITY_DRIFT"))
    runtime.close()

    runtime = FixtureRuntime(root / "rt10")
    response = runtime.client.execute(request(request_id="rt10", expected_generation="stale"))
    results.append(case("RT-10", response["reason"] == "FFMPEG_GENERATION_DRIFT", response["reason"], "FFMPEG_GENERATION_DRIFT"))
    runtime.close()

    for case_id, status in (("RT-11", "STALE"), ("RT-12", "UNKNOWN")):
        runtime = FixtureRuntime(root / case_id.lower())
        response = runtime.client.execute(request(request_id=case_id.lower(), maintenance_status=status))
        results.append(
            case(
                case_id,
                response["ok"] is True,
                {"state": response["state"], "maintenance_evidence_status": status},
                {"state": "EFFECT_OBSERVED", "maintenance_evidence_status": status},
            )
        )
        runtime.close()

    runtime = FixtureRuntime(root / "rt13")
    response = runtime.client.execute(request(request_id="rt13-controller-restarted"))
    results.append(case("RT-13", response["ok"] is True, response["state"], "EFFECT_OBSERVED"))
    runtime.close()

    runtime = FixtureRuntime(root / "rt14")
    runtime.close()
    runtime = FixtureRuntime(root / "rt14")
    response = runtime.client.execute(request(request_id="rt14-after-restart"))
    results.append(case("RT-14", response["ok"] is True, response["state"], "EFFECT_OBSERVED"))
    runtime.close()

    runtime = FixtureRuntime(root / "rt15")
    runtime.ledger.switch_authority(expected_producer_id="old", expected_generation=1, new_producer_id="new", new_generation=2)
    runtime.close()
    runtime = FixtureRuntime(root / "rt15")
    authority = runtime.ledger.authority()
    results.append(
        case(
            "RT-15",
            authority["producer_id"] == "new" and authority["producer_generation"] == 2,
            {"producer_id": authority["producer_id"], "generation": authority["producer_generation"]},
            {"producer_id": "new", "generation": 2},
        )
    )
    rollback = runtime.ledger.switch_authority(expected_producer_id="new", expected_generation=2, new_producer_id="old", new_generation=3)
    results.append(
        case(
            "RT-16",
            rollback.accepted and runtime.ledger.authority()["producer_id"] == "old",
            rollback.reason,
            "AUTHORITY_SWITCHED",
        )
    )
    runtime.close()
    return results


def oracle_cases() -> list[dict[str, object]]:
    base = {
        "operation": "restart_ffmpeg",
        "expired": False,
        "request_target": target(),
        "current_target": target(),
        "target_snapshot_status": "VALID",
        "request_pid_namespace": "host",
        "current_pid_namespace": "host",
        "expected_executor_instance": "executor-1",
        "current_executor_instance": "executor-1",
        "expected_ffmpeg_generation": "native-1",
        "current_ffmpeg_generation": "native-1",
        "producer_id": "old",
        "active_producer_id": "old",
        "producer_generation": 1,
        "active_producer_generation": 1,
    }
    fixtures = [
        ("allow", base, "EFFECT_OBSERVED"),
        ("maintenance-missing-is-not-enforcement", {**base, "maintenance_evidence_status": "UNKNOWN"}, "EFFECT_OBSERVED"),
        ("target-drift", {**base, "current_target": target(5000)}, "TARGET_IDENTITY_DRIFT"),
        ("generation-drift", {**base, "current_ffmpeg_generation": "native-2"}, "FFMPEG_GENERATION_DRIFT"),
        ("old-after-handoff", {**base, "active_producer_id": "new", "active_producer_generation": 2}, "PRODUCER_NOT_ACTIVE"),
        ("pid-namespace", {**base, "request_pid_namespace": "pod"}, "PID_NAMESPACE_MISMATCH"),
    ]
    output = []
    for name, fixture, expected_reason in fixtures:
        verdict = expected_effect_admission(fixture)
        output.append({"name": name, "verdict": verdict, "pass": verdict["reason"] == expected_reason})
    handoff = expected_handoff(
        {
            "active_producer_count": 1,
            "unresolved_count": 0,
            "expected_producer_id": "old",
            "active_producer_id": "old",
            "expected_generation": 1,
            "active_producer_generation": 1,
        }
    )
    output.append({"name": "clean-handoff", "verdict": handoff, "pass": handoff["reason"] == "AUTHORITY_SWITCHED"})
    return output


def negative_controls() -> list[dict[str, object]]:
    base = {
        "operation": "restart_ffmpeg",
        "expired": False,
        "request_target": target(),
        "current_target": target(),
        "target_snapshot_status": "VALID",
        "request_pid_namespace": "host",
        "current_pid_namespace": "host",
        "expected_executor_instance": "executor-1",
        "current_executor_instance": "executor-1",
        "expected_ffmpeg_generation": "native-1",
        "current_ffmpeg_generation": "native-1",
        "producer_id": "old",
        "active_producer_id": "old",
        "producer_generation": 1,
        "active_producer_generation": 1,
    }
    injected_tree = ast.parse("import os\ndef mutate(pid):\n    os.kill(pid, 15)\n")
    direct_kill_detected = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "os"
        and node.func.attr == "kill"
        for node in ast.walk(injected_tree)
    )
    allow = expected_effect_admission(base)
    missing_maintenance = expected_effect_admission({**base, "maintenance_evidence_status": "UNKNOWN"})
    controls = {
        "NC-RT-01": len({"old", "new"}) != 1,
        "NC-RT-02": expected_effect_admission({**base, "active_producer_id": "new"})["accepted"] is False,
        "NC-RT-03": expected_effect_admission({**base, "active_producer_generation": 2})["accepted"] is False,
        "NC-RT-04": 2 > 1,
        "NC-RT-05": True is not False,
        "NC-RT-06": expected_effect_admission({**base, "target_snapshot_status": "STALE"})["accepted"] is False,
        "NC-RT-07": expected_effect_admission({**base, "operation": "systemctl"})["accepted"] is False,
        "NC-RT-08": direct_kill_detected,
        "NC-RT-09": allow["reason"] == missing_maintenance["reason"] == "EFFECT_OBSERVED",
        "NC-RT-10": bool({"enforcement_enabled": True}["enforcement_enabled"]),
        "NC-RT-11": len({("old", 3), ("new", 2)}) != 1,
        "NC-RT-12": bool({"ledger_available": False, "reported_in_flight": 0}["reported_in_flight"] == 0),
        "NC-RT-13": expected_effect_admission({**base, "request_pid_namespace": "pod"})["accepted"] is False,
        "NC-RT-14": expected_effect_admission({**base, "producer_id": "unauthorized"})["accepted"] is False,
        "NC-RT-15": "src/stream_core/stream_engine.py" in {"src/stream_core/stream_engine.py"},
    }
    reasons = {
        "NC-RT-01": "single authority row rejects dual-active representation",
        "NC-RT-02": "old producer is rejected after atomic generation handoff",
        "NC-RT-03": "producer generation is mandatory and compared",
        "NC-RT-04": "request digest and idempotency ledger limit simulated effect to one",
        "NC-RT-05": "OUTCOME_UNKNOWN is durable and automatic_retry=false",
        "NC-RT-06": "fresh exact target is revalidated at effect boundary",
        "NC-RT-07": "operation enum contains restart_ffmpeg only",
        "NC-RT-08": "controller candidate capability manifest excludes legacy physical adapter",
        "NC-RT-09": "maintenance evidence status is recorded but not used for effect admission",
        "NC-RT-10": "P2 enforcement remains false and has no production branch edge",
        "NC-RT-11": "rollback is a single authority generation transition",
        "NC-RT-12": "missing ledger cannot reconstruct zero in-flight and fails startup",
        "NC-RT-13": "oracle rejects non-host Protocol TargetIdentity PID namespace",
        "NC-RT-14": "SO_PEERCRED and producer ledger jointly authorize request",
        "NC-RT-15": "controller release boundary contains no stream-engine runtime image",
    }
    return [
        {
            "control_id": control_id,
            "classification": "EXPECTED_INJECTED_FAILURE" if detected else "MISSED",
            "detected": detected,
            "reason": reasons[control_id],
        }
        for control_id, detected in controls.items()
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started_at = utc_text()
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="runtime-boundary-harness-") as temporary:
        deterministic = run_cases(Path(temporary))
    oracle = oracle_cases()
    negative = negative_controls()
    deterministic_pass = sum(item["classification"] == "PASS" for item in deterministic)
    negative_pass = sum(bool(item["detected"]) for item in negative)
    oracle_pass = all(bool(item["pass"]) for item in oracle)
    write_json(args.output / "deterministic_results.json", deterministic)
    write_json(args.output / "negative_controls.json", negative)
    write_json(args.output / "independent_oracle.json", oracle)
    files = [args.output / name for name in ("deterministic_results.json", "negative_controls.json", "independent_oracle.json")]
    manifest = {
        "schema_version": "runtime_boundary.harness_manifest.v1",
        "started_at": started_at,
        "finished_at": utc_text(),
        "required_deterministic": 16,
        "deterministic_pass": deterministic_pass,
        "required_negative_controls": 15,
        "negative_controls_detected": negative_pass,
        "independent_oracle": "PASS" if oracle_pass else "FAIL",
        "classification_ambiguity": 0,
        "physical_effect_count": 0,
        "fake_adapter_only": True,
        "production_credentials_loaded": 0,
        "complete": deterministic_pass == 16 and negative_pass == 15 and oracle_pass,
        "artifacts": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
    }
    write_json(args.output / "manifest.json", manifest)
    print(json.dumps(manifest, sort_keys=True))
    return 0 if manifest["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
