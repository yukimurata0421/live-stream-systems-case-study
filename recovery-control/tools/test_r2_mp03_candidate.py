#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jsonschema

from snapshot_projection import ProjectionProjector


def run_container(*, image: str, projection_dir: Path, state_dir: Path) -> subprocess.CompletedProcess[str]:
    script = """
import json
import time
from maintenance_audit import global_emitter
from watchers.fast_recovery import audit_mp03

time.sleep(0.2)
audit_mp03(
    phase='OBSERVATION', operation='observe_fast_recovery_loop',
    resource_identity='ffmpeg/pid/4100', correlation_id='candidate-native-operation',
    in_flight_evidence={'status':'PROPOSED','count':0,'source':'candidate no-effect fixture'},
    generation_evidence={'status':'PROPOSED','native_token':'candidate-native-operation'},
    actual_production_decision='LEGACY_LOOP_EVALUATION_CONTINUES',
)
emitter = global_emitter()
ok = emitter.wait_until_drained(2.0)
print(json.dumps({'drained':ok,'health':emitter.health_snapshot()},sort_keys=True))
"""
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--mount",
            f"type=bind,src={projection_dir},dst=/projection,readonly",
            "--mount",
            f"type=bind,src={state_dir},dst=/state",
            "--entrypoint",
            "python3",
            image,
            "-c",
            script,
        ],
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--mount",
            f"type=bind,src={state_dir},dst=/state",
            "--entrypoint",
            "chmod",
            image,
            "-R",
            "a+rwX",
            "/state",
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=30,
    )
    return result


def source_snapshot(now: datetime, target: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "available": True,
        "snapshot_id": "maintenance-snapshot-r2-candidate-fixture",
        "producer_id": "arena-maintenance-shadow",
        "producer_instance_id": "candidate-test-producer-1",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
        "maintenance_state": "INACTIVE",
        "maintenance_id": "",
        "maintenance_generation": 2,
        "authority_epoch": 34,
        "authority_session_id": "candidate-test-session",
        "target_identity": target,
        "source_target_identity": target,
        "target_snapshot_id": "target-snapshot-r2-candidate-fixture",
        "target_snapshot_status": "VALID",
        "target_snapshot_age_seconds": 0.1,
        "authorizations": [],
        "physical_effect_count": 0,
        "production_behavior_modified": False,
    }


def event_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--event-schema", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.candidate_manifest.read_text(encoding="utf-8"))
    temporary = Path(tempfile.mkdtemp(prefix="r2-mp03-candidate-"))
    try:
        projection_dir = temporary / "projection"
        projection_dir.mkdir()
        valid_state = temporary / "valid-state"
        valid_state.mkdir()
        missing_state = temporary / "missing-state"
        missing_state.mkdir()
        now = datetime.now(UTC)
        target = {
            "host_id": "dell-yuki",
            "host_boot_id": "boot-candidate-fixture",
            "namespace": "stream-v3",
            "pod_uid": "pod-candidate-fixture",
            "container_name": "stream-engine",
            "container_id": "containerd://stream-engine-candidate-fixture",
            "ffmpeg_generation": "ffmpeg-candidate-fixture",
            "ffmpeg_pid": 4100,
        }
        source = temporary / "source.json"
        source.write_text(json.dumps(source_snapshot(now, target)), encoding="utf-8")
        projection = ProjectionProjector(
            source_path=source,
            output_path=projection_dir / "maintenance-snapshot.json",
            sequence_path=temporary / "sequence.json",
            producer_id="dell-maintenance-snapshot-projector",
            producer_instance_id="projector-candidate-fixture",
            expected_source_producer_id="arena-maintenance-shadow",
            ttl_seconds=60,
        ).publish(now=now)
        valid = run_container(image=args.image, projection_dir=projection_dir, state_dir=valid_state)
        valid_events = event_rows(valid_state / "p1-audit/mp03/maintenance-audit.jsonl")
        valid_event = valid_events[-1] if valid_events else {}

        (projection_dir / "maintenance-snapshot.json").unlink()
        missing = run_container(image=args.image, projection_dir=projection_dir, state_dir=missing_state)
        missing_events = event_rows(missing_state / "p1-audit/mp03/maintenance-audit.jsonl")
        missing_event = missing_events[-1] if missing_events else {}

        schema = json.loads(args.event_schema.read_text(encoding="utf-8"))
        schema_error = ""
        try:
            jsonschema.Draft202012Validator(schema).validate(valid_event)
        except jsonschema.ValidationError as exc:
            schema_error = exc.message

        expected_hashes = {path: value["sha256"] for path, value in manifest["source_files"].items()}
        hash_script = (
            "import hashlib,json,pathlib; paths="
            + repr([f"/app/{path}" for path in expected_hashes])
            + "; values={p.removeprefix('/app/'):hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in paths}; "
            + "print(json.dumps(values,sort_keys=True))"
        )
        hashes_completed = subprocess.run(
            ["docker", "run", "--rm", "--network", "none", "--entrypoint", "python3", args.image, "-c", hash_script],
            check=False,
            text=True,
            capture_output=True,
            timeout=30,
        )
        runtime_hashes = json.loads(hashes_completed.stdout) if hashes_completed.returncode == 0 else {}
        checks = {
            "valid_container_exit_zero": valid.returncode == 0,
            "missing_container_exit_zero": missing.returncode == 0,
            "startup_log_enforcement_false": '"p2_enforcement_enabled":false' in valid.stdout,
            "valid_event_count_one": len(valid_events) == 1,
            "missing_event_count_one": len(missing_events) == 1,
            "schema_valid": not schema_error,
            "audit_would_allow": valid_event.get("audit_verdict") == "WOULD_ALLOW",
            "p2_disabled_allow": valid_event.get("p2_disabled_verdict") == "ALLOW",
            "projection_bound": valid_event.get("maintenance_projection_id") == projection["projection_id"],
            "projection_sequence_bound": valid_event.get("maintenance_projection_sequence") == projection["projection_sequence"],
            "target_exact": valid_event.get("target_identity") == target,
            "missing_projection_unknown": missing_event.get("audit_verdict") == "UNKNOWN",
            "missing_projection_does_not_block": missing_event.get("production_behavior_modified") is False,
            "p2_enforcement_false": valid_event.get("p2_enforcement_enabled") is False,
            "production_branch_signal_none": valid_event.get("p2_production_branch_signal") is None,
            "source_hash_identity": runtime_hashes == expected_hashes,
        }
        payload = {
            "schema_version": "recovery_control.r2_mp03_candidate_test.v1",
            "image": args.image,
            "release_id": manifest["release_id"],
            "checks": checks,
            "valid_container_stdout": valid.stdout.splitlines(),
            "valid_container_stderr": valid.stderr.splitlines(),
            "missing_container_stdout": missing.stdout.splitlines(),
            "missing_container_stderr": missing.stderr.splitlines(),
            "valid_event": valid_event,
            "missing_projection_event": missing_event,
            "schema_error": schema_error,
            "runtime_hashes": runtime_hashes,
            "expected_hashes": expected_hashes,
            "pass": all(checks.values()),
            "physical_effect_count": 0,
            "production_behavior_modified": False,
        }
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "candidate_test.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(payload, sort_keys=True))
        return 0 if payload["pass"] else 1
    finally:
        shutil.rmtree(temporary)


if __name__ == "__main__":
    raise SystemExit(main())
