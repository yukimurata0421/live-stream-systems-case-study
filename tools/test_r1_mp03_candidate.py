#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jsonschema


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--event-schema", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.candidate_manifest.read_text(encoding="utf-8"))
    temporary = Path(tempfile.mkdtemp(prefix="r1-mp03-candidate-"))
    try:
        state_dir = temporary / "p1-audit/mp03"
        state_dir.mkdir(parents=True)
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
        snapshot = {
            "schema_version": "maintenance.audit_state_snapshot.v2",
            "available": True,
            "snapshot_id": "maintenance-snapshot-candidate-fixture",
            "producer_id": "candidate-test-independent-fixture",
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
            "target_snapshot_id": "target-snapshot-candidate-fixture",
            "target_snapshot_status": "VALID",
            "target_snapshot_age_seconds": 0.1,
            "authorizations": [],
        }
        (state_dir / "maintenance-audit-state.json").write_text(
            json.dumps(snapshot, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        script = """
import json
import time
from pathlib import Path
from maintenance_audit import global_emitter
from watchers.fast_recovery import audit_mp03

time.sleep(0.1)
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
        completed = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--mount",
                f"type=bind,src={temporary},dst=/state",
                "--entrypoint",
                "python3",
                args.image,
                "-c",
                script,
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=30,
        )
        event_path = state_dir / "maintenance-audit.jsonl"
        health_path = state_dir / "maintenance-audit-health.json"
        events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()] if event_path.exists() else []
        event = events[-1] if events else {}
        schema = json.loads(args.event_schema.read_text(encoding="utf-8"))
        schema_error = ""
        try:
            jsonschema.Draft202012Validator(schema).validate(event)
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
            "container_exit_zero": completed.returncode == 0,
            "startup_log_enforcement_false": '"p2_enforcement_enabled":false' in completed.stdout,
            "event_count_one": len(events) == 1,
            "schema_valid": not schema_error,
            "audit_would_allow": event.get("audit_verdict") == "WOULD_ALLOW",
            "p2_disabled_allow": event.get("p2_disabled_verdict") == "ALLOW",
            "p2_enforcement_false": event.get("p2_enforcement_enabled") is False,
            "production_branch_signal_none": event.get("p2_production_branch_signal") is None,
            "production_behavior_unchanged": event.get("production_behavior_modified") is False,
            "target_exact": event.get("target_identity") == target,
            "native_identity_bound": event.get("native_operation_id") == "candidate-native-operation",
            "health_written": health_path.exists(),
            "source_hash_identity": runtime_hashes == expected_hashes,
        }
        payload: dict[str, Any] = {
            "schema_version": "recovery_control.r1_mp03_candidate_test.v1",
            "image": args.image,
            "release_id": manifest["release_id"],
            "container_exit_code": completed.returncode,
            "container_stdout": completed.stdout.splitlines(),
            "container_stderr": completed.stderr.splitlines(),
            "schema_error": schema_error,
            "event": event,
            "runtime_hashes": runtime_hashes,
            "expected_hashes": expected_hashes,
            "checks": checks,
            "pass": all(checks.values()),
            "physical_effect_count": 0,
        }
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "candidate_test.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        shutil.copy2(event_path, args.output_dir / "candidate_audit.jsonl")
        if health_path.exists():
            shutil.copy2(health_path, args.output_dir / "candidate_health.json")
        print(json.dumps(payload, sort_keys=True))
        return 0 if payload["pass"] else 1
    finally:
        shutil.rmtree(temporary)


if __name__ == "__main__":
    raise SystemExit(main())
