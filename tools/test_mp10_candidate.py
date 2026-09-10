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


def source_snapshot(now: datetime, target: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "available": True,
        "snapshot_id": "maintenance-snapshot-mp10-fixture",
        "producer_id": "arena-maintenance-shadow",
        "producer_instance_id": "mp10-test-producer-1",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
        "maintenance_state": "INACTIVE",
        "maintenance_id": "",
        "maintenance_generation": 2,
        "authority_epoch": 34,
        "authority_session_id": "mp10-test-session",
        "target_identity": target,
        "source_target_identity": target,
        "target_snapshot_id": "target-snapshot-mp10-fixture",
        "target_snapshot_status": "VALID",
        "target_snapshot_age_seconds": 0.1,
        "authorizations": [],
        "physical_effect_count": 0,
        "production_behavior_modified": False,
    }


def run_container(*, image: str, projection_dir: Path, state_dir: Path) -> subprocess.CompletedProcess[str]:
    script = """
import json
import sys
import time
sys.path.insert(0, '/app/src/stream_core')
import stream_engine
from maintenance_audit import global_emitter

engine = object.__new__(stream_engine.StreamEngine)
engine.run_id = 'mp10-run-fixture'
engine.restart_count = 3
time.sleep(0.2)
engine.audit_self_recovery(
    phase='EFFECT_BOUNDARY', operation='start_ffmpeg',
    resource='stream-engine/ffmpeg', correlation_id='mp10-native-operation', count=1,
)
emitter = global_emitter()
ok = emitter.wait_until_drained(2.0)
print(json.dumps({'drained': ok, 'health': emitter.health_snapshot()}, sort_keys=True))
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


def rows(path: Path) -> list[dict[str, Any]]:
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
    temporary = Path(tempfile.mkdtemp(prefix="mp10-candidate-"))
    try:
        projection_dir = temporary / "projection"
        projection_dir.mkdir()
        state_dir = temporary / "state"
        state_dir.mkdir()
        now = datetime.now(UTC)
        target = {
            "host_id": "dell-yuki",
            "host_boot_id": "boot-mp10-fixture",
            "namespace": "stream-v3",
            "pod_uid": "pod-mp10-fixture",
            "container_name": "stream-engine",
            "container_id": "containerd://stream-engine-mp10-fixture",
            "ffmpeg_generation": "ffmpeg-mp10-fixture",
            "ffmpeg_pid": 4100,
        }
        source = temporary / "source.json"
        source.write_text(json.dumps(source_snapshot(now, target)), encoding="utf-8")
        projection = ProjectionProjector(
            source_path=source,
            output_path=projection_dir / "maintenance-snapshot.json",
            sequence_path=temporary / "sequence.json",
            producer_id="dell-maintenance-snapshot-projector",
            producer_instance_id="projector-mp10-fixture",
            expected_source_producer_id="arena-maintenance-shadow",
            ttl_seconds=60,
        ).publish(now=now)
        completed = run_container(image=args.image, projection_dir=projection_dir, state_dir=state_dir)
        events = rows(state_dir / "p1-audit/mp10/maintenance-audit.jsonl")
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
        hashes = subprocess.run(
            ["docker", "run", "--rm", "--network", "none", "--entrypoint", "python3", args.image, "-c", hash_script],
            check=False,
            text=True,
            capture_output=True,
            timeout=30,
        )
        runtime_hashes = json.loads(hashes.stdout) if hashes.returncode == 0 else {}
        source_texts = {
            path: (args.candidate_manifest.parent / "overlay/app" / path).read_text(encoding="utf-8") for path in expected_hashes
        }
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
            "projection_bound": event.get("maintenance_projection_id") == projection["projection_id"],
            "native_generation_not_maintenance_generation": event.get("native_operation_generation") == "mp10-run-fixture:3",
            "source_hash_identity": runtime_hashes == expected_hashes,
            "effect_callback_fail_isolated": "_notify_effect_boundary" in source_texts["src/stream_core/engine/ffmpeg_lifecycle.py"],
            "stale_helper_callback_fail_isolated": "except BaseException" in source_texts["src/stream_core/engine/process_discovery.py"],
            "no_production_branch_signal_reference": "p2_production_branch_signal" not in source_texts["src/stream_core/stream_engine.py"],
        }
        payload = {
            "schema_version": "recovery_control.mp10_candidate_test.v1",
            "image": args.image,
            "release_id": manifest["release_id"],
            "checks": checks,
            "container_stdout": completed.stdout.splitlines(),
            "container_stderr": completed.stderr.splitlines(),
            "event": event,
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
