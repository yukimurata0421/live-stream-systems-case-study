#!/usr/bin/env python3
"""Capture live RuntimeIdentity shadow evidence without mutation capability."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def command(arguments: list[str]) -> str:
    completed = subprocess.run(arguments, check=False, capture_output=True, text=True, timeout=20)
    if completed.returncode != 0:
        raise RuntimeError(f"read-only command failed: {arguments[0]} rc={completed.returncode}")
    return completed.stdout


def json_command(arguments: list[str]) -> dict[str, Any]:
    value = json.loads(command(arguments))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object from {arguments[0]}")
    return value


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def runtime_pod() -> dict[str, Any]:
    pods = json_command(
        [
            "kubectl",
            "-n",
            "stream-v3",
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
            "-o",
            "json",
        ]
    )
    running = [item for item in pods.get("items", []) if item.get("status", {}).get("phase") == "Running"]
    if len(running) != 1:
        raise RuntimeError(f"expected one running runtime Pod, got {len(running)}")
    pod = running[0]
    return {
        "uid": pod["metadata"]["uid"],
        "containers": {
            value["name"]: {
                "container_id": value.get("containerID", ""),
                "ready": value.get("ready", False),
                "restart_count": value.get("restartCount", 0),
            }
            for value in pod.get("status", {}).get("containerStatuses", [])
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    args = parser.parse_args()
    if args.samples < 2 or args.interval_seconds < 0:
        raise SystemExit("at least two samples and a non-negative interval are required")

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    samples: list[dict[str, Any]] = []
    target_fields = {
        "host_id",
        "host_boot_id",
        "namespace",
        "pod_uid",
        "container_name",
        "container_id",
        "ffmpeg_generation",
        "ffmpeg_pid",
    }
    runtime_fields = {
        "host_id",
        "host_boot_id",
        "namespace",
        "pod_uid",
        "stream_engine_container_name",
        "stream_engine_container_id",
        "runtime_generation",
    }
    for index in range(args.samples):
        snapshot = json_command(["sudo", "-n", "cat", "/var/lib/stream-recovery-control/dell/target_snapshot.json"])
        now = datetime.now(UTC)
        target = snapshot.get("target_identity")
        runtime = snapshot.get("runtime_identity")
        valid_until = parse_utc(str(snapshot.get("valid_until") or ""))
        samples.append(
            {
                "sample": index + 1,
                "captured_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "snapshot_id": snapshot.get("snapshot_id"),
                "runtime_snapshot_id": snapshot.get("runtime_snapshot_id"),
                "observed_at": snapshot.get("observed_at"),
                "valid_until": snapshot.get("valid_until"),
                "fresh_at_capture": valid_until > now,
                "status": snapshot.get("status"),
                "runtime_status": snapshot.get("runtime_status"),
                "target_identity": target,
                "runtime_identity": runtime,
                "target_exact": isinstance(target, dict) and set(target) == target_fields,
                "runtime_exact": isinstance(runtime, dict) and set(runtime) == runtime_fields,
                "identity_binding": isinstance(target, dict)
                and isinstance(runtime, dict)
                and runtime.get("host_id") == target.get("host_id")
                and runtime.get("host_boot_id") == target.get("host_boot_id")
                and runtime.get("namespace") == target.get("namespace")
                and runtime.get("pod_uid") == target.get("pod_uid")
                and runtime.get("stream_engine_container_name") == target.get("container_name")
                and runtime.get("stream_engine_container_id") == target.get("container_id"),
            }
        )
        if index + 1 < args.samples:
            time.sleep(args.interval_seconds)

    pod = runtime_pod()
    service = command(
        [
            "systemctl",
            "show",
            "dell-target-snapshot-shadow.service",
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "MainPID",
            "-p",
            "NRestarts",
            "-p",
            "Result",
            "-p",
            "ExecStart",
        ]
    )
    public = json_command(["curl", "-fsS", "--max-time", "15", "https://yukimurata0421.dev/stream-v3-prometheus.json"])
    identities = [row["runtime_identity"] for row in samples]
    observed = [parse_utc(str(row["observed_at"])) for row in samples]
    baseline_containers = baseline["pod"]["containers"]
    checks = {
        "all_target_valid": all(row["status"] == "VALID" for row in samples),
        "all_runtime_valid": all(row["runtime_status"] == "VALID" for row in samples),
        "all_fresh": all(row["fresh_at_capture"] for row in samples),
        "all_target_exact": all(row["target_exact"] for row in samples),
        "all_runtime_exact": all(row["runtime_exact"] for row in samples),
        "all_identity_bound": all(row["identity_binding"] for row in samples),
        "runtime_identity_stable": all(identity == identities[0] for identity in identities),
        "observed_at_monotonic": observed == sorted(observed),
        "snapshot_advanced": len({row["snapshot_id"] for row in samples}) >= 2,
        "runtime_snapshot_advanced": len({row["runtime_snapshot_id"] for row in samples}) >= 2,
        "candidate_exec_path": f"/opt/{args.release_id}/" in service,
        "service_active": "ActiveState=active" in service and "SubState=running" in service,
        "service_restart_count_zero": "NRestarts=0" in service,
        "pod_uid_unchanged": pod["uid"] == baseline["pod"]["uid"],
        "containers_unchanged": all(
            pod["containers"][name]["container_id"] == value["container_id"] for name, value in baseline_containers.items()
        ),
        "containers_ready": all(value["ready"] for value in pod["containers"].values()),
        "container_restart_counts_zero": all(value["restart_count"] == 0 for value in pod["containers"].values()),
        "public_bad_zero": int(public.get("summary", {}).get("bad", -1)) == 0,
        "public_unknown_zero": int(public.get("summary", {}).get("unknown", -1)) == 0,
    }
    payload = {
        "schema_version": "runtime_identity.shadow_live.v1",
        "release_id": args.release_id,
        "samples": samples,
        "service": service.splitlines(),
        "pod": pod,
        "public": {"generated_at_iso": public.get("generated_at_iso"), "summary": public.get("summary")},
        "checks": checks,
        "unknown_count": sum(row["status"] != "VALID" or row["runtime_status"] != "VALID" for row in samples),
        "physical_effect_count": 0,
        "production_recovery_behavior_modified": False,
        "complete": all(checks.values()),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
