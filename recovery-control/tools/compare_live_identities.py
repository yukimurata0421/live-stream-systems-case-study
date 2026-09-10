#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def service_properties(value: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in value.get("properties", []):
        if isinstance(line, str) and "=" in line:
            key, raw = line.split("=", 1)
            result[key] = raw
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    before = json.loads(args.before.read_text(encoding="utf-8"))
    after = json.loads(args.after.read_text(encoding="utf-8"))
    before_containers = before["pod"]["containers"]
    after_containers = after["pod"]["containers"]
    container_checks = {
        name: {
            "container_id_unchanged": before_containers[name]["container_id"] == after_containers[name]["container_id"],
            "image_unchanged": before_containers[name]["image"] == after_containers[name]["image"],
            "restart_count_before": before_containers[name]["restart_count"],
            "restart_count_after": after_containers[name]["restart_count"],
            "ready_after": after_containers[name]["ready"],
        }
        for name in sorted(before_containers)
    }
    before_agent = service_properties(before["dell_agent_service"])
    after_agent = service_properties(after["dell_agent_service"])
    checks = {
        "deployment_uid_unchanged": before["deployment"]["uid"] == after["deployment"]["uid"],
        "deployment_generation_unchanged": before["deployment"]["generation"] == after["deployment"]["generation"],
        "pod_uid_unchanged": before["pod"]["uid"] == after["pod"]["uid"],
        "all_container_ids_unchanged": all(value["container_id_unchanged"] for value in container_checks.values()),
        "all_container_images_unchanged": all(value["image_unchanged"] for value in container_checks.values()),
        "all_restart_counts_zero": all(value["restart_count_after"] == 0 for value in container_checks.values()),
        "all_containers_ready": all(value["ready_after"] is True for value in container_checks.values()),
        "ffmpeg_pid_unchanged": before["fast_recovery_state"]["last_pid"] == after["fast_recovery_state"]["last_pid"],
        "fast_recovery_restart_events_unchanged": before["fast_recovery_state"]["restart_event_count"]
        == after["fast_recovery_state"]["restart_event_count"],
        "dell_agent_pid_unchanged": before_agent.get("MainPID") == after_agent.get("MainPID"),
        "dell_agent_restarts_zero": after_agent.get("NRestarts") == "0",
        "candidate_not_loaded": after.get("candidate_loaded") is False,
    }
    payload = {
        "schema_version": "recovery_control.live_identity_comparison.v1",
        "before_timestamp_jst": before["timestamp_jst"],
        "after_timestamp_jst": after["timestamp_jst"],
        "checks": checks,
        "containers": container_checks,
        "pass": all(checks.values()),
        "unexpected_restart_count": sum(int(value["restart_count_after"]) for value in container_checks.values()),
        "production_behavior_modified": False,
        "physical_effect_count": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
