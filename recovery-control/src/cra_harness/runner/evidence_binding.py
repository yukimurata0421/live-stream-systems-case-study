from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from cra_harness.oracles.evidence_binding import evidence_binding_violations

TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot-dell-1",
    "namespace": "stream-v3",
    "pod_uid": "pod-current",
    "container_name": "stream-engine",
    "container_id": "containerd://current",
    "ffmpeg_generation": "ffmpeg-current",
    "ffmpeg_pid": 4242,
}


def negative_controls() -> list[dict[str, Any]]:
    old_pod = {**TARGET, "pod_uid": "pod-old"}
    old_ffmpeg = {**TARGET, "ffmpeg_generation": "ffmpeg-old", "ffmpeg_pid": 3131}
    partial = {key: value for key, value in TARGET.items() if key != "container_id"}
    snapshot_base = {
        "domain": "SNAPSHOT",
        "exists": True,
        "integrity_ok": True,
        "fresh": True,
        "startup_reconciled": True,
        "unresolved_count": 0,
        "active_fence_count": 0,
        "transaction_uncertain": False,
        "maintenance_state": "INACTIVE",
        "candidate_generation": 8,
        "current_generation": 8,
    }
    return [
        {
            "control_id": "NC-E01",
            "domain": "HOST",
            "left": {"host_id": "dell-yuki", "host_boot_id": "boot-d", "os_hostname": "yuki"},
            "right": {"host_id": "arena-server", "host_boot_id": "boot-a", "os_hostname": "yuki"},
            "observed_result": "SAME",
        },
        {
            "control_id": "NC-E02",
            "domain": "HOST",
            "left": {"host_id": "dell-yuki", "host_boot_id": "boot-old"},
            "right": {"host_id": "dell-yuki", "host_boot_id": "boot-new"},
            "observed_result": "SAME",
        },
        {
            "control_id": "NC-E03",
            "domain": "HOST",
            "left": {"host_id": "", "host_boot_id": "boot-a", "os_hostname": "yuki"},
            "right": {"host_id": "", "host_boot_id": "boot-b", "os_hostname": "yuki"},
            "observed_result": "SAME",
        },
        {**snapshot_base, "control_id": "NC-E04", "exists": False, "observed_result": "INACTIVE"},
        {**snapshot_base, "control_id": "NC-E05", "fresh": False, "observed_result": "INACTIVE"},
        {**snapshot_base, "control_id": "NC-E06", "startup_reconciled": False, "observed_result": "INACTIVE"},
        {
            **snapshot_base,
            "control_id": "NC-E07",
            "candidate_generation": 7,
            "current_generation": 8,
            "observed_result": "INACTIVE",
        },
        {**snapshot_base, "control_id": "NC-E08", "integrity_ok": False, "observed_result": "INACTIVE"},
        {
            "control_id": "NC-E09",
            "domain": "TARGET",
            "expected": TARGET,
            "observed": partial,
            "fresh": True,
            "observed_result": "MATCH",
        },
        {
            "control_id": "NC-E10",
            "domain": "TARGET",
            "expected": TARGET,
            "observed": old_pod,
            "fresh": True,
            "observed_result": "MATCH",
        },
        {
            "control_id": "NC-E11",
            "domain": "TARGET",
            "expected": TARGET,
            "observed": old_ffmpeg,
            "fresh": True,
            "observed_result": "MATCH",
        },
        {
            "control_id": "NC-E12",
            "domain": "TARGET",
            "expected": TARGET,
            "observed": TARGET,
            "fresh": False,
            "observed_result": "MATCH",
        },
        {
            "control_id": "NC-E13",
            "domain": "GENERATION",
            "maintenance_mapping_confirmed": False,
            "native_event_id": "event-8",
            "operation_generation": 8,
            "current_generation": 8,
            "observed_result": "CURRENT",
        },
        {
            "control_id": "NC-E14",
            "domain": "GENERATION",
            "maintenance_mapping_confirmed": True,
            "operation_generation": None,
            "current_generation": 0,
            "observed_result": "CURRENT",
        },
        {
            "control_id": "NC-E15",
            "domain": "GENERATION",
            "maintenance_mapping_confirmed": True,
            "operation_generation": 7,
            "current_generation": 8,
            "observed_result": "CURRENT",
        },
    ]


def run() -> dict[str, Any]:
    cases = deepcopy(negative_controls())
    results = []
    for case in cases:
        violations = evidence_binding_violations(case)
        results.append(
            {
                "control_id": case["control_id"],
                "classification": "EXPECTED_INJECTED_FAILURE" if violations else "MISSED_INJECTED_FAILURE",
                "violations": violations,
            }
        )
    detected = sum(result["classification"] == "EXPECTED_INJECTED_FAILURE" for result in results)
    return {
        "schema_version": "cra.evidence_binding_harness.v1",
        "negative_control_count": len(results),
        "negative_control_detected": detected,
        "negative_control_detection_rate": detected / len(results),
        "independent_oracle": "PASS" if detected == len(results) else "FAIL",
        "physical_effect_count": 0,
        "production_behavior_modified": False,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evidence Binding independent Negative Control harness")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run()
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    raise SystemExit(0 if result["independent_oracle"] == "PASS" else 1)


if __name__ == "__main__":
    main()
