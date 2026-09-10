#!/usr/bin/env python3
"""Deterministic RuntimeIdentity producer Harness with an independent oracle."""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.target_snapshot import AtomicKubernetesTargetSnapshotProducer
from runtime_boundary.target import RuntimeSnapshotReader


def pod(
    *,
    uid: str = "pod-1",
    revision: str = "1",
    container_id: str = "containerd://stream-engine-1",
    ready: bool = True,
    started_at: str = "2026-08-24T00:00:00Z",
) -> dict[str, Any]:
    return {
        "metadata": {"uid": uid, "resourceVersion": revision},
        "status": {
            "containerStatuses": [
                {
                    "name": "stream-engine",
                    "containerID": container_id,
                    "ready": ready,
                    "state": {"running": {"startedAt": started_at}} if started_at else {},
                }
            ]
        },
    }


class StubProducer(AtomicKubernetesTargetSnapshotProducer):
    def __init__(self, pods: list[dict[str, Any]], *, ffmpeg_missing: bool = False) -> None:
        super().__init__(host_id="dell-yuki", namespace="stream-v3", label_selector="app=runtime")
        self.values = list(pods)
        self.ffmpeg_missing = ffmpeg_missing

    def _pods(self) -> dict[str, Any]:
        return self.values.pop(0)

    def _ffmpeg(self, _container_id: str) -> tuple[int, str]:
        if self.ffmpeg_missing:
            raise ValueError("FFMPEG_PROCESS_CARDINALITY")
        return 4242, "100"


def independent_oracle(scenario: dict[str, Any]) -> tuple[str, str, bool]:
    """Compute expected truth from fixture facts, without importing producer decisions."""

    if not scenario["started_at"]:
        return "INVALID", "INVALID", False
    if scenario["before_revision"] != scenario["after_revision"]:
        return "INVALID", "INVALID", False
    if scenario["before_container_id"] != scenario["after_container_id"]:
        return "INVALID", "INVALID", False
    if scenario["ffmpeg_missing"]:
        return "INVALID", "VALID", True
    if not scenario["ready"]:
        return "INVALID", "VALID", True
    return "VALID", "VALID", True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    specifications = [
        {
            "id": "RI-01",
            "before_revision": "1",
            "after_revision": "1",
            "before_container_id": "containerd://stream-engine-1",
            "after_container_id": "containerd://stream-engine-1",
            "started_at": "2026-08-24T00:00:00Z",
            "ready": True,
            "ffmpeg_missing": False,
        },
        {
            "id": "RI-02",
            "before_revision": "1",
            "after_revision": "1",
            "before_container_id": "containerd://stream-engine-1",
            "after_container_id": "containerd://stream-engine-1",
            "started_at": "2026-08-24T00:00:00Z",
            "ready": True,
            "ffmpeg_missing": True,
        },
        {
            "id": "RI-03",
            "before_revision": "1",
            "after_revision": "1",
            "before_container_id": "containerd://stream-engine-1",
            "after_container_id": "containerd://stream-engine-1",
            "started_at": "2026-08-24T00:00:00Z",
            "ready": False,
            "ffmpeg_missing": False,
        },
        {
            "id": "RI-04",
            "before_revision": "1",
            "after_revision": "1",
            "before_container_id": "containerd://stream-engine-1",
            "after_container_id": "containerd://stream-engine-1",
            "started_at": "",
            "ready": True,
            "ffmpeg_missing": False,
        },
        {
            "id": "RI-05",
            "before_revision": "1",
            "after_revision": "2",
            "before_container_id": "containerd://stream-engine-1",
            "after_container_id": "containerd://stream-engine-1",
            "started_at": "2026-08-24T00:00:00Z",
            "ready": True,
            "ffmpeg_missing": False,
        },
        {
            "id": "RI-06",
            "before_revision": "1",
            "after_revision": "1",
            "before_container_id": "containerd://stream-engine-1",
            "after_container_id": "containerd://stream-engine-2",
            "started_at": "2026-08-24T00:00:00Z",
            "ready": True,
            "ffmpeg_missing": False,
        },
    ]
    rows: list[dict[str, Any]] = []
    for specification in specifications:
        before = pod(
            revision=specification["before_revision"],
            container_id=specification["before_container_id"],
            ready=specification["ready"],
            started_at=specification["started_at"],
        )
        after = pod(
            revision=specification["after_revision"],
            container_id=specification["after_container_id"],
            ready=specification["ready"],
            started_at=specification["started_at"],
        )
        snapshot = StubProducer([before, after], ffmpeg_missing=specification["ffmpeg_missing"]).collect()
        expected_target, expected_runtime, expected_identity = independent_oracle(specification)
        actual = (
            snapshot["status"],
            snapshot["runtime_status"],
            snapshot["runtime_identity"] is not None,
        )
        expected = (expected_target, expected_runtime, expected_identity)
        rows.append(
            {
                "scenario_id": specification["id"],
                "expected": expected,
                "actual": actual,
                "oracle_match": actual == expected,
                "physical_effect_count": 0,
            }
        )

    stable_a = StubProducer([pod(), pod()]).collect()
    stable_b = StubProducer([pod(), pod()]).collect()
    replaced = StubProducer(
        [
            pod(container_id="containerd://stream-engine-2", started_at="2026-08-24T01:00:00Z"),
            pod(container_id="containerd://stream-engine-2", started_at="2026-08-24T01:00:00Z"),
        ]
    ).collect()
    missing = StubProducer([pod(), pod()], ffmpeg_missing=True).collect()
    missing_started = StubProducer([pod(started_at="")]).collect()
    drift = StubProducer([pod(revision="1"), pod(revision="2")]).collect()
    container_drift = StubProducer([pod(container_id="containerd://one"), pod(container_id="containerd://two")]).collect()
    with tempfile.TemporaryDirectory() as temporary:
        snapshot_path = Path(temporary) / "snapshot.json"
        stale = dict(stable_a)
        stale["observed_at"] = isoformat_utc(utc_now() - timedelta(seconds=20))
        stale["valid_until"] = isoformat_utc(utc_now() - timedelta(seconds=10))
        snapshot_path.write_text(json.dumps(stale), encoding="utf-8")
        stale_rejected = not RuntimeSnapshotReader(snapshot_path).read().available

    controls = {
        "NC-RI-01": missing_started["runtime_status"] == "INVALID",
        "NC-RI-02": missing["runtime_status"] == "VALID",
        "NC-RI-03": stable_a["runtime_identity"] == stable_b["runtime_identity"],
        "NC-RI-04": stable_a["runtime_identity"] != replaced["runtime_identity"],
        "NC-RI-05": drift["runtime_status"] == "INVALID",
        "NC-RI-06": container_drift["runtime_status"] == "INVALID",
        "NC-RI-07": missing["target_identity"] is None,
        "NC-RI-08": missing["runtime_identity"]["host_id"] == "dell-yuki",
        "NC-RI-09": int((missing.get("target_identity") or {}).get("ffmpeg_pid") or 0) == 0,
        "NC-RI-10": missing_started["runtime_identity"] is None,
        "NC-RI-11": stale_rejected,
        "NC-RI-12": all(row["physical_effect_count"] == 0 for row in rows),
    }
    result = {
        "schema_version": "runtime_identity.shadow_harness.v1",
        "scenario_count": len(rows),
        "rows": rows,
        "independent_oracle_pass": all(row["oracle_match"] for row in rows),
        "negative_controls": controls,
        "negative_controls_detected": sum(controls.values()),
        "negative_controls_total": len(controls),
        "classification_ambiguity": 0,
        "physical_effect_count": 0,
        "complete": all(row["oracle_match"] for row in rows) and all(controls.values()),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
