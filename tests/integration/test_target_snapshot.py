from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.target import FileTargetObserver
from dell_recovery_agent.target_snapshot import AtomicKubernetesTargetSnapshotProducer


def _pod(
    *,
    revision: str = "1",
    container_id: str = "containerd://abc",
    ready: bool = True,
    started_at: str = "2026-08-24T00:00:00Z",
) -> dict[str, object]:
    return {
        "metadata": {"uid": "pod-1", "resourceVersion": revision},
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
    def __init__(self, pods: list[dict[str, object]]) -> None:
        super().__init__(host_id="dell", namespace="stream-v3", label_selector="app=runtime")
        self.pods = pods

    def _pods(self) -> dict[str, object]:
        return self.pods.pop(0)

    def _ffmpeg(self, container_id: str) -> tuple[int, str]:
        return (42, "100")


def test_atomic_snapshot_is_complete() -> None:
    snapshot = StubProducer([_pod(), _pod()]).collect()
    assert snapshot["status"] == "VALID"
    assert snapshot["runtime_status"] == "VALID"
    assert set(snapshot["runtime_identity"]) == {
        "host_id",
        "host_boot_id",
        "namespace",
        "pod_uid",
        "stream_engine_container_name",
        "stream_engine_container_id",
        "runtime_generation",
    }
    assert set(snapshot["target_identity"]) == {
        "host_id",
        "host_boot_id",
        "namespace",
        "pod_uid",
        "container_name",
        "container_id",
        "ffmpeg_generation",
        "ffmpeg_pid",
    }


def test_atomic_snapshot_rejects_generation_race() -> None:
    snapshot = StubProducer([_pod(revision="1"), _pod(revision="2")]).collect()
    assert snapshot["status"] == "INVALID"
    assert snapshot["reason_code"] == "SNAPSHOT_UNSTABLE"
    assert snapshot["runtime_status"] == "INVALID"
    assert snapshot["runtime_reason_code"] == "RUNTIME_SNAPSHOT_UNSTABLE"


def test_runtime_identity_remains_valid_when_ffmpeg_is_missing() -> None:
    producer = StubProducer([_pod(), _pod()])

    def missing(_container_id: str) -> tuple[int, str]:
        raise ValueError("FFMPEG_PROCESS_CARDINALITY")

    producer._ffmpeg = missing  # type: ignore[method-assign]
    snapshot = producer.collect()
    assert snapshot["status"] == "INVALID"
    assert snapshot["reason_code"] == "FFMPEG_PROCESS_CARDINALITY"
    assert snapshot["target_identity"] is None
    assert snapshot["runtime_status"] == "VALID"
    assert snapshot["runtime_identity"]["pod_uid"] == "pod-1"


def test_runtime_identity_requires_real_container_started_at() -> None:
    snapshot = StubProducer([_pod(started_at="")]).collect()
    assert snapshot["status"] == "INVALID"
    assert snapshot["runtime_status"] == "INVALID"
    assert snapshot["reason_code"] == "RUNTIME_CONTAINER_STARTED_AT_UNAVAILABLE"
    assert snapshot["runtime_identity"] is None


def test_runtime_identity_is_stable_and_changes_with_container_instance() -> None:
    first = StubProducer([_pod(), _pod()]).collect()
    repeated = StubProducer([_pod(), _pod()]).collect()
    replaced = StubProducer(
        [
            _pod(container_id="containerd://def", started_at="2026-08-24T01:00:00Z"),
            _pod(container_id="containerd://def", started_at="2026-08-24T01:00:00Z"),
        ]
    ).collect()
    assert first["runtime_identity"] == repeated["runtime_identity"]
    assert first["runtime_identity"] != replaced["runtime_identity"]


def test_runtime_identity_survives_not_ready_without_enabling_ffmpeg_target() -> None:
    snapshot = StubProducer([_pod(ready=False), _pod(ready=False)]).collect()
    assert snapshot["status"] == "INVALID"
    assert snapshot["reason_code"] == "TARGET_CONTAINER_NOT_READY"
    assert snapshot["target_identity"] is None
    assert snapshot["runtime_status"] == "VALID"
    assert snapshot["runtime_container_ready"] is False


def test_file_observer_rejects_stale_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps(
            {
                "schema": "cra_dell_recovery.target_snapshot.v1",
                "status": "VALID",
                "valid_until": isoformat_utc(utc_now() - timedelta(seconds=1)),
                "target_identity": {},
            }
        ),
        encoding="utf-8",
    )
    observer = FileTargetObserver(path)
    assert observer.observe() is None
    assert observer.last_reason_code == "STALE_TARGET"
