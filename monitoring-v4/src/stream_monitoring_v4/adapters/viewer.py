from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection

from .base import AdapterBatch
from .snapshot_support import read_source, source_timestamp


PRODUCER_REVISION = "stream-monitoring-v4-r2.3"


def viewer_status(payload: Mapping[str, Any]) -> str:
    frame_ok = payload.get("frame_ok") is True
    if frame_ok and (payload.get("black_detected") is True or payload.get("freeze_detected") is True):
        return "bad"
    if frame_ok:
        return "good"
    return "unknown"


def viewer_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": str(payload.get("status", ""))[:32],
        "frame_ok": payload.get("frame_ok") is True,
        "black_detected": payload.get("black_detected") is True,
        "freeze_detected": payload.get("freeze_detected") is True,
        "reason_present": bool(str(payload.get("reason", "")).strip()),
    }
    for key in ("consecutive_probe_failures", "consecutive_visual_failures", "duration_sec"):
        value = payload.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result[key] = value
    return result


class ViewerSyntheticAdapter:
    source = "viewer_synthetic_state"

    def __init__(self, *, state_file: Path, deadline_sec: float = 2.0) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source="viewer_synthetic",
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        receipt = snapshot.received_at
        observed_at = source_timestamp(
            snapshot,
            ("checked_at_utc",),
            source="viewer_synthetic",
            received_at=receipt,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        status = viewer_status(snapshot.payload)
        payload = viewer_payload(snapshot.payload)
        observations = (
            ObservationEnvelope.create(
                domain="viewer_external",
                source="viewer_synthetic",
                source_event_id=f"{snapshot.sha256}:viewer",
                source_generation=snapshot.sha256,
                evidence_role="supporting",
                status=status,
                reason_code=f"viewer_synthetic_{status}",
                observed_at=observed_at,
                received_at=receipt,
                freshness_limit_sec=900,
                producer_revision=PRODUCER_REVISION,
                payload=payload,
            ),
            ObservationEnvelope.create(
                domain="rendering",
                source="viewer_frame",
                source_event_id=f"{snapshot.sha256}:frame",
                source_generation=snapshot.sha256,
                evidence_role="supporting",
                status=status,
                reason_code=f"viewer_frame_{status}",
                observed_at=observed_at,
                received_at=receipt,
                freshness_limit_sec=900,
                producer_revision=PRODUCER_REVISION,
                payload=payload,
            ),
        )
        return AdapterBatch(self.source, observations, tuple(rejections))
