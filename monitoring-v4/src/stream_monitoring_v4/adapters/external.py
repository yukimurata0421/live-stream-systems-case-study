from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection

from .base import AdapterBatch
from .snapshot_support import read_source, source_timestamp, status_word


PRODUCER_REVISION = "stream-monitoring-v4-r2.3"
TARGET_ALLOWLIST = ("public_status", "youtube_public_video")
REASON_ALLOWLIST = frozenset(
    {
        "all_external_targets_passed",
        "one_or_more_external_targets_failed",
        "external_target_evidence_incomplete",
    }
)


def external_status(payload: Mapping[str, Any]) -> str:
    return status_word(payload.get("status"))


def external_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_targets = payload.get("targets")
    targets = raw_targets if isinstance(raw_targets, Mapping) else {}
    safe_targets: dict[str, dict[str, Any]] = {}
    for name in TARGET_ALLOWLIST:
        raw = targets.get(name)
        if not isinstance(raw, Mapping):
            continue
        item: dict[str, Any] = {"status": str(raw.get("status", ""))[:32]}
        for key in (
            "fresh_locations",
            "observed_locations",
            "passed_locations",
            "minimum_locations",
            "minimum_pass_ratio",
            "pass_ratio",
            "sample_age_seconds",
        ):
            value = raw.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                item[key] = value
        safe_targets[name] = item
    raw_reason = str(payload.get("reason", ""))
    result: dict[str, Any] = {
        "status": str(payload.get("status", ""))[:32],
        "reason": raw_reason if raw_reason in REASON_ALLOWLIST else "source_reason_unrecognized",
        "formal_sli": payload.get("formal_sli") is True,
        "targets": safe_targets,
    }
    for key in ("checked_at_utc", "evidence_at_utc", "status_since_utc"):
        if isinstance(payload.get(key), str):
            result[key] = str(payload[key])[:40]
    for key in ("evidence_age_seconds", "status_duration_seconds", "consecutive_status_samples"):
        value = payload.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result[key] = value
    return result


class ExternalBlackboxAdapter:
    source = "external_blackbox_state"

    def __init__(self, *, state_file: Path, deadline_sec: float = 2.0) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source="external_blackbox",
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        receipt = snapshot.received_at
        observed_at = source_timestamp(
            snapshot,
            ("evidence_at_utc",),
            source="external_blackbox",
            received_at=receipt,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        status = external_status(snapshot.payload)
        item = ObservationEnvelope.create(
            domain="viewer_external",
            source="external_blackbox",
            source_event_id=snapshot.sha256,
            source_generation=snapshot.sha256,
            evidence_role="supporting",
            status=status,
            reason_code=f"external_blackbox_{status}",
            observed_at=observed_at,
            received_at=receipt,
            freshness_limit_sec=900,
            producer_revision=PRODUCER_REVISION,
            payload=external_payload(snapshot.payload),
        )
        return AdapterBatch(self.source, (item,), tuple(rejections))
