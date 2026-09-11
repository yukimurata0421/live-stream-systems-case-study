from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.observation import (
    ObservationEnvelope,
    ObservationRejection,
)
from stream_contracts.monitoring_v4.time import unix_ts, utc_text
from stream_contracts.monitoring_v4.youtube_api import YouTubeApiEvidence

from .base import AdapterBatch
from .json_file import SnapshotReadError, read_json_snapshot


SOURCE = "youtube_api_direct_state"
LIFECYCLE_SOURCE = "youtube_api_direct_lifecycle"
INPUT_QUALITY_SOURCE = "youtube_input_quality_api_direct"
FRESHNESS_LIMIT_SEC = 300


def _lifecycle(evidence: YouTubeApiEvidence) -> tuple[str, str]:
    if evidence.result_kind != "observed":
        return "unknown", f"youtube_api_{evidence.result_kind}"
    if evidence.lifecycle_status in {"live", "liveStarting", "testing", "testStarting"}:
        return "good", "youtube_api_lifecycle_live"
    if evidence.lifecycle_status in {"complete", "revoked"}:
        return "bad", "youtube_api_lifecycle_explicitly_ended"
    return "unknown", "youtube_api_lifecycle_unknown"


def _input_quality(evidence: YouTubeApiEvidence) -> tuple[str, str]:
    if evidence.result_kind != "observed":
        return "unknown", f"youtube_api_{evidence.result_kind}"
    if evidence.stream_status == "error":
        return "bad", "youtube_api_stream_error"
    if evidence.stream_status in {"created", "inactive", "ready"}:
        return "not_applicable", "youtube_api_stream_not_active"
    if evidence.stream_status != "active":
        return "unknown", "youtube_api_stream_status_unknown"
    warning_or_worse = any(
        item["severity"] in {"warning", "error"}
        for item in evidence.configuration_issues
    )
    unknown_issue = any(
        item["severity"] == "unrecognized"
        for item in evidence.configuration_issues
    )
    if warning_or_worse:
        return "bad", "youtube_api_input_quality_explicit_issue"
    if evidence.stream_health_status == "good" and not unknown_issue:
        return "good", "youtube_api_input_quality_good"
    if unknown_issue:
        return "unknown", "youtube_api_input_quality_issue_unrecognized"
    if evidence.stream_health_status == "noData":
        return "unknown", "youtube_api_input_quality_nodata"
    if evidence.stream_health_status in {"ok", "bad"}:
        return "bad", "youtube_api_input_quality_explicit_issue"
    return "unknown", "youtube_api_input_quality_unknown"


def _payload(evidence: YouTubeApiEvidence) -> dict[str, Any]:
    return {
        "probe_status": evidence.probe_status,
        "result_kind": evidence.result_kind,
        "lifecycle_status": evidence.lifecycle_status,
        "stream_status": evidence.stream_status,
        "stream_health_status": evidence.stream_health_status,
        "configuration_issues": [dict(item) for item in evidence.configuration_issues],
        "broadcast_id_sha256": evidence.broadcast_id_sha256,
        "bound_stream_id_sha256": evidence.bound_stream_id_sha256,
        "active_broadcast_count": evidence.active_broadcast_count,
        "api_request_count": evidence.api_request_count,
        "oauth_scope_class": evidence.oauth_scope_class,
        "oauth_scope_count": evidence.oauth_scope_count,
        "collector_id": SOURCE,
    }


class YouTubeApiEvidenceAdapter:
    source = SOURCE

    def __init__(
        self,
        *,
        state_file: Path,
        deadline_sec: float = 2.0,
        max_future_sec: int = 0,
    ) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))
        self.max_future_sec = max(0, int(max_future_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        try:
            snapshot = read_json_snapshot(
                self.state_file,
                max_bytes=64 * 1024,
                received_at=received_at,
            )
        except SnapshotReadError as exc:
            rejected_at = received_at or utc_text(int(time.time()))
            return AdapterBatch(
                SOURCE,
                rejections=(
                    ObservationRejection.create(
                        source=SOURCE,
                        reason_code=exc.reason_code,
                        detail=exc.detail,
                        received_at=rejected_at,
                        payload_sha256=exc.payload_sha256,
                    ),
                ),
            )
        try:
            evidence = YouTubeApiEvidence.from_dict(snapshot.payload)
            observed_ts = unix_ts(evidence.collected_at)
            received_ts = unix_ts(snapshot.received_at)
            if observed_ts > received_ts + self.max_future_sec:
                raise ValueError("collector timestamp is in the future")
        except (TypeError, ValueError) as exc:
            return AdapterBatch(
                SOURCE,
                rejections=(
                    ObservationRejection.create(
                        source=SOURCE,
                        reason_code="source_contract_invalid",
                        detail=str(exc)[:500],
                        received_at=snapshot.received_at,
                        payload_sha256=snapshot.sha256,
                    ),
                ),
            )
        if evidence.probe_status != "ok":
            return AdapterBatch(
                SOURCE,
                rejections=(
                    ObservationRejection.create(
                        source=SOURCE,
                        reason_code=evidence.result_kind,
                        detail="direct YouTube API measurement is unavailable",
                        received_at=snapshot.received_at,
                        payload_sha256=snapshot.sha256,
                    ),
                ),
                read_succeeded=True,
            )
        lifecycle_status, lifecycle_reason = _lifecycle(evidence)
        quality_status, quality_reason = _input_quality(evidence)
        generation = evidence.broadcast_id_sha256 or snapshot.sha256
        payload = _payload(evidence)
        observations = (
            ObservationEnvelope.create(
                domain="youtube_lifecycle",
                source=LIFECYCLE_SOURCE,
                source_event_id=f"{snapshot.sha256}:lifecycle",
                source_generation=generation,
                evidence_role="current_correlated",
                status=lifecycle_status,
                reason_code=lifecycle_reason,
                observed_at=evidence.collected_at,
                received_at=snapshot.received_at,
                freshness_limit_sec=FRESHNESS_LIMIT_SEC,
                producer_revision=evidence.collector_revision,
                payload=payload,
            ),
            ObservationEnvelope.create(
                domain="youtube_input_quality",
                source=INPUT_QUALITY_SOURCE,
                source_event_id=f"{snapshot.sha256}:input-quality",
                source_generation=generation,
                evidence_role="current_correlated",
                status=quality_status,
                reason_code=quality_reason,
                observed_at=evidence.collected_at,
                received_at=snapshot.received_at,
                freshness_limit_sec=FRESHNESS_LIMIT_SEC,
                producer_revision=evidence.collector_revision,
                payload=payload,
            ),
        )
        return AdapterBatch(SOURCE, observations=observations, read_succeeded=True)
