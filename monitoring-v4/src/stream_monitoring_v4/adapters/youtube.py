from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection
from stream_contracts.monitoring_v4.time import parse_utc, unix_ts, utc_text

from .base import AdapterBatch
from .json_file import JsonSnapshot, SnapshotReadError, read_json_snapshot


WATCHDOG_LIFECYCLE_KEYS = (
    "status",
    "healthy",
    "video_id",
    "expected_video_id",
    "api_live_state",
    "oauth_life_cycle_status",
    "oauth_stream_status",
    "oauth_stream_health_status",
    "oauth_stream_health_issues",
    "availability_ok",
    "public_ok",
    "evidence_state",
    "incident_stage",
    "failure_kind",
    "judgment",
    "judgment_reason",
    "remote_sample_id",
)
WATCHDOG_DELIVERY_KEYS = (
    "stream_active",
    "ingest_connected",
    "local_ok",
    "ffmpeg_generation",
    "oauth_stream_status",
    "oauth_stream_health_status",
)
WATCHDOG_INPUT_QUALITY_KEYS = (
    "oauth_probe_ok",
    "oauth_stream_status",
    "oauth_stream_health_status",
    "oauth_stream_health_issues",
    "ingest_connected",
)
RESOLVER_KEYS = (
    "video_id",
    "source",
    "expected_video_id",
    "candidate_new_url_found",
    "candidate_new_video_id",
    "selected_candidate_policy",
    "url_preservation_active",
    "remote_ended_confirmed",
    "remote_ended_reason",
    "quota_guard_active",
    "fast_mode_active",
)
PRODUCER_REVISION = "stream-monitoring-v4-r2.2"


def _text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    return value.strip() if isinstance(value, str) else ""


def _sanitized(payload: Mapping[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in keys:
        value = payload.get(key)
        if isinstance(value, (str, int, float, bool, type(None))):
            result[key] = value
        elif isinstance(value, list) and all(isinstance(item, str) for item in value[:20]):
            result[key] = [item[:120] for item in value[:20]]
    return result


def watchdog_lifecycle_status(payload: Mapping[str, Any]) -> str:
    if _text(payload, "failure_kind").lower() == "remote_ended":
        return "bad"
    lifecycle = _text(payload, "oauth_life_cycle_status").lower()
    api_state = _text(payload, "api_live_state").lower()
    if lifecycle in {"complete", "completed", "ended"} or api_state in {"complete", "completed", "ended"}:
        return "bad"
    if lifecycle in {"live", "livestarting", "testing", "teststarting"}:
        return "good"
    if api_state == "live":
        return "good"
    return "unknown"


def watchdog_delivery_status(payload: Mapping[str, Any]) -> str:
    signals = (payload.get("stream_active"), payload.get("ingest_connected"), payload.get("local_ok"))
    if any(value is False for value in signals):
        return "bad"
    if all(value is True for value in signals):
        return "good"
    return "unknown"


def _input_quality_issue_details(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    raw = payload.get("oauth_stream_health_issue_details")
    if not isinstance(raw, list):
        return []
    result: list[dict[str, str]] = []
    for item in raw[:32]:
        if not isinstance(item, Mapping):
            continue
        issue_type = _text(item, "type")[:80]
        severity = _text(item, "severity").lower()[:16]
        if issue_type or severity:
            result.append({"type": issue_type, "severity": severity})
    return result


def watchdog_input_quality_status(payload: Mapping[str, Any]) -> tuple[str, str]:
    if payload.get("oauth_probe_ok") is not True:
        return "unknown", "input_quality_oauth_probe_failed"
    if _text(payload, "oauth_stream_status").lower() != "active":
        return "not_applicable", "input_quality_stream_not_active"
    if payload.get("ingest_connected") is not True:
        return "not_applicable", "input_quality_local_ingest_disconnected"
    health = _text(payload, "oauth_stream_health_status").lower()
    details = _input_quality_issue_details(payload)
    warning_or_worse = any(item["severity"] in {"warning", "error"} for item in details)
    if health == "good" and not warning_or_worse:
        return "good", "input_quality_current_good"
    # YouTube documents noData as the backend having no health information. It
    # is therefore diagnostic absence, not an explicit current quality fault.
    if health == "nodata":
        return "unknown", "input_quality_health_nodata"
    if health == "ok":
        return "bad", "input_quality_health_warning"
    if health == "bad":
        return "bad", "input_quality_health_error"
    if warning_or_worse:
        return "bad", "input_quality_configuration_issue"
    return "unknown", "input_quality_health_unknown"


def _input_quality_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = _sanitized(payload, WATCHDOG_INPUT_QUALITY_KEYS)
    details = _input_quality_issue_details(payload)
    if details:
        result["oauth_stream_health_issue_details"] = details
    return result


def resolver_status(payload: Mapping[str, Any]) -> str:
    if payload.get("remote_ended_confirmed") is True:
        return "bad"
    video_id = _text(payload, "video_id")
    expected = _text(payload, "expected_video_id")
    if video_id and (not expected or video_id == expected):
        return "good"
    return "unknown"


class YouTubeStateAdapter:
    """Read existing YouTube state through an allowlist and never write back to it."""

    source = "youtube_state_files"

    def __init__(
        self,
        *,
        watchdog_stats_file: Path,
        resolver_state_file: Path,
        deadline_sec: float = 2.0,
        max_future_sec: int = 0,
    ) -> None:
        self.watchdog_stats_file = Path(watchdog_stats_file)
        self.resolver_state_file = Path(resolver_state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))
        self.max_future_sec = max(0, int(max_future_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        observations: list[ObservationEnvelope] = []
        rejections: list[ObservationRejection] = []
        self._watchdog(received_at, observations, rejections)
        self._resolver(received_at, observations, rejections)
        return AdapterBatch(self.source, tuple(observations), tuple(rejections))

    def _read(
        self,
        path: Path,
        source: str,
        received_at: str | None,
        rejections: list[ObservationRejection],
    ) -> JsonSnapshot | None:
        try:
            return read_json_snapshot(path, received_at=received_at)
        except SnapshotReadError as exc:
            rejected_at = received_at or utc_text(int(time.time()))
            rejections.append(
                ObservationRejection.create(
                    source=source,
                    reason_code=exc.reason_code,
                    detail=exc.detail,
                    received_at=rejected_at,
                    payload_sha256=exc.payload_sha256,
                )
            )
            return None

    def _timestamp(
        self,
        payload: Mapping[str, Any],
        keys: tuple[str, ...],
        *,
        source: str,
        received_at: str,
        sha256: str,
        rejections: list[ObservationRejection],
    ) -> str | None:
        raw = next((str(payload.get(key, "")).strip() for key in keys if str(payload.get(key, "")).strip()), "")
        try:
            parse_utc(raw, field="source timestamp")
        except ValueError as exc:
            rejections.append(
                ObservationRejection.create(
                    source=source,
                    reason_code="source_timestamp_invalid",
                    detail=str(exc)[:500],
                    received_at=received_at,
                    payload_sha256=sha256,
                )
            )
            return None
        if unix_ts(raw) > unix_ts(received_at) + self.max_future_sec:
            rejections.append(
                ObservationRejection.create(
                    source=source,
                    reason_code="source_timestamp_future",
                    detail=f"source timestamp exceeds received_at by more than {self.max_future_sec}s",
                    received_at=received_at,
                    payload_sha256=sha256,
                )
            )
            return None
        return raw

    def _watchdog(
        self,
        received_at: str | None,
        observations: list[ObservationEnvelope],
        rejections: list[ObservationRejection],
    ) -> None:
        snapshot = self._read(self.watchdog_stats_file, "youtube_watchdog", received_at, rejections)
        if snapshot is None:
            return
        receipt = snapshot.received_at
        lifecycle_observed_at = self._timestamp(
            snapshot.payload,
            (
                "remote_probe_ts_utc",
                "data_api_checked_ts_utc",
                "oauth_checked_ts_utc",
                "stats_file_updated_at_utc",
                "ts_utc",
            ),
            source="youtube_watchdog",
            received_at=receipt,
            sha256=snapshot.sha256,
            rejections=rejections,
        )
        event = _text(snapshot.payload, "remote_sample_id") or snapshot.sha256
        if lifecycle_observed_at is not None:
            lifecycle_status = watchdog_lifecycle_status(snapshot.payload)
            observations.append(
                ObservationEnvelope.create(
                    domain="youtube_lifecycle",
                    source="youtube_watchdog",
                    source_event_id=f"{event}:lifecycle",
                    source_generation=event,
                    evidence_role="current_authoritative",
                    status=lifecycle_status,
                    reason_code=f"remote_lifecycle_{lifecycle_status}",
                    observed_at=lifecycle_observed_at,
                    received_at=receipt,
                    freshness_limit_sec=180,
                    producer_revision=PRODUCER_REVISION,
                    payload=_sanitized(snapshot.payload, WATCHDOG_LIFECYCLE_KEYS),
                )
            )
        input_quality_keys = ("oauth_checked_ts_utc",) if "oauth_checked_ts_utc" in snapshot.payload else ("ts_utc",)
        input_quality_observed_at = self._timestamp(
            snapshot.payload,
            input_quality_keys,
            source="youtube_input_quality_oauth",
            received_at=receipt,
            sha256=snapshot.sha256,
            rejections=rejections,
        )
        if input_quality_observed_at is not None:
            input_status, input_reason = watchdog_input_quality_status(snapshot.payload)
            observations.append(
                ObservationEnvelope.create(
                    domain="youtube_input_quality",
                    source="youtube_input_quality_oauth",
                    source_event_id=f"{event}:input-quality",
                    source_generation=event,
                    evidence_role="current_authoritative",
                    status=input_status,
                    reason_code=input_reason,
                    observed_at=input_quality_observed_at,
                    received_at=receipt,
                    freshness_limit_sec=600,
                    producer_revision=PRODUCER_REVISION,
                    payload=_input_quality_payload(snapshot.payload),
                )
            )
        delivery_observed_at = self._timestamp(
            snapshot.payload,
            ("stats_file_updated_at_utc", "ts_utc"),
            source="runtime_delivery_watchdog",
            received_at=receipt,
            sha256=snapshot.sha256,
            rejections=rejections,
        )
        if delivery_observed_at is not None:
            delivery_status = watchdog_delivery_status(snapshot.payload)
            delivery_generation = _text(snapshot.payload, "ffmpeg_generation") or event
            observations.append(
                ObservationEnvelope.create(
                    domain="delivery",
                    source="runtime_delivery_watchdog",
                    source_event_id=f"{event}:delivery",
                    source_generation=delivery_generation,
                    evidence_role="current_authoritative",
                    status=delivery_status,
                    reason_code=f"runtime_delivery_{delivery_status}",
                    observed_at=delivery_observed_at,
                    received_at=receipt,
                    freshness_limit_sec=180,
                    producer_revision=PRODUCER_REVISION,
                    payload=_sanitized(snapshot.payload, WATCHDOG_DELIVERY_KEYS),
                )
            )

    def _resolver(
        self,
        received_at: str | None,
        observations: list[ObservationEnvelope],
        rejections: list[ObservationRejection],
    ) -> None:
        snapshot = self._read(self.resolver_state_file, "youtube_video_resolver", received_at, rejections)
        if snapshot is None:
            return
        receipt = snapshot.received_at
        observed_at = self._timestamp(
            snapshot.payload,
            ("ts_utc",),
            source="youtube_video_resolver",
            received_at=receipt,
            sha256=snapshot.sha256,
            rejections=rejections,
        )
        if observed_at is None:
            return
        status = resolver_status(snapshot.payload)
        generation = _text(snapshot.payload, "video_id") or snapshot.sha256
        observations.append(
            ObservationEnvelope.create(
                domain="youtube_lifecycle",
                source="youtube_video_resolver",
                source_event_id=snapshot.sha256,
                source_generation=generation,
                evidence_role="current_correlated",
                status=status,
                reason_code=f"same_url_resolver_{status}",
                observed_at=observed_at,
                received_at=receipt,
                freshness_limit_sec=90,
                producer_revision=PRODUCER_REVISION,
                payload=_sanitized(snapshot.payload, RESOLVER_KEYS),
            )
        )
