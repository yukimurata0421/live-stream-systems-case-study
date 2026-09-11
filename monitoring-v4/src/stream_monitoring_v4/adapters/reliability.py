from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection

from .base import AdapterBatch
from .snapshot_support import bounded_strings, read_source, source_timestamp


PRODUCER_REVISION = "stream-monitoring-v4-r6.0"


def prometheus_projection_status(payload: Mapping[str, Any]) -> tuple[str, str]:
    if payload.get("available") is not True or payload.get("eligible") is not True:
        return "unknown", "input_quality_prometheus_unavailable"
    if payload.get("good") is True:
        return "good", "input_quality_prometheus_good"
    if payload.get("good") is False:
        return "bad", "input_quality_prometheus_bad"
    return "unknown", "input_quality_prometheus_unknown"


def rolling_projection_status(payload: Mapping[str, Any]) -> tuple[str, str]:
    if str(payload.get("measurement_status", "")).lower() != "valid":
        return "unknown", "input_quality_rolling_unknown"
    if payload.get("target_met_on_observed_samples") is True:
        return "good", "input_quality_rolling_target_met"
    if payload.get("target_met_on_observed_samples") is False:
        return "bad", "input_quality_rolling_target_missed"
    return "unknown", "input_quality_rolling_unknown"


def _number_fields(payload: Mapping[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {
        key: payload[key]
        for key in keys
        if isinstance(payload.get(key), (int, float)) and not isinstance(payload.get(key), bool)
    }


class InputQualityProjectionAdapter:
    """Read cached v3 reliability projections without granting them current authority."""

    source = "input_quality_projection_state"

    def __init__(self, *, burn_status_file: Path, deadline_sec: float = 2.0) -> None:
        self.burn_status_file = Path(burn_status_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        observations: list[ObservationEnvelope] = []
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.burn_status_file,
            source="youtube_input_quality_projection",
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        receipt = snapshot.received_at
        feedback_root = snapshot.payload.get("fast_feedback")
        feedback_by_name = feedback_root if isinstance(feedback_root, Mapping) else {}
        raw_feedback = feedback_by_name.get("youtube_input_quality")
        feedback = raw_feedback if isinstance(raw_feedback, Mapping) else {}
        prometheus_raw = feedback.get("prometheus_current")
        prometheus = prometheus_raw if isinstance(prometheus_raw, Mapping) else {}
        if prometheus:
            observed_at = source_timestamp(
                replace(snapshot, payload=prometheus),
                ("ts_utc",),
                source="youtube_input_quality_prometheus",
                received_at=receipt,
                rejections=rejections,
            )
            if observed_at is not None:
                status, reason = prometheus_projection_status(prometheus)
                observations.append(
                    ObservationEnvelope.create(
                        domain="youtube_input_quality",
                        source="youtube_input_quality_prometheus",
                        source_event_id=f"{snapshot.sha256}:prometheus",
                        source_generation=snapshot.sha256,
                        evidence_role="current_correlated",
                        status=status,
                        reason_code=reason,
                        observed_at=observed_at,
                        received_at=receipt,
                        freshness_limit_sec=360,
                        producer_revision=PRODUCER_REVISION,
                        payload={
                            key: prometheus[key]
                            for key in ("available", "eligible", "good")
                            if isinstance(prometheus.get(key), bool)
                        },
                    )
                )
        checked_at = source_timestamp(
            snapshot,
            ("checked_at_utc",),
            source="youtube_input_quality_rolling",
            received_at=receipt,
            rejections=rejections,
        )
        if checked_at is not None and feedback:
            status, reason = rolling_projection_status(feedback)
            payload = {
                "measurement_status": str(feedback.get("measurement_status", ""))[:32],
                "measurement_unknown_reasons": bounded_strings(
                    feedback.get("measurement_unknown_reasons")
                ),
                "target_met_on_observed_samples": feedback.get("target_met_on_observed_samples"),
                "source_disagreement": feedback.get("source_disagreement") is True,
                "source_disagreement_current": feedback.get("source_disagreement_current") is True,
                **_number_fields(
                    feedback,
                    ("sli_pct", "coverage_pct", "source_freshness_pct"),
                ),
            }
            observations.append(
                ObservationEnvelope.create(
                    domain="youtube_input_quality",
                    source="youtube_input_quality_rolling",
                    source_event_id=f"{snapshot.sha256}:rolling",
                    source_generation=snapshot.sha256,
                    evidence_role="historical",
                    status=status,
                    reason_code=reason,
                    observed_at=checked_at,
                    received_at=receipt,
                    freshness_limit_sec=3600,
                    producer_revision=PRODUCER_REVISION,
                    payload=payload,
                )
            )
        return AdapterBatch(self.source, tuple(observations), tuple(rejections))
