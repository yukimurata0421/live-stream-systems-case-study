from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.observation import ObservationRejection
from stream_contracts.monitoring_v4.sli import SLIProjection
from stream_contracts.monitoring_v4.time import parse_utc, unix_ts, utc_text

from stream_monitoring_v4.adapters.snapshot_support import read_source, source_timestamp
from stream_monitoring_v4.storage.ports import ReliabilityProjectionRepository


POLICY_REVISION = "monitoring-v4-sli-policy-r6.0"
FORMAL_SOURCE_MAX_AGE_SEC = 2 * 3600
FAST_SOURCE_MAX_AGE_SEC = 15 * 60


@dataclass(frozen=True)
class ObjectiveSpec:
    objective_id: str
    window: str
    duration_sec: int
    target_pct: float


FORMAL_OBJECTIVES = {
    item.objective_id: item
    for item in (
        ObjectiveSpec("youtube_availability", "rolling_7d", 7 * 86400, 99.0),
        ObjectiveSpec("same_url_preservation", "rolling_30d", 30 * 86400, 99.9),
        ObjectiveSpec("upload_ceiling", "rolling_24h", 86400, 99.0),
        ObjectiveSpec("youtube_input_quality", "rolling_7d", 7 * 86400, 99.0),
        ObjectiveSpec("audio_correctness", "rolling_7d", 7 * 86400, 99.5),
    )
}
FAST_INPUT_QUALITY = ObjectiveSpec("youtube_input_quality", "rolling_1h", 3600, 99.0)
EXPECTED_PROJECTION_KEYS = frozenset(
    {("formal", item.objective_id, item.window) for item in FORMAL_OBJECTIVES.values()}
    | {("fast", FAST_INPUT_QUALITY.objective_id, FAST_INPUT_QUALITY.window)}
)


def projection_integrity(
    projections: tuple[SLIProjection, ...] | list[SLIProjection],
    rejections: tuple[ObservationRejection, ...] | list[ObservationRejection],
) -> dict[str, Any]:
    actual = {
        (item.assessment_scope, item.objective_id, item.window) for item in projections
    }
    missing = sorted(EXPECTED_PROJECTION_KEYS - actual)
    unexpected = sorted(actual - EXPECTED_PROJECTION_KEYS)
    return {
        "complete": not missing and not unexpected and not rejections,
        "expected_count": len(EXPECTED_PROJECTION_KEYS),
        "projection_count": len(projections),
        "rejection_count": len(rejections),
        "missing_keys": [list(item) for item in missing],
        "unexpected_keys": [list(item) for item in unexpected],
    }


@dataclass(frozen=True)
class ReliabilityProjectionRun:
    projections: tuple[SLIProjection, ...]
    rejections: tuple[ObservationRejection, ...]
    inserted: int
    duplicates: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "projections": [item.to_dict() for item in self.projections],
            "rejections": [item.to_dict() for item in self.rejections],
            "inserted": self.inserted,
            "duplicates": self.duplicates,
            "no_current_incident_input": True,
            "no_automatic_recovery": True,
        }


def _number(payload: Mapping[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _reasons(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    result: list[str] = []
    for item in value[:32]:
        raw = str(item).strip().lower()
        if re.fullmatch(r"[a-z][a-z0-9_.-]{1,95}", raw):
            result.append(raw)
        elif raw:
            result.append("source_reason_unparseable")
    return tuple(sorted(set(result)))


def _window(spec: ObjectiveSpec, end_at: str) -> tuple[str, str]:
    end = parse_utc(end_at, field="projection window end")
    return utc_text(end - timedelta(seconds=spec.duration_sec)), utc_text(end)


def _formal_projection(
    spec: ObjectiveSpec,
    gate: Mapping[str, Any],
    *,
    source_sha256: str,
    source_checked_at: str,
    evaluated_at: str,
) -> SLIProjection:
    compliance = str(gate.get("compliance_status", "unknown")).strip().lower()
    if compliance not in {"met", "breached", "unknown"}:
        compliance = "unknown"
    reasons = _reasons(gate.get("measurement_unknown_reasons"))
    if compliance == "unknown" and not reasons:
        reasons = ("source_compliance_unknown",)
    start_at, end_at = _window(spec, source_checked_at)
    evidence_id = stable_id("evd", "operational_reliability_status", source_sha256, spec.objective_id)
    payload: dict[str, Any] = {
        "measurement_status": str(gate.get("measurement_status", ""))[:32],
        "sli_pct": _number(gate, "sli_pct"),
        "target_pct": spec.target_pct,
        "source_disagreement_current": gate.get("source_disagreement_current") is True,
        "source_artifact": "operational_reliability_status.json",
    }
    return SLIProjection.create(
        objective_id=spec.objective_id,
        window=spec.window,
        assessment_scope="formal",
        is_official_window=True,
        observed=None,
        eligible=None,
        bad=None,
        missing=None,
        coverage_pct=_number(gate, "coverage_pct"),
        source_freshness_pct=_number(gate, "source_freshness_pct"),
        source_disagreement=gate.get("source_disagreement") is True,
        compliance_status=compliance,
        measurement_unknown_reasons=reasons,
        window_start=start_at,
        window_end=end_at,
        evaluated_at=evaluated_at,
        policy_revision=POLICY_REVISION,
        evidence_ids=(evidence_id,),
        payload=payload,
    )


def _fast_projection(
    feedback: Mapping[str, Any],
    *,
    source_sha256: str,
    source_checked_at: str,
    evaluated_at: str,
) -> SLIProjection:
    spec = FAST_INPUT_QUALITY
    start_at, end_at = _window(spec, source_checked_at)
    evidence_id = stable_id("evd", "operational_reliability_burn_status", source_sha256, spec.objective_id)
    payload = {
        "measurement_status": str(feedback.get("measurement_status", ""))[:32],
        "sli_pct": _number(feedback, "sli_pct"),
        "target_pct": spec.target_pct,
        "target_met_on_observed_samples": feedback.get("target_met_on_observed_samples"),
        "source_disagreement_current": feedback.get("source_disagreement_current") is True,
        "source_artifact": "operational_reliability_burn_status.json",
    }
    return SLIProjection.create(
        objective_id=spec.objective_id,
        window=spec.window,
        assessment_scope="fast",
        is_official_window=False,
        observed=None,
        eligible=None,
        bad=None,
        missing=None,
        coverage_pct=_number(feedback, "coverage_pct"),
        source_freshness_pct=_number(feedback, "source_freshness_pct"),
        source_disagreement=feedback.get("source_disagreement") is True,
        compliance_status="unknown",
        measurement_unknown_reasons=_reasons(feedback.get("measurement_unknown_reasons")),
        window_start=start_at,
        window_end=end_at,
        evaluated_at=evaluated_at,
        policy_revision=POLICY_REVISION,
        evidence_ids=(evidence_id,),
        payload=payload,
    )


class ReliabilityProjector:
    """Copy precomputed v3 SLI facts into a separate analytical projection table."""

    def __init__(
        self,
        repository: ReliabilityProjectionRepository,
        *,
        formal_status_file: Path,
        burn_status_file: Path,
    ) -> None:
        self.repository = repository
        self.formal_status_file = Path(formal_status_file)
        self.burn_status_file = Path(burn_status_file)

    def run_once(self, *, received_at: str | None = None) -> ReliabilityProjectionRun:
        projections: list[SLIProjection] = []
        rejections: list[ObservationRejection] = []
        self._read_formal(received_at, projections, rejections)
        self._read_fast(received_at, projections, rejections)
        inserted = 0
        duplicates = 0
        with self.repository.transaction() as connection:
            for item in projections:
                if self.repository.save_sli_projection(item, connection=connection):
                    inserted += 1
                else:
                    duplicates += 1
            for item in rejections:
                self.repository.append_rejection(item, connection=connection)
        attempted_keys = {
            (item.objective_id, item.assessment_scope, item.window) for item in projections
        }
        canonical = tuple(
            item
            for item in self.repository.current_sli_projections()
            if (item.objective_id, item.assessment_scope, item.window) in attempted_keys
        )
        status = "good" if projections and not rejections else ("unknown" if projections else "bad")
        self.repository.set_component_health(
            "reliability_projector",
            status,
            f"projections={len(projections)} rejections={len(rejections)}",
            now_ts=(
                int(parse_utc(received_at).timestamp())
                if received_at is not None
                else int(time.time())
            ),
        )
        return ReliabilityProjectionRun(canonical, tuple(rejections), inserted, duplicates)

    def _boundary_ok(
        self,
        payload: Mapping[str, Any],
        *,
        source: str,
        received_at: str,
        sha256: str,
        rejections: list[ObservationRejection],
    ) -> bool:
        if payload.get("no_automatic_recovery") is True:
            return True
        rejections.append(
            ObservationRejection.create(
                source=source,
                reason_code="reliability_recovery_boundary_invalid",
                detail="source does not assert no_automatic_recovery=true",
                received_at=received_at,
                payload_sha256=sha256,
            )
        )
        return False

    @staticmethod
    def _fresh_source_timestamp(
        checked_at: str,
        *,
        received_at: str,
        max_age_sec: int,
        source: str,
        sha256: str,
        rejections: list[ObservationRejection],
    ) -> bool:
        age_sec = unix_ts(received_at) - unix_ts(checked_at)
        if 0 <= age_sec <= max(1, int(max_age_sec)):
            return True
        rejections.append(
            ObservationRejection.create(
                source=source,
                reason_code="reliability_source_stale",
                detail=(
                    f"source checked_at age {age_sec}s exceeds "
                    f"the {max(1, int(max_age_sec))}s projection boundary"
                ),
                received_at=received_at,
                payload_sha256=sha256,
            )
        )
        return False

    def _read_formal(
        self,
        received_at: str | None,
        projections: list[SLIProjection],
        rejections: list[ObservationRejection],
    ) -> None:
        source = "reliability_formal_projection"
        snapshot = read_source(
            self.formal_status_file,
            source=source,
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None or not self._boundary_ok(
            snapshot.payload,
            source=source,
            received_at=snapshot.received_at if snapshot is not None else received_at or utc_text(int(time.time())),
            sha256=snapshot.sha256,
            rejections=rejections,
        ):
            return
        receipt = snapshot.received_at
        checked_at = source_timestamp(
            snapshot,
            ("checked_at_utc",),
            source=source,
            received_at=receipt,
            rejections=rejections,
        )
        gates_raw = snapshot.payload.get("formal_gates")
        gates = gates_raw if isinstance(gates_raw, Mapping) else {}
        if checked_at is None or not self._fresh_source_timestamp(
            checked_at,
            received_at=receipt,
            max_age_sec=FORMAL_SOURCE_MAX_AGE_SEC,
            source=source,
            sha256=snapshot.sha256,
            rejections=rejections,
        ):
            return
        for objective_id, spec in FORMAL_OBJECTIVES.items():
            raw_gate = gates.get(objective_id)
            if isinstance(raw_gate, Mapping):
                projections.append(
                    _formal_projection(
                        spec,
                        raw_gate,
                        source_sha256=snapshot.sha256,
                        source_checked_at=checked_at,
                        evaluated_at=receipt,
                    )
                )
            else:
                rejections.append(
                    ObservationRejection.create(
                        source=source,
                        reason_code="formal_objective_missing",
                        detail=f"formal objective missing: {objective_id}",
                        received_at=receipt,
                        payload_sha256=snapshot.sha256,
                    )
                )

    def _read_fast(
        self,
        received_at: str | None,
        projections: list[SLIProjection],
        rejections: list[ObservationRejection],
    ) -> None:
        source = "reliability_fast_projection"
        snapshot = read_source(
            self.burn_status_file,
            source=source,
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None or not self._boundary_ok(
            snapshot.payload,
            source=source,
            received_at=snapshot.received_at if snapshot is not None else received_at or utc_text(int(time.time())),
            sha256=snapshot.sha256,
            rejections=rejections,
        ):
            return
        receipt = snapshot.received_at
        checked_at = source_timestamp(
            snapshot,
            ("checked_at_utc",),
            source=source,
            received_at=receipt,
            rejections=rejections,
        )
        feedback_root = snapshot.payload.get("fast_feedback")
        feedback_by_name = feedback_root if isinstance(feedback_root, Mapping) else {}
        feedback = feedback_by_name.get("youtube_input_quality")
        if checked_at is None or not self._fresh_source_timestamp(
            checked_at,
            received_at=receipt,
            max_age_sec=FAST_SOURCE_MAX_AGE_SEC,
            source=source,
            sha256=snapshot.sha256,
            rejections=rejections,
        ):
            return
        if isinstance(feedback, Mapping):
            projections.append(
                _fast_projection(
                    feedback,
                    source_sha256=snapshot.sha256,
                    source_checked_at=checked_at,
                    evaluated_at=receipt,
                )
            )
        else:
            rejections.append(
                ObservationRejection.create(
                    source=source,
                    reason_code="fast_objective_missing",
                    detail="fast objective missing: youtube_input_quality",
                    received_at=receipt,
                    payload_sha256=snapshot.sha256,
                )
            )
