from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping, Sequence

from ._decode import (
    boolean,
    contract_object,
    object_value,
    optional_number,
    string_array,
    text,
)
from .ids import json_copy, payload_sha256, stable_id, validate_id
from .observation import _name
from .time import parse_utc, require_not_before


ASSESSMENT_SCOPES = frozenset({"formal", "fast", "trend", "supporting"})
COMPLIANCE_STATES = frozenset({"met", "breached", "unknown"})


def _optional_nonnegative(value: float | int | None, *, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a number or null")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{field} must be finite and non-negative")
    return number


def _optional_percentage(value: float | int | None, *, field: str) -> float | None:
    number = _optional_nonnegative(value, field=field)
    if number is not None and number > 100:
        raise ValueError(f"{field} must be between 0 and 100")
    return number


@dataclass(frozen=True)
class SLIProjection:
    SCHEMA: ClassVar[str] = "monitoring_v4.sli_projection.v1"

    projection_id: str
    objective_id: str
    window: str
    assessment_scope: str
    is_official_window: bool
    observed: float | None
    eligible: float | None
    bad: float | None
    missing: float | None
    coverage_pct: float | None
    source_freshness_pct: float | None
    source_disagreement: bool
    compliance_status: str
    measurement_unknown_reasons: tuple[str, ...]
    window_start: str
    window_end: str
    evaluated_at: str
    policy_revision: str
    evidence_ids: tuple[str, ...]
    no_automatic_recovery: bool
    payload_sha256: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        validate_id(self.projection_id, field="projection_id")
        _name(self.objective_id, field="objective_id")
        _name(self.window, field="window")
        _name(self.assessment_scope, field="assessment_scope", allowed=ASSESSMENT_SCOPES)
        if not isinstance(self.is_official_window, bool):
            raise ValueError("is_official_window must be boolean")
        if self.assessment_scope == "formal" and not self.is_official_window:
            raise ValueError("formal assessment must use an official window")
        if self.assessment_scope != "formal" and self.compliance_status != "unknown":
            raise ValueError("non-formal assessment cannot assert compliance")
        object.__setattr__(self, "observed", _optional_nonnegative(self.observed, field="observed"))
        object.__setattr__(self, "eligible", _optional_nonnegative(self.eligible, field="eligible"))
        object.__setattr__(self, "bad", _optional_nonnegative(self.bad, field="bad"))
        object.__setattr__(self, "missing", _optional_nonnegative(self.missing, field="missing"))
        object.__setattr__(
            self,
            "coverage_pct",
            _optional_percentage(self.coverage_pct, field="coverage_pct"),
        )
        object.__setattr__(
            self,
            "source_freshness_pct",
            _optional_percentage(self.source_freshness_pct, field="source_freshness_pct"),
        )
        if not isinstance(self.source_disagreement, bool):
            raise ValueError("source_disagreement must be boolean")
        _name(self.compliance_status, field="compliance_status", allowed=COMPLIANCE_STATES)
        for reason in self.measurement_unknown_reasons:
            _name(reason, field="measurement_unknown_reason")
        parse_utc(self.window_start, field="window_start")
        parse_utc(self.window_end, field="window_end")
        parse_utc(self.evaluated_at, field="evaluated_at")
        require_not_before(
            self.window_end,
            self.window_start,
            later_field="window_end",
            earlier_field="window_start",
        )
        require_not_before(
            self.evaluated_at,
            self.window_end,
            later_field="evaluated_at",
            earlier_field="window_end",
        )
        if not self.policy_revision or len(self.policy_revision) > 200:
            raise ValueError("policy_revision must contain 1-200 characters")
        for ident in self.evidence_ids:
            validate_id(ident, field="evidence_id")
        if self.no_automatic_recovery is not True:
            raise ValueError("SLI projection must not grant automatic recovery")
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be an object")
        copied = json_copy(self.payload)
        if payload_sha256(copied) != self.payload_sha256:
            raise ValueError("payload_sha256 does not match payload")
        object.__setattr__(self, "measurement_unknown_reasons", tuple(self.measurement_unknown_reasons))
        object.__setattr__(self, "evidence_ids", tuple(self.evidence_ids))
        object.__setattr__(self, "payload", copied)

    @classmethod
    def create(
        cls,
        *,
        objective_id: str,
        window: str,
        assessment_scope: str,
        is_official_window: bool,
        observed: float | int | None,
        eligible: float | int | None,
        bad: float | int | None,
        missing: float | int | None,
        coverage_pct: float | int | None,
        source_freshness_pct: float | int | None,
        source_disagreement: bool,
        compliance_status: str,
        measurement_unknown_reasons: Sequence[str],
        window_start: str,
        window_end: str,
        evaluated_at: str,
        policy_revision: str,
        evidence_ids: Sequence[str],
        payload: Mapping[str, Any],
    ) -> "SLIProjection":
        reasons = tuple(sorted(set(measurement_unknown_reasons)))
        evidence = tuple(sorted(set(evidence_ids)))
        copied = json_copy(payload)
        ident = stable_id(
            "sli",
            cls.SCHEMA,
            objective_id,
            window,
            assessment_scope,
            is_official_window,
            observed,
            eligible,
            bad,
            missing,
            coverage_pct,
            source_freshness_pct,
            source_disagreement,
            compliance_status,
            reasons,
            window_start,
            window_end,
            policy_revision,
            evidence,
            copied,
        )
        return cls(
            ident,
            objective_id,
            window,
            assessment_scope,
            is_official_window,
            observed,
            eligible,
            bad,
            missing,
            coverage_pct,
            source_freshness_pct,
            source_disagreement,
            compliance_status,
            reasons,
            window_start,
            window_end,
            evaluated_at,
            policy_revision,
            evidence,
            True,
            payload_sha256(copied),
            copied,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "projection_id": self.projection_id,
            "objective_id": self.objective_id,
            "window": self.window,
            "assessment_scope": self.assessment_scope,
            "is_official_window": self.is_official_window,
            "observed": self.observed,
            "eligible": self.eligible,
            "bad": self.bad,
            "missing": self.missing,
            "coverage_pct": self.coverage_pct,
            "source_freshness_pct": self.source_freshness_pct,
            "source_disagreement": self.source_disagreement,
            "compliance_status": self.compliance_status,
            "measurement_unknown_reasons": list(self.measurement_unknown_reasons),
            "window_start": self.window_start,
            "window_end": self.window_end,
            "evaluated_at": self.evaluated_at,
            "policy_revision": self.policy_revision,
            "evidence_ids": list(self.evidence_ids),
            "no_automatic_recovery": self.no_automatic_recovery,
            "payload_sha256": self.payload_sha256,
            "payload": json_copy(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SLIProjection":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset(
                {
                    "projection_id",
                    "objective_id",
                    "window",
                    "assessment_scope",
                    "is_official_window",
                    "observed",
                    "eligible",
                    "bad",
                    "missing",
                    "coverage_pct",
                    "source_freshness_pct",
                    "source_disagreement",
                    "compliance_status",
                    "measurement_unknown_reasons",
                    "window_start",
                    "window_end",
                    "evaluated_at",
                    "policy_revision",
                    "evidence_ids",
                    "no_automatic_recovery",
                    "payload_sha256",
                    "payload",
                }
            ),
        )
        return cls(
            projection_id=text(raw, "projection_id"),
            objective_id=text(raw, "objective_id"),
            window=text(raw, "window"),
            assessment_scope=text(raw, "assessment_scope"),
            is_official_window=boolean(raw, "is_official_window"),
            observed=optional_number(raw, "observed"),
            eligible=optional_number(raw, "eligible"),
            bad=optional_number(raw, "bad"),
            missing=optional_number(raw, "missing"),
            coverage_pct=optional_number(raw, "coverage_pct"),
            source_freshness_pct=optional_number(raw, "source_freshness_pct"),
            source_disagreement=boolean(raw, "source_disagreement"),
            compliance_status=text(raw, "compliance_status"),
            measurement_unknown_reasons=string_array(raw, "measurement_unknown_reasons"),
            window_start=text(raw, "window_start"),
            window_end=text(raw, "window_end"),
            evaluated_at=text(raw, "evaluated_at"),
            policy_revision=text(raw, "policy_revision"),
            evidence_ids=string_array(raw, "evidence_ids"),
            no_automatic_recovery=boolean(raw, "no_automatic_recovery"),
            payload_sha256=text(raw, "payload_sha256"),
            payload=object_value(raw, "payload"),
        )
