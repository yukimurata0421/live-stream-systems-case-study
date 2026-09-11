from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping

from ._decode import contract_object, integer, object_array, text
from .ids import stable_id, validate_id
from .time import parse_utc, require_not_before, unix_ts


_SOURCE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,199}$")
_ROLLOUT_REASON = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,119}$")


def _source_id(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not _SOURCE_ID.fullmatch(value):
        raise ValueError(f"invalid {field}: {value!r}")
    return value


@dataclass(frozen=True)
class RuntimeLifecycleEvent:
    """Sanitized, completed runtime edge event projected from the v3 JSONL ledger."""

    SCHEMA: ClassVar[str] = "monitoring_v4.runtime_lifecycle_event.v1"

    event_id: str
    event_type: str
    opened_at: str
    recovered_at: str
    run_id: str
    restart_count: int
    exit_code: int
    delay_sec: int
    scheduled_event_id: str
    recovered_event_id: str

    def __post_init__(self) -> None:
        _source_id(self.event_id, field="event_id")
        if self.event_type != "ffmpeg_auto_recovered":
            raise ValueError(f"unsupported runtime lifecycle event_type: {self.event_type!r}")
        parse_utc(self.opened_at, field="opened_at")
        parse_utc(self.recovered_at, field="recovered_at")
        require_not_before(
            self.recovered_at,
            self.opened_at,
            later_field="recovered_at",
            earlier_field="opened_at",
        )
        if unix_ts(self.recovered_at) - unix_ts(self.opened_at) > 3600:
            raise ValueError("runtime lifecycle recovery exceeds one hour")
        _source_id(self.run_id, field="run_id")
        if type(self.restart_count) is not int or self.restart_count < 1:
            raise ValueError("restart_count must be positive")
        if type(self.exit_code) is not int or not -999 <= self.exit_code <= 999:
            raise ValueError("exit_code is outside the bounded contract")
        if type(self.delay_sec) is not int or not 0 <= self.delay_sec <= 3600:
            raise ValueError("delay_sec is outside the bounded contract")
        _source_id(self.scheduled_event_id, field="scheduled_event_id")
        _source_id(self.recovered_event_id, field="recovered_event_id")

    @property
    def duration_sec(self) -> int:
        return unix_ts(self.recovered_at) - unix_ts(self.opened_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "opened_at": self.opened_at,
            "recovered_at": self.recovered_at,
            "run_id": self.run_id,
            "restart_count": self.restart_count,
            "exit_code": self.exit_code,
            "delay_sec": self.delay_sec,
            "scheduled_event_id": self.scheduled_event_id,
            "recovered_event_id": self.recovered_event_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimeLifecycleEvent":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset(
                {
                    "event_id",
                    "event_type",
                    "opened_at",
                    "recovered_at",
                    "run_id",
                    "restart_count",
                    "exit_code",
                    "delay_sec",
                    "scheduled_event_id",
                    "recovered_event_id",
                }
            ),
        )
        return cls(
            event_id=text(raw, "event_id"),
            event_type=text(raw, "event_type"),
            opened_at=text(raw, "opened_at"),
            recovered_at=text(raw, "recovered_at"),
            run_id=text(raw, "run_id"),
            restart_count=integer(raw, "restart_count"),
            exit_code=integer(raw, "exit_code"),
            delay_sec=integer(raw, "delay_sec"),
            scheduled_event_id=text(raw, "scheduled_event_id"),
            recovered_event_id=text(raw, "recovered_event_id"),
        )


@dataclass(frozen=True)
class RuntimeLifecycleProjection:
    SCHEMA: ClassVar[str] = "monitoring_v4.runtime_lifecycle_projection.v1"

    events: tuple[RuntimeLifecycleEvent, ...]

    def __post_init__(self) -> None:
        events = tuple(self.events)
        if len(events) > 32:
            raise ValueError("runtime lifecycle projection exceeds 32 events")
        if len({item.event_id for item in events}) != len(events):
            raise ValueError("runtime lifecycle projection contains duplicate event_id")
        if list(events) != sorted(events, key=lambda item: (item.opened_at, item.event_id)):
            raise ValueError("runtime lifecycle projection events must be chronologically sorted")
        object.__setattr__(self, "events", events)

    def to_dict(self) -> dict[str, Any]:
        return {"schema": self.SCHEMA, "events": [item.to_dict() for item in self.events]}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimeLifecycleProjection":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset({"events"}),
        )
        raw_events = object_array(raw, "events")
        return cls(
            tuple(
                RuntimeLifecycleEvent.from_dict(item)
                for item in raw_events
            )
        )


@dataclass(frozen=True)
class VerifiedRolloutEvidence:
    SCHEMA: ClassVar[str] = "monitoring_v4.verified_rollout_evidence.v1"

    evidence_id: str
    rollout_id: str
    planned_at: str
    expires_at: str
    pod_started_at: str
    pod_uid: str
    reason: str
    workload: str
    verification: str

    def __post_init__(self) -> None:
        validate_id(self.evidence_id, field="evidence_id")
        _source_id(self.rollout_id, field="rollout_id")
        parse_utc(self.planned_at, field="planned_at")
        parse_utc(self.expires_at, field="expires_at")
        parse_utc(self.pod_started_at, field="pod_started_at")
        require_not_before(
            self.expires_at,
            self.planned_at,
            later_field="expires_at",
            earlier_field="planned_at",
        )
        if not 300 <= unix_ts(self.expires_at) - unix_ts(self.planned_at) <= 3600:
            raise ValueError("verified rollout window must be between 300 and 3600 seconds")
        if not unix_ts(self.planned_at) - 60 <= unix_ts(self.pod_started_at) <= unix_ts(self.expires_at):
            raise ValueError("pod_started_at is outside the verified rollout window")
        _source_id(self.pod_uid, field="pod_uid")
        if not _ROLLOUT_REASON.fullmatch(self.reason):
            raise ValueError(f"invalid rollout reason: {self.reason!r}")
        if self.workload != "deployment/stream-v3-runtime":
            raise ValueError(f"unsupported rollout workload: {self.workload!r}")
        if self.verification != "annotation_window_and_pod_start":
            raise ValueError(f"unsupported rollout verification: {self.verification!r}")
        expected_id = stable_id(
            "evd",
            self.SCHEMA,
            self.rollout_id,
            self.planned_at,
            self.expires_at,
            self.pod_started_at,
            self.pod_uid,
        )
        if self.evidence_id != expected_id:
            raise ValueError("evidence_id does not match rollout evidence content")

    @classmethod
    def create(
        cls,
        *,
        rollout_id: str,
        planned_at: str,
        expires_at: str,
        pod_started_at: str,
        pod_uid: str,
        reason: str,
    ) -> "VerifiedRolloutEvidence":
        evidence_id = stable_id(
            "evd",
            cls.SCHEMA,
            rollout_id,
            planned_at,
            expires_at,
            pod_started_at,
            pod_uid,
        )
        return cls(
            evidence_id=evidence_id,
            rollout_id=rollout_id,
            planned_at=planned_at,
            expires_at=expires_at,
            pod_started_at=pod_started_at,
            pod_uid=pod_uid,
            reason=reason,
            workload="deployment/stream-v3-runtime",
            verification="annotation_window_and_pod_start",
        )

    def contains(self, timestamp: str) -> bool:
        value = unix_ts(timestamp)
        return unix_ts(self.planned_at) - 60 <= value <= unix_ts(self.expires_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "evidence_id": self.evidence_id,
            "rollout_id": self.rollout_id,
            "planned_at": self.planned_at,
            "expires_at": self.expires_at,
            "pod_started_at": self.pod_started_at,
            "pod_uid": self.pod_uid,
            "reason": self.reason,
            "workload": self.workload,
            "verification": self.verification,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "VerifiedRolloutEvidence":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset(
                {
                    "evidence_id",
                    "rollout_id",
                    "planned_at",
                    "expires_at",
                    "pod_started_at",
                    "pod_uid",
                    "reason",
                    "workload",
                    "verification",
                }
            ),
        )
        return cls(
            evidence_id=text(raw, "evidence_id"),
            rollout_id=text(raw, "rollout_id"),
            planned_at=text(raw, "planned_at"),
            expires_at=text(raw, "expires_at"),
            pod_started_at=text(raw, "pod_started_at"),
            pod_uid=text(raw, "pod_uid"),
            reason=text(raw, "reason"),
            workload=text(raw, "workload"),
            verification=text(raw, "verification"),
        )


@dataclass(frozen=True)
class RuntimeRolloutProjection:
    SCHEMA: ClassVar[str] = "monitoring_v4.runtime_rollout_projection.v1"

    observed_at: str
    evidence: tuple[VerifiedRolloutEvidence, ...]

    def __post_init__(self) -> None:
        parse_utc(self.observed_at, field="observed_at")
        observed_ts = unix_ts(self.observed_at)
        evidence = tuple(self.evidence)
        if len(evidence) > 8:
            raise ValueError("runtime rollout projection exceeds 8 entries")
        if len({item.evidence_id for item in evidence}) != len(evidence):
            raise ValueError("runtime rollout projection contains duplicate evidence_id")
        if list(evidence) != sorted(
            evidence, key=lambda item: (item.planned_at, item.evidence_id)
        ):
            raise ValueError("runtime rollout projection evidence must be chronologically sorted")
        if any(unix_ts(item.planned_at) > observed_ts for item in evidence):
            raise ValueError("planned rollout evidence is newer than its projection")
        if any(unix_ts(item.pod_started_at) > observed_ts for item in evidence):
            raise ValueError("rollout pod start is newer than its projection")
        object.__setattr__(self, "evidence", evidence)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "observed_at": self.observed_at,
            "evidence": [item.to_dict() for item in self.evidence],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimeRolloutProjection":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset({"observed_at", "evidence"}),
        )
        raw_evidence = object_array(raw, "evidence")
        return cls(
            observed_at=text(raw, "observed_at"),
            evidence=tuple(
                VerifiedRolloutEvidence.from_dict(item)
                for item in raw_evidence
            ),
        )
