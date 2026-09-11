from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Mapping, Sequence

from ._decode import contract_object, integer, string_array, text
from .ids import stable_id, validate_id
from .observation import DOMAINS, _name
from .time import parse_utc, require_not_before


EPISODE_STATUSES = frozenset({"active", "closed"})
TRANSITION_PHASES = frozenset({"detected", "repeat", "recovered"})
SEVERITIES = frozenset({"info", "warning", "critical"})


@dataclass(frozen=True)
class IncidentEpisode:
    SCHEMA: ClassVar[str] = "monitoring_v4.incident_episode.v1"

    episode_id: str
    domain: str
    status: str
    severity: str
    opened_at: str
    last_bad_at: str
    closed_at: str
    bad_samples: int
    unknown_samples: int
    last_transition_at: str
    next_notification_at: str
    policy_revision: str
    summary: str
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        validate_id(self.episode_id, field="episode_id")
        _name(self.domain, field="domain", allowed=DOMAINS)
        _name(self.status, field="episode_status", allowed=EPISODE_STATUSES)
        _name(self.severity, field="severity", allowed=SEVERITIES)
        parse_utc(self.opened_at, field="opened_at")
        parse_utc(self.last_bad_at, field="last_bad_at")
        parse_utc(self.last_transition_at, field="last_transition_at")
        require_not_before(
            self.last_bad_at,
            self.opened_at,
            later_field="last_bad_at",
            earlier_field="opened_at",
        )
        if self.closed_at:
            parse_utc(self.closed_at, field="closed_at")
            require_not_before(
                self.closed_at,
                self.opened_at,
                later_field="closed_at",
                earlier_field="opened_at",
            )
        if self.status == "closed" and not self.closed_at:
            raise ValueError("closed episode requires closed_at")
        if self.status == "active" and self.closed_at:
            raise ValueError("active episode must not have closed_at")
        if self.status == "active":
            parse_utc(self.next_notification_at, field="next_notification_at")
            require_not_before(
                self.next_notification_at,
                self.last_transition_at,
                later_field="next_notification_at",
                earlier_field="last_transition_at",
            )
        elif self.next_notification_at:
            raise ValueError("closed episode must not have next_notification_at")
        if not self.policy_revision or len(self.policy_revision) > 200:
            raise ValueError("policy_revision must contain 1-200 characters")
        if type(self.bad_samples) is not int or type(self.unknown_samples) is not int:
            raise ValueError("sample counters must be integers")
        if self.bad_samples < 0 or self.unknown_samples < 0:
            raise ValueError("sample counters must be non-negative")
        if not self.summary or len(self.summary) > 300:
            raise ValueError("summary must contain 1-300 characters")
        for reason in self.reason_codes:
            _name(reason, field="reason_code")
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))

    @classmethod
    def open(
        cls,
        *,
        domain: str,
        severity: str,
        opened_at: str,
        summary: str,
        reason_codes: Sequence[str],
        current_snapshot_id: str,
        current_state: str,
        next_notification_at: str,
        policy_revision: str,
    ) -> "IncidentEpisode":
        ident = stable_id("inc", domain, opened_at, current_snapshot_id, policy_revision)
        return cls(
            episode_id=ident,
            domain=domain,
            status="active",
            severity=severity,
            opened_at=opened_at,
            last_bad_at=opened_at,
            closed_at="",
            bad_samples=1 if current_state == "bad" else 0,
            unknown_samples=1 if current_state == "unknown" else 0,
            last_transition_at=opened_at,
            next_notification_at=next_notification_at,
            policy_revision=policy_revision,
            summary=summary,
            reason_codes=tuple(reason_codes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "episode_id": self.episode_id,
            "domain": self.domain,
            "status": self.status,
            "severity": self.severity,
            "opened_at": self.opened_at,
            "last_bad_at": self.last_bad_at,
            "closed_at": self.closed_at,
            "bad_samples": self.bad_samples,
            "unknown_samples": self.unknown_samples,
            "last_transition_at": self.last_transition_at,
            "next_notification_at": self.next_notification_at,
            "policy_revision": self.policy_revision,
            "summary": self.summary,
            "reason_codes": list(self.reason_codes),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "IncidentEpisode":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset(
                {
                    "episode_id",
                    "domain",
                    "status",
                    "severity",
                    "opened_at",
                    "last_bad_at",
                    "closed_at",
                    "bad_samples",
                    "unknown_samples",
                    "last_transition_at",
                    "next_notification_at",
                    "policy_revision",
                    "summary",
                    "reason_codes",
                }
            ),
        )
        return cls(
            episode_id=text(raw, "episode_id"),
            domain=text(raw, "domain"),
            status=text(raw, "status"),
            severity=text(raw, "severity"),
            opened_at=text(raw, "opened_at"),
            last_bad_at=text(raw, "last_bad_at"),
            closed_at=text(raw, "closed_at"),
            bad_samples=integer(raw, "bad_samples"),
            unknown_samples=integer(raw, "unknown_samples"),
            last_transition_at=text(raw, "last_transition_at"),
            next_notification_at=text(raw, "next_notification_at"),
            policy_revision=text(raw, "policy_revision"),
            summary=text(raw, "summary"),
            reason_codes=string_array(raw, "reason_codes"),
        )


@dataclass(frozen=True)
class IncidentTransition:
    SCHEMA: ClassVar[str] = "monitoring_v4.incident_transition.v1"

    transition_id: str
    episode_id: str
    domain: str
    phase: str
    severity: str
    occurred_at: str
    current_snapshot_id: str
    summary: str
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        validate_id(self.transition_id, field="transition_id")
        validate_id(self.episode_id, field="episode_id")
        validate_id(self.current_snapshot_id, field="current_snapshot_id")
        _name(self.domain, field="domain", allowed=DOMAINS)
        _name(self.phase, field="transition_phase", allowed=TRANSITION_PHASES)
        _name(self.severity, field="severity", allowed=SEVERITIES)
        parse_utc(self.occurred_at, field="occurred_at")
        if not self.summary or len(self.summary) > 300:
            raise ValueError("summary must contain 1-300 characters")
        for reason in self.reason_codes:
            _name(reason, field="reason_code")
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))

    @classmethod
    def create(
        cls,
        *,
        episode_id: str,
        domain: str,
        phase: str,
        severity: str,
        occurred_at: str,
        current_snapshot_id: str,
        summary: str,
        reason_codes: Sequence[str],
    ) -> "IncidentTransition":
        ident = stable_id("trn", episode_id, phase, occurred_at, current_snapshot_id)
        return cls(
            ident,
            episode_id,
            domain,
            phase,
            severity,
            occurred_at,
            current_snapshot_id,
            summary,
            tuple(reason_codes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "transition_id": self.transition_id,
            "episode_id": self.episode_id,
            "domain": self.domain,
            "phase": self.phase,
            "severity": self.severity,
            "occurred_at": self.occurred_at,
            "current_snapshot_id": self.current_snapshot_id,
            "summary": self.summary,
            "reason_codes": list(self.reason_codes),
        }
