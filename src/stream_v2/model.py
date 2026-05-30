from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

State = Literal["healthy", "degraded", "failed", "recovering", "unknown"]
Confidence = Literal["high", "medium", "low", "unknown"]
SubsystemName = Literal["rendering", "music", "local_delivery", "youtube_lifecycle", "monitoring"]

DESTRUCTIVE_ACTIONS = {
    "restart_stream",
    "force_current_broadcast_live",
    "bind_current_stream",
    "transition_current_broadcast",
    "create_replacement_broadcast",
    "cleanup_stale_broadcast",
}
YOUTUBE_DESTRUCTIVE_ACTIONS = {
    "force_current_broadcast_live",
    "bind_current_stream",
    "transition_current_broadcast",
    "create_replacement_broadcast",
    "cleanup_stale_broadcast",
}


@dataclass(frozen=True)
class EvidenceRecord:
    source: str
    event_id: str
    subsystem: str
    name: str
    verdict: str
    observed_at_utc: str
    age_sec: Optional[float]
    ttl_sec: float
    confidence: Confidence
    target: dict[str, Any] = field(default_factory=dict)
    raw_ref: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "event_id": self.event_id,
            "subsystem": self.subsystem,
            "name": self.name,
            "verdict": self.verdict,
            "observed_at_utc": self.observed_at_utc,
            "age_sec": None if self.age_sec is None else round(self.age_sec, 3),
            "ttl_sec": self.ttl_sec,
            "confidence": self.confidence,
            "target": self.target,
            "raw_ref": self.raw_ref,
        }


@dataclass(frozen=True)
class SubsystemStatus:
    name: str
    state: State
    confidence: Confidence
    evidence: list[str] = field(default_factory=list)
    evidence_records: list[EvidenceRecord] = field(default_factory=list)
    evidence_age_sec: Optional[float] = None
    last_ok_ts_utc: str = ""
    caused_by_subsystems: list[str] = field(default_factory=list)
    affects_subsystems: list[str] = field(default_factory=list)
    recommended_action: str = "none"
    blocked_by: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "state": self.state,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "evidence_records": [ev.to_dict() for ev in self.evidence_records],
            "evidence_age_sec": None if self.evidence_age_sec is None else round(self.evidence_age_sec, 3),
            "last_ok_ts_utc": self.last_ok_ts_utc,
            "caused_by_subsystems": self.caused_by_subsystems,
            "affects_subsystems": self.affects_subsystems,
            "recommended_action": self.recommended_action,
            "blocked_by": self.blocked_by,
        }
        out.update(self.extra)
        return out


@dataclass(frozen=True)
class ReplacementPolicy:
    allowed: bool
    reason: str
    required_missing: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "required_missing": self.required_missing,
        }


@dataclass(frozen=True)
class OverallStatus:
    state: State
    stream_public_state: str
    expected_video_id: str
    expected_url_state: str
    degraded_subsystems: list[str]
    oldest_evidence_ts_utc: str
    consistency_window_sec: Optional[float]
    max_consistency_window_sec: float
    objective_sli: dict[str, Any]
    recommended_action: str
    action_scope: str
    action_reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "stream_public_state": self.stream_public_state,
            "expected_video_id": self.expected_video_id,
            "expected_url_state": self.expected_url_state,
            "degraded_subsystems": self.degraded_subsystems,
            "oldest_evidence_ts_utc": self.oldest_evidence_ts_utc,
            "consistency_window_sec": None if self.consistency_window_sec is None else round(self.consistency_window_sec, 3),
            "max_consistency_window_sec": self.max_consistency_window_sec,
            "objective_sli": self.objective_sli,
            "recommended_action": self.recommended_action,
            "action_scope": self.action_scope,
            "action_reason": self.action_reason,
        }


@dataclass(frozen=True)
class SubsystemsSnapshot:
    ts_utc: str
    schema_version: int
    run_id: str
    overall: OverallStatus
    rendering: SubsystemStatus
    music: SubsystemStatus
    local_delivery: SubsystemStatus
    youtube_lifecycle: SubsystemStatus
    monitoring: SubsystemStatus

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts_utc": self.ts_utc,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "overall": self.overall.to_dict(),
            "rendering": self.rendering.to_dict(),
            "music": self.music.to_dict(),
            "local_delivery": self.local_delivery.to_dict(),
            "youtube_lifecycle": self.youtube_lifecycle.to_dict(),
            "monitoring": self.monitoring.to_dict(),
        }

    @property
    def subsystems(self) -> dict[str, SubsystemStatus]:
        return {
            "rendering": self.rendering,
            "music": self.music,
            "local_delivery": self.local_delivery,
            "youtube_lifecycle": self.youtube_lifecycle,
            "monitoring": self.monitoring,
        }


@dataclass(frozen=True)
class ActionCandidate:
    action: str
    scope: str
    priority: int
    destructive_level: str
    would_preserve_url: bool
    preconditions_met: bool
    blocked_by: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "scope": self.scope,
            "priority": self.priority,
            "destructive_level": self.destructive_level,
            "would_preserve_url": self.would_preserve_url,
            "preconditions_met": self.preconditions_met,
            "blocked_by": self.blocked_by,
        }

    @property
    def is_destructive(self) -> bool:
        return self.action in DESTRUCTIVE_ACTIONS

    @property
    def is_youtube_destructive(self) -> bool:
        return self.action in YOUTUBE_DESTRUCTIVE_ACTIONS
