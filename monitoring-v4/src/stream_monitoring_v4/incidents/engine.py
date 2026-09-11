from __future__ import annotations

from dataclasses import dataclass, replace

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.time import unix_ts, utc_text

from .policy import IncidentPolicy


SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}


@dataclass(frozen=True)
class CandidateState:
    domain: str
    state: str
    first_seen_at: str
    last_seen_at: str
    samples: int
    snapshot_id: str
    reason_codes: tuple[str, ...]

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> "CandidateState":
        return cls(
            domain=str(value["domain"]),
            state=str(value["state"]),
            first_seen_at=str(value["first_seen_at"]),
            last_seen_at=str(value["last_seen_at"]),
            samples=int(value["samples"]),
            snapshot_id=str(value["snapshot_id"]),
            reason_codes=tuple(str(item) for item in value.get("reason_codes", ())),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "state": self.state,
            "first_seen_at": self.first_seen_at,
            "last_seen_at": self.last_seen_at,
            "samples": self.samples,
            "snapshot_id": self.snapshot_id,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class IncidentEvaluation:
    candidate: CandidateState | None
    episode: IncidentEpisode | None
    transition: IncidentTransition | None


def _candidate(current: DomainCurrent, previous: CandidateState | None, *, now_at: str) -> CandidateState:
    reasons = tuple(current.reason_codes)
    if previous is None or previous.state != current.state or previous.reason_codes != reasons:
        return CandidateState(
            current.domain,
            current.state,
            current.observed_at,
            current.observed_at,
            1,
            current.snapshot_id,
            reasons,
        )
    if previous.snapshot_id == current.snapshot_id:
        return previous
    return CandidateState(
        current.domain,
        current.state,
        previous.first_seen_at,
        current.observed_at,
        previous.samples + 1,
        current.snapshot_id,
        reasons,
    )


def _transition(
    episode: IncidentEpisode,
    current: DomainCurrent,
    *,
    phase: str,
    severity: str,
    now_at: str,
) -> IncidentTransition:
    return IncidentTransition.create(
        episode_id=episode.episode_id,
        domain=current.domain,
        phase=phase,
        severity=severity,
        occurred_at=now_at,
        current_snapshot_id=current.snapshot_id,
        summary=episode.summary,
        reason_codes=current.reason_codes,
    )


def evaluate_current(
    current: DomainCurrent,
    policy: IncidentPolicy,
    *,
    now_at: str,
    active_episode: IncidentEpisode | None,
    pending_candidate: CandidateState | None,
) -> IncidentEvaluation:
    if current.domain != policy.domain:
        raise ValueError("current domain and incident policy do not match")

    if policy.suppress_during_maintenance and current.payload.get("maintenance") is True:
        return IncidentEvaluation(None, active_episode, None)

    if not policy.is_actionable(current.state, current.reason_codes):
        if active_episode is None:
            return IncidentEvaluation(None, None, None)
        closed = replace(
            active_episode,
            status="closed",
            closed_at=now_at,
            last_transition_at=now_at,
            next_notification_at="",
            reason_codes=tuple(current.reason_codes),
        )
        return IncidentEvaluation(
            None,
            closed,
            _transition(closed, current, phase="recovered", severity="info", now_at=now_at),
        )

    if active_episode is None:
        candidate = _candidate(current, pending_candidate, now_at=now_at)
        min_samples, min_duration, severity = policy.threshold(current.state)
        if current.payload.get("planned_rollout") is True:
            severity = "info"
        elapsed = max(0, unix_ts(now_at) - unix_ts(candidate.first_seen_at))
        if candidate.samples < min_samples or elapsed < min_duration:
            return IncidentEvaluation(candidate, None, None)
        repeat_interval = policy.repeat_interval(current.state, current.reason_codes)
        episode = IncidentEpisode.open(
            domain=current.domain,
            severity=severity,
            opened_at=candidate.first_seen_at,
            summary=policy.summary,
            reason_codes=current.reason_codes,
            current_snapshot_id=current.snapshot_id,
            current_state=current.state,
            next_notification_at=utc_text(unix_ts(now_at) + repeat_interval),
            policy_revision=policy.revision,
        )
        episode = replace(
            episode,
            bad_samples=candidate.samples if current.state == "bad" else 0,
            unknown_samples=candidate.samples if current.state == "unknown" else 0,
            last_bad_at=current.observed_at,
            last_transition_at=now_at,
        )
        return IncidentEvaluation(
            None,
            episode,
            _transition(episode, current, phase="detected", severity=severity, now_at=now_at),
        )

    if pending_candidate and pending_candidate.snapshot_id == current.snapshot_id:
        return IncidentEvaluation(None, active_episode, None)
    updated = replace(
        active_episode,
        last_bad_at=current.observed_at,
        bad_samples=active_episode.bad_samples + (1 if current.state == "bad" else 0),
        unknown_samples=active_episode.unknown_samples + (1 if current.state == "unknown" else 0),
        reason_codes=tuple(current.reason_codes),
    )
    repeat_interval = policy.repeat_interval(current.state, current.reason_codes)
    desired_severity = policy.threshold(current.state)[2]
    if current.payload.get("planned_rollout") is True:
        desired_severity = "info"
    if SEVERITY_RANK[desired_severity] > SEVERITY_RANK[active_episode.severity]:
        updated = replace(
            updated,
            severity=desired_severity,
            last_transition_at=now_at,
            next_notification_at=utc_text(unix_ts(now_at) + repeat_interval),
        )
        return IncidentEvaluation(
            None,
            updated,
            _transition(updated, current, phase="repeat", severity=desired_severity, now_at=now_at),
        )
    due_ts = unix_ts(active_episode.next_notification_at)
    # A diagnostic/unknown family may have a much slower repeat cadence than a
    # later explicit bad state. Never leave the old long deadline in place when
    # the currently actionable evidence requires faster feedback.
    clamped_due_ts = min(due_ts, unix_ts(now_at) + repeat_interval)
    if clamped_due_ts != due_ts:
        due_ts = clamped_due_ts
        updated = replace(updated, next_notification_at=utc_text(due_ts))
    if unix_ts(now_at) < due_ts:
        return IncidentEvaluation(None, updated, None)
    severity = active_episode.severity
    next_due_ts = due_ts
    while next_due_ts <= unix_ts(now_at):
        next_due_ts += repeat_interval
    updated = replace(
        updated,
        severity=severity,
        last_transition_at=now_at,
        next_notification_at=utc_text(next_due_ts),
    )
    return IncidentEvaluation(
        None,
        updated,
        _transition(updated, current, phase="repeat", severity=severity, now_at=now_at),
    )
