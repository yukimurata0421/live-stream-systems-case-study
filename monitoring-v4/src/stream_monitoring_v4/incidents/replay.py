from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition

from .engine import CandidateState, evaluate_current
from .policy import IncidentPolicy


@dataclass(frozen=True)
class ReplayStep:
    current: DomainCurrent
    now_at: str


def replay(steps: Iterable[ReplayStep], policy: IncidentPolicy) -> list[IncidentTransition]:
    candidate: CandidateState | None = None
    episode: IncidentEpisode | None = None
    transitions: list[IncidentTransition] = []
    processed: set[str] = set()
    for step in steps:
        if step.current.snapshot_id in processed:
            continue
        processed.add(step.current.snapshot_id)
        evaluation = evaluate_current(
            step.current,
            policy,
            now_at=step.now_at,
            active_episode=episode if episode and episode.status == "active" else None,
            pending_candidate=candidate,
        )
        candidate = evaluation.candidate
        episode = evaluation.episode or episode
        if episode and episode.status == "closed":
            episode = None
        if evaluation.transition:
            transitions.append(evaluation.transition)
    return transitions


def phases(steps: Iterable[ReplayStep], policy: IncidentPolicy) -> list[str]:
    return [item.phase for item in replay(steps, policy)]
