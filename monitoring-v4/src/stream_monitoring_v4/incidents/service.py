from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.notification import NotificationIntent
from stream_contracts.monitoring_v4.time import unix_ts, utc_text

from stream_monitoring_v4.notifications.intent import build_notification_intents
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.storage.ports import IncidentRepository

from .engine import CandidateState, evaluate_current
from .policy import IncidentPolicy


@dataclass(frozen=True)
class ProcessResult:
    duplicate: bool
    episode: IncidentEpisode | None
    transition: IncidentTransition | None
    intents: tuple[NotificationIntent, ...]


class IncidentService:
    def __init__(
        self,
        repository: IncidentRepository,
        policies: Mapping[str, IncidentPolicy],
        route_policy: RoutePolicy,
        *,
        delivery_epoch_id: str | None = None,
    ) -> None:
        self.repository = repository
        self.policies = dict(policies)
        self.route_policy = route_policy
        self.delivery_epoch_id = delivery_epoch_id

    def process(self, current: DomainCurrent, *, now_at: str) -> ProcessResult:
        effective_now_at = utc_text(
            max(unix_ts(now_at), unix_ts(current.reduced_at))
        )
        with self.repository.transaction() as connection:
            if self.repository.current_was_processed(current.snapshot_id, connection=connection):
                return ProcessResult(True, None, None, ())
            # A transition/candidate must never outlive the immutable current
            # evidence it names. The reducer normally inserted this row first;
            # direct/replayed service use is made equally safe here without
            # changing domain_current ownership.
            self.repository.append_current_snapshot(current, connection=connection)
            episode = self.repository.active_episode(current.domain, connection=connection)
            raw_candidate = self.repository.candidate(current.domain, connection=connection)
            candidate = CandidateState.from_dict(raw_candidate) if raw_candidate else None
            evaluation = evaluate_current(
                current,
                self.policies[current.domain],
                now_at=effective_now_at,
                active_episode=episode,
                pending_candidate=candidate,
            )
            if evaluation.candidate is None:
                self.repository.delete_candidate(current.domain, connection=connection)
            else:
                self.repository.save_candidate(evaluation.candidate.to_dict(), connection=connection)
            if evaluation.episode is not None:
                self.repository.save_episode(evaluation.episode, connection=connection)
            intents: tuple[NotificationIntent, ...] = ()
            if evaluation.transition is not None:
                inserted = self.repository.append_transition(evaluation.transition, connection=connection)
                if inserted and evaluation.episode is not None:
                    intents = build_notification_intents(
                        evaluation.transition,
                        evaluation.episode,
                        self.route_policy,
                    )
                    for intent in intents:
                        self.repository.append_intent(
                            intent,
                            connection=connection,
                            delivery_epoch_id=self.delivery_epoch_id,
                        )
            self.repository.mark_current_processed(
                current.snapshot_id,
                current.domain,
                processed_at=effective_now_at,
                connection=connection,
            )
            return ProcessResult(False, evaluation.episode, evaluation.transition, intents)
