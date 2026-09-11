from __future__ import annotations

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.runtime_evidence import RuntimeLifecycleEvent
from stream_contracts.monitoring_v4.time import unix_ts

from stream_monitoring_v4.notifications.intent import build_notification_intents
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.storage.ports import RuntimeLifecycleRepository

from .service import ProcessResult


LIFECYCLE_POLICY_REVISION = "monitoring-v4-runtime-lifecycle-r4.1"


class RuntimeLifecycleIncidentService:
    """Create one recovery-only episode for a completed edge missed by polling.

    The event is historical evidence, never a replacement for delivery current.
    A recovery transition is emitted only after a delivery snapshot at or after
    the recovered edge verifies that current delivery is good.
    """

    def __init__(
        self,
        repository: RuntimeLifecycleRepository,
        route_policy: RoutePolicy,
        *,
        recent_sec: int = 1800,
        delivery_epoch_id: str | None = None,
    ) -> None:
        self.repository = repository
        self.route_policy = route_policy
        self.recent_sec = max(60, int(recent_sec))
        self.delivery_epoch_id = delivery_epoch_id

    def process(
        self,
        observation: ObservationEnvelope,
        *,
        delivery_current: DomainCurrent,
        now_at: str,
    ) -> ProcessResult | None:
        if (
            observation.domain != "delivery"
            or observation.source != "runtime_lifecycle_events"
            or observation.reason_code != "ffmpeg_child_auto_recovered"
        ):
            return None
        try:
            event = RuntimeLifecycleEvent.from_dict(observation.payload)
        except (TypeError, ValueError):
            return None
        age_sec = unix_ts(now_at) - unix_ts(event.recovered_at)
        if age_sec < 0 or age_sec > self.recent_sec:
            return None
        if delivery_current.state != "good":
            return None
        if unix_ts(delivery_current.observed_at) < unix_ts(event.recovered_at):
            return None

        summary = f"ffmpeg child restart completed after exit code {event.exit_code}"
        reason_codes = (
            "ffmpeg_child_auto_recovered",
            "ffmpeg_exit_nonzero" if event.exit_code else "ffmpeg_exit_zero",
        )
        episode = IncidentEpisode(
            episode_id=self._episode_id(observation),
            domain="delivery",
            status="closed",
            severity="critical",
            opened_at=event.opened_at,
            last_bad_at=event.opened_at,
            closed_at=event.recovered_at,
            bad_samples=1,
            unknown_samples=0,
            last_transition_at=event.recovered_at,
            next_notification_at="",
            policy_revision=LIFECYCLE_POLICY_REVISION,
            summary=summary,
            reason_codes=reason_codes,
        )
        # This transition represents the immutable runtime edge, not whichever
        # delivery snapshot happened to verify it first. Keep the identity
        # event-scoped so a later good snapshot cannot create another recovery.
        transition = IncidentTransition(
            transition_id=stable_id(
                "trn",
                episode.episode_id,
                "recovered",
                event.recovered_at,
                observation.source_event_id,
            ),
            episode_id=episode.episode_id,
            domain="delivery",
            phase="recovered",
            severity="info",
            occurred_at=event.recovered_at,
            current_snapshot_id=delivery_current.snapshot_id,
            summary=summary,
            reason_codes=reason_codes,
        )
        with self.repository.transaction() as connection:
            self.repository.append_current_snapshot(
                delivery_current,
                connection=connection,
            )
            if self.repository.has_recovered_episode_overlapping(
                "delivery",
                opened_at=event.opened_at,
                recovered_at=event.recovered_at,
                excluding_policy_revision=LIFECYCLE_POLICY_REVISION,
                connection=connection,
            ):
                # A sampled current incident already emitted the recovery. The
                # edge ledger fills polling gaps; it must not duplicate a
                # recovery that the ordinary incident path observed.
                return None
            if self.repository.episode_has_transition(
                episode.episode_id,
                "recovered",
                connection=connection,
            ):
                # Also recognizes transitions written by an older lifecycle
                # revision whose ID included a changing current snapshot.
                return ProcessResult(True, episode, None, ())
            self.repository.save_episode(episode, connection=connection)
            inserted = self.repository.append_transition(transition, connection=connection)
            intents = ()
            if inserted:
                intents = build_notification_intents(transition, episode, self.route_policy)
                for intent in intents:
                    self.repository.append_intent(
                        intent,
                        connection=connection,
                        delivery_epoch_id=self.delivery_epoch_id,
                    )
        return ProcessResult(not inserted, episode, transition if inserted else None, intents)

    @staticmethod
    def _episode_id(observation: ObservationEnvelope) -> str:
        return stable_id(
            "inc",
            LIFECYCLE_POLICY_REVISION,
            observation.source,
            observation.source_event_id,
        )
