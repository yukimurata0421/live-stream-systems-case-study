from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Protocol, runtime_checkable

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.notification import NotificationIntent
from stream_contracts.monitoring_v4.observation import (
    ObservationEnvelope,
    ObservationRejection,
)
from stream_contracts.monitoring_v4.sli import SLIProjection


class TransactionalStore(Protocol):
    def transaction(self, *, immediate: bool = True) -> AbstractContextManager[Any]: ...


class ObservationReader(Protocol):
    def latest_observations(
        self,
        domain: str,
        *,
        limit: int = 200,
    ) -> list[ObservationEnvelope]: ...


class ObservationAppender(Protocol):
    def append_observation(
        self,
        item: ObservationEnvelope,
        *,
        connection: Any | None = None,
    ) -> bool: ...


class RejectionWriter(Protocol):
    def append_rejection(
        self,
        item: ObservationRejection,
        *,
        connection: Any | None = None,
    ) -> bool: ...


class CurrentReader(Protocol):
    def current(
        self,
        domain: str,
        *,
        connection: Any | None = None,
    ) -> DomainCurrent | None: ...


class CurrentSnapshotWriter(Protocol):
    def append_current_snapshot(
        self,
        item: DomainCurrent,
        *,
        connection: Any | None = None,
    ) -> bool: ...


class CurrentCanonicalWriter(Protocol):
    def save_current(
        self,
        item: DomainCurrent,
        *,
        connection: Any | None = None,
    ) -> bool: ...


class ComponentHealthWriter(Protocol):
    def set_component_health(
        self,
        component: str,
        status: str,
        detail: str,
        *,
        now_ts: int,
    ) -> None: ...


class NotificationIntentWriter(Protocol):
    def append_intent(
        self,
        item: NotificationIntent,
        *,
        connection: Any,
        delivery_epoch_id: str | None = None,
    ) -> bool: ...


class ActiveEpisodeReader(Protocol):
    def active_episode(
        self,
        domain: str,
        *,
        connection: Any | None = None,
    ) -> IncidentEpisode | None: ...


class EpisodeWriter(Protocol):
    def save_episode(self, item: IncidentEpisode, *, connection: Any) -> None: ...


class LifecycleDedupReader(Protocol):

    def has_recovered_episode_overlapping(
        self,
        domain: str,
        *,
        opened_at: str,
        recovered_at: str,
        excluding_policy_revision: str,
        connection: Any,
    ) -> bool: ...

    def episode_has_transition(
        self,
        episode_id: str,
        phase: str,
        *,
        connection: Any,
    ) -> bool: ...


class IncidentCandidateStore(Protocol):
    def candidate(self, domain: str, *, connection: Any) -> dict[str, Any] | None: ...

    def save_candidate(self, value: dict[str, Any], *, connection: Any) -> None: ...

    def delete_candidate(self, domain: str, *, connection: Any) -> None: ...


class TransitionWriter(Protocol):
    def append_transition(
        self,
        item: IncidentTransition,
        *,
        connection: Any,
    ) -> bool: ...


class IncidentProcessingLedger(Protocol):
    def current_was_processed(self, snapshot_id: str, *, connection: Any) -> bool: ...

    def mark_current_processed(
        self,
        snapshot_id: str,
        domain: str,
        *,
        processed_at: str,
        connection: Any,
    ) -> None: ...


class SLIProjectionWriter(Protocol):
    def save_sli_projection(
        self,
        item: SLIProjection,
        *,
        connection: Any | None = None,
    ) -> bool: ...


class SLIProjectionReader(Protocol):
    def current_sli_projections(self) -> list[SLIProjection]: ...


class OutboxSummaryReader(Protocol):
    def undelivered_intent_count(
        self,
        *,
        now_ts: int,
        max_attempts: int,
        attempt_timeout_sec: int,
    ) -> int: ...

    def delivery_outbox_counts(self, *, max_attempts: int = 8) -> dict[str, int]: ...


class MonitoringMetricsReader(Protocol):
    def monitoring_metrics_snapshot(self) -> dict[str, Any]: ...


@runtime_checkable
class CurrentReducerRepository(
    TransactionalStore,
    ObservationReader,
    CurrentReader,
    CurrentCanonicalWriter,
    ComponentHealthWriter,
    Protocol,
):
    """Capabilities owned by current reduction; no incident or delivery methods."""


@runtime_checkable
class ObserverRepository(
    TransactionalStore,
    ObservationAppender,
    RejectionWriter,
    ComponentHealthWriter,
    Protocol,
):
    """Append-only observation boundary used by read-only adapters."""


@runtime_checkable
class IncidentRepository(
    TransactionalStore,
    CurrentSnapshotWriter,
    ActiveEpisodeReader,
    EpisodeWriter,
    IncidentCandidateStore,
    IncidentProcessingLedger,
    TransitionWriter,
    NotificationIntentWriter,
    Protocol,
):
    """Atomic current-to-incident transition and intent boundary."""


@runtime_checkable
class ReliabilityProjectionRepository(
    TransactionalStore,
    RejectionWriter,
    SLIProjectionWriter,
    SLIProjectionReader,
    ComponentHealthWriter,
    Protocol,
):
    """Analytical projection boundary; it cannot mutate incident state."""


@runtime_checkable
class ExporterRepository(
    ObservationReader,
    CurrentReader,
    SLIProjectionReader,
    OutboxSummaryReader,
    MonitoringMetricsReader,
    Protocol,
):
    """Read model exposed to the Prometheus renderer."""


@runtime_checkable
class RuntimeLifecycleRepository(
    TransactionalStore,
    CurrentSnapshotWriter,
    EpisodeWriter,
    LifecycleDedupReader,
    TransitionWriter,
    NotificationIntentWriter,
    Protocol,
):
    """Recovery-edge ledger without candidate or current-decision ownership."""


@runtime_checkable
class NotificationDispatchRepository(Protocol):
    """Fenced outbox delivery boundary, intentionally separate from incidents."""

    def acquire_fenced_lease(
        self,
        name: str,
        owner: str,
        *,
        now_ts: int,
        ttl_sec: int,
    ) -> int | None: ...

    def renew_fenced_lease(
        self,
        name: str,
        owner: str,
        fence_token: int,
        *,
        now_ts: int,
        ttl_sec: int,
    ) -> bool: ...

    def release_lease(
        self,
        name: str,
        owner: str,
        fence_token: int | None = None,
    ) -> None: ...

    def quarantine_stale_delivery_attempts(
        self,
        *,
        now_ts: int,
        attempt_timeout_sec: int,
    ) -> int: ...

    def due_intents(
        self,
        *,
        now_ts: int,
        max_attempts: int,
        attempt_timeout_sec: int,
        limit: int,
    ) -> list[NotificationIntent]: ...

    def begin_delivery_attempt(
        self,
        intent_id: str,
        owner: str,
        *,
        lease_name: str,
        fence_token: int,
        now_ts: int,
    ) -> dict[str, Any]: ...

    def finish_delivery_attempt(
        self,
        attempt: dict[str, Any],
        *,
        success: bool,
        status_code: int,
        detail: str,
        retry_after_sec: int,
        now_ts: int,
        outcome_state: str,
    ) -> str: ...

    def undelivered_intent_count(
        self,
        *,
        now_ts: int,
        max_attempts: int,
        attempt_timeout_sec: int,
    ) -> int: ...
