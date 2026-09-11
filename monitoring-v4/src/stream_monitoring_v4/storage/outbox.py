"""Composed repository boundary for intents, leases, and delivery attempts."""

from stream_monitoring_v4.storage.outbox_parts.constants import (
    ATTEMPT_STATES,
    TERMINAL_ATTEMPT_STATES,
)
from stream_monitoring_v4.storage.outbox_parts.delivery import (
    DeliveryAttemptRepositoryMixin,
)
from stream_monitoring_v4.storage.outbox_parts.intents import IntentRepositoryMixin
from stream_monitoring_v4.storage.outbox_parts.leases import LeaseRepositoryMixin


class OutboxRepositoryMixin(
    IntentRepositoryMixin,
    LeaseRepositoryMixin,
    DeliveryAttemptRepositoryMixin,
):
    """Credential-free intent ledger plus separately fenced delivery state."""


__all__ = [
    "ATTEMPT_STATES",
    "OutboxRepositoryMixin",
    "TERMINAL_ATTEMPT_STATES",
]
