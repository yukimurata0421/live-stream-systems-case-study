from __future__ import annotations

from collections.abc import Iterable

from stream_contracts.monitoring_v4.notification import NotificationIntent

from .base import DeliveryResponse


class RecordingProvider:
    """Credential-free provider used for isolated R5 validation."""

    def __init__(self) -> None:
        self.intents: list[NotificationIntent] = []
        self._sent_ids: set[str] = set()

    def send(self, intent: NotificationIntent) -> DeliveryResponse:
        if intent.intent_id in self._sent_ids:
            return DeliveryResponse(True, 208, "already_recorded_by_idempotency_key")
        self._sent_ids.add(intent.intent_id)
        self.intents.append(intent)
        return DeliveryResponse(True, 204, "recorded_by_isolated_provider")


class ScriptedProvider:
    """Deterministic failure injector for timeout/429/5xx/crash tests."""

    def __init__(self, responses: Iterable[DeliveryResponse | BaseException]) -> None:
        self.responses = list(responses)
        self.intents: list[NotificationIntent] = []

    def send(self, intent: NotificationIntent) -> DeliveryResponse:
        self.intents.append(intent)
        if not self.responses:
            return DeliveryResponse(True, 204, "script_exhausted_success")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response
