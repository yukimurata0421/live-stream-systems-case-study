from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from stream_contracts.monitoring_v4.notification import NotificationIntent


@dataclass(frozen=True)
class DeliveryResponse:
    success: bool
    status_code: int
    detail: str
    retry_after_sec: int = 0
    delivery_uncertain: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.success, bool):
            raise ValueError("delivery success must be boolean")
        if type(self.status_code) is not int or not 0 <= self.status_code <= 599:
            raise ValueError("delivery status_code is outside the supported range")
        if not isinstance(self.detail, str) or not self.detail or len(self.detail) > 500:
            raise ValueError("delivery detail must contain 1-500 characters")
        if type(self.retry_after_sec) is not int or self.retry_after_sec < 0:
            raise ValueError("delivery retry_after_sec must be non-negative")
        if not isinstance(self.delivery_uncertain, bool):
            raise ValueError("delivery uncertainty must be boolean")
        if self.success and not 200 <= self.status_code < 300:
            raise ValueError("successful delivery requires a 2xx status")
        if self.success and self.delivery_uncertain:
            raise ValueError("successful delivery cannot be uncertain")


class NotificationProvider(Protocol):
    def send(self, intent: NotificationIntent) -> DeliveryResponse: ...
