from __future__ import annotations

import signal
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Mapping
from uuid import uuid4

from stream_monitoring_v4.storage.ports import NotificationDispatchRepository

from .providers.base import DeliveryResponse, NotificationProvider


class _ProviderDeadline(BaseException):
    pass


@contextmanager
def _provider_deadline(timeout_sec: int) -> Iterator[None]:
    """Enforce a wall-clock deadline without leaving an unkillable worker thread."""

    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("notification provider must run on the dispatcher main thread")

    def expired(_signum: int, _frame: object) -> None:
        raise _ProviderDeadline()

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, 0)
    if previous_timer[0] > 0:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        raise RuntimeError("dispatcher process already owns a real-time alarm")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, max(0.001, float(timeout_sec)))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


@dataclass(frozen=True)
class DispatchSummary:
    lease_acquired: bool
    attempted: int
    sent: int
    failed: int
    pending: int
    uncertain: int = 0


def _outcome_state(response: DeliveryResponse) -> str:
    if response.success:
        return "succeeded"
    if response.delivery_uncertain or int(response.status_code) == 0:
        return "uncertain"
    status = int(response.status_code)
    if status in {408, 425, 429} or status >= 500:
        return "retryable_failed"
    return "permanent_failed"


class NotificationDispatcher:
    def __init__(
        self,
        repository: NotificationDispatchRepository,
        providers: Mapping[str, NotificationProvider],
        *,
        owner: str,
        lease_ttl_sec: int = 180,
        max_attempts: int = 8,
        attempt_timeout_sec: int = 120,
        instance_id: str | None = None,
    ) -> None:
        self.repository = repository
        self.providers = dict(providers)
        if not owner.strip():
            raise ValueError("dispatcher owner is required")
        self.owner = owner.strip()
        self.instance_id = (instance_id or uuid4().hex).strip()
        if not self.instance_id:
            raise ValueError("dispatcher instance_id is required")
        self.lease_owner = f"{self.owner}/{self.instance_id}"
        self.lease_ttl_sec = max(1, int(lease_ttl_sec))
        self.max_attempts = max(1, int(max_attempts))
        self.attempt_timeout_sec = max(1, int(attempt_timeout_sec))
        if self.lease_ttl_sec <= self.attempt_timeout_sec:
            raise ValueError("dispatcher lease_ttl_sec must exceed attempt_timeout_sec")

    def dispatch_once(self, *, now_ts: int, limit: int = 20) -> DispatchSummary:
        lease_name = "notification_dispatcher"
        fence_token = self.repository.acquire_fenced_lease(
            lease_name,
            self.lease_owner,
            now_ts=now_ts,
            ttl_sec=self.lease_ttl_sec,
        )
        if fence_token is None:
            pending = self.repository.undelivered_intent_count(
                now_ts=now_ts,
                max_attempts=self.max_attempts,
                attempt_timeout_sec=self.attempt_timeout_sec,
            )
            return DispatchSummary(False, 0, 0, 0, pending, 0)
        attempted = sent = failed = uncertain = 0
        started_monotonic = time.monotonic()

        def operation_ts() -> int:
            return now_ts + max(0, int(time.monotonic() - started_monotonic))

        try:
            self.repository.quarantine_stale_delivery_attempts(
                now_ts=now_ts,
                attempt_timeout_sec=self.attempt_timeout_sec,
            )
            intents = self.repository.due_intents(
                now_ts=now_ts,
                max_attempts=self.max_attempts,
                attempt_timeout_sec=self.attempt_timeout_sec,
                limit=limit,
            )
            for intent in intents:
                attempt_now = operation_ts()
                if not self.repository.renew_fenced_lease(
                    lease_name,
                    self.lease_owner,
                    fence_token,
                    now_ts=attempt_now,
                    ttl_sec=self.lease_ttl_sec,
                ):
                    break
                attempt = self.repository.begin_delivery_attempt(
                    intent.intent_id,
                    self.lease_owner,
                    lease_name=lease_name,
                    fence_token=fence_token,
                    now_ts=attempt_now,
                )
                attempted += 1
                provider = self.providers.get(intent.route)
                if provider is None:
                    response = DeliveryResponse(
                        False,
                        400,
                        "provider_not_configured",
                        0,
                        False,
                    )
                else:
                    try:
                        with _provider_deadline(self.attempt_timeout_sec):
                            response = provider.send(intent)
                    except _ProviderDeadline:
                        response = DeliveryResponse(
                            False,
                            0,
                            "provider_deadline_exceeded_delivery_uncertain",
                            0,
                            True,
                        )
                    except TimeoutError:
                        response = DeliveryResponse(
                            False,
                            0,
                            "provider_timeout_delivery_uncertain",
                            0,
                            True,
                        )
                    except Exception as exc:
                        response = DeliveryResponse(
                            False,
                            0,
                            f"provider_exception_delivery_uncertain:{type(exc).__name__}",
                            0,
                            True,
                        )
                if not isinstance(response, DeliveryResponse):
                    response = DeliveryResponse(
                        False,
                        0,
                        "provider_invalid_response_delivery_uncertain",
                        0,
                        True,
                    )
                outcome_state = _outcome_state(response)
                finish_now = operation_ts()
                try:
                    self.repository.finish_delivery_attempt(
                        attempt,
                        success=response.success,
                        status_code=response.status_code,
                        detail=response.detail,
                        retry_after_sec=response.retry_after_sec,
                        now_ts=finish_now,
                        outcome_state=outcome_state,
                    )
                except RuntimeError:
                    outcome_state = "uncertain"
                if outcome_state == "succeeded":
                    sent += 1
                else:
                    failed += 1
                    uncertain += int(outcome_state == "uncertain")
            pending = self.repository.undelivered_intent_count(
                now_ts=operation_ts(),
                max_attempts=self.max_attempts,
                attempt_timeout_sec=self.attempt_timeout_sec,
            )
            return DispatchSummary(
                True,
                attempted,
                sent,
                failed,
                pending,
                uncertain,
            )
        finally:
            self.repository.release_lease(
                lease_name,
                self.lease_owner,
                fence_token,
            )
