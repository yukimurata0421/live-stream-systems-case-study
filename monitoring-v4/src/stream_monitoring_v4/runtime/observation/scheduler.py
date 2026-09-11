from __future__ import annotations

import time
from collections import deque
from multiprocessing.context import BaseContext
from typing import Sequence

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.adapters.base import AdapterBatch, ReadOnlyAdapter

from .worker import ActiveAdapter, adapter_process, rejection_batch


class AdapterProcessScheduler:
    """Bounded process scheduler with killable startup and collection deadlines."""

    def __init__(self, process_context: BaseContext, *, max_workers: int) -> None:
        self.process_context = process_context
        self.max_workers = max(1, int(max_workers))

    @staticmethod
    def _rejected_at(*, fixed_clock: bool, started_at: str) -> str:
        return started_at if fixed_clock else utc_text(int(time.time()))

    @classmethod
    def _reject(
        cls,
        batches: list[AdapterBatch],
        adapter: ReadOnlyAdapter,
        *,
        reason_code: str,
        detail: str,
        fixed_clock: bool,
        started_at: str,
    ) -> None:
        batches.append(
            rejection_batch(
                adapter,
                reason_code=reason_code,
                detail=detail,
                received_at=cls._rejected_at(
                    fixed_clock=fixed_clock,
                    started_at=started_at,
                ),
            )
        )

    def _start(
        self,
        adapter: ReadOnlyAdapter,
        *,
        fixed_received_at: str | None,
        fixed_clock: bool,
        started_at: str,
        batches: list[AdapterBatch],
    ) -> ActiveAdapter | None:
        receiver, sender = self.process_context.Pipe(duplex=False)
        process = self.process_context.Process(
            target=adapter_process,
            args=(adapter, fixed_received_at, sender),
            daemon=True,
            name=f"monitoring-v4-adapter-{adapter.source[:32]}",
        )
        try:
            process.start()
        except Exception as exc:
            receiver.close()
            sender.close()
            self._reject(
                batches,
                adapter,
                reason_code="adapter_process_start_failed",
                detail=f"adapter process raised {type(exc).__name__}",
                fixed_clock=fixed_clock,
                started_at=started_at,
            )
            return None
        sender.close()
        return ActiveAdapter(adapter, process, receiver, time.monotonic())

    @staticmethod
    def _outcome(item: ActiveAdapter) -> tuple[str, object] | None:
        if not item.output.poll():
            return None
        try:
            raw = item.output.recv()
        except (EOFError, OSError):
            return None
        if isinstance(raw, tuple) and len(raw) == 2:
            return raw
        return None

    @staticmethod
    def _close_finished(item: ActiveAdapter) -> None:
        item.process.join(timeout=0.2)
        item.output.close()

    @staticmethod
    def _terminate(item: ActiveAdapter) -> None:
        if item.process.is_alive():
            item.process.terminate()
            item.process.join(timeout=0.5)
        if item.process.is_alive():
            item.process.kill()
            item.process.join(timeout=0.5)
        item.output.close()

    def _poll_item(
        self,
        item: ActiveAdapter,
        *,
        now: float,
        fixed_clock: bool,
        started_at: str,
        batches: list[AdapterBatch],
        timed_out: list[str],
    ) -> tuple[bool, bool]:
        outcome = self._outcome(item)
        if outcome is not None:
            kind, value = outcome
            if kind == "started":
                item.started_monotonic = time.monotonic()
                return True, False
            if kind == "ok" and isinstance(value, AdapterBatch):
                batches.append(value)
            else:
                self._reject(
                    batches,
                    item.adapter,
                    reason_code="adapter_exception",
                    detail=f"adapter raised {str(value)[:96]}",
                    fixed_clock=fixed_clock,
                    started_at=started_at,
                )
            self._close_finished(item)
            return True, True
        if not item.process.is_alive():
            self._reject(
                batches,
                item.adapter,
                reason_code="adapter_process_failed",
                detail=f"adapter process exited {item.process.exitcode}",
                fixed_clock=fixed_clock,
                started_at=started_at,
            )
            self._close_finished(item)
            return True, True
        startup_expired = (
            item.started_monotonic is None
            and now - item.launched_monotonic >= 5.0
        )
        execution_expired = (
            item.started_monotonic is not None
            and now - item.started_monotonic
            >= max(0.01, float(item.adapter.deadline_sec))
        )
        if not (startup_expired or execution_expired):
            return False, False
        self._terminate(item)
        timed_out.append(item.adapter.source)
        self._reject(
            batches,
            item.adapter,
            reason_code=(
                "adapter_process_start_timeout"
                if startup_expired
                else "adapter_deadline_exceeded"
            ),
            detail=(
                "adapter process did not start within 5s"
                if startup_expired
                else (
                    f"adapter exceeded {item.adapter.deadline_sec:g}s "
                    "read-only deadline"
                )
            ),
            fixed_clock=fixed_clock,
            started_at=started_at,
        )
        return True, True

    @staticmethod
    def _sleep_until_progress(active: Sequence[ActiveAdapter]) -> None:
        remaining = min(
            max(
                0.0,
                (
                    float(item.adapter.deadline_sec)
                    - (time.monotonic() - item.started_monotonic)
                    if item.started_monotonic is not None
                    else 5.0 - (time.monotonic() - item.launched_monotonic)
                ),
            )
            for item in active
        )
        time.sleep(min(0.01, max(0.001, remaining)))

    def collect(
        self,
        adapters: Sequence[ReadOnlyAdapter],
        *,
        fixed_received_at: str | None,
        started_at: str,
        fixed_clock: bool,
    ) -> tuple[list[AdapterBatch], list[str]]:
        queued = deque(adapters)
        active: list[ActiveAdapter] = []
        batches: list[AdapterBatch] = []
        timed_out: list[str] = []
        try:
            while queued or active:
                while queued and len(active) < self.max_workers:
                    started = self._start(
                        queued.popleft(),
                        fixed_received_at=fixed_received_at,
                        fixed_clock=fixed_clock,
                        started_at=started_at,
                        batches=batches,
                    )
                    if started is not None:
                        active.append(started)
                progressed = False
                now = time.monotonic()
                for item in tuple(active):
                    item_progressed, complete = self._poll_item(
                        item,
                        now=now,
                        fixed_clock=fixed_clock,
                        started_at=started_at,
                        batches=batches,
                        timed_out=timed_out,
                    )
                    progressed = progressed or item_progressed
                    if complete:
                        active.remove(item)
                if active and not progressed:
                    self._sleep_until_progress(active)
        finally:
            for item in active:
                self._terminate(item)
        return batches, timed_out
