from __future__ import annotations

from dataclasses import dataclass
from multiprocessing.connection import Connection

from stream_contracts.monitoring_v4.observation import ObservationRejection
from stream_monitoring_v4.adapters.base import AdapterBatch, ReadOnlyAdapter


def adapter_process(
    adapter: ReadOnlyAdapter,
    received_at: str | None,
    output: Connection,
) -> None:
    """Collect one sanitized adapter in a killable core process boundary."""

    try:
        output.send(("started", None))
        output.send(("ok", adapter.collect(received_at=received_at)))
    except BaseException as exc:
        try:
            output.send(("error", type(exc).__name__))
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        output.close()


@dataclass
class ActiveAdapter:
    adapter: ReadOnlyAdapter
    process: object
    output: Connection
    launched_monotonic: float
    started_monotonic: float | None = None


def rejection_batch(
    adapter: ReadOnlyAdapter,
    *,
    reason_code: str,
    detail: str,
    received_at: str,
) -> AdapterBatch:
    return AdapterBatch(
        adapter.source,
        rejections=(
            ObservationRejection.create(
                source=adapter.source,
                reason_code=reason_code,
                detail=detail,
                received_at=received_at,
            ),
        ),
    )
