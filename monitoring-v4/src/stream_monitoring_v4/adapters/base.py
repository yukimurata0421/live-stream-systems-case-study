from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection


@dataclass(frozen=True)
class AdapterBatch:
    source: str
    observations: tuple[ObservationEnvelope, ...] = ()
    rejections: tuple[ObservationRejection, ...] = ()
    read_succeeded: bool = False


class ReadOnlyAdapter(Protocol):
    source: str
    deadline_sec: float

    def collect(self, *, received_at: str | None = None) -> AdapterBatch: ...
