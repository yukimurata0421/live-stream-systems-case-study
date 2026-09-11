from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from stream_contracts.monitoring_v4.observation import (
    ObservationEnvelope,
    ObservationRejection,
)

from .base import AdapterBatch
from .snapshot_support import read_source, source_timestamp


PRODUCER_REVISION = "stream-monitoring-v4-r5.1-operational"


def mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def nonnegative_int(value: Any) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        return None
    return value


class OperationalSnapshotAdapter:
    domain = ""
    source = ""
    timestamp_keys: Sequence[str] = ()
    freshness_limit_sec = 300
    evidence_role = "current_authoritative"

    def __init__(self, *, state_file: Path, deadline_sec: float = 2.0) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def status(self, payload: Mapping[str, Any]) -> str:
        raise NotImplementedError

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source=self.source,
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        observed_at = source_timestamp(
            snapshot,
            self.timestamp_keys,
            source=self.source,
            received_at=snapshot.received_at,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        status = self.status(snapshot.payload)
        observation = ObservationEnvelope.create(
            domain=self.domain,
            source=self.source,
            source_event_id=snapshot.sha256,
            source_generation=snapshot.sha256,
            evidence_role=self.evidence_role,
            status=status,
            reason_code=f"{self.source}_{status}",
            observed_at=observed_at,
            received_at=snapshot.received_at,
            freshness_limit_sec=self.freshness_limit_sec,
            producer_revision=PRODUCER_REVISION,
            payload=self.safe_payload(snapshot.payload),
        )
        return AdapterBatch(
            self.source,
            (observation,),
            tuple(rejections),
            read_succeeded=True,
        )
