from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import (
    ObservationEnvelope,
    ObservationRejection,
)

from ._operational_base import PRODUCER_REVISION, nonnegative_int
from .base import AdapterBatch
from .snapshot_support import read_source, source_timestamp


def notification_status(
    state: Mapping[str, Any],
    outbox: Mapping[str, Any],
) -> str:
    pending = nonnegative_int(outbox.get("pending_count"))
    invalid = nonnegative_int(outbox.get("invalid_row_count"))
    active = nonnegative_int(state.get("active_incident_count"))
    if (
        state.get("state_contract_valid") is not True
        or outbox.get("source_present") is not True
        or pending is None
        or invalid is None
        or active is None
    ):
        return "unknown"
    return "good" if pending == 0 and invalid == 0 else "bad"


class NotificationDeliveryAdapter:
    domain = "notification_delivery"
    source = "notification_delivery"

    def __init__(
        self,
        *,
        state_file: Path,
        outbox_file: Path,
        deadline_sec: float = 2.0,
    ) -> None:
        self.state_file = Path(state_file)
        self.outbox_file = Path(outbox_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        state = read_source(
            self.state_file,
            source=self.source,
            received_at=received_at,
            rejections=rejections,
        )
        outbox = read_source(
            self.outbox_file,
            source=self.source,
            received_at=received_at,
            rejections=rejections,
        )
        if state is None or outbox is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        observed_at = source_timestamp(
            state,
            ("updated_ts_utc",),
            source=self.source,
            received_at=state.received_at,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        pending = outbox.payload.get("pending_count")
        invalid = outbox.payload.get("invalid_row_count")
        status = notification_status(state.payload, outbox.payload)
        observation = ObservationEnvelope.create(
            domain=self.domain,
            source=self.source,
            source_event_id=f"{state.sha256}:{outbox.sha256}",
            source_generation=f"{state.sha256}:{outbox.sha256}",
            evidence_role="current_authoritative",
            status=status,
            reason_code=f"notification_delivery_{status}",
            observed_at=observed_at,
            received_at=state.received_at,
            freshness_limit_sec=180,
            producer_revision=PRODUCER_REVISION,
            payload={
                "maintenance": state.payload.get("maintenance_active") is True,
                "active_incident_count": state.payload.get("active_incident_count", 0),
                "pending_count": pending,
                "invalid_row_count": invalid,
                "max_attempts": outbox.payload.get("max_attempts", 0),
                "source_present": outbox.payload.get("source_present") is True,
                "content_projected": False,
            },
        )
        return AdapterBatch(
            self.source,
            (observation,),
            tuple(rejections),
            read_succeeded=True,
        )
