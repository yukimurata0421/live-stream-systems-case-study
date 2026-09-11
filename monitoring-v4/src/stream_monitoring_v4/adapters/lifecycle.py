from __future__ import annotations

from pathlib import Path

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection
from stream_contracts.monitoring_v4.runtime_evidence import RuntimeLifecycleProjection
from stream_contracts.monitoring_v4.time import unix_ts, utc_text

from .base import AdapterBatch
from .snapshot_support import read_source


PRODUCER_REVISION = "stream-monitoring-v4-runtime-lifecycle-r2.1"


class RuntimeLifecycleAdapter:
    """Read sanitized completed edge events without treating them as current state."""

    source = "runtime_lifecycle_state"

    def __init__(self, *, state_file: Path, deadline_sec: float = 2.0) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source="runtime_lifecycle_events",
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        try:
            projection = RuntimeLifecycleProjection.from_dict(snapshot.payload)
        except (TypeError, ValueError) as exc:
            rejections.append(
                ObservationRejection.create(
                    source="runtime_lifecycle_events",
                    reason_code="runtime_lifecycle_contract_invalid",
                    detail=str(exc)[:500],
                    received_at=snapshot.received_at,
                    payload_sha256=snapshot.sha256,
                )
            )
            return AdapterBatch(self.source, rejections=tuple(rejections))

        observations: list[ObservationEnvelope] = []
        for event in projection.events:
            if unix_ts(event.recovered_at) > unix_ts(snapshot.received_at):
                rejections.append(
                    ObservationRejection.create(
                        source="runtime_lifecycle_events",
                        reason_code="source_timestamp_future",
                        detail="runtime lifecycle recovery is newer than its stable receipt",
                        received_at=snapshot.received_at,
                        payload_sha256=snapshot.sha256,
                    )
                )
                continue
            observations.append(
                ObservationEnvelope.create(
                    domain="delivery",
                    source="runtime_lifecycle_events",
                    source_event_id=event.event_id,
                    source_generation=f"{event.run_id}:{event.restart_count}",
                    evidence_role="historical",
                    status="not_applicable",
                    reason_code="ffmpeg_child_auto_recovered",
                    observed_at=event.recovered_at,
                    received_at=snapshot.received_at,
                    freshness_limit_sec=86400,
                    producer_revision=PRODUCER_REVISION,
                    payload=event.to_dict(),
                )
            )
        return AdapterBatch(
            self.source,
            observations=tuple(observations),
            rejections=tuple(rejections),
            read_succeeded=not rejections,
        )
