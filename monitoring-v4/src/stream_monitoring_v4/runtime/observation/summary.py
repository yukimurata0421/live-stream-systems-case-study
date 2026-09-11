from __future__ import annotations

from typing import Sequence

from stream_monitoring_v4.adapters.base import AdapterBatch


def source_results(
    batches: Sequence[AdapterBatch],
    timed_out: Sequence[str],
) -> tuple[dict[str, object], ...]:
    timed_out_set = set(timed_out)
    return tuple(
        {
            "source": batch.source,
            "outcome": (
                "timeout"
                if batch.source in timed_out_set
                else "partial"
                if batch.rejections and (batch.observations or batch.read_succeeded)
                else "observed"
                if batch.observations or batch.read_succeeded
                else "rejected"
                if batch.rejections
                else "empty"
            ),
            "observations": len(batch.observations),
            "rejections": len(batch.rejections),
            "observation_events": [
                {
                    "source": item.source,
                    "observation_id": item.observation_id,
                    "observed_at": item.observed_at,
                    "received_at": item.received_at,
                    "freshness_limit_sec": item.freshness_limit_sec,
                    "evidence_role": item.evidence_role,
                    "status": item.status,
                }
                for item in sorted(
                    batch.observations,
                    key=lambda value: (
                        value.source,
                        value.observed_at,
                        value.observation_id,
                    ),
                )
            ],
            "rejection_events": [
                {
                    "source": item.source,
                    "reason_code": item.reason_code,
                    "received_at": item.received_at,
                }
                for item in sorted(
                    batch.rejections,
                    key=lambda value: (
                        value.source,
                        value.received_at,
                        value.rejection_id,
                    ),
                )
            ],
        }
        for batch in sorted(batches, key=lambda item: item.source)
    )
