from __future__ import annotations

import time
from multiprocessing import get_context
from typing import Sequence

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.adapters.base import ReadOnlyAdapter
from stream_monitoring_v4.storage.ports import ObserverRepository

from .models import ObserverRun
from .scheduler import AdapterProcessScheduler
from .summary import source_results


class Observer:
    def __init__(
        self,
        repository: ObserverRepository,
        *,
        max_workers: int = 4,
        process_start_method: str = "forkserver",
    ) -> None:
        self.repository = repository
        self.scheduler = AdapterProcessScheduler(
            get_context(process_start_method),
            max_workers=max_workers,
        )

    def run_once(
        self,
        adapters: Sequence[ReadOnlyAdapter],
        *,
        now_ts: int | None = None,
    ) -> ObserverRun:
        started_ts = int(time.time() if now_ts is None else now_ts)
        started_at = utc_text(started_ts)
        started_monotonic = time.monotonic()
        # A fixed receipt is injected only for deterministic replay/tests. A
        # live adapter captures receipt after its own stable snapshot read.
        fixed_received_at = started_at if now_ts is not None else None
        batches, timed_out = self.scheduler.collect(
            adapters,
            fixed_received_at=fixed_received_at,
            started_at=started_at,
            fixed_clock=now_ts is not None,
        )

        inserted = 0
        duplicates = 0
        rejected = 0
        with self.repository.transaction() as connection:
            for batch in batches:
                for item in batch.observations:
                    if self.repository.append_observation(
                        item,
                        connection=connection,
                    ):
                        inserted += 1
                    else:
                        duplicates += 1
                for item in batch.rejections:
                    if self.repository.append_rejection(
                        item,
                        connection=connection,
                    ):
                        rejected += 1
        rejection_events = sum(len(batch.rejections) for batch in batches)
        completed_ts = max(
            started_ts,
            int(time.time()) if now_ts is None else started_ts,
        )
        duration_ms = max(
            0,
            int(round((time.monotonic() - started_monotonic) * 1000)),
        )
        status = "bad" if timed_out else ("unknown" if rejection_events else "good")
        self.repository.set_component_health(
            "observer",
            status,
            (
                f"inserted={inserted} duplicates={duplicates} "
                f"rejections_seen={rejection_events} rejections_inserted={rejected} "
                f"timed_out={len(timed_out)}"
            ),
            now_ts=completed_ts,
        )
        return ObserverRun(
            started_at=started_at,
            completed_at=utc_text(completed_ts),
            duration_ms=duration_ms,
            adapters=len(adapters),
            inserted_observations=inserted,
            duplicate_observations=duplicates,
            inserted_rejections=rejected,
            timed_out_sources=tuple(sorted(timed_out)),
            source_results=source_results(batches, timed_out),
            collected_observations=tuple(
                sorted(
                    (
                        item
                        for batch in batches
                        for item in batch.observations
                    ),
                    key=lambda item: (
                        item.source,
                        item.observed_at,
                        item.observation_id,
                    ),
                )
            ),
        )
