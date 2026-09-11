from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Sequence

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.time import utc_text

from stream_monitoring_v4.adapters.base import ReadOnlyAdapter
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.incidents.service import IncidentService, ProcessResult
from stream_monitoring_v4.incidents.lifecycle import RuntimeLifecycleIncidentService
from stream_monitoring_v4.runtime.observer import Observer, ObserverRun


@dataclass(frozen=True)
class ShadowPipelineResult:
    observer: ObserverRun
    decision_at: str
    currents: tuple[DomainCurrent, ...]
    incidents: tuple[ProcessResult, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "observer": self.observer.to_dict(),
            "decision_at": self.decision_at,
            "currents": [item.to_dict() for item in self.currents],
            "transitions": [
                item.transition.to_dict() for item in self.incidents if item.transition is not None
            ],
            "notification_intents": [
                intent.to_dict() for item in self.incidents for intent in item.intents
            ],
        }


class ShadowPipeline:
    """R0-R5 pipeline that ends at an isolated notification intent outbox."""

    def __init__(
        self,
        observer: Observer,
        reducer: CurrentReducerService,
        incidents: IncidentService,
        lifecycle_incidents: RuntimeLifecycleIncidentService | None = None,
    ) -> None:
        self.observer = observer
        self.reducer = reducer
        self.incidents = incidents
        self.lifecycle_incidents = lifecycle_incidents

    def run_once(
        self,
        adapters: Sequence[ReadOnlyAdapter],
        domains: Sequence[str],
        *,
        now_ts: int | None = None,
        now_at: str | None = None,
    ) -> ShadowPipelineResult:
        observation = self.observer.run_once(adapters, now_ts=now_ts)
        decision_ts = int(time.time()) if now_ts is None else int(now_ts)
        decision_at = now_at or utc_text(decision_ts)
        currents = tuple(self.reducer.reduce(domains, now_ts=decision_ts))
        results = [self.incidents.process(current, now_at=decision_at) for current in currents]
        if self.lifecycle_incidents is not None:
            delivery = next(item for item in currents if item.domain == "delivery")
            for item in observation.collected_observations:
                event_result = self.lifecycle_incidents.process(
                    item,
                    delivery_current=delivery,
                    now_at=decision_at,
                )
                if event_result is not None:
                    results.append(event_result)
        return ShadowPipelineResult(observation, decision_at, currents, tuple(results))
