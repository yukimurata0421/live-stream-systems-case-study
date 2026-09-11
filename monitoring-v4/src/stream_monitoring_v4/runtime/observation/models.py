from __future__ import annotations

from dataclasses import dataclass

from stream_contracts.monitoring_v4.observation import ObservationEnvelope


@dataclass(frozen=True)
class ObserverRun:
    started_at: str
    completed_at: str
    duration_ms: int
    adapters: int
    inserted_observations: int
    duplicate_observations: int
    inserted_rejections: int
    timed_out_sources: tuple[str, ...]
    source_results: tuple[dict[str, object], ...] = ()
    collected_observations: tuple[ObservationEnvelope, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "adapters": self.adapters,
            "inserted_observations": self.inserted_observations,
            "duplicate_observations": self.duplicate_observations,
            "inserted_rejections": self.inserted_rejections,
            "timed_out_sources": list(self.timed_out_sources),
            "source_results": [dict(item) for item in self.source_results],
        }
