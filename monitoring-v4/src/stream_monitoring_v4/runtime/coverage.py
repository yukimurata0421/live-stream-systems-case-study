from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.time import unix_ts, utc_text


@dataclass(frozen=True)
class SourceCoverage:
    source: str
    window_start: str
    window_end: str
    cadence_sec: int
    expected: int
    observed: int
    missing: int
    duplicate_or_extra: int
    coverage_pct: float

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "cadence_sec": self.cadence_sec,
            "expected": self.expected,
            "observed": self.observed,
            "missing": self.missing,
            "duplicate_or_extra": self.duplicate_or_extra,
            "coverage_pct": self.coverage_pct,
        }


def source_coverage(
    observations: Iterable[ObservationEnvelope],
    *,
    source: str,
    window_start_ts: int,
    window_end_ts: int,
    cadence_sec: int,
) -> SourceCoverage:
    cadence = int(cadence_sec)
    if cadence <= 0:
        raise ValueError("cadence_sec must be positive")
    if window_end_ts <= window_start_ts:
        raise ValueError("coverage window must have positive duration")
    duration = window_end_ts - window_start_ts
    expected = (duration + cadence - 1) // cadence
    matching = [
        item
        for item in observations
        if item.source == source and window_start_ts <= unix_ts(item.observed_at) < window_end_ts
    ]
    buckets = {
        (unix_ts(item.observed_at) - window_start_ts) // cadence
        for item in matching
    }
    observed = min(expected, len(buckets))
    missing = max(0, expected - observed)
    coverage_pct = round(100.0 * observed / expected, 6)
    return SourceCoverage(
        source=source,
        window_start=utc_text(window_start_ts),
        window_end=utc_text(window_end_ts),
        cadence_sec=cadence,
        expected=expected,
        observed=observed,
        missing=missing,
        duplicate_or_extra=max(0, len(matching) - observed),
        coverage_pct=coverage_pct,
    )
