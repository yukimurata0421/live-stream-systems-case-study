from __future__ import annotations

from typing import Any, Mapping

from stream_contracts.monitoring_v4.time import unix_ts, utc_text

from ._util import bucket, expected_cycles, nearest_rank, safe_int
from .policy import COLLECTOR_ADAPTERS, SOURCE_CADENCES


class CoverageAccumulator:
    """Reduce received-cycle coverage without retaining decoded row payloads."""

    def __init__(self, *, start_ts: int, end_ts: int) -> None:
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.collector_successful: dict[str, set[int]] = {
            name: set() for name in COLLECTOR_ADAPTERS
        }
        self.collector_attempted: dict[str, set[int]] = {
            name: set() for name in COLLECTOR_ADAPTERS
        }
        self.source_present: dict[str, set[int]] = {
            name: set() for name in SOURCE_CADENCES
        }
        self.source_fresh: dict[str, set[int]] = {
            name: set() for name in SOURCE_CADENCES
        }
        self.source_timestamps: dict[str, set[int]] = {
            name: set() for name in SOURCE_CADENCES
        }
        self.source_identities: dict[str, set[str]] = {
            name: set() for name in SOURCE_CADENCES
        }

    def add(self, *, timestamp: int, observer: Mapping[str, Any]) -> None:
        cycle_in_window = self.start_ts <= timestamp < self.end_ts
        cycle_bucket = bucket(timestamp, self.start_ts, 60) if cycle_in_window else -1
        results = (
            observer.get("source_results")
            if isinstance(observer.get("source_results"), list)
            else []
        )
        for result in results:
            if not isinstance(result, Mapping):
                continue
            adapter = str(result.get("source", ""))
            if cycle_in_window and adapter in self.collector_attempted:
                self.collector_attempted[adapter].add(cycle_bucket)
                if result.get("outcome") == "observed":
                    self.collector_successful[adapter].add(cycle_bucket)
            events = result.get("observation_events")
            for event in events if isinstance(events, list) else []:
                if not isinstance(event, Mapping):
                    continue
                source = str(event.get("source", ""))
                if source not in self.source_present:
                    continue
                if cycle_in_window:
                    self.source_present[source].add(cycle_bucket)
                try:
                    observed_ts = unix_ts(str(event.get("observed_at", "")))
                    received_ts = unix_ts(str(event.get("received_at", "")))
                    freshness_limit_sec = safe_int(event.get("freshness_limit_sec"), 0)
                except (TypeError, ValueError):
                    continue
                if (
                    cycle_in_window
                    and freshness_limit_sec > 0
                    and 0 <= received_ts - observed_ts <= freshness_limit_sec
                ):
                    self.source_fresh[source].add(cycle_bucket)
                if self.start_ts <= observed_ts < self.end_ts:
                    self.source_timestamps[source].add(observed_ts)
                    self.source_identities[source].add(str(event.get("observation_id", "")))

    def collector_coverage(self) -> list[dict[str, Any]]:
        expected = expected_cycles(self.start_ts, self.end_ts, 60)
        output: list[dict[str, Any]] = []
        for source in COLLECTOR_ADAPTERS:
            observed = min(expected, len(self.collector_successful[source]))
            output.append(
                {
                    "source": source,
                    "cadence_sec": 60,
                    "expected": expected,
                    "attempted": min(expected, len(self.collector_attempted[source])),
                    "observed": observed,
                    "missing": max(0, expected - observed),
                    "coverage_pct": (
                        round(100.0 * observed / expected, 6) if expected else 0.0
                    ),
                }
            )
        return output

    def source_cycle_coverage(self) -> list[dict[str, Any]]:
        expected = expected_cycles(self.start_ts, self.end_ts, 60)
        output: list[dict[str, Any]] = []
        for source, policy in SOURCE_CADENCES.items():
            observed = min(expected, len(self.source_present[source]))
            fresh_observed = min(observed, len(self.source_fresh[source]))
            output.append(
                {
                    "source": source,
                    "window_start": utc_text(self.start_ts),
                    "window_end": utc_text(self.end_ts),
                    "coverage_basis": "one accepted source observation per scheduled v4 cycle, bucketed by cycle receipt",
                    "expected": expected,
                    "observed": observed,
                    "missing": max(0, expected - observed),
                    "coverage_pct": (
                        round(100.0 * observed / expected, 6) if expected else 0.0
                    ),
                    "fresh": fresh_observed,
                    "stale_or_unverifiable": max(0, observed - fresh_observed),
                    "fresh_coverage_pct": (
                        round(100.0 * fresh_observed / expected, 6) if expected else 0.0
                    ),
                    "freshness_pct_when_observed": (
                        round(100.0 * fresh_observed / observed, 6) if observed else 0.0
                    ),
                    "producer_cadence_sec": policy.cadence_sec,
                    "producer_cadence_basis": policy.basis,
                    "cadence_sec": policy.cadence_sec,
                    "cadence_basis": policy.basis,
                }
            )
        return output

    def production_cadence(self) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for source, policy in SOURCE_CADENCES.items():
            ordered = sorted(self.source_timestamps[source])
            gaps = [later - earlier for earlier, later in zip(ordered, ordered[1:])]
            output.append(
                {
                    "source": source,
                    "basis": "diagnostic producer event-time gaps; excluded from the coverage gate",
                    "declared_cadence_sec": policy.cadence_sec,
                    "cadence_basis": policy.basis,
                    "unique_observation_ids": len(self.source_identities[source]),
                    "unique_observed_timestamps": len(ordered),
                    "first_observed_at": utc_text(ordered[0]) if ordered else "",
                    "last_observed_at": utc_text(ordered[-1]) if ordered else "",
                    "p95_gap_sec": nearest_rank(gaps, 0.95),
                    "max_gap_sec": max(gaps) if gaps else None,
                    "gaps_over_declared_cadence": sum(
                        gap > policy.cadence_sec for gap in gaps
                    ),
                }
            )
        return output
