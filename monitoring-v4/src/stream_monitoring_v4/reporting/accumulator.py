from __future__ import annotations

from typing import Any, Mapping

from ._util import bucket, safe_int
from .coverage import CoverageAccumulator
from .parity import ParityAccumulator


class ShadowReportAccumulator:
    """Reduce reporter rows without retaining their decoded JSON payloads."""

    def __init__(self, *, coverage_start_ts: int, parity_start_ts: int, end_ts: int) -> None:
        self.coverage_start_ts = int(coverage_start_ts)
        self.parity_start_ts = int(parity_start_ts)
        self.end_ts = int(end_ts)
        self.first_ts = 0
        self.last_ts = 0
        self.raw_row_count = 0
        self.coverage = CoverageAccumulator(start_ts=coverage_start_ts, end_ts=end_ts)
        self.parity = ParityAccumulator()
        self.projection_failure_buckets: set[int] = set()
        self.latest_cycle: dict[str, Any] = {}

    def add(self, row: Mapping[str, Any]) -> None:
        timestamp = int(row["started_ts"])
        self.raw_row_count += 1
        self.first_ts = timestamp if not self.first_ts else min(self.first_ts, timestamp)
        self.last_ts = max(self.last_ts, timestamp)

        observer = row.get("observer") if isinstance(row.get("observer"), Mapping) else {}
        self.coverage.add(timestamp=timestamp, observer=observer)

        cycle_in_parity = self.parity_start_ts <= timestamp < self.end_ts
        parity_bucket = (
            bucket(timestamp, self.parity_start_ts, 60) if cycle_in_parity else -1
        )
        parity = row.get("parity") if isinstance(row.get("parity"), Mapping) else {}
        payload_valid = self.parity.add(
            parity,
            cycle_in_window=cycle_in_parity,
            bucket_id=parity_bucket,
        )
        projection = (
            parity.get("projection_integrity")
            if isinstance(parity.get("projection_integrity"), Mapping)
            else {}
        )
        projection_ok = (
            projection.get("complete") is True
            and safe_int(projection.get("rejection_count"), -1) == 0
            and safe_int(projection.get("projection_count"), -1)
            == safe_int(projection.get("expected_count"), -2)
        )
        if cycle_in_parity and not projection_ok:
            self.projection_failure_buckets.add(parity_bucket)

        cycle_summary = {
            "started_ts": timestamp,
            "parity_payload_valid": payload_valid,
            "parity_equivalent": parity.get("equivalent") is True,
            "parity_accepted_difference_count": safe_int(
                parity.get("accepted_difference_count"), 0
            ),
            "parity_unclassified_contract_difference_count": safe_int(
                parity.get("unclassified_contract_difference_count"), 0
            ),
            "projection_integrity_complete": projection_ok,
            "projection_count": safe_int(projection.get("projection_count"), -1),
            "projection_expected_count": safe_int(
                projection.get("expected_count"), -1
            ),
            "projection_rejection_count": safe_int(
                projection.get("rejection_count"), -1
            ),
        }
        self._merge_latest_cycle(cycle_summary)

    def _merge_latest_cycle(self, cycle_summary: Mapping[str, Any]) -> None:
        timestamp = int(cycle_summary["started_ts"])
        if not self.latest_cycle or timestamp > int(self.latest_cycle["started_ts"]):
            self.latest_cycle = dict(cycle_summary)
            return
        if timestamp != int(self.latest_cycle["started_ts"]):
            return
        self.latest_cycle = {
            **self.latest_cycle,
            "parity_payload_valid": (
                self.latest_cycle.get("parity_payload_valid") is True
                and cycle_summary["parity_payload_valid"] is True
            ),
            "parity_equivalent": (
                self.latest_cycle.get("parity_equivalent") is True
                and cycle_summary["parity_equivalent"] is True
            ),
            "parity_accepted_difference_count": max(
                safe_int(self.latest_cycle.get("parity_accepted_difference_count")),
                safe_int(cycle_summary["parity_accepted_difference_count"]),
            ),
            "parity_unclassified_contract_difference_count": max(
                safe_int(
                    self.latest_cycle.get(
                        "parity_unclassified_contract_difference_count"
                    )
                ),
                safe_int(cycle_summary["parity_unclassified_contract_difference_count"]),
            ),
            "projection_integrity_complete": (
                self.latest_cycle.get("projection_integrity_complete") is True
                and cycle_summary["projection_integrity_complete"] is True
            ),
            "projection_count": min(
                safe_int(self.latest_cycle.get("projection_count"), -1),
                safe_int(cycle_summary["projection_count"], -1),
            ),
            "projection_expected_count": max(
                safe_int(self.latest_cycle.get("projection_expected_count"), -1),
                safe_int(cycle_summary["projection_expected_count"], -1),
            ),
            "projection_rejection_count": max(
                safe_int(self.latest_cycle.get("projection_rejection_count"), -1),
                safe_int(cycle_summary["projection_rejection_count"], -1),
            ),
        }

    @property
    def parity_buckets(self) -> set[int]:
        return self.parity.buckets

    @property
    def parity_invalid_buckets(self) -> set[int]:
        return self.parity.invalid_buckets

    def collector_coverage(self) -> list[dict[str, Any]]:
        return self.coverage.collector_coverage()

    def source_cycle_coverage(self) -> list[dict[str, Any]]:
        return self.coverage.source_cycle_coverage()

    def production_cadence(self) -> list[dict[str, Any]]:
        return self.coverage.production_cadence()

    def parity_totals(self) -> dict[str, int]:
        return self.parity.totals()

    def projection_failure_count(self, *, start_ts: int) -> int:
        first_bucket = max(0, bucket(int(start_ts), self.parity_start_ts, 60))
        return sum(item >= first_bucket for item in self.projection_failure_buckets)
