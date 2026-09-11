from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from .accumulator import ShadowReportAccumulator


class ShadowCycleReportReader(Protocol):
    def iter_shadow_cycle_report_rows(
        self,
        *,
        build_revision: str,
        source_revision: str,
        start_ts: int,
        end_ts: int,
    ) -> Iterable[Mapping[str, Any]]: ...


def accumulate_shadow_rows(
    repository: ShadowCycleReportReader,
    *,
    build_revision: str,
    source_revision: str,
    coverage_start_ts: int,
    parity_start_ts: int,
    end_ts: int,
) -> ShadowReportAccumulator:
    accumulator = ShadowReportAccumulator(
        coverage_start_ts=coverage_start_ts,
        parity_start_ts=parity_start_ts,
        end_ts=end_ts,
    )
    for row in repository.iter_shadow_cycle_report_rows(
        build_revision=build_revision,
        source_revision=source_revision,
        start_ts=parity_start_ts,
        end_ts=end_ts,
    ):
        accumulator.add(row)
    return accumulator
