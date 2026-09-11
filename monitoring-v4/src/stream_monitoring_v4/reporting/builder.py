from __future__ import annotations

import re
from typing import Any

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.runtime.source_revision import UNKNOWN_SOURCE_REVISION

from ._util import expected_cycles
from .accumulator import ShadowReportAccumulator
from .policy import MAXIMUM_ACCEPTED_DIFFERENCE_PCT, MINIMUM_EQUIVALENT_CYCLE_PCT
from .read_model import ShadowCycleReportReader, accumulate_shadow_rows


_IMMUTABLE_BUILD_REVISION = re.compile(r"^[0-9a-f]{40}$")


def _coverage_section(
    accumulator: ShadowReportAccumulator,
    *,
    coverage_start: int,
    now_ts: int,
    elapsed: bool,
    minimum_coverage_pct: float,
    revision_gate: bool,
) -> dict[str, Any]:
    collector = accumulator.collector_coverage()
    sources = accumulator.source_cycle_coverage()
    projection_failures = accumulator.projection_failure_count(
        start_ts=coverage_start
    )
    collector_ready = all(
        item["coverage_pct"] >= minimum_coverage_pct for item in collector
    )
    sources_ready = all(
        item["coverage_pct"] >= minimum_coverage_pct
        and item["fresh_coverage_pct"] >= minimum_coverage_pct
        for item in sources
    )
    return {
        "window_start": utc_text(coverage_start),
        "window_end": utc_text(now_ts),
        "elapsed": elapsed,
        "minimum_coverage_pct": minimum_coverage_pct,
        "collector_adapters": collector,
        "journal_sources": sources,
        "source_cycle_availability": sources,
        "production_cadence": accumulator.production_cadence(),
        "projection_failed_cycles": projection_failures,
        "coverage_model": (
            "collector and source availability are received-time cycle coverage; "
            "source freshness is evaluated per cycle; producer event-time gaps are diagnostic"
        ),
        "gate_met": (
            elapsed
            and collector_ready
            and sources_ready
            and projection_failures == 0
            and revision_gate
        ),
    }


def _parity_section(
    accumulator: ShadowReportAccumulator,
    *,
    parity_start: int,
    now_ts: int,
    elapsed: bool,
    minimum_coverage_pct: float,
    revision_gate: bool,
) -> dict[str, Any]:
    expected = expected_cycles(parity_start, now_ts, 60)
    coverage_pct = (
        round(100.0 * len(accumulator.parity_buckets) / expected, 6)
        if expected
        else 0.0
    )
    totals = accumulator.parity_totals()
    cycles = len(accumulator.parity_buckets)
    equivalent_pct = (
        round(100.0 * totals["equivalent"] / cycles, 6) if cycles else 0.0
    )
    accepted_pct = (
        round(100.0 * totals["accepted"] / cycles, 6) if cycles else 0.0
    )
    return {
        "window_start": utc_text(parity_start),
        "window_end": utc_text(now_ts),
        "elapsed": elapsed,
        "cycles": cycles,
        "raw_rows": accumulator.raw_row_count,
        "expected_cycles": expected,
        "cycle_coverage_pct": coverage_pct,
        "equivalent_cycles": totals["equivalent"],
        "equivalent_cycle_pct": equivalent_pct,
        "minimum_equivalent_cycle_pct": MINIMUM_EQUIVALENT_CYCLE_PCT,
        "accepted_difference_count": totals["accepted"],
        "accepted_difference_pct_of_cycles": accepted_pct,
        "maximum_accepted_difference_pct": MAXIMUM_ACCEPTED_DIFFERENCE_PCT,
        "unclassified_contract_difference_count": totals["violations"],
        "unconverged_candidate_difference_count": totals["unconverged_candidates"],
        "invalid_payload_count": totals["payload_invalid"],
        "verified_rollout_evidence_count": totals["verified_rollout_evidence"],
        "conflicting_rollout_evidence_count": totals[
            "conflicting_rollout_evidence"
        ],
        # The first rollout-named field is a report-v4 compatibility alias that
        # historically counted every retrospective proof. Keep its value stable
        # and expose unambiguous aggregate/type-specific fields alongside it.
        "retrospectively_verified_difference_count": totals[
            "retrospectively_verified"
        ],
        "retrospectively_verified_rollout_difference_count": totals[
            "retrospectively_verified"
        ],
        "retrospectively_verified_planned_rollout_difference_count": totals[
            "retrospectively_verified_rollout"
        ],
        "retrospectively_verified_snapshot_difference_count": totals[
            "retrospectively_verified_snapshot"
        ],
        "gate_met": (
            elapsed
            and coverage_pct >= minimum_coverage_pct
            and equivalent_pct >= MINIMUM_EQUIVALENT_CYCLE_PCT
            and accepted_pct <= MAXIMUM_ACCEPTED_DIFFERENCE_PCT
            and totals["violations"] == 0
            and totals["payload_invalid"] == 0
            and totals["conflicting_rollout_evidence"] == 0
            and revision_gate
        ),
    }


def _latest_cycle(accumulator: ShadowReportAccumulator) -> dict[str, Any]:
    return {
        **accumulator.latest_cycle,
        "started_at": (
            utc_text(int(accumulator.latest_cycle["started_ts"]))
            if accumulator.latest_cycle
            else ""
        ),
    }


def build_report(
    repository: ShadowCycleReportReader,
    *,
    build_revision: str,
    source_revision: str,
    now_ts: int,
    minimum_coverage_pct: float = 99.0,
) -> dict[str, Any]:
    coverage_start = now_ts - 7 * 86400
    parity_start = now_ts - 14 * 86400
    accumulator = accumulate_shadow_rows(
        repository,
        build_revision=build_revision,
        source_revision=source_revision,
        coverage_start_ts=coverage_start,
        parity_start_ts=parity_start,
        end_ts=now_ts,
    )
    first_ts = accumulator.first_ts
    last_ts = accumulator.last_ts
    seven_day_elapsed = first_ts > 0 and first_ts <= coverage_start + 60
    fourteen_day_elapsed = first_ts > 0 and first_ts <= parity_start + 60
    build_revision_immutable = bool(_IMMUTABLE_BUILD_REVISION.fullmatch(build_revision))
    source_revision_known = source_revision != UNKNOWN_SOURCE_REVISION
    revision_gate = build_revision_immutable and source_revision_known
    projection_failures_14d = len(accumulator.projection_failure_buckets)
    return {
        "schema": "monitoring_v4.shadow_evidence_report.v4",
        "generated_at": utc_text(now_ts),
        "build_revision": build_revision,
        "build_revision_immutable": build_revision_immutable,
        "source_revision": source_revision,
        "source_revision_known": source_revision_known,
        "safety_boundary": {
            "real_delivery_enabled": False,
            "runtime_mutation_enabled": False,
            "raspberry_pi_dependency": False,
        },
        "coverage_7d": _coverage_section(
            accumulator,
            coverage_start=coverage_start,
            now_ts=now_ts,
            elapsed=seven_day_elapsed,
            minimum_coverage_pct=minimum_coverage_pct,
            revision_gate=revision_gate,
        ),
        "parity_14d": _parity_section(
            accumulator,
            parity_start=parity_start,
            now_ts=now_ts,
            elapsed=fourteen_day_elapsed,
            minimum_coverage_pct=minimum_coverage_pct,
            revision_gate=revision_gate,
        ),
        "projection_integrity_14d": {
            "cycles": len(accumulator.parity_buckets),
            "failed_cycles": projection_failures_14d,
            "gate_met": (
                fourteen_day_elapsed
                and projection_failures_14d == 0
                and revision_gate
            ),
        },
        "latest_cycle": _latest_cycle(accumulator),
        "revision_window": {
            "first_cycle_at": utc_text(first_ts) if first_ts else "",
            "last_cycle_at": utc_text(last_ts) if last_ts else "",
            "revision_change_resets_time_gates": True,
            "build_revision_change_resets_time_gates": True,
            "source_revision_change_resets_time_gates": True,
        },
    }
