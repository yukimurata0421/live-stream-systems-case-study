#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from typing import Any

from stream_contracts.monitoring_v4.time import unix_ts, utc_text
from stream_monitoring_v4.reporting.builder import (
    _coverage_section,
    _latest_cycle,
    _parity_section,
)
from stream_monitoring_v4.reporting.read_model import accumulate_shadow_rows
from stream_monitoring_v4.runtime.source_revision import UNKNOWN_SOURCE_REVISION
from stream_monitoring_v4.storage.postgres import PostgresMonitoringRepository


IMMUTABLE_BUILD_REVISION = re.compile(r"^[0-9a-f]{40}$")
CHECKPOINTS = (
    ("24h", 24 * 3600, "operational_audit"),
    ("72h", 72 * 3600, "operational_audit"),
    ("7d", 7 * 86400, "coverage_gate"),
    ("14d", 14 * 86400, "parity_gate"),
)


def _checkpoint_status(*, elapsed: bool, gate_met: bool | None) -> str:
    if not elapsed:
        return "NOT_YET_ELIGIBLE"
    if gate_met is None:
        return "READY_FOR_FULL_CHECKPOINT_AUDIT"
    return "PASS" if gate_met else "FAIL"


def build_soak_checkpoint(
    repository: Any,
    *,
    epoch_start_ts: int,
    build_revision: str,
    source_revision: str,
    now_ts: int,
    minimum_coverage_pct: float = 99.0,
) -> dict[str, Any]:
    if now_ts <= epoch_start_ts:
        raise ValueError("now_ts must be later than epoch_start_ts")
    accumulator = accumulate_shadow_rows(
        repository,
        build_revision=build_revision,
        source_revision=source_revision,
        coverage_start_ts=epoch_start_ts,
        parity_start_ts=epoch_start_ts,
        end_ts=now_ts,
    )
    first_ts = accumulator.first_ts
    last_ts = accumulator.last_ts
    epoch_anchored = first_ts > 0 and first_ts <= epoch_start_ts + 60
    build_revision_immutable = bool(IMMUTABLE_BUILD_REVISION.fullmatch(build_revision))
    source_revision_known = source_revision != UNKNOWN_SOURCE_REVISION
    revision_gate = build_revision_immutable and source_revision_known
    elapsed_sec = now_ts - epoch_start_ts

    seven_day_elapsed = epoch_anchored and elapsed_sec >= 7 * 86400
    fourteen_day_elapsed = epoch_anchored and elapsed_sec >= 14 * 86400
    coverage = _coverage_section(
        accumulator,
        coverage_start=epoch_start_ts,
        now_ts=now_ts,
        elapsed=seven_day_elapsed,
        minimum_coverage_pct=minimum_coverage_pct,
        revision_gate=revision_gate,
    )
    parity = _parity_section(
        accumulator,
        parity_start=epoch_start_ts,
        now_ts=now_ts,
        elapsed=fourteen_day_elapsed,
        minimum_coverage_pct=minimum_coverage_pct,
        revision_gate=revision_gate,
    )
    projection_failures = len(accumulator.projection_failure_buckets)
    projection_gate = (
        fourteen_day_elapsed and projection_failures == 0 and revision_gate
    )

    checkpoints: list[dict[str, Any]] = []
    for name, required_sec, kind in CHECKPOINTS:
        checkpoint_elapsed = epoch_anchored and elapsed_sec >= required_sec
        gate_met: bool | None = None
        if kind == "coverage_gate":
            gate_met = bool(coverage["gate_met"])
        elif kind == "parity_gate":
            gate_met = bool(parity["gate_met"] and projection_gate)
        checkpoints.append(
            {
                "name": name,
                "kind": kind,
                "required_elapsed_sec": required_sec,
                "elapsed": checkpoint_elapsed,
                "status": _checkpoint_status(elapsed=checkpoint_elapsed, gate_met=gate_met),
            }
        )

    eligible_failures = [
        item for item in checkpoints if item["elapsed"] and item["status"] == "FAIL"
    ]
    if not revision_gate:
        decision = "INVALID_REVISION_IDENTITY"
    elif not first_ts:
        decision = "EVIDENCE_PENDING"
    elif not epoch_anchored:
        decision = "EPOCH_ANCHOR_GAP"
    elif eligible_failures:
        decision = "GATE_FAIL"
    elif checkpoints[-1]["status"] == "PASS":
        decision = "SOAK_COMPLETE_CANDIDATE"
    else:
        decision = "SOAK_CONTINUE"

    return {
        "schema": "monitoring_v4.soak_checkpoint.v1",
        "generated_at": utc_text(now_ts),
        "decision": decision,
        "epoch": {
            "start": utc_text(epoch_start_ts),
            "elapsed_sec": elapsed_sec,
            "first_cycle_at": utc_text(first_ts) if first_ts else "",
            "last_cycle_at": utc_text(last_ts) if last_ts else "",
            "anchored_within_one_cycle": epoch_anchored,
            "history_before_epoch_excluded": True,
        },
        "identity": {
            "build_revision": build_revision,
            "build_revision_immutable": build_revision_immutable,
            "source_revision": source_revision,
            "source_revision_known": source_revision_known,
            "revision_gate": revision_gate,
        },
        "checkpoints": checkpoints,
        "coverage_epoch": coverage,
        "parity_epoch": parity,
        "projection_integrity_epoch": {
            "cycles": len(accumulator.parity_buckets),
            "failed_cycles": projection_failures,
            "fourteen_day_gate_met": projection_gate,
        },
        "latest_cycle": _latest_cycle(accumulator),
        "safety_boundary": {
            "database_mutation_enabled": False,
            "real_delivery_enabled": False,
            "runtime_mutation_enabled": False,
            "full_operational_checkpoint_audit_still_required": True,
        },
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Evaluate one immutable Monitoring v4 soak epoch read-only"
    )
    result.add_argument("--epoch-start", required=True)
    result.add_argument("--build-revision", required=True)
    result.add_argument("--source-revision", required=True)
    result.add_argument("--now-ts", type=int, default=None)
    result.add_argument("--minimum-coverage-pct", type=float, default=99.0)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    now_ts = int(time.time() if args.now_ts is None else args.now_ts)
    repository = PostgresMonitoringRepository(
        application_name="monitoring-v4-soak-checkpoint",
        pool_min_size=0,
        pool_max_size=1,
    )
    try:
        payload = build_soak_checkpoint(
            repository,
            epoch_start_ts=unix_ts(args.epoch_start),
            build_revision=args.build_revision,
            source_revision=args.source_revision,
            now_ts=now_ts,
            minimum_coverage_pct=args.minimum_coverage_pct,
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return 0
    finally:
        repository.close()


if __name__ == "__main__":
    raise SystemExit(main())
