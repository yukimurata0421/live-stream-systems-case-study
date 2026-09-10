from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from collections import Counter
from itertools import product
from pathlib import Path
from typing import Any

from cra_harness.covering_array import coverage_report, generate_covering_array
from cra_harness.runner.no_action_randomized import _gate_case
from cra_no_action_soak.gate import evaluate_no_action_soak

SCHEMA = "cra.no_action_covering_campaign.v1"
SEED = 20260902
FAULT_SCENARIOS = (
    "healthy",
    "early_window",
    "sample_gap",
    "timestamp_regression",
    "release_mismatch",
    "service_failure",
    "service_failure_counter_regression",
    "restart_increase",
    "sequence_regression",
    "command_delivery_enabled",
    "control_capability_nonzero",
    "cra_physical_effect_nonzero",
    "credential_too_low",
    "disk_too_low",
    "resident_memory_growth",
    "open_fd_growth",
    "command_route_present",
    "duplicate_effect_scope",
    "unexplained_target_transition",
    "target_change_without_transition",
    "epoch_baseline_drift",
    "effect_boundary_exceeds_request",
    "effect_unknown_exceeds_boundary",
    "compound_release_and_service_failure",
    "compound_temporal_and_sequence_regression",
)
FACTOR_VALUES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("fault_scenario", FAULT_SCENARIOS),
    ("sample_count", tuple(str(value) for value in range(4, 14))),
    ("injection_position", ("first", "early", "middle_early", "middle_late", "late", "last")),
    ("clock_pattern", ("uniform", "front_loaded", "back_loaded", "modular", "early_boundary", "late_boundary")),
    ("resource_profile", tuple(f"resource-{index}" for index in range(6))),
    ("effect_counter_profile", ("zero", "quarter", "half", "near_all", "all")),
    ("persistence_profile", tuple(f"sqlite-wal-archive-{index}" for index in range(5))),
    ("identity_profile", tuple(f"identity-{index}" for index in range(5))),
    ("observation_step", ("1", "2", "10", "100")),
    ("projection_step", ("1", "3", "11", "101")),
    ("oracle_limit_margin", ("0.1", "0.5", "1.0", "10.0")),
)
FACTOR_SIZES = tuple(len(values) for _, values in FACTOR_VALUES)
HIGH_RISK_INDICES = (0, 2, 3, 5, 6, 10)
HIGH_RISK_LEVEL_INDICES = (
    (22, 23, 24),  # unknown-boundary and both compound fault scenarios
    (0, 2, 4, 5),  # first, both middle/late boundaries, and last
    (1, 2, 5),  # front-loaded, back-loaded, and late-boundary clocks
    (0, 3, 4),  # no evidence, near-all, and all effect counters
    (0, 3, 4),  # empty, WAL/archive-heavy, and maximum persistence profiles
    (0, 2, 3),  # tight, nominal, and wide oracle margins
)


def _mandatory_high_risk_rows() -> tuple[tuple[int, ...], ...]:
    rows: list[tuple[int, ...]] = []
    for values in product(*HIGH_RISK_LEVEL_INDICES):
        row = [0] * len(FACTOR_SIZES)
        for index, value in zip(HIGH_RISK_INDICES, values, strict=True):
            row[index] = value
        rows.append(tuple(row))
    return tuple(rows)


def _source_hashes() -> dict[str, str]:
    project_root = Path(__file__).resolve().parents[3]
    paths = {
        Path(__file__).resolve(),
        Path(generate_covering_array.__code__.co_filename).resolve(),
        Path(_gate_case.__code__.co_filename).resolve(),
        Path(evaluate_no_action_soak.__code__.co_filename).resolve(),
    }
    return {str(path.relative_to(project_root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def campaign_rows(*, case_count: int, seed: int) -> tuple[tuple[int, ...], ...]:
    return generate_covering_array(
        FACTOR_SIZES,
        strength=4,
        target_count=case_count,
        mandatory_rows=_mandatory_high_risk_rows(),
        seed=seed,
    )


def execute_rows(rows: tuple[tuple[int, ...], ...], *, seed: int) -> dict[str, Any]:
    started = time.perf_counter()
    failures: list[dict[str, Any]] = []
    failure_count = 0
    scenario_counts: Counter[str] = Counter()
    value_counts = [Counter[int]() for _ in FACTOR_SIZES]
    result_hasher = hashlib.sha256()
    simulated_seconds = 0.0
    for index, row in enumerate(rows, start=1):
        if len(row) != len(FACTOR_SIZES) or any(not 0 <= value < FACTOR_SIZES[position] for position, value in enumerate(row)):
            raise ValueError("COVERING_CAMPAIGN_ROW_INVALID")
        scenario = FAULT_SCENARIOS[row[0]]
        case_seed = int.from_bytes(hashlib.sha256(f"{seed}:{index}:{row}".encode()).digest()[:8], "big")
        result = _gate_case(scenario, random.Random(case_seed), case_seed, factors=row)
        scenario_counts[scenario] += 1
        for position, value in enumerate(row):
            value_counts[position][value] += 1
        simulated_seconds += float(result["simulated_seconds"])
        case_summary = {
            "case_index": index,
            "case_seed": case_seed,
            "factors": row,
            "scenario": scenario,
            "passed": bool(result["passed"]),
            "classification": result["classification"],
            "actual": result["actual"],
        }
        result_hasher.update(json.dumps(case_summary, separators=(",", ":"), sort_keys=True).encode())
        result_hasher.update(b"\n")
        if not result["passed"]:
            failure_count += 1
            if len(failures) < 100:
                failures.append(case_summary)
    coverage = coverage_report(rows, FACTOR_SIZES, strength=4)
    mandatory = set(_mandatory_high_risk_rows())
    row_set = set(rows)
    mandatory_present = sum(row in row_set for row in mandatory)
    elapsed = time.perf_counter() - started
    passed = failure_count == 0 and coverage["coverage_complete"] is True and mandatory_present == len(mandatory)
    return {
        "schema": SCHEMA,
        "result": "PASS" if passed else "FAIL",
        "seed": seed,
        "case_count": len(rows),
        "passed_case_count": len(rows) - failure_count,
        "failure_count": failure_count,
        "failure_examples": failures,
        "factor_model": {name: list(values) for name, values in FACTOR_VALUES},
        "factor_value_counts": {
            FACTOR_VALUES[index][0]: {FACTOR_VALUES[index][1][value]: count for value, count in sorted(counter.items())}
            for index, counter in enumerate(value_counts)
        },
        "scenario_counts": dict(sorted(scenario_counts.items())),
        "global_coverage": coverage,
        "high_risk_six_way": {
            "factor_names": [FACTOR_VALUES[index][0] for index in HIGH_RISK_INDICES],
            "factor_values": {
                FACTOR_VALUES[index][0]: [FACTOR_VALUES[index][1][value] for value in HIGH_RISK_LEVEL_INDICES[position]]
                for position, index in enumerate(HIGH_RISK_INDICES)
            },
            "expected_combination_count": len(mandatory),
            "observed_combination_count": mandatory_present,
            "coverage_complete": mandatory_present == len(mandatory),
        },
        "case_results_sha256": result_hasher.hexdigest(),
        "simulated_days": round(simulated_seconds / 86_400, 6),
        "wall_duration_seconds": round(elapsed, 6),
        "boundary": {
            "production_state_read_count": 0,
            "credential_read_count": 0,
            "network_call_count": 0,
            "production_mutation_count": 0,
            "live_port_bind_count": 0,
        },
        "formal_soak_replacement": False,
    }


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the 4-way CRA NO_ACTION covering campaign")
    parser.add_argument("--cases", type=int, default=15_000)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generated_at = time.time()
    source_hashes = _source_hashes()
    rows = campaign_rows(case_count=args.cases, seed=args.seed)
    report = execute_rows(rows, seed=args.seed)
    if _source_hashes() != source_hashes:
        raise RuntimeError("COVERING_CAMPAIGN_SOURCE_DRIFT")
    report["generated_at_unix"] = generated_at
    report["source_hashes"] = source_hashes
    _atomic_json(args.output, report)
    print(
        json.dumps(
            {
                "artifact": str(args.output),
                "artifact_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                "case_count": report["case_count"],
                "result": report["result"],
                "global_coverage": report["global_coverage"],
                "high_risk_six_way": report["high_risk_six_way"],
                "case_results_sha256": report["case_results_sha256"],
                "simulated_days": report["simulated_days"],
                "wall_duration_seconds": report["wall_duration_seconds"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
