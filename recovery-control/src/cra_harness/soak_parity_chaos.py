from __future__ import annotations

import argparse
import json
import random
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cra_no_action_soak.parity import ParityRecord, evaluate_monitoring_parity
from cra_no_action_soak.time import isoformat_utc

SCHEMA = "cra.soak_parity_convergence_chaos.v1"
POLICY_REVISION = "monitoring-v4-parity-convergence-r2"
START = datetime(2026, 9, 1, tzinfo=UTC)


def _equivalent(*, domain: str, observed_at: datetime) -> dict[str, Any]:
    if domain == "delivery":
        expected_source = "subsystems_status.local_delivery"
        actual_sources = ["runtime_delivery_watchdog"]
    else:
        expected_source = "operational_reliability_burn_status.raw_current"
        actual_sources = ["youtube_input_quality_oauth"]
    observed = isoformat_utc(observed_at)
    return {
        "schema": "monitoring_v4.live_parity.v2",
        "parity_policy_revision": POLICY_REVISION,
        "equivalent": True,
        "accepted_difference_count": 0,
        "unclassified_contract_difference_count": 0,
        "input_integrity_errors": [],
        "verified_rollouts": [],
        "domains": {
            domain: {
                "match": True,
                "classification": "equivalent",
                "expected_state": "good",
                "actual_state": "good",
                "expected_source": expected_source,
                "actual_sources": actual_sources,
                "expected_observed_at": observed,
                "actual_observed_at": observed,
                "normalization_error": "",
            }
        },
    }


def _candidate(
    *,
    domain: str,
    actual_at: datetime,
    skew_seconds: int,
) -> dict[str, Any]:
    payload = _equivalent(domain=domain, observed_at=actual_at)
    policy = (
        ("subsystems_status.local_delivery", ["runtime_delivery_watchdog"], 180, 360)
        if domain == "delivery"
        else (
            "operational_reliability_burn_status.raw_current",
            ["youtube_input_quality_oauth"],
            600,
            600,
        )
    )
    expected_at = actual_at + timedelta(seconds=skew_seconds)
    payload.update(
        {
            "equivalent": False,
            "unclassified_contract_difference_count": 1,
        }
    )
    payload["domains"][domain] = {
        "match": False,
        "classification": "candidate_source_snapshot_skew_convergence",
        "expected_state": "bad",
        "actual_state": "good",
        "expected_source": policy[0],
        "actual_sources": policy[1],
        "expected_observed_at": isoformat_utc(expected_at),
        "actual_observed_at": isoformat_utc(actual_at),
        "snapshot_skew_sec": skew_seconds,
        "normalization_error": "",
        "convergence_policy_revision": POLICY_REVISION,
        "maximum_absolute_skew_sec": policy[2],
        "convergence_timeout_sec": policy[3],
    }
    return payload


def _record(
    index: int,
    observed_at: datetime,
    payload: dict[str, Any],
    *,
    cycle_id: str | None = None,
) -> ParityRecord:
    return ParityRecord(
        sample_index=index,
        observed_at=observed_at,
        monitoring_cycle_id=cycle_id or f"cycle-{index}",
        parity_clean=payload["equivalent"] is True,
        payload=payload,
    )


def _case(rng: random.Random, index: int) -> tuple[str, list[ParityRecord], str]:
    scenario = rng.choice(
        (
            "converged",
            "pending",
            "timeout",
            "late_proof",
            "wrong_source",
            "policy_drift",
            "normalization_error",
            "cycle_mutation",
            "count_contradiction",
            "self_accepted",
            "input_integrity",
            "readiness_contradiction",
            "identical_cycle_replay",
        )
    )
    domain = rng.choice(("delivery", "youtube_input_quality"))
    maximum_skew = 180 if domain == "delivery" else 600
    timeout = 360 if domain == "delivery" else 600
    skew = rng.choice((-1, 1)) * rng.randint(1, maximum_skew)
    base = START + timedelta(seconds=index * 2_000)
    candidate = _candidate(domain=domain, actual_at=base, skew_seconds=skew)
    ahead = base + timedelta(seconds=max(0, skew))
    lagging_side = "actual" if skew > 0 else "expected"
    proof_at = ahead + timedelta(seconds=rng.randint(1, timeout))
    proof = _equivalent(domain=domain, observed_at=proof_at)
    if lagging_side == "actual":
        proof["domains"][domain]["actual_observed_at"] = isoformat_utc(proof_at)
    else:
        proof["domains"][domain]["expected_observed_at"] = isoformat_utc(proof_at)

    records = [_record(0, base, candidate)]
    expected = "terminal"
    if scenario == "converged":
        records.append(_record(1, proof_at, proof))
        expected = "accepted"
    elif scenario == "pending":
        expected = "pending"
    elif scenario == "timeout":
        records.append(
            _record(
                1,
                ahead + timedelta(seconds=timeout + 1),
                _candidate(domain=domain, actual_at=base, skew_seconds=skew),
                cycle_id="cycle-0",
            )
        )
    elif scenario == "late_proof":
        late = ahead + timedelta(seconds=timeout + 1)
        records.append(_record(1, late, _equivalent(domain=domain, observed_at=late)))
    elif scenario == "wrong_source":
        candidate["domains"][domain]["actual_sources"] = ["unrelated_source"]
        records = [_record(0, base, candidate)]
    elif scenario == "policy_drift":
        candidate["domains"][domain]["convergence_policy_revision"] = "future-policy"
        records = [_record(0, base, candidate)]
    elif scenario == "normalization_error":
        candidate["domains"][domain]["normalization_error"] = "source_invalid"
        records = [_record(0, base, candidate)]
    elif scenario == "cycle_mutation":
        changed = deepcopy(candidate)
        changed["domains"][domain]["actual_state"] = "unknown"
        records.append(_record(1, base + timedelta(seconds=1), changed, cycle_id="cycle-0"))
    elif scenario == "count_contradiction":
        candidate["unclassified_contract_difference_count"] = 0
        records = [_record(0, base, candidate)]
    elif scenario == "self_accepted":
        candidate["accepted_difference_count"] = 1
        records = [_record(0, base, candidate)]
    elif scenario == "input_integrity":
        candidate["input_integrity_errors"] = ["duplicate_actual_domain"]
        records = [_record(0, base, candidate)]
    elif scenario == "readiness_contradiction":
        records = [
            ParityRecord(
                sample_index=0,
                observed_at=base,
                monitoring_cycle_id="cycle-0",
                parity_clean=True,
                payload=candidate,
            )
        ]
    elif scenario == "identical_cycle_replay":
        records.append(_record(1, base + timedelta(seconds=1), deepcopy(candidate), cycle_id="cycle-0"))
        expected = "pending"
    return scenario, records, expected


def run(*, cases: int, seed: int) -> dict[str, Any]:
    if cases <= 0:
        raise ValueError("cases must be positive")
    rng = random.Random(seed)
    counts: dict[str, int] = {}
    failures: list[dict[str, Any]] = []
    for index in range(cases):
        scenario, records, expected = _case(rng, index)
        counts[scenario] = counts.get(scenario, 0) + 1
        blockers, evidence = evaluate_monitoring_parity(records)
        actual = (
            "accepted"
            if evidence["accepted_difference_count"] == 1 and not blockers
            else "pending"
            if blockers == {"SOAK_MONITORING_PARITY_CONVERGENCE_PENDING"}
            else "terminal"
        )
        if actual != expected:
            failures.append(
                {
                    "case_index": index,
                    "scenario": scenario,
                    "expected": expected,
                    "actual": actual,
                    "blockers": sorted(blockers),
                    "evidence": evidence,
                }
            )
    return {
        "schema": SCHEMA,
        "seed": seed,
        "case_count": cases,
        "scenario_counts": dict(sorted(counts.items())),
        "failure_count": len(failures),
        "failures": failures[:100],
        "production_touched": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Randomized soak parity convergence oracle")
    parser.add_argument("--cases", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=20260901)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(cases=args.cases, seed=args.seed)
    encoded = json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    if result["failure_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
