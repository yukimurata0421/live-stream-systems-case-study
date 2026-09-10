from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cra_no_action_soak.time import parse_utc

PARITY_SCHEMA = "monitoring_v4.live_parity.v2"
PARITY_POLICY_REVISION = "monitoring-v4-parity-convergence-r2"
CURRENT_STATES = frozenset({"good", "bad", "unknown"})


@dataclass(frozen=True)
class SnapshotConvergencePolicy:
    expected_source: str
    actual_sources: tuple[str, ...]
    maximum_absolute_skew_seconds: int
    convergence_timeout_seconds: int


POLICIES: dict[tuple[str, str], SnapshotConvergencePolicy] = {
    ("delivery", "subsystems_status.local_delivery"): SnapshotConvergencePolicy(
        expected_source="subsystems_status.local_delivery",
        actual_sources=("runtime_delivery_watchdog",),
        maximum_absolute_skew_seconds=180,
        convergence_timeout_seconds=360,
    ),
    (
        "youtube_input_quality",
        "operational_reliability_burn_status.raw_current",
    ): SnapshotConvergencePolicy(
        expected_source="operational_reliability_burn_status.raw_current",
        actual_sources=("youtube_input_quality_oauth",),
        maximum_absolute_skew_seconds=600,
        convergence_timeout_seconds=600,
    ),
}


@dataclass(frozen=True)
class ParityRecord:
    sample_index: int
    observed_at: datetime
    monitoring_cycle_id: str
    parity_clean: bool
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class _Candidate:
    sample_index: int
    cycle_id: str
    domain: str
    policy: SnapshotConvergencePolicy
    expected_timestamp: int
    actual_timestamp: int
    expected_state: str
    actual_state: str

    @property
    def lagging_side(self) -> str:
        return "actual" if self.expected_timestamp > self.actual_timestamp else "expected"

    @property
    def ahead_timestamp(self) -> int:
        return max(self.expected_timestamp, self.actual_timestamp)


@dataclass(frozen=True)
class _Proof:
    sample_index: int
    domain: str
    expected_source: str
    actual_sources: tuple[str, ...]
    expected_timestamp: int
    actual_timestamp: int
    expected_state: str
    actual_state: str


def _timestamp(value: object, code: str) -> int:
    if not isinstance(value, str):
        raise ValueError(code)
    return int(parse_utc(value).timestamp())


def _sources(value: object, code: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(code)
    sources = tuple(value)
    if (
        not sources
        or not all(isinstance(item, str) and item.strip() == item and item for item in sources)
        or tuple(sorted(set(sources))) != sources
    ):
        raise ValueError(code)
    return sources


def _mapping(value: object, code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(code)
    return dict(value)


def _candidate(
    *,
    sample_index: int,
    cycle_id: str,
    domain: str,
    detail: Mapping[str, Any],
) -> _Candidate:
    if detail.get("classification") != "candidate_source_snapshot_skew_convergence":
        raise ValueError("SOAK_MONITORING_PARITY_UNCLASSIFIED_DIFFERENCE")
    if detail.get("match") is not False:
        raise ValueError("SOAK_MONITORING_PARITY_CANDIDATE_MATCH_INVALID")
    expected_state = detail.get("expected_state")
    actual_state = detail.get("actual_state")
    if expected_state not in CURRENT_STATES or actual_state not in CURRENT_STATES or expected_state == actual_state:
        raise ValueError("SOAK_MONITORING_PARITY_CANDIDATE_STATE_INVALID")
    if detail.get("normalization_error") not in {"", None}:
        raise ValueError("SOAK_MONITORING_PARITY_NORMALIZATION_ERROR")
    expected_source = detail.get("expected_source")
    if not isinstance(expected_source, str):
        raise ValueError("SOAK_MONITORING_PARITY_EXPECTED_SOURCE_INVALID")
    policy = POLICIES.get((domain, expected_source))
    if policy is None:
        raise ValueError("SOAK_MONITORING_PARITY_SOURCE_PAIR_NOT_ALLOWLISTED")
    actual_sources = _sources(
        detail.get("actual_sources"),
        "SOAK_MONITORING_PARITY_ACTUAL_SOURCES_INVALID",
    )
    if actual_sources != policy.actual_sources:
        raise ValueError("SOAK_MONITORING_PARITY_ACTUAL_SOURCE_MISMATCH")
    if detail.get("convergence_policy_revision") != PARITY_POLICY_REVISION:
        raise ValueError("SOAK_MONITORING_PARITY_POLICY_REVISION_MISMATCH")
    if detail.get("maximum_absolute_skew_sec") != policy.maximum_absolute_skew_seconds:
        raise ValueError("SOAK_MONITORING_PARITY_SKEW_BOUND_MISMATCH")
    if detail.get("convergence_timeout_sec") != policy.convergence_timeout_seconds:
        raise ValueError("SOAK_MONITORING_PARITY_TIMEOUT_BOUND_MISMATCH")
    expected_timestamp = _timestamp(
        detail.get("expected_observed_at"),
        "SOAK_MONITORING_PARITY_EXPECTED_TIME_INVALID",
    )
    actual_timestamp = _timestamp(
        detail.get("actual_observed_at"),
        "SOAK_MONITORING_PARITY_ACTUAL_TIME_INVALID",
    )
    skew = expected_timestamp - actual_timestamp
    if (
        skew == 0
        or abs(skew) > policy.maximum_absolute_skew_seconds
        or type(detail.get("snapshot_skew_sec")) is not int
        or detail.get("snapshot_skew_sec") != skew
    ):
        raise ValueError("SOAK_MONITORING_PARITY_CANDIDATE_SKEW_INVALID")
    return _Candidate(
        sample_index=sample_index,
        cycle_id=cycle_id,
        domain=domain,
        policy=policy,
        expected_timestamp=expected_timestamp,
        actual_timestamp=actual_timestamp,
        expected_state=str(expected_state),
        actual_state=str(actual_state),
    )


def _proof(sample_index: int, domain: str, detail: Mapping[str, Any]) -> _Proof:
    if detail.get("match") is not True or detail.get("classification") != "equivalent":
        raise ValueError("SOAK_MONITORING_PARITY_EQUIVALENT_DETAIL_INVALID")
    expected_state = detail.get("expected_state")
    actual_state = detail.get("actual_state")
    expected_source = detail.get("expected_source")
    if (
        expected_state not in CURRENT_STATES
        or actual_state != expected_state
        or not isinstance(expected_source, str)
        or detail.get("normalization_error") not in {"", None}
    ):
        raise ValueError("SOAK_MONITORING_PARITY_EQUIVALENT_PROOF_INVALID")
    return _Proof(
        sample_index=sample_index,
        domain=domain,
        expected_source=expected_source,
        actual_sources=_sources(
            detail.get("actual_sources"),
            "SOAK_MONITORING_PARITY_EQUIVALENT_SOURCES_INVALID",
        ),
        expected_timestamp=_timestamp(
            detail.get("expected_observed_at"),
            "SOAK_MONITORING_PARITY_EQUIVALENT_EXPECTED_TIME_INVALID",
        ),
        actual_timestamp=_timestamp(
            detail.get("actual_observed_at"),
            "SOAK_MONITORING_PARITY_EQUIVALENT_ACTUAL_TIME_INVALID",
        ),
        expected_state=str(expected_state),
        actual_state=str(actual_state),
    )


def _cycle_payload(
    record: ParityRecord,
) -> tuple[list[_Candidate], list[_Proof], bool]:
    parity = dict(record.payload)
    if parity.get("schema") != PARITY_SCHEMA:
        raise ValueError("SOAK_MONITORING_PARITY_SCHEMA_INVALID")
    if parity.get("parity_policy_revision") != PARITY_POLICY_REVISION:
        raise ValueError("SOAK_MONITORING_PARITY_POLICY_REVISION_MISMATCH")
    domains = _mapping(parity.get("domains"), "SOAK_MONITORING_PARITY_DOMAINS_INVALID")
    if not domains:
        raise ValueError("SOAK_MONITORING_PARITY_DOMAINS_EMPTY")
    integrity = parity.get("input_integrity_errors")
    if not isinstance(integrity, list) or not all(isinstance(item, str) and item for item in integrity):
        raise ValueError("SOAK_MONITORING_PARITY_INPUT_INTEGRITY_INVALID")
    if integrity:
        raise ValueError("SOAK_MONITORING_PARITY_INPUT_INTEGRITY_ERROR")
    if type(parity.get("accepted_difference_count")) is not int or parity.get("accepted_difference_count") != 0:
        raise ValueError("SOAK_MONITORING_PARITY_SELF_ACCEPTED_DIFFERENCE")
    declared_violations = parity.get("unclassified_contract_difference_count")
    if type(declared_violations) is not int or declared_violations < 0:
        raise ValueError("SOAK_MONITORING_PARITY_VIOLATION_COUNT_INVALID")

    candidates: list[_Candidate] = []
    proofs: list[_Proof] = []
    all_match = True
    mismatch_count = 0
    for raw_domain, raw_detail in sorted(domains.items()):
        if not isinstance(raw_domain, str) or not raw_domain:
            raise ValueError("SOAK_MONITORING_PARITY_DOMAIN_INVALID")
        detail = _mapping(raw_detail, "SOAK_MONITORING_PARITY_DETAIL_INVALID")
        match = detail.get("match")
        if not isinstance(match, bool):
            raise ValueError("SOAK_MONITORING_PARITY_MATCH_INVALID")
        all_match = all_match and match
        if match:
            proofs.append(_proof(record.sample_index, raw_domain, detail))
        else:
            mismatch_count += 1
            candidates.append(
                _candidate(
                    sample_index=record.sample_index,
                    cycle_id=record.monitoring_cycle_id,
                    domain=raw_domain,
                    detail=detail,
                )
            )
    if parity.get("equivalent") is not all_match or declared_violations != mismatch_count:
        raise ValueError("SOAK_MONITORING_PARITY_TOP_LEVEL_CONTRADICTION")
    if record.parity_clean is not all_match:
        raise ValueError("SOAK_MONITORING_PARITY_READINESS_CONTRADICTION")
    return candidates, proofs, all_match


def _converged(candidate: _Candidate, proof: _Proof) -> bool:
    if proof.sample_index <= candidate.sample_index:
        return False
    if (
        proof.domain != candidate.domain
        or proof.expected_source != candidate.policy.expected_source
        or proof.actual_sources != candidate.policy.actual_sources
    ):
        return False
    caught_up = proof.actual_timestamp if candidate.lagging_side == "actual" else proof.expected_timestamp
    return candidate.ahead_timestamp < caught_up <= candidate.ahead_timestamp + candidate.policy.convergence_timeout_seconds


def evaluate_monitoring_parity(
    records: Sequence[ParityRecord],
) -> tuple[set[str], dict[str, int]]:
    """Evaluate cycle parity without losing bounded, later-proven convergence."""

    blockers: set[str] = set()
    unique: list[ParityRecord] = []
    cycle_payloads: dict[str, str] = {}
    for record in records:
        try:
            canonical = json.dumps(record.payload, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError):
            blockers.add(f"SOAK_MONITORING_PARITY_PAYLOAD_INVALID:{record.sample_index}")
            continue
        previous = cycle_payloads.get(record.monitoring_cycle_id)
        if previous is None:
            cycle_payloads[record.monitoring_cycle_id] = canonical
            unique.append(record)
        elif previous != canonical:
            blockers.add(f"SOAK_MONITORING_PARITY_CYCLE_MUTATED:{record.sample_index}")

    candidates: list[_Candidate] = []
    proofs: list[_Proof] = []
    equivalent_cycles = 0
    candidate_cycles = 0
    for record in unique:
        try:
            cycle_candidates, cycle_proofs, equivalent = _cycle_payload(record)
        except ValueError as error:
            blockers.add(f"{error}:{record.sample_index}")
            continue
        candidates.extend(cycle_candidates)
        proofs.extend(cycle_proofs)
        equivalent_cycles += int(equivalent)
        candidate_cycles += int(bool(cycle_candidates))

    accepted = 0
    pending = 0
    timed_out = 0
    last_observed = max((int(record.observed_at.timestamp()) for record in records), default=0)
    for candidate in candidates:
        if any(_converged(candidate, proof) for proof in proofs):
            accepted += 1
            continue
        deadline = candidate.ahead_timestamp + candidate.policy.convergence_timeout_seconds
        if last_observed <= deadline:
            pending += 1
        else:
            timed_out += 1
            blockers.add(f"SOAK_MONITORING_PARITY_CONVERGENCE_TIMEOUT:{candidate.domain}:{candidate.sample_index}")
    if pending:
        blockers.add("SOAK_MONITORING_PARITY_CONVERGENCE_PENDING")
    return blockers, {
        "cycle_count": len(unique),
        "equivalent_cycle_count": equivalent_cycles,
        "candidate_cycle_count": candidate_cycles,
        "candidate_difference_count": len(candidates),
        "accepted_difference_count": accepted,
        "pending_difference_count": pending,
        "timed_out_difference_count": timed_out,
        "invalid_or_unclassified_count": len([item for item in blockers if item != "SOAK_MONITORING_PARITY_CONVERGENCE_PENDING"]),
    }
