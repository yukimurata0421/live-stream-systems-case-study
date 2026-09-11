from __future__ import annotations

from typing import Any, Mapping

from stream_contracts.monitoring_v4.time import unix_ts
from stream_monitoring_v4.compatibility.parity_convergence import (
    PARITY_CONVERGENCE_POLICY_REVISION,
    rollout_convergence_candidate_detail_valid,
    snapshot_convergence_candidate,
)


def parity_payload_valid(parity: Mapping[str, Any]) -> bool:
    """Validate immutable per-cycle parity before it can become report proof."""

    if parity.get("schema") != "monitoring_v4.live_parity.v2":
        return False
    domains = parity.get("domains")
    if not isinstance(domains, Mapping) or not domains:
        return False
    integrity_errors = parity.get("input_integrity_errors", [])
    if not isinstance(integrity_errors, list) or not all(
        isinstance(item, str) and item for item in integrity_errors
    ):
        return False
    violations = 0
    all_match = True
    for raw_domain, raw_detail in domains.items():
        if not isinstance(raw_domain, str) or not raw_domain:
            return False
        if not isinstance(raw_detail, Mapping) or not isinstance(
            raw_detail.get("match"), bool
        ):
            return False
        match = raw_detail["match"] is True
        all_match = all_match and match
        classification_value = raw_detail.get("classification")
        if not isinstance(classification_value, str):
            return False
        classification = classification_value
        if match:
            if classification != "equivalent":
                return False
        elif classification == "candidate_source_snapshot_skew_convergence":
            if (
                parity.get("parity_policy_revision")
                != PARITY_CONVERGENCE_POLICY_REVISION
            ):
                return False
            candidate = snapshot_convergence_candidate(raw_domain, raw_detail)
            if candidate is None:
                return False
            if (
                raw_detail.get("convergence_policy_revision")
                != PARITY_CONVERGENCE_POLICY_REVISION
                or type(raw_detail.get("maximum_absolute_skew_sec")) is not int
                or raw_detail.get("maximum_absolute_skew_sec")
                != candidate.policy.maximum_absolute_skew_sec
                or type(raw_detail.get("convergence_timeout_sec")) is not int
                or raw_detail.get("convergence_timeout_sec")
                != candidate.policy.convergence_timeout_sec
            ):
                return False
            violations += 1
        elif classification == "candidate_verified_planned_rollout_convergence":
            if raw_domain not in {"delivery", "rendering"}:
                return False
            if not rollout_convergence_candidate_detail_valid(raw_domain, raw_detail):
                return False
            if not isinstance(raw_detail.get("rollout_evidence_id"), str) or not str(
                raw_detail.get("rollout_evidence_id", "")
            ):
                return False
            try:
                unix_ts(str(raw_detail.get("actual_observed_at", "")))
            except ValueError:
                return False
            violations += 1
        elif classification.startswith("accepted_"):
            # Immutable cycles cannot self-authorize an accepted difference.
            # Only the report accumulator may derive acceptance from a later
            # bounded proof.
            return False
        elif classification:
            violations += 1
        else:
            return False
    declared_accepted = parity.get("accepted_difference_count")
    declared_violations = parity.get("unclassified_contract_difference_count")
    if type(declared_accepted) is not int or type(declared_violations) is not int:
        return False
    return (
        parity.get("equivalent") is all_match
        and (not integrity_errors or not all_match)
        and declared_accepted == 0
        and declared_violations == violations
    )
