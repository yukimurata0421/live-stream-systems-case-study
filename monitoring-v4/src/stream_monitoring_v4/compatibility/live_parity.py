from __future__ import annotations

from typing import Any, Mapping, Sequence

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.runtime_evidence import RuntimeRolloutProjection
from stream_contracts.monitoring_v4.time import unix_ts

from .parity_convergence import (
    PARITY_CONVERGENCE_POLICY_REVISION,
    rollout_convergence_candidate_detail_valid,
    selected_actual_sources,
    snapshot_convergence_candidate,
)


def live_parity_report(
    actual: Sequence[DomainCurrent],
    expected: Mapping[str, Any],
    rollout_evidence: RuntimeRolloutProjection | None = None,
) -> dict[str, Any]:
    """Compare normalized legacy and v4 current without granting authority."""

    actual_groups: dict[str, list[DomainCurrent]] = {}
    for item in actual:
        actual_groups.setdefault(item.domain, []).append(item)
    actual_by_domain = {
        domain: items[0] for domain, items in actual_groups.items() if len(items) == 1
    }
    expected_by_domain: dict[str, Mapping[str, Any]] = {}
    expected_structure_errors: dict[str, str] = {}
    for raw_domain, raw_item in expected.items():
        domain = str(raw_domain)
        if not isinstance(raw_domain, str) or not raw_domain.strip():
            expected_structure_errors[domain] = "expected_domain_invalid"
        if isinstance(raw_item, Mapping):
            expected_by_domain[domain] = raw_item
        else:
            expected_by_domain[domain] = {}
            expected_structure_errors[domain] = "expected_item_not_object"

    domains: dict[str, dict[str, Any]] = {}
    violations = 0
    input_integrity_errors: list[str] = []
    for domain, items in actual_groups.items():
        if len(items) > 1:
            input_integrity_errors.append(f"duplicate_actual_domain:{domain}")
    for domain, error in expected_structure_errors.items():
        input_integrity_errors.append(f"{error}:{domain}")

    for domain in sorted(set(expected_by_domain) | set(actual_groups)):
        expected_item = expected_by_domain.get(domain, {})
        expected_state = str(expected_item.get("state", "<missing>"))
        actual_item = actual_by_domain.get(domain)
        duplicate_actual = len(actual_groups.get(domain, ())) > 1
        actual_state = (
            "<ambiguous>"
            if duplicate_actual
            else actual_item.state if actual_item is not None else "<missing>"
        )
        match = expected_state == actual_state
        normalization_error = expected_structure_errors.get(
            domain,
            str(expected_item.get("normalization_error", "")),
        )
        expected_observed_at = str(expected_item.get("observed_at", ""))
        actual_observed_at = actual_item.observed_at if actual_item is not None else ""
        actual_sources: tuple[str, ...] = ()
        actual_source_integrity_error = "actual_domain_missing"
        if duplicate_actual:
            actual_source_integrity_error = "actual_domain_duplicate"
        elif actual_item is not None:
            actual_sources, actual_source_integrity_error = selected_actual_sources(
                actual_item.payload
            )
        snapshot_skew_sec: int | None = None
        try:
            if expected_observed_at and actual_observed_at:
                snapshot_skew_sec = unix_ts(expected_observed_at) - unix_ts(
                    actual_observed_at
                )
        except ValueError:
            snapshot_skew_sec = None
        verified_rollout = None
        if domain in {"delivery", "rendering"} and actual_observed_at:
            for item in (
                rollout_evidence.evidence if rollout_evidence is not None else ()
            ):
                try:
                    if item.contains(actual_observed_at):
                        verified_rollout = item
                        break
                except ValueError:
                    continue
        detail: dict[str, Any] = {
            "expected_state": expected_state,
            "actual_state": actual_state,
            "match": match,
            "classification": "",
            "expected_source": str(expected_item.get("source", ""))[:96],
            "actual_sources": list(actual_sources),
            "expected_observed_at": expected_observed_at,
            "actual_observed_at": actual_observed_at,
            "snapshot_skew_sec": snapshot_skew_sec,
            "normalization_error": normalization_error[:96],
            "actual_source_integrity_error": actual_source_integrity_error,
            "rollout_evidence_id": verified_rollout.evidence_id if verified_rollout else "",
            "rollout_id": verified_rollout.rollout_id if verified_rollout else "",
            "convergence_policy_revision": PARITY_CONVERGENCE_POLICY_REVISION,
            "maximum_absolute_skew_sec": 0,
            "convergence_timeout_sec": 0,
        }
        snapshot_candidate = snapshot_convergence_candidate(domain, detail)
        if duplicate_actual:
            classification = "invalid_actual_domain_duplicate"
            violations += 1
        elif domain in expected_structure_errors:
            classification = "invalid_expected_domain_payload"
            violations += 1
        elif match:
            classification = "equivalent"
        elif verified_rollout is not None and rollout_convergence_candidate_detail_valid(
            domain,
            detail,
        ):
            # A rollout window alone is not proof that the mismatch was benign.
            # The revision-pinned report may accept this candidate only after a
            # later cycle converges inside the same bounded rollout window.
            classification = "candidate_verified_planned_rollout_convergence"
            violations += 1
        elif snapshot_candidate is not None:
            # Positive and negative skew are both possible because two files
            # from the same producer path are sampled independently. This
            # remains an immutable-cycle violation until a later cycle proves
            # that the lagging view passed the original ahead timestamp and
            # semantic parity returned inside the pair-specific bound.
            classification = "candidate_source_snapshot_skew_convergence"
            detail["maximum_absolute_skew_sec"] = (
                snapshot_candidate.policy.maximum_absolute_skew_sec
            )
            detail["convergence_timeout_sec"] = (
                snapshot_candidate.policy.convergence_timeout_sec
            )
            violations += 1
        else:
            classification = "unclassified_contract_difference"
            violations += 1
        detail["classification"] = classification
        domains[domain] = detail

    return {
        "schema": "monitoring_v4.live_parity.v2",
        "parity_policy_revision": PARITY_CONVERGENCE_POLICY_REVISION,
        "equivalent": bool(domains) and all(item["match"] for item in domains.values()),
        "accepted_difference_count": 0,
        "unclassified_contract_difference_count": violations,
        "input_integrity_errors": sorted(input_integrity_errors),
        "verified_rollouts": [
            item.to_dict()
            for item in (
                rollout_evidence.evidence if rollout_evidence is not None else ()
            )
        ],
        "domains": domains,
    }
