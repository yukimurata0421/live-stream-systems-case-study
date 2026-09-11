from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from stream_contracts.monitoring_v4.runtime_evidence import VerifiedRolloutEvidence
from stream_contracts.monitoring_v4.time import unix_ts
from stream_monitoring_v4.compatibility.parity_convergence import (
    SnapshotConvergenceCandidate,
    decoded_actual_sources,
    snapshot_candidate_converged,
    snapshot_convergence_candidate,
)

from .parity_contract import parity_payload_valid
from .policy import PARITY_CONVERGENCE_GRACE_SEC


@dataclass(frozen=True)
class _EquivalentProof:
    bucket_id: int
    expected_state: str
    actual_state: str
    expected_source: str
    expected_observed_at: str
    actual_observed_at: str
    actual_sources: tuple[str, ...]

    def as_detail(self) -> dict[str, Any]:
        return {
            "match": True,
            "classification": "equivalent",
            "expected_state": self.expected_state,
            "actual_state": self.actual_state,
            "expected_source": self.expected_source,
            "expected_observed_at": self.expected_observed_at,
            "actual_observed_at": self.actual_observed_at,
            "actual_sources": list(self.actual_sources),
            "normalization_error": "",
        }


@dataclass(frozen=True)
class _PendingDifference:
    bucket_id: int
    domain: str
    classification: str
    expected_source: str
    expected_observed_at: str
    actual_observed_at: str
    actual_sources: tuple[str, ...]
    rollout_evidence_id: str
    snapshot_candidate: SnapshotConvergenceCandidate | None


class ParityAccumulator:
    """Aggregate parity once, with cycle/domain de-duplication and bounded proof."""

    def __init__(self) -> None:
        self.buckets: set[int] = set()
        self.bucket_equivalent: dict[int, bool] = {}
        self.invalid_buckets: set[int] = set()
        self.rollouts: dict[str, VerifiedRolloutEvidence] = {}
        self.conflicting_rollout_ids: set[str] = set()
        self.equivalent_proofs: dict[str, set[_EquivalentProof]] = {}
        self.direct_violation_keys: set[tuple[int, str]] = set()
        self.pending: dict[tuple[int, str], _PendingDifference] = {}

    def add(
        self,
        parity: Mapping[str, Any],
        *,
        cycle_in_window: bool,
        bucket_id: int,
    ) -> bool:
        payload_valid = parity_payload_valid(parity)
        if cycle_in_window:
            self.buckets.add(bucket_id)
            if not payload_valid:
                self.invalid_buckets.add(bucket_id)
            self.bucket_equivalent[bucket_id] = (
                self.bucket_equivalent.get(bucket_id, True)
                and payload_valid
                and parity.get("equivalent") is True
            )
        # An invalid or out-of-window payload cannot contribute rollout or
        # convergence proof, even if some nested fields happen to look valid.
        if not cycle_in_window or not payload_valid:
            return payload_valid

        raw_rollouts = parity.get("verified_rollouts")
        for raw in raw_rollouts if isinstance(raw_rollouts, list) else []:
            if not isinstance(raw, Mapping):
                continue
            try:
                item = VerifiedRolloutEvidence.from_dict(raw)
            except (TypeError, ValueError):
                continue
            previous = self.rollouts.get(item.evidence_id)
            if previous is not None and previous != item:
                self.rollouts.pop(item.evidence_id, None)
                self.conflicting_rollout_ids.add(item.evidence_id)
            elif item.evidence_id not in self.conflicting_rollout_ids:
                self.rollouts[item.evidence_id] = item

        domains = parity.get("domains")
        assert isinstance(domains, Mapping)
        for raw_domain, raw_detail in domains.items():
            domain = str(raw_domain)
            detail = raw_detail if isinstance(raw_detail, Mapping) else {}
            key = (bucket_id, domain)
            classification = str(detail.get("classification", ""))
            if classification == "equivalent":
                proof = self._equivalent_proof(bucket_id, detail)
                if proof is not None and domain in {
                    "delivery",
                    "rendering",
                    "youtube_input_quality",
                }:
                    self.equivalent_proofs.setdefault(domain, set()).add(proof)
                continue
            if classification in {
                "candidate_source_snapshot_skew_convergence",
                "candidate_verified_planned_rollout_convergence",
            }:
                pending = self._pending_difference(
                    bucket_id,
                    domain,
                    classification,
                    detail,
                )
                previous = self.pending.get(key)
                if pending is None or (previous is not None and previous != pending):
                    self.pending.pop(key, None)
                    self.direct_violation_keys.add(key)
                elif key not in self.direct_violation_keys:
                    self.pending[key] = pending
                continue
            self.pending.pop(key, None)
            self.direct_violation_keys.add(key)
        return payload_valid

    @staticmethod
    def _equivalent_proof(
        bucket_id: int,
        detail: Mapping[str, Any],
    ) -> _EquivalentProof | None:
        actual_sources = decoded_actual_sources(detail.get("actual_sources"))
        if actual_sources is None:
            return None
        expected_state = detail.get("expected_state")
        actual_state = detail.get("actual_state")
        expected_source = detail.get("expected_source")
        expected_observed_at = detail.get("expected_observed_at")
        actual_observed_at = detail.get("actual_observed_at")
        if (
            not isinstance(expected_state, str)
            or not isinstance(actual_state, str)
            or not isinstance(expected_source, str)
            or not isinstance(expected_observed_at, str)
            or not isinstance(actual_observed_at, str)
            or detail.get("normalization_error") not in {"", None}
        ):
            return None
        try:
            unix_ts(expected_observed_at)
            unix_ts(actual_observed_at)
        except ValueError:
            return None
        return _EquivalentProof(
            bucket_id,
            expected_state,
            actual_state,
            expected_source,
            expected_observed_at,
            actual_observed_at,
            actual_sources,
        )

    @staticmethod
    def _pending_difference(
        bucket_id: int,
        domain: str,
        classification: str,
        detail: Mapping[str, Any],
    ) -> _PendingDifference | None:
        actual_sources = decoded_actual_sources(detail.get("actual_sources"))
        if actual_sources is None:
            return None
        expected_source = detail.get("expected_source")
        expected_observed_at = detail.get("expected_observed_at")
        actual_observed_at = detail.get("actual_observed_at")
        rollout_evidence_id = detail.get("rollout_evidence_id", "")
        if (
            not isinstance(expected_source, str)
            or not isinstance(expected_observed_at, str)
            or not isinstance(actual_observed_at, str)
            or not isinstance(rollout_evidence_id, str)
        ):
            return None
        snapshot_candidate = (
            snapshot_convergence_candidate(domain, detail)
            if classification == "candidate_source_snapshot_skew_convergence"
            else None
        )
        if (
            classification == "candidate_source_snapshot_skew_convergence"
            and snapshot_candidate is None
        ):
            return None
        return _PendingDifference(
            bucket_id,
            domain,
            classification,
            expected_source,
            expected_observed_at,
            actual_observed_at,
            actual_sources,
            rollout_evidence_id,
            snapshot_candidate,
        )

    def _rollout_converged(self, pending: _PendingDifference) -> bool:
        if not pending.rollout_evidence_id:
            return False
        if pending.rollout_evidence_id in self.conflicting_rollout_ids:
            return False
        rollout = self.rollouts.get(pending.rollout_evidence_id)
        if rollout is None:
            return False
        try:
            if not rollout.contains(pending.actual_observed_at):
                return False
            actual_ts = unix_ts(pending.actual_observed_at)
            deadline = unix_ts(rollout.expires_at) + PARITY_CONVERGENCE_GRACE_SEC
        except ValueError:
            return False
        for proof in self.equivalent_proofs.get(pending.domain, ()):
            if proof.bucket_id <= pending.bucket_id:
                continue
            if (
                proof.expected_source != pending.expected_source
                or proof.actual_sources != pending.actual_sources
            ):
                continue
            try:
                proof_actual_ts = unix_ts(proof.actual_observed_at)
            except ValueError:
                continue
            if actual_ts < proof_actual_ts <= deadline:
                return True
        return False

    def _snapshot_converged(self, pending: _PendingDifference) -> bool:
        candidate = pending.snapshot_candidate
        if candidate is None:
            return False
        return any(
            proof.bucket_id > pending.bucket_id
            and snapshot_candidate_converged(candidate, proof.as_detail())
            for proof in self.equivalent_proofs.get(pending.domain, ())
        )

    def totals(self) -> dict[str, int]:
        accepted = 0
        violations = len(self.direct_violation_keys)
        retrospectively_verified = 0
        retrospectively_verified_rollout = 0
        retrospectively_verified_snapshot = 0
        unconverged_candidates = 0
        for key, pending in self.pending.items():
            if key in self.direct_violation_keys:
                continue
            if (
                pending.classification
                == "candidate_verified_planned_rollout_convergence"
            ):
                converged = self._rollout_converged(pending)
                kind = "rollout"
            else:
                converged = self._snapshot_converged(pending)
                kind = "snapshot"
            if converged:
                accepted += 1
                retrospectively_verified += 1
                if kind == "rollout":
                    retrospectively_verified_rollout += 1
                else:
                    retrospectively_verified_snapshot += 1
            else:
                violations += 1
                unconverged_candidates += 1
        return {
            "accepted": accepted,
            "violations": violations,
            "equivalent": sum(self.bucket_equivalent.values()),
            "verified_rollout_evidence": len(self.rollouts),
            "conflicting_rollout_evidence": len(self.conflicting_rollout_ids),
            "retrospectively_verified": retrospectively_verified,
            "retrospectively_verified_rollout": retrospectively_verified_rollout,
            "retrospectively_verified_snapshot": retrospectively_verified_snapshot,
            "unconverged_candidates": unconverged_candidates,
            "payload_invalid": len(self.invalid_buckets),
        }
