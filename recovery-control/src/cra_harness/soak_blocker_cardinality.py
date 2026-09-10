from __future__ import annotations

import hashlib
import json
from typing import Any

from cra_no_action_soak.gate import _summarize_blockers


def run_blocker_cardinality_chaos(*, reason_count: int, samples_per_reason: int) -> dict[str, Any]:
    if isinstance(reason_count, bool) or not isinstance(reason_count, int) or reason_count < 1:
        raise ValueError("BLOCKER_CHAOS_REASON_COUNT_INVALID")
    if isinstance(samples_per_reason, bool) or not isinstance(samples_per_reason, int) or samples_per_reason < 1:
        raise ValueError("BLOCKER_CHAOS_SAMPLE_COUNT_INVALID")

    injected = {
        f"SOAK_SYNTHETIC_PERSISTENT_{reason:04d}:{sample}" for reason in range(reason_count) for sample in range(samples_per_reason)
    }
    blockers, evidence = _summarize_blockers(injected)
    detailed_injected = {f"SOAK_SYNTHETIC_DETAIL:{sample}:{'EVEN' if sample % 2 == 0 else 'ODD'}" for sample in range(samples_per_reason)}
    detail_blockers, detail_evidence = _summarize_blockers(detailed_injected)
    failures: list[str] = []
    if len(injected) != reason_count * samples_per_reason:
        failures.append("INJECTED_CARDINALITY_MISMATCH")
    if len(blockers) != reason_count:
        failures.append("BOUNDED_CARDINALITY_MISMATCH")
    for reason in range(reason_count):
        code = f"SOAK_SYNTHETIC_PERSISTENT_{reason:04d}"
        if evidence.get(code) != {
            "occurrence_count": samples_per_reason,
            "first_sample_index": 0,
            "last_sample_index": samples_per_reason - 1,
        }:
            failures.append(f"OCCURRENCE_EVIDENCE_MISMATCH:{reason:04d}")
    expected_detail_counts = {
        "SOAK_SYNTHETIC_DETAIL:EVEN": (samples_per_reason + 1) // 2,
        "SOAK_SYNTHETIC_DETAIL:ODD": samples_per_reason // 2,
    }
    if detail_blockers != sorted(expected_detail_counts):
        failures.append("DETAIL_IDENTITY_COLLAPSED")
    for code, expected_count in expected_detail_counts.items():
        if detail_evidence.get(code, {}).get("occurrence_count") != expected_count:
            failures.append(f"DETAIL_OCCURRENCE_EVIDENCE_MISMATCH:{code}")
    canonical = json.dumps(
        {"blockers": blockers, "evidence": evidence},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return {
        "schema": "cra.soak_blocker_cardinality_chaos.v1",
        "classification": "PASS" if not failures else "FAIL",
        "reason_count": reason_count,
        "samples_per_reason": samples_per_reason,
        "injected_blocker_count": len(injected),
        "bounded_blocker_count": len(blockers),
        "cardinality_reduction_ratio": len(injected) / len(blockers),
        "semantic_detail_blocker_count": len(detail_blockers),
        "semantic_detail_identity_preserved": detail_blockers == sorted(expected_detail_counts),
        "summary_sha256": hashlib.sha256(canonical).hexdigest(),
        "failure_count": len(failures),
        "failures": failures,
        "physical_effect_count": 0,
        "production_target_touched": False,
    }
