#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def evaluate(policy: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    violations: list[str] = []
    rows = {str(row.get("scenario_id")): row for row in policy.get("rows", [])}
    unsafe = rows.get("POL-07", {})
    candidate = unsafe.get("candidate", {})
    expected_change = unsafe.get("parity") == "INTENTIONAL_SAFETY_DIFFERENCE_NOT_ACTIVATED"

    checks = {
        "policy_complete": policy.get("complete") is True,
        "policy_oracle_pass": policy.get("independent_oracle_pass") is True,
        "policy_negative_controls_complete": policy.get("negative_controls_detected") == policy.get("negative_controls_total")
        and all(policy.get("negative_controls", {}).values()),
        "unexplained_policy_drift_zero": policy.get("unexplained_drift") == 0,
        "single_intentional_difference": policy.get("intentional_change_not_activated") == 1 and expected_change,
        "unsafe_identity_is_no_action": candidate.get("intent_type") == "NO_ACTION"
        and candidate.get("effect_owner") == "none"
        and candidate.get("effect_scope") == "none"
        and candidate.get("failure_domain") == "UNKNOWN"
        and candidate.get("automatic_retry") is False,
        "decision_accepted": decision.get("status") == "ACCEPTED" and decision.get("decision_id") == "DEC-08-SAFE-NO-ACTION-V11",
        "decision_release_exact": decision.get("release_id") == "runtime-boundary-20260825T0850JST-v11",
        "identity_fallback_forbidden": decision.get("identity_fallback_allowed") is False,
        "maintenance_p2_enforcement_forbidden": decision.get("maintenance_p2_enforcement_allowed") is False,
        "one_recreate_only": decision.get("maximum_controlled_recreate_count") == 1,
        "accepted_semantics_exact": decision.get("missing_stale_unknown_runtime_identity")
        == {
            "automatic_retry": False,
            "effect_scope": "none",
            "intent": "NO_ACTION",
            "physical_effect_count": 0,
        },
    }
    for name, passed in checks.items():
        if not passed:
            violations.append(name.upper())
    return {
        "schema_version": "option_bd.policy_acceptance_gate.v1",
        "release_id": decision.get("release_id"),
        "checks": checks,
        "violation_codes": violations,
        "accepted_intentional_difference_count": 1 if not violations else 0,
        "unexplained_policy_drift": policy.get("unexplained_drift"),
        "identity_fallback_allowed": decision.get("identity_fallback_allowed"),
        "maintenance_p2_enforcement_allowed": decision.get("maintenance_p2_enforcement_allowed"),
        "physical_effect_count": 0,
        "pass": not violations,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--decision", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(
        json.loads(args.policy.read_text(encoding="utf-8")),
        json.loads(args.decision.read_text(encoding="utf-8")),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
