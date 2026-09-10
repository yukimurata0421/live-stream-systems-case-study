#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    reason_kind: str
    observation: dict[str, Any]
    old_scope: str
    expected_intent: str
    expected_scope: str
    expected_parity: str


def runtime_observation(*, lifecycle: str, cardinality: str, valid: bool = True) -> dict[str, Any]:
    return {
        "runtime_snapshot_status": "VALID" if valid else "UNKNOWN",
        "runtime_identity": {"oracle_presence_only": True} if valid else None,
        "runtime_lifecycle_state": lifecycle,
        "managed_child_cardinality": cardinality,
    }


SCENARIOS = (
    Scenario("POL-01", "tcp_stall", {}, "ffmpeg_child", "RESTART_FFMPEG", "ffmpeg_child", "EXACT_PARITY"),
    Scenario("POL-02", "remote_warning", {}, "ffmpeg_child", "RESTART_FFMPEG", "ffmpeg_child", "EXACT_PARITY"),
    Scenario(
        "POL-03",
        "ffmpeg_missing",
        runtime_observation(lifecycle="RESTART_DELAY", cardinality="0"),
        "runtime",
        "RECONCILE_FFMPEG",
        "ffmpeg_child_convergence",
        "SEMANTIC_PARITY",
    ),
    Scenario(
        "POL-04",
        "ffmpeg_missing",
        runtime_observation(lifecycle="FFMPEG_EXITED", cardinality="0"),
        "runtime",
        "ESCALATE_RUNTIME_RECOVERY",
        "runtime",
        "EXACT_SCOPE_PARITY",
    ),
    Scenario(
        "POL-05",
        "ffmpeg_missing",
        runtime_observation(lifecycle="FFMPEG_RUNNING", cardinality="1"),
        "runtime",
        "NO_ACTION",
        "none",
        "SEMANTIC_PARITY_OBSERVATION_SKEW",
    ),
    Scenario(
        "POL-06",
        "ffmpeg_missing",
        runtime_observation(lifecycle="RESTART_DELAY", cardinality="2+"),
        "runtime",
        "NO_ACTION",
        "none",
        "UNSUPPORTED_SAFE_BLOCK",
    ),
    Scenario(
        "POL-07",
        "ffmpeg_missing",
        runtime_observation(lifecycle="UNKNOWN", cardinality="UNKNOWN", valid=False),
        "runtime",
        "NO_ACTION",
        "none",
        "INTENTIONAL_SAFETY_DIFFERENCE_NOT_ACTIVATED",
    ),
    Scenario("POL-08", "network_down", {}, "none", "NO_ACTION", "none", "EXACT_PARITY"),
    Scenario("POL-09", "low_upload_pressure", {}, "none", "NO_ACTION", "none", "EXACT_PARITY"),
    Scenario("POL-10", "healthy", {}, "none", "NO_ACTION", "none", "EXACT_PARITY"),
    Scenario("POL-11", "startup_transient", {}, "none", "NO_ACTION", "none", "EXACT_PARITY"),
    Scenario("POL-12", "target_unavailable", {}, "none", "NO_ACTION", "none", "EXACT_PARITY"),
)


def independent_oracle(scenario: Scenario) -> dict[str, str]:
    # Intentionally does not import the production selector, validators or comparator.
    return {
        "intent_type": scenario.expected_intent,
        "effect_scope": scenario.expected_scope,
        "parity": scenario.expected_parity,
    }


def parity(scenario: Scenario, candidate: dict[str, Any]) -> str:
    intent = str(candidate.get("intent_type") or "")
    scope = str(candidate.get("effect_scope") or "")
    if intent != scenario.expected_intent or scope != scenario.expected_scope:
        return "UNEXPLAINED_DRIFT"
    return scenario.expected_parity


def source_inventory(v3_root: Path) -> dict[str, Any]:
    fast_path = v3_root / "src/watchers/fast_recovery.py"
    decision_path = v3_root / "src/watchers/fast_recovery_core/decision.py"
    fast_text = fast_path.read_text(encoding="utf-8")
    decision_tree = ast.parse(decision_path.read_text(encoding="utf-8"))
    literal_reasons: set[str] = set()
    for node in ast.walk(decision_tree):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple) and node.value.elts:
            first = node.value.elts[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                literal_reasons.add(first.value)
    modeled = {row.reason_kind for row in SCENARIOS}
    physical_calls = {name: fast_text.count(name) for name in ("restart_stream(", "restart_ffmpeg_child(", "os.kill(", ".terminate(")}
    reachable_restart_reasons = sorted(reason for reason in literal_reasons if reason)
    return {
        "selector_literal_reasons": sorted(literal_reasons),
        "reachable_restart_reasons": reachable_restart_reasons,
        "modeled_reasons": sorted(modeled),
        "unmodeled_restart_reasons": sorted(set(reachable_restart_reasons) - modeled),
        "network_down_pre_selector_return": fast_text.find("if network.network_down:")
        < fast_text.find("reason_kind, reason = recovery_decision.select_restart_reason("),
        "physical_call_token_counts": physical_calls,
        "legacy_gate_tokens": {
            "cooldown": "RESTART_GUARD_SEC" in fast_text and "restart guard active" in fast_text,
            "budget": "HOURLY_DOWNTIME_BUDGET_SEC" in fast_text
            and "DAILY_DOWNTIME_BUDGET_SEC" in fast_text
            and "used_downtime_budget_sec" in fast_text,
        },
    }


def negative_controls(rows: list[dict[str, Any]]) -> dict[str, bool]:
    by_id = {row["scenario_id"]: row for row in rows}

    def drift(scenario_id: str, **mutation: str) -> bool:
        scenario = next(item for item in SCENARIOS if item.scenario_id == scenario_id)
        candidate = dict(by_id[scenario_id]["candidate"])
        candidate.update(mutation)
        return parity(scenario, candidate) == "UNEXPLAINED_DRIFT"

    return {
        "NC-PS-01": drift("POL-03", intent_type="RESTART_FFMPEG"),
        "NC-PS-02": drift("POL-03", effect_scope="arbitrary_pid"),
        "NC-PS-03": drift("POL-05", intent_type="RECONCILE_FFMPEG"),
        "NC-PS-04": drift("POL-06", intent_type="RECONCILE_FFMPEG"),
        "NC-PS-05": all(row["candidate"]["automatic_retry"] is False for row in rows),
        "NC-PS-06": by_id["POL-07"]["candidate"]["intent_type"] == "NO_ACTION",
        "NC-PS-07": by_id["POL-07"]["candidate"]["intent_type"] == "NO_ACTION",
        "NC-PS-08": drift("POL-01", intent_type="RECONCILE_FFMPEG"),
        "NC-PS-09": drift("POL-08", intent_type="RESTART_FFMPEG"),
        "NC-PS-10": drift("POL-08", intent_type="ESCALATE_RUNTIME_RECOVERY"),
        "NC-PS-11": drift("POL-01", effect_scope="runtime"),
        "NC-PS-12": False,
        "NC-PS-13": False,
        "NC-PS-14": by_id["POL-04"]["candidate"]["effect_owner"] == "legacy-runtime-recovery-adapter",
        "NC-PS-15": by_id["POL-07"]["candidate"]["failure_domain"] == "UNKNOWN",
    }


def spec_consistency(spec_path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec_reasons = {str(row["reason_kind"]) for row in spec["rows"]}
    implementation_reasons = {str(row["reason_kind"]) for row in rows}
    return {
        "schema_version": spec.get("schema_version"),
        "missing_in_spec": sorted(implementation_reasons - spec_reasons),
        "missing_in_implementation": sorted(spec_reasons - implementation_reasons),
        "pass": spec_reasons == implementation_reasons,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v3-root", type=Path, required=True)
    parser.add_argument("--recovery-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.v3_root / "src"))
    from watchers.fast_recovery_core.policy import select_recovery_intent

    rows: list[dict[str, Any]] = []
    for scenario in SCENARIOS:
        candidate = select_recovery_intent(
            scenario.reason_kind,
            runtime_observation=scenario.observation,
        ).to_dict()
        oracle = independent_oracle(scenario)
        rows.append(
            {
                "scenario_id": scenario.scenario_id,
                "reason_kind": scenario.reason_kind,
                "old_scope": scenario.old_scope,
                "candidate": candidate,
                "oracle": oracle,
                "oracle_match": candidate["intent_type"] == oracle["intent_type"] and candidate["effect_scope"] == oracle["effect_scope"],
                "parity": parity(scenario, candidate),
            }
        )
    controls = negative_controls(rows)
    inventory = source_inventory(args.v3_root)
    controls["NC-PS-12"] = bool(inventory["legacy_gate_tokens"]["cooldown"])
    controls["NC-PS-13"] = bool(inventory["legacy_gate_tokens"]["budget"])
    consistency = spec_consistency(args.recovery_root / "policy/fast_recovery_policy_v2.json", rows)
    unexplained = sum(row["parity"] == "UNEXPLAINED_DRIFT" for row in rows)
    intentional_not_activated = sum(row["parity"] == "INTENTIONAL_SAFETY_DIFFERENCE_NOT_ACTIVATED" for row in rows)
    terminal = {
        "schema_version": "fast_recovery.policy_harness.v1",
        "scenario_count": len(rows),
        "rows": rows,
        "source_inventory": inventory,
        "spec_consistency": consistency,
        "negative_controls": controls,
        "negative_controls_detected": sum(controls.values()),
        "negative_controls_total": len(controls),
        "independent_oracle_pass": all(row["oracle_match"] for row in rows),
        "unexplained_drift": unexplained,
        "intentional_change_not_activated": intentional_not_activated,
        "migration_policy_gate": "BLOCKED" if intentional_not_activated or unexplained else "PASS",
        "complete": all(
            (
                all(row["oracle_match"] for row in rows),
                all(controls.values()),
                not inventory["unmodeled_restart_reasons"],
                inventory["network_down_pre_selector_return"],
                consistency["pass"],
                unexplained == 0,
            )
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(terminal, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in terminal.items() if key != "rows"}, sort_keys=True))
    return 0 if terminal["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
