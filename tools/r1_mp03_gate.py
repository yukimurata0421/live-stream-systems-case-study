#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cra_harness.oracles.r1_mp03 import expected_mp03_verdict, injected_control_violations
from maintenance_audit import AuditObservation, AuditPathRole, AuditPhase, evaluate_audit_decision
from maintenance_enforcement import evaluate_enforcement_shadow

TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot-current",
    "namespace": "stream-v3",
    "pod_uid": "pod-current",
    "container_name": "stream-engine",
    "container_id": "containerd://stream-engine-current",
    "ffmpeg_generation": "ffmpeg-current",
    "ffmpeg_pid": 4100,
}


def _snapshot(case: dict[str, Any], now: datetime) -> dict[str, Any] | None:
    if not case["maintenance_available"]:
        return None
    expected = dict(TARGET)
    observed = dict(TARGET)
    if not case["target_valid"]:
        expected["ffmpeg_pid"] = 0
    if not case["target_match"]:
        observed["ffmpeg_pid"] = 4200
    case["observed_target"] = observed
    return {
        "available": True,
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(seconds=30) if case["maintenance_fresh"] else now - timedelta(seconds=1))
        .isoformat()
        .replace("+00:00", "Z"),
        "maintenance_state": "ESTABLISHED" if case["maintenance_active"] else "INACTIVE",
        "maintenance_id": "maintenance-r1",
        "maintenance_generation": 8,
        "authority_epoch": 34,
        "authority_session_id": "session-r1",
        "target_identity": expected,
        "authorizations": [],
    }


def deterministic_results() -> list[dict[str, Any]]:
    cases = [
        ("R1-O-01-inactive", False, True, True, True, True, "OBSERVATION"),
        ("R1-O-02-active", True, True, True, True, True, "ADMISSION"),
        ("R1-O-03-effect", True, True, True, True, True, "EFFECT_BOUNDARY"),
        ("R1-O-04-missing", False, False, True, True, True, "OBSERVATION"),
        ("R1-O-05-stale", False, True, False, True, True, "OBSERVATION"),
        ("R1-O-06-target-unknown", True, True, True, False, True, "ADMISSION"),
        ("R1-O-07-target-drift", True, True, True, True, False, "ADMISSION"),
        ("R1-O-08-generation-drift", True, True, True, True, True, "EFFECT_BOUNDARY"),
    ]
    now = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for name, active, available, fresh, target_valid, target_match, phase in cases:
        generation_match = name != "R1-O-08-generation-drift"
        fixture = {
            "maintenance_active": active,
            "maintenance_available": available,
            "maintenance_fresh": fresh,
            "target_valid": target_valid,
            "target_match": target_match,
            "generation_match": generation_match,
            "phase": phase,
        }
        snapshot = _snapshot(fixture, now)
        observation = AuditObservation(
            path_id="MP-03",
            phase=AuditPhase(phase),
            operation="restart_ffmpeg",
            path_role=AuditPathRole.NORMAL_MUTATOR,
            process_service="fast-recovery-loop",
            target_identity=fixture.get("observed_target", TARGET),
            operation_generation=8 if generation_match else 7,
        )
        audit = evaluate_audit_decision(observation, snapshot, now=now)
        p2 = evaluate_enforcement_shadow(observation, snapshot, enforcement_enabled=False)
        actual = {
            "audit_verdict": audit.verdict.value,
            "audit_reason": audit.reason_code,
            "p2_disabled_verdict": p2.disposition.value,
            "effect_boundary_revalidated": p2.effect_boundary_revalidated,
            "production_behavior_modified": p2.production_behavior_modified,
            "production_branch_signal": p2.production_branch_signal,
            "physical_effect_count": p2.physical_effect_count,
        }
        expected = expected_mp03_verdict(fixture)
        rows.append({"case": name, "expected": expected, "actual": actual, "pass": actual == expected})
    return rows


def negative_control_results() -> list[dict[str, Any]]:
    controls = [
        ("NC-R1-01", {"audit_changed_production_decision": True}),
        ("NC-R1-02", {"p2_blocked_action": True}),
        ("NC-R1-03", {"audit_exception_stopped_loop": True}),
        ("NC-R1-04", {"snapshot_status": "MISSING", "audit_verdict": "WOULD_ALLOW"}),
        ("NC-R1-05", {"target_status": "STALE", "target_treated_exact": True}),
        ("NC-R1-06", {"host_id_source": "OS_HOSTNAME"}),
        ("NC-R1-07", {"native_id_used_as_maintenance_generation": True}),
        ("NC-R1-08", {"audit_absence_used_as_in_flight_zero": True}),
        ("NC-R1-09", {"phase": "EFFECT_BOUNDARY", "effect_boundary_revalidated": False}),
        ("NC-R1-10", {"enforcement_enabled": True}),
    ]
    return [
        {
            "control": name,
            "injected": injected,
            "detected_violations": injected_control_violations(injected),
            "classification": "EXPECTED_INJECTED_FAILURE" if injected_control_violations(injected) else "MISSED",
        }
        for name, injected in controls
    ]


def static_non_interference(source: Path) -> dict[str, Any]:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    audit_call_parents: list[str] = []

    def contains_audit_call(value: ast.AST | None) -> bool:
        return value is not None and any(
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Name)
            and child.func.id in {"audit_mp03", "audit_maintenance_decision", "evaluate_enforcement_shadow"}
            for child in ast.walk(value)
        )

    for node in ast.walk(tree):
        value: ast.AST | None = None
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.Return)):
            value = node.value
        elif isinstance(node, (ast.If, ast.While)):
            value = node.test
        if contains_audit_call(value):
            audit_call_parents.append(type(node).__name__)
    return {
        "source": str(source),
        "audit_or_p2_used_by_assignment_return_or_branch": len(audit_call_parents),
        "contexts": audit_call_parents,
        "pass": not audit_call_parents,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fast-recovery-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    deterministic = deterministic_results()
    controls = negative_control_results()
    static = static_non_interference(args.fast_recovery_source)
    payload = {
        "schema_version": "recovery_control.r1_mp03_gate.v1",
        "deterministic": deterministic,
        "negative_controls": controls,
        "static_non_interference": static,
        "deterministic_pass": all(row["pass"] for row in deterministic),
        "negative_controls_detected": sum(row["classification"] == "EXPECTED_INJECTED_FAILURE" for row in controls),
        "negative_control_count": len(controls),
        "independent_oracle_imports_production_evaluator": False,
        "pass": all(row["pass"] for row in deterministic)
        and all(row["classification"] == "EXPECTED_INJECTED_FAILURE" for row in controls)
        and static["pass"],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
