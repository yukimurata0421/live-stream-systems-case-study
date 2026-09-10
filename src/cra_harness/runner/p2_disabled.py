from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cra_harness.oracles.p2_disabled import p2_violations
from maintenance_audit import AuditObservation, AuditPathRole, AuditPhase
from maintenance_enforcement import evaluate_enforcement_shadow, evaluate_establishment_shadow

TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot-dell-1",
    "namespace": "stream-v3",
    "pod_uid": "pod-current",
    "container_name": "stream-engine",
    "container_id": "containerd://current",
    "ffmpeg_generation": "ffmpeg-current",
    "ffmpeg_pid": 4242,
}


def snapshot(*, active: bool, authorization: dict[str, Any] | None = None) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "available": True,
        "observed_at": now.isoformat(),
        "fresh_until": (now + timedelta(minutes=1)).isoformat(),
        "maintenance_state": "ESTABLISHED" if active else "INACTIVE",
        "maintenance_id": "maintenance-p2" if active else "",
        "maintenance_generation": 8,
        "authority_epoch": 30,
        "authority_session_id": "session-30",
        "target_identity": TARGET,
        "source_target_identity": TARGET,
        "target_snapshot_status": "VALID",
        "authorizations": [authorization] if authorization else [],
    }


def observation(*, planned: bool = False, phase: AuditPhase = AuditPhase.ADMISSION, generation: int = 8) -> AuditObservation:
    return AuditObservation(
        path_id="MP-07" if planned else "MP-04",
        phase=phase,
        operation="restart_deployment",
        path_role=AuditPathRole.PLANNED_EXECUTOR if planned else AuditPathRole.NORMAL_MUTATOR,
        process_service="p2-disabled-harness",
        resource_identity="deployment/stream-v3/stream-v3-runtime",
        target_identity=TARGET,
        operation_generation=generation,
        authorization_id="maintenance-auth-1" if planned else "",
    )


def valid_authorization() -> dict[str, Any]:
    return {
        "authorization_id": "maintenance-auth-1",
        "authorization_kind": "MAINTENANCE_MUTATION",
        "state": "ACCEPTED",
        "single_use": True,
        "use_count": 0,
        "maintenance_generation": 8,
        "executor_id": "planned_rollout_executor",
        "operation": "restart_deployment",
        "resource_identity": "deployment/stream-v3/stream-v3-runtime",
        "source_target_identity": TARGET,
        "expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
    }


def deterministic_results() -> list[dict[str, Any]]:
    missing = evaluate_enforcement_shadow(observation(), None)
    inactive = evaluate_enforcement_shadow(observation(), snapshot(active=False))
    active = evaluate_enforcement_shadow(observation(), snapshot(active=True))
    stale = evaluate_enforcement_shadow(observation(generation=7), snapshot(active=True))
    wrong = {**TARGET, "pod_uid": "pod-old"}
    wrong_observation = AuditObservation(**{**observation().__dict__, "target_identity": wrong})
    wrong_result = evaluate_enforcement_shadow(wrong_observation, snapshot(active=True))
    planned = evaluate_enforcement_shadow(observation(planned=True), snapshot(active=True, authorization=valid_authorization()))
    replayed = valid_authorization()
    replayed["use_count"] = 1
    replay = evaluate_enforcement_shadow(observation(planned=True), snapshot(active=True, authorization=replayed))
    ack = {"maintenance_generation": 8, "target_identity": TARGET, "in_flight_status": "CONFIRMED", "in_flight_count": 0}
    ack_ok = evaluate_establishment_shadow({"MP-04": ack}, ["MP-04"], maintenance_generation=8, target_identity=TARGET)
    ack_missing = evaluate_establishment_shadow({}, ["MP-04"], maintenance_generation=8, target_identity=TARGET)
    effect = evaluate_enforcement_shadow(observation(phase=AuditPhase.EFFECT_BOUNDARY), snapshot(active=True))
    values = [missing, inactive, active, stale, wrong_result, planned, replay, ack_ok, ack_missing, effect]
    return [
        {
            "scenario_id": f"P2-D-{index:02d}",
            "disposition": result.disposition.value,
            "reason_code": result.reason_code,
            "production_branch_signal": result.production_branch_signal,
            "production_adapter_connected": result.production_adapter_connected,
            "production_behavior_modified": result.production_behavior_modified,
            "physical_effect_count": result.physical_effect_count,
            "effect_boundary_revalidated": result.effect_boundary_revalidated,
        }
        for index, result in enumerate(values, start=1)
    ]


def negative_controls() -> list[dict[str, Any]]:
    base = {
        "kind": "MUTATION",
        "snapshot_available": True,
        "snapshot_fresh": True,
        "maintenance_active": True,
        "target_match": True,
        "generation_match": True,
        "planned": False,
        "authorization_present": False,
        "authorization_replayed": False,
        "authorization_expired": False,
    }
    return [
        {**base, "control_id": "NC-P2-01", "snapshot_available": False, "actual": "ALLOW"},
        {**base, "control_id": "NC-P2-02", "snapshot_available": False, "actual": "ALLOW"},
        {**base, "control_id": "NC-P2-03", "generation_match": False, "actual": "ALLOW"},
        {**base, "control_id": "NC-P2-04", "target_match": False, "actual": "ALLOW"},
        {
            "control_id": "NC-P2-05",
            "kind": "ACK_SET",
            "all_required_present": True,
            "in_flight_confirmed": False,
            "in_flight_count": 0,
            "actual": "ACK",
        },
        {
            "control_id": "NC-P2-06",
            "kind": "ACK_SET",
            "all_required_present": False,
            "in_flight_confirmed": True,
            "in_flight_count": 0,
            "actual": "ACK",
        },
        {**base, "control_id": "NC-P2-07", "actual": "ALLOW", "effect_boundary_revalidated": False},
        {
            **base,
            "control_id": "NC-P2-08",
            "planned": True,
            "authorization_present": True,
            "authorization_replayed": True,
            "actual": "ALLOW",
        },
        {
            **base,
            "control_id": "NC-P2-09",
            "planned": True,
            "authorization_present": True,
            "authorization_expired": True,
            "actual": "ALLOW",
        },
        {**base, "control_id": "NC-P2-10", "actual": "ALLOW"},
    ]


def run() -> dict[str, Any]:
    deterministic = deterministic_results()
    controls = deepcopy(negative_controls())
    control_results = [
        {
            "control_id": case["control_id"],
            "violations": p2_violations(case),
            "classification": "EXPECTED_INJECTED_FAILURE" if p2_violations(case) else "MISSED_INJECTED_FAILURE",
        }
        for case in controls
    ]
    all_deterministic_safe = all(
        item["production_branch_signal"] is None
        and item["production_adapter_connected"] is False
        and item["production_behavior_modified"] is False
        and item["physical_effect_count"] == 0
        for item in deterministic
    )
    detected = sum(item["classification"] == "EXPECTED_INJECTED_FAILURE" for item in control_results)
    return {
        "schema_version": "maintenance.p2_disabled_harness.v1",
        "enforcement_enabled": False,
        "deterministic_count": len(deterministic),
        "deterministic_safe": all_deterministic_safe,
        "negative_control_count": len(control_results),
        "negative_control_detected": detected,
        "independent_oracle": "PASS" if detected == len(control_results) else "FAIL",
        "physical_effect_count": 0,
        "production_adapter_connected": False,
        "production_behavior_modified": False,
        "status": "LOCAL_VERIFIED" if all_deterministic_safe and detected == len(control_results) else "PENDING",
        "deterministic": deterministic,
        "negative_controls": control_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run()
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    raise SystemExit(0 if result["status"] == "LOCAL_VERIFIED" else 1)


if __name__ == "__main__":
    main()
