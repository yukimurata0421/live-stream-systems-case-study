#!/usr/bin/env python3
"""Evaluate one Maintenance audit observation without invoking any mutator."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from maintenance_audit import AuditObservation, AuditPathRole, AuditPhase, evaluate_audit_decision


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--phase", choices=("ADMISSION", "EFFECT_BOUNDARY"), required=True)
    parser.add_argument("--operation-generation", type=int)
    parser.add_argument("--correlation-id", required=True)
    args = parser.parse_args()
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    target = snapshot.get("source_target_identity")
    observation = AuditObservation(
        path_id="MP-04",
        phase=AuditPhase(args.phase),
        operation="controlled_no_effect_restart_deployment_probe",
        path_role=AuditPathRole.NORMAL_MUTATOR,
        process_service="controlled-no-effect-maintenance-probe",
        resource_identity="deployment/stream-v3/stream-v3-runtime",
        correlation_id=args.correlation_id,
        target_identity=target if isinstance(target, dict) else None,
        operation_generation=args.operation_generation,
        in_flight_evidence={"status": "NOT_APPLICABLE", "count": None, "source": "no-effect probe"},
        generation_evidence={
            "status": "CONFIRMED" if args.operation_generation is not None else "MISSING",
            "source": "controlled maintenance shadow generation",
        },
    )
    decision = evaluate_audit_decision(observation, snapshot, now=datetime.now(UTC))
    classification = (
        "CONTROLLED_NO_EFFECT_STALE_WINDOW" if decision.reason_code == "STALE_MAINTENANCE_GENERATION" else "CONTROLLED_NO_EFFECT_BOUNDARY"
    )
    result = {
        "schema_version": "maintenance.controlled_no_effect_probe.v1",
        "correlation_id": args.correlation_id,
        "phase": args.phase,
        "operation_generation": args.operation_generation,
        "observed_maintenance_generation": snapshot.get("maintenance_generation"),
        "observed_maintenance_state": snapshot.get("maintenance_state"),
        "target_snapshot_status": snapshot.get("target_snapshot_status"),
        "audit_verdict": decision.verdict.value,
        "audit_reason": decision.reason_code,
        "physical_effect_count": 0,
        "production_behavior_modified": False,
        "probe_classification": classification,
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
