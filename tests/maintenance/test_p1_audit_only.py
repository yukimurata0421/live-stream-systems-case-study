from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jsonschema

from cra_harness.oracles.p1_audit import scenario_violations
from cra_harness.runner.p1_audit import TARGET, deterministic_cases, negative_controls
from maintenance_audit import (
    EXACT_TARGET_FIELDS,
    AuditEmitter,
    AuditObservation,
    AuditPathRole,
    AuditPhase,
    AuditVerdict,
    audit_maintenance_decision,
    evaluate_audit_decision,
    set_global_emitter_for_tests,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def active_snapshot() -> dict[str, Any]:
    return {
        "schema_version": "maintenance.audit_state_snapshot.v1",
        "available": True,
        "observed_at": "2026-08-23T09:59:59.000Z",
        "fresh_until": "2026-08-23T10:05:00.000Z",
        "maintenance_state": "ESTABLISHED",
        "maintenance_id": "maintenance-p1",
        "maintenance_generation": 8,
        "authority_epoch": 27,
        "authority_session_id": "session-a",
        "target_identity": dict(TARGET),
        "authorizations": [],
    }


def observation(phase: AuditPhase = AuditPhase.ADMISSION) -> AuditObservation:
    return AuditObservation(
        path_id="MP-03",
        phase=phase,
        operation="RESTART_FFMPEG",
        path_role=AuditPathRole.NORMAL_MUTATOR,
        process_service="test-fast-recovery",
        target_identity=TARGET,
        operation_generation=8,
    )


def test_exact_target_identity_matches_protocol_v1_contract() -> None:
    schema = json.loads((PROJECT_ROOT / "contracts/cra_dell_recovery/v1/command.schema.json").read_text(encoding="utf-8"))
    protocol_fields = tuple(schema["$defs"]["target_identity"]["required"])
    assert set(EXACT_TARGET_FIELDS) == set(protocol_fields)
    assert EXACT_TARGET_FIELDS == (
        "host_id",
        "host_boot_id",
        "namespace",
        "pod_uid",
        "container_name",
        "container_id",
        "ffmpeg_generation",
        "ffmpeg_pid",
    )


def test_audit_schemas_accept_fixture_event_and_state() -> None:
    state_schema = json.loads((PROJECT_ROOT / "contracts/maintenance/v2/audit_state_snapshot.schema.json").read_text(encoding="utf-8"))
    event_schema = json.loads((PROJECT_ROOT / "contracts/maintenance/v2/audit_event.schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(state_schema)
    jsonschema.Draft202012Validator.check_schema(event_schema)
    jsonschema.Draft202012Validator(state_schema).validate(active_snapshot())

    events: list[dict[str, Any]] = []
    emitter = AuditEmitter(
        enabled=True,
        state_supplier=active_snapshot,
        event_writer=lambda event: events.append(dict(event)),
        queue_capacity=8,
        state_refresh_seconds=60.0,
        host="test-host",
    )
    try:
        deadline = datetime.now(UTC).timestamp() + 1.0
        while datetime.now(UTC).timestamp() < deadline:
            emitter.emit(observation())
            if emitter.wait_until_drained(0.2) and events:
                break
        assert events
        jsonschema.Draft202012Validator(event_schema).validate(events[-1])
        assert events[-1]["production_behavior_modified"] is False
    finally:
        emitter.close()


def test_active_maintenance_is_audit_block_but_never_enforcement() -> None:
    decision = evaluate_audit_decision(observation(), active_snapshot(), now=datetime(2026, 8, 23, 10, 0, 0, tzinfo=UTC))
    assert decision.verdict == AuditVerdict.WOULD_BLOCK
    assert decision.production_behavior_modified is False


def test_backend_failure_is_lost_evidence_not_production_control() -> None:
    def fail(_event: Mapping[str, Any]) -> None:
        raise OSError("injected")

    emitter = AuditEmitter(
        enabled=True,
        state_supplier=active_snapshot,
        event_writer=fail,
        queue_capacity=8,
        state_refresh_seconds=60.0,
    )
    production_trace: list[str] = []
    try:
        emitter.emit(observation(AuditPhase.EFFECT_BOUNDARY))
        assert emitter.wait_until_drained(1.0)
        production_trace.append("LEGACY_EFFECT_PATH_CONTINUED")
        assert emitter.health_snapshot()["lost"] >= 1
        assert production_trace == ["LEGACY_EFFECT_PATH_CONTINUED"]
    finally:
        emitter.close()


def test_live_observability_fields_and_health_snapshot_are_persisted(tmp_path: Path, monkeypatch: Any) -> None:
    event_path = tmp_path / "audit.jsonl"
    health_path = tmp_path / "health.json"
    monkeypatch.setenv("MAINTENANCE_AUDIT_EVENT_FILE", str(event_path))
    monkeypatch.setenv("MAINTENANCE_AUDIT_HEALTH_FILE", str(health_path))
    emitter = AuditEmitter(enabled=True, state_supplier=active_snapshot, state_refresh_seconds=60.0)
    try:
        deadline = datetime.now(UTC).timestamp() + 1.0
        while datetime.now(UTC).timestamp() < deadline:
            emitter.emit(observation())
            if emitter.wait_until_drained(0.2) and event_path.exists():
                break
    finally:
        emitter.close()

    event = json.loads(event_path.read_text(encoding="utf-8").splitlines()[-1])
    health = json.loads(health_path.read_text(encoding="utf-8"))
    assert event["process_instance_id"] == health["process_instance_id"]
    assert event["process_id"] == os.getpid()
    assert event["thread_id"] > 0
    assert event["monotonic_ns"] > 0
    assert event["audit_hook_latency_us"] >= 0
    assert event["queue_depth_before"] >= 0
    assert health["written"] >= 1
    assert health["lost"] == 0
    assert health["queue_high_watermark"] >= 1
    assert health["closed"] is True
    assert health["production_behavior_modified"] is False


def test_process_exit_drains_global_emitter(tmp_path: Path) -> None:
    event_path = tmp_path / "oneshot.jsonl"
    health_path = tmp_path / "oneshot-health.json"
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
            "MAINTENANCE_AUDIT_ENABLED": "1",
            "MAINTENANCE_AUDIT_EVENT_FILE": str(event_path),
            "MAINTENANCE_AUDIT_HEALTH_FILE": str(health_path),
        }
    )
    script = """
from maintenance_audit import audit_maintenance_decision
audit_maintenance_decision(
    path_id='MP-04', phase='ADMISSION', operation='PATCH_DEPLOYMENT',
    path_role='NORMAL_MUTATOR', process_service='oneshot-test',
)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        env=environment,
        capture_output=True,
        text=True,
        timeout=5.0,
    )
    assert completed.returncode == 0, completed.stderr
    event = json.loads(event_path.read_text(encoding="utf-8").splitlines()[-1])
    health = json.loads(health_path.read_text(encoding="utf-8"))
    assert event["audit_verdict"] == "UNKNOWN"
    assert health["written"] == 1
    assert health["lost"] == 0
    assert health["closed"] is True


def test_public_hook_contains_all_exceptions_and_returns_unknown() -> None:
    set_global_emitter_for_tests(AuditEmitter(enabled=False))
    try:
        result = audit_maintenance_decision(
            path_id="MP-03",
            phase="invalid-phase",
            operation="RESTART_FFMPEG",
            path_role="NORMAL_MUTATOR",
            process_service="test",
        )
        assert result.decision.verdict == AuditVerdict.UNKNOWN
        assert result.decision.production_behavior_modified is False
    finally:
        set_global_emitter_for_tests(None)


def test_deterministic_p1_and_independent_negative_controls() -> None:
    deterministic = deterministic_cases()
    controls = negative_controls()
    assert len(deterministic) == 10
    assert all(not scenario_violations(case) for case in deterministic)
    assert len(controls) == 10
    assert all(scenario_violations(case) for case in controls)


def test_inventory_keeps_unknown_and_partial_sources_explicit() -> None:
    inventory = json.loads((PROJECT_ROOT / "harness/fixtures/p1_production_mutation_paths.json").read_text(encoding="utf-8"))
    assert inventory["logical_path_count"] == 14
    assert inventory["manual_path_coverage"] == "PARTIAL"
    assert any(item["in_flight"]["status"] == "MISSING" for item in inventory["paths"])
    assert any(item["generation"]["status"] == "MISSING" for item in inventory["paths"])
