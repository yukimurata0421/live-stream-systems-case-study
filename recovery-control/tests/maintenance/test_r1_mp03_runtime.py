from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jsonschema

from maintenance_audit import AuditEmitter, AuditObservation, AuditPathRole, AuditPhase

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
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def snapshot(*, active: bool = False) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "available": True,
        "snapshot_id": "snapshot-r1",
        "producer_id": "independent-test-producer",
        "producer_instance_id": "producer-instance-r1",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
        "maintenance_state": "ESTABLISHED" if active else "INACTIVE",
        "maintenance_id": "maintenance-r1" if active else "",
        "maintenance_generation": 8,
        "authority_epoch": 34,
        "authority_session_id": "authority-session-r1",
        "target_identity": dict(TARGET),
        "source_target_identity": dict(TARGET),
        "target_snapshot_id": "target-snapshot-r1",
        "target_snapshot_status": "VALID",
        "target_snapshot_age_seconds": 0.1,
        "authorizations": [],
    }


def observation(phase: AuditPhase = AuditPhase.OBSERVATION) -> AuditObservation:
    return AuditObservation(
        path_id="MP-03",
        phase=phase,
        operation="observe_fast_recovery_loop" if phase == AuditPhase.OBSERVATION else "restart_ffmpeg",
        path_role=AuditPathRole.NORMAL_MUTATOR,
        process_service="fast-recovery-loop",
        correlation_id="mp03-r1-native-operation",
        bind_source_target=True,
        p2_disabled_evaluation=True,
        native_operation_id="mp03-r1-native-operation",
        native_operation_generation="native-generation-r1",
        actual_production_decision="LEGACY_EFFECT_CALL_PROCEEDS",
        in_flight_evidence={"status": "PROPOSED", "count": 0},
        generation_evidence={"status": "PROPOSED", "native_token": "native-generation-r1"},
    )


def emit_once(monkeypatch: Any, *, active: bool, phase: AuditPhase) -> tuple[dict[str, Any], dict[str, Any]]:
    monkeypatch.setenv("MAINTENANCE_ENFORCEMENT_ENABLED", "0")
    events: list[dict[str, Any]] = []
    emitter = AuditEmitter(
        enabled=True,
        state_supplier=lambda: snapshot(active=active),
        event_writer=lambda event: events.append(dict(event)),
        state_refresh_seconds=0.01,
        host_id="dell-yuki",
        host_boot_id="boot-current",
        os_hostname="runtime-hostname",
    )
    try:
        time.sleep(0.03)
        result = emitter.emit(observation(phase))
        assert result.evidence_enqueued is True
        assert emitter.wait_until_drained(1.0)
        assert len(events) == 1
        return events[0], emitter.health_snapshot()
    finally:
        emitter.close()


def test_mp03_inactive_loop_binds_identity_and_p2_allow(monkeypatch: Any) -> None:
    event, health = emit_once(monkeypatch, active=False, phase=AuditPhase.OBSERVATION)
    schema = json.loads((PROJECT_ROOT / "contracts/maintenance/v2/audit_event.v2.schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema).validate(event)
    assert event["audit_verdict"] == "WOULD_ALLOW"
    assert event["p2_disabled_verdict"] == "ALLOW"
    assert event["target_identity"] == TARGET
    assert event["host_id"] == "dell-yuki"
    assert event["os_hostname"] == "runtime-hostname"
    assert event["native_operation_id"] == "mp03-r1-native-operation"
    assert event["production_behavior_modified"] is False
    assert event["p2_production_branch_signal"] is None
    assert event["p2_production_adapter_connected"] is False
    assert health["p2_disabled_evaluations"] == 1
    assert health["p2_disabled_failures"] == 0


def test_mp03_active_effect_is_would_block_but_legacy_proceeds(monkeypatch: Any) -> None:
    event, _health = emit_once(monkeypatch, active=True, phase=AuditPhase.EFFECT_BOUNDARY)
    assert event["audit_verdict"] == "WOULD_BLOCK"
    assert event["p2_disabled_verdict"] == "BLOCK"
    assert event["p2_effect_boundary_revalidated"] is True
    assert event["actual_production_decision"] == "LEGACY_EFFECT_CALL_PROCEEDS"
    assert event["production_behavior_modified"] is False
    assert event["p2_production_behavior_modified"] is False
    assert event["p2_physical_effect_count"] == 0


def test_misconfigured_p2_true_is_contained_without_branch_signal(monkeypatch: Any) -> None:
    monkeypatch.setenv("MAINTENANCE_ENFORCEMENT_ENABLED", "1")
    events: list[dict[str, Any]] = []
    emitter = AuditEmitter(
        enabled=True,
        state_supplier=lambda: snapshot(active=True),
        event_writer=lambda event: events.append(dict(event)),
        state_refresh_seconds=0.01,
    )
    try:
        time.sleep(0.03)
        result = emitter.emit(observation(AuditPhase.EFFECT_BOUNDARY))
        assert result.evidence_enqueued is True
        assert emitter.wait_until_drained(1.0)
        assert events[0]["p2_disabled_verdict"] == "UNKNOWN"
        assert events[0]["p2_disabled_reason"] == "P2_DISABLED_EVALUATION_ERROR"
        assert events[0]["actual_production_decision"] == "LEGACY_EFFECT_CALL_PROCEEDS"
        assert emitter.health_snapshot()["p2_disabled_failures"] == 1
    finally:
        emitter.close()
