from __future__ import annotations

from datetime import UTC, datetime

from maintenance_audit import (
    AuditEmitter,
    AuditObservation,
    AuditPathRole,
    AuditPhase,
    AuditVerdict,
    audit_maintenance_decision,
    evaluate_audit_decision,
    set_global_emitter_for_tests,
)

TARGET = {
    "host_id": "dell",
    "host_boot_id": "boot-a",
    "namespace": "stream-v3",
    "pod_uid": "pod-a",
    "container_name": "stream-engine",
    "container_id": "container-a",
    "ffmpeg_generation": "run-a:0:4100",
    "ffmpeg_pid": 4100,
}


def snapshot() -> dict[str, object]:
    return {
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


def test_audit_would_block_never_changes_production_behavior() -> None:
    decision = evaluate_audit_decision(
        AuditObservation(
            path_id="MP-03",
            phase=AuditPhase.EFFECT_BOUNDARY,
            operation="RESTART_FFMPEG",
            path_role=AuditPathRole.NORMAL_MUTATOR,
            process_service="fast-recovery-loop",
            target_identity=TARGET,
            operation_generation=8,
        ),
        snapshot(),
        now=datetime(2026, 8, 23, 10, 0, 0, tzinfo=UTC),
    )
    assert decision.verdict == AuditVerdict.WOULD_BLOCK
    assert decision.production_behavior_modified is False


def test_disabled_hook_is_constant_time_noop_contract() -> None:
    set_global_emitter_for_tests(AuditEmitter(enabled=False))
    try:
        result = audit_maintenance_decision(
            path_id="MP-03",
            phase="ADMISSION",
            operation="RESTART_FFMPEG",
            path_role="NORMAL_MUTATOR",
            process_service="fast-recovery-loop",
        )
        assert result.decision.verdict == AuditVerdict.NOT_APPLICABLE
        assert result.evidence_status == "DISABLED"
        assert result.decision.production_behavior_modified is False
    finally:
        set_global_emitter_for_tests(None)


def test_malformed_hook_input_is_contained_as_unknown() -> None:
    set_global_emitter_for_tests(AuditEmitter(enabled=False))
    try:
        result = audit_maintenance_decision(
            path_id="MP-03",
            phase="not-a-phase",
            operation="RESTART_FFMPEG",
            path_role="NORMAL_MUTATOR",
            process_service="fast-recovery-loop",
        )
        assert result.decision.verdict == AuditVerdict.UNKNOWN
        assert result.decision.production_behavior_modified is False
    finally:
        set_global_emitter_for_tests(None)
