from __future__ import annotations

import json
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from dell_recovery_agent.maintenance_snapshot import ShadowMaintenanceSnapshotCache
from maintenance_audit import AuditEmitter, AuditObservation, AuditPathRole, AuditPhase, AuditVerdict
from maintenance_shadow.snapshot import ShadowSnapshotProducer
from maintenance_shadow.store import MaintenanceShadowStore, ShadowState

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = PROJECT_ROOT / "migrations/maintenance_shadow/001_initial.sql"
TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot-1",
    "namespace": "stream-v3",
    "pod_uid": "pod-1",
    "container_name": "stream-engine",
    "container_id": "containerd://runtime-1",
    "ffmpeg_generation": "ffmpeg-1",
    "ffmpeg_pid": 4242,
}


def target_snapshot(now: datetime) -> dict[str, Any]:
    return {
        "schema": "cra_dell_recovery.target_snapshot.v1",
        "snapshot_id": "target-snapshot-1",
        "status": "VALID",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "valid_until": (now + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        "target_identity": TARGET,
    }


def make_store(tmp_path: Path) -> MaintenanceShadowStore:
    store = MaintenanceShadowStore(tmp_path / "maintenance.db", MIGRATION, producer_id="arena-maintenance-shadow")
    proof = store.reconcile_startup()
    assert proof.positive_inactive_proof
    return store


def test_wal_positive_inactive_proof_and_no_effect_rehearsal(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    assert store.connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert store.connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    identifier, generation = store.start_rehearsal(TARGET, maintenance_id="maintenance-evidence-1")
    for state in (
        ShadowState.QUIESCING,
        ShadowState.ESTABLISHED,
        ShadowState.ABORTING,
        ShadowState.ABORTED,
        ShadowState.INACTIVE,
    ):
        store.transition(identifier, generation, state, reason="DETERMINISTIC_NO_EFFECT_REHEARSAL")
    assert store.physical_effect_count() == 0
    assert store.startup_proof().positive_inactive_proof
    store.close()


def test_restart_with_unresolved_transaction_is_safe_blocked_not_inactive(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.start_rehearsal(TARGET, maintenance_id="maintenance-crash-window")
    store.close()
    restarted = MaintenanceShadowStore(tmp_path / "maintenance.db", MIGRATION, producer_id="arena-maintenance-shadow")
    before = dict(restarted.state())
    proof = restarted.reconcile_startup()
    after = dict(restarted.state())
    assert before["maintenance_state"] == "STARTUP_RECONCILING"
    assert after["maintenance_state"] == "SAFE_BLOCKED"
    assert proof.unresolved_transaction_count == 1
    assert not proof.positive_inactive_proof
    restarted.close()


def test_v2_snapshot_and_audit_event_bind_exact_target_and_host_identity(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    target_path = tmp_path / "target.json"
    target_path.write_text(json.dumps(target_snapshot(now)), encoding="utf-8")
    authority_path = tmp_path / "authority.json"
    authority_path.write_text(json.dumps({"authority_epoch": 44, "authority_session_id": "authority-session-44"}), encoding="utf-8")
    store = make_store(tmp_path)
    producer = ShadowSnapshotProducer(
        store,
        target_snapshot_path=target_path,
        authority_projection_path=authority_path,
        output_path=tmp_path / "snapshot.json",
        ttl_seconds=30,
    )
    snapshot = producer.publish()
    schema = json.loads((PROJECT_ROOT / "contracts/maintenance/v2/audit_state_snapshot.v2.schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema).validate(snapshot)
    assert snapshot["source_target_identity"] == TARGET
    assert snapshot["target_snapshot_status"] == "VALID"
    assert snapshot["proof"]["positive_inactive_proof"] is True

    events: list[dict[str, Any]] = []
    emitter = AuditEmitter(
        enabled=True,
        state_supplier=lambda: snapshot,
        event_writer=lambda event: events.append(dict(event)),
        state_refresh_seconds=0.01,
        host_id="arena-server",
        host_boot_id="arena-boot-1",
        os_hostname="yuki",
    )
    try:
        deadline = time.monotonic() + 1
        while not events and time.monotonic() < deadline:
            time.sleep(0.02)
            result = emitter.emit(
                AuditObservation(
                    path_id="MP-04",
                    phase=AuditPhase.ADMISSION,
                    operation="restart_deployment",
                    path_role=AuditPathRole.NORMAL_MUTATOR,
                    process_service="test-remote-recovery",
                    bind_source_target=True,
                )
            )
            emitter.wait_until_drained(0.2)
        assert result.decision.verdict == AuditVerdict.WOULD_ALLOW
        event = events[-1]
        event_schema = json.loads((PROJECT_ROOT / "contracts/maintenance/v2/audit_event.v2.schema.json").read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator(event_schema).validate(event)
        assert event["host_id"] == "arena-server"
        assert event["os_hostname"] == "yuki"
        assert event["target_identity"] == TARGET
        assert event["maintenance_snapshot_id"] == snapshot["snapshot_id"]
        assert event["production_behavior_modified"] is False
    finally:
        emitter.close()
        store.close()


def test_v1_snapshot_replay_remains_v1_and_host_argument_is_compatible() -> None:
    now = datetime.now(UTC)
    snapshot = {
        "schema_version": "maintenance.audit_state_snapshot.v1",
        "available": True,
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        "maintenance_state": "INACTIVE",
        "maintenance_id": "",
        "maintenance_generation": 0,
        "authority_epoch": 0,
        "authority_session_id": "",
        "target_identity": None,
        "authorizations": [],
    }
    events: list[dict[str, Any]] = []
    emitter = AuditEmitter(
        enabled=True,
        state_supplier=lambda: snapshot,
        event_writer=lambda event: events.append(dict(event)),
        host="legacy-host",
        state_refresh_seconds=0.01,
    )
    try:
        time.sleep(0.03)
        emitter.emit(
            AuditObservation(
                path_id="MP-04",
                phase=AuditPhase.ADMISSION,
                operation="restart_deployment",
                path_role=AuditPathRole.NORMAL_MUTATOR,
                process_service="legacy-replay",
            )
        )
        assert emitter.wait_until_drained(1)
        assert events[-1]["schema_version"] == "maintenance.audit_event.v1"
        assert events[-1]["host"] == "legacy-host"
        assert "host_id" not in events[-1]
    finally:
        emitter.close()


def test_active_v2_with_stale_target_is_unknown_not_allow(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    identifier, generation = store.start_rehearsal(TARGET)
    store.transition(identifier, generation, ShadowState.QUIESCING, reason="test")
    store.transition(identifier, generation, ShadowState.ESTABLISHED, reason="test")
    now = datetime.now(UTC)
    target_path = tmp_path / "stale-target.json"
    stale = target_snapshot(now - timedelta(minutes=5))
    target_path.write_text(json.dumps(stale), encoding="utf-8")
    snapshot = ShadowSnapshotProducer(
        store,
        target_snapshot_path=target_path,
        authority_projection_path=None,
        output_path=tmp_path / "snapshot.json",
        ttl_seconds=30,
    ).build()
    emitter = AuditEmitter(enabled=True, state_supplier=lambda: snapshot, event_writer=lambda _: None, state_refresh_seconds=0.01)
    try:
        time.sleep(0.03)
        result = emitter.emit(
            AuditObservation(
                path_id="MP-04",
                phase=AuditPhase.EFFECT_BOUNDARY,
                operation="restart_deployment",
                path_role=AuditPathRole.NORMAL_MUTATOR,
                process_service="test",
                target_identity=TARGET,
            )
        )
        assert result.decision.verdict == AuditVerdict.UNKNOWN
        assert result.decision.reason_code == "MAINTENANCE_TARGET_STALE"
    finally:
        emitter.close()
        store.close()


def test_database_is_real_sqlite_and_integrity_is_ok(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.close()
    connection = sqlite3.connect(tmp_path / "maintenance.db")
    try:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("SELECT count(*) FROM coordinator_transitions").fetchone()[0] == 1
    finally:
        connection.close()


def test_dell_cache_schema_rejects_corrupt_snapshot(tmp_path: Path) -> None:
    cache = ShadowMaintenanceSnapshotCache(
        tmp_path / "cache.json",
        PROJECT_ROOT / "contracts/maintenance/v2/audit_state_snapshot.v2.schema.json",
    )
    with pytest.raises(jsonschema.ValidationError):
        cache.accept(
            {
                "schema_version": "maintenance.audit_state_snapshot.v2",
                "snapshot_id": "corrupt-partial",
                "producer_id": "arena-maintenance-shadow",
                "producer_instance_id": "instance",
                "observed_at": datetime.now(UTC).isoformat(),
                "fresh_until": (datetime.now(UTC) + timedelta(seconds=10)).isoformat(),
                "physical_effect_count": 0,
                "production_behavior_modified": False,
            }
        )
    assert not (tmp_path / "cache.json").exists()
