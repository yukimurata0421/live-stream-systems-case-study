from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cra_dell_recovery.effect_scope import effect_scope_id
from cra_dell_recovery.errors import LedgerUnavailable
from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.storage import DellStore
from runtime_boundary import EffectLedger, EffectRequest

ROOT = Path(__file__).resolve().parents[2]


def _runtime_request() -> EffectRequest:
    now = datetime.now(UTC)
    return EffectRequest.from_mapping(
        {
            "schema_version": "runtime.effect_request.v1",
            "request_id": "legacy-runtime-request",
            "producer_id": "old",
            "producer_generation": 1,
            "operation": "restart_ffmpeg",
            "reason": "legacy fixture",
            "issued_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=5)).isoformat(),
            "target_identity": {
                "host_id": "dell",
                "host_boot_id": "boot-a",
                "namespace": "stream-v3",
                "pod_uid": "pod-a",
                "container_name": "stream-engine",
                "container_id": "containerd://a",
                "ffmpeg_generation": "generation-a",
                "ffmpeg_pid": 4100,
            },
            "expected_ffmpeg_generation": "generation-a",
            "idempotency_key": "legacy-runtime-request",
            "correlation_id": "legacy-runtime-request",
            "target_snapshot_id": "snapshot-a",
            "runtime_observation_id": "observation-a",
            "expected_executor_instance_id": "executor-a",
            "maintenance_evidence_status": "AVAILABLE",
            "projection_id": "projection-a",
            "projection_sequence": 1,
        }
    )


def test_runtime_ledger_backfills_consumed_scope_before_reopening(tmp_path: Path) -> None:
    path = tmp_path / "runtime.db"
    ledger = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)
    request = _runtime_request()
    assert ledger.accept(request)["accepted"] is True
    assert ledger.transition(request.request_id, expected="ACCEPTED", state="EXECUTION_STARTED")
    ledger.connection.execute("DELETE FROM effect_scope_fences")
    ledger.close()

    reopened = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)
    try:
        scope = reopened.scope(request.effect_scope_id)
        assert scope is not None
        assert scope["state"] == "OUTCOME_UNKNOWN"
        assert scope["physical_attempt_count"] == 1
    finally:
        reopened.close()


def test_runtime_ledger_rejects_legacy_same_generation_pid_split(tmp_path: Path) -> None:
    path = tmp_path / "runtime.db"
    ledger = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)
    first = _runtime_request()
    assert ledger.accept(first)["accepted"] is True
    ledger.close()

    second_target = {**first.target_identity, "ffmpeg_pid": 4200}
    second_scope = effect_scope_id("restart_ffmpeg", second_target)
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP INDEX one_runtime_effect_per_logical_generation")
        connection.execute("UPDATE effect_scope_fences SET logical_generation_scope_id=NULL")
        connection.execute(
            """INSERT INTO effect_scope_fences(
                   effect_scope_id,logical_generation_scope_id,action,identity_json,
                   owner_request_id,owner_request_digest,state,effect_boundary_reached,
                   physical_attempt_count,result_json,created_at,updated_at
               ) VALUES(?,NULL,'restart_ffmpeg',?,'legacy-pid-split','digest-b',
                   'EFFECT_OBSERVED',1,1,NULL,?,?)""",
            (
                second_scope,
                json.dumps(second_target, sort_keys=True, separators=(",", ":")),
                isoformat_utc(utc_now()),
                isoformat_utc(utc_now()),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="RUNTIME_LOGICAL_GENERATION_PID_INVARIANT_BROKEN"):
        EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)

    # Startup failure closes its handle; the database remains available for
    # explicit operator reconciliation instead of leaking a locked connection.
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("SELECT count(*) FROM effect_scope_fences").fetchone()[0] == 2
    finally:
        connection.close()


def test_dell_legacy_local_action_is_fenced_and_exportable_after_migration(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    baseline = migrations / "001_initial.sql"
    baseline.write_text((ROOT / "migrations/dell/001_initial.sql").read_text(encoding="utf-8"), encoding="utf-8")
    database = tmp_path / "dell.db"
    old = DellStore(database, baseline)
    old.bootstrap(
        agent_id="dell-agent",
        installation_id="installation-a",
        host_id="dell",
        host_boot_id="boot-a",
        target_id="stream-target",
    )
    old.set_authority_state("stream-target", "LOCAL_FALLBACK", "LEGACY_FIXTURE")
    session = old.begin_local_fallback("stream-target")
    target = TargetIdentity("dell", "boot-a", "stream-v3", "pod-a", "stream-engine", "containerd://a", "generation-a", 4100)
    stamp = isoformat_utc(utc_now())
    with old.write() as db:
        db.execute(
            "INSERT INTO local_actions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "legacy-local-action",
                session,
                "stream-target",
                1,
                "restart_ffmpeg",
                "confirmed_tcp_stall",
                json.dumps(target.to_dict()),
                json.dumps({"tcp_stall_confirmed": True}),
                "EFFECT_OBSERVED",
                stamp,
                stamp,
            ),
        )
    old.close()
    (migrations / "002_effect_scope_and_local_journal.sql").write_text(
        (ROOT / "migrations/dell/002_effect_scope_and_local_journal.sql").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    migrated = DellStore(database, baseline)
    try:
        assert migrated.read_one("SELECT physical_attempt_count FROM effect_scope_fences")[0] == 1
        journal = migrated.read_one("SELECT record_state FROM local_action_journal")
        assert journal is not None and journal[0] == "EFFECT_OBSERVED"
    finally:
        migrated.close()


def test_logical_generation_migration_safe_blocks_preexisting_pid_split(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for name in (
        "001_initial.sql",
        "002_effect_scope_and_local_journal.sql",
        "003_single_managed_target.sql",
        "004_authority_action_readiness.sql",
    ):
        (migrations / name).write_text((ROOT / f"migrations/dell/{name}").read_text(encoding="utf-8"), encoding="utf-8")
    baseline = migrations / "001_initial.sql"
    database = tmp_path / "dell.db"
    store = DellStore(database, baseline)
    store.bootstrap(
        agent_id="dell-agent",
        installation_id="installation-a",
        host_id="dell",
        host_boot_id="boot-a",
        target_id="stream-target",
    )
    first = TargetIdentity("dell", "boot-a", "stream-v3", "pod-a", "stream-engine", "containerd://a", "generation-a", 4100)
    second = TargetIdentity.from_dict({**first.to_dict(), "ffmpeg_pid": 4200})
    stamp = isoformat_utc(utc_now())
    with store.write() as db:
        for owner, target in (("command-a", first), ("command-b", second)):
            db.execute(
                """INSERT INTO effect_scope_fences(
                       effect_scope_id,target_id,action,exact_target_json,owner_kind,owner_id,state,
                       effect_boundary_reached,physical_attempt_count,result_json,created_at,updated_at
                   ) VALUES(?,?,'restart_ffmpeg',?,'CENTRAL_COMMAND',?,'EFFECT_OBSERVED',1,1,NULL,?,?)""",
                (
                    effect_scope_id("restart_ffmpeg", target),
                    "stream-target",
                    json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True),
                    owner,
                    stamp,
                    stamp,
                ),
            )
    store.close()
    (migrations / "005_logical_generation_fence.sql").write_text(
        (ROOT / "migrations/dell/005_logical_generation_fence.sql").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    with pytest.raises(LedgerUnavailable, match="GENERATION_PID_INVARIANT_BROKEN"):
        DellStore(database, baseline)

    connection = sqlite3.connect(database)
    try:
        assert connection.execute("SELECT authority_state,state_reason FROM authority_fences").fetchone() == (
            "SAFE_BLOCKED",
            "GENERATION_PID_INVARIANT_BROKEN",
        )
    finally:
        connection.close()
