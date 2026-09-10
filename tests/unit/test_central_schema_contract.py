from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from cra_authority.storage import CentralStore
from cra_dell_recovery.errors import CommandBlocked, LedgerUnavailable
from cra_dell_recovery.models import RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.time import isoformat_utc, utc_now

ROOT = Path(__file__).resolve().parents[2]


def test_startup_rejects_unexpected_schema_object(tmp_path: Path) -> None:
    database = tmp_path / "central.db"
    store = CentralStore(database, ROOT / "migrations/central/001_initial.sql")
    store.close()
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE unexpected_shadow(value TEXT) STRICT")
    finally:
        connection.close()

    with pytest.raises(LedgerUnavailable, match="CENTRAL_SCHEMA_CONTRACT_DRIFT"):
        CentralStore(database, ROOT / "migrations/central/001_initial.sql")


def test_startup_rejects_preexisting_foreign_key_violation(tmp_path: Path) -> None:
    database = tmp_path / "central.db"
    store = CentralStore(database, ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="agent-installation-1",
    )
    store.close()
    stamp = isoformat_utc(utc_now())
    connection = sqlite3.connect(database)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute(
            "INSERT INTO monitoring_evidence_projections VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "orphan-projection",
                "missing-target",
                "arena-monitoring-v4",
                "arena-release-a",
                "cycle-orphan",
                "revision-orphan",
                1,
                "incident-orphan",
                "CONFIRMED",
                stamp,
                stamp,
                stamp,
                "{}",
                "a" * 64,
                "key-a",
                "signature-a",
                stamp,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(LedgerUnavailable, match="foreign_key_check"):
        CentralStore(database, ROOT / "migrations/central/001_initial.sql")


def test_startup_rejects_unresolved_state_index_drift(tmp_path: Path) -> None:
    database = tmp_path / "central.db"
    store = CentralStore(database, ROOT / "migrations/central/001_initial.sql")
    store.close()
    connection = sqlite3.connect(database)
    try:
        connection.execute("DROP INDEX one_unresolved_command_per_target")
        connection.execute(
            """CREATE UNIQUE INDEX one_unresolved_command_per_target ON commands(target_id)
               WHERE status IN ('COMMITTED','OUTBOX_PENDING')"""
        )
    finally:
        connection.close()

    with pytest.raises(LedgerUnavailable, match="CENTRAL_UNRESOLVED_STATE_CONTRACT_DRIFT"):
        CentralStore(database, ROOT / "migrations/central/001_initial.sql")


def test_startup_rejects_logical_generation_index_drift(tmp_path: Path) -> None:
    database = tmp_path / "central.db"
    store = CentralStore(database, ROOT / "migrations/central/001_initial.sql")
    store.close()
    connection = sqlite3.connect(database)
    try:
        connection.execute("DROP INDEX one_central_effect_per_logical_generation")
        connection.execute(
            """CREATE INDEX one_central_effect_per_logical_generation
               ON effect_scope_ledger(logical_generation_scope_id)"""
        )
    finally:
        connection.close()

    with pytest.raises(LedgerUnavailable, match="CENTRAL_LOGICAL_GENERATION_INDEX_DRIFT"):
        CentralStore(database, ROOT / "migrations/central/001_initial.sql")


def test_startup_rejects_preexisting_same_generation_pid_split(tmp_path: Path) -> None:
    database = tmp_path / "central.db"
    store = CentralStore(database, ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="agent-installation-1",
    )
    first = TargetIdentity("dell", "boot-a", "default", "pod-a", "stream-engine", "container-a", "run-a:0:4100", 4100)
    second = TargetIdentity.from_dict({**first.to_dict(), "ffmpeg_pid": 4200})
    stamp = isoformat_utc(utc_now())
    store.connection.execute("DROP INDEX one_central_effect_per_logical_generation")
    for index, target in enumerate((first, second), start=1):
        store.connection.execute(
            """INSERT INTO effect_scope_ledger(
                   effect_scope_id,logical_generation_scope_id,target_id,action,exact_target_json,
                   origin_kind,origin_id,state,effect_boundary_reached_at,physical_attempt_count,
                   source_evidence_digest,created_at,updated_at
               ) VALUES(?,NULL,'stream-target','restart_ffmpeg',?,'CENTRAL_COMMAND',?,
                   'EFFECT_OBSERVED',?,1,?,?,?)""",
            (
                f"{'a' if index == 1 else 'b'}" * 64,
                json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True),
                f"legacy-command-{index}",
                stamp,
                f"digest-{index}",
                stamp,
                stamp,
            ),
        )
    store.close()
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """CREATE UNIQUE INDEX one_central_effect_per_logical_generation
               ON effect_scope_ledger(logical_generation_scope_id)
               WHERE logical_generation_scope_id IS NOT NULL"""
        )
    finally:
        connection.close()

    with pytest.raises(CommandBlocked, match="CENTRAL_LOGICAL_GENERATION_PID_INVARIANT_BROKEN"):
        CentralStore(database, ROOT / "migrations/central/001_initial.sql")

    connection = sqlite3.connect(database)
    try:
        assert connection.execute("SELECT count(*) FROM effect_scope_ledger").fetchone()[0] == 2
    finally:
        connection.close()


def test_legacy_effect_failed_without_signed_attempt_is_migrated_as_unknown(
    tmp_path: Path,
    codecs: tuple[object, object],
) -> None:
    database = tmp_path / "central.db"
    store = CentralStore(database, ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="agent-installation-1",
    )
    now = utc_now()
    target = TargetIdentity("dell", "boot-a", "default", "pod-a", "stream-engine", "container-a", "run-a:0:4100", 4100)
    authorization = RecoveryAuthorizationInput(
        authorization_id="auth-legacy-failed",
        incident_id="incident-legacy-failed",
        source_episode_id="episode-legacy-failed",
        target_id="stream-target",
        action="restart_ffmpeg",
        reason_code="confirmed_tcp_stall",
        policy_revision="policy-a",
        observation_revision="observation-a",
        expected_target=target,
        blockers=(),
        authorized_at=isoformat_utc(now),
        expires_at=isoformat_utc(now + timedelta(minutes=2)),
    )
    store.add_authorization(authorization)
    store.create_command("auth-legacy-failed", codecs[0], command_id="command-legacy-failed")  # type: ignore[arg-type]
    store.connection.execute("DELETE FROM effect_scope_ledger")
    store.connection.execute(
        "UPDATE commands SET status='EFFECT_FAILED',updated_at=? WHERE command_id='command-legacy-failed'",
        (isoformat_utc(now + timedelta(seconds=1)),),
    )
    store.close()

    reopened = CentralStore(database, ROOT / "migrations/central/001_initial.sql")
    try:
        effect = reopened.read_one("SELECT state,physical_attempt_count,effect_boundary_reached_at FROM effect_scope_ledger")
        assert effect is not None
        assert effect["state"] == "OUTCOME_UNKNOWN"
        assert effect["physical_attempt_count"] == 1
        assert effect["effect_boundary_reached_at"] is not None
    finally:
        reopened.close()
