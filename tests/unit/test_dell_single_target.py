from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cra_dell_recovery.errors import LedgerUnavailable
from dell_recovery_agent.storage import DellStore

ROOT = Path(__file__).resolve().parents[2]


def test_dell_agent_persists_one_immutable_managed_target(tmp_path: Path) -> None:
    store = DellStore(tmp_path / "dell.sqlite3", ROOT / "migrations/dell/001_initial.sql")
    try:
        store.bootstrap(
            agent_id="dell-agent",
            installation_id="installation-a",
            host_id="dell-stream-runtime",
            host_boot_id="boot-a",
            target_id="stream-target",
        )
        store.assert_managed_target("stream-target")
        binding = store.read_one("SELECT target_id FROM agent_target_binding WHERE singleton_id=1")
        assert binding is not None and binding[0] == "stream-target"
        with pytest.raises(ValueError, match="DELL_AGENT_CONFIGURED_TARGET_MISMATCH"):
            store.assert_managed_target("other-target")
            with pytest.raises(sqlite3.IntegrityError, match="DELL_AGENT_SINGLE_TARGET_VIOLATION"):
                store.connection.execute(
                    """INSERT INTO authority_fences(
                           target_id,authority_state,highest_authority_epoch_seen,
                           active_authority_session_id,active_controller_instance_id,
                           highest_command_seq_consumed,heartbeat_seq,lease_duration_ms,
                           last_heartbeat_received_at,state_reason,last_reconciled_at,version,action_ready
                       ) VALUES (?,'SAFE_BLOCKED',0,NULL,NULL,0,0,15000,NULL,?,NULL,0,0)""",
                    ("other-target", "NEGATIVE_CONTROL"),
                )
    finally:
        store.close()


def test_migration_fails_closed_if_legacy_agent_contains_multiple_targets(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    baseline = migrations / "001_initial.sql"
    baseline.write_text((ROOT / "migrations/dell/001_initial.sql").read_text(encoding="utf-8"), encoding="utf-8")
    database = tmp_path / "legacy.sqlite3"
    legacy = DellStore(database, baseline)
    legacy.bootstrap(
        agent_id="dell-agent",
        installation_id="installation-a",
        host_id="dell-stream-runtime",
        host_boot_id="boot-a",
        target_id="stream-target",
    )
    legacy.connection.execute(
        """INSERT INTO authority_fences
           VALUES (?,'SAFE_BLOCKED',0,NULL,NULL,0,0,15000,NULL,?,NULL,0)""",
        ("unexpected-second-target", "LEGACY_FIXTURE"),
    )
    legacy.close()
    for name in ("002_effect_scope_and_local_journal.sql", "003_single_managed_target.sql"):
        (migrations / name).write_text((ROOT / f"migrations/dell/{name}").read_text(encoding="utf-8"), encoding="utf-8")

    with pytest.raises(LedgerUnavailable, match="DELL_AGENT_MULTIPLE_TARGETS_UNSUPPORTED"):
        DellStore(database, baseline)
