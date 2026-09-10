from __future__ import annotations

import hashlib
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from cra_authority.storage import CentralStore
from cra_dell_recovery.effect_scope import effect_scope_id
from cra_dell_recovery.errors import CommandBlocked, SignatureValidationError
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now

CENTRAL_TABLES = {
    "control_plane_identity",
    "targets",
    "incidents",
    "incident_transitions",
    "recovery_authorizations",
    "authority_sessions",
    "commands",
    "command_transitions",
    "outbox_messages",
    "delivery_attempts",
    "agent_messages",
    "recovery_verifications",
    "recovery_verification_messages",
    "monitoring_evidence_projections",
    "cra_policy_decisions",
    "dell_journal_records",
    "dell_journal_watermarks",
    "cra_verifier_decisions",
    "effect_scope_ledger",
    "effect_reconciliations",
    "schema_migrations",
}
DELL_TABLES = {
    "agent_identity",
    "agent_target_binding",
    "authority_fences",
    "agent_safety_policies",
    "reconciliation_sessions",
    "agent_commands",
    "execution_attempts",
    "local_fallback_sessions",
    "local_actions",
    "agent_transitions",
    "agent_events",
    "effect_scope_fences",
    "effect_scope_events",
    "local_action_journal",
    "local_action_journal_acks",
    "schema_migrations",
}


def tables(connection: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")}


def test_central_migration_has_all_contract_tables(environment: object) -> None:
    assert tables(environment.central.connection) == CENTRAL_TABLES  # type: ignore[attr-defined]


def test_dell_migration_has_all_contract_tables(environment: object) -> None:
    assert tables(environment.dell.connection) == DELL_TABLES  # type: ignore[attr-defined]


def test_required_sqlite_pragmas_are_verified(environment: object) -> None:
    for ledger in (environment.central, environment.dell):  # type: ignore[attr-defined]
        status = ledger.status
        assert status.journal_mode == "wal"
        assert status.synchronous == 2
        assert status.foreign_keys == 1
        assert status.busy_timeout == 5000
        assert status.wal_autocheckpoint == 1000
        assert status.trusted_schema == 0
        assert status.quick_check == "ok"
        assert status.foreign_key_check == "ok"


def test_transactional_outbox_and_vertical_slice(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    assert environment.central.command_count() == 1  # type: ignore[attr-defined]
    assert environment.central.outbox_count() == 1  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    environment.central.begin_delivery(str(command["command_id"]))  # type: ignore[attr-defined]
    receipt = service.handle_command(command)
    environment.central.record_receipt(  # type: ignore[attr-defined]
        str(command["command_id"]),
        receipt,
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    status = service.command_status(str(command["command_id"]))
    environment.central.record_status(status, verifier=environment.central_codec)  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]
    assert (
        environment.central.connection.execute(  # type: ignore[attr-defined]
            "SELECT status FROM commands WHERE command_id=?", (command["command_id"],)
        ).fetchone()[0]
        == "EFFECT_OBSERVED"
    )
    assert (
        environment.central.connection.execute(  # type: ignore[attr-defined]
            "SELECT count(*) FROM agent_messages WHERE command_id=?", (command["command_id"],)
        ).fetchone()[0]
        == 2
    )
    effect = environment.central.read_one(  # type: ignore[attr-defined]
        """SELECT state,physical_attempt_count,effect_boundary_reached_at
           FROM effect_scope_ledger WHERE origin_id=?""",
        (command["command_id"],),
    )
    assert effect is not None
    assert effect["state"] == "EFFECT_OBSERVED"
    assert effect["physical_attempt_count"] == 1
    assert effect["effect_boundary_reached_at"] == status["effect_observed_at"]


def test_pre_effect_failure_does_not_consume_physical_budget(environment: object) -> None:
    command = environment.command("pre-effect-abort")  # type: ignore[attr-defined]
    environment.central.begin_delivery(str(command["command_id"]))  # type: ignore[attr-defined]
    status = environment.agent_codec.encode(  # type: ignore[attr-defined]
        {
            "protocol": "cra_dell_recovery.command_status.v1",
            "message_type": "command_status",
            "status_id": "status-pre-effect-abort",
            "command_id": command["command_id"],
            "authority_epoch": command["authority_epoch"],
            "command_seq": command["command_seq"],
            "agent_installation_id": "agent-installation-1",
            "effect_scope_id": effect_scope_id("restart_ffmpeg", environment.target),  # type: ignore[attr-defined]
            "command_state": "EFFECT_FAILED",
            "reason_code": "PRE_EFFECT_CHECK_FAILED",
            "attempt_count": 0,
            "before_target": environment.target.to_dict(),  # type: ignore[attr-defined]
            "after_target": None,
            "effect_observed_at": None,
            "agent_observed_at": isoformat_utc(utc_now()),
            "key_id": environment.agent_codec.signer.key_id,  # type: ignore[attr-defined]
        }
    )

    environment.central.record_status(status, verifier=environment.central_codec)  # type: ignore[attr-defined]

    effect = environment.central.read_one(  # type: ignore[attr-defined]
        "SELECT state,physical_attempt_count,effect_boundary_reached_at FROM effect_scope_ledger"
    )
    assert effect is not None
    assert tuple(effect) == ("PRE_EFFECT_ABORTED", 0, None)
    assert (
        environment.central.central_policy_blockers(  # type: ignore[attr-defined]
            "stream-target",
            now=utc_now(),
            minimum_action_interval_sec=60,
            hourly_action_limit=1,
            daily_action_limit=1,
        )
        == ()
    )


def test_agent_status_id_is_idempotent_but_conflicting_payload_is_rejected(environment: object) -> None:
    command = environment.command("status-id-conflict")  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    environment.central.begin_delivery(str(command["command_id"]))  # type: ignore[attr-defined]
    receipt = service.handle_command(command)
    environment.central.record_receipt(  # type: ignore[attr-defined]
        str(command["command_id"]),
        receipt,
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    status = service.command_status(str(command["command_id"]))
    environment.central.record_status(status, verifier=environment.central_codec)  # type: ignore[attr-defined]
    environment.central.record_status(status, verifier=environment.central_codec)  # type: ignore[attr-defined]

    conflict = environment.agent_codec.encode(  # type: ignore[attr-defined]
        {**status, "reason_code": "CONFLICTING_REPLAY"}
    )
    with pytest.raises(CommandBlocked, match="COMMAND_STATUS_ID_CONFLICT"):
        environment.central.record_status(conflict, verifier=environment.central_codec)  # type: ignore[attr-defined]
    assert environment.central.read_one("SELECT count(*) FROM agent_messages")[0] == 2  # type: ignore[attr-defined]


def test_agent_status_rejects_future_and_conflicting_effect_timestamps(environment: object) -> None:
    command = environment.command("status-time-conflict")  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    environment.central.begin_delivery(str(command["command_id"]))  # type: ignore[attr-defined]
    receipt = service.handle_command(command)
    environment.central.record_receipt(  # type: ignore[attr-defined]
        str(command["command_id"]),
        receipt,
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    status = service.command_status(str(command["command_id"]))
    future = isoformat_utc(utc_now() + timedelta(seconds=5))
    future_status = environment.agent_codec.encode(  # type: ignore[attr-defined]
        {**status, "status_id": "status-future", "agent_observed_at": future, "effect_observed_at": future}
    )
    with pytest.raises(CommandBlocked, match="COMMAND_STATUS_TIMESTAMP_IN_FUTURE"):
        environment.central.record_status(future_status, verifier=environment.central_codec)  # type: ignore[attr-defined]

    environment.central.record_status(status, verifier=environment.central_codec)  # type: ignore[attr-defined]
    changed_boundary = isoformat_utc(parse_utc(str(status["effect_observed_at"])) + timedelta(milliseconds=1))
    changed = environment.agent_codec.encode(  # type: ignore[attr-defined]
        {
            **status,
            "status_id": "status-changed-boundary",
            "effect_observed_at": changed_boundary,
            "agent_observed_at": changed_boundary,
        }
    )
    with pytest.raises(CommandBlocked, match="CENTRAL_EFFECT_BOUNDARY_TIMESTAMP_CONFLICT"):
        environment.central.record_status(changed, verifier=environment.central_codec)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("state", "attempt_count", "reason"),
    [
        ("ACCEPTED", 1, "COMMAND_STATUS_PRE_EFFECT_STATE_HAS_ATTEMPT"),
        ("REJECTED", 1, "COMMAND_STATUS_PRE_EFFECT_STATE_HAS_ATTEMPT"),
    ],
)
def test_agent_status_state_and_attempt_boundary_must_agree(
    environment: object,
    state: str,
    attempt_count: int,
    reason: str,
) -> None:
    command = environment.command(f"status-attempt-{state.lower()}")  # type: ignore[attr-defined]
    environment.central.begin_delivery(str(command["command_id"]))  # type: ignore[attr-defined]
    observed = isoformat_utc(utc_now())
    status = environment.agent_codec.encode(  # type: ignore[attr-defined]
        {
            "protocol": "cra_dell_recovery.command_status.v1",
            "message_type": "command_status",
            "status_id": f"status-attempt-{state.lower()}",
            "command_id": command["command_id"],
            "authority_epoch": command["authority_epoch"],
            "command_seq": command["command_seq"],
            "agent_installation_id": "agent-installation-1",
            "effect_scope_id": effect_scope_id("restart_ffmpeg", environment.target),  # type: ignore[attr-defined]
            "command_state": state,
            "reason_code": "INCONSISTENT_ATTEMPT_FIXTURE",
            "attempt_count": attempt_count,
            "before_target": environment.target.to_dict(),  # type: ignore[attr-defined]
            "after_target": (
                {
                    **environment.target.to_dict(),  # type: ignore[attr-defined]
                    "ffmpeg_generation": f"{environment.target.ffmpeg_generation}:fake-successor",  # type: ignore[attr-defined]
                    "ffmpeg_pid": environment.target.ffmpeg_pid + 1,  # type: ignore[attr-defined]
                }
                if attempt_count
                else None
            ),
            "effect_observed_at": observed if attempt_count else None,
            "agent_observed_at": observed,
            "key_id": environment.agent_codec.signer.key_id,  # type: ignore[attr-defined]
        }
    )

    with pytest.raises(CommandBlocked, match=reason):
        environment.central.record_status(status, verifier=environment.central_codec)  # type: ignore[attr-defined]
    effect = environment.central.read_one(  # type: ignore[attr-defined]
        "SELECT state,physical_attempt_count FROM effect_scope_ledger"
    )
    assert effect is not None and tuple(effect) == ("RESERVED", 0)


def test_pre_boundary_crash_remains_unknown_without_consuming_budget(environment: object) -> None:
    command = environment.command("pre-boundary-unknown")  # type: ignore[attr-defined]
    environment.central.begin_delivery(str(command["command_id"]))  # type: ignore[attr-defined]
    status = environment.agent_codec.encode(  # type: ignore[attr-defined]
        {
            "protocol": "cra_dell_recovery.command_status.v1",
            "message_type": "command_status",
            "status_id": "status-pre-boundary-unknown",
            "command_id": command["command_id"],
            "authority_epoch": command["authority_epoch"],
            "command_seq": command["command_seq"],
            "agent_installation_id": "agent-installation-1",
            "effect_scope_id": effect_scope_id("restart_ffmpeg", environment.target),  # type: ignore[attr-defined]
            "command_state": "OUTCOME_UNKNOWN",
            "reason_code": "AGENT_RESTART_AFTER_EXECUTION_STARTED",
            "attempt_count": 0,
            "before_target": environment.target.to_dict(),  # type: ignore[attr-defined]
            "after_target": None,
            "effect_observed_at": None,
            "agent_observed_at": isoformat_utc(utc_now()),
            "key_id": environment.agent_codec.signer.key_id,  # type: ignore[attr-defined]
        }
    )

    environment.central.record_status(status, verifier=environment.central_codec)  # type: ignore[attr-defined]
    effect = environment.central.read_one(  # type: ignore[attr-defined]
        "SELECT state,physical_attempt_count,effect_boundary_reached_at FROM effect_scope_ledger"
    )
    assert effect is not None and tuple(effect) == ("OUTCOME_UNKNOWN", 0, None)
    assert "CENTRAL_UNRESOLVED_COMMAND" in environment.central.central_policy_blockers(  # type: ignore[attr-defined]
        "stream-target",
        now=utc_now(),
        minimum_action_interval_sec=60,
        hourly_action_limit=1,
        daily_action_limit=1,
    )


def test_effect_reconciliation_is_append_only_and_preserves_raw_state(environment: object) -> None:
    command = environment.command("reconciliation-record")  # type: ignore[attr-defined]
    scope_id = effect_scope_id("restart_ffmpeg", environment.target)  # type: ignore[attr-defined]

    assert (
        environment.central.record_effect_reconciliation(  # type: ignore[attr-defined]
            effect_scope_id_value=scope_id,
            reconciled_state="OUTCOME_UNKNOWN",
            evidence={"source": "operator-review", "raw_state_preserved": True},
            reconciliation_id="effect-reconciliation-a",
            recorded_at=str(command["issued_at"]),
        )
        == "RECORDED"
    )
    assert (
        environment.central.record_effect_reconciliation(  # type: ignore[attr-defined]
            effect_scope_id_value=scope_id,
            reconciled_state="OUTCOME_UNKNOWN",
            evidence={"source": "operator-review", "raw_state_preserved": True},
            reconciliation_id="effect-reconciliation-a",
            recorded_at=str(command["issued_at"]),
        )
        == "DUPLICATE"
    )
    assert environment.central.read_one("SELECT state FROM effect_scope_ledger")[0] == "RESERVED"  # type: ignore[attr-defined]
    assert environment.central.read_one("SELECT count(*) FROM effect_reconciliations")[0] == 1  # type: ignore[attr-defined]


def test_central_rejects_same_generation_with_changed_pid_before_outbox(environment: object) -> None:
    first = environment.command("logical-generation-first")  # type: ignore[attr-defined]
    # Close the first command/incident as a historical action. The durable
    # generation fence must remain even after ordinary unresolved-row gates clear.
    environment.central.connection.execute(  # type: ignore[attr-defined]
        "UPDATE commands SET status='VERIFIED' WHERE command_id=?",
        (first["command_id"],),
    )
    environment.central.connection.execute(  # type: ignore[attr-defined]
        "UPDATE incidents SET state='RECOVERED' WHERE incident_id='incident-logical-generation-first'"
    )
    changed_pid = type(environment.target).from_dict(  # type: ignore[attr-defined]
        {**environment.target.to_dict(), "ffmpeg_pid": 4200}  # type: ignore[attr-defined]
    )
    authorization = environment.authorization(  # type: ignore[attr-defined]
        "logical-generation-second", target=changed_pid
    )
    environment.central.add_authorization(authorization)  # type: ignore[attr-defined]

    with pytest.raises(CommandBlocked, match="CENTRAL_LOGICAL_GENERATION_ALREADY_FENCED"):
        environment.central.create_command(  # type: ignore[attr-defined]
            authorization.authorization_id,
            environment.central_codec,  # type: ignore[attr-defined]
            command_id="command-logical-generation-second",
        )
    assert environment.central.command_count() == 1  # type: ignore[attr-defined]
    assert environment.central.outbox_count() == 1  # type: ignore[attr-defined]


def test_central_rejects_unverified_or_misbound_agent_messages(environment: object) -> None:
    command = environment.command("agent-binding")  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    environment.central.begin_delivery(str(command["command_id"]))  # type: ignore[attr-defined]
    receipt = service.handle_command(command)

    tampered_receipt = {**receipt, "reason_code": "TAMPERED"}
    with pytest.raises(SignatureValidationError):
        environment.central.record_receipt(  # type: ignore[attr-defined]
            str(command["command_id"]),
            tampered_receipt,
            verifier=environment.central_codec,  # type: ignore[attr-defined]
        )

    misbound_receipt = environment.agent_codec.encode({**receipt, "command_id": "different-command"})  # type: ignore[attr-defined]
    with pytest.raises(CommandBlocked, match="COMMAND_RECEIPT_BINDING_MISMATCH"):
        environment.central.record_receipt(  # type: ignore[attr-defined]
            str(command["command_id"]),
            misbound_receipt,
            verifier=environment.central_codec,  # type: ignore[attr-defined]
        )

    environment.central.record_receipt(  # type: ignore[attr-defined]
        str(command["command_id"]),
        receipt,
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    status = service.command_status(str(command["command_id"]))
    misbound_status = environment.agent_codec.encode({**status, "effect_scope_id": "0" * 64})  # type: ignore[attr-defined]
    with pytest.raises(CommandBlocked, match="COMMAND_STATUS_BINDING_MISMATCH"):
        environment.central.record_status(misbound_status, verifier=environment.central_codec)  # type: ignore[attr-defined]
    assert environment.central.read_one("SELECT count(*) FROM agent_messages")[0] == 1  # type: ignore[attr-defined]


def test_legacy_monitoring_owned_final_verification_path_is_disabled(environment: object) -> None:
    with pytest.raises(CommandBlocked, match="LEGACY_MONITORING_VERIFICATION_PATH_DISABLED"):
        environment.central.record_recovery_verification({})  # type: ignore[attr-defined]


def test_exact_duplicate_returns_saved_state_without_second_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    first = service.handle_command(command)
    duplicate = service.handle_command(command)
    assert first["disposition"] == "ACCEPTED"
    assert duplicate["disposition"] == "DUPLICATE"
    assert duplicate["command_state"] == "EFFECT_OBSERVED"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_status_query_after_ack_loss_does_not_execute_again(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    service.handle_command(command)
    status = service.command_status(str(command["command_id"]))
    environment.central_codec.decode(status)  # type: ignore[attr-defined]
    assert status["command_state"] == "EFFECT_OBSERVED"
    assert status["attempt_count"] == 1
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_online_backup_api_creates_integral_copy(environment: object, tmp_path: Path) -> None:
    environment.command()  # type: ignore[attr-defined]
    backup = tmp_path / "backup/central-backup.db"
    digest = environment.central.backup_to(backup)  # type: ignore[attr-defined]
    assert digest == hashlib.sha256(backup.read_bytes()).hexdigest()
    restored = sqlite3.connect(backup)
    try:
        assert restored.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert restored.execute("SELECT count(*) FROM commands").fetchone()[0] == 1
    finally:
        restored.close()


def test_restore_marks_old_outbox_terminal(environment: object, tmp_path: Path) -> None:
    environment.command()  # type: ignore[attr-defined]
    backup = tmp_path / "old.db"
    environment.central.backup_to(backup)  # type: ignore[attr-defined]
    restored = CentralStore(backup, Path(__file__).resolve().parents[2] / "migrations/central/001_initial.sql")
    try:
        restored.mark_restored("backup-1")
        assert restored.pending_envelopes() == []
        assert (
            restored.connection.execute("SELECT restore_state FROM control_plane_identity").fetchone()[0] == "RESTORED_NEEDS_RECONCILIATION"
        )
        assert restored.connection.execute("SELECT state FROM outbox_messages").fetchone()[0] == "TERMINAL"
        assert restored.connection.execute("SELECT status FROM commands").fetchone()[0] == "SUPERSEDED"
    finally:
        restored.close()


def test_central_and_dell_are_distinct_database_files(environment: object) -> None:
    assert environment.central.path != environment.dell.path  # type: ignore[attr-defined]
    assert environment.central.connection is not environment.dell.connection  # type: ignore[attr-defined]
