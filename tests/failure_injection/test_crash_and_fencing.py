from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from cra_dell_recovery.crash import CrashPoint, InjectedCrash, crash_at
from cra_dell_recovery.errors import CommandBlocked
from cra_dell_recovery.models import TargetIdentity


def resign(environment: object, command: dict[str, object], **changes: object) -> dict[str, object]:
    updated = {**command, **changes}
    return environment.central_codec.signer.sign(updated)  # type: ignore[attr-defined]


def test_central_before_commit_crash_rolls_back_intent_and_outbox(environment: object) -> None:
    authorization = environment.authorization()  # type: ignore[attr-defined]
    environment.central.add_authorization(authorization)  # type: ignore[attr-defined]
    with pytest.raises(InjectedCrash):
        environment.central.create_command(  # type: ignore[attr-defined]
            authorization.authorization_id,
            environment.central_codec,  # type: ignore[attr-defined]
            command_id="command-crash",
            crash=crash_at(CrashPoint.CENTRAL_BEFORE_COMMIT),
        )
    assert environment.central.command_count() == 0  # type: ignore[attr-defined]
    assert environment.central.outbox_count() == 0  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_central_after_commit_crash_recovers_same_envelope(environment: object) -> None:
    authorization = environment.authorization()  # type: ignore[attr-defined]
    environment.central.add_authorization(authorization)  # type: ignore[attr-defined]
    with pytest.raises(InjectedCrash):
        environment.central.create_command(  # type: ignore[attr-defined]
            authorization.authorization_id,
            environment.central_codec,  # type: ignore[attr-defined]
            command_id="command-after-commit",
            crash=crash_at(CrashPoint.CENTRAL_AFTER_COMMIT_BEFORE_SEND),
        )
    pending = environment.central.pending_envelopes()  # type: ignore[attr-defined]
    assert len(pending) == 1
    assert pending[0]["command_id"] == "command-after-commit"
    assert pending[0]["command_seq"] == 1
    assert environment.central.command_count() == 1  # type: ignore[attr-defined]


def test_dell_accept_commit_crash_is_superseded_without_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    with pytest.raises(InjectedCrash):
        service.handle_command(command, crash=crash_at(CrashPoint.DELL_AFTER_ACCEPT_COMMIT))
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]
    environment.dell.startup_recover("stream-target")  # type: ignore[attr-defined]
    row = environment.dell.connection.execute(  # type: ignore[attr-defined]
        "SELECT state FROM agent_commands WHERE command_id=?", (command["command_id"],)
    ).fetchone()
    assert row[0] == "SUPERSEDED_AFTER_AGENT_RESTART"
    duplicate = service.handle_command(command)
    assert duplicate["disposition"] == "DUPLICATE"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_execution_started_crash_becomes_outcome_unknown(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    with pytest.raises(InjectedCrash):
        service.handle_command(command, crash=crash_at(CrashPoint.DELL_AFTER_EXECUTION_STARTED_COMMIT))
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]
    environment.dell.startup_recover("stream-target")  # type: ignore[attr-defined]
    assert (
        environment.dell.connection.execute(  # type: ignore[attr-defined]
            "SELECT state FROM agent_commands WHERE command_id=?", (command["command_id"],)
        ).fetchone()[0]
        == "OUTCOME_UNKNOWN"
    )
    status = service.command_status(str(command["command_id"]))
    assert status["attempt_count"] == 0
    service.handle_command(command)
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_effect_ack_loss_never_causes_second_physical_attempt(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    with pytest.raises(InjectedCrash):
        service.handle_command(command, crash=crash_at(CrashPoint.DELL_AFTER_FAKE_EFFECT_BEFORE_STATUS))
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]
    environment.dell.startup_recover("stream-target")  # type: ignore[attr-defined]
    status = service.command_status(str(command["command_id"]))
    assert status["command_state"] == "OUTCOME_UNKNOWN"
    assert status["attempt_count"] == 1
    service.handle_command(command)
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_cra_ack_loss_retries_same_outbox_command_without_second_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    with pytest.raises(InjectedCrash):
        environment.central.deliver_command(  # type: ignore[attr-defined]
            str(command["command_id"]),
            service.handle_command,
            verifier=environment.central_codec,  # type: ignore[attr-defined]
            crash=crash_at(CrashPoint.CENTRAL_AFTER_SEND_BEFORE_RECEIPT),
        )
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]
    receipt = environment.central.deliver_command(  # type: ignore[attr-defined]
        str(command["command_id"]),
        service.handle_command,
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    assert receipt["disposition"] == "DUPLICATE"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]
    assert (
        environment.central.connection.execute(  # type: ignore[attr-defined]
            "SELECT state FROM outbox_messages WHERE command_id=?", (command["command_id"],)
        ).fetchone()[0]
        == "ACKED"
    )


def test_target_change_between_two_observations_blocks_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    changed = TargetIdentity(
        **{**environment.target.to_dict(), "ffmpeg_generation": "run-b:0:4200", "ffmpeg_pid": 4200}  # type: ignore[attr-defined]
    )
    service = environment.service(environment.target, changed)  # type: ignore[attr-defined]
    receipt = service.handle_command(command)
    assert receipt["reason_code"] == "STALE_TARGET"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_missing_target_observation_blocks_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(None)  # type: ignore[attr-defined]
    receipt = service.handle_command(command)
    assert receipt["reason_code"] == "TARGET_OBSERVATION_UNAVAILABLE"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_stale_authority_epoch_blocks_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    stale = resign(environment, command, authority_epoch=2)
    receipt = environment.service().handle_command(stale)  # type: ignore[attr-defined]
    assert receipt["reason_code"] == "STALE_AUTHORITY_EPOCH"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_missing_process_monotonic_lease_blocks_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    environment.dell.set_process_lease_valid("stream-target", False)  # type: ignore[attr-defined]
    receipt = environment.service().handle_command(command)  # type: ignore[attr-defined]
    assert receipt["reason_code"] == "AUTHORITY_LEASE_NOT_VALID"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_sequence_gap_blocks_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    gap = resign(environment, command, command_seq=2, idempotency_key="1:2:auth-1")
    receipt = environment.service().handle_command(gap)  # type: ignore[attr-defined]
    assert receipt["reason_code"] == "SEQUENCE_GAP"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_command_id_conflict_blocks_second_effect(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    service.handle_command(command)
    conflict = resign(environment, command, reason_code="confirmed_tcp_stall", issued_at="2026-08-23T00:00:00.000Z")
    receipt = service.handle_command(conflict)
    assert receipt["reason_code"] == "COMMAND_ID_CONFLICT"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_hard_cooldown_is_enforced_from_persisted_attempt_history(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    service.handle_command(command)
    next_command = resign(
        environment,
        command,
        command_id="command-2",
        message_id="message-command-2",
        authorization_id="auth-2",
        idempotency_key="1:2:auth-2",
        command_seq=2,
    )
    receipt = service.handle_command(next_command)
    assert receipt["reason_code"] == "HARD_COOLDOWN_ACTIVE"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_ledger_commit_failure_blocks_effect(environment: object, monkeypatch: pytest.MonkeyPatch) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]

    @contextmanager
    def unavailable() -> Iterator[sqlite3.Connection]:
        raise sqlite3.OperationalError("simulated SQLITE_IOERR")
        yield environment.dell.connection  # type: ignore[attr-defined]  # pragma: no cover

    monkeypatch.setattr(environment.dell, "write", unavailable)  # type: ignore[attr-defined]
    receipt = service.handle_command(command)
    assert receipt["reason_code"] == "LEDGER_UNAVAILABLE"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_central_restore_forbids_new_command(environment: object) -> None:
    authorization = environment.authorization()  # type: ignore[attr-defined]
    environment.central.add_authorization(authorization)  # type: ignore[attr-defined]
    environment.central.mark_restored("backup-old")  # type: ignore[attr-defined]
    with pytest.raises(CommandBlocked, match="RESTORED_NEEDS_RECONCILIATION"):
        environment.central.create_command(  # type: ignore[attr-defined]
            authorization.authorization_id,
            environment.central_codec,  # type: ignore[attr-defined]
        )
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_restore_crash_point_occurs_after_fail_closed_state_commit(environment: object) -> None:
    with pytest.raises(InjectedCrash):
        environment.central.mark_restored(  # type: ignore[attr-defined]
            "backup-old", crash=crash_at(CrashPoint.CRA_AFTER_RESTORE)
        )
    assert (
        environment.central.connection.execute(  # type: ignore[attr-defined]
            "SELECT restore_state FROM control_plane_identity"
        ).fetchone()[0]
        == "RESTORED_NEEDS_RECONCILIATION"
    )
