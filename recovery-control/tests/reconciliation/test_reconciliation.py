from __future__ import annotations

from pathlib import Path

import pytest

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_authority.reconciliation import CentralReconciler
from cra_authority.storage import CentralStore
from cra_dell_recovery.models import MonitoringReadiness
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.execution import FakeTargetObserver
from dell_recovery_agent.reconciliation import DellReconciliationService


def reconciler(environment: object, central: CentralStore | None = None) -> CentralReconciler:
    peer = DellReconciliationService(
        environment.dell,  # type: ignore[attr-defined]
        environment.agent_codec,  # type: ignore[attr-defined]
        FakeTargetObserver(environment.target),  # type: ignore[attr-defined]
    )
    return CentralReconciler(
        central or environment.central,  # type: ignore[attr-defined]
        environment.central_codec,  # type: ignore[attr-defined]
        peer,
    )


def test_explicit_reconciliation_installs_max_epoch_plus_one(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    new_epoch = reconciler(environment).reconcile("stream-target")
    assert new_epoch == 2
    central = environment.central.authority_snapshot("stream-target")  # type: ignore[attr-defined]
    dell = environment.dell.fence("stream-target")  # type: ignore[attr-defined]
    assert central["authority_state"] == "CENTRAL_ACTIVE"
    assert central["current_authority_epoch"] == 2
    assert central["next_command_seq"] == 1
    assert dell["authority_state"] == "CENTRAL_SUSPECT"
    assert dell["highest_authority_epoch_seen"] == 2
    assert dell["highest_command_seq_consumed"] == 0


def test_heartbeat_can_resume_only_after_reconciliation(environment: object) -> None:
    environment.central.mark_restored("old-backup")  # type: ignore[attr-defined]
    publisher = AuthorityHeartbeatPublisher(
        environment.central,
        environment.central_codec,  # type: ignore[attr-defined]
    )
    ready = MonitoringReadiness(True, True, isoformat_utc(utc_now()))
    assert publisher.build("stream-target", ready) is None
    assert reconciler(environment).reconcile("stream-target") == 2
    heartbeat = publisher.build("stream-target", ready)
    assert heartbeat is not None
    lease = AgentAuthorityLease(
        environment.dell,
        environment.agent_codec,  # type: ignore[attr-defined]
    )
    assert lease.receive(heartbeat) == "CENTRAL_ACTIVE"


def test_old_backup_restore_supersedes_pending_and_uses_dell_highwater(environment: object, tmp_path: Path) -> None:
    old_command = environment.command("old")  # type: ignore[attr-defined]
    backup = tmp_path / "backups/central-old.db"
    environment.central.backup_to(backup)  # type: ignore[attr-defined]
    assert reconciler(environment).reconcile("stream-target") == 2

    restored = CentralStore(backup, Path(__file__).resolve().parents[2] / "migrations/central/001_initial.sql")
    try:
        restored.mark_restored("old-backup")
        assert restored.pending_envelopes() == []
        new_epoch = reconciler(environment, restored).reconcile("stream-target")
        assert new_epoch == 3
        assert (
            restored.connection.execute("SELECT status FROM commands WHERE command_id=?", (old_command["command_id"],)).fetchone()[0]
            == "SUPERSEDED"
        )
        assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]
    finally:
        restored.close()


def test_agent_ledger_loss_is_safe_blocked_not_rebuilt_from_central(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    environment.dell.connection.execute(  # type: ignore[attr-defined]
        "UPDATE agent_identity SET ledger_state='LEDGER_LOST' WHERE singleton_id=1"
    )
    environment.dell.set_authority_state("stream-target", "SAFE_BLOCKED", "LEDGER_LOST")  # type: ignore[attr-defined]
    receipt = environment.service().handle_command(command)  # type: ignore[attr-defined]
    assert receipt["reason_code"] == "LEDGER_UNAVAILABLE"
    assert (
        environment.dell.connection.execute(  # type: ignore[attr-defined]
            "SELECT count(*) FROM agent_commands"
        ).fetchone()[0]
        == 0
    )
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


class MismatchedChallengeTargetPeer:
    def __init__(self, environment: object, delegate: DellReconciliationService) -> None:
        self.environment = environment
        self.delegate = delegate

    def issue_challenge(self, target_id: str) -> dict[str, object]:
        challenge = self.environment.central_codec.decode(self.delegate.issue_challenge(target_id))  # type: ignore[attr-defined]
        challenge["target_id"] = "other-target"
        return self.environment.agent_codec.encode(challenge)  # type: ignore[attr-defined,no-any-return]

    def journal_page(self, payload: dict[str, object]) -> dict[str, object]:
        return self.delegate.journal_page(payload)  # type: ignore[arg-type,return-value]

    def commit(self, payload: dict[str, object]) -> dict[str, object]:
        return self.delegate.commit(payload)  # type: ignore[arg-type,return-value]


def test_signed_challenge_for_different_target_is_rejected(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    peer = MismatchedChallengeTargetPeer(
        environment,
        DellReconciliationService(
            environment.dell,  # type: ignore[attr-defined]
            environment.agent_codec,  # type: ignore[attr-defined]
            FakeTargetObserver(environment.target),  # type: ignore[attr-defined]
        ),
    )

    with pytest.raises(ValueError, match="challenge target binding mismatch"):
        CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")  # type: ignore[attr-defined,arg-type]
    assert environment.central.authority_snapshot("stream-target")["current_authority_epoch"] == 1  # type: ignore[attr-defined]


class MismatchedCommitResponsePeer:
    def __init__(self, delegate: DellReconciliationService, field: str, value: object) -> None:
        self.delegate = delegate
        self.field = field
        self.value = value

    def issue_challenge(self, target_id: str) -> dict[str, object]:
        return self.delegate.issue_challenge(target_id)

    def journal_page(self, payload: dict[str, object]) -> dict[str, object]:
        return self.delegate.journal_page(payload)  # type: ignore[arg-type,return-value]

    def commit(self, payload: dict[str, object]) -> dict[str, object]:
        result = self.delegate.commit(payload)  # type: ignore[arg-type]
        result[self.field] = self.value
        return result


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("target_id", "other-target"),
        ("authority_epoch", 999),
        ("authority_session_id", "session-other"),
    ],
)
def test_commit_response_binding_mismatch_blocks_central_install(
    environment: object,
    field: str,
    value: object,
) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    delegate = DellReconciliationService(
        environment.dell,  # type: ignore[attr-defined]
        environment.agent_codec,  # type: ignore[attr-defined]
        FakeTargetObserver(environment.target),  # type: ignore[attr-defined]
    )
    peer = MismatchedCommitResponsePeer(delegate, field, value)

    with pytest.raises(ValueError, match="commit response binding mismatch"):
        CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")  # type: ignore[attr-defined,arg-type]
    assert environment.central.authority_snapshot("stream-target")["current_authority_epoch"] == 1  # type: ignore[attr-defined]
