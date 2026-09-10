from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import timedelta

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_dell_recovery.errors import AuthorityDeadlineExceeded
from cra_dell_recovery.models import MonitoringReadiness
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.execution import AgentService, FakeTargetObserver
from dell_recovery_agent.reconciliation import DellReconciliationService


@dataclass
class FakeMonotonic:
    value: float = 100.0
    called: threading.Event = field(default_factory=threading.Event)

    def __call__(self) -> float:
        self.called.set()
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _stall_writer(store, operation, clock: FakeMonotonic):  # type: ignore[no-untyped-def]
    writer_held = threading.Event()
    release_writer = threading.Event()
    result: list[object] = []

    def blocker() -> None:
        with store.write():
            writer_held.set()
            assert release_writer.wait(timeout=2)

    def actor() -> None:
        try:
            result.append(operation())
        except BaseException as error:
            result.append(error)

    blocker_thread = threading.Thread(target=blocker)
    actor_thread = threading.Thread(target=actor)
    blocker_thread.start()
    assert writer_held.wait(timeout=2)
    actor_thread.start()
    assert clock.called.wait(timeout=2)
    clock.advance(4.25)
    release_writer.set()
    blocker_thread.join(timeout=2)
    actor_thread.join(timeout=2)
    assert not blocker_thread.is_alive()
    assert not actor_thread.is_alive()
    assert len(result) == 1
    return result[0]


def test_nc_db_deadline_01_stale_heartbeat_cannot_restore_authority(environment) -> None:  # type: ignore[no-untyped-def]
    clock = FakeMonotonic()
    publisher = AuthorityHeartbeatPublisher(environment.central, environment.central_codec, lease_ttl_seconds=15)
    heartbeat = publisher.build(
        "stream-target",
        MonitoringReadiness(True, True, isoformat_utc(utc_now()), "db-deadline-negative-control"),
    )
    assert heartbeat is not None
    lease = AgentAuthorityLease(
        environment.dell,
        environment.agent_codec,
        suspect_after_seconds=4,
        lease_ttl_seconds=15,
        critical_db_deadline_seconds=3,
        monotonic=clock,
    )

    result = _stall_writer(environment.dell, lambda: lease.receive(heartbeat), clock)

    assert result == "HEARTBEAT_DB_DEADLINE_EXCEEDED"
    assert environment.dell.process_lease_valid("stream-target") is False
    assert lease.tick("stream-target") == "CENTRAL_SUSPECT"
    assert environment.dell.fence("stream-target")["authority_state"] == "CENTRAL_SUSPECT"
    assert environment.adapter.attempt_count == 0


def test_nc_db_deadline_01_stale_command_validation_cannot_commit(environment) -> None:  # type: ignore[no-untyped-def]
    clock = FakeMonotonic()
    command = environment.command("db-deadline")
    service = AgentService(
        environment.dell,
        environment.agent_codec,
        FakeTargetObserver(environment.target, environment.target),
        environment.adapter,
        critical_db_deadline_seconds=3,
        monotonic=clock,
    )

    result = _stall_writer(environment.dell, lambda: service.handle_command(command), clock)

    assert isinstance(result, dict)
    assert result["reason_code"] == "DB_DEADLINE_AUTHORITY_INVALIDATED"
    assert environment.dell.process_lease_valid("stream-target") is False
    assert environment.dell.read_one("SELECT count(*) FROM agent_commands")[0] == 0
    assert environment.adapter.attempt_count == 0


def test_nc_db_deadline_01_stale_reconciliation_cannot_install_epoch(environment) -> None:  # type: ignore[no-untyped-def]
    clock = FakeMonotonic()
    peer = DellReconciliationService(
        environment.dell,
        environment.agent_codec,
        FakeTargetObserver(environment.target),
        critical_db_deadline_seconds=3,
        monotonic=clock,
    )
    challenge = environment.central_codec.decode(peer.issue_challenge("stream-target"))
    assert environment.dell.fence("stream-target")["action_ready"] == 0
    now = utc_now()
    commit = environment.central_codec.encode(
        {
            "protocol": "cra_dell_recovery.reconciliation_commit.v1",
            "message_type": "reconciliation_commit",
            "commit_id": "commit-db-deadline",
            "reconciliation_id": challenge["reconciliation_id"],
            "challenge_id": challenge["challenge_id"],
            "challenge_nonce": challenge["challenge_nonce"],
            "target_id": "stream-target",
            "controller_instance_id": "cra-controller",
            "agent_installation_id": challenge["agent_installation_id"],
            "proposed_authority_epoch": 2,
            "proposed_authority_session_id": "session-db-deadline",
            "journal_ack_sequence": challenge["local_journal_high_water"],
            "journal_ack_record_digest": challenge["local_journal_high_digest"],
            "issued_at": isoformat_utc(now),
            "expires_at": isoformat_utc(now + timedelta(seconds=30)),
            "key_id": environment.central_codec.signer.key_id,
        }
    )

    result = _stall_writer(environment.dell, lambda: peer.commit(commit), clock)

    assert isinstance(result, AuthorityDeadlineExceeded)
    fence = environment.dell.fence("stream-target")
    assert fence["authority_state"] == "RECONCILING"
    assert fence["highest_authority_epoch_seen"] == 1
    assert environment.adapter.attempt_count == 0
