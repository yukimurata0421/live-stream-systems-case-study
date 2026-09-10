from __future__ import annotations

import threading

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_dell_recovery.models import MonitoringReadiness
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.runner.compound import FakeMonotonic
from dell_recovery_agent.authority import AgentAuthorityLease


def test_repeated_suspect_ticks_do_not_amplify_transition_writes(environment) -> None:  # type: ignore[no-untyped-def]
    clock = FakeMonotonic()
    publisher = AuthorityHeartbeatPublisher(environment.central, environment.central_codec, lease_ttl_seconds=15)
    lease = AgentAuthorityLease(environment.dell, environment.agent_codec, monotonic=clock)
    heartbeat = publisher.build(
        "stream-target",
        MonitoringReadiness(True, True, isoformat_utc(utc_now()), "idempotent-transition-test"),
    )
    assert heartbeat is not None
    assert lease.receive(heartbeat) == "CENTRAL_ACTIVE"
    clock.advance(5)
    assert lease.tick("stream-target") == "CENTRAL_SUSPECT"
    transition_count = environment.dell.connection.execute("SELECT count(*) FROM agent_transitions").fetchone()[0]
    for _ in range(20):
        clock.advance(0.1)
        assert lease.tick("stream-target") == "CENTRAL_SUSPECT"
    assert environment.dell.connection.execute("SELECT count(*) FROM agent_transitions").fetchone()[0] == transition_count


def test_critical_fence_read_uses_committed_reader_snapshot(environment) -> None:  # type: ignore[no-untyped-def]
    writer_entered = threading.Event()
    release_writer = threading.Event()
    reader_finished = threading.Event()
    observed_states: list[str] = []
    committed_before = str(environment.dell.fence("stream-target")["authority_state"])

    def writer() -> None:
        with environment.dell.write() as db:
            db.execute(
                "UPDATE authority_fences SET authority_state='CENTRAL_SUSPECT',state_reason='UNCOMMITTED_TEST' WHERE target_id=?",
                ("stream-target",),
            )
            writer_entered.set()
            assert release_writer.wait(timeout=2)

    def reader() -> None:
        observed_states.append(str(environment.dell.fence("stream-target")["authority_state"]))
        reader_finished.set()

    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)
    writer_thread.start()
    assert writer_entered.wait(timeout=2)
    reader_thread.start()
    assert reader_finished.wait(timeout=2)
    assert observed_states == [committed_before]
    release_writer.set()
    writer_thread.join(timeout=2)
    reader_thread.join(timeout=2)
    assert environment.dell.fence("stream-target")["authority_state"] == "CENTRAL_SUSPECT"
