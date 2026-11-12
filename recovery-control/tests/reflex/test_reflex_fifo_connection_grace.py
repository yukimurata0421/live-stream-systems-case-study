"""A FIFO socket replacement starts fresh stall evidence and a fresh grace window."""

from __future__ import annotations

from datetime import datetime

import pytest

from cra.decide import NONE, RESTART
from tests.reflex.support import Harness
from tests.reflex.test_reflex_agent import use_policy

TCP = {
    "bytes_sent": 100_000,
    "bytes_acked": 1_000,
    "notsent": 300_000,
    "unacked": 120,
    "lastsnd_ms": 20,
    "transport_connection_generation": 7,
    "socket_cookie": "a7",
    "socket_inode": "5007",
}


def tick(h: Harness, tcp: dict):
    return h.tick(stalled=True, runtime_lifecycle_state="FFMPEG_RUNNING", tcp_metrics=dict(tcp))


@pytest.mark.parametrize("bound", [False, True], ids=["legacy", "bound"])
@pytest.mark.parametrize(
    "field,value",
    [("transport_connection_generation", 8), ("socket_cookie", "a8"), ("socket_inode", "5008")],
)
def test_connection_change_resets_streak_and_waits_a_full_new_90_seconds(harness, tmp_path, bound, field, value):
    h = harness
    use_policy(h, tmp_path, version=3, stall_grace_sec=90)
    h.pod.bound = bound
    tick(h, TCP)
    for _ in range(44):
        assert tick(h, TCP).outcome == NONE
    assert h.agent.tracker.streak == 44
    old_start = h.agent.tracker.stall_started_at

    changed = {**TCP, field: value}
    assert tick(h, changed).outcome == NONE
    new_start = h.clock()
    assert h.agent.tracker.streak == 0
    assert h.agent.tracker.stall_started_at is None
    assert h.prober.calls == 0
    assert h.executor.requests == []
    assert h.agent.store.db.execute("SELECT count(*) FROM actions").fetchone()[0] == 0
    for index in range(1, 45):
        decision = tick(h, changed)
        assert decision.outcome == NONE
        assert h.agent.tracker.streak == index
        assert h.agent.tracker.stall_started_at == new_start != old_start
        assert h.executor.requests == []
    assert (h.clock() - new_start).total_seconds() == 88
    assert tick(h, changed).outcome == RESTART
    assert len(h.executor.requests) == 1
    request = h.executor.requests[0]
    assert request["schema_version"] == "runtime.typed_effect_request.v2"
    assert (datetime.fromisoformat(request["issued_at"]) - new_start).total_seconds() == 90
    assert h.prober.calls == 1


@pytest.mark.parametrize("stage", ["before_reservation", "after_reservation"])
def test_connection_change_at_dispatch_cannot_send_and_preserves_consumption(harness, tmp_path, monkeypatch, stage):
    h = harness
    use_policy(h, tmp_path, version=3)
    tick(h, TCP)
    for _ in range(4):
        tick(h, TCP)
    changed = {**TCP, "transport_connection_generation": 8}
    if stage == "before_reservation":
        original = h.prober.probe

        def probe(endpoint):
            result = original(endpoint)
            h.pod.write(stalled=True, tcp_metrics=changed)
            return result

        monkeypatch.setattr(h.prober, "probe", probe)
    else:
        original = h.agent.store.reserve_action

        def reserve(**kwargs):
            result = original(**kwargs)
            h.pod.write(stalled=True, tcp_metrics=changed)
            return result

        monkeypatch.setattr(h.agent.store, "reserve_action", reserve)
    decision = tick(h, TCP)
    assert decision.outcome == NONE
    assert decision.reason == "DISPATCH_BLOCKED"
    assert decision.blockers == ("TARGET_CHANGED",)
    assert h.agent.tracker.streak == 0
    assert h.agent.tracker.stall_started_at is None
    assert h.executor.requests == []
    rows = h.agent.store.db.execute("SELECT state FROM actions").fetchall()
    if stage == "after_reservation":
        assert [r["state"] for r in rows] == ["CANCELLED_BEFORE_SEND"]
        assert h.agent.store.history(now=h.clock(), generation_key=f"pod-a/gen-{h.pod.generation}", mode="enforce").actions_last_hour == 1
    else:
        assert rows == []


def test_legacy_metrics_without_connection_metadata_keep_the_existing_grace(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path, version=3, stall_grace_sec=90)
    legacy = {k: v for k, v in TCP.items() if k not in ("transport_connection_generation", "socket_cookie", "socket_inode")}
    tick(h, legacy)
    for _ in range(44):
        assert tick(h, legacy).outcome == NONE
    assert tick(h, legacy).outcome == RESTART
    assert len(h.executor.requests) == 1


def test_missing_then_restored_connection_metadata_cannot_reuse_stall_time(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path, version=3, stall_grace_sec=90)
    tick(h, TCP)
    for _ in range(44):
        tick(h, TCP)
    missing = {k: v for k, v in TCP.items() if k not in ("transport_connection_generation", "socket_cookie", "socket_inode")}
    assert tick(h, missing).outcome == NONE
    assert h.agent.tracker.streak == 0
    assert tick(h, TCP).outcome == NONE
    assert h.agent.tracker.streak == 0
    assert h.agent.tracker.stall_started_at is None
    assert h.executor.requests == []
