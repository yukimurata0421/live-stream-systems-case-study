"""Regressions for dry-run integrity and the final dispatch boundary; effects are fake."""

from __future__ import annotations

import argparse
import dataclasses
import json
import threading
import time
from datetime import datetime

import pytest

from cra.central.cli import cmd_verify
from cra.central.store import CentralStore
from cra.central.verify import verify
from cra.decide import RESTART, WOULD_RESTART
from cra.observe import Endpoint, Network, Prober
from tests.reflex.support import T0, Harness
from tests.reflex.test_reflex_agent import stall, use_policy
from tests.reflex.test_reflex_central import POLICIES, _record, _reserved


def action(h: Harness):
    return h.agent.store.db.execute("SELECT * FROM actions ORDER BY id DESC LIMIT 1").fetchone()


def test_ingest_probe_follows_route_and_unmapped_route_blocks(harness):
    h = harness
    h.config = dataclasses.replace(h.config, ingest_endpoints={"FALLBACK": Endpoint("127.0.0.1", 1936)})
    h.agent = h.new_agent(h.agent.store)
    for _ in range(6):
        decision = h.tick(stalled=True, delivery_route="FALLBACK", delivery_route_generation=1)
    assert decision.outcome == WOULD_RESTART
    assert h.prober.endpoints == [Endpoint("127.0.0.1", 1936)]
    assert h.tick(stalled=True, delivery_route="UNKNOWN").reason == "TARGET_MISMATCH"
    assert len(h.prober.endpoints) == 1


@pytest.mark.parametrize("missing", [True, False])
def test_observation_discontinuity_restarts_stall_confirmation(harness, missing):
    h = harness
    stall(h, 5)
    assert h.agent.tracker.streak == 4
    h.clock.advance(600)
    if missing:
        h.config.observation_path.unlink()
        assert h.agent.step().reason == "OBSERVATION_MISSING"
        assert h.agent.tracker.streak == 0
    assert h.tick(stalled=True).outcome == "NONE"
    assert h.agent.tracker.streak == 0
    assert h.agent.store.db.execute("SELECT COUNT(*) FROM actions").fetchone()[0] == 0


def test_ffmpeg_absence_is_not_reported_as_healthy(harness):
    decision = harness.tick(stalled=False, ffmpeg_running=False, ffmpeg_generation="", tcp_metrics={})
    assert decision.reason == "FFMPEG_NOT_RUNNING"
    assert harness.executor.requests == []


def test_slow_probe_uses_fresh_request_timestamp(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)

    def probe(endpoint):
        h.clock.advance(4)
        h.pod.write(stalled=True)
        return Network(True, 2, True)

    h.prober.probe = probe
    assert stall(h).outcome == RESTART
    request = h.executor.requests[0]
    assert datetime.fromisoformat(request["issued_at"]) == h.clock()
    assert (datetime.fromisoformat(request["expires_at"]) - h.clock()).total_seconds() == 3


def test_missing_observation_after_failed_network_probe_resets_tracker(harness):
    h = harness

    def probe(endpoint):
        h.config.observation_path.unlink()
        return Network(False, 0, False)

    h.prober.probe = probe
    assert stall(h).reason == "OBSERVATION_MISSING"
    assert h.agent.tracker.streak == 0
    assert h.executor.requests == []


@pytest.mark.parametrize("change", ["stop", "policy", "generation", "executor", "route", "healthy", "stale", "shutdown"])
def test_changes_during_probe_never_send_or_reserve(harness, tmp_path, change):
    h = harness
    use_policy(h, tmp_path)
    original = h.agent.policy()

    def probe(endpoint):
        if change == "stop":
            h.config.stop_file.touch()
        elif change == "policy":
            h.agent.policy = lambda: dataclasses.replace(original, mode="dry_run")
        elif change == "stale":
            h.clock.advance(4)
        elif change == "shutdown":
            h.agent.cancelled = lambda: True
        else:
            if change == "generation":
                h.pod.new_generation()
            overrides = {"executor_instance_id": "different"} if change == "executor" else {}
            if change == "route":
                overrides["delivery_route_generation"] = 2
            h.pod.write(stalled=change != "healthy", **overrides)
        return Network(True, 2, True)

    h.prober.probe = probe
    assert stall(h).outcome == "NONE"
    assert h.executor.requests == []
    assert action(h) is None


def test_stop_during_reservation_cancels_but_keeps_claim(harness, tmp_path, monkeypatch):
    h = harness
    use_policy(h, tmp_path)
    reserve = h.agent.store.reserve_action

    def stopping_reserve(**kwargs):
        result = reserve(**kwargs)
        h.config.stop_file.touch()
        return result

    monkeypatch.setattr(h.agent.store, "reserve_action", stopping_reserve)
    assert "LOCAL_STOP" in stall(h).blockers
    assert h.executor.requests == []
    assert action(h)["state"] == "CANCELLED_BEFORE_SEND"
    h.config.stop_file.unlink()
    assert "GENERATION_ALREADY_ACTED" in h.tick(stalled=True).blockers


@pytest.mark.parametrize("change", ["policy", "generation", "network_age"])
def test_final_guard_runs_after_durable_reservation(harness, tmp_path, monkeypatch, change):
    h = harness
    use_policy(h, tmp_path)
    reserve = h.agent.store.reserve_action
    policy = h.agent.policy()

    def changed_reserve(**kwargs):
        result = reserve(**kwargs)
        if change == "policy":
            h.agent.policy = lambda: dataclasses.replace(policy, mode="off")
        elif change == "generation":
            h.pod.new_generation()
            h.pod.write(stalled=True)
        else:
            h.clock.advance(4)
            h.pod.write(stalled=True)
        return result

    monkeypatch.setattr(h.agent.store, "reserve_action", changed_reserve)
    assert stall(h).outcome == "NONE"
    assert h.executor.requests == []
    assert action(h)["state"] == "CANCELLED_BEFORE_SEND"


def test_rejected_keeps_generation_claim_and_budget(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    h.executor.response = {"ok": False, "state": "REJECTED"}
    stall(h)
    h.clock.advance(120)
    stall(h)
    assert len(h.executor.requests) == 1
    assert action(h)["state"] == "REJECTED"
    assert h.agent.store.history(now=h.clock(), generation_key="pod-a/gen-1", mode="enforce").actions_last_hour == 1


@pytest.mark.parametrize(
    "state,uptime,executor,expected",
    [
        ("EFFECT_OBSERVED", 0, "exec-1", "DELIVERY_RESUMED"),
        ("OUTCOME_UNKNOWN", 0, "exec-1", None),
        ("EFFECT_OBSERVED", 120, "exec-1", "GENERATION_CHANGED_UNATTRIBUTED"),
        ("EFFECT_OBSERVED", 0, "different", "GENERATION_CHANGED_UNATTRIBUTED"),
    ],
)
def test_recovery_requires_observed_effect_and_successor_start(harness, tmp_path, state, uptime, executor, expected):
    h = harness
    use_policy(h, tmp_path)
    h.executor.response = TimeoutError() if state == "OUTCOME_UNKNOWN" else {"ok": True, "state": state}
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = uptime
    h.tick(stalled=False, executor_instance_id=executor, **({"recovery_successor": None} if state == "OUTCOME_UNKNOWN" else {}))
    h.tick(stalled=False, executor_instance_id=executor)
    assert action(h)["verify_result"] == expected
    assert action(h)["state"] == state


def test_successor_waits_for_flow_in_verification_window(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=True)
    assert action(h)["verify_result"] is None
    h.tick(stalled=False)
    assert action(h)["verify_result"] == "DELIVERY_RESUMED"


def test_parallel_network_probes_share_a_deadline():
    release = threading.Event()

    def blocked(*_):
        release.wait(1)
        return True

    prober = Prober(["127.0.0.1"], run=blocked, gateway=lambda: "127.0.0.1", connect=blocked)
    start = time.monotonic()
    try:
        network = prober.probe(Endpoint("127.0.0.1", 1936), budget_sec=0.05)
        assert time.monotonic() - start < 0.5
        assert network.timed_out and not network.ingest_ok
    finally:
        release.set()


def test_seq_gap_is_not_acked_and_replay_repairs_it_with_history(tmp_path):
    store = CentralStore(tmp_path / "central.db")
    records = [{"seq": i, "ts": T0.isoformat(), "kind": "heartbeat", "payload": {}} for i in range(1, 4)]
    assert store.ingest("a", "i", records[1:]) == 0
    assert store.audit_records() == []
    assert store.integrity()["missing_seq_count"] == 1
    assert store.ingest("a", "i", records[:1]) == 1
    assert store.integrity()["unresolved_gaps"] == 1
    assert store.ingest("a", "i", records[1:]) == 3
    assert store.integrity()["unresolved_gaps"] == 0
    assert store.integrity()["missing_seq_count"] == 0
    assert store.integrity()["gap_observations"] == 1
    assert [r["seq"] for r in store.audit_records()] == [1, 2, 3]


def test_existing_hole_is_detected_and_repaired_without_overwrite(tmp_path):
    store = CentralStore(tmp_path / "central.db")
    store.db.execute("INSERT INTO audit VALUES ('a','i',2,?,'heartbeat','{}',?)", (T0.isoformat(), T0.isoformat()))
    assert store.integrity()["missing_seq_count"] == 1
    record = {"seq": 1, "ts": T0.isoformat(), "kind": "heartbeat", "payload": {}}
    assert store.ingest("a", "i", [record]) == 2
    assert store.integrity()["missing_seq_count"] == 0


def test_verify_command_reports_gap_counts_and_retains_resolved_history(tmp_path, capsys):
    db = tmp_path / "central.db"
    store = CentralStore(db)
    config = tmp_path / "central.json"
    config.write_text(
        json.dumps({"db_path": str(db), "policy_envelope_path": "unused", "policy_key_path": "unused", "agents": {"a": "unused"}})
    )
    args = argparse.Namespace(config=config, agent=None)
    now = datetime.now(T0.tzinfo).isoformat()
    records = [{"seq": i, "ts": now, "kind": "heartbeat", "payload": {}} for i in (1, 2)]
    store.ingest("a", "i", records[1:])
    assert cmd_verify(args) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["audit_integrity"]["missing_seq_count"] == 1
    assert report["conflicting_records"] == 0
    assert "I9_AUDIT_GAP" in {v["code"] for v in report["violations"]}
    store.ingest("a", "i", records[:1])
    assert cmd_verify(args) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["audit_integrity"]["missing_seq_count"] == 0
    assert report["audit_integrity"]["unresolved_gaps"] == 1
    store.ingest("a", "i", records)
    assert cmd_verify(args) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["audit_integrity"]["missing_seq_count"] == 0
    assert report["audit_integrity"]["gap_observations"] == 1


def test_verifier_covers_empty_expected_agents_and_cross_instance_dry_limits():
    assert {v["code"] for v in verify([], {}, now=T0, expected_agents={"dell"})["violations"]} == {
        "AUDIT_EMPTY",
        "AGENT_UNOBSERVED",
    }
    first = _reserved(1, 1, "pod/g1", 0, mode="dry_run")
    second = {**_reserved(1, 1, "pod/g1", 0.1, mode="dry_run"), "instance_id": "new-db"}
    records = [first, second, _record(2, "action_result", {"action_id": 1, "state": "DRY_RUN"})]
    codes = {v["code"] for v in verify(records, POLICIES, now=T0)["violations"]}
    assert {"I1_DUPLICATE_GENERATION_ACTION", "I6_COOLDOWN"} <= codes
