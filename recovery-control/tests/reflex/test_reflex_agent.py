from __future__ import annotations

import dataclasses
import json
import sqlite3
from pathlib import Path

import pytest

from cra import policy as policy_mod
from cra import signing
from cra.agent import CentralConfig
from cra.decide import NONE, RESTART, WOULD_RESTART
from cra.observe import Network
from cra.policy import Policy
from cra.sync import POLICY_KEY
from tests.reflex.support import Harness


def use_policy(h: Harness, tmp_path: Path, **fields) -> Policy:
    """Install a validly signed cached policy, as if adopted from CRA."""
    key_path = tmp_path / f"policy-{len(list(tmp_path.glob('policy-*')))}.key"
    public = signing.generate_key(key_path)
    policy = dataclasses.replace(Policy(), **{"version": 1, "mode": "enforce", "stall_grace_sec": 0, **fields})
    envelope = policy_mod.sign_envelope(signing.load_private_key(key_path), policy)
    h.config = dataclasses.replace(
        h.config, central=CentralConfig(url="http://127.0.0.1:9", policy_public_key=public, agent_key_path=key_path)
    )
    h.agent.store.put(POLICY_KEY, json.dumps(envelope))
    h.agent = h.new_agent(h.agent.store)
    return policy


def stall(h: Harness, n: int = 6):
    """One baseline sample then n-1 stalled samples (streak counts from the second; default confirms 5)."""
    decision = None
    for _ in range(n):
        decision = h.tick(stalled=True)
    return decision


# --- detection -------------------------------------------------------------------------


def test_healthy_stream_never_probes_or_acts(harness: Harness) -> None:
    for _ in range(10):
        assert harness.tick(stalled=False).reason == "HEALTHY"
    assert harness.prober.calls == 0
    assert harness.executor.requests == []


def test_single_stalled_sample_is_not_confirmed(harness: Harness) -> None:
    harness.tick(stalled=False)
    decision = harness.tick(stalled=True)
    assert (decision.outcome, decision.reason) == (NONE, "STALL_UNCONFIRMED")
    assert harness.prober.calls == 0


def test_repeated_read_of_same_observation_is_not_new_evidence(harness: Harness) -> None:
    harness.tick(stalled=True)
    harness.tick(stalled=True)  # streak 1
    for _ in range(5):
        harness.clock.advance(0.5)
        assert harness.agent.step().reason == "STALL_UNCONFIRMED"  # same file, same sequence
    assert harness.executor.requests == []


def test_missing_tcp_metrics_never_acts(harness: Harness) -> None:
    for _ in range(5):
        decision = harness.tick(stalled=True, tcp_metrics={})
    assert decision.reason == "NO_TCP_METRICS"


def test_stale_and_missing_observation(harness: Harness) -> None:
    assert harness.agent.step().reason == "OBSERVATION_MISSING"
    harness.pod.write(stalled=False)
    harness.clock.advance(10)
    assert harness.agent.step().reason == "OBSERVATION_STALE"


# --- dry run ---------------------------------------------------------------------------


def test_default_policy_is_dry_run_and_records_once_per_generation(harness: Harness) -> None:
    assert stall(harness).outcome == WOULD_RESTART
    for _ in range(5):
        decision = harness.tick(stalled=True)
    assert "GENERATION_ALREADY_ACTED" in decision.blockers
    assert harness.executor.requests == []
    assert harness.kinds().count("action_reserved") == 1


def test_dry_run_verification_distinguishes_self_recovery(harness: Harness) -> None:
    stall(harness)
    harness.tick(stalled=False)
    assert "action_verified" in harness.kinds()
    row = harness.agent.store.db.execute("SELECT verify_result FROM actions").fetchone()
    assert row[0] == "SELF_RECOVERED"


def test_dry_run_verification_still_stalled(harness: Harness) -> None:
    stall(harness)
    for _ in range(40):
        harness.tick(stalled=True)
    row = harness.agent.store.db.execute("SELECT verify_result FROM actions").fetchone()
    assert row[0] == "STILL_STALLED"


# --- enforce: the invariants -----------------------------------------------------------


def test_enforce_sends_exactly_one_request_per_generation(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    assert stall(harness).outcome == RESTART
    for _ in range(10):
        harness.tick(stalled=True)
    assert len(harness.executor.requests) == 1
    request = harness.executor.requests[0]
    assert request["intent_type"] == "RESTART_FFMPEG"
    assert request["expected_ffmpeg_generation"] == "gen-1"
    assert request["idempotency_key"] == request["request_id"] == request["correlation_id"]
    assert request["producer_id"] == "cra-agent" and request["producer_generation"] == 3


def test_request_id_is_derived_from_generation(harness: Harness, tmp_path: Path) -> None:
    from cra.effect import request_id_for

    use_policy(harness, tmp_path)
    stall(harness)
    assert harness.executor.requests[0]["request_id"] == request_id_for("pod-a/gen-1")


def test_agent_restart_cannot_resend_for_same_generation(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    stall(harness)
    harness.agent = harness.new_agent()  # process restart, same database
    stall(harness, 5)
    assert len(harness.executor.requests) == 1


def test_reservation_failure_means_no_request(harness: Harness, tmp_path: Path, monkeypatch) -> None:
    use_policy(harness, tmp_path)

    def broken(**_):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(harness.agent.store, "reserve_action", broken)
    with pytest.raises(sqlite3.OperationalError):
        stall(harness)
    assert harness.executor.requests == []


def test_unknown_outcome_is_never_retried_on_same_generation(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    harness.executor.response = TimeoutError("socket timeout")
    stall(harness)
    harness.executor.response = {"ok": True, "state": "EFFECT_OBSERVED"}
    for _ in range(50):
        harness.tick(stalled=True)
    assert len(harness.executor.requests) == 1
    state = harness.agent.store.db.execute("SELECT state FROM actions").fetchone()[0]
    assert state == "OUTCOME_UNKNOWN"


def test_interrupted_reservation_is_closed_as_unknown_on_startup(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    harness.tick(stalled=True)
    harness.agent.store.reserve_action(
        now=harness.clock(),
        generation_key="pod-a/gen-1",
        mode="enforce",
        request_id="x",
        policy_version=1,
        bytes_sent=0,
        evidence={},
    )
    harness.agent = harness.new_agent()
    state = harness.agent.store.db.execute("SELECT state FROM actions").fetchone()[0]
    assert state == "OUTCOME_UNKNOWN"
    stall(harness, 5)
    assert harness.executor.requests == []


def test_new_generation_may_be_restarted_after_cooldown(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path, cooldown_sec=60)
    stall(harness)
    harness.pod.new_generation()
    harness.pod.uptime = 300
    decision = stall(harness)
    assert "COOLDOWN" in decision.blockers
    harness.clock.advance(60)
    stall(harness)
    assert [r["expected_ffmpeg_generation"] for r in harness.executor.requests] == ["gen-1", "gen-2"]


def test_hourly_limit(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path, cooldown_sec=10, max_per_hour=2, max_per_day=12)
    decisions = []
    for _ in range(4):
        decisions.append(stall(harness))
        harness.clock.advance(30)
        harness.pod.new_generation()
    assert len(harness.executor.requests) == 2
    assert "HOURLY_LIMIT" in decisions[-1].blockers
    harness.clock.advance(3600)
    assert stall(harness).outcome == RESTART


@pytest.mark.parametrize(
    ("network", "blocker"),
    [
        (Network(gateway_ok=False, public_ok_count=0, ingest_ok=False), "NETWORK_DOWN"),
        (Network(gateway_ok=True, public_ok_count=2, ingest_ok=False), "INGEST_UNREACHABLE"),
        (Network(gateway_ok=True, public_ok_count=2, ingest_ok=True, timed_out=True), "NETWORK_PROBE_TIMEOUT"),
    ],
)
def test_non_local_failures_never_restart(harness: Harness, tmp_path: Path, network: Network, blocker: str) -> None:
    use_policy(harness, tmp_path)
    harness.prober.network = network
    assert blocker in stall(harness).blockers
    assert harness.executor.requests == []


@pytest.mark.parametrize(
    ("setup", "blocker"),
    [
        (lambda h: setattr(h.pod, "uptime", 5), "FFMPEG_TOO_YOUNG"),
        (lambda h: setattr(h.pod, "in_flight", 1), "EXECUTOR_IN_FLIGHT"),
        (lambda h: setattr(h.pod, "producer", ("independent-controller", 2)), "NOT_EXECUTOR_AUTHORITY"),
        (lambda h: h.config.stop_file.write_text(""), "LOCAL_STOP"),
    ],
)
def test_guards_block_enforce(harness: Harness, tmp_path: Path, setup, blocker: str) -> None:
    use_policy(harness, tmp_path)
    setup(harness)
    assert blocker in stall(harness).blockers
    assert harness.executor.requests == []


def test_mode_off(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path, mode="off")
    assert "MODE_OFF" in stall(harness).blockers
    assert harness.agent.store.db.execute("SELECT COUNT(*) FROM actions").fetchone()[0] == 0


def test_invalid_cached_policy_falls_back_to_builtin_dry_run(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    envelope = json.loads(harness.agent.store.get(POLICY_KEY))
    envelope["policy"]["max_per_hour"] = 60  # tampered after signing
    harness.agent.store.put(POLICY_KEY, json.dumps(envelope))
    assert stall(harness).outcome == WOULD_RESTART
    assert harness.executor.requests == []


# --- verification ----------------------------------------------------------------------


def test_enforce_verification_recovered(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    stall(harness)
    harness.pod.new_generation()
    harness.pod.uptime = 0
    harness.tick(stalled=False)
    harness.tick(stalled=False)
    row = harness.agent.store.db.execute("SELECT verify_result FROM actions").fetchone()
    assert row[0] == "DELIVERY_RESUMED"


def test_enforce_verification_not_recovered(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    stall(harness)
    for _ in range(40):
        harness.tick(stalled=True)
    row = harness.agent.store.db.execute("SELECT verify_result FROM actions").fetchone()
    assert row[0] == "NOT_RECOVERED"


def test_audit_is_append_only(harness: Harness) -> None:
    harness.tick(stalled=False)
    with pytest.raises(sqlite3.IntegrityError):
        harness.agent.store.db.execute("DELETE FROM audit")
    with pytest.raises(sqlite3.IntegrityError):
        harness.agent.store.db.execute("UPDATE audit SET kind = 'x'")


def test_decisions_are_logged_on_change_only(harness: Harness) -> None:
    for _ in range(20):
        harness.tick(stalled=False)
    assert harness.kinds().count("decision") == 1


def test_once_does_not_create_state(tmp_path: Path, monkeypatch, capsys) -> None:
    from cra import agent as agent_mod

    config = {
        "agent_id": "x",
        "db_path": str(tmp_path / "state" / "agent.db"),
        "observation_path": str(tmp_path / "obs.json"),
        "executor_socket": str(tmp_path / "effect.sock"),
        "producer_id": "cra-agent",
        "producer_generation": 3,
        "stop_file": str(tmp_path / "STOP"),
        "status_path": str(tmp_path / "run" / "status.json"),
        "ingest_host": "127.0.0.1",
        "ingest_port": 9,
        "central": {"url": "http://127.0.0.1:9", "policy_public_key": "AAAA", "agent_key_path": str(tmp_path / "k")},
    }
    (tmp_path / "agent.json").write_text(json.dumps(config))
    assert agent_mod.main(["once", "--config", str(tmp_path / "agent.json")]) == 0
    assert "OBSERVATION_MISSING" in capsys.readouterr().out
    assert not (tmp_path / "state").exists() and not (tmp_path / "run").exists()


def test_verified_recovery_closes_the_executor_fence(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    stall(harness)
    request_id = harness.executor.requests[0]["request_id"]
    scope = {
        "effect_scope_id": "scope-1",
        "owner_request_id": request_id,
        "owner_request_digest": "digest-1",
        "state": "EFFECT_OBSERVED_AWAITING_VERIFICATION",
        "identity": {"ffmpeg_pid": 4242},
    }
    harness.agent.unresolved_reader = lambda _socket: [scope]
    harness.executor.response = {"ok": True, "state": "RECONCILED_EFFECT_OBSERVED"}
    harness.pod.in_flight = 1
    harness.pod.new_generation()
    harness.pod.uptime = 0
    harness.tick(stalled=False)
    harness.tick(stalled=False)
    harness.clock.advance(30)
    harness.tick(stalled=False)
    sent = [r for r in harness.executor.requests if r["schema_version"] == "runtime.effect_reconciliation_request.v1"]
    assert len(sent) == 1
    assert sent[0]["owner_request_id"] == request_id and sent[0]["resolution"] == "EFFECT_OBSERVED"
    assert sent[0]["evidence"]["transport"]["tcp_probe_ok"] is True
    for _ in range(5):  # done: never resent
        harness.clock.advance(30)
        harness.tick(stalled=False)
    assert len([r for r in harness.executor.requests if r["schema_version"].endswith("reconciliation_request.v1")]) == 1


def test_fence_of_an_unknown_outcome_is_left_for_an_operator(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    harness.executor.response = TimeoutError("lost")
    stall(harness)
    scope = {
        "effect_scope_id": "s",
        "owner_request_id": harness.executor.requests[0]["request_id"],
        "owner_request_digest": "d",
        "state": "OUTCOME_UNKNOWN",
        "identity": {},
    }
    harness.agent.unresolved_reader = lambda _socket: [scope]
    harness.pod.in_flight = 1
    harness.pod.new_generation()
    for _ in range(40):
        harness.clock.advance(5)
        harness.tick(stalled=False)
    assert not [r for r in harness.executor.requests if r["schema_version"].endswith("reconciliation_request.v1")]
