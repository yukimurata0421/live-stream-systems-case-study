"""The agent waits out the stream engine's own recovery before acting (stall_grace_sec).

Shape taken from 2026-10-23 06:31 JST: the connection died with the daily WAN renumbering,
FFmpeg exited by itself about 14 s after the stall began and the engine started the next
generation 5 s later. Within the grace window the agent records the stall but does not act.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

import cra.agent
from cra import policy as policy_mod
from cra.decide import NONE, RESTART, WOULD_RESTART
from cra.policy import BUILTIN, Policy, PolicyError
from tests.reflex.support import Harness
from tests.reflex.test_reflex_agent import use_policy


@pytest.fixture
def graced(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> Harness:
    monkeypatch.setattr(cra.agent, "BUILTIN", BUILTIN)  # the real default: 90 s
    harness.agent = harness.new_agent(harness.agent.store)
    return harness


def stall_for(h: Harness, seconds: int):
    decision = h.tick(stalled=True)  # baseline sample
    for _ in range(seconds // 2):
        decision = h.tick(stalled=True)
    return decision


def test_confirmed_stall_inside_grace_is_recorded_but_not_acted_on(graced: Harness) -> None:
    decision = stall_for(graced, 20)
    assert (decision.outcome, decision.reason, decision.blockers) == (NONE, "STALL_CONFIRMED", ("WITHIN_ENGINE_GRACE",))
    assert graced.prober.calls == 0  # no network probing while waiting
    assert graced.agent.store.db.execute("SELECT COUNT(*) FROM actions").fetchone()[0] == 0
    assert "WITHIN_ENGINE_GRACE" in json.dumps([r["payload"] for r in graced.agent.store.read_audit(0, 1000)])


def test_engine_self_recovery_inside_grace_never_becomes_a_candidate(graced: Harness) -> None:
    stall_for(graced, 14)
    graced.pod.new_generation()  # FFmpeg exited by itself; the engine started the next one
    for _ in range(30):
        assert graced.tick(stalled=False).outcome == NONE
    assert graced.agent.store.db.execute("SELECT COUNT(*) FROM actions").fetchone()[0] == 0


def test_stall_outliving_the_grace_window_becomes_a_candidate(graced: Harness) -> None:
    assert stall_for(graced, 88).outcome == NONE
    assert WOULD_RESTART in stall_for_more(graced, 4)
    assert graced.prober.calls >= 1


def stall_for_more(h: Harness, seconds: int) -> list:
    return [h.tick(stalled=True).outcome for _ in range(seconds // 2)]


def test_enforce_waits_for_the_grace_window(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path, stall_grace_sec=60)
    assert stall_for(harness, 40).blockers == ("WITHIN_ENGINE_GRACE",)
    assert harness.executor.requests == []
    assert RESTART in stall_for_more(harness, 24)
    assert len(harness.executor.requests) == 1


def test_ffmpeg_already_exiting_is_left_to_the_engine(harness: Harness, tmp_path: Path) -> None:
    use_policy(harness, tmp_path)
    harness.tick(stalled=True)
    for _ in range(6):
        decision = harness.tick(stalled=True, runtime_lifecycle_state="STOPPING")
    assert "FFMPEG_NOT_IN_RUNNING_STATE" in decision.blockers
    assert harness.executor.requests == []


def test_grace_counts_from_the_sample_before_the_first_stalled_interval(graced: Harness) -> None:
    graced.tick(stalled=False)
    started = graced.clock()
    for _ in range(3):
        graced.tick(stalled=True)
    assert graced.agent.tracker.stall_started_at == started


def test_v1_policy_without_the_field_gets_the_default(tmp_path: Path) -> None:
    body = {k: v for k, v in Policy(version=1).to_dict().items() if k != "stall_grace_sec"}
    assert policy_mod.parse(body).stall_grace_sec == 90
    assert policy_mod.parse({**body, "stall_grace_sec": 0}).stall_grace_sec == 0
    with pytest.raises(PolicyError):
        policy_mod.parse({**body, "stall_grace_sec": 601})


def test_signed_v1_policy_stays_valid(tmp_path: Path) -> None:
    from cra import signing

    public = signing.generate_key(tmp_path / "k")
    body = {k: v for k, v in Policy(version=1).to_dict().items() if k != "stall_grace_sec"}
    envelope = {"policy": body, "signature": signing.sign(signing.load_private_key(tmp_path / "k"), body)}
    assert policy_mod.open_envelope(envelope, public) == dataclasses.replace(Policy(version=1), stall_grace_sec=90)
