"""Source-cadence regression through the real compatibility notification planner."""
from datetime import datetime, timezone
import json

import pytest

from stream_monitoring_v4.compatibility import notification_observation as evidence
from stream_monitoring_v4.compatibility import observation_time as timing
from stream_monitoring_v4.compatibility.twitch_alert_notify import key, plan

WALL = 1_800_000_000.
VIEWER = "StreamV3ViewerSyntheticProbeFailed"
CASES = [
    ("StreamV3RecoveryActionPending", 60, 300),
    ("StreamV3CurrentFail", 240, 300),
    ("StreamV3MapDeliveryCriticalUnhealthy", 300, 360),
    (VIEWER, 300, 600),
]


def reading(t, *, step=0, boot="boot-1"):
    return timing.Reading(WALL + t + step, 1000. + t, "fixture", boot, "a" * 64)


def frame(name, t, *, good=False, boot="boot-1"):
    return timing.attach({"action": "none" if good else "reload_overlay", "execute": False,
                          "healthy": good, "delivery_critical_ok": good, "probe_ok": good,
                          "status": "healthy" if good else "failed"},
                         evidence.SPECS[name][1], now=reading(t, boot=boot))


def alert(name):
    return {"labels": {"alertname": name, "severity": "warning"}, "state": "firing",
            "activeAt": datetime.fromtimestamp(WALL, timezone.utc).isoformat()}


def tick(name, t, state, payload, *, firing=True, step=0, boot="boot-1"):
    now = reading(t, step=step, boot=boot)
    samples = evidence.Evidence({name: payload}, now=now)
    messages, state = plan([alert(name)] if firing else [], json.loads(json.dumps(state)),
                           now.wall, repeat_sec=3600, observation_evidence=samples, reading=now)
    return messages, state, samples[name][0]


@pytest.mark.parametrize("name,period,_loss_at", CASES)
@pytest.mark.parametrize("step", [0, 10000, -10000])
def test_normal_samples_at_the_source_cadence_confirm_recovery(name, period, _loss_at, step):
    messages, state, _ = tick(name, 0, {}, frame(name, 0))
    assert len(messages) == 1
    normal = frame(name, period, good=True)
    messages, state, sample = tick(name, period, state, normal, firing=False, step=step)
    assert sample.valid and not messages and key(alert(name)) in state
    messages, state, _ = tick(name, period + 30, state, normal, firing=False, step=step)
    assert not messages and state[key(alert(name))]["observation_continuity"]["count"] == 1
    messages, state, sample = tick(name, 2 * period, state, frame(name, 2 * period, good=True),
                                  firing=False, step=step)
    assert sample.valid and len(messages) == 1 and state == {}


@pytest.mark.parametrize("name,_period,loss_at", CASES)
def test_loss_deadline_uses_the_source_window_and_cached_reads_do_not_renew_it(name, _period, loss_at):
    original = frame(name, 0)
    _, state, _ = tick(name, 0, {}, original)
    for t in (30, 150, loss_at - 1):
        messages, state, sample = tick(name, t, state, original)
        assert not messages and not state[key(alert(name))]["observation_lost"]
        if name == VIEWER:
            assert sample.valid
    messages, state, _ = tick(name, loss_at, state, original)
    assert len(messages) == 1 and str(loss_at) in messages[0]["content"]
    assert state[key(alert(name))]["observation_lost"]
    messages, _, _ = tick(name, loss_at + 30, state, original)
    assert messages == []


def test_gap_longer_than_the_source_ttl_restarts_normal_continuity():
    _, state, _ = tick(VIEWER, 0, {}, frame(VIEWER, 0))
    _, state, _ = tick(VIEWER, 300, state, frame(VIEWER, 300, good=True), firing=False)
    messages, state, _ = tick(VIEWER, 901, state, frame(VIEWER, 901, good=True), firing=False)
    assert not messages and state[key(alert(VIEWER))]["observation_continuity"]["count"] == 1
    messages, state, _ = tick(VIEWER, 960, state, frame(VIEWER, 960, good=True), firing=False)
    assert len(messages) == 1 and state == {}


def test_missing_sample_breaks_normal_continuity_even_inside_the_source_ttl():
    _, state, _ = tick(VIEWER, 0, {}, frame(VIEWER, 0))
    _, state, _ = tick(VIEWER, 300, state, frame(VIEWER, 300, good=True), firing=False)
    _, state, _ = tick(VIEWER, 330, state, {}, firing=False)
    messages, state, _ = tick(VIEWER, 600, state, frame(VIEWER, 600, good=True), firing=False)
    assert not messages and state[key(alert(VIEWER))]["observation_continuity"]["count"] == 1
    messages, state, _ = tick(VIEWER, 900, state, frame(VIEWER, 900, good=True), firing=False)
    assert len(messages) == 1 and state == {}


def test_cached_normal_sample_cannot_complete_recovery_at_a_slow_cadence():
    _, state, _ = tick(VIEWER, 0, {}, frame(VIEWER, 0))
    normal = frame(VIEWER, 300, good=True)
    for t in range(300, 901, 30):
        _, state, _ = tick(VIEWER, t, state, normal, firing=False)
        assert key(alert(VIEWER)) in state
        assert state[key(alert(VIEWER))]["observation_continuity"]["count"] == 1


def test_reboot_cannot_reuse_the_preceding_normal_observation():
    _, state, _ = tick(VIEWER, 0, {}, frame(VIEWER, 0))
    _, state, _ = tick(VIEWER, 300, state, frame(VIEWER, 300, good=True), firing=False)
    messages, state, _ = tick(VIEWER, 600, state, frame(VIEWER, 600, good=True, boot="boot-2"),
                             firing=False, boot="boot-2")
    assert not messages and state[key(alert(VIEWER))]["observation_continuity"]["count"] == 1
    messages, state, _ = tick(VIEWER, 900, state, frame(VIEWER, 900, good=True, boot="boot-2"),
                             firing=False, boot="boot-2")
    assert len(messages) == 1 and state == {}


def test_source_restoration_is_announced_while_the_media_incident_remains_bad():
    old = frame(VIEWER, 0)
    _, state, _ = tick(VIEWER, 0, {}, old)
    messages, state, _ = tick(VIEWER, 600, state, old)
    assert len(messages) == 1 and state[key(alert(VIEWER))]["observation_loss_notified"]
    messages, state, _ = tick(VIEWER, 630, state, frame(VIEWER, 630))
    assert messages == []
    messages, state, _ = tick(VIEWER, 930, state, frame(VIEWER, 930))
    assert len(messages) == 1 and key(alert(VIEWER)) in state
    assert not state[key(alert(VIEWER))].get("observation_loss_notified")
    messages, _, _ = tick(VIEWER, 960, state, frame(VIEWER, 930))
    assert messages == []
