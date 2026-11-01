"""Compatibility notifications through verified producer facts, with no sends."""
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path

from stream_monitoring_v4.compatibility import notification_observation as evidence
from stream_monitoring_v4.compatibility import observation_time as timing
from stream_monitoring_v4.compatibility.twitch_alert_notify import plan, key

WALL = 1_800_000_000.
NAME = "StreamV3RecoveryActionPending"


def reading(t=0, *, step=0, boot="boot-1"):
    return timing.Reading(WALL + t + step, 1000. + t, "local", boot, "a" * 64)


def alert():
    return {"labels": {"alertname": NAME, "severity": "warning"}, "state": "firing",
            "activeAt": datetime.fromtimestamp(WALL - 600, timezone.utc).isoformat()}


def sample(t, *, bad=True, step=0, boot="boot-1"):
    metadata = timing.stamp("recovery_plan", now=reading(t, boot=boot))
    return timing.qualify(metadata, now=reading(t, step=step, boot=boot)), "BAD" if bad else "GOOD"


def tick(t, state, *, value=None, firing=True, step=0, boot="boot-1"):
    return plan([alert()] if firing else [], state, WALL + t + step, repeat_sec=3600,
                observation_evidence={NAME: sample(t, step=step, boot=boot) if value is None else value},
                reading=reading(t, step=step, boot=boot))


def test_contract_copy_matches_the_canonical_v3_implementation():
    canonical = Path(__file__).resolve().parents[2] / "src/stream_core/common/observation_time.py"
    assert Path(timing.__file__).read_bytes() == canonical.read_bytes()


def test_confirmed_failure_is_immediate_and_one_missing_poll_does_not_resolve():
    messages, state = tick(0, {})
    assert len(messages) == 1
    missing = timing.Qualified("UNVERIFIABLE", "missing"), "UNKNOWN"
    messages, state = tick(30, state, firing=False, value=missing)
    assert messages == [] and key(alert()) in state
    messages, state = tick(60, state)
    assert messages == []


def test_distinct_normal_samples_resolve_even_while_old_prometheus_alert_is_firing():
    _, state = tick(0, {})
    normal = sample(60, bad=False)
    messages, state = tick(60, state, value=normal)
    assert messages == []
    messages, state = tick(120, state, value=normal)
    assert messages == []
    messages, state = tick(125, state, value=sample(125, bad=False))
    assert len(messages) == 1 and "Resolved" in messages[0]["content"]
    assert state == {}


def test_unknown_then_good_requires_new_normal_continuity():
    _, state = tick(0, {})
    _, state = tick(60, state, value=sample(60, bad=False), firing=False)
    missing = timing.Qualified("UNVERIFIABLE", "clock_unavailable"), "UNKNOWN"
    _, state = tick(90, state, value=missing, firing=False)
    messages, state = tick(120, state, value=sample(120, bad=False), firing=False)
    assert messages == []
    messages, state = tick(165, state, value=sample(165, bad=False), firing=False)
    assert len(messages) == 1 and state == {}


def test_clock_steps_do_not_trigger_reminders_or_escalation():
    _, state = tick(0, {})
    for elapsed, step in [(60, 10000), (120, -10000), (180, 20000)]:
        messages, state = tick(elapsed, state, step=step)
        assert messages == []
        assert state[key(alert())]["channel"] == "discord"


def test_reboot_does_not_reuse_the_old_recovery_candidate():
    _, state = tick(0, {})
    _, state = tick(60, state, value=sample(60, bad=False), firing=False)
    messages, state = tick(90, state, value=sample(90, bad=False, boot="boot-2"), firing=False, boot="boot-2")
    assert messages == []
    messages, state = tick(150, state, value=sample(150, bad=False, boot="boot-2"), firing=False, boot="boot-2")
    assert len(messages) == 1 and state == {}


def test_alternating_unknowns_warn_after_five_minutes_without_media_failure_or_recovery():
    state = {}
    all_messages = []
    for t in range(0, 361, 30):
        value = timing.Qualified("UNVERIFIABLE", "clock" if t % 60 else "missing"), "UNKNOWN"
        messages, state = tick(t, state, value=value, firing=t % 60 == 0)
        all_messages.extend(messages)
    assert len(all_messages) == 1
    assert "Delivery failure or recovery is unconfirmed" in all_messages[0]["content"]
    assert state[key(alert())]["observation_pending"] is True


def test_pending_observation_warning_gets_its_own_resolution_without_orphan_media_recovery():
    state = {}
    missing = timing.Qualified("UNVERIFIABLE", "missing"), "UNKNOWN"
    for t in (0, 300):
        _, state = tick(t, state, value=missing)
    _, state = tick(330, state, value=sample(330, bad=False), firing=False)
    messages, state = tick(390, state, value=sample(390, bad=False), firing=False)
    assert len(messages) == 1 and "Monitoring resumed" in messages[0]["content"]
    assert "Resolved:" not in messages[0]["content"] and state == {}


def test_file_reader_preserves_sample_identity_and_rejects_old_boot(tmp_path):
    payload = timing.attach({"action": "none", "execute": False}, "recovery_plan", now=reading())
    (tmp_path / "recovery_plan_observation.json").write_text(json.dumps(payload))
    first = evidence.read(tmp_path, {NAME}, now=reading(10))[NAME]
    second = evidence.read(tmp_path, {NAME}, now=reading(60))[NAME]
    assert first[0].valid and first[1] == "GOOD"
    assert first[0].sample == second[0].sample
    assert not evidence.read(tmp_path, {NAME}, now=reading(60, boot="boot-2"))[NAME][0].valid


def test_expiry_during_other_input_collection_does_not_create_a_fresh_failure(tmp_path):
    payload = timing.attach({"action": "reload_overlay", "execute": False}, "recovery_plan", now=reading())
    (tmp_path / "recovery_plan_observation.json").write_text(json.dumps(payload))
    values = evidence.read(tmp_path, {NAME}, now=reading(179))
    assert values[NAME][0].valid
    messages, state = plan([alert()], {}, WALL + 181, repeat_sec=3600,
                           observation_evidence=values, reading=reading(181))
    assert messages == [] and state[key(alert())]["observation_pending"] is True
