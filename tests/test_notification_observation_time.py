"""Boot, UTC-step and cache boundaries through notification state/outbox."""
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import re

import pytest

from stream_core.common import observation_time as timing
from stream_core.common import clock_contract as clock
from stream_core.common.adsb_timing import adsb_source_timing, adsb_freshness_signals
from stream_core.notifications import observation, renderer, state as notify_state, status_loop

WALL = 1_800_000_000.
MACHINE = "a" * 64


def test_k8s_unconfigured_local_audio_is_optional_but_registered_source_loss_is_not(tmp_path, monkeypatch):
    monkeypatch.setenv("STREAM_RUNTIME_SUPERVISOR", "k8s")
    state = {}
    for t in (0, 310):
        rows = observation.collect(tmp_path, [], {}, now=reading(t))
        result = observation.reconcile([], {}, state, rows.observations, now=reading(t), config={})
        assert "observation:audio_probe:unavailable" not in {x["id"] for x in result}
    source = tmp_path / "watchdog/pulse_health_state.json"
    source.parent.mkdir()
    source.write_text(json.dumps(timing.attach({"dj_missing_count": 0, "capture_missing_count": 0},
                                             "audio_probe", now=reading(320))))
    rows = observation.collect(tmp_path, [], {}, now=reading(320))
    observation.reconcile([], {}, state, rows.observations, now=reading(320), config={})
    source.unlink()
    for t in (330, 640):
        rows = observation.collect(tmp_path, [], {}, now=reading(t))
        result = observation.reconcile([], {}, state, rows.observations, now=reading(t), config={})
    assert "observation:audio_probe:unavailable" in {x["id"] for x in result}


def reading(t=0, *, step=0, boot="boot-1"):
    return timing.Reading(WALL + t + step, 1000. + t, "local", boot, MACHINE)


def metadata(source="youtube_watchdog", t=0, *, step=0, boot="boot-1"):
    return timing.stamp(source, now=reading(t, step=step, boot=boot))


def qualified(t, *, sample_t=None, step=0, boot="boot-1"):
    return timing.qualify(metadata(t=t if sample_t is None else sample_t, boot=boot),
                          now=reading(t, step=step, boot=boot))


def test_utc_step_does_not_age_a_local_sample_or_complete_a_timer():
    sample = qualified(30, sample_t=0, step=3600)
    assert sample.valid and sample.age_upper == 30 and sample.utc_status == "FAULT"
    state, verdict, _ = timing.advance({}, qualified(0), "BAD", now=reading(), bad_hold=300)
    state, verdict, _ = timing.advance(state, sample, "BAD", now=reading(30, step=3600), bad_hold=300)
    assert verdict == "UNKNOWN"
    assert state["count"] == 1  # same cached source sample


@pytest.mark.parametrize("field,value", [("observed_boottime", True), ("observed_boottime", float("nan")),
                                        ("observed_at_utc", "2100-01-01T00:00:00"), ("sample_id", "")])
def test_invalid_time_metadata_is_never_healthy(field, value):
    value_map = {**metadata(), field: value}
    assert not timing.qualify(value_map, now=reading()).valid


def test_expiry_and_receiver_or_producer_reboot_fail_closed():
    assert timing.qualify(metadata(), now=reading(181)).status == "STALE"
    assert timing.qualify(metadata(), now=reading(1, boot="boot-2")).status == "UNVERIFIABLE"
    assert timing.qualify(metadata(boot="boot-2"), now=reading(1)).status == "UNVERIFIABLE"


def test_duplicate_or_regressing_samples_never_vote_for_recovery():
    state, verdict, _ = timing.advance({}, qualified(0), "GOOD", now=reading())
    for t in (30, 60, 120):
        state, verdict, _ = timing.advance(state, qualified(t, sample_t=0), "GOOD", now=reading(t))
        assert verdict == "UNKNOWN" and state["count"] == 1
    state, verdict, _ = timing.advance(state, qualified(125), "GOOD", now=reading(125))
    assert verdict == "GOOD"
    regressed = timing.qualify(metadata(t=124), now=reading(126))
    state, verdict, _ = timing.advance(state, regressed, "GOOD", now=reading(126))
    assert verdict == "UNKNOWN" and state["count"] == 0


def test_unknown_between_good_samples_restarts_confirmation():
    state, _, _ = timing.advance({}, qualified(0), "GOOD", now=reading())
    missing = timing.Qualified("UNVERIFIABLE", "missing")
    state, verdict, _ = timing.advance(state, missing, "UNKNOWN", now=reading(30))
    state, verdict, _ = timing.advance(state, qualified(60), "GOOD", now=reading(60))
    assert verdict == "UNKNOWN"
    state, verdict, _ = timing.advance(state, qualified(105), "GOOD", now=reading(105))
    assert verdict == "GOOD"


def test_observation_loss_deadline_advances_through_all_unknown_reasons():
    state = {}
    for t, reason in [(0, "clock"), (60, "missing"), (120, "clock"), (240, "missing"), (300, "clock")]:
        state, verdict, lost = timing.advance(state, timing.Qualified("UNVERIFIABLE", reason), "UNKNOWN", now=reading(t))
        assert lost == (t >= 300)
        assert verdict == "UNKNOWN"


def test_real_clock_contract_converts_remote_utc_with_uncertainty():
    remote = metadata(t=0)
    remote.update(source_host="peer", machine_id_sha256="b" * 64, boot_id="peer-boot",
                  observed_at_utc=datetime.fromtimestamp(WALL + 10, timezone.utc).isoformat())
    model = clock.Estimate(9.98, 10.02, WALL, 1000, "peer-boot", "b" * 64, "boot-1", .04)
    sample = timing.qualify(remote, now=reading(1), host="peer", clock_model=model)
    assert sample.valid and sample.age_upper == pytest.approx(1.021)
    assert not timing.qualify(remote, now=reading(181), clock_model=model).valid
    assert not timing.qualify(remote, now=reading(1, boot="boot-2"), clock_model=model).valid
    assert not timing.qualify(remote, now=reading(1), host="wrong", clock_model=model).valid


def test_clock_mesh_model_is_bound_and_expires_without_refresh_on_read(tmp_path):
    fields = {"low": 9.98, "high": 10.02, "received_wall": WALL, "received_mono": 1000.,
              "boot_id": "remote-boot", "machine": "b" * 64, "local_boot": "boot-1", "network_rtt": .04}
    mesh = {"schema": "stream.clock_mesh.v1", "boot_id": "boot-1", "mono": 1000., "clock_synchronized": True,
            "peers": {"remote": {"status": "OBSERVED", "boot_id": "remote-boot", "estimate": fields, "synchronized": True}}}
    path = tmp_path / "mesh.json"
    path.write_text(json.dumps(mesh))
    model = timing.upstream_clock_model({"upstream_clock_host": "remote"}, now=reading(1), path=path)
    assert model is not None
    assert timing.upstream_clock_model({"upstream_clock_host": "remote"}, now=reading(91), path=path) is None
    assert timing.load_clock_model(mesh, "remote", now=reading(1, boot="boot-2")) is None
    fields["high"] = float("nan")
    assert timing.load_clock_model(mesh, "remote", now=reading(1)) is None


def test_delayed_old_samples_cannot_imitate_a_long_healthy_window():
    state, _, _ = timing.advance({}, qualified(0), "GOOD", now=reading())
    old_second = qualified(60, sample_t=1)
    state, verdict, _ = timing.advance(state, old_second, "GOOD", now=reading(60))
    assert verdict == "UNKNOWN" and state["count"] == 2


def test_corrupt_continuity_state_cannot_confirm_recovery():
    state = {"receiver_boot": "boot-1", "last_new": 1000., "source_identity": ["local", "boot-1", "youtube_watchdog"],
             "source_mono": "invalid", "since": float("nan"), "count": "bad", "verdict": "GOOD"}
    state, verdict, _ = timing.advance(state, qualified(60), "GOOD", now=reading(60))
    assert verdict == "UNKNOWN" and state["count"] == 1


def test_delayed_capture_does_not_prevent_publication_but_is_unverifiable():
    payload = timing.attach({"healthy": True}, "youtube_watchdog", now=replace(reading(), uncertainty=.1))
    assert payload["healthy"] is True
    assert not timing.qualify(payload["observation_time"], now=reading()).valid


def test_unbound_upstream_utc_is_not_treated_as_a_shared_clock():
    data = timing.attach({"source_now": WALL}, "adsb_overlay_probe", now=reading())
    assert timing.source_lag_bounds_at_collection(data, now=reading()) is None


def test_clock_uncertainty_straddling_the_limit_is_explicit():
    data = timing.attach({"source_now": WALL - 29.}, "adsb_overlay_probe", now=reading())
    model = clock.Estimate(-2., 2., WALL, 1000., "peer-boot", "b" * 64, "boot-1", .04)
    low, high = timing.source_lag_bounds_at_collection(data, now=reading(), model=model)
    assert low == 27. and high == 31.
    data["source_heartbeat_ok"] = None
    data["status"] = "unhealthy"
    result = adsb_source_timing(data, now=WALL, reading=reading(), source_clock_model=model)
    assert result["qualification"] == "upstream_age_uncertain"
    assert result["available"] is False
    signals = adsb_freshness_signals(data, datetime.fromtimestamp(WALL, timezone.utc),
                                     reading=reading(), source_clock_model=model)
    assert signals["fresh"] is True and signals["ok"] is False
    assert signals["stale"] is False and signals["heartbeat_ok"] is None


def test_report_order_and_map_duration_use_source_boottime_after_utc_reversal(tmp_path, monkeypatch):
    from stream_core.notifications import incidents
    monkeypatch.setattr(timing, "capture", lambda: reading(120))
    rows = []
    for t, step in [(0, 3600), (60, 0), (120, 0)]:
        item = {"ts_utc": datetime.fromtimestamp(WALL + t + step, timezone.utc).isoformat(),
                "checked_at_utc": datetime.fromtimestamp(WALL + t + step, timezone.utc).isoformat(),
                "target": "overlay", "delivery_critical_ok": False,
                "observation_time": timing.stamp("map_runtime", now=reading(t, step=step))}
        rows.append(item)
    path = tmp_path / "history.jsonl"
    path.write_text("\n".join(json.dumps(x) for x in rows) + "\n")
    latest, age = incidents.latest_jsonl_item(path, target="overlay", now_ts=int(WALL + 120))
    assert latest["observation_time"]["observed_boottime"] == 1120.
    assert age == 0
    _started, duration = incidents._trailing_problem_duration(path, now_ts=int(WALL + 120), predicate=lambda x: x["delivery_critical_ok"] is False)
    assert duration == 120


def test_real_nonce_exchange_does_not_compare_host_monotonic_epochs():
    actual_boot = clock.identity()["boot_id"]
    sent, received = {"wall": WALL, "mono": 1000.}, {"wall": WALL + .2, "mono": 1000.2}
    envelope = {"schema": "stream.clock_exchange.v1", "nonce": "c" * 32,
                "machine_id_sha256": "b" * 64, "boot_id": "remote-boot",
                "received": {"wall": WALL + 10.05, "mono": 90000.},
                "sent": {"wall": WALL + 10.1, "mono": 90000.05}}
    remote = metadata()
    remote.update(source_host="remote", machine_id_sha256="b" * 64, boot_id="remote-boot",
                  observed_boottime=89999.)
    current = replace(reading(.3), boot=actual_boot)
    sample = timing.qualify(remote, now=current, exchange=(clock, envelope, "c" * 32, sent, received))
    assert sample.valid and sample.age_upper == pytest.approx(1.35)
    wrong = timing.qualify(remote, now=current, exchange=(clock, envelope, "d" * 32, sent, received))
    assert not wrong.valid


def test_adsb_metadata_separates_collection_precision_and_clock_adjustment():
    data = {"sample_ts": WALL, "source_now": WALL + .1, "source_heartbeat_ok": True, "upstream_clock_local": True,
            "observation_time": timing.stamp("adsb_overlay_probe", now=replace(reading(), wall=WALL + .2))}
    result = adsb_source_timing(data, now=WALL + .3, reading=replace(reading(), wall=WALL + .3))
    assert result["available"] and result["heartbeat_fresh"]
    result = adsb_source_timing(data, now=WALL + 3600.3, reading=replace(reading(), wall=WALL + 3600.3))
    assert result["available"] and result["utc_status"] == "FAULT"


def test_bounded_diagnostics_keep_original_sample_time_and_do_not_renew_a_copy(tmp_path):
    from stream_core.notifications import time_diagnostics
    data = timing.attach({"sample_ts": WALL, "source_now": WALL}, "adsb_overlay_probe", now=reading())
    assert time_diagnostics.record(tmp_path, "adsb_overlay_probe", data, now=reading(10), max_bytes=1500)
    assert not time_diagnostics.record(tmp_path, "adsb_overlay_probe", data, now=reading(60), max_bytes=1500)
    for t in (20, 30, 40, 50):
        next_data = timing.attach({"sample_ts": WALL + t, "source_now": WALL + t}, "adsb_overlay_probe", now=reading(t))
        time_diagnostics.record(tmp_path, "adsb_overlay_probe", next_data, now=reading(t), max_bytes=1500)
    assert (tmp_path / "events.jsonl").stat().st_size <= 1500
    assert (tmp_path / "events.jsonl.1").stat().st_size <= 1500
    latest = json.loads((tmp_path / "latest.json").read_text())
    assert latest["source_now"] == WALL + 50


def test_diagnostic_symlink_or_fifo_cannot_block_a_notification(tmp_path):
    import os
    from stream_core.notifications import time_diagnostics
    (tmp_path / "latest.json").symlink_to(tmp_path / "elsewhere")
    data = timing.attach({}, "adsb_overlay_probe", now=reading())
    with pytest.raises(ValueError):
        time_diagnostics.record(tmp_path, "adsb_overlay_probe", data, now=reading())
    (tmp_path / "latest.json").unlink()
    os.mkfifo(tmp_path / "latest.json")
    with pytest.raises(ValueError):
        time_diagnostics.record(tmp_path, "adsb_overlay_probe", data, now=reading())


def test_oauth_cache_retains_its_original_observation_identity():
    from watchers.youtube_monitor import stats_writer
    written = []
    callbacks = dict(enrich_status_with_recovery_context=lambda x: x,
        classify_judgment=lambda *_args: ("ok", "healthy"), write_stats=written.append,
        append_event=lambda _x: None, should_emit_ok_event=lambda: False)
    payload = {"status": "ok", "healthy": True, "oauth_probe_performed": True,
               "oauth_checked_ts_utc": "2027-01-15T08:00:00Z"}
    stats_writer.record_status(payload, **callbacks)
    first = written[-1]
    stats_writer.record_status({**payload, "oauth_probe_performed": False}, previous_stats=first, **callbacks)
    cached = written[-1]
    assert cached["oauth_observation_time"] == first["oauth_observation_time"]
    assert cached["observation_time"]["sample_id"] != first["observation_time"]["sample_id"]
    stats_writer.record_status({**payload, "oauth_checked_ts_utc": "2027-01-15T08:01:00Z"}, previous_stats=cached, **callbacks)
    assert written[-1]["oauth_observation_time"]["sample_id"] != first["oauth_observation_time"]["sample_id"]


def assert_notification_message_consistent(phase, content):
    if phase != "recovered":
        return
    current_fields = {"current_observation", "current_status", "recovery_status"}
    unconfirmed = re.compile(
        r"\b(?:unknown|pending|unconfirmed|unverified)\b|not (?:yet )?(?:verified|confirmed)|\u5fa9\u65e7\u672a\u78ba\u8a8d",
        re.IGNORECASE,
    )
    for line in content.splitlines():
        field, separator, value = line.strip().partition("=")
        if separator and field.strip() in current_fields:
            assert not unconfirmed.search(value), "recovered notification contains an unconfirmed current state"


@pytest.fixture
def notifications(tmp_path, monkeypatch):
    values = {"items": [], "data": {}, "now": reading(), "rendered_phases": {}, "consistency_errors": []}
    sent = []
    state_file = tmp_path / "state.json"
    config = {"enabled": True, "webhook_url": "fixture", "username": "fixture", "slack_enabled": False,
              "repeat_sec": 900, "report_stale_sec": 1800, "outbox_ttl_sec": 86400,
              "outbox_max_pending": 50, "outbox_flush_limit": 10}

    def collection():
        sources = {}
        for name in {"youtube_watchdog"} | values["data"].keys():
            payload = values["data"].get(name, {})
            sample = timing.qualify(payload.get("observation_time"), now=values["now"], source=name, max_age=300.)
            sources[name] = (sample, payload, observation.complete(name, payload))
        return {"sources": sources, "bad_ids": {x["id"] for x in values["items"]},
                "observe": {"checks": {"current_fail": bool(values["items"])}}}

    def format_message(**kwargs):
        if kwargs["phase"] == "recovered":
            assert all(not item.get("_observation_pending") for item in kwargs["incidents"]), "recovered item retains pending observation"
        content = renderer.format_discord_message(**kwargs)
        values["rendered_phases"][content] = kwargs["phase"]
        return content

    def sender(_url, content, **_kwargs):
        try:
            assert_notification_message_consistent(values["rendered_phases"][content], content)
        except AssertionError as exc:
            values["consistency_errors"].append(str(exc))
            raise
        sent.append(content)
        return True, "http_204"

    ctx = status_loop.NotifyStatusContext(
        notify_events_file=tmp_path / "events.jsonl", notify_outbox_file=tmp_path / "outbox.jsonl",
        load_config=lambda: config, load_state=lambda: notify_state.load_notify_state(state_file),
        save_state=lambda x: notify_state.save_notify_state(x, state_file),
        collect_incidents=lambda **_kw: values["items"], observation_collection=collection,
        recovery_observation_for_incident=lambda _id, now: (now, "legacy absence"),
        format_message=format_message, send_webhook=sender,
        maintenance_notification_incident=lambda _now: None, fast_recovery_events_file=tmp_path / "fast.jsonl",
        stream_engine_events_file=tmp_path / "engine.jsonl", stream_watchdog_events_file=tmp_path / "watchdog.jsonl",
        runtime_state_base_dir=tmp_path)
    monkeypatch.setattr(timing, "capture", lambda: values["now"])

    def tick(t, bad=None, *, missing=False, step=0, repeat_sample=False, boot="boot-1"):
        values["now"] = reading(t, step=step, boot=boot)
        if bad is not None:
            values["items"] = [{"id": "stream:current_fail", "severity": "critical", "component": "stream_health",
                                "summary": "confirmed pipeline failure", "observed_ts": int(WALL + t),
                                "evidence": "fixture", "recovery_type": "observation", "follow_up": "check"}] if bad else []
        if missing:
            values["data"] = {}
        elif not repeat_sample:
            values["data"] = {"youtube_watchdog": {"observation_time": metadata(t=t, boot=boot),
                "status": "warn" if bad else "ok", "healthy": not bad, "local_ok": not bad, "ingest_connected": not bad}}
        result = status_loop.notify_status(ctx=ctx, now_ts=int(values["now"].wall))
        assert not values["consistency_errors"], "; ".join(values["consistency_errors"])
        return result

    return tick, sent, lambda: notify_state.load_notify_state(state_file), values


def test_native_detection_recovery_outbox_and_one_missing_sample(notifications):
    tick, sent, state, _ = notifications
    assert tick(0, True) == 0
    assert len(sent) == 1 and "Incident detected" in sent[0]
    tick(60, False, missing=True)
    assert len(sent) == 1 and "stream:current_fail" in state()["active"]
    assert state()["active"]["stream:current_fail"]["last_incident"]["_observation_pending"] is True
    tick(120, False)
    tick(150, False, repeat_sample=True)
    assert len(sent) == 1
    tick(180, False)
    assert len(sent) == 2 and "Recovery follow-up" in sent[1]
    assert "verified distinct normal observations" in sent[1]
    assert "current_observation=unknown" not in sent[1]
    assert "last_bad_evidence=fixture" in sent[1]
    assert state()["active"] == {}


def test_native_utc_fault_allows_verified_recovery_and_never_creates_stream_failure(notifications):
    tick, sent, state, _ = notifications
    tick(0, True)
    tick(60, False, step=3600)
    tick(120, False, step=-3600)
    assert len(sent) == 2
    assert state()["active"] == {}


def test_native_reboot_cannot_reuse_previous_confirmation(notifications):
    tick, sent, state, _ = notifications
    tick(0, True)
    tick(60, False)
    tick(90, False, boot="boot-2")
    assert "stream:current_fail" in state()["active"]
    tick(150, False, boot="boot-2")
    assert len(sent) == 2


def test_native_long_observation_loss_warns_instead_of_silent_recovery(notifications):
    tick, sent, state, _ = notifications
    tick(0, True)
    for t in range(60, 361, 60):
        tick(t, False, missing=True)
    assert "stream:current_fail" in state()["active"]
    assert "observation:youtube_watchdog:unavailable" in state()["active"]
    assert any("monitoring evidence" in message for message in sent[1:])
    assert not any("Recovery follow-up" in message for message in sent)

def test_valid_slo_measurement_with_active_burn_is_not_a_normal_observation(tmp_path):
    from stream_core.notifications.incidents import operational_reliability_incidents
    ident = "reliability:upload_ceiling_multi_window_burn"
    payload = timing.attach({"multi_window_burn_alerts": [{"family": "upload_ceiling", "severity": "warning"}],
        "fast_feedback": {}, "measurement_status_by_family": {"upload_ceiling": "valid"}},
        "reliability_burn", now=reading())
    path = tmp_path / "operational_reliability_burn_status.json"
    path.write_text(json.dumps(payload))
    # Exercise the actual owner, not just a preconstructed bad-ID list.
    original = timing.capture
    try:
        timing.capture = lambda: reading()
        items = operational_reliability_incidents(status_file=path, now_ts=int(WALL))
    finally:
        timing.capture = original
    rows = observation.collect(tmp_path, items, {}, now=reading())
    assert observation.evidence(rows.observations, ident, now=reading())[1] == "BAD"
    emitted = observation.reconcile(items, {}, {}, rows.observations, now=reading(), config={})
    assert ident in {item["id"] for item in emitted}
    normal = {**payload, "multi_window_burn_alerts": []}
    assert observation.normal_for(ident, "reliability_burn", normal, {}) is True


def test_formal_slo_incidents_keep_the_existing_owner_verdict():
    for suffix in ("coverage_or_freshness", "source_disagreement"):
        ident = "reliability:upload_ceiling_" + suffix
        assert observation.source_for(ident) is None
        item = {"id": ident, "severity": "warning"}
        assert observation.reconcile([item], {}, {}, {"sources": {}}, now=reading(), config={}) == [item]


def test_pending_observation_label_is_only_for_an_open_incident():
    item = {"id": "external:blackbox_failed", "component": "external_blackbox_evidence",
            "severity": "warning", "summary": "external checker failure", "evidence": "prior failure",
            "_observation_pending": True, "_recovery_evidence": "verified normal observations"}
    for phase in ("detected", "status"):
        message = renderer.format_discord_message(phase=phase, incidents=[item], state={"active": {}}, now_ts=int(WALL))
        assert "current_observation=unknown" in message
    message = renderer.format_discord_message(phase="recovered", incidents=[item], state={"active": {}}, now_ts=int(WALL))
    assert "current_observation=unknown" not in message
    assert "recovery_evidence=verified normal observations" in message
    assert "last_bad_evidence=prior failure" in message
    assert item["_observation_pending"] is True


@pytest.mark.parametrize("contradiction", [
    "current_observation=unknown (incident retained until recovery is verified)",
    "current_observation=\u5fa9\u65e7\u672a\u78ba\u8a8d",
    "recovery_status = PENDING",
])
def test_recovery_message_gate_rejects_contradiction_at_delivery(notifications, monkeypatch, contradiction):
    tick, sent, _, _ = notifications
    original = renderer.format_discord_message

    def contradictory_message(**kwargs):
        content = original(**kwargs)
        return content + "\n  " + contradiction if kwargs["phase"] == "recovered" else content

    monkeypatch.setattr(renderer, "format_discord_message", contradictory_message)
    tick(0, True)
    tick(60, False, missing=True)
    tick(120, False)
    with pytest.raises(AssertionError, match="recovered notification contains an unconfirmed current state"):
        tick(180, False)
    assert len(sent) == 1


def test_recovery_message_gate_allows_historical_failure_and_open_pending_state():
    assert_notification_message_consistent("recovered", "last_bad_evidence=current_observation=unknown\nrecovery_evidence=verified normal observations")
    assert_notification_message_consistent("status", "current_observation=unknown (recovery not yet verified)")
