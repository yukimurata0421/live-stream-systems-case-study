"""Exercise notification routing, persisted pairing, and delivery ordering."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stream_core.notifications import renderer, state as notify_state, status_loop


@pytest.fixture
def notifications(tmp_path: Path):
    fixture = SimpleNamespace(incidents=[], sent={"slack": [], "discord": []}, fail_slack=False, fail_discord=False)
    config = {
        "enabled": True, "webhook_url": "https://discord.example/webhook", "username": "test",
        "slack_enabled": True, "slack_webhook_url": "https://slack.example/webhook",
        "slack_username": "test", "repeat_sec": 3600, "report_stale_sec": 1800,
        "outbox_ttl_sec": 86400, "outbox_max_pending": 50, "outbox_flush_limit": 10,
    }
    state_file = tmp_path / "notify-state.json"

    def send(route, content):
        if route == "slack" and fixture.fail_slack:
            return False, "http_503"
        if route == "discord" and fixture.fail_discord:
            return False, "http_503"
        fixture.sent[route].append(content)
        return True, "http_200"

    ctx = status_loop.NotifyStatusContext(
        notify_events_file=tmp_path / "events.jsonl", notify_outbox_file=tmp_path / "outbox.jsonl",
        load_config=lambda: config, load_state=lambda: notify_state.load_notify_state(state_file),
        save_state=lambda value: notify_state.save_notify_state(value, state_file),
        collect_incidents=lambda **_: fixture.incidents,
        recovery_observation_for_incident=lambda _ident, now: (now, "current evidence recovered"),
        format_message=renderer.format_discord_message,
        send_webhook=lambda _url, content, **_: send("discord", content),
        send_slack_webhook=lambda _url, content, **_: send("slack", content),
        maintenance_notification_incident=lambda _: None,
        fast_recovery_events_file=tmp_path / "fast.jsonl",
        stream_engine_events_file=tmp_path / "engine.jsonl",
        stream_watchdog_events_file=tmp_path / "watchdog.jsonl", runtime_state_base_dir=tmp_path,
    )
    fixture.tick = lambda now: status_loop.notify_status(ctx=ctx, now_ts=now)
    fixture.state = lambda: notify_state.load_notify_state(state_file)
    fixture.config, fixture.ctx = config, ctx
    return fixture


def incident(ident="stream:current_fail", severity="critical", observed=1000):
    return {"id": ident, "severity": severity, "component": "stream_health", "summary": "test incident",
            "observed_ts": observed, "evidence": "test evidence", "recovery_type": "observation", "follow_up": "check"}


def test_critical_detection_and_recovery_go_only_to_slack(notifications):
    n = notifications
    n.incidents = [incident()]
    assert n.tick(1000) == 0
    assert n.tick(5000) == 0
    n.incidents = []
    assert n.tick(5060) == 0
    assert n.tick(5120) == 0
    assert not n.sent["discord"]
    assert len(n.sent["slack"]) == 3
    assert "Incident detected" in n.sent["slack"][0]
    assert "Ongoing incident status" in n.sent["slack"][1]
    assert "Recovery follow-up" in n.sent["slack"][2]
    assert n.state()["active"] == {}


@pytest.mark.parametrize("ident", ["public_probe:429_or_bot_confirmation_repeated", "reliability:upload_ceiling_multi_window_burn", "reliability:upload_ceiling_coverage_or_freshness"])
def test_warnings_never_escalate_by_age_or_component(notifications, ident):
    n = notifications
    n.incidents = [incident(ident, "warning", observed=1)]
    assert n.tick(10000) == 0
    assert n.tick(20000) == 0
    n.incidents = []
    assert n.tick(20100) == 0
    assert not n.sent["slack"]
    assert len(n.sent["discord"]) == 3
    assert "Recovery" in n.sent["discord"][-1]


def test_critical_upgrade_is_immediate_and_downgrade_keeps_slack_followup(notifications):
    n = notifications
    n.incidents = [incident(severity="warning")]
    assert n.tick(1000) == 0
    n.incidents = [incident()]
    assert n.tick(1001) == 0  # No wait for the ordinary one-hour reminder interval.
    n.incidents = [incident(severity="warning")]
    assert n.tick(1002) == 0
    n.incidents = []
    assert n.tick(1003) == 0
    assert len(n.sent["slack"]) == 2
    assert "Incident detected" in n.sent["slack"][0]
    assert "Recovery follow-up" in n.sent["slack"][1]
    assert len(n.sent["discord"]) == 2  # Its original warning also receives a matching resolution.


def test_partial_recovery_reports_other_active_incidents(notifications):
    n = notifications
    n.incidents = [incident(), incident("report:warning", "warning")]
    assert n.tick(1000) == 0
    n.incidents = [incident("report:warning", "warning")]
    assert n.tick(1060) == 0
    assert "active_incidents=1" in n.sent["slack"][-1]
    assert "resolved_incidents=1" in n.sent["slack"][-1]


def test_recovery_waits_for_failed_detection_then_arrives_in_order(notifications):
    n = notifications
    n.fail_slack = True
    n.fail_discord = True
    n.incidents = [incident()]
    assert n.tick(1000) == 1
    n.incidents = []
    assert n.tick(1060) == 1
    assert n.sent["slack"] == []
    n.fail_slack = False
    n.fail_discord = False
    assert n.tick(1120) == 0
    assert len(n.sent["slack"]) == 2
    assert "Incident detected" in n.sent["slack"][0]
    assert "Recovery follow-up" in n.sent["slack"][1]
    assert not n.sent["discord"]
    assert n.ctx.notify_outbox_file.read_text() == ""


def test_disabled_slack_falls_back_to_discord(notifications):
    n = notifications
    n.config["slack_enabled"] = False
    n.incidents = [incident()]
    assert n.tick(1000) == 0
    n.incidents = []
    assert n.tick(1060) == 0
    assert not n.sent["slack"]
    assert len(n.sent["discord"]) == 2


def test_legacy_warning_receipt_does_not_send_slack_recovery(notifications):
    n = notifications
    item = incident(severity="warning")
    n.ctx.save_state({"active": {item["id"]: {"last_incident": item, "first_seen_ts": 1,
                      "slack_first_notified_ts": 100, "first_notified_ts": 100, "last_bad_ts": 100}}})
    assert n.tick(1000) == 0
    assert not n.sent["slack"]
    assert len(n.sent["discord"]) == 1


def test_restart_information_stays_on_discord(notifications):
    n = notifications
    item = incident("runtime:lifecycle:example", "info")
    rc = status_loop.deliver_notify_messages(ctx=n.ctx, messages=[("auto_recovered", [item])],
                                            state={"active": {"other": {}}}, cfg=n.config, now_ts=1000, dry_run=False)
    assert rc == (1, 0, 0)
    assert not n.sent["slack"]
    assert "active_incidents=1" in n.sent["discord"][0]


def test_slack_failure_falls_back_and_recovery_follows_actual_discord_delivery(notifications):
    n=notifications;n.fail_slack=True;n.incidents=[incident()]
    assert n.tick(1000)==0
    assert len(n.sent['discord'])==1
    assert 'Slack delivery failed' in n.sent['discord'][0]
    stored=n.state()['active']['stream:current_fail']
    assert stored['discord_first_notified_ts']==1000
    assert not stored.get('slack_first_notified_ts')
    assert n.tick(1060)==0
    assert len(n.sent['discord'])==1  # No fallback flood while Slack remains broken.
    n.incidents=[]
    assert n.tick(1120)==0
    assert 'Recovery follow-up' in n.sent['discord'][-1]
    assert not n.sent['slack']


def test_critical_reminder_is_sent_after_thirty_minutes(notifications):
    n=notifications;n.incidents=[incident()]
    assert n.tick(1000)==0
    assert n.tick(2799)==0
    assert len(n.sent['slack'])==1
    assert n.tick(2800)==0
    assert len(n.sent['slack'])==2


@pytest.mark.parametrize('ident',['youtube:enable_auto_stop_false_encoder_gap','external:blackbox_failed','fast_recovery:recovery_unconfirmed:tcp_stall:1000'])
def test_important_warning_reaches_slack_immediately(notifications,ident):
    n=notifications;n.incidents=[incident(ident,'warning')]
    assert n.tick(1000)==0
    assert len(n.sent['slack'])==1 and not n.sent['discord']


def test_long_warning_escalates_after_two_hours(notifications):
    n=notifications;n.incidents=[incident('report:warning','warning')]
    assert n.tick(1000)==0
    assert n.tick(8199)==0
    assert not n.sent['slack']
    assert n.tick(8200)==0
    assert len(n.sent['slack'])==1


def test_large_incident_set_is_split_without_acknowledging_hidden_items(notifications):
    n=notifications
    n.incidents=[{**incident(f'warning:{i}','warning'),'summary':f'visible incident {i} '+('x'*300)} for i in range(20)]
    assert n.tick(1000)==0
    assert len(n.sent['discord'])>2
    for i in range(20):assert any(f'visible incident {i} ' in body for body in n.sent['discord'])
    assert all(len(body)<=1900 for body in n.sent['discord'])
    assert all(row.get('discord_first_notified_ts') for row in n.state()['active'].values())


def test_pending_delivery_is_not_silently_removed_after_ttl(notifications):
    n=notifications;n.fail_discord=True;n.config['slack_enabled']=False
    n.incidents=[incident()]
    assert n.tick(1000)==1
    n.incidents=[]
    assert n.tick(100000)==1
    rows=[json.loads(line) for line in n.ctx.notify_outbox_file.read_text().splitlines()]
    assert any(row['phase']=='detected' and row['created_ts']==1000 for row in rows)


def test_capacity_overflow_has_durable_record(notifications):
    n=notifications;n.fail_discord=True;n.config['outbox_max_pending']=1
    n.incidents=[{**incident(f'warning:{i}','warning'),'summary':str(i)+'x'*700} for i in range(6)]
    assert n.tick(1000)==1
    dead=n.ctx.notify_outbox_file.with_name(n.ctx.notify_outbox_file.name+'.deadletter.jsonl')
    assert dead.exists() and dead.stat().st_size>0
    rows=[json.loads(line) for line in dead.read_text().splitlines()]
    assert all(row['disposition']=='pending_capacity_exceeded' for row in rows)


def test_short_quality_warning_is_debounced_and_recovery_is_not_spurious(notifications):
    n=notifications;n.incidents=[incident('reliability:youtube_input_quality_fast_feedback','warning')]
    for now in (1000,1060,1120,1180,1240,1300):assert n.tick(now)==0
    n.incidents=[];assert n.tick(1360)==0
    assert not n.sent['discord'] and not n.sent['slack']


def test_quality_warning_that_persists_ten_minutes_is_delivered(notifications):
    n=notifications;n.incidents=[incident('reliability:youtube_input_quality_fast_feedback','warning')]
    for now in range(1000,1601,60):assert n.tick(now)==0
    assert len(n.sent['discord'])==1 and not n.sent['slack']


def test_maintenance_keeps_event_scan_window_until_exit(notifications,monkeypatch):
    n=notifications;n.ctx.load_state()
    from dataclasses import replace
    maintenance={'id':'maintenance','_first_seen_ts':1000,'evidence':'planned'}
    n.ctx=replace(n.ctx,maintenance_notification_incident=lambda _:maintenance)
    assert status_loop.notify_status(ctx=n.ctx,now_ts=1000)==0
    assert status_loop.notify_status(ctx=n.ctx,now_ts=5000)==0
    captured=[]
    monkeypatch.setattr(status_loop,'fast_recovery_recovery_events',lambda **kw:(captured.append(kw['recent_sec']) or []))
    n.ctx=replace(n.ctx,maintenance_notification_incident=lambda _:None)
    assert status_loop.notify_status(ctx=n.ctx,now_ts=5060)==0
    assert captured==[4240]


def test_partial_observe_failure_is_not_hidden_and_health_exit_two_is_not_a_crash(tmp_path):
    from stream_core.notifications import incidents
    def collect(rc,payload,error=''):
        return incidents.collect_notification_incidents(observe_payload=lambda _:(rc,payload,error),
            stream1090_report_events_file=tmp_path/'overlay',upstream_report_events_file=tmp_path/'upstream',now_ts=1000)
    found=collect(1,{'checks':{'current_fail':False}},'partial stage failure')
    assert 'observe:execution_failed' in {x['id'] for x in found}
    found=collect(2,{'pass':False,'checks':{'current_fail':True}})
    ids={x['id'] for x in found}
    assert 'stream:current_fail' in ids and 'observe:execution_failed' not in ids


def test_missing_measurements_have_their_own_incidents_when_delivery_is_healthy(tmp_path):
    from stream_core.notifications import incidents
    payload={'pass':False,'api_report_judgment':'api_report_timer_attention','api_report_timers_active':False,
             'checks':{'current_fail':True,'youtube_stats_stale':True,'pulse_pass':True}}
    found=incidents.collect_notification_incidents(observe_payload=lambda _:(2,payload,''),
        stream1090_report_events_file=tmp_path/'overlay',upstream_report_events_file=tmp_path/'upstream',now_ts=1000)
    ids={x['id'] for x in found}
    assert {'youtube:watchdog_stats_stale','api_report:timer_monitoring_gap','stream1090:overlay_report','stream1090:upstream_report'}<=ids
    assert 'stream:current_fail' not in ids


def test_queued_recovery_follows_discord_fallback_even_if_slack_recovers_mid_flush(notifications):
    from dataclasses import replace
    n=notifications;n.fail_slack=True;n.fail_discord=True;n.incidents=[incident()]
    assert n.tick(1000)==1
    n.incidents=[];assert n.tick(1060)==1
    attempts=[]
    def intermittent(*a,**kw):attempts.append(1);return len(attempts)>1,'intermittent'
    n.fail_discord=False
    ctx=replace(n.ctx,send_slack_webhook=intermittent)
    assert status_loop.notify_status(ctx=ctx,now_ts=1120)==0
    assert len(attempts)==1
    assert len(n.sent['discord'])==2
    assert 'Incident detected' in n.sent['discord'][0] and 'Recovery follow-up' in n.sent['discord'][1]
