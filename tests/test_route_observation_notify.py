"""Use the existing delivery loop, persisted state and outbox with fake senders."""
import dataclasses
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import sys

import pytest
from stream_core.notifications import route_observation as observation
from test_notification_routing import notifications


def module(path, name):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);sys.modules[name]=value;spec.loader.exec_module(value)
    return value


@pytest.fixture
def pipeline(notifications):
    n=notifications
    baseline=os.environ.get('STREAM_NOTIFY_BASELINE_DIR')
    if baseline:
        root=Path(baseline)
        loop=module(root/'status_loop.py','pinned_notify_loop')
        loop.notify_outbox=module(root/'outbox.py','pinned_notify_outbox')
        render=module(root/'renderer.py','pinned_notify_renderer')
        n.ctx=dataclasses.replace(n.ctx,format_message=render.format_discord_message)
    else:
        from stream_core.notifications import status_loop as loop
    n.view={'status':'VERIFIED','assessment':'CLOCK_FAULT','clock_observation_hold_sec':299}
    n.loop=loop
    n.tick=lambda now:loop.notify_status(ctx=observation.wrap_context(n.ctx,lambda:n.view),now_ts=now)
    return n


def test_threshold_dedup_unknown_and_positive_recovery(pipeline):
    n=pipeline
    assert n.tick(1000)==0 and not n.sent['slack']
    n.view['clock_observation_hold_sec']=300
    assert n.tick(1001)==0 and len(n.sent['slack'])==1
    assert observation.IDENT in n.state()['active']
    assert n.tick(1061)==0 and len(n.sent['slack'])==1
    n.view={'status':'UNKNOWN'}
    assert n.tick(1121)==0 and observation.IDENT in n.state()['active']
    # Recreate the wrapper on every tick, as a fresh monitor process would.
    n.view={'status':'VERIFIED','assessment':'HEALTHY','clock_observation_hold_sec':0}
    assert n.tick(1181)==0 and len(n.sent['slack'])==2
    assert not n.state()['active'] and not n.sent['discord']
    assert n.tick(1241)==0 and len(n.sent['slack'])==2


def test_failed_alert_and_recovery_reuse_existing_outbox_in_order(pipeline):
    n=pipeline;n.fail_slack=n.fail_discord=True;n.view['clock_observation_hold_sec']=300
    assert n.tick(1000)==1
    assert n.ctx.notify_outbox_file.stat().st_size>0
    n.view={'status':'VERIFIED','assessment':'HEALTHY','clock_observation_hold_sec':0}
    assert n.tick(1060)==1 and not n.sent['slack']
    n.fail_slack=n.fail_discord=False
    assert n.tick(1120)==0 and len(n.sent['slack'])==2
    assert 'Incident detected' in n.sent['slack'][0] and 'Recovery follow-up' in n.sent['slack'][1]
    assert n.ctx.notify_outbox_file.read_text()==''


def test_slack_disabled_uses_existing_fallback(pipeline):
    n=pipeline;n.config['slack_enabled']=False;n.view['clock_observation_hold_sec']=300
    assert n.tick(1000)==0
    assert len(n.sent['discord'])==1 and not n.sent['slack']


@pytest.mark.parametrize('held',[None,True,-1,float('nan'),float('inf'),299])
def test_unvalidated_duration_cannot_create_alert(held):
    assert observation.incident({'status':'VERIFIED','assessment':'CLOCK_FAULT',
                                'clock_observation_hold_sec':held},None,1000) is None


def test_pinned_reader_and_bad_extension_preserve_existing_incident(pipeline,tmp_path,monkeypatch):
    from stream_core.notifications import status_loop
    n=pipeline;n.view['clock_observation_hold_sec']=300;n.tick(1000)
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_MODULE',str(tmp_path/'missing.py'))
    ctx=status_loop.route_observation_context(n.ctx)
    assert any(x['id']==observation.IDENT for x in ctx.collect_incidents(now_ts=1060))
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_GUARD_SHA256','0'*64)
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_GUARD_MODULE',str(tmp_path/'absent.py'))
    with pytest.raises((ValueError,FileNotFoundError)):observation.pinned_reader()


def test_module_hash_failure_does_not_load_code(pipeline,tmp_path,monkeypatch):
    from stream_core.notifications import status_loop
    source=tmp_path/'extension.py';source.write_text('raise AssertionError("must not execute")')
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_MODULE',str(source))
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_SHA256','0'*64)
    assert status_loop.route_observation_context(pipeline.ctx).collect_incidents(now_ts=1000)==[]


def test_loaded_extension_uses_pinned_guard_freshness_and_existing_delivery(pipeline,tmp_path,monkeypatch):
    status_loop=pipeline.loop
    root=Path(__file__).resolve().parents[1]
    guard=root/'monitoring-v4/src/stream_monitoring_v4/compatibility/route_causality.py'
    if not guard.exists():guard=root.parent/'stream_v4/src/stream_monitoring_v4/compatibility/route_causality.py'
    helper=root/'src/stream_core/notifications/route_observation.py'
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_MODULE',str(helper))
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_SHA256',hashlib.sha256(helper.read_bytes()).hexdigest())
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_GUARD_MODULE',str(guard))
    monkeypatch.setenv('STREAM_ROUTE_OBSERVATION_GUARD_SHA256',hashlib.sha256(guard.read_bytes()).hexdigest())
    ctx=status_loop.route_observation_context(pipeline.ctx)
    reader=inspect.getclosurevars(ctx.collect_incidents).nonlocals['read_context']
    source=tmp_path/'fresh-projection.json';reader.__globals__['DEFAULT_PATH']=source
    clock=reader.__globals__['clock']
    value={'schema':'stream.route_causality.v1','boot_id':clock.identity()['boot_id'],
        'received_mono':clock.mono(),'status':'VERIFIED','source_age_upper_seconds':0,
        'cause_id':'a'*32,'cause_active':True,'fallback_active':False,'assessment':'CLOCK_FAULT',
        'reason':'clock_unavailable','secondary_recovery_blocked':True,'clock_observation_hold_sec':300}
    source.write_text(json.dumps(value));monkeypatch.delenv('STREAM_ROUTE_OBSERVATION_MODULE')
    assert status_loop.notify_status(ctx=ctx,now_ts=1000)==0
    assert len(pipeline.sent['slack'])==1
    value['received_mono']-=60;source.write_text(json.dumps(value))
    assert status_loop.notify_status(ctx=ctx,now_ts=1060)==0
    assert observation.IDENT in pipeline.state()['active']


def test_clock_hold_alert_survives_brief_nonclock_gap(pipeline):
    n=pipeline;n.view['clock_observation_hold_sec']=300
    assert n.tick(1000)==0 and len(n.sent['slack'])==1
    n.view={'status':'VERIFIED','assessment':'OBSERVATION_LOST','clock_observation_hold_sec':315}
    assert n.tick(1015)==0 and observation.IDENT in n.state()['active']
    n.view={'status':'VERIFIED','assessment':'CLOCK_FAULT','clock_observation_hold_sec':330}
    assert n.tick(1030)==0 and len(n.sent['slack'])==1
    n.view={'status':'VERIFIED','assessment':'HEALTHY','clock_observation_hold_sec':0}
    assert n.tick(1090)==0 and len(n.sent['slack'])==2

@pytest.mark.parametrize('duration',[True,None,-1,float('nan'),float('inf')])
def test_bad_clock_diagnostic_duration_cannot_alert(duration):
    assert observation.incident({'status':'VERIFIED','assessment':'HEALTHY',
        'clock_diagnostic':{'status':'FAULT','fault_for_sec':duration}},None,1000) is None

def test_utc_diagnostic_alert_is_independent_of_healthy_media(pipeline):
    n=pipeline
    n.view={'status':'VERIFIED','assessment':'HEALTHY','clock_diagnostic':{'status':'FAULT','fault_for_sec':300}}
    assert n.tick(1000)==0 and len(n.sent['slack'])==1
    n.view={'status':'VERIFIED','assessment':'HEALTHY','clock_diagnostic':{'status':'UNAVAILABLE','fault_for_sec':315}}
    assert n.tick(1060)==0 and observation.IDENT in n.state()['active']
    n.view={'status':'VERIFIED','assessment':'MEDIA_FAILURE','clock_diagnostic':{'status':'VERIFIED','fault_for_sec':0}}
    assert n.tick(1120)==0 and len(n.sent['slack'])==2
    assert observation.IDENT not in n.state()['active']
