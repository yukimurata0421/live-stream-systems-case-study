"""Planned history is excluded only when every remaining observation is normal."""
import copy
from dataclasses import replace
import importlib.util
import json
from pathlib import Path

import pytest

from stream_core.common import observation_time as timing
from stream_core.common.manual_maintenance_facts import notification_stops
from stream_core.operational_reliability import maintenance_admission as policy
from stream_core.notifications import incidents, observation, status_loop
from test_manual_maintenance_notifications import session, reading, WALL, utc
from test_notification_routing import notifications, incident


END=int(WALL)
START=END-1800
RESUME=END-900


def data(end=END):
    grid=list(range(end-21600,end+1,60))
    metrics={k:{} for k in ['audio_ok','audio_evidence_age_seconds','audio_evidence_available',
                           'youtube_input_quality_eligible','youtube_input_quality_good']}
    events=[]
    for t in grid:
        stopped=START<=t<RESUME
        for k in metrics:metrics[k][t]=0 if stopped and k in ('audio_ok','youtube_input_quality_eligible','youtube_input_quality_good') else 1
        if not stopped:
            events.append((t,{'oauth_probe_ok':True,'oauth_stream_status':'active',
                'oauth_stream_health_status':'good','oauth_stream_health_issues':0,
                'oauth_checked_ts_utc':utc(t),'local_ok':True,'ingest_connected':True}))
    plan={'schema':'stream.notification_maintenance_plan.v1','observed_epoch':end,
          'source_sample_id':'plan-sample','active_verified':False,
          'stops':[{'session_id':'one','generation':1,'source_stopped_at':START,'source_resumed_at':RESUME}]}
    return metrics,events,plan


def payload(end=END):
    m,e,p=data(end)
    a=policy.evaluate(m,e,p,end=end)
    raw={'formal_sli':False,'status':'ok','checked_at_utc':utc(end),
         'measurement_status_by_family':{'audio_correctness':'valid'},
         'fast_feedback':{'youtube_input_quality':{'measurement_status':'unknown',
            'measurement_unknown_reasons':['minimum_coverage_not_met'],'coverage_pct':75}},
         'multi_window_burn_alerts':[{'family':'audio_correctness','rule':'fast_15m_1h','severity':'critical','burn_rates':{'15m':20,'1h':20}}]}
    return policy.bind(raw,a)


def test_only_notification_view_changes_and_original_incidents_are_preserved(tmp_path):
    v=payload();raw=copy.deepcopy(v);raw.pop('notification_maintenance')
    assert set(v['notification_maintenance']['excluded_ids'])==set(policy.IDS.values())
    path=tmp_path/'operational_reliability_burn_status.json';path.write_text(json.dumps(v))
    assert incidents.operational_reliability_incidents(status_file=path,now_ts=END)==[]
    path.write_text(json.dumps(raw))
    assert {r['id'] for r in incidents.operational_reliability_incidents(status_file=path,now_ts=END)}==set(policy.IDS.values())
    assert raw=={k:value for k,value in v.items() if k!='notification_maintenance'}


@pytest.mark.parametrize('fault',['before','after','missing_age','unavailable','stale_age','current'])
def test_audio_outside_failure_or_unknown_is_never_hidden(fault):
    m,e,p=data();t=START-60 if fault=='before' else RESUME+600
    if fault in ('before','after','current'):m['audio_ok'][END if fault=='current' else t]=0
    if fault=='missing_age':m['audio_evidence_age_seconds'].pop(t)
    if fault=='unavailable':m['audio_evidence_available'][t]=0
    if fault=='stale_age':m['audio_evidence_age_seconds'][t]=999
    a=policy.evaluate(m,e,p,end=END)
    assert policy.IDS['audio_correctness'] not in a['excluded_ids']


@pytest.mark.parametrize('fault',['bad','gap','probe_failed','current_bad','current_disagreement','current_missing'])
def test_youtube_outside_failure_or_unknown_is_never_hidden(fault):
    m,e,p=data()
    if fault=='bad':
        row=next(r for t,r in e if t==RESUME+300);row['oauth_stream_health_status']='bad';row['oauth_stream_health_issues']=1
    if fault=='probe_failed':next(r for t,r in e if t==RESUME+300)['oauth_probe_ok']=False
    if fault=='gap':
        e=[(t,r) for t,r in e if not RESUME+60<=t<END-60]
        for t,r in e:
            if t==RESUME:r['oauth_checked_ts_utc']=utc(RESUME-599)
    if fault=='current_bad':e[-1][1]['oauth_stream_health_status']='bad';e[-1][1]['oauth_stream_health_issues']=1
    if fault=='current_disagreement':m['youtube_input_quality_good'][END]=0
    if fault=='current_missing':m['youtube_input_quality_good']={}
    assert policy.IDS['youtube_input_quality'] not in policy.evaluate(m,e,p,end=END)['excluded_ids']


def test_verified_open_stop_can_have_no_normal_short_window():
    m,e,p=data();p['active_verified']=True;p['stops'][0]['source_resumed_at']=None;p['observed_epoch']=END+1
    for t in m['audio_ok']:
        if t>=START:m['audio_ok'][t]=0
    a=policy.evaluate(m,e,p,end=END)
    assert a['families']['audio_correctness']['windows']['15m']['expected_points']==0
    assert a['families']['audio_correctness']['normal'] is True
    p['active_verified']=False
    with pytest.raises(ValueError):policy.evaluate(m,e,p,end=END)


@pytest.mark.parametrize('fault',['stale','tampered','formal','wrong_id','malformed'])
def test_missing_stale_or_other_assessment_cannot_clear_an_incident(fault):
    v=payload();ident=policy.IDS['audio_correctness'];now=END
    if fault=='stale':now+=601
    if fault=='tampered':v['multi_window_burn_alerts'][0]['severity']='warning'
    if fault=='formal':v['formal_sli']=True
    if fault=='wrong_id':ident='reliability:youtube_availability_multi_window_burn'
    if fault=='malformed':v['notification_maintenance']=None
    assert not policy.permits(v,ident,now=now)


def test_assessment_lasts_through_the_real_five_minute_timer_only():
    v=payload();ident=policy.IDS['audio_correctness']
    assert policy.permits(v,ident,now=END+350)
    assert not policy.permits(v,ident,now=END+601)
    v['status']='unknown'
    assert not policy.permits(v,ident,now=END+60)
    assert not policy.permits(payload(),'reliability:youtube_input_quality_fast_feedback',now=END)


def test_minute_boundary_uses_receiver_qualification_time(tmp_path,monkeypatch):
    from stream_core.notifications import manual_maintenance
    from test_manual_maintenance_notifications import bundle,save
    value=bundle(-10,phase='NORMAL')
    value['manual_maintenance']['notification_stops']=data()[2]['stops']
    save(tmp_path,value);monkeypatch.setattr(timing,'capture',lambda:reading())
    plan=manual_maintenance.notification_plan(tmp_path)
    assert plan['observed_epoch']==END-10 and plan['qualified_epoch']==END
    m,e,_=data();assert set(policy.evaluate(m,e,plan,end=END)['excluded_ids'])==set(policy.IDS.values())
    plan['qualified_epoch']=END+40
    with pytest.raises(ValueError):policy.evaluate(m,e,plan,end=END)


def test_startup_extension_requires_a_fresh_post_resume_capture():
    m,e,p=data();m['audio_ok'][RESUME]=0;m['audio_ok'][RESUME+60]=0
    m['audio_evidence_age_seconds'][RESUME+120]=1
    a=policy.evaluate(m,e,p,end=END)
    assert a['families']['audio_correctness']['excluded_intervals'][0][1]==RESUME+120
    for t in range(RESUME,RESUME+361,60):m['audio_ok'][t]=0
    assert policy.IDS['audio_correctness'] not in policy.evaluate(m,e,p,end=END)['excluded_ids']


def test_root_history_requires_armed_confirmed_applied_matching_session(tmp_path):
    v=session();v['phase']='NORMAL';v['slate_player_confirmation']={'kind':'operator_attestation','youtube':True,'twitch':True,'at':WALL-5}
    v['effects'].append({'name':'source_resume','role':'dell','action':'scale_main','result':'APPLIED',
        'at':WALL-1,'request':{'uid':'main','after':1}})
    args={'boot':'dell-boot','machine_sha256':'dell-machine','observed_epoch':WALL,'uid':__import__('os').getuid()}
    assert len(notification_stops(tmp_path,v,**args))==1
    history=tmp_path/'history';history.mkdir(mode=0o755);p=history/'one.json';p.write_text(json.dumps(v));p.chmod(0o644)
    assert len(notification_stops(tmp_path,{},**args))==1
    p.chmod(0o666);assert notification_stops(tmp_path,{},**args)==[]
    v['effects'][-1]['result']='OUTCOME_UNKNOWN';assert notification_stops(tmp_path,v,**args)==[]
    v['effects'][-1]['result']='APPLIED';v['phase']='UNKNOWN'
    assert notification_stops(tmp_path,v,**args)==[]


def test_reclassification_requires_distinct_good_observations(notifications,monkeypatch):
    n=notifications
    initial={'active':{ident:{'first_seen_ts':END-600,'first_notified_ts':END-500,
        'last_bad_ts':END-60,'last_incident':incident(ident,'warning',END-60)} for ident in policy.IDS.values()}}
    n.ctx.save_state(initial)
    current={'t':0}
    def collection():
        now=reading(current['t']);v=timing.attach(payload(END+current['t']),'reliability_burn',now=now)
        q=timing.qualify(v['observation_time'],now=now,source='reliability_burn',max_age=180)
        return {'sources':{'reliability_burn':(q,v,True)},'observe':{},'bad_ids':set()}
    ctx=replace(n.ctx,observation_collection=collection)
    for t in (0,60):
        current['t']=t;monkeypatch.setattr(timing,'capture',lambda:reading(current['t']))
        assert status_loop.notify_status(ctx=ctx,now_ts=END+t)==0
        if t==0:assert n.state()['active']
    assert n.state()['active']=={}
    assert any('planned_maintenance_reclassification' in text for text in n.sent['discord'])


def test_optional_counterfactual_keeps_default_report_unchanged(tmp_path):
    from stream_core.cli_support import sli_report as s
    ctx=s.SliReportContext(youtube_watchdog_events_file=tmp_path/'missing')
    args={'windows':'15m,1h,6h','end_time':str(END),'query_range':lambda *_:[]}
    normal=s.build_sli_report(ctx,**args)
    adjusted=s.build_sli_report(ctx,notification_plan=data()[2],**args)
    assert normal=={k:v for k,v in adjusted.items() if k!='notification_maintenance'}


@pytest.mark.parametrize('bootstrap', [False, True])
def test_quick_assessment_reclassifies_older_hourly_duplicate_during_bootstrap(tmp_path, bootstrap):
    quick=payload();hourly=copy.deepcopy(quick);hourly.pop('notification_maintenance')
    hourly['formal_sli']=True
    quick_path=tmp_path/'quick.json';quick_path.write_text(json.dumps(quick))
    hourly_path=tmp_path/'hourly.json';hourly_path.write_text(json.dumps(hourly))
    def collect():
        return incidents.collect_notification_incidents(
            observe_payload=lambda _: (0, {'pass':True,'checks':{}}, ''),
            stream1090_report_events_file=tmp_path/'missing-stream1090',
            upstream_report_events_file=tmp_path/'missing-upstream',
            operational_reliability_status_file=hourly_path,
            operational_reliability_burn_status_file=quick_path,
            now_ts=END,bootstrap_grace_active=bootstrap)
    assert not {r['id'] for r in collect()} & set(policy.IDS.values())
    quick['multi_window_burn_alerts'][0]['severity']='warning'
    quick_path.write_text(json.dumps(quick))
    assert policy.IDS['audio_correctness'] in {r['id'] for r in collect()}
    assert json.loads(hourly_path.read_text())==hourly


def test_bundled_quick_entrypoint_wires_assessment_and_preserves_raw_fields(tmp_path,monkeypatch):
    from types import SimpleNamespace
    spec=importlib.util.spec_from_file_location('maintenance_public_rollup',
        Path(__file__).resolve().parents[1]/'ops/scripts/stream_v3_operational_reliability_rollup.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    m,e,p=data();assessment=policy.evaluate(m,e,p,end=END)
    report={'windows':{},'notification_maintenance':assessment}
    received={}
    def build(ctx,**kwargs):
        received.update(kwargs)
        return report
    monkeypatch.setattr(module,'build_sli_report',build)
    monkeypatch.setattr(module.manual_maintenance,'notification_plan',lambda _:p)
    monkeypatch.setattr(module,'git_revision',lambda _:{'deployment_revision_valid':True,'head':'fixture'})
    monkeypatch.setattr(module,'multi_window_burn_alerts',lambda _:[{'family':'audio_correctness','rule':'fast_15m_1h'}])
    monkeypatch.setattr(timing,'capture',lambda:reading())
    args=SimpleNamespace(state_root=tmp_path,watchdog_events_file=None,database=None,
        now=END,quick=True,repo_root=tmp_path,prometheus_url='http://127.0.0.1:9090',output=tmp_path/'status.json')
    result=module.run(args)
    assert received['notification_plan']==p and received['end_time']==str(END)
    assert result['multi_window_burn_alerts']==[{'family':'audio_correctness','rule':'fast_15m_1h'}]
    assert policy.permits(result,policy.IDS['audio_correctness'],now=END)
    assert json.loads(args.output.read_text())==json.loads(json.dumps(result))
    assert not (tmp_path/'operational_reliability.sqlite3').exists()
