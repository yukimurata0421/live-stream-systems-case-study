"""Root session → observation → bounded notification admission, without live effects."""
import copy
from dataclasses import replace
from datetime import datetime, timezone
import json
import hashlib
import os
from pathlib import Path

import pytest

from stream_core.common import manual_maintenance_facts as source, observation_time as timing
from stream_core.notifications import manual_maintenance as policy, measurement_continuity, status_loop
from test_notification_routing import notifications, incident

WALL = 1_800_000_000.


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def reading(t=0, step=0):
    return timing.Reading(WALL+t+step, 1000+t, 'monitor-host', 'monitor-boot', 'a'*64)


def session(boot='dell-boot'):
    return {'schema':'stream_v3.manual_maintenance.v1', 'phase':'MAINTENANCE',
        'session_id':'session-one', 'generation':1, 'deadline':WALL+600,
        'before':{'dell':{'identity':{'boot':boot,'machine':'dell-machine'},'main':{'uid':'main'}},
                  'oci':{'identity':{'boot':'oci-boot'}}},
        'armed_fields':{'alwaysAvailable':True,'alwaysAvailableFile':'/private/slate.mp4','alwaysAvailableTracks':[]},
        'private_credentials':'never-export',
        'effects':[
            {'name':'source_stop','role':'dell','action':'scale_main','result':'APPLIED',
             'at':WALL-10,'request':{'uid':'main','after':0}},
            {'name':'arm_slate','role':'oci','action':'path_patch','result':'APPLIED',
             'request':{'after':{'alwaysAvailable':True,'alwaysAvailableFile':'/private/slate.mp4','alwaysAvailableTracks':[]}}}]}


def bundle(t=0, *, phase='MAINTENANCE', deadline=WALL+600, step=0):
    value=session();value.update(phase=phase,deadline=deadline)
    facts=source.project_manual_maintenance(value,boot='dell-boot',machine_sha256='dell-machine',observed_epoch=WALL+t)
    facts['main_status']={'uid':'main','replicas':0,'active_pods':0}
    facts=timing.attach(facts,'manual_maintenance',now=reading(t,step))
    return {'manual_maintenance':facts,
        'observer_contract':{'runtime_attested':True,'compatible':True},
        'state':{'source_boot_id':'dell-boot','targets':{'oci_relay':{
            'capture_ok':True,'source_binding_ok':True,
            'captured_at_utc':utc(WALL+t-1)}}},
        'native_delivery':{'source_boot_id':'oci-boot','fresh':True,'source_age_seconds':1,
            'observed_at_utc':utc(WALL+t-1),
            'destinations':{x:{'state':'forwarding','flowing':True,'classification':'target_good'}
                            for x in ['youtube','twitch']}}}


def save(root, value):
    p=root/'oci_monitor/observation.json';p.parent.mkdir(exist_ok=True)
    p.write_text(json.dumps(value))


def test_root_reader_exports_only_bound_observation_fields(tmp_path):
    value=session(Path('/proc/sys/kernel/random/boot_id').read_text().strip())
    value['before']['dell']['identity']['machine']=hashlib.sha256(Path('/etc/machine-id').read_text().strip().encode()).hexdigest()
    value['deadline']=__import__('time').time()+600
    value['effects'][0]['at']=__import__('time').time()-10
    p=tmp_path/'session.json';p.write_text(json.dumps(value));p.chmod(0o644)
    result=source.read_manual_maintenance(tmp_path,uid=os.getuid())
    assert result['available'] and result['expected_main_stopped']
    assert 'never-export' not in json.dumps(result) and 'slate.mp4' not in json.dumps(result)
    p.chmod(0o666)
    assert not source.read_manual_maintenance(tmp_path,uid=os.getuid())['available']


@pytest.mark.parametrize('fault',['symlink','fifo','oversize','malformed'])
def test_root_reader_rejects_unsafe_or_unbounded_files(tmp_path,fault):
    path=tmp_path/'session.json'
    if fault=='symlink':path.symlink_to(tmp_path/'missing')
    if fault=='fifo':os.mkfifo(path)
    if fault=='oversize':path.write_bytes(b' '*65537)
    if fault=='malformed':path.write_text('{')
    assert source.read_manual_maintenance(tmp_path,uid=os.getuid())['available'] is False


@pytest.mark.parametrize('fault',['unknown','boot','machine','target','arm','deadline','generation'])
def test_unresolved_or_unbound_session_cannot_defer_a_warning(fault):
    value=session()
    if fault=='unknown':value['effects'][0]['result']='OUTCOME_UNKNOWN'
    if fault=='boot':value['before']['dell']['identity']['boot']=''
    if fault=='machine':value['before']['dell']['identity']['machine']='other-machine'
    if fault=='target':value['effects'][0]['request']['uid']='other-main'
    if fault=='arm':value['effects'][1]['request']['after']={}
    if fault=='deadline':value['deadline']=float('inf')
    if fault=='generation':value['generation']=True
    with pytest.raises((ValueError,KeyError)):
        source.project_manual_maintenance(value,boot='dell-boot',machine_sha256='dell-machine',observed_epoch=WALL)


def test_only_two_measurement_ids_are_eligible_and_state_survives_restart():
    state={};ids,alarms=policy.assess(bundle(),state,now=reading())
    assert ids==set(policy.DEFERRED_IDS) and not alarms
    restored=json.loads(json.dumps(state))
    assert policy.assess(bundle(10),restored,now=reading(10))[0]==ids
    assert policy.assess(bundle(20,phase='NORMAL'),restored,now=reading(20))==(set(),[])


@pytest.mark.parametrize('fault',['stale','source_boot','main_uid','main_running','terminating',
                                   'youtube_stopped','twitch_stopped','native_boot','unattested'])
def test_unknown_or_failed_outputs_do_not_enable_suppression(fault):
    value=bundle();state={};policy.assess(value,state,now=reading())
    if fault=='source_boot':value['state']['source_boot_id']='other-dell'
    if fault=='main_uid':value['manual_maintenance']['main_status']['uid']='other'
    if fault=='main_running':value['manual_maintenance']['main_status']['replicas']=1
    if fault=='terminating':value['manual_maintenance']['main_status']['active_pods']=1
    if fault.endswith('_stopped'):
        value['native_delivery']['destinations'][fault.split('_')[0]]['flowing']=False
    if fault=='native_boot':value['native_delivery']['source_boot_id']='new-oci'
    if fault=='unattested':value['observer_contract']['runtime_attested']=False
    ids,alarms=policy.assess(value,state,now=reading(36) if fault=='stale' else reading())
    assert not ids and alarms


def test_capture_unavailable_is_bounded_and_a_repeated_poll_cannot_extend_deadline():
    state={};value=bundle(deadline=WALL+100)
    value['state']['targets']['oci_relay']['capture_ok']=False
    assert policy.assess(value,state,now=reading())==(set(),[])
    value=bundle(91,deadline=WALL+100);value['state']['targets']['oci_relay']['capture_ok']=False
    assert policy.assess(value,state,now=reading(91))[1][0]['id'].endswith('slate_output_unavailable')
    value=bundle(99,deadline=WALL+100,step=-3600)
    policy.assess(value,state,now=reading(99,-3600))
    value=bundle(101,deadline=WALL+100,step=-3600)
    assert policy.assess(value,state,now=reading(101,-3600))[1][0]['id'].endswith('deadline_exceeded')


def test_same_generation_cannot_gain_a_new_deadline_or_session():
    for change in ['deadline','session']:
        state={};policy.assess(bundle(),state,now=reading())
        value=bundle(10)
        value['manual_maintenance']['deadline' if change=='deadline' else 'session_id']=WALL+700 if change=='deadline' else 'new-session'
        ids,alerts=policy.assess(value,state,now=reading(10))
        assert not ids and alerts


def test_planned_dell_reboot_keeps_the_same_session_and_deadline():
    state={};assert policy.assess(bundle(),state,now=reading())[0]
    old_identity=copy.deepcopy(state['manual_maintenance_notifications']['identity'])
    value=bundle(10)
    value['manual_maintenance']=source.project_manual_maintenance(session(),boot='new-dell-boot',
        machine_sha256='dell-machine',observed_epoch=WALL+10)
    value['manual_maintenance']['main_status']={'uid':'main','replicas':0,'active_pods':0}
    value['manual_maintenance']=timing.attach(value['manual_maintenance'],'manual_maintenance',now=reading(10))
    value['state']['source_boot_id']='new-dell-boot'
    assert policy.assess(value,state,now=reading(10))[0]==set(policy.DEFERRED_IDS)
    assert state['manual_maintenance_notifications']['identity']==old_identity
    value['manual_maintenance']['deadline']+=100
    assert not policy.assess(value,state,now=reading(10))[0]


def test_real_notification_loop_defers_expected_loss_and_keeps_independent_alarm(notifications,monkeypatch):
    n=notifications;root=n.ctx.runtime_state_base_dir
    monkeypatch.setattr(timing,'capture',lambda:reading())
    save(root,bundle())
    n.incidents=[incident(x,'warning',WALL) for x in policy.DEFERRED_IDS]
    assert n.tick(int(WALL))==0 and not any(n.sent.values())
    n.incidents.append(incident('external:slate_receiver_failed','critical',WALL))
    assert n.tick(int(WALL))==0
    assert n.sent['slack'] and 'external:slate_receiver_failed' in n.state()['active']
    value=bundle();value['native_delivery']['destinations']['youtube']['flowing']=False
    save(root,value);n.incidents=[];n.tick(int(WALL))
    assert 'manual_maintenance:slate_output_unavailable' in n.state()['active']


def test_existing_incident_is_not_cleared_by_maintenance(notifications,monkeypatch):
    n=notifications;root=n.ctx.runtime_state_base_dir
    monkeypatch.setattr(timing,'capture',lambda:reading())
    ident='reliability:upload_ceiling_coverage_or_freshness'
    n.incidents=[incident(ident,'warning',WALL)];n.tick(int(WALL))
    assert ident in n.state()['active']
    save(root,bundle());n.tick(int(WALL))
    assert ident in n.state()['active']
    assert ident not in n.state()['manual_maintenance_notifications']['deferred_ids']


def test_legacy_blanket_maintenance_cannot_hide_a_manual_deadline_alarm(notifications,monkeypatch):
    n=notifications
    monkeypatch.setattr(timing,'capture',lambda:reading(100))
    save(n.ctx.runtime_state_base_dir,bundle(100,deadline=WALL+10))
    context=replace(n.ctx,maintenance_notification_incident=lambda _:{'id':'maintenance:mode_active'})
    assert status_loop.notify_status(ctx=context,now_ts=int(WALL+100))==0
    assert 'manual_maintenance:deadline_exceeded' in n.state()['active']
    assert n.sent['slack']


def test_return_uses_original_recovery_rules_and_detects_new_loss(notifications,monkeypatch):
    from stream_core.notifications import upload_source
    n=notifications;root=n.ctx.runtime_state_base_dir
    (root/'upload_measurement/prometheus-source.json').unlink()
    condition={'fresh':False,'sustained':False}
    monkeypatch.setattr(upload_source,'current_upload_source',lambda *_:{'measurement':condition,'history':condition})
    ident='reliability:upload_ceiling_coverage_or_freshness'
    for t in range(0,301,60):
        monkeypatch.setattr(timing,'capture',lambda t=t:reading(t))
        save(root,bundle(t));n.incidents=[incident(ident,'warning',WALL+t)];n.tick(int(WALL+t))
    assert not n.state()['active']
    condition.update(fresh=True)
    for t in range(360,601,60):
        monkeypatch.setattr(timing,'capture',lambda t=t:reading(t))
        save(root,bundle(t,phase='NORMAL'));n.tick(int(WALL+t))
    assert not n.state()['active']  # incomplete 15-minute recovery window is not a new failure
    condition.update(fresh=False)
    for t in range(660,901,60):
        monkeypatch.setattr(timing,'capture',lambda t=t:reading(t))
        save(root,bundle(t,phase='NORMAL'));n.tick(int(WALL+t))
    assert ident in n.state()['active']


@pytest.mark.parametrize('ident,evidence,expected', [
    ('map:delivery_critical', 'active=2m28s reasons=deployment_ready,pod_topology_ready,runtime_readiness,render_heartbeat,browser_contract', True),
    ('stream:current_fail', 'youtube_status=ok youtube_judgment=ok youtube_stats_stale=True pulse_pass=True', True),
    ('stream:current_fail', 'youtube_status=warn youtube_judgment=ng youtube_stats_stale=False pulse_pass=True', False),
    ('stream:current_fail', 'youtube_status=ok youtube_judgment=ok youtube_stats_stale=True pulse_pass=False', False),
    ('map:delivery_critical', 'reasons=unexpected_encoder_failure', False),
    ('map:delivery_critical', 'missing_reasons', False),
    ('map:webgl_failure', 'reasons=browser_contract', False),
    ('reliability:audio_correctness_multi_window_burn', 'rule=fast_15m_1h', False),
])
def test_reported_maintenance_incidents_have_narrow_admission(tmp_path,ident,evidence,expected):
    save(tmp_path,bundle())
    item={**incident(ident,'critical',WALL),'evidence':evidence}
    rows,ids=policy.apply([item],{}, {},root=tmp_path,now=reading())
    assert (ident in ids)==expected
    assert (not any(r['id']==ident for r in rows))==expected
    if not expected and ident.startswith('reliability:'):
        assert rows[0]['evidence']==evidence and rows[0]['severity']=='critical'
        assert 'Planned maintenance' in rows[0]['diagnostic_context']


def test_existing_map_incident_and_failed_slate_are_not_muted(tmp_path):
    value=bundle();save(tmp_path,value)
    item={**incident('map:delivery_critical','critical',WALL),'evidence':'reasons=deployment_ready'}
    assert policy.apply([item],{item['id']:{}},{},root=tmp_path,now=reading())[0][0]['id']==item['id']
    value['native_delivery']['destinations']['youtube']['flowing']=False;save(tmp_path,value)
    rows,ids=policy.apply([item],{}, {},root=tmp_path,now=reading())
    assert item['id'] not in ids
    assert {r['id'] for r in rows}=={item['id'],'manual_maintenance:slate_output_unavailable'}


def test_resume_history_is_context_only_and_expires(tmp_path):
    v=session();v['phase']='VERIFYING'
    v['effects'].append({'name':'source_resume','role':'dell','action':'scale_main',
        'result':'APPLIED','at':WALL-1,'request':{'uid':'main','after':1}})
    f=source.project_manual_maintenance(v,boot='dell-boot',machine_sha256='dell-machine',observed_epoch=WALL)
    assert f['source_stopped_at']==WALL-10 and f['source_resumed_at']==WALL-1
    value=bundle();value['manual_maintenance']=timing.attach(f,'manual_maintenance',now=reading())
    save(tmp_path,value);state={}
    item={**incident('reliability:audio_correctness_multi_window_burn','critical',WALL),'evidence':'burn_rates unchanged'}
    rows,ids=policy.apply([item],{},state,root=tmp_path,now=reading())
    assert not ids and rows[0]['evidence']==item['evidence']
    assert 'Planned main stop history=' in rows[0]['diagnostic_context']
    save(tmp_path,bundle(10,phase='NORMAL'))
    rows,ids=policy.apply([item],{},state,root=tmp_path,now=reading(10))
    assert not ids and 'Planned main stop history=' in rows[0]['diagnostic_context']
    rows,_=policy.apply([item],{},state,root=tmp_path,now=reading(21601))
    assert 'diagnostic_context' not in rows[0]


def test_actual_status_loop_keeps_burn_and_suppresses_planned_map_stop(notifications,monkeypatch):
    n=notifications;save(n.ctx.runtime_state_base_dir,bundle())
    monkeypatch.setattr(timing,'capture',lambda:reading())
    n.incidents=[{**incident('map:delivery_critical','critical',WALL),'evidence':'reasons=deployment_ready'},
                 incident('reliability:audio_correctness_multi_window_burn','critical',WALL)]
    assert n.tick(int(WALL))==0
    assert set(n.state()['active'])=={'reliability:audio_correctness_multi_window_burn'}
    assert len(n.sent['slack'])==1 and 'Planned maintenance' in n.sent['slack'][0]


def test_reader_starting_after_normal_recovers_context_but_never_defers(tmp_path):
    v=session();v['phase']='NORMAL'
    v['effects'].append({'name':'source_resume','role':'dell','action':'scale_main',
        'result':'APPLIED','at':WALL-1,'request':{'uid':'main','after':1}})
    f=source.project_manual_maintenance(v,boot='dell-boot',machine_sha256='dell-machine',observed_epoch=WALL)
    assert f['phase']=='NORMAL' and not f['expected_main_stopped']
    assert f['source_resumed_at']==WALL-1
    value=bundle();value['manual_maintenance']=timing.attach(f,'manual_maintenance',now=reading())
    save(tmp_path,value)
    rows,ids=policy.apply([incident('reliability:audio_correctness_multi_window_burn','critical',WALL)],{}, {},root=tmp_path,now=reading())
    assert not ids and 'Planned main stop history=' in rows[0]['diagnostic_context']
    v['effects'][-1]['request']['uid']='wrong'
    f=source.project_manual_maintenance(v,boot='dell-boot',machine_sha256='dell-machine',observed_epoch=WALL)
    assert f['available'] and 'source_resumed_at' not in f


def test_receiver_clock_is_captured_after_atomic_publication_read(tmp_path,monkeypatch):
    def read_after_new_publication(root):
        monkeypatch.setattr(timing,'capture',lambda:reading(1))
        return bundle(1)
    monkeypatch.setattr(timing,'capture',lambda:reading())
    monkeypatch.setattr(policy,'_read_bundle',read_after_new_publication)
    rows,ids=policy.apply([],{}, {},root=tmp_path)
    assert not rows and ids==set(policy.DEFERRED_IDS)
