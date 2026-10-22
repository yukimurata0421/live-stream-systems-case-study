import copy
import importlib.util
import json
from pathlib import Path
import types

import pytest
from stream_v3.viewer_clock import utc_timestamp
from stream_v3.oci_monitor_metrics import write_metrics


def load_script(name):
    path=Path(__file__).parents[1]/'ops/scripts'/name
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_unknown_warning_and_recovery_need_distinct_fresh_observations():
    module=load_script('stream_v3_clock_alert_notify.py')
    alert={'state':'firing','labels':{'alertname':'ViewerClockObservationUnavailable','target':'oci_relay','severity':'warning'}}
    messages,state=module.plan_clock([alert],{},1000,repeat_sec=3600,recovery={})
    assert len(messages)==1 and messages[0]['channel']=='discord'
    assert 'Public-side' in messages[0]['content']
    assert not module.plan_clock([alert],state,1010,repeat_sec=3600,recovery={})[0]
    messages,unobserved=module.plan_clock([],state,1020,repeat_sec=3600,recovery={})
    assert not messages and unobserved==state
    _,candidate=module.plan_clock([],state,1030,repeat_sec=3600,recovery={'oci_relay':1028})
    assert not module.plan_clock([],candidate,1040,repeat_sec=3600,recovery={'oci_relay':1028})[0]
    messages,resolved=module.plan_clock([],candidate,1050,repeat_sec=3600,recovery={'oci_relay':1048})
    assert len(messages)==1 and not resolved and 'not delivery recovery' in messages[0]['content']
    second=copy.deepcopy(alert);second['labels']['target']='youtube_fallback'
    messages,state=module.plan_clock([alert,second],{},1000,repeat_sec=3600,recovery={})
    assert len(messages)==len(state)==2


def test_stale_or_duplicate_prometheus_series_do_not_resolve(monkeypatch):
    module=load_script('stream_v3_clock_alert_notify.py')
    value={'status':'success','data':{'resultType':'vector','result':[
        {'metric':{'target':'oci_relay'},'value':[1000,'990']},
        {'metric':{'target':'youtube_fallback'},'value':[1000,'995']}]}}
    class Response:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self,*args):return json.dumps(value).encode()
    urls=[]
    def fetch(url,**kwargs):urls.append(url);return Response()
    monkeypatch.setattr(module.urllib.request,'urlopen',fetch)
    assert module.fetch_recovery(1000)['observer']==990
    assert urls[-1].endswith('&time=1000')
    value['data']['result'][0]['value'][0]=1000.001
    assert module.fetch_recovery(1000)['observer']==990
    value['data']['result'][0]['value'][0]=1002
    assert 'oci_relay' not in module.fetch_recovery(1000)
    value['data']['result'][0]['value'][0]=1000
    assert module.fetch_recovery(1100)=={}
    value['data']['result'].append(copy.deepcopy(value['data']['result'][0]))
    assert module.fetch_recovery(1000)=={}


def test_thumbnail_recovery_does_not_change_relay_recovery_proof(monkeypatch):
    module=load_script('stream_v3_clock_alert_notify.py')
    value={'status':'success','data':{'resultType':'vector','result':[
        {'metric':{'target':t},'value':[1000,str(stamp)]} for t,stamp in [('oci_relay',990),('youtube_fallback',995),('twitch_thumbnail',900)]]}}
    class Response:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self,*args):return json.dumps(value).encode()
    monkeypatch.setattr(module.urllib.request,'urlopen',lambda *_a,**_k:Response())
    proof=module.fetch_recovery(1000)
    assert proof['observer']==990 and proof['twitch_thumbnail']==900
    alert={'state':'firing','labels':{'alertname':'TwitchThumbnailStale','target':'twitch_thumbnail'}}
    messages,state=module.plan_clock([alert],{},1000,repeat_sec=3600,recovery={})
    assert len(messages)==1 and 'ten minutes' in messages[0]['content'] and 'Viewer-side video playback is unverified' in messages[0]['content']


@pytest.mark.parametrize('change',['fresh','expiry','pod','boot','peer','not_ready'])
def test_runtime_attestation_cache_is_bounded_and_identity_sensitive(monkeypatch,capsys,change):
    module=load_script('stream_v3_oci_monitor_pull.py')
    body={'state':{'pod_uid':'pod','targets':{'youtube_fallback':{'transport_peer_ip':'peer'}}}}
    cache={'checked_epoch':1000.,'ssh_boot_id':'boot','pod_uid':'pod','fallback_peer_ip':'peer',
           'observer_runtime':{'available':True,'pods':[{'uid':'pod'}]},'fallback_status':{'available':True}}
    if change=='expiry':cache['checked_epoch']=939.
    if change=='pod':cache['pod_uid']='old'
    if change=='boot':cache['ssh_boot_id']='old'
    if change=='peer':cache['fallback_peer_ip']='old'
    if change=='not_ready':cache['observer_runtime']['pods']=[];cache['checked_epoch']=989.
    class Response:
        headers={'X-Stream-Observation-Signature':'fixture'}
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self,*args):return json.dumps(body).encode()
    monkeypatch.setattr(module.time,'time',lambda:1000.)
    import urllib.request
    monkeypatch.setattr(urllib.request,'build_opener',lambda *_:types.SimpleNamespace(open=lambda *_a,**_k:Response()))
    original=Path.read_text
    monkeypatch.setattr(Path,'read_text',lambda p,*a,**k:'boot' if str(p)=='/proc/sys/kernel/random/boot_id' else original(p,*a,**k))
    calls=[]
    import subprocess
    def query(*a,**k):calls.append(1);raise subprocess.TimeoutExpired('fixed read',2)
    monkeypatch.setattr(subprocess,'run',query)
    remote=module.REMOTE_READ.replace('__AFTER_CYCLE__','0').replace('__POD_UID__',"'pod'").replace('__RUNTIME_CACHE__',repr(cache))
    exec(remote,{})
    result=json.loads(capsys.readouterr().out)
    assert result['runtime_cache_reused']==(change=='fresh')
    assert len(calls)==(0 if change=='fresh' else 1)
    if change!='fresh':assert result['observer_runtime']['available'] is False


def test_quality_metrics_keep_the_original_missing_observation_age():
    values={}
    class Writer:
        def metric(self,name,value,labels=None,**kwargs):values[(name,(labels or {}).get('target'))]=value
    observation={'schema':'stream_v3.oci_monitor_bundle.v1','state':{'targets':{}},'collector':{'observation_quality':{
        'oci_relay':{'required':True,'required_since_utc':utc_timestamp(900),'last_valid_at_utc':utc_timestamp(1000)}}}}
    write_metrics(Writer(),observation,now=1301)
    assert values['stream_v3_monitor_probe_seconds_since_last_valid_observation','oci_relay']==301
    assert values['stream_v3_monitor_probe_observation_required','oci_relay']==1
    assert values['stream_v3_monitor_probe_recovery_unixtime','oci_relay']==0


def test_transport_failure_keeps_last_admitted_observation_time(monkeypatch,tmp_path):
    module=load_script('stream_v3_oci_monitor_pull.py')
    state=tmp_path/'observation.json'
    old={'collector':{'last_cycle_identity':['boot','pod',10],'observation_quality':{
        'oci_relay':{'required':True,'required_since_utc':utc_timestamp(900),'last_valid_at_utc':utc_timestamp(1000)}}}}
    state.write_text(json.dumps(old))
    def fail(*_a,**_k):raise OSError('fixture transport unavailable')
    monkeypatch.setattr(module.ProcessRunner,'run',fail)
    assert not module.collect({'ssh_host':'fixture'},tmp_path/'ram',state)
    current=json.loads(state.read_text())
    assert current['collector']['observation_quality']['oci_relay']['last_valid_at_utc']==utc_timestamp(1000)
    assert current['collector']['last_cycle_identity']==['boot','pod',10]


def test_reader_session_change_preserves_the_clock_freeze_chain():
    from stream_v3.frame_receiver import ReceivedFrame
    from stream_v3.oci_monitor_probe import MonitorWorker
    from stream_v3.verified_clock import VerifiedClockAnalyzer,VerifiedClockConfig
    from stream_v3.viewer_clock import ClockReading
    ppm=b'P6\n200 100\n255\n'+b'\x80'*(200*100*3)
    worker=object.__new__(MonitorWorker);analyzer=VerifiedClockAnalyzer('oci_relay',VerifiedClockConfig((0,0,.5,1),(.5,0,.5,1)))
    for session,stamp in ((1,1000),(2,1070)):
        if session==2:analyzer.unknown('capture_stream_ended',now=1060)
        frame=ReceivedFrame(ppm,stamp,stamp,stamp,0,session)
        capture,_,meta=worker._decoded(frame,'stable-source',stamp)
        readings=iter([ClockReading('09:00:00',99),ClockReading('00:00:00',99)])
        result,_=analyzer.analyze(capture,lambda _:next(readings),now=stamp)
    assert meta['reader_session']==2 and capture.identity=='stable-source'
    assert result['clock_state']=='STALLED_SUSPECT' and result['clock_frozen_seconds']==70
    assert result['freeze_has_observation_gap'] and result['clock_frozen_observed_seconds']==0
