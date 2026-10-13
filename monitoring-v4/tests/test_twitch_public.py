import json
from stream_monitoring_v4.compatibility.twitch_public import project as raw_project,REVISION
BINDING={"user_id":"100000001","user_login":"tokyoflighttracker","collector_revision":REVISION}
def project(raw,reception,now): return raw_project(raw,reception,now,BINDING)

NOW=1791853000.0
def facts(**kwargs):
    from datetime import datetime,timezone
    stamp=lambda t:datetime.fromtimestamp(t,timezone.utc).isoformat()
    value={'schema':'monitoring_v4.twitch_api_evidence.v1','source_host_id':'arena-monitoring-facts',
           'collector_revision':REVISION,'user_id':'100000001','user_login':'tokyoflighttracker',
           'collected_at':stamp(NOW-10),'valid_until':stamp(NOW+170),'freshness_ttl_sec':180,
           'probe_status':'ok','http_status':200,'error_reason':'','provider_state':'LIVE',
           'stream_id':'12345','started_at':stamp(NOW-100),'viewer_count':1,
           'secret':'private-secret','private_address':'192.0.2.99'}
    value.update(kwargs);return value

def test_public_allowlist_removes_private_material():
    result=project(facts(),{},NOW)
    assert result['observation']['provider_state']=='LIVE'
    assert result['session']['id']=='12345'
    assert 'private-secret' not in json.dumps(result) and '192.168.' not in json.dumps(result)
    assert 'collector_revision' not in json.dumps(result)

def test_stale_live_is_unknown_and_has_no_session():
    result=project(facts(),{},NOW+200)
    assert result['observation']['provider_state']=='UNKNOWN'
    assert result['observation']['status']=='STALE' and result['session']['id'] is None

def test_unconfigured_is_not_offline():
    result=project(facts(probe_status='unknown',http_status=0,error_reason='credentials_missing',provider_state='UNKNOWN'),{},NOW)
    assert result['observation']['status']=='NOT_CONFIGURED'
    assert result['observation']['provider_state']=='UNKNOWN'

def test_identity_drift_or_api_error_is_unknown():
    for raw in [facts(user_id='999'),facts(collector_revision='wrong'),facts(http_status=401)]:
        result=project(raw,{},NOW)
        assert result['observation']['provider_state']=='UNKNOWN' and result['session']['id'] is None

def test_reception_is_history_without_live_inference():
    receipt={'verified_at':'2026-10-13T00:53:00+00:00','video':'1080p H.264','audio':'48 kHz stereo AAC','non_silent':True,'raw_token':'private-secret'}
    result=project({},receipt,NOW)
    assert result['reception']['non_silent'] and result['observation']['provider_state']=='UNKNOWN'
    assert 'private-secret' not in json.dumps(result)
