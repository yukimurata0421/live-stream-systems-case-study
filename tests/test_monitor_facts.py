import copy
from stream_v3.monitor_facts import native_facts,classify
from stream_v3.viewer_clock import utc_timestamp


def raw(t,amount=10):
    return {'schema':'stream_v3.relay_native_facts.v1','source_host_id':'relay-host','source_boot_id':'boot','observed_at_utc':utc_timestamp(t),
            'source_monotonic_seconds':t,'relay_active':True,'input':{'available':True,'ready':True,'bytes':amount,'connection_id':'input'},
            'destinations':{p:{'state':'forwarding','bytes':amount,'id':p} for p in ('youtube','twitch')}}


def test_native_duplicates_resets_staleness_and_target_absence():
    first=native_facts(raw(100),{},now=100)
    assert first['destinations']['youtube']['classification']=='observation_unknown'
    second=native_facts(raw(160,20),first,now=160)
    assert second['destinations']['youtube']['flowing'] is True
    assert native_facts(raw(160,20),second,now=170)['destinations']['youtube']['flowing'] is True
    assert native_facts(raw(160,20),second,now=251)['destinations']['youtube']['classification']=='observation_unknown'
    reset=raw(220,1);reset['destinations']['youtube']['id']='new'
    assert native_facts(reset,second,now=220)['destinations']['youtube']['flowing'] is None
    unknown={'checked_at_utc':utc_timestamp(220),'capture_ok':False,'clock_state':'UNKNOWN','reason':'capture_tls_failed'}
    assert classify(unknown,second,now=220,target='oci_relay')['classification']=='observation_unknown'
    absent=copy.deepcopy(second);absent['input']['ready']=False
    assert classify(unknown,absent,now=220,target='oci_relay')['classification']=='target_bad'
