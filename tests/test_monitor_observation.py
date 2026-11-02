from stream_core.common import monitor_observation as adapter, observation_time as timing
from stream_core.common.observation_contract import TargetState,Validity


def reading(t=0,wall=1800000000,boot='boot'):
    return timing.Reading(wall,1000+t,'local',boot,'a'*64)


def test_fresh_snapshot_is_not_overall_delivery_health_and_utc_step_does_not_age_it():
    raw=timing.attach({'healthy':True},'youtube_watchdog',now=reading())
    fact=adapter.sample('youtube_watchdog',raw,now=reading(30,wall=1900000000),max_age=300)
    assert fact.validity==Validity.VALID and fact.target_state==TargetState.UNKNOWN
    assert fact.age_upper_seconds==30


def test_reboot_expiry_and_missing_metadata_never_become_healthy():
    raw=timing.attach({},'youtube_watchdog',now=reading())
    for value,now in [(raw,reading(301)),(raw,reading(1,boot='next')),({},reading())]:
        fact=adapter.sample('youtube_watchdog',value,now=now,max_age=300)
        assert fact.validity!=Validity.VALID and fact.target_state==TargetState.UNKNOWN


def test_clock_sync_is_a_separate_scope_and_stale_failure_is_unknown():
    raw={'boot_id':'boot','mono':1000,'clock_synchronized':False}
    fact=adapter.mesh(raw,now=reading(5))
    assert fact.target_state==TargetState.FAIL and fact.scope=='host_clock_synchronization'
    assert adapter.mesh(raw,now=reading(91)).target_state==TargetState.UNKNOWN


def test_all_missing_sources_emit_bounded_unknowns(tmp_path):
    rows=adapter.read(tmp_path,tmp_path/'missing-mesh',now=reading())
    assert len(rows)==len(adapter.SOURCES)+1
    assert all(x.validity==Validity.UNAVAILABLE and x.target_state==TargetState.UNKNOWN for x in rows)


def test_actual_isolated_entry_serves_unknowns_without_source_files(tmp_path):
    import json,subprocess,sys,urllib.request,resource
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    def limit():
        resource.setrlimit(resource.RLIMIT_CPU,(8,8))
        resource.setrlimit(resource.RLIMIT_AS,(192*1024**2,192*1024**2))
    child=subprocess.Popen([sys.executable,'-I','-B',str(root/'ops/scripts/stream_v3_monitor_observation_exporter.py'),
        '--state-root',str(tmp_path),'--clock-mesh',str(tmp_path/'mesh'),'--source-host','fixture','--port','0'],
        stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,preexec_fn=limit)
    try:
        import selectors
        sel=selectors.DefaultSelector();sel.register(child.stdout,selectors.EVENT_READ)
        assert sel.select(timeout=5),'entry did not become ready'
        ready=json.loads(child.stdout.readline());sel.close()
        with urllib.request.urlopen('http://127.0.0.1:'+str(ready['port'])+'/metrics',timeout=3)as response:raw=response.read().decode()
        assert 'source="adsb_overlay_probe"'in raw
        assert raw.count('validity="UNAVAILABLE"} 1')==len(adapter.SOURCES)+1
        assert child.poll()is None
    finally:
        child.terminate();child.communicate(timeout=5)
