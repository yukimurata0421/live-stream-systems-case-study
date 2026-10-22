import importlib.util
import json
from pathlib import Path
import types
import copy
from datetime import datetime,timezone
import io

import pytest


def source_module():
    path=Path(__file__).parents[1]/'ops/scripts/stream_v3_twitch_thumbnail_source.py'
    spec=importlib.util.spec_from_file_location('thumbnail_source',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('value',[
    'http://static-cdn.jtvnw.net/previews-ttv/live_user_fixture-{width}x{height}.jpg',
    'https://static-cdn.jtvnw.net.evil.test/previews-ttv/live_user_fixture-{width}x{height}.jpg',
    'https://static-cdn.jtvnw.net/previews-ttv/live_user_other-{width}x{height}.jpg',
    'https://static-cdn.jtvnw.net/previews-ttv/live_user_fixture-{width}x{height}.jpg?token=private',
    'https://user@static-cdn.jtvnw.net/previews-ttv/live_user_fixture-{width}x{height}.jpg',None])
def test_url_must_be_the_api_template_for_this_channel(value):
    assert not source_module().thumbnail_template(value,'fixture')


def test_source_mirror_preserves_payload_requests_and_api_success_on_io_error(tmp_path,monkeypatch):
    module=source_module();writes=[];requests=[]
    url='https://static-cdn.jtvnw.net/previews-ttv/live_user_fixture-{width}x{height}.jpg'
    payload={'schema':'monitoring_v4.twitch_api_evidence.v1','user_login':'fixture','probe_status':'ok','provider_state':'LIVE','sequence':1}
    class Collector:
        def _request(self,url,*args,**kwargs):requests.append(url);return {'data':[{'thumbnail_url':url_template}]}
        def collect(self):self._request('https://api.twitch.tv/helix/streams?user_id=fixture');return dict(payload)
    url_template=url
    def write(path,text):writes.append((Path(path),json.loads(text)))
    base=types.SimpleNamespace(SCHEMA=payload['schema'],TwitchCollector=Collector,atomic_write=write,STREAMS_URL='https://api.twitch.tv/helix/streams')
    mirror=tmp_path/'thumbnail_source.json';monkeypatch.setattr(module,'ram_path',lambda _:True)
    module.attach(base,mirror);result=base.TwitchCollector().collect();base.atomic_write(tmp_path/'twitch_api_evidence.json',json.dumps(result))
    assert result==payload and len(requests)==1
    assert writes[0][1]==payload and writes[1][1]['thumbnail_url']==url
    monkeypatch.setattr(module,'ram_path',lambda _:False)
    base.atomic_write(tmp_path/'twitch_api_evidence.json',json.dumps(result))
    assert writes[-1][0].name=='twitch_api_evidence.json'


def thumbnail_body(color=128):
    from PIL import Image
    output=io.BytesIO();Image.new('RGB',(1920,1080),(color,)*3).save(output,'JPEG')
    return output.getvalue()


def reading(stamp):
    from stream_v3.viewer_clock import ClockReading
    values=iter([ClockReading(datetime.fromtimestamp(stamp+9*3600,timezone.utc).strftime('%H:%M:%S'),95),
                 ClockReading(datetime.fromtimestamp(stamp,timezone.utc).strftime('%H:%M:%S'),95)])
    return lambda _:next(values)


def target(now):
    from stream_v3.viewer_clock import utc_timestamp
    return {'stream_id':'stream','collected_at':utc_timestamp(now),'started_at':utc_timestamp(now-10000)}


def test_duplicate_5_minute_images_are_normal_and_not_new_samples():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    analyzer=ThumbnailAnalyzer({});now=1700000000
    first=analyzer.observe(thumbnail_body(),target(now),reading(now-100),now=now)
    assert first['ocr_verified'] and first['unique_samples_total']==1 and first['clock_state']=='CLOCK_OBSERVED'
    def no_ocr(_):raise AssertionError('duplicate must reuse verified reading')
    duplicate=analyzer.observe(None,target(now+60),no_ocr,now=now+60)
    assert duplicate['freshness_state']=='FRESH' and duplicate['unique_samples_total']==1 and not duplicate['unique_sample']
    warning=analyzer.observe(None,target(now+600),no_ocr,now=now+600)
    assert warning['freshness_state']=='STALE_SUSPECT' and warning['frame_age_seconds']==700
    correlated=analyzer.observe(None,target(now+900),no_ocr,now=now+900,forward_flowing=True)
    assert correlated['freshness_state']=='STALE_CORRELATED'


def test_clock_can_jump_5_minutes_between_adjacent_polls():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    analyzer=ThumbnailAnalyzer({});now=1700000000
    analyzer.observe(thumbnail_body(),target(now),reading(now-330),now=now)
    row=analyzer.observe(thumbnail_body(129),target(now+60),reading(now-30),now=now+60)
    assert row['clock_state']=='ADVANCING_CONFIRMED' and row['unique_samples_total']==2
    assert row['frame_age_seconds']==90


def test_uncertain_sample_and_restart_do_not_erase_last_verified_anchor():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    analyzer=ThumbnailAnalyzer({});now=1700000000
    analyzer.observe(thumbnail_body(),target(now),reading(now-100),now=now)
    bad=analyzer.observe(thumbnail_body(129),target(now+60),reading(now-160),now=now+60)
    assert not bad['ocr_verified'] and bad['reason']=='thumbnail_clock_regressed'
    restarted=ThumbnailAnalyzer({},json.loads(json.dumps(bad)))
    bad=restarted.observe(thumbnail_body(130),target(now+120),reading(now-160),now=now+120)
    assert not bad['ocr_verified'] and bad['unique_samples_total']==1


def test_utc_midnight_and_identical_image_after_24_hours():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    analyzer=ThumbnailAnalyzer({});now=datetime(2026,10,21,23,59,50,tzinfo=timezone.utc).timestamp()
    source=target(now)
    analyzer.observe(thumbnail_body(),source,reading(now-110),now=now)
    row=analyzer.observe(thumbnail_body(129),source,reading(now+70),now=now+180)
    assert row['clock_state']=='ADVANCING_CONFIRMED' and row['frame_age_seconds']==110
    row=analyzer.observe(None,source,lambda _:None,now=now+180+86400)
    assert row['frame_age_seconds']==86510 and row['unique_samples_total']==2


def test_future_clock_and_previous_stream_thumbnail_are_uncertain():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    from stream_v3.viewer_clock import utc_timestamp
    now=1700000000
    future=ThumbnailAnalyzer({}).observe(thumbnail_body(),target(now),reading(now+20),now=now)
    assert not future['ocr_verified']
    source={**target(now),'started_at':utc_timestamp(now-10)}
    old=ThumbnailAnalyzer({}).observe(thumbnail_body(),source,reading(now-100),now=now)
    assert not old['ocr_verified']


@pytest.mark.parametrize('mismatch',['sequence','source_boot_id','stream_id','user_login','collected_at'])
def test_source_must_match_current_api_evidence_and_host(mismatch):
    from stream_v3.twitch_thumbnail import validate_source
    from stream_v3.viewer_clock import utc_timestamp
    from stream_v3.viewer_clock_capture import ProbeError
    now=1700000000
    source={'schema':'stream_v3.twitch_thumbnail_source.v1','source_host_id':'arena-monitoring-facts','source_boot_id':'boot',
            'collector_instance':'instance','collector_revision':'revision','sequence':1,'user_id':'id','user_login':'fixture',
            'stream_id':'s','probe_status':'ok','provider_state':'LIVE','collected_at':utc_timestamp(now),
            'valid_until':utc_timestamp(now+180),'thumbnail_status':'available'}
    api={**source,'schema':'monitoring_v4.twitch_api_evidence.v1'}
    assert validate_source(source,api,{'boot_id':'boot','user_id':'id','user_login':'fixture'},now=now)
    source[mismatch]='changed'
    with pytest.raises(ProbeError):validate_source(source,api,{'boot_id':'boot','user_id':'id','user_login':'fixture'},now=now)


def test_old_publication_never_becomes_current_health_or_recovery():
    from stream_v3.twitch_thumbnail_metrics import write_metrics
    from stream_v3.viewer_clock import utc_timestamp
    values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=value
    row={'schema':'stream_v3.twitch_thumbnail_observation.v1','checked_at_utc':utc_timestamp(1000),'api_collected_at_utc':utc_timestamp(1000),'capture_ok':True,'ocr_verified':True,'api_live':True,
         'display_clock_at_utc':utc_timestamp(990),'last_valid_observation_at_utc':utc_timestamp(1000),'last_progress_at_utc':utc_timestamp(1000)}
    write_metrics(Writer(),row,now=1000,last_cycle_epoch=1000)
    assert values['stream_v3_twitch_thumbnail_progress_observed']==1
    write_metrics(Writer(),row,now=1400,last_cycle_epoch=1000)
    assert values['stream_v3_twitch_thumbnail_capture_ok']==0 and values['stream_v3_twitch_thumbnail_recovery_unixtime']==0
    assert values['stream_v3_twitch_thumbnail_seconds_since_last_cycle']==400


def test_thumbnail_redirects_and_provider_retry_after_are_respected(monkeypatch):
    import stream_v3.twitch_thumbnail as module
    from stream_v3.viewer_clock_capture import ProbeError
    from urllib.error import HTTPError
    from email.utils import formatdate
    handle=io.BytesIO()
    with pytest.raises(ProbeError):module.NoRedirect().redirect_request(None,handle,302,'',{},'http://127.0.0.1/private')
    assert handle.closed
    client=module.ThumbnailHTTP();monkeypatch.setattr(module.time,'time',lambda:1700000000)
    monkeypatch.setattr(module.time,'monotonic',lambda:100.)
    def limited(*_a,**_k):raise HTTPError('https://static-cdn.jtvnw.net/',429,'',{'Retry-After':formatdate(1700000300,usegmt=True)},io.BytesIO())
    client.opener=types.SimpleNamespace(open=limited)
    with pytest.raises(ProbeError,match='rate_limited'):client.get('https://static-cdn.jtvnw.net/',deadline=110.)
    assert client.retry_at==400
    with pytest.raises(ProbeError,match='retry_after'):client.get('https://static-cdn.jtvnw.net/',deadline=110.)


def test_missing_ram_mirror_is_visible_while_the_existing_api_reports_live(tmp_path,monkeypatch):
    import stream_v3.twitch_thumbnail_worker as module
    from stream_v3.viewer_clock import utc_timestamp
    now=1700000000;monkeypatch.setattr(module.time,'time',lambda:now)
    api={'schema':'monitoring_v4.twitch_api_evidence.v1','source_host_id':'arena-monitoring-facts','source_boot_id':'boot',
         'user_id':'id','user_login':'fixture','probe_status':'ok','provider_state':'LIVE','collected_at':utc_timestamp(now),'valid_until':utc_timestamp(now+180)}
    path=tmp_path/'api.json';path.write_text(json.dumps(api))
    config={'api_file':str(path),'source_file':str(tmp_path/'missing-ram-source.json'),'boot_id':'boot','user_id':'id','user_login':'fixture'}
    worker=module.Worker(config,tmp_path/'worker',http=types.SimpleNamespace(retry_at=0),ocr=object())
    row=worker.once()
    assert row['api_live'] is True and row['capture_ok'] is False and row['clock_state']=='UNKNOWN'
    assert row['api_collected_at_utc']==utc_timestamp(now)
