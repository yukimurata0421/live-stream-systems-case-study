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
    waiting=analyzer.observe(None,target(now+600),no_ocr,now=now+600)
    assert waiting['freshness_state']=='AGING' and waiting['frame_age_seconds']==700
    warning=analyzer.observe(None,target(now+780),no_ocr,now=now+780)
    assert warning['freshness_state']=='STALE_SUSPECT' and warning['frame_age_seconds']==880
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


def test_future_clock_is_uncertain_but_previous_stream_thumbnail_is_waiting():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    from stream_v3.viewer_clock import utc_timestamp
    now=1700000000
    future=ThumbnailAnalyzer({}).observe(thumbnail_body(),target(now),reading(now+20),now=now)
    assert not future['ocr_verified']
    source={**target(now),'started_at':utc_timestamp(now-10)}
    old=ThumbnailAnalyzer({}).observe(thumbnail_body(),source,reading(now-100),now=now)
    assert not old['ocr_verified'] and old['clock_state']=='WAITING_NEW_STREAM_THUMBNAIL'


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


def thumbnail_worker_fixture(tmp_path,monkeypatch):
    import stream_v3.twitch_thumbnail_worker as module
    from stream_v3.viewer_clock import utc_timestamp
    clock=[1700000000.]
    monkeypatch.setattr(module.time,'time',lambda:clock[0]);monkeypatch.setattr(module,'native_forward',lambda _,**kwargs:None)
    config={'api_file':str(tmp_path/'api.json'),'source_file':str(tmp_path/'source.json'),
            'boot_id':'boot','user_id':'id','user_login':'fixture'}
    def publish(stream='first',started=None):
        now=clock[0]
        api={'schema':'monitoring_v4.twitch_api_evidence.v1','source_host_id':'arena-monitoring-facts','source_boot_id':'boot',
             'collector_instance':'instance','collector_revision':'revision','sequence':int(now),'user_id':'id','user_login':'fixture',
             'stream_id':stream,'started_at':utc_timestamp(now-10000 if started is None else started),'probe_status':'ok',
             'provider_state':'LIVE','collected_at':utc_timestamp(now),'valid_until':utc_timestamp(now+180)}
        Path(config['api_file']).write_text(json.dumps(api))
        Path(config['source_file']).write_text(json.dumps({**api,'schema':'stream_v3.twitch_thumbnail_source.v1',
            'thumbnail_status':'available','thumbnail_url':'https://static-cdn.jtvnw.net/previews-ttv/live_user_fixture-{width}x{height}.jpg'}))
    class HTTP:
        etag=None;cache_url=None;retry_at=0
        body=thumbnail_body();calls=[]
        def get(self,url,**kwargs):
            self.calls.append((self.etag,self.cache_url));self.etag='fixture-etag';self.cache_url=url
            return self.body,{'http_status':304 if self.body is None else 200,'download_bytes':len(self.body or b'')}
    http=HTTP();http.calls=[]
    ocr=types.SimpleNamespace(read=None)
    worker=module.Worker(config,tmp_path/'worker',http=http,ocr=ocr)
    return module,worker,http,ocr,clock,publish


def test_native_http_client_reuses_handlers_without_tls_and_recovers_from_errors(monkeypatch):
    import gc
    import ssl
    import threading
    from http.server import BaseHTTPRequestHandler,HTTPServer
    import stream_v3.twitch_thumbnail_worker as module
    now=1700000000
    payload={'schema':'stream.relay_public.v1','source_fresh':True,'observed_at':module.utc_timestamp(now),
             'relay':{'status':'RUNNING'},'input':{'status':'FLOWING'},'destinations':{'twitch':{'status':'FLOWING'}}}
    response=[200,json.dumps(payload).encode()]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path=='/twitch-delivery'
            self.send_response(response[0]);self.send_header('Content-Length',str(len(response[1])));self.end_headers()
            self.wfile.write(response[1])
        def log_message(self,*args):pass
    def no_tls(*args,**kwargs):pytest.fail('localhost HTTP must not allocate a TLS context')
    monkeypatch.setattr(ssl,'_create_default_https_context',no_tls)
    monkeypatch.setenv('http_proxy','http://127.0.0.1:1')
    with HTTPServer(('127.0.0.1',0),Handler) as server:
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        opener=module.native_forward_opener();handlers=tuple(opener.handlers)
        class LocalEndpoint:
            def open(self,url,*,timeout):
                assert url=='http://127.0.0.1:9116/twitch-delivery' and timeout==2
                return opener.open(f'http://127.0.0.1:{server.server_port}/twitch-delivery',timeout=timeout)
        endpoint=LocalEndpoint();gc_enabled=gc.isenabled();gc.disable()
        try:
            for _ in range(400):assert module.native_forward(now,opener=endpoint)['flowing'] is True
            assert tuple(opener.handlers)==handlers
            response[:]=[503,b'unavailable']
            for _ in range(40):assert module.native_forward(now,opener=endpoint) is None
            response[:]=[200,b'invalid json'];assert module.native_forward(now,opener=endpoint) is None
            response[1]=b'x'*32769;assert module.native_forward(now,opener=endpoint) is None
            response[1]=json.dumps({**payload,'observed_at':module.utc_timestamp(now-91)}).encode()
            assert module.native_forward(now,opener=endpoint) is None
            class TimedOut:
                def open(self,*args,**kwargs):raise TimeoutError()
            assert module.native_forward(now,opener=TimedOut()) is None
            response[1]=json.dumps(payload).encode();assert module.native_forward(now,opener=endpoint)['flowing'] is True
        finally:
            if gc_enabled:gc.enable()
            server.shutdown();thread.join(3)


def test_worker_keeps_one_native_http_client_across_polls(tmp_path,monkeypatch):
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    clients=[]
    def forward(now,*,opener):clients.append(opener);return {}
    monkeypatch.setattr(module,'native_forward',forward)
    for _ in range(3):
        publish();read=reading(clock[0]-100);ocr.read=lambda image,**kwargs:read(image)
        assert worker.once()['ocr_verified'];http.body=None;clock[0]+=60
    assert len(clients)==3 and all(client is worker.forward_opener for client in clients)


def test_jst_midnight_stream_change_discards_etag_then_waits_without_evidence(tmp_path,monkeypatch):
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    clock[0]=datetime(2026,10,22,14,59,50,tzinfo=timezone.utc).timestamp()
    start=clock[0];read=reading(start-100);ocr.read=lambda image,**kwargs:read(image)
    publish();assert worker.once()['ocr_verified']
    clock[0]+=60;publish('second',start+50);read=reading(start-100)
    row=worker.once()
    assert http.calls[-1]==(None,None)
    assert row['clock_state']=='WAITING_NEW_STREAM_THUMBNAIL' and not row['ocr_verified']
    assert row['startup_grace_started_at_utc']==module.utc_timestamp(start+50)
    assert not row['stream_clock_observed'] and row['unique_samples_total']==1
    http.body=None;clock[0]+=60;publish('second',start+50)
    ocr.read=lambda *_a,**_k:pytest.fail('304 of the known old frame must not need OCR')
    row=worker.once()
    assert row['clock_state']=='WAITING_NEW_STREAM_THUMBNAIL' and http.calls[-1][0]=='fixture-etag'
    assert not list((tmp_path/'worker/evidence').rglob('*.pgm'))
    clock[0]=start+480;publish('second',start+50);http.body=thumbnail_body(129)
    read=reading(start+400);ocr.read=lambda image,**kwargs:read(image)
    row=worker.once()
    assert row['clock_state']=='CLOCK_OBSERVED' and row['stream_clock_observed'] and row['unique_samples_total']==2
    from stream_v3.twitch_thumbnail_metrics import write_metrics
    values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=value
    write_metrics(Writer(),row,now=clock[0])
    assert values['stream_v3_twitch_thumbnail_startup_grace_remaining_seconds']==0


def test_stream_startup_grace_survives_restart_and_api_unknown_without_being_extended(tmp_path,monkeypatch):
    from stream_v3.twitch_thumbnail_metrics import write_metrics
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    start=clock[0];publish('new',start);Path(worker.config['source_file']).unlink()
    row=worker.once();assert row['startup_grace_started_at_utc']==module.utc_timestamp(start)
    clock[0]=start+400;publish('new',start);Path(worker.config['source_file']).unlink()
    restarted=module.Worker(worker.config,worker.root,http=http,ocr=ocr);row=restarted.once()
    values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=value
    write_metrics(Writer(),row,now=clock[0])
    assert values['stream_v3_twitch_thumbnail_startup_grace_remaining_seconds']==320
    Path(worker.config['api_file']).unlink();clock[0]=start+500;unknown=restarted.once()
    assert unknown['startup_grace_started_at_utc']==module.utc_timestamp(start)
    clock[0]=start+721;publish('new',start);Path(worker.config['source_file']).unlink();row=restarted.once()
    write_metrics(Writer(),row,now=clock[0])
    assert values['stream_v3_twitch_thumbnail_startup_grace_remaining_seconds']==0
    assert values['stream_v3_twitch_thumbnail_seconds_since_last_valid_observation']==721


@pytest.mark.parametrize('provider_backoff',[False,True])
def test_normal_poll_wait_and_provider_backoff_are_explicit_not_unknown(tmp_path,monkeypatch,provider_backoff):
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    publish();worker.previous['last_request_at_utc']=module.utc_timestamp(clock[0]-10)
    if provider_backoff:worker.previous['next_fetch_not_before_utc']=module.utc_timestamp(clock[0]+500)
    row=worker.once()
    assert row['clock_state']==('WAITING_RETRY' if provider_backoff else 'WAITING_POLL')
    assert row['capture_attempted'] is False and not http.calls
    assert not row.get('evidence_id')


def test_exception_after_http_does_not_leave_a_poisoned_304_cache(tmp_path,monkeypatch):
    from stream_v3.viewer_clock_capture import ProbeError
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    publish()
    def missing(*_a,**_k):raise ProbeError('thumbnail_cache_unavailable')
    worker.analyzer.observe=missing;row=worker.once()
    assert row['clock_state']=='UNKNOWN' and http.etag is None and len(http.calls)==1


@pytest.mark.parametrize('bad_content',[None,'[]','null','not json','x'*65537])
def test_unreadable_or_invalid_publication_still_exports_worker_age(tmp_path,bad_content):
    from stream_v3.twitch_thumbnail_metrics import PublicationReader,write_metrics
    from stream_v3.viewer_clock import utc_timestamp
    reader=PublicationReader(started_at=990);path=tmp_path/'state.json'
    path.write_text(json.dumps({'schema':'stream_v3.twitch_thumbnail_observation.v1','checked_at_utc':utc_timestamp(1000)}))
    assert reader.read(path,now=1000)
    if bad_content is None:path.unlink()
    else:path.write_text(bad_content)
    row=reader.read(path,now=1400);assert row=={}
    values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=value
    write_metrics(Writer(),row,now=1400,last_cycle_epoch=reader.last_cycle_epoch)
    assert values['stream_v3_twitch_thumbnail_seconds_since_last_cycle']==400
    assert values['stream_v3_twitch_thumbnail_state_readable']==0
    assert values['stream_v3_twitch_thumbnail_clock_verified']==0
    first_read=PublicationReader(started_at=1000);assert first_read.read(path,now=1400)=={}
    assert first_read.last_cycle_epoch==1000


@pytest.mark.parametrize('frame_age,expected',[(600,'AGING'),(720,'AGING'),(840,'AGING'),(841,'STALE_SUSPECT')])
def test_one_missed_provider_refresh_does_not_become_stale(frame_age,expected):
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    analyzer=ThumbnailAnalyzer({});now=1700000000
    row=analyzer.observe(thumbnail_body(),target(now),reading(now-frame_age),now=now)
    assert row['freshness_state']==expected


@pytest.mark.parametrize('binary_case',['same','different','low_confidence'])
def test_binary_retry_requires_the_same_two_clock_values_and_original_confidence(binary_case):
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    from stream_v3.viewer_clock import ClockReading
    now=1700000000;stamp=now-100
    jst=datetime.fromtimestamp(stamp+9*3600,timezone.utc).strftime('%H:%M:%S')
    utc=datetime.fromtimestamp(stamp,timezone.utc).strftime('%H:%M:%S')
    other=datetime.fromtimestamp(stamp+60,timezone.utc).strftime('%H:%M:%S')
    results=iter([ClockReading(jst,95),ClockReading(utc,58),ClockReading(jst,95),ClockReading(utc,58),
                  ClockReading(jst,95),ClockReading(other if binary_case=='different' else utc,68 if binary_case=='low_confidence' else 95)])
    images=[]
    def ocr(image):images.append(image);return next(results)
    row=ThumbnailAnalyzer({}).observe(thumbnail_body(),target(now),ocr,now=now)
    assert len(images)==(6 if binary_case=='same' else 8)
    assert set(images[5].pixels)<={0,255} and row['ocr_binary_retried']
    assert row['ocr_verified'] is (binary_case=='same')
    assert row['ocr_attempted'] and row['ocr_observations_total']==row['ocr_binary_retries_total']==1


def binary_reading(stamp):
    from stream_v3.viewer_clock import ClockReading
    jst=datetime.fromtimestamp(stamp+9*3600,timezone.utc).strftime('%H:%M:%S')
    utc=datetime.fromtimestamp(stamp,timezone.utc).strftime('%H:%M:%S')
    values=iter([ClockReading(jst,95),ClockReading(utc,58),ClockReading(jst,95),ClockReading(utc,58),
                 ClockReading(jst,95),ClockReading(utc,95)])
    return lambda _:next(values)


def test_retry_rate_excludes_cached_images_and_keeps_counters_after_restart():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    now=1700000000;analyzer=ThumbnailAnalyzer({})
    first=analyzer.observe(thumbnail_body(),target(now),binary_reading(now-100),now=now)
    assert first['ocr_verified'] and first['ocr_observations_total']==first['ocr_binary_retries_total']==1
    def no_ocr(_):pytest.fail('a cached image is not another OCR observation or retry')
    duplicate=analyzer.observe(None,target(now+60),no_ocr,now=now+60)
    assert not duplicate['ocr_attempted'] and not duplicate['ocr_binary_retried']
    assert duplicate['ocr_observations_total']==duplicate['ocr_binary_retries_total']==1
    restarted=ThumbnailAnalyzer({},json.loads(json.dumps(duplicate)))
    duplicate=restarted.observe(thumbnail_body(),target(now+120),no_ocr,now=now+120)
    assert duplicate['ocr_observations_total']==duplicate['ocr_binary_retries_total']==1
    row=restarted.observe(thumbnail_body(129),target(now+180),reading(now+80),now=now+180)
    assert row['ocr_attempted'] and not row['ocr_binary_retried']
    assert row['ocr_observations_total']==2 and row['ocr_binary_retries_total']==1


def test_worker_keeps_retry_counters_through_capture_failure_and_restart(tmp_path,monkeypatch):
    from stream_v3.viewer_clock_capture import ProbeError
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    read=binary_reading(clock[0]-100);ocr.read=lambda image,**_kwargs:read(image)
    publish();first=worker.once();assert first['ocr_binary_retries_total']==1
    original_get=http.get
    def failure(*_args,**_kwargs):raise ProbeError('thumbnail_transport_failed')
    http.get=failure;clock[0]+=60;publish();failed=worker.once()
    assert failed['ocr_observations_total']==failed['ocr_binary_retries_total']==1
    assert not failed['ocr_attempted'] and not failed['ocr_binary_retried']
    http.get=original_get;http.body=thumbnail_body(129);clock[0]+=60;publish()
    read=reading(clock[0]-100)
    restarted=module.Worker(worker.config,worker.root,http=http,ocr=ocr);row=restarted.once()
    assert row['ocr_verified'] and row['ocr_observations_total']==2 and row['ocr_binary_retries_total']==1
    history=[json.loads(line) for line in (worker.root/'viewer_clock_history.jsonl').read_text().splitlines()]
    assert [(r['ocr_attempted'],r['ocr_binary_retried']) for r in history]==[(True,True),(False,False),(True,False)]


def test_failed_ocr_is_in_the_denominator_without_becoming_a_binary_retry(tmp_path,monkeypatch):
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    def broken(*_args,**_kwargs):raise RuntimeError('fixture OCR failure')
    ocr.read=broken;publish();row=worker.once()
    assert row['clock_state']=='OCR_UNCERTAIN' and row['ocr_attempted']
    assert row['ocr_observations_total']==1 and row['ocr_binary_retries_total']==0
    assert not row['ocr_binary_retried']


def test_retry_metrics_are_counters_and_scrapes_do_not_recount_cached_results():
    from stream_v3.twitch_thumbnail_metrics import write_metrics
    from stream_v3.viewer_clock import utc_timestamp
    values={}
    class Writer:
        def metric(self,name,value,**kwargs):values[name]=(value,kwargs)
    row={'schema':'stream_v3.twitch_thumbnail_observation.v1','checked_at_utc':utc_timestamp(1000),
         'ocr_attempted':False,'ocr_binary_retried':True,'ocr_interpolation_retried':True,
         'ocr_observations_total':5,'ocr_binary_retries_total':2,'ocr_interpolation_retries_total':1}
    write_metrics(Writer(),row,now=1000);write_metrics(Writer(),row,now=1001)
    for suffix,count in [('ocr_observations_total',5),('ocr_binary_retries_total',2),('ocr_interpolation_retries_total',1)]:
        value,options=values['stream_v3_twitch_thumbnail_'+suffix]
        assert value==count and options['metric_type']=='counter'
        assert options['labels']=={'target':'twitch_thumbnail','scope':'twitch_provider_thumbnail'}
    assert values['stream_v3_twitch_thumbnail_ocr_binary_retried'][0]==0
    assert values['stream_v3_twitch_thumbnail_ocr_interpolation_retried'][0]==0


def interpolation_reading(stamp,case='same',images=None):
    from stream_v3.viewer_clock import ClockReading
    jst=datetime.fromtimestamp(stamp+9*3600,timezone.utc).strftime('%H:%M:%S')
    utc=datetime.fromtimestamp(stamp,timezone.utc).strftime('%H:%M:%S')
    original=[ClockReading(jst,95),ClockReading(utc,0)]
    if case=='malformed_jst':original=[ClockReading('112345',0),ClockReading(utc,95)]
    if case=='malformed_utc':original=[ClockReading(jst,95),ClockReading('04735:15',0)]
    if case=='no_anchor':original=[ClockReading(jst,60),ClockReading(utc,60)]
    if case=='conflicting_original':original=[ClockReading(jst,95),ClockReading('00:00:00',0)]
    candidate=[ClockReading(jst,95),ClockReading(utc,95)]
    if case=='changed_value':
        candidate=[ClockReading(datetime.fromtimestamp(stamp+60+9*3600,timezone.utc).strftime('%H:%M:%S'),95),
                   ClockReading(datetime.fromtimestamp(stamp+60,timezone.utc).strftime('%H:%M:%S'),95)]
    if case=='mismatched_pair':candidate[1]=ClockReading('00:00:00',95)
    if case=='low_confidence':candidate[1]=ClockReading(utc,69)
    values=original*2
    if case not in ('malformed_jst','malformed_utc','conflicting_original'):values+=original
    values=iter(values+candidate)
    def read(image):
        if images is not None:images.append(image)
        if case=='timeout' and image.width>100:raise TimeoutError('fixture deadline')
        return next(values)
    return read


@pytest.mark.parametrize('case',['same','malformed_jst','malformed_utc','changed_value','mismatched_pair',
                                'low_confidence','no_anchor','conflicting_original','timeout'])
def test_interpolation_requires_an_unchanged_confident_anchor_and_consistent_clocks(case):
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    now=1700000000;images=[]
    row=ThumbnailAnalyzer({}).observe(thumbnail_body(),target(now),interpolation_reading(now-100,case,images),now=now)
    accepted=case in ('same','malformed_jst','malformed_utc')
    assert row['ocr_verified'] is accepted
    assert row['ocr_interpolation_retried'] is (case not in ('no_anchor','conflicting_original'))
    assert len(images)<=8
    assert row['ocr_interpolation_retries_total']==int(row['ocr_interpolation_retried'])
    if not accepted:assert row['clock_state']=='OCR_UNCERTAIN' and not row['clock_advancing']


def test_interpolation_preserves_freeze_staleness_and_last_verified_anchor():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    now=1700000000;stamp=now-100;analyzer=ThumbnailAnalyzer({})
    first=analyzer.observe(thumbnail_body(),target(now),reading(stamp),now=now)
    frozen=analyzer.observe(thumbnail_body(129),target(now+900),interpolation_reading(stamp),now=now+900)
    assert frozen['ocr_verified'] and frozen['clock_state']=='UNCHANGED' and not frozen['clock_advancing']
    assert frozen['freshness_state']=='STALE_SUSPECT' and frozen['frame_age_seconds']==1000
    assert frozen['unique_samples_total']==first['unique_samples_total']
    bad=analyzer.observe(thumbnail_body(130),target(now+960),interpolation_reading(stamp+60,'changed_value'),now=now+960)
    assert not bad['ocr_verified'] and bad['last_verified']['display_clock_at_utc']==first['display_clock_at_utc']


def test_interpolation_counters_survive_restart_without_counting_duplicate_frames():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    now=1700000000;analyzer=ThumbnailAnalyzer({})
    first=analyzer.observe(thumbnail_body(),target(now),interpolation_reading(now-100),now=now)
    assert first['ocr_interpolation_retries_total']==1 and first['ocr_interpolation_retried']
    def no_ocr(_):pytest.fail('cached clock must not run interpolation')
    duplicate=analyzer.observe(None,target(now+60),no_ocr,now=now+60)
    assert not duplicate['ocr_interpolation_retried'] and duplicate['ocr_interpolation_retries_total']==1
    analyzer=ThumbnailAnalyzer({},json.loads(json.dumps(duplicate)))
    restored=analyzer.observe(thumbnail_body(),target(now+120),no_ocr,now=now+120)
    assert not restored['ocr_interpolation_retried'] and restored['ocr_interpolation_retries_total']==1


@pytest.mark.parametrize('case',['future','previous_stream','regressed'])
def test_interpolation_does_not_bypass_timestamp_or_stream_binding(case):
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    from stream_v3.viewer_clock import utc_timestamp
    now=1700000000;source=target(now);analyzer=ThumbnailAnalyzer({})
    if case=='regressed':analyzer.observe(thumbnail_body(129),source,reading(now-50),now=now)
    if case=='previous_stream':source['started_at']=utc_timestamp(now-10)
    row=analyzer.observe(thumbnail_body(),source,interpolation_reading(now+20 if case=='future' else now-100),now=now)
    assert row['ocr_interpolation_retried'] and not row['ocr_verified'] and not row['clock_advancing']
    assert row['clock_state']==('WAITING_NEW_STREAM_THUMBNAIL' if case=='previous_stream' else 'OCR_UNCERTAIN')


def test_worker_keeps_interpolation_counter_and_clears_activity_on_capture_failure(tmp_path,monkeypatch):
    from stream_v3.viewer_clock_capture import ProbeError
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    read=interpolation_reading(clock[0]-100);ocr.read=lambda image,**_kwargs:read(image)
    publish();first=worker.once()
    assert first['ocr_interpolation_retried'] and first['ocr_interpolation_retries_total']==1
    def failure(*_args,**_kwargs):raise ProbeError('thumbnail_transport_failed')
    http.get=failure;clock[0]+=60;publish()
    restarted=module.Worker(worker.config,worker.root,http=http,ocr=ocr);failed=restarted.once()
    assert failed['ocr_interpolation_retries_total']==1 and not failed['ocr_interpolation_retried']


def test_retry_acceptance_is_counted_only_for_verified_readings_not_cached_or_regressed():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    now=1700000000;analyzer=ThumbnailAnalyzer({})
    first=analyzer.observe(thumbnail_body(),target(now),interpolation_reading(now-100),now=now)
    assert first['ocr_method']=='interpolation' and first['ocr_failure_reason']=='none'
    assert first['ocr_interpolation_accepted_total']==1 and first['ocr_binary_accepted_total']==0
    assert first['ocr_stage_results']['original']=='low_confidence'
    assert first['ocr_stage_results']['interpolation']=='accepted'
    row=analyzer.observe(None,target(now+60),lambda _:pytest.fail('no repeated OCR'),now=now+60)
    assert row['ocr_method']=='cached' and row['ocr_source_method']=='interpolation'
    assert row['ocr_interpolation_accepted_total']==1
    analyzer=ThumbnailAnalyzer({},json.loads(json.dumps(row)))
    row=analyzer.observe(thumbnail_body(129),target(now+120),interpolation_reading(now-160),now=now+120)
    assert not row['ocr_verified'] and row['ocr_failure_reason']=='time_regressed'
    assert row['ocr_method']=='none' and row['ocr_stage_results']['interpolation']=='accepted'
    assert row['ocr_interpolation_accepted_total']==1 and row['ocr_interpolation_retries_total']==2


@pytest.mark.parametrize('kind',['timeout','process_failed','invalid_time','pair_mismatch'])
def test_ocr_failure_codes_are_bounded_and_preserve_an_uncertain_observation(kind):
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    from stream_v3.viewer_clock import ClockReading
    now=1700000000
    def failed(_):
        if kind=='timeout':raise TimeoutError('private diagnostic detail')
        if kind=='process_failed':raise RuntimeError('private diagnostic detail')
        return ClockReading('garbled' if kind=='invalid_time' else '00:00:00',95)
    row=ThumbnailAnalyzer({}).observe(thumbnail_body(),target(now),failed,now=now)
    assert row['clock_state']=='OCR_UNCERTAIN' and not row['ocr_verified']
    assert row['ocr_failure_reason']==kind
    assert 'private diagnostic detail' not in json.dumps(row)


def test_continuous_uncertain_images_get_distinct_bounded_evidence_and_roles(tmp_path,monkeypatch):
    import zipfile
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    from stream_v3.viewer_clock import ClockReading
    ocr.read=lambda *_args,**_kwargs:ClockReading('unreadable',0)
    publish();first=worker.once();assert first['evidence_status']=='saved'
    clock[0]+=60;publish();same=worker.once()
    assert same['evidence_status']=='reused' and same['evidence_id']==first['evidence_id']
    clock[0]+=60;publish();http.body=thumbnail_body(129);second=worker.once()
    assert second['evidence_status']=='saved' and second['evidence_id']!=first['evidence_id']
    assert second['clock_state']==same['clock_state']=='OCR_UNCERTAIN'
    with zipfile.ZipFile(worker.root/'evidence'/second['evidence_id']) as archive:
        assert 'jst.pgm' in archive.namelist() and 'utc.pgm' in archive.namelist()
    restarted=module.Worker(worker.config,worker.root,http=http,ocr=ocr)
    clock[0]+=60;publish();again=restarted.once()
    assert again['evidence_status']=='reused' and len(list((worker.root/'evidence').glob('*.zip')))==2


def test_new_metric_labels_are_fixed_and_acceptance_counts_do_not_change_on_scrape():
    from stream_v3.twitch_thumbnail_metrics import write_metrics
    from stream_v3.viewer_clock import utc_timestamp
    captured=[]
    class Writer:
        def metric(self,name,value,**kwargs):captured.append((name,value,kwargs))
    row={'schema':'stream_v3.twitch_thumbnail_observation.v1','checked_at_utc':utc_timestamp(1000),
         'ocr_method':'interpolation','ocr_failure_reason':'none','ocr_interpolation_accepted_total':3,
         'ocr_binary_accepted_total':2,'evidence_status':'capacity_limited'}
    write_metrics(Writer(),row,now=1000)
    assert any(n.endswith('ocr_interpolation_accepted_total') and v==3 and k['metric_type']=='counter' for n,v,k in captured)
    assert sum(v for n,v,_ in captured if n.endswith('ocr_method'))==1
    assert sum(v for n,v,_ in captured if n.endswith('ocr_failure_reason'))==1
    captured.clear();row.update(ocr_method='private-value',ocr_failure_reason='private-value',evidence_status='private-value')
    write_metrics(Writer(),row,now=1000)
    assert 'private-value' not in json.dumps(captured)


def test_roi_configuration_change_requires_a_new_reading_instead_of_cached_health():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    now=1700000000;analyzer=ThumbnailAnalyzer({'utc_roi':(.081,.968,.04,.022)})
    old=analyzer.observe(thumbnail_body(),target(now),reading(now-100),now=now)
    changed=ThumbnailAnalyzer({'utc_roi':(.0795,.968,.0415,.022)},old)
    row=changed.observe(thumbnail_body(),target(now+60),reading(now-100),now=now+60)
    assert row['ocr_attempted'] and row['ocr_method']=='original' and row['clock_state']=='UNCHANGED'


def test_contrast_acceptance_is_counted_and_not_confused_with_a_failed_later_binding():
    from stream_v3.twitch_thumbnail import ThumbnailAnalyzer
    from stream_v3.viewer_clock import ClockReading
    now=1700000000
    def contrast_reader(stamp):
        jst=datetime.fromtimestamp(stamp+9*3600,timezone.utc).strftime('%H:%M:%S')
        utc=datetime.fromtimestamp(stamp,timezone.utc).strftime('%H:%M:%S')
        values=iter([ClockReading(jst,95),ClockReading(utc,40),ClockReading(jst,95),ClockReading(utc,95)])
        return lambda _:next(values)
    analyzer=ThumbnailAnalyzer({})
    row=analyzer.observe(thumbnail_body(),target(now),contrast_reader(now-100),now=now)
    assert row['ocr_method']=='contrast' and row['ocr_contrast_retried']
    assert row['ocr_contrast_accepted_total']==row['ocr_contrast_retries_total']==1
    bad=analyzer.observe(thumbnail_body(129),target(now+60),contrast_reader(now-160),now=now+60)
    assert bad['ocr_failure_reason']=='time_regressed' and bad['ocr_contrast_retries_total']==2
    assert bad['ocr_contrast_accepted_total']==1


def test_capture_gap_does_not_release_current_evidence_when_storage_is_full(tmp_path,monkeypatch):
    from stream_v3.viewer_clock import ClockReading
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    ocr.read=lambda *_args,**_kwargs:ClockReading('unreadable',0)
    publish();first=worker.once();name=first['evidence_id'];assert name
    clock[0]+=60;Path(worker.config['api_file']).unlink();unknown=worker.once()
    assert unknown['clock_state']=='UNKNOWN' and worker.store.active['twitch_thumbnail'].name==name
    worker.store.max_bytes=worker.store.disk_bytes(worker.root/'evidence'/name)
    clock[0]+=60;publish();http.body=thumbnail_body(129);row=worker.once()
    assert row['evidence_status']=='capacity_limited' and (worker.root/'evidence'/name).exists()


@pytest.mark.parametrize('header,expected',[('120',120),('0',0),('86400',86400),('86401',None),('-1',None),('1.5',None),
                                            ('',None),('１２',None),('9999999',None),(None,None)])
def test_cdn_age_header_is_a_bounded_integer_or_absent(header,expected):
    from email.message import Message
    from stream_v3.twitch_thumbnail import cdn_age
    headers=Message()
    if header is not None:headers['Age']=header
    assert cdn_age(headers)==expected



def test_thumbnail_http_reports_cdn_age_for_200_and_304():
    import threading
    import time
    from http.server import BaseHTTPRequestHandler,HTTPServer
    from stream_v3.twitch_thumbnail import ThumbnailHTTP
    state={'etag':'"a"','age':'120'}
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body=thumbnail_body()
            if self.headers.get('If-None-Match')==state['etag']:
                self.send_response(304);self.send_header('ETag',state['etag']);self.send_header('Age',state['age']);self.end_headers();return
            self.send_response(200);self.send_header('Content-Type','image/jpeg');self.send_header('ETag',state['etag'])
            self.send_header('Age',state['age']);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    with HTTPServer(('127.0.0.1',0),Handler) as server:
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            http=ThumbnailHTTP();url=f'http://127.0.0.1:{server.server_port}/thumb.jpg'
            body,meta=http.get(url,deadline=time.monotonic()+5)
            assert body and meta=={'http_status':200,'download_bytes':len(body),'cdn_age_seconds':120}
            state['age']='180';body,meta=http.get(url,deadline=time.monotonic()+5)
            assert body is None and meta=={'http_status':304,'download_bytes':0,'cdn_age_seconds':180}
            state.update(etag='"b"',age='bogus');body,meta=http.get(url,deadline=time.monotonic()+5)
            assert body and meta['cdn_age_seconds'] is None
        finally:server.shutdown();thread.join(3)


def timing_fixture(tmp_path,monkeypatch):
    module,worker,http,ocr,clock,publish=thumbnail_worker_fixture(tmp_path,monkeypatch)
    meta={'cdn_age_seconds':30}
    original=type(http).get
    def get(self,url,**kwargs):
        body,base=original(self,url,**kwargs);return body,{**base,**meta}
    monkeypatch.setattr(type(http),'get',get)
    def poll(clock_age=None,*,failed=False):
        from stream_v3.viewer_clock import ClockReading
        if failed:ocr.read=lambda image,**kwargs:ClockReading(error='ocr_failed')
        elif clock_age is not None:
            read=reading(clock[0]-clock_age);ocr.read=lambda image,**kwargs:read(image)
        return worker.once()
    return module,worker,http,ocr,clock,publish,meta,poll


def test_worker_splits_first_sight_age_and_keeps_it_for_the_same_image(tmp_path,monkeypatch):
    from stream_v3.viewer_clock import utc_timestamp
    module,worker,http,ocr,clock,publish,meta,poll=timing_fixture(tmp_path,monkeypatch)
    publish();start=clock[0];row=poll(80)
    assert row['ocr_verified'] and row['unique_sample']
    assert (row['discovery_lag_seconds'],row['cdn_age_at_discovery_seconds'],row['origin_side_age_estimate_seconds'])==(80,30,50)
    assert row['image_changed_at_utc']==utc_timestamp(start) and row['image_changed_cdn_age_seconds']==30
    # A 304 a minute later: the per-image split stays, the current Age moves on.
    clock[0]+=60;publish();http.body=None;meta['cdn_age_seconds']=90
    row=poll()
    assert row['image_duplicate'] and row['cdn_age_seconds']==90
    assert (row['discovery_lag_seconds'],row['origin_side_age_estimate_seconds'],row['image_changed_at_utc'])==(80,50,utc_timestamp(start))
    # Without an Age header the origin side is unknown, never guessed.
    clock[0]+=60;publish();http.body=thumbnail_body(90);meta['cdn_age_seconds']=None
    row=poll(70)
    assert row['unique_sample'] and row['discovery_lag_seconds']==70 and row['origin_side_age_estimate_seconds'] is None
    # A new stream never inherits the previous stream's timing.
    clock[0]+=60;publish(stream='second');http.body=None
    row=poll()
    assert all(row[key] is None for key in module.IMAGE_KEYS+module.LAG_KEYS)


def test_restart_and_full_refetch_of_the_same_image_are_not_image_changes(tmp_path,monkeypatch):
    from stream_v3.viewer_clock import utc_timestamp
    module,worker,http,ocr,clock,publish,meta,poll=timing_fixture(tmp_path,monkeypatch)
    publish();start=clock[0];first=poll(80)
    # A restart loses the in-memory ETag, so the same image comes back as a full 200 body.
    restarted=module.Worker(worker.config,worker.root,http=http,ocr=ocr)
    clock[0]+=60;publish();meta['cdn_age_seconds']=90
    row=restarted.once()
    assert row['image_duplicate'] and not row['unique_sample']
    assert row['image_changed_at_utc']==utc_timestamp(start) and row['image_changed_cdn_age_seconds']==30
    assert {k:row[k] for k in module.LAG_KEYS}=={k:first[k] for k in module.LAG_KEYS}


def test_first_sight_is_kept_when_ocr_fails_before_a_later_success(tmp_path,monkeypatch):
    from stream_v3.viewer_clock import utc_timestamp
    module,worker,http,ocr,clock,publish,meta,poll=timing_fixture(tmp_path,monkeypatch)
    publish();start=clock[0];stamp=start-80
    row=poll(failed=True)
    assert not row['ocr_verified'] and row['image_changed_at_utc']==utc_timestamp(start)
    assert row['discovery_lag_seconds'] is None
    # The same image is fetched again a minute later (Age 90) and OCR now succeeds.
    clock[0]+=60;publish();meta['cdn_age_seconds']=90
    read=reading(stamp);ocr.read=lambda image,**kwargs:read(image)
    row=worker.once()
    assert row['ocr_verified'] and row['unique_sample']
    assert (row['discovery_lag_seconds'],row['cdn_age_at_discovery_seconds'],row['origin_side_age_estimate_seconds'])==(80,30,50)


def test_provider_timing_metrics_require_a_fresh_verified_reading():
    from stream_v3.twitch_thumbnail_metrics import write_metrics
    from stream_v3.viewer_clock import utc_timestamp
    captured={}
    class Writer:
        def metric(self,name,value,**kwargs):captured[name]=value
    now=1000000.
    row={'schema':'stream_v3.twitch_thumbnail_observation.v1','checked_at_utc':utc_timestamp(now-10),'api_live':True,
         'api_collected_at_utc':utc_timestamp(now-10),'capture_ok':True,'ocr_verified':True,'display_clock_at_utc':utc_timestamp(now-200),
         'cdn_age_seconds':150,'discovery_lag_seconds':80.,'cdn_age_at_discovery_seconds':30,'origin_side_age_estimate_seconds':50.,
         'image_changed_at_utc':utc_timestamp(now-130)}
    write_metrics(Writer(),row,now=now)
    p='stream_v3_twitch_thumbnail_'
    assert (captured[p+'cdn_age_seconds'],captured[p+'discovery_lag_seconds'],captured[p+'origin_side_age_estimate_seconds'])==(150,80.,50.)
    assert captured[p+'cdn_age_at_discovery_seconds']==30 and captured[p+'seconds_since_image_change']==130
    captured.clear();write_metrics(Writer(),{**row,'ocr_verified':False},now=now)
    assert not any(k.endswith(('cdn_age_seconds','discovery_lag_seconds','origin_side_age_estimate_seconds')) for k in captured)
    captured.clear();write_metrics(Writer(),{**row,'origin_side_age_estimate_seconds':float('inf'),'cdn_age_seconds':True},now=now)
    assert p+'origin_side_age_estimate_seconds' not in captured and p+'cdn_age_seconds' not in captured
