"""Exercise HTTP handoffs, stale evidence and pod health without provider requests."""
import importlib.util
import io
import json
from pathlib import Path
import threading
import time
import types
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError

import pytest

from stream_v3.thumbnail_http import http_opener, publication_server, read_json
from stream_v3.twitch_thumbnail_worker import Worker
from stream_v3.twitch_thumbnail_metrics import PublicationReader
from stream_v3.viewer_clock import utc_timestamp
from test_twitch_thumbnail import reading, thumbnail_body, source_module


@pytest.fixture
def bridge():
    module = source_module()
    server, publish = module.facts_server(('127.0.0.1', 0), 'http://127.0.0.1:1/unavailable')
    url = 'http://127.0.0.1:' + str(server.server_port)
    yield module, publish, url
    server.shutdown(); server.server_close()


def evidence(now):
    api = dict(schema='monitoring_v4.twitch_api_evidence.v1', source_host_id='arena-monitoring-facts',
               source_boot_id='boot', collector_instance='collector', collector_revision='revision', sequence=42,
               user_id='id', user_login='fixture', stream_id='stream', started_at=utc_timestamp(now-10000),
               provider_state='LIVE', probe_status='ok', collected_at=utc_timestamp(now), valid_until=utc_timestamp(now+180))
    source = dict(api, schema='stream_v3.twitch_thumbnail_source.v1', thumbnail_status='available',
                  thumbnail_url='https://static-cdn.jtvnw.net/previews-ttv/live_user_fixture-{width}x{height}.jpg')
    return api, source


def worker(tmp_path, url, now):
    class ImageHTTP:
        etag = None
        cache_url = None
        calls = 0
        retry_at = 0
        def get(self, url, *, deadline):
            self.calls += 1
            return thumbnail_body(), {'http_status': 200}
    values = reading(now-100)
    ocr = types.SimpleNamespace(read=lambda image, **_: values(image))
    config = dict(source_url=url+'/bundle', forward_url=url+'/forward', boot_id='boot', user_id='id', user_login='fixture')
    return Worker(config, tmp_path, http=ImageHTTP(), ocr=ocr)


def test_real_http_bundle_worker_publication_and_metrics(bridge, tmp_path):
    module, publish, url = bridge
    now = time.time(); api, source = evidence(now)
    publish(api, source)
    observed = read_json(url+'/bundle', opener=http_opener())
    assert observed['api'] == api and observed['source'] == source
    w = worker(tmp_path, url, now)
    result = w.once()
    assert result['ocr_verified'] is True and w.http.calls == 1
    assert result['forward_observed_at_utc'] is None
    server = publication_server(('127.0.0.1',0), w)
    endpoint = 'http://127.0.0.1:'+str(server.server_port)
    try:
        reader = PublicationReader(started_at=now)
        received = reader.read_url(endpoint+'/observation', now=time.time())
        assert received == result
        assert json.loads((tmp_path/'observation.json').read_text()) == result
        last = reader.last_cycle_epoch
        assert reader.read_url(endpoint+'/missing', now=now+300) == {}
        assert reader.last_cycle_epoch == last
        assert read_json(endpoint+'/healthz', opener=http_opener())['loop_responsive']
        w.last_cycle_monotonic -= 181
        with pytest.raises(HTTPError): read_json(endpoint+'/healthz', opener=http_opener())
    finally:
        server.shutdown(); server.server_close()
    # Persisted request spacing and numeric anchor survive the pod replacement.
    again = worker(tmp_path, url, time.time())
    result2 = again.once()
    assert again.http.calls == 0 and result2['clock_state'] == 'WAITING_POLL'
    for key in ('display_clock_at_utc','last_progress_at_utc','image_sha256','unique_samples_total'):
        assert result2['last_verified'][key] == result['last_verified'][key]


@pytest.mark.parametrize('change', ['boot', 'target', 'sequence', 'stale', 'future', 'schema', 'nonobject'])
def test_http_does_not_weaken_source_admission(bridge, tmp_path, change):
    _, publish, url = bridge
    now=time.time(); api, source=evidence(now)
    if change=='boot': api['source_boot_id']=source['source_boot_id']='other-boot'
    if change=='target': api['user_id']=source['user_id']='other'
    if change=='sequence': source['sequence']+=1
    if change=='stale':
        api['collected_at']=source['collected_at']=utc_timestamp(now-181)
        api['valid_until']=source['valid_until']=utc_timestamp(now-1)
    if change=='future': api['collected_at']=source['collected_at']=utc_timestamp(now+30)
    if change=='schema': source['schema']='wrong'
    if change=='nonobject': source=[]
    publish(api,source)
    w=worker(tmp_path,url,now); row=w.once()
    assert not row['capture_ok'] and not row['ocr_verified'] and w.http.calls==0
    assert row['clock_state']=='UNKNOWN'
    assert time.monotonic()-w.last_cycle_monotonic<2


def test_bundle_is_coherent_and_does_not_depend_on_disk_mirror(bridge, tmp_path, monkeypatch):
    module,publish,url=bridge; now=time.time();api,source=evidence(now);writes=[]
    class Collector:
        def _request(self,url):return {'data':[{'thumbnail_url':source['thumbnail_url']}]}
        def collect(self):self._request('https://api.twitch.tv/helix/streams?user_id=id');return api
    def write(path,text): writes.append(Path(path).name)
    baseline=types.SimpleNamespace(SCHEMA=api['schema'],TwitchCollector=Collector,atomic_write=write,STREAMS_URL='https://api.twitch.tv/helix/streams')
    monkeypatch.setattr(module,'ram_path',lambda _:False)
    module.attach(baseline,tmp_path/'thumbnail_source.json',publish_bundle=publish)
    value=baseline.TwitchCollector().collect()
    baseline.atomic_write(tmp_path/'twitch_api_evidence.json',json.dumps(value))
    got=read_json(url+'/bundle',opener=http_opener())
    assert writes==['twitch_api_evidence.json'] and got['api']==api
    assert got['source']['sequence']==api['sequence'] and got['source']['thumbnail_status']=='available'


def test_bridge_has_no_arbitrary_proxy_or_write_api(bridge):
    from urllib.request import Request
    _,_,url=bridge
    for path in ['/bundle','/forward','/etc/passwd','/forward?url=http://example.invalid/']:
        with pytest.raises((HTTPError,ValueError)):read_json(url+path,opener=http_opener())
    with pytest.raises(HTTPError) as error: http_opener().open(Request(url+'/bundle',data=b'{}',method='POST'))
    assert error.value.code==501
    error.value.close()


def test_remote_reader_rejects_oversize_redirect_and_nonobject():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/redirect':
                self.send_response(302);self.send_header('Location','http://127.0.0.1:1/private');self.end_headers();return
            data=b'x'*65537 if self.path=='/large' else b'[]'
            self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
        def log_message(self,*_):pass
    server=HTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
    url='http://127.0.0.1:'+str(server.server_port)
    try:
        for path in ['/redirect','/large','/array']:
            assert PublicationReader(started_at=1).read_url(url+path,now=2)=={}
            with pytest.raises((ValueError,HTTPError)):read_json(url+path,opener=http_opener())
    finally:server.shutdown();server.server_close()


def test_manifest_has_only_owned_storage_and_no_control_credentials():
    spec=json.loads((Path(__file__).parents[1]/'ops/kubernetes/twitch-thumbnail/manifest.json').read_text())
    deployment=next(i for i in spec['items'] if i['kind']=='Deployment')
    pod=deployment['spec']['template']['spec']
    assert deployment['spec']['strategy']['type']=='Recreate'
    assert pod['automountServiceAccountToken'] is False and not pod.get('hostNetwork')
    assert all('hostPath' not in v and 'secret' not in v for v in pod['volumes'])
    assert pod['containers'][0]['securityContext']['readOnlyRootFilesystem'] is True
    config=json.loads(next(i for i in spec['items'] if i['kind']=='ConfigMap')['data']['config.json'])
    assert 'api_file' not in config and 'source_file' not in config
    assert 'localhost' not in json.dumps(config) and '127.0.0.1' not in json.dumps(config)
