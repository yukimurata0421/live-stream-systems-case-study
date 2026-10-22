"""Exercise the source adapter against the public collector, without network access."""
import importlib.util
import json
from pathlib import Path
import sys


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module)
    return module


def test_real_public_collector_payload_and_normal_request_count_are_preserved(tmp_path,monkeypatch):
    root=Path(__file__).parents[1]
    api=load('thumbnail_public_api_fixture',root/'monitoring-v4/src/stream_monitoring_v4/collectors/twitch_api.py')
    adapter=load('thumbnail_public_source_fixture',root/'ops/scripts/stream_v3_twitch_thumbnail_source.py')
    credentials=tmp_path/'credentials';credentials.mkdir()
    (credentials/'client-id').write_text('dummyClient123')
    (credentials/'client-secret').write_text('dummySecret123')
    template='https://static-cdn.jtvnw.net/previews-ttv/live_user_fixturechannel-{width}x{height}.jpg'
    requests=[]
    def transport(url,headers,body,timeout):
        requests.append(url)
        if url==api.TOKEN_URL:return {'access_token':'dummyToken123','expires_in':7200,'token_type':'bearer'},{}
        if url==api.VALIDATE_URL:return {'client_id':'dummyClient123','expires_in':7200,'scopes':[]},{}
        if url.startswith(api.USERS_URL):return {'data':[{'id':'100000001','login':'fixturechannel'}]},{}
        assert url.startswith(api.STREAMS_URL)
        return {'data':[{'id':'900000001','user_id':'100000001','user_login':'fixturechannel','type':'live',
                         'started_at':'2026-10-12T22:00:00Z','viewer_count':2,'thumbnail_url':template}]},{}
    options=dict(transport=transport,wall=lambda:1791845400.,monotonic=lambda:1000.)
    original=api.TwitchCollector(credentials,'100000001','fixturechannel',**options)
    original.instance='fixture-instance'
    first=original.collect();normal=original.collect()
    mirror=tmp_path/'ram-source.json'
    monkeypatch.setattr(adapter,'ram_path',lambda _:True)
    adapter.attach(api,mirror)
    wrapped=api.TwitchCollector(credentials,'100000001','fixturechannel',**options)
    wrapped.instance='fixture-instance'
    before=len(requests);assert wrapped.collect()==first
    assert len(requests)-before==4
    before=len(requests);payload=wrapped.collect();assert payload==normal
    assert payload['api_request_count']==len(requests)-before==1
    output=tmp_path/'output/twitch_api_evidence.json'
    api.atomic_write(output,json.dumps(payload))
    assert json.loads(output.read_text())==normal
    assert json.loads(mirror.read_text())['thumbnail_url']==template
    assert 'thumbnail_url' not in normal
