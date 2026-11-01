#!/usr/bin/env python3
"""Mirror the existing Helix response into a coherent private bundle without extra API requests."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import HTTPHandler, HTTPDefaultErrorHandler, HTTPErrorProcessor, OpenerDirector
from urllib.parse import urlsplit

SCHEMA='stream_v3.twitch_thumbnail_source.v1'


def thumbnail_template(url,login):
    try:
        parsed=urlsplit(url)
        expected='/previews-ttv/live_user_'+login+'-{width}x{height}.jpg'
        return (isinstance(url,str) and len(url)<=2048 and parsed.scheme=='https'
                and parsed.hostname=='static-cdn.jtvnw.net' and parsed.port in (None,443)
                and parsed.username is None and parsed.password is None and not parsed.query
                and not parsed.fragment and parsed.path==expected)
    except (ValueError,TypeError,AttributeError):return False


def ram_path(path):
    resolved=str(path.resolve());mounts=[]
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        left,right=line.split(' - ',1);mount=left.split()[4].replace('\\040',' ')
        if resolved==mount or resolved.startswith(mount.rstrip('/')+'/'):mounts.append((len(mount),right.split()[0]))
    return bool(mounts) and max(mounts)[1]=='tmpfs'


def facts_server(address,forward_url):
    """Bridge existing observations into the pod network; no credentials or control API.

    The bundle is one immutable publication from the original Helix result. The
    forward bridge retains the original observation timestamp and freshness flag.
    Only this transitional host adapter knows the legacy loopback endpoint.
    """
    latest={};lock=threading.Lock()
    opener=OpenerDirector()
    for handler in (HTTPHandler(),HTTPDefaultErrorHandler(),HTTPErrorProcessor()):opener.add_handler(handler)
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup();self.connection.settimeout(2)
        def do_GET(self):
            status=200
            if self.path=='/bundle':
                with lock:value=latest.copy()
                if not value:status=503
            elif self.path=='/forward':
                try:
                    with opener.open(forward_url,timeout=2) as response:raw=response.read(32769)
                    if len(raw)>32768:raise ValueError('too_large')
                    value=json.loads(raw)
                    if not isinstance(value,dict) or value.get('schema')!='stream.relay_public.v1':raise ValueError('schema')
                except Exception as error:
                    if hasattr(error,'close'):error.close()
                    status,value=503,{'error':'forward_unavailable'}
            else:status,value=404,{'error':'not_found'}
            raw=json.dumps(value,allow_nan=False).encode()
            self.send_response(status);self.send_header('Content-Type','application/json')
            self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(raw)))
            self.end_headers();self.wfile.write(raw)
        def log_message(self,*_):pass
    server=HTTPServer(address,Handler)
    def publish_bundle(api,source):
        with lock:latest.update(schema='stream_v3.twitch_thumbnail_bundle.v1',api=api,source=source)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    return server,publish_bundle


def attach(baseline,path,*,publish_bundle=None):
    """Add-on failure never turns a successful Helix observation into failure."""
    if baseline.SCHEMA!='monitoring_v4.twitch_api_evidence.v1':raise ValueError('unsupported_api_contract')
    original_class,original_write=baseline.TwitchCollector,baseline.atomic_write
    pending={};revision=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    class Collector(original_class):
        def _request(self,url,*args,**kwargs):
            result=super()._request(url,*args,**kwargs)
            if url.startswith(baseline.STREAMS_URL+'?'):
                rows=result.get('data',[])
                pending['url']=rows[0].get('thumbnail_url') if isinstance(rows,list) and len(rows)==1 and isinstance(rows[0],dict) else None
            return result

        def collect(self,*args,**kwargs):
            pending.clear()
            return super().collect(*args,**kwargs)

    def publish(target,text):
        original_write(target,text)
        if Path(target).name!='twitch_api_evidence.json':return
        try:
            payload=json.loads(text);url=pending.get('url')
            fields=('source_host_id','source_boot_id','collector_instance','collector_revision','sequence','user_id','user_login',
                    'stream_id','started_at','provider_state','probe_status','collected_at','valid_until','freshness_ttl_sec')
            sidecar={k:payload.get(k) for k in fields}
            sidecar.update(schema=SCHEMA,adapter_revision=revision,thumbnail_url=None,thumbnail_status='unavailable')
            if payload.get('probe_status')=='ok' and payload.get('provider_state')=='OFFLINE':sidecar['thumbnail_status']='offline'
            elif payload.get('probe_status')=='ok' and payload.get('provider_state')=='LIVE' and thumbnail_template(url,payload.get('user_login','')):
                sidecar.update(thumbnail_url=url,thumbnail_status='available')
            if publish_bundle is not None:publish_bundle(payload,sidecar)
            if not ram_path(path):raise ValueError('thumbnail_source_requires_tmpfs')
            original_write(path,json.dumps(sidecar,sort_keys=True,allow_nan=False)+'\n')
        except Exception:
            print(json.dumps({'event':'thumbnail_source_unavailable'}),flush=True)
    baseline.TwitchCollector=Collector;baseline.atomic_write=publish
    return baseline


def main():
    path=Path(os.environ['TWITCH_API_BASELINE']).resolve()
    # The root-owned current release can advance independently; its revision
    # remains diagnostic, rather than a manual hash admission requirement.
    for parent in (path,*path.parents):
        if parent==Path('/'):break
        stat=parent.stat()
        if stat.st_uid!=0 or stat.st_mode&0o022:raise ValueError('api_baseline_not_root_owned')
    spec=importlib.util.spec_from_file_location('thumbnail_api_baseline',path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    server=None;publish_bundle=None
    if os.environ.get('TWITCH_THUMBNAIL_FACTS_BIND'):
        host,port=os.environ['TWITCH_THUMBNAIL_FACTS_BIND'].rsplit(':',1)
        server,publish_bundle=facts_server((host,int(port)),os.environ['TWITCH_THUMBNAIL_FORWARD_URL'])
    try:attach(module,Path(os.environ['TWITCH_THUMBNAIL_SOURCE_FILE']),publish_bundle=publish_bundle)
    except Exception:print(json.dumps({'event':'thumbnail_source_extension_incompatible'}),flush=True)
    try:return module.main()
    finally:
        if server:server.shutdown();server.server_close()


if __name__=='__main__':raise SystemExit(main())
