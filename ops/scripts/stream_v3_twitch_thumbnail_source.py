#!/usr/bin/env python3
"""Mirror a thumbnail URL from the existing Helix response into RAM without extra API requests."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
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


def attach(baseline,path):
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
    try:attach(module,Path(os.environ['TWITCH_THUMBNAIL_SOURCE_FILE']))
    except Exception:print(json.dumps({'event':'thumbnail_source_extension_incompatible'}),flush=True)
    return module.main()


if __name__=='__main__':raise SystemExit(main())
