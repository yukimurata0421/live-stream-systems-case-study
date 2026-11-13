#!/usr/bin/env python3
"""Generate public artifacts on their observation owner; export only that bundle."""
from __future__ import annotations
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import ProxyHandler, build_opener

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
SCRIPTS=ROOT/'ops/public-publisher/site/scripts'
sys.path.insert(0,str(SCRIPTS))
from stream_core.common import observation_time as timing, clock_contract as clock, upload_samples
FILES=('stream-v3-prometheus.json','stream-v3-loki.json','reliability-indicators.json',
       'clock-observation.json','relay-observation.json','twitch-observation.json','twitch-sli.json','oci-egress.json')
MAX_BYTES=2*1024*1024
SCHEMA='stream.public_observation_bundle.v1'


def module(name, path):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);sys.modules[name]=value;spec.loader.exec_module(value)
    return value


def command(args, *, env=None, timeout=30, cwd=None):
    r=subprocess.run(args,env=env,cwd=cwd,capture_output=True,timeout=timeout,check=True)
    if len(r.stdout)>MAX_BYTES:raise ValueError('public_source_too_large')
    return json.loads(r.stdout)


def local_url(url):
    with build_opener(ProxyHandler({})).open(url,timeout=5)as r:raw=r.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('public_source_too_large')
    return json.loads(raw)


def generated(value):
    from datetime import datetime
    stamp=value['generated_at']
    return datetime.fromisoformat(stamp.replace('Z','+00:00')).timestamp() if isinstance(stamp,str) else stamp


def wrap(value, started, finished):
    stamp=generated(value)
    # Compare UTC only on this host at generation. Forward the original age
    # through a boot-bound receipt, never regenerate freshness on reads.
    if not timing.finite(stamp) or not 0<=finished.wall-stamp<=180:raise ValueError('public_source_age_invalid')
    mono=min(started.mono,finished.mono-(finished.wall-stamp))
    return {'value':value,'boot_id':finished.boot,'generated_mono':mono}


def build(state_root, previous=None, *, report_root=ROOT):
    import reliability_projection as reliability
    import collect_clock_public as clocks
    import collect_relay_public as relay
    import collect_twitch_public as twitch
    import collect_twitch_sli as sli
    import collect_oci_egress_public as egress
    legacy=module('public_status_generator',ROOT/'ops/public-publisher/status/collect_stream_v3_public.py')
    # Local Prometheus/Loki, no SSH in the observation owner's query path.
    legacy.RECOVERY_SSH_TARGET=''
    def query(base,path,params):
        from urllib.parse import urlencode
        return local_url(base+path+'?'+urlencode(params))
    def prom(q):
        v=query('http://127.0.0.1:9090','/api/v1/query',{'query':q})
        if v.get('status')!='success':raise ValueError('query_failed')
        return v['data']['result']
    legacy.prom_query=prom
    def prom_range(q,start,end,step):
        v=query('http://127.0.0.1:9090','/api/v1/query_range',{'query':q,'start':start,'end':end,'step':step})
        if v.get('status')!='success':raise ValueError('query_failed')
        return v['data']['result']
    legacy.prom_query_range=prom_range
    def loki(q,limit,window_sec=None):
        params={'query':q,'limit':limit,'direction':'BACKWARD'}
        if window_sec:
            end=time.time_ns();params.update(start=end-window_sec*10**9,end=end)
        v=query('http://127.0.0.1:3100','/loki/api/v1/query_range',params)
        if v.get('status')!='success':raise ValueError('query_failed')
        return v['data']['result']
    legacy.loki_query=loki
    def prometheus_value():
        value = legacy.collect_prometheus()
        source = upload_samples.read_json(state_root/'subsystems_status.json', MAX_BYTES)
        if not isinstance(source, dict):
            source = {}
        qualified = timing.qualify(source.get('observation_time'), source='subsystems', max_age=180)
        legacy.attach_subsystem_details(value, source, qualified=qualified.valid)
        return value
    def clock_value():
        spec=json.dumps({'step':clocks.STEP,'buckets':clocks.BUCKETS,'instant':clocks.INSTANT,'range':clocks.RANGE})
        return clocks.build(command(['/usr/bin/python3','-I','-c',clocks.REMOTE,spec]),time.time())
    def reliability_value():
        env={**os.environ,'STREAM_RUNTIME_STATE_DIR':str(state_root),'STREAM_RUNTIME_LOG_DIR':str(state_root/'logs'),
             'STREAM_V3_PUBLIC_AUDIO_MAINTENANCE':'1'}
        raw=command([str(report_root/'bin/stream-prod'),'sli-report','--windows','24h,7d,30d','--json'],env=env,timeout=60,cwd=report_root)
        if raw.get('metric_errors'):raise ValueError('sli_report_incomplete')
        active=upload_samples.read_json(state_root/'stream_notify_state.json',2*1024*1024).get('active',{})
        rows=[{'id':ident,**{k:row.get('last_incident',{}).get(k) for k in ('severity','recovery_type','observed_ts')}}for ident,row in active.items()]
        twitch_value=command(['/usr/bin/python3','-B','/opt/stream-v4-twitch-sli-public/current/twitch_sli_public.py'])
        picture=command(['/usr/bin/python3','-I','-c',reliability.PICTURE_REMOTE_SCRIPT])
        return reliability.build_public_payload(raw,active_incidents=reliability.normalize_active_incidents(rows),
            active_incidents_status='ok',twitch_sli=twitch_value,picture=picture,now=time.time())
    builders={
      FILES[0]:prometheus_value, FILES[1]:legacy.collect_loki,
      FILES[2]:reliability_value, FILES[3]:clock_value,
      FILES[4]:lambda:relay.validate(command(['/usr/bin/python3','-B','/opt/stream-v4-relay-public/current/relay_public.py']),time.time()),
      FILES[5]:lambda:twitch.validate(command(['/usr/bin/python3','-B','/opt/stream-v4-twitch-public/current/twitch_public.py'])),
      FILES[6]:lambda:sli.validate(command(['/usr/bin/python3','-B','/opt/stream-v4-twitch-sli-public/current/twitch_sli_public.py']),time.time()),
      FILES[7]:lambda:egress.validate(local_url('http://127.0.0.1:9119/public.json'),time.time())}
    old=(previous or {}).get('artifacts',{})
    artifacts={};errors={}
    for name,make in builders.items():
        try:
            start=timing.capture();value=make();artifacts[name]=wrap(value,start,timing.capture())
        except (OSError,ValueError,TypeError,KeyError,RuntimeError,subprocess.SubprocessError)as e:
            errors[name]=type(e).__name__+(':'+str(e) if isinstance(e,ValueError) else '')
            if name in old:artifacts[name]=old[name]
    return {'schema':SCHEMA,'artifacts':artifacts,'errors':errors}


def read_packet(path,nonce):
    start=clock.capture();packet=upload_samples.read_json(path,MAX_BYTES)
    if packet.get('schema')!=SCHEMA or set(packet.get('artifacts',{}))-set(FILES):raise ValueError('bundle_invalid')
    return {'schema':SCHEMA,'clock':clock.envelope(nonce,start),'artifacts':packet['artifacts']}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('mode',choices=('build','read'))
    p.add_argument('--state-root',type=Path);p.add_argument('--output',type=Path,required=True);p.add_argument('--nonce')
    p.add_argument('--report-root',type=Path,default=ROOT)
    a=p.parse_args()
    if a.mode=='read':print(json.dumps(read_packet(a.output,a.nonce),allow_nan=False));return
    old=upload_samples.read_json(a.output,MAX_BYTES) if a.output.exists() else None
    packet=build(a.state_root,old,report_root=a.report_root);upload_samples.write_json(a.output,packet)
    print(json.dumps({'artifacts':sorted(packet['artifacts']),'errors':packet['errors']}))
    if packet['errors']:raise SystemExit(1)

if __name__=='__main__':main()
