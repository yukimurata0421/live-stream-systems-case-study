#!/usr/bin/env python3
"""Publish only arena-generated projections, with a boot-bound freshness proof."""
from __future__ import annotations
from datetime import datetime,timezone
import json
import math
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import clock_contract as clock

PUBLIC=Path(__file__).resolve().parents[1]/'public'
FILES={'stream-v3-prometheus.json':'stream-v3-prometheus-public.v1',
       'stream-v3-loki.json':'stream-v3-loki-public.v1',
       'reliability-indicators.json':'stream-v3-reliability-public.v6',
       'clock-observation.json':'stream.clock_observation_public.v1',
       'relay-observation.json':'stream.relay_public.v1',
       'twitch-observation.json':'stream-v3-twitch-public.v1',
       'twitch-sli.json':'stream.twitch_sli_public.v1',
       'oci-egress.json':'stream.oci_egress_public.v1'}
SCHEMA='stream.public_observation_bundle.v1'


def timestamp(v):
    if isinstance(v,str):return datetime.fromisoformat(v.replace('Z','+00:00')).timestamp()
    if type(v) not in (int,float) or not math.isfinite(v):raise ValueError('timestamp_invalid')
    return v


def validate(name,value):
    if not isinstance(value,dict) or value.get('schema')!=FILES[name]:raise ValueError('schema_invalid')
    stamp=timestamp(value['generated_at'])
    if name=='clock-observation.json':
        from collect_clock_public import validate as check
        check(value,stamp)
    elif name=='relay-observation.json':
        from collect_relay_public import validate as check
        check(value,stamp)
    elif name=='twitch-observation.json':
        from collect_twitch_public import validate as check
        check(value,now=stamp)
    elif name=='twitch-sli.json':
        from collect_twitch_sli import validate as check
        check(value,stamp)
    elif name=='oci-egress.json':
        from collect_oci_egress_public import validate as check
        check(value,stamp)
    # The remaining schemas are constructed by the owner's fixed sanitizers.
    # No raw query result, internal file, command or output name is selectable.
    if len(json.dumps(value,allow_nan=False).encode())>1024*1024:raise ValueError('artifact_too_large')
    return value


def accept(packet,*,nonce,sent,received,machine):
    if not isinstance(packet,dict) or set(packet)!= {'schema','clock','artifacts'} or packet['schema']!=SCHEMA:raise ValueError('bundle_invalid')
    artifacts=packet['artifacts']
    if not isinstance(artifacts,dict) or set(artifacts)-set(FILES):raise ValueError('artifact_not_allowlisted')
    result={};errors={}
    for name,item in artifacts.items():
        try:
            if set(item)!={'value','boot_id','generated_mono'}:raise ValueError('receipt_invalid')
            age=clock.monotonic_source_age(packet['clock'],nonce,sent,received,machine=machine,
                boot_id=item['boot_id'],source_mono=item['generated_mono'],max_age=180)
            value=validate(name,item['value']).copy()
            # Presentation timestamps express the proved original age on the
            # publisher clock; fetching the same source never renews its age.
            stamp=received['wall']-age
            value['generated_at']=(datetime.fromtimestamp(stamp,timezone.utc).isoformat()
                                   if isinstance(value['generated_at'],str) else stamp)
            if 'generated_at_iso' in value:value['generated_at_iso']=datetime.fromtimestamp(stamp,timezone.utc).isoformat()
            result[name]=value
        except (ValueError,TypeError,KeyError,OverflowError):errors[name]='source_unverifiable'
    return result,errors


def write(path,value):
    fd,name=tempfile.mkstemp(prefix='.observation-',dir=path.parent)
    try:
        with os.fdopen(fd,'w')as out:
            os.fchmod(out.fileno(),0o644);json.dump(value,out,allow_nan=False,separators=(',',':'));out.write('\n');out.flush();os.fsync(out.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)


def main():
    nonce=secrets.token_hex(16);sent=clock.capture()
    command=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','arena-server','python3','-I','-B',
      '/opt/stream-v3-public-projection/current/ops/scripts/stream_v3_public_projection.py','read',
      '--output','/var/lib/stream-v3-public-projection/bundle.json','--nonce',nonce]
    try:
        r=subprocess.run(command,capture_output=True,timeout=12);received=clock.capture()
        if r.returncode or len(r.stdout)>2*1024*1024:raise ValueError('fetch_failed')
        machine=json.loads(Path(__file__).with_name('clock-peers.json').read_text())['arena_machine_sha256']
        values,errors=accept(json.loads(r.stdout),nonce=nonce,sent=sent,received=received,machine=machine)
        PUBLIC.mkdir(parents=True,exist_ok=True)
        for name,value in values.items():write(PUBLIC/name,value)
        print(json.dumps({'source':'arena_public_bundle','updated':sorted(values),'retained':sorted(set(FILES)-set(values)),'errors':errors}))
    except (OSError,ValueError,TypeError,KeyError,subprocess.SubprocessError):
        print(json.dumps({'source':'arena_public_bundle','updated':[],'retained':sorted(FILES),'error':'fetch_or_contract_invalid'}))
    # Existing files keep their previous age; a partial refresh is not recovery.
    return 0

if __name__=='__main__':raise SystemExit(main())
