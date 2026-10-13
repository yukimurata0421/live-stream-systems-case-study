"""Fixed read-only OCI probe. Emits counters and resources, never URLs/errors."""
from __future__ import annotations
import json
import hashlib
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import build_opener, ProxyHandler

API = 'http://127.0.0.1:19998'
DAILY = Path('/var/lib/stream-twitch-daily/status.json')


def api(path):
    with build_opener(ProxyHandler({})).open(API+path, timeout=2) as r:
        raw = r.read(65537)
    if len(raw)>65536:raise ValueError('BOUNDED_RESPONSE_REQUIRED')
    return json.loads(raw)


def counters():
    values=[int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
    return sum(values),values[3]+values[4]


def observe():
    a,ai=counters();time.sleep(.2);b,bi=counters()
    mem={k:int(v.split()[0])*1024 for k,v in (x.split(':',1) for x in Path('/proc/meminfo').read_text().splitlines())}
    disk=os.statvfs('/')
    props=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show','stream-v3-oci-relay.service','-p','ActiveState','-p','MainPID','-p','NRestarts'],text=True,timeout=3).splitlines())
    result={'schema':'stream.relay_raw.v1','host_id':'relay-host','machine_id_sha256':hashlib.sha256(Path('/etc/machine-id').read_bytes()).hexdigest(),'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'observed_at':datetime.now(timezone.utc).isoformat(),'monotonic_seconds':time.clock_gettime(time.CLOCK_BOOTTIME),
            'vm':{'cpu_percent':round(100*(1-(bi-ai)/(b-a)),2) if b>a else None,'cpu_count':os.cpu_count(),
                  'memory_total_bytes':mem['MemTotal'],'memory_used_bytes':mem['MemTotal']-mem['MemAvailable'],
                  'disk_total_bytes':disk.f_blocks*disk.f_frsize,'disk_available_bytes':disk.f_bavail*disk.f_frsize,
                  'uptime_seconds':float(Path('/proc/uptime').read_text().split()[0])},
            'relay':{'active':props['ActiveState']=='active','pid':int(props['MainPID']),'restart_count':int(props['NRestarts'])},
            'input':{'available':False,'ready':None,'bytes':None,'connection_id':None},'destinations':{},
            'daily':{'state':'UNKNOWN','observed_at':None,'next_renewal_at':None,'offline_wait_seconds':None}}
    try:
        path=api('/v3/paths/get/relay')
        result['input']={'available':True,'ready':path['ready'] is True,'bytes':path['inboundBytes'],'connection_id':path['source']['id'] if path.get('source') else None}
        rows=api('/v3/paths/forward-dests/list?path=relay')['items']
        for pos,name in [(1,'youtube'),(2,'twitch')]:
            matches=[x for x in rows if x['pos']==pos]
            if len(matches)!=1:continue
            x=matches[0]
            result['destinations'][name]={'id':x['id'],'state':x['state'] if x['state'] in ['forwarding','idle','connecting'] else 'unknown','bytes':x['outboundBytes']}
    except Exception:pass
    try:
        if DAILY.stat().st_size>16384:raise ValueError()
        d=json.loads(DAILY.read_text())
        allowed={'TRACKING','WAITING_CREDENTIALS','WAIT_STOP','WAIT_OFFLINE','WAIT_START','VERIFYING','CANCELLED','RENEWED','HOLD','UNKNOWN'}
        result['daily']={k:d.get(k) for k in ['observed_at','next_renewal_at','offline_wait_seconds']}
        result['daily']['state']=d['state'] if d.get('state') in allowed else 'UNKNOWN'
    except Exception:pass
    return result


if __name__=='__main__':
    try:print(json.dumps(observe(),allow_nan=False,separators=(',',':')))
    except Exception:print('{"schema":"stream.relay_raw.v1","error":"OBSERVATION_UNAVAILABLE"}')
