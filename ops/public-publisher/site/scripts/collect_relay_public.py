"""Copy only arena's credential-free relay projection into the public tree."""
import json
import math
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

PUBLIC=Path(__file__).resolve().parents[1]/'public'
ISSUES={'RELAY_STOPPED','INPUT_NOT_PROGRESSING','YOUTUBE_NOT_PROGRESSING','TWITCH_NOT_PROGRESSING','VM_MEMORY_HIGH','VM_DISK_LOW','DAILY_RENEWAL_HELD','TWITCH_OFFLINE','RELAY_OBSERVATION_UNAVAILABLE','TWITCH_API_UNAVAILABLE'}


def keys(value,expected):
    if not isinstance(value,dict) or set(value)!=set(expected):raise ValueError('UNEXPECTED_PUBLIC_FIELD')


def timestamp(v):
    if v is not None:
        if not isinstance(v,str) or len(v)>40:raise ValueError()
        t=datetime.fromisoformat(v.replace('Z','+00:00'))
        if t.tzinfo is None:raise ValueError()
        return t.timestamp()


def numeric(v,upper=1e12):
    if v is not None and (type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=upper):raise ValueError()


def validate(v,now):
    keys(v,['schema','generated_at','observed_at','status','source_fresh','vm','relay','input','destinations','daily','twitch_api','issues'])
    if v['schema']!='stream.relay_public.v1' or type(v['generated_at']) not in (int,float) or not 0<=now-v['generated_at']<=180:raise ValueError()
    timestamp(v['observed_at'])
    if v['status'] not in ['OBSERVED','UNKNOWN'] or type(v['source_fresh']) is not bool:raise ValueError()
    keys(v['vm'],['status','cpu_percent','memory_percent','disk_free_percent','uptime_seconds'])
    if v['vm']['status'] not in ['OBSERVED','UNKNOWN']:raise ValueError()
    for k in ['cpu_percent','memory_percent','disk_free_percent']:numeric(v['vm'][k],100)
    numeric(v['vm']['uptime_seconds'])
    keys(v['relay'],['status','restart_count']);numeric(v['relay']['restart_count'])
    if v['relay']['status'] not in ['RUNNING','STOPPED','UNKNOWN']:raise ValueError()
    keys(v['destinations'],['youtube','twitch'])
    for d in [v['input'],*v['destinations'].values()]:
        keys(d,['status','mbps']);numeric(d['mbps'],10000)
        if d['status'] not in ['FLOWING','STALLED','CONNECTED','DISCONNECTED','UNKNOWN']:raise ValueError()
    keys(v['daily'],['state','next_renewal_at','planned_break']);timestamp(v['daily']['next_renewal_at'])
    if v['daily']['state'] not in ['TRACKING','WAITING_CREDENTIALS','WAIT_STOP','WAIT_OFFLINE','WAIT_START','VERIFYING','CANCELLED','RENEWED','HOLD','UNKNOWN'] or type(v['daily']['planned_break']) is not bool:raise ValueError()
    keys(v['twitch_api'],['state','observed_at','source_fresh']);timestamp(v['twitch_api']['observed_at'])
    if v['twitch_api']['state'] not in ['LIVE','OFFLINE','UNKNOWN'] or type(v['twitch_api']['source_fresh']) is not bool:raise ValueError()
    if not isinstance(v['issues'],list) or len(v['issues'])>10 or any(x not in ISSUES for x in v['issues']):raise ValueError()
    return v


def main():
    path=PUBLIC/'relay-observation.json'
    try:
        r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','arena-server','python3','-B','/opt/stream-v4-relay-public/current/relay_public.py'],capture_output=True,timeout=12)
        if r.returncode or len(r.stdout)>16384:raise ValueError()
        value=validate(json.loads(r.stdout),time.time())
    except (OSError,ValueError,TypeError,KeyError,subprocess.TimeoutExpired):
        print(json.dumps({'relay_public_source':'unavailable','last_artifact_retained':path.exists()}))
        # Retain the original timestamps: a failed fetch never rejuvenates evidence.
        return 0
    fd,name=tempfile.mkstemp(prefix='.relay-',dir=PUBLIC)
    try:
        with os.fdopen(fd,'w') as out:
            os.fchmod(out.fileno(),0o644);json.dump(value,out,allow_nan=False);out.write('\n');out.flush();os.fsync(out.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)
    return 0


if __name__=='__main__':raise SystemExit(main())
