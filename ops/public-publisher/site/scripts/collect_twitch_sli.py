"""Publish only arena's allowlisted SLI projection; keep original age on failure."""
import json
import math
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

PUBLIC=Path(__file__).resolve().parents[1]/'public'
DEFINITIONS={
 'provider_live':('Twitch LIVE availability','Helix Get Streams: LIVE / known provider observations.',99.0),
 'api_measurement':('API observation availability','Successful fresh Helix observation minutes / all expected minutes; includes collection gaps.',99.0),
 'relay_flow':('Twitch relay continuity','OCI forward counter progressing / known relay observations.',99.0),
 'daily_renewal':('Daily session renewal','OFFLINE → new LIVE stream ID and start time, within 600 s of 00:00 JST.',100.0)}
# Accept the exact prior wording during an upgrade or rollback; arbitrary text is rejected.
API_DEFINITIONS={DEFINITIONS['api_measurement'],
 ('API measurement coverage','Successful fresh Helix observations / all expected minutes.',99.0)}

def keys(v,wanted):
    if type(v) is not dict or set(v)!=set(wanted):raise ValueError('UNEXPECTED_PUBLIC_FIELD')

def timestamp(v):
    if type(v) is not str or len(v)>40:raise ValueError()
    d=datetime.fromisoformat(v.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError()
    return d.timestamp()

def number(v,upper=1e9,nullable=False):
    if nullable and v is None:return
    if type(v) not in (float,int) or not math.isfinite(v) or not 0<=v<=upper:raise ValueError()

def count(v,upper):
    if type(v) is not int or not 0<=v<=upper:raise ValueError()

def validate(v,now):
    keys(v,['schema','generated_at','history_started_at','cadence_sec','freshness_limit_sec','primary_window','coverage_gate_percent','planned_downtime_included','windows','timeline'])
    if v['schema']!='stream.twitch_sli_public.v1' or not 0<=now-timestamp(v['generated_at'])<=180:raise ValueError()
    if timestamp(v['history_started_at'])>now or v['cadence_sec']!=60 or v['freshness_limit_sec']!=180 or v['primary_window']!='7d' or v['coverage_gate_percent']!=99.0 or v['planned_downtime_included'] is not True:raise ValueError()
    if type(v['windows']) is not list or len(v['windows'])!=2:raise ValueError()
    for w,(name,length) in zip(v['windows'],[('24h',1440),('7d',10080)]):
        keys(w,['id','start_at','end_at','full_history','metrics'])
        if w['id']!=name or timestamp(w['end_at'])-timestamp(w['start_at'])!=length*60 or type(w['full_history']) is not bool:raise ValueError()
        if type(w['metrics']) is not list or len(w['metrics'])!=4:raise ValueError()
        for m,kind in zip(w['metrics'],DEFINITIONS):
            base=['id','label','definition','target_percent','value_percent','assessment']
            extra=['good_events','bad_events','unknown_events','pending_events','expected_events'] if kind=='daily_renewal' else ['conservative_percent','coverage_percent','good_minutes','bad_minutes','unknown_minutes','missing_minutes','expected_minutes','error_budget_minutes','confirmed_budget_used_percent']
            keys(m,base+extra)
            definitions=API_DEFINITIONS if kind=='api_measurement' else {DEFINITIONS[kind]}
            if m['id']!=kind or (m['label'],m['definition'],m['target_percent']) not in definitions or m['assessment'] not in ['COLLECTING','UNKNOWN','MET','BREACHED','NO_DUE']:raise ValueError()
            number(m['value_percent'],100,True)
            if kind=='daily_renewal':
                for k in extra:count(m[k],8)
                if sum(m[k] for k in extra[:-1])!=m['expected_events']:raise ValueError()
            else:
                for k in ['conservative_percent','coverage_percent']:number(m[k],100)
                for k in ['error_budget_minutes','confirmed_budget_used_percent']:number(m[k])
                for k in ['good_minutes','bad_minutes','unknown_minutes','missing_minutes','expected_minutes']:count(m[k],length)
                if m['expected_minutes']!=length or sum(m[k] for k in ['good_minutes','bad_minutes','unknown_minutes','missing_minutes'])!=length:raise ValueError()
    if type(v['timeline']) is not list or len(v['timeline'])!=24:raise ValueError()
    for b in v['timeline']:
        keys(b,['start_at','good','bad','unknown','missing']);timestamp(b['start_at'])
        for k in ['good','bad','unknown','missing']:count(b[k],60)
        if sum(b[k] for k in ['good','bad','unknown','missing'])!=60:raise ValueError()
    return v

def main():
    path=PUBLIC/'twitch-sli.json'
    try:
        r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','arena-server','python3','-B','/opt/stream-v4-twitch-sli-public/current/twitch_sli_public.py'],capture_output=True,timeout=12)
        if r.returncode or len(r.stdout)>24576:raise ValueError()
        value=validate(json.loads(r.stdout),time.time())
    except (OSError,ValueError,TypeError,KeyError,subprocess.TimeoutExpired):
        print(json.dumps({'twitch_sli_source':'unavailable','last_artifact_retained':path.exists()}));return 0
    fd,name=tempfile.mkstemp(prefix='.sli-',dir=PUBLIC)
    try:
        with os.fdopen(fd,'w') as out:
            os.fchmod(out.fileno(),0o644);json.dump(value,out,allow_nan=False);out.write('\n');out.flush();os.fsync(out.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)
    return 0

if __name__=='__main__':raise SystemExit(main())
