"""Current upload-source recovery evidence; historical SLO gates stay unchanged."""
from __future__ import annotations
import json
import math
import os
import stat
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener
from stream_core.common import upload_samples, upload_prometheus_source

RECOVERY_SECONDS = 900
FRESH_SECONDS = 180
TAIL_BYTES = 262144
QUERY = 'stream_v3_upload_latest_age_seconds{job="stream_v3_arena_monitor",instance="127.0.0.1:9108"} and on(job,instance) (up{job="stream_v3_arena_monitor",instance="127.0.0.1:9108"} == 1)'

def stamp(value):
    d=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError()
    return d.timestamp()

def _raw_times(path: Path, now: int) -> list[float]:
    """Read at most one bounded tail, preserving the producer timestamps."""
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):raise ValueError()
        f.seek(0,os.SEEK_END);size=f.tell();offset=max(0,size-TAIL_BYTES);f.seek(offset)
        raw=f.read(TAIL_BYTES)
    lines=raw.splitlines()
    if offset:lines=lines[1:]
    times=[]
    for line in lines:
        try:
            row=json.loads(line)
            if row.get('kind')!='tcp_send_sample':continue
            t=stamp(row.get('ts_utc'))
            value=row.get('mbps',row.get('send_mbps'))
            if type(value) not in (float,int) or not math.isfinite(value) or value<0:continue
        except (ValueError,TypeError,AttributeError):continue
        if t>now:raise ValueError()
        if now-RECOVERY_SECONDS-FRESH_SECONDS<=t<=now:times.append(t)
    return times

def _raw_summary(times: list[float], now: int) -> dict:
    result={'available':False,'fresh':False,'sustained':False,'latest_age_seconds':None}
    if not times:return result
    times=sorted(set(times));age=now-times[-1];fresh=age<=FRESH_SECONDS
    sustained=(fresh and times[0]<=now-RECOVERY_SECONDS and
               all(b-a<=FRESH_SECONDS for a,b in zip(times,times[1:])))
    result.update(available=True,fresh=fresh,sustained=sustained,latest_age_seconds=age)
    return result

def raw_health(path: Path, now: int) -> dict:
    independent = upload_samples.current_times(path.parent.parent, now)
    if independent is not None:
        return _raw_summary(independent, now)
    result=_raw_summary([],now)
    try:
        try:times=_raw_times(path,now)
        except FileNotFoundError:times=[]
        result=_raw_summary(times,now)
        if result['sustained']:return result
        # copytruncate moves the recovery window into .1. Read only that
        # immediate predecessor; deduplicate overlap without refreshing its age.
        try:times.extend(_raw_times(path.with_name(path.name+'.1'),now))
        except FileNotFoundError:pass
        return _raw_summary(times,now)
    except (OSError,ValueError):return _raw_summary([],now)

def fetch_prometheus(now: int, source: dict | None = None) -> dict:
    end=now//60*60
    query=urlencode({'query':QUERY if source is None else upload_prometheus_source.query(source),'start':end-RECOVERY_SECONDS,'end':end,'step':60})
    with build_opener(ProxyHandler({})).open('http://127.0.0.1:9090/api/v1/query_range?'+query,timeout=2) as response:
        raw=response.read(16385)
    if len(raw)>16384:raise ValueError()
    return json.loads(raw)

def prom_health(payload: dict, now: int, source: dict | None = None) -> dict:
    result={'available':False,'fresh':False,'sustained':False}
    try:
        if payload.get('status')!='success' or payload['data'].get('resultType')!='matrix':return result
        rows=payload['data']['result']
        if len(rows)!=1:return result
        metric=rows[0]['metric']
        expected = ({'job':'stream_v3_arena_monitor','instance':'127.0.0.1:9108'}
                    if source is None else upload_prometheus_source.expected_labels(source))
        if any(metric.get(key)!=value for key,value in expected.items()):return result
        end=now//60*60;expected=set(range(end-RECOVERY_SECONDS,end+1,60));values={}
        for t,value in rows[0]['values']:
            t=float(t);age=float(value)
            if not math.isfinite(t) or not math.isfinite(age) or t!=int(t) or t>now or age<0:return result
            if t in values:return result
            if t in expected:values[int(t)]=age
        if end not in values:return result
        fresh=values[end]<=FRESH_SECONDS
        result.update(available=True,fresh=fresh,sustained=(set(values)==expected and all(a<=FRESH_SECONDS for a in values.values())))
    except (KeyError,TypeError,ValueError,OverflowError):pass
    return result

def current_upload_source(state_root: Path, now: int) -> dict:
    raw=raw_health(state_root/'logs/fast_recovery_events.jsonl',now)
    # Current measurement and durable history are different failure domains.
    # The combined qualification below still requires both original windows.
    legacy=False
    try:
        source=upload_prometheus_source.selected(state_root)
        legacy=source is None
        prom=prom_health(fetch_prometheus(now,source),now,source)
    except (OSError,ValueError):prom={'available':False,'fresh':False,'sustained':False}
    if not raw['available']:status,reason='UNKNOWN','current_raw_upload_sample_unavailable'
    elif not raw['fresh']:status,reason='FAILED','current_raw_upload_sample_stale'
    elif not prom['available']:status,reason='UNKNOWN','current_upload_prometheus_evidence_unavailable'
    elif not prom['fresh']:status,reason='FAILED','current_upload_prometheus_sample_stale'
    elif not (raw['sustained'] and prom['sustained']):status,reason='RECOVERING','upload_source_recovery_window_incomplete'
    else:status,reason='RECOVERED','raw_and_prometheus_fresh_for_15m'
    result={'status':status,'reason':reason}
    if not legacy:result.update(measurement=prom,history=raw)
    return result
