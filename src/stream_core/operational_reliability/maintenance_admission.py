"""Notification-only counterfactuals. Formal reports and raw evidence stay intact."""
from __future__ import annotations

import hashlib
import json
import math

SCHEMA = 'stream.notification_maintenance_assessment.v1'
IDS = {'audio_correctness':'reliability:audio_correctness_multi_window_burn',
       'youtube_input_quality':'reliability:youtube_input_quality_coverage_or_freshness'}
RETURN_GRACE = 300  # Existing notification/bootstrap and return-verification horizon.
ASSESSMENT_TTL = 600  # Five-minute evaluator, timer accuracy and collection runtime.


def finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def raw_digest(payload):
    return hashlib.sha256(json.dumps({k:payload.get(k) for k in (
        'checked_at_utc','fast_feedback','multi_window_burn_alerts','formal_sli')},
        sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def intervals(plan, *, end):
    if (not isinstance(plan,dict) or plan.get('schema')!='stream.notification_maintenance_plan.v1'
            or not finite(plan.get('observed_epoch'))
            or not isinstance(plan.get('stops'),list) or len(plan['stops'])>18):
        raise ValueError('unverifiable_maintenance_plan')
    qualified=plan.get('qualified_epoch',plan['observed_epoch'])
    if (not finite(qualified) or not -1<=qualified-plan['observed_epoch']<=35
            or not -1<=qualified-end<=180):
        raise ValueError('maintenance_plan_expired')
    result=[]
    for row in plan['stops']:
        start,stop=row['source_stopped_at'],row['source_resumed_at']
        if not finite(start) or not isinstance(row.get('session_id'),str) or type(row.get('generation')) is not int:
            raise ValueError('unbound_stop')
        if stop is None:
            if plan.get('active_verified') is not True:raise ValueError('unconfirmed_active_stop')
            stop=qualified+1  # Include the current grid point in a verified open stop.
        if not finite(stop) or not 0<stop-start<=86400 or stop>qualified+1:
            raise ValueError('invalid_stop_interval')
        if start<=end and stop+RETURN_GRACE>=end-21600:
            result.append((start,stop,row['source_resumed_at'] is None))
    return result


def outside(stamp, spans):
    return not any(a<=stamp<b for a,b in spans)


def normal_ranges(start,end,spans):
    result=[];cursor=start
    for a,b in sorted(spans):
        a=max(start,math.ceil(a));b=min(end,math.ceil(b))
        if b<=cursor:continue
        if a>cursor:result.append((cursor,a))
        cursor=max(cursor,b)
    if cursor<end:result.append((cursor,end))
    return result


def evaluate(metrics, events, plan, *, end, step=60):
    from stream_core.cli_support import sli_report as s
    spans=intervals(plan,end=end)
    result={'schema':SCHEMA,'end_epoch':end,'source_sample_id':plan['source_sample_id'],
            'stops':plan['stops'],'families':{},'excluded_ids':[]}
    if not spans:return result
    audio=metrics.get('audio_ok',{});ages=metrics.get('audio_evidence_age_seconds',{})
    available=metrics.get('audio_evidence_available',{})
    # Extend a return gap only as far as the first fresh, healthy post-resume
    # audio capture, and never beyond the existing 300-second startup horizon.
    audio_spans=[]
    for a,b,active in spans:
        good=[t for t,v in audio.items() if not active and b<=t<=min(end,b+RETURN_GRACE)
              and v>=1 and available.get(t)==1 and finite(ages.get(t))
              and 0<=ages[t]<=s.AUDIO_SOURCE_MAX_AGE_SEC and t-ages[t]>=b]
        audio_spans.append((a,min(good) if good else b))
    quality_spans=[]
    for a,b,active_stop in spans:
        good=[]
        for t,row in events:
            if active_stop or not b<=t<=min(end,b+RETURN_GRACE):continue
            c=s.classify_input_quality_sample(row,now_ts=t,max_age_sec=s.INPUT_QUALITY_MAX_SAMPLE_SPAN_SEC)
            if c.get('eligible') is True and c.get('good') is True and c.get('checked_ts',0)>=b:
                good.append(t)
        quality_spans.append((a,min(good) if good else b))
    active=plan.get('active_verified') is True
    audio_windows={}
    for label,seconds in [('15m',900),('1h',3600),('6h',21600)]:
        grid=[t for t in range(end-seconds,end+1,step) if outside(t,audio_spans)]
        valid=[t for t in grid if finite(audio.get(t)) and available.get(t)==1
               and finite(ages.get(t)) and 0<=ages[t]<=s.AUDIO_SOURCE_MAX_AGE_SEC]
        bad=sum(audio[t]<1 for t in valid)
        coverage=len(valid)/len(grid)*100 if grid else None
        audio_windows[label]={'expected_points':len(grid),'observed_points':len(valid),
            'bad_points':bad,'coverage_pct':coverage,'excluded_points':seconds//step+1-len(grid),
            'normal':(bool(grid) and len(valid)==len(grid) and bad==0) or (not grid and active)}
    latest=max(audio,default=0)
    current_audio=(0<=end-latest<=max(90,step*2) and audio.get(latest,0)>=1
        and available.get(latest)==1 and finite(ages.get(latest))
        and 0<=ages[latest]<=s.AUDIO_SOURCE_MAX_AGE_SEC)
    audio_ok=all(w['normal'] for w in audio_windows.values()) and (active or current_audio)
    result['families']['audio_correctness']={'normal':audio_ok,'windows':audio_windows,
        'current_normal':current_audio,'excluded_intervals':audio_spans}
    ranges=normal_ranges(end-3600,end,quality_spans)
    rows=[s.youtube_input_quality_measurements(events,start_ts=a,end_ts=b) for a,b in ranges]
    total=sum(b-a for a,b in ranges);covered=sum(r['timeline_covered_seconds'] for r in rows)
    eligible=sum(r['eligible_seconds'] for r in rows);bad=sum(r['bad_seconds'] for r in rows)
    coverage=covered/total*100 if total else None
    cross=s.current_input_quality_crosscheck(events,metrics.get('youtube_input_quality_eligible',{}),
        metrics.get('youtube_input_quality_good',{}),end_ts=end,step_sec=step)
    current_quality=(cross.get('source_disagreement_current') is False
        and all(cross.get(k,{}).get('available') is True and cross[k].get('eligible') is True
                and cross[k].get('good') is True for k in ('raw_current','prometheus_current')))
    quality_ok=((bool(total) and eligible==total and covered==total and bad==0)
                or (total==0 and active)) and (active or current_quality)
    result['families']['youtube_input_quality']={'normal':quality_ok,'normal_seconds':total,
        'covered_seconds':covered,'eligible_seconds':eligible,'bad_seconds':bad,'coverage_pct':coverage,
        'current_normal':current_quality,'excluded_intervals':quality_spans}
    for family,row in result['families'].items():
        if row['normal']:result['excluded_ids'].append(IDS[family])
    return result


def bind(payload, assessment):
    if assessment is not None:
        payload['notification_maintenance']={**assessment,'raw_digest':raw_digest(payload)}
    return payload


def permits(payload, ident, *, now):
    if ident not in IDS.values() or not isinstance(payload,dict):return False
    try:
        a=payload['notification_maintenance']
        family=next(k for k,v in IDS.items() if v==ident)
        return (payload.get('formal_sli') is False and payload.get('status')=='ok' and a.get('schema')==SCHEMA
            and finite(a.get('end_epoch')) and 0<=now-a['end_epoch']<=ASSESSMENT_TTL
            and a.get('raw_digest')==raw_digest(payload)
            and ident in a.get('excluded_ids',[]) and a['families'][family]['normal'] is True)
    except (KeyError,TypeError,ValueError,OverflowError,StopIteration,AttributeError):
        return False
