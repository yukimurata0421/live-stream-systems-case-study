#!/usr/bin/env python3
"""Add clock-quality warnings to the existing notification loop and reuse its delivery baseline."""
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import time
import urllib.request
from urllib.parse import urlencode

NAMES={'ViewerClockObservationUnavailable','ViewerClockObserverUnavailable','TwitchThumbnailObservationUnavailable','TwitchThumbnailWorkerUnavailable','TwitchThumbnailStale'}
TITLES={'oci_relay':'OCI return-feed clock observation unavailable',
        'youtube_fallback':'Direct YouTube fallback clock observation unavailable',
        'observer':'Clock observer status unavailable',
        'twitch_thumbnail':'Twitch thumbnail clock observation unavailable'}


def fetch_recovery(now):
    """Absent/stale series never count as recovery; this is not delivery proof."""
    try:
        query='{__name__=~"stream_v3_monitor_probe_recovery_unixtime|stream_v3_twitch_thumbnail_recovery_unixtime",job="stream_v3_clock_observer"}'
        # Query evaluation normally occurs just after the caller's `now`.
        # Pin it to that instant so a millisecond of HTTP latency is not
        # mistaken for a future-dated recovery proof.
        with urllib.request.urlopen('http://127.0.0.1:9090/api/v1/query?'+urlencode({'query':query,'time':now}),timeout=3) as response:
            raw=response.read(65537)
        if len(raw)>65536:return {}
        value=json.loads(raw)
        if value.get('status')!='success' or value['data'].get('resultType')!='vector':return {}
        result={}
        for row in value['data']['result']:
            target=row['metric'].get('target');sample,value=row['value'];stamp=float(value)
            limit=180 if target=='twitch_thumbnail' else 60
            # Prometheus rounds evaluation timestamps to milliseconds.
            if target in ('oci_relay','youtube_fallback','twitch_thumbnail') and math.isfinite(stamp) and 0<=now-stamp<=limit and -1<=now-float(sample)<=60:
                if target in result:return {}  # Ambiguous scrape identity fails closed.
                result[target]=stamp
        if {'oci_relay','youtube_fallback'}.issubset(result):result['observer']=min(result[t] for t in ('oci_relay','youtube_fallback'))
        return result
    except (OSError,ValueError,KeyError,TypeError,OverflowError):return {}


def plan_clock(alerts,state,now,*,repeat_sec,recovery):
    messages=[];current={}
    for alert in alerts:
        labels=alert.get('labels') or {};name=labels.get('alertname');target=labels.get('target','observer')
        if name not in NAMES or alert.get('state')!='firing' or target not in TITLES:continue
        key=name+'[target='+target+']';prior=state.get(key)
        title='Twitch thumbnail clock is over fourteen minutes old' if name=='TwitchThumbnailStale' else TITLES[target]
        if prior is None or now-prior['last_sent']>=repeat_sec:
            content=(
                '[warning] '+title+'\nNo valid clock observation or observer status for over five minutes.'
                '\nScope: delivery-host -> OCI relay -> delivery-host, or the direct-upload monitoring branch. Public-side reception is unverified.'
                '\nCheck delivery using the existing sender and API observations.')
            if target=='twitch_thumbnail':
                meaning=('The clock in the official thumbnail is over fourteen minutes old.' if name=='TwitchThumbnailStale'
                         else 'Observer publication is over five minutes old or its metric has been absent for ten minutes.' if name=='TwitchThumbnailWorkerUnavailable'
                         else 'No valid official thumbnail clock observation for over five minutes. The first clock of a new stream has a startup allowance of at most twelve minutes.')
                content='[warning] '+title+'\n'+meaning+'\nScope: the Twitch API thumbnail image. Viewer-side video playback is unverified.Check delivery using the existing sender and API observations.'
            messages.append({'key':key,'channel':'discord','content':content})
            current[key]={'name':name,'target':target,'title':title,'since':prior['since'] if prior else now,'last_sent':now,'channel':'discord'}
        else:current[key]={k:v for k,v in prior.items() if k!='clock_recovery_candidate_at'}
    for key,prior in state.items():
        if key in current:continue
        stamp=recovery.get(prior.get('target'))
        candidate=prior.get('clock_recovery_candidate_at')
        if stamp is None:
            current[key]={k:v for k,v in prior.items() if k!='clock_recovery_candidate_at'}
        elif candidate is None or stamp-candidate<20:
            current[key]={**prior,'clock_recovery_candidate_at':stamp if candidate is None else candidate}
        else:
            messages.append({'key':key,'channel':'discord','content':'[resolved] Clock observation received again\nTarget: '+prior.get('target','observer')+
                             '\nConfirmed by two distinct fresh observations at least 20 seconds apart. This is not delivery recovery.'})
    return messages,current


def main():
    path=Path(os.environ['CLOCK_NOTIFY_BASELINE'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=os.environ['CLOCK_NOTIFY_BASELINE_SHA256']:
        raise ValueError('notification_baseline_changed')
    spec=importlib.util.spec_from_file_location('clock_notify_baseline',path)
    baseline=importlib.util.module_from_spec(spec);spec.loader.exec_module(baseline)
    original=baseline.plan
    def plan(alerts,state,now,**kwargs):
        clock_state={k:v for k,v in state.items() if v.get('name') in NAMES}
        other={k:v for k,v in state.items() if v.get('name') not in NAMES}
        messages,current=original([a for a in alerts if (a.get('labels') or {}).get('alertname') not in NAMES],other,now,**kwargs)
        own_messages,own_current=plan_clock(alerts,clock_state,now,repeat_sec=kwargs['repeat_sec'],recovery=fetch_recovery(now))
        return messages+own_messages,{**current,**own_current}
    baseline.plan=plan
    return baseline.main()


if __name__=='__main__':raise SystemExit(main())
