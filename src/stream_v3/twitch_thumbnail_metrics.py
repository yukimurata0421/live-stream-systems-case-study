"""Fixed-label thumbnail observations kept separate from player reception."""
from datetime import datetime
import math


def age(value,now):
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        result=now-stamp.timestamp()
        return max(0.,result) if stamp.tzinfo and math.isfinite(result) and result>=-2 else None
    except (AttributeError,TypeError,ValueError,OverflowError):return None


def write_metrics(writer,observation,*,now,last_cycle_epoch=None):
    row=observation if isinstance(observation,dict) and observation.get('schema')=='stream_v3.twitch_thumbnail_observation.v1' else {}
    labels={'target':'twitch_thumbnail','scope':'twitch_provider_thumbnail'}
    checked=age(row.get('checked_at_utc'),now);fresh=checked is not None and checked<=180
    cycle_age=max(0.,now-last_cycle_epoch) if type(last_cycle_epoch) in (int,float) else checked
    api_age=age(row.get('api_collected_at_utc'),now)
    live=row.get('api_live') is True and fresh and api_age is not None and api_age<=180
    captured=live and row.get('capture_ok') is True
    verified=captured and row.get('ocr_verified') is True
    frame_age=age(row.get('display_clock_at_utc'),now)
    progress_age=age(row.get('last_progress_at_utc'),now)
    forward_age=age(row.get('forward_observed_at_utc'),now)
    forward_fresh=fresh and forward_age is not None and forward_age<=90
    values={'observation_fresh':int(fresh),'api_live':int(live),'capture_ok':int(captured),'clock_verified':int(verified),
            'image_duplicate':int(captured and row.get('image_duplicate') is True),
            'progress_observed':int(verified and progress_age is not None and progress_age<=600),
            'forward_flowing':int(row['forward_flowing']) if forward_fresh and type(row.get('forward_flowing')) is bool else -1,
            'recovery_unixtime':(now-checked) if verified and frame_age is not None and frame_age<=360 else 0}
    valid_age=age(row.get('last_valid_observation_at_utc') or row.get('monitoring_started_at_utc'),now)
    required_age=age(row.get('observation_required_since_utc'),now)
    if required_age is not None:valid_age=required_age if valid_age is None else min(valid_age,required_age)
    if valid_age is not None:values['seconds_since_last_valid_observation']=valid_age
    if frame_age is not None and verified:values['frame_age_seconds']=frame_age
    values['quality_history_available']=int(valid_age is not None)
    if cycle_age is not None:values['seconds_since_last_cycle']=cycle_age
    for suffix,value in values.items():writer.metric('stream_v3_twitch_thumbnail_'+suffix,value,labels=labels)
    value=row.get('unique_samples_total')
    if type(value) is int and value>=0:writer.metric('stream_v3_twitch_thumbnail_unique_samples_total',value,labels=labels,metric_type='counter')
    states=('FRESH','AGING','STALE_SUSPECT','STALE_CORRELATED','UNKNOWN','SOURCE_OFFLINE')
    state='SOURCE_OFFLINE' if fresh and row.get('api_live') is False else 'UNKNOWN'
    if verified and frame_age is not None:
        state='FRESH' if frame_age<=360 else 'AGING' if frame_age<=600 else 'STALE_CORRELATED' if frame_age>900 and forward_fresh and row.get('forward_flowing') is True else 'STALE_SUSPECT'
    for value in states:writer.metric('stream_v3_twitch_thumbnail_freshness_state',int(state==value),labels={**labels,'state':value})
