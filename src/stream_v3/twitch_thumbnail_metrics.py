"""Fixed-label thumbnail observations kept separate from player reception."""
from datetime import datetime
import json
import math
from pathlib import Path


def age(value,now):
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        result=now-stamp.timestamp()
        return max(0.,result) if stamp.tzinfo and math.isfinite(result) and result>=-2 else None
    except (AttributeError,TypeError,ValueError,OverflowError):return None


class PublicationReader:
    """Retain the last publication timestamp across read failures so gaps stay observable."""
    def __init__(self,*,started_at):self.last_cycle_epoch=started_at

    def read(self,path,*,now):
        try:
            with Path(path).open('rb') as file:raw=file.read(65537)
            if len(raw)>65536:return {}
            value=json.loads(raw)
        except (OSError,ValueError):return {}
        if not isinstance(value,dict) or value.get('schema')!='stream_v3.twitch_thumbnail_observation.v1':return {}
        checked=age(value.get('checked_at_utc'),now)
        if checked is not None:self.last_cycle_epoch=now-checked
        return value


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
            'state_readable':int(bool(row)),
            'capture_attempted':int(fresh and row.get('capture_attempted') is True),
            'request_deferred':int(fresh and row.get('clock_state') in ('WAITING_POLL','WAITING_RETRY')),
            'ocr_attempted':int(fresh and row.get('ocr_attempted') is True),
            'ocr_binary_retried':int(fresh and row.get('ocr_attempted') is True and row.get('ocr_binary_retried') is True),
            'ocr_interpolation_retried':int(fresh and row.get('ocr_attempted') is True and row.get('ocr_interpolation_retried') is True),
            'ocr_contrast_retried':int(fresh and row.get('ocr_attempted') is True and row.get('ocr_contrast_retried') is True),
            'image_duplicate':int(captured and row.get('image_duplicate') is True),
            'progress_observed':int(verified and progress_age is not None and progress_age<=600),
            'forward_flowing':int(row['forward_flowing']) if forward_fresh and type(row.get('forward_flowing')) is bool else -1,
            'recovery_unixtime':(now-checked) if verified and frame_age is not None and frame_age<=360 else 0}
    valid_age=age(row.get('last_valid_observation_at_utc') or row.get('monitoring_started_at_utc'),now)
    required_age=age(row.get('observation_required_since_utc'),now)
    if required_age is not None:valid_age=required_age if valid_age is None else min(valid_age,required_age)
    startup_age=age(row.get('startup_grace_started_at_utc'),now)
    values['startup_grace_remaining_seconds']=max(0.,720-startup_age) if live and startup_age is not None and row.get('stream_clock_observed') is not True else 0.
    if valid_age is not None:values['seconds_since_last_valid_observation']=valid_age
    if frame_age is not None and verified:values['frame_age_seconds']=frame_age
    values['quality_history_available']=int(valid_age is not None)
    if cycle_age is not None:values['seconds_since_last_cycle']=cycle_age
    for suffix,value in values.items():writer.metric('stream_v3_twitch_thumbnail_'+suffix,value,labels=labels)
    for suffix in ('unique_samples_total','ocr_observations_total','ocr_binary_retries_total','ocr_interpolation_retries_total',
                   'ocr_binary_accepted_total','ocr_interpolation_accepted_total',
                   'ocr_contrast_retries_total','ocr_contrast_accepted_total'):
        value=row.get(suffix)
        if type(value) is int and value>=0:writer.metric('stream_v3_twitch_thumbnail_'+suffix,value,labels=labels,metric_type='counter')
    methods=('none','cached','original','contrast','binary','interpolation')
    failures=('none','not_attempted','invalid_time','low_confidence','pair_mismatch','timeout','process_failed',
              'transform_failed','value_changed','timestamp_binding','time_regressed','previous_stream')
    method=row.get('ocr_method') if fresh and row.get('ocr_method') in methods else 'none'
    failure=row.get('ocr_failure_reason') if fresh and row.get('ocr_failure_reason') in failures else 'not_attempted'
    for value in methods:writer.metric('stream_v3_twitch_thumbnail_ocr_method',int(value==method),labels={**labels,'method':value})
    for value in failures:writer.metric('stream_v3_twitch_thumbnail_ocr_failure_reason',int(value==failure),labels={**labels,'reason':value})
    statuses=('not_required','saved','reused','capacity_limited','roi_unavailable','storage_unavailable')
    status=row.get('evidence_status') if fresh and row.get('evidence_status') in statuses else 'not_required'
    for value in statuses:writer.metric('stream_v3_twitch_thumbnail_evidence_status',int(value==status),labels={**labels,'status':value})
    states=('FRESH','AGING','STALE_SUSPECT','STALE_CORRELATED','UNKNOWN','SOURCE_OFFLINE','WAITING_NEW_STREAM_THUMBNAIL','WAITING_POLL','WAITING_RETRY')
    state='SOURCE_OFFLINE' if fresh and row.get('api_live') is False else 'UNKNOWN'
    if live and row.get('clock_state') in states[6:]:state=row['clock_state']
    if verified and frame_age is not None:
        state='FRESH' if frame_age<=360 else 'AGING' if frame_age<=840 else 'STALE_CORRELATED' if frame_age>900 and forward_fresh and row.get('forward_flowing') is True else 'STALE_SUSPECT'
    for value in states:writer.metric('stream_v3_twitch_thumbnail_freshness_state',int(state==value),labels={**labels,'state':value})
