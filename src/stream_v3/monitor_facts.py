"""Combine existing native relay and image observations as facts, without control authority."""
from datetime import datetime
import math


def age(value,now):
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        if stamp.tzinfo is None:return None
        result=now-stamp.timestamp()
        return max(0.,result) if math.isfinite(result) and result>=-2 else None
    except (ValueError,TypeError,AttributeError,OverflowError):return None


def native_facts(raw,previous,*,now):
    raw=raw if isinstance(raw,dict) else {};previous=previous if isinstance(previous,dict) else {}
    elapsed_age=age(raw.get('observed_at_utc'),now)
    fresh=(raw.get('schema')=='stream_v3.relay_native_facts.v1' and raw.get('source_host_id')=='relay-host'
           and bool(raw.get('source_boot_id')) and elapsed_age is not None and elapsed_age<=90)
    same=raw.get('source_boot_id')==previous.get('source_boot_id')
    try:elapsed=float(raw['source_monotonic_seconds'])-float(previous['source_monotonic_seconds']) if same else None
    except (ValueError,TypeError,KeyError):elapsed=None
    result={**raw,'fresh':fresh,'source_age_seconds':elapsed_age,'input':dict(raw.get('input') or {}),'destinations':{}}
    def flow(row,old,key):
        value=None
        if fresh and same and row.get(key) and row.get(key)==old.get(key):
            if elapsed==0:value=old.get('flowing')
            elif elapsed is not None and 0<elapsed<=90 and type(row.get('bytes')) is int and type(old.get('bytes')) is int:
                if row['bytes']>=old['bytes']:value=row['bytes']>old['bytes']
        return {**row,'flowing':value}
    result['input']=flow(result['input'],previous.get('input') or {},'connection_id')
    for name in ('youtube','twitch'):
        row=dict((raw.get('destinations') or {}).get(name) or {})
        row=flow(row,(previous.get('destinations') or {}).get(name) or {},'id')
        if not fresh:state,reason='observation_unknown','native_observation_stale'
        elif name=='twitch' and raw.get('maintenance_state')=='WAIT_OFFLINE':state,reason='standby','scheduled_forward_pause'
        elif row.get('state') not in ('forwarding','connecting','error','idle'):state,reason='observation_unknown','forward_state_unavailable'
        elif row.get('state')!='forwarding':state,reason='target_bad','forward_not_forwarding'
        elif row['flowing'] is False:state,reason='target_bad','forward_bytes_not_increasing'
        elif row['flowing'] is True:state,reason='target_good','forward_bytes_increasing'
        else:state,reason='observation_unknown','forward_comparison_unavailable'
        result['destinations'][name]={**row,'classification':state,'reason':reason}
    return result


def classify(row,native,*,now,target):
    current=dict(row);fresh=age(row.get('checked_at_utc'),now)
    if target=='youtube_fallback' and row.get('expected_active') is False:
        state,reason='standby','fallback_inactive'
    elif fresh is None or fresh>30:
        state,reason='observation_unknown','frame_observation_stale'
    elif row.get('capture_ok') is True and row.get('clock_state')=='BLACK_FRAME':
        state,reason='target_bad','black_frame'
    elif (row.get('capture_ok') is True and row.get('clock_state')=='STALLED_CONFIRMED'
          and row.get('ocr_verified') is True and row.get('clock_pair_consistent') is True):
        state,reason='target_bad','clock_stalled_confirmed'
    elif target=='oci_relay' and native.get('fresh') and native.get('relay_active') is False:
        state,reason='target_bad','relay_not_running'
    elif target=='oci_relay' and native.get('fresh') and (native.get('input') or {}).get('available') is True and (native.get('input') or {}).get('ready') is False:
        state,reason='target_bad','relay_no_publisher'
    elif (row.get('capture_ok') is True and row.get('clock_state')=='ADVANCING_CONFIRMED'
          and row.get('ocr_verified') is True and row.get('clock_pair_consistent') is True):
        state,reason='target_good','verified_clock_advancing'
    else:
        state='observation_unknown';reason=row.get('reason','frame_observation_unavailable')
    current.update(classification=state,classification_reason=reason)
    if target=='oci_relay' and native.get('fresh') and row.get('capture_ok') is False:
        current['internal_observation_disagreement']=(native.get('input') or {}).get('ready') is True and (native.get('input') or {}).get('flowing') is True
    return current
