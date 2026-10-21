"""Expose separate frame, native-forward, and observer-quality facts without recovery or notification decisions."""
from datetime import datetime
import math

STATES=('WARMUP','ADVANCING_PROBABLE','ADVANCING_CONFIRMED','STALLED_SUSPECT','STALLED_CONFIRMED','BLACK_FRAME','OCR_UNCERTAIN','UNKNOWN')
CLASSES=('target_good','target_bad','observation_unknown','standby')
STAGES=('none','transport','decode','analysis','publication','binding','native','stale','standby')
REASONS=set('capture_tls_failed capture_connect_failed capture_timeout capture_source_unavailable capture_decode_failed capture_starting capture_stream_ended capture_frame_stale capture_reader_io_error capture_executable_unavailable snapshot_io_error clock_pair_unreliable clock_cadence_drift clock_time_regressed clock_time_jump ocr_time_unchanged ocr_time_advanced comparison_unavailable fallback_inactive fallback_stream_not_observed fallback_status_unavailable fallback_producer_identity_changed fallback_producer_not_ready_or_ambiguous fallback_transport_producer_mismatch relay_not_running relay_no_publisher runtime_attestation_unavailable frame_observation_stale black_frame observer_analysis_or_resource_error roi_invalid_or_too_small'.split())


def age(value,now):
    try:
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        if stamp.tzinfo is None:return None
        result=now-stamp.timestamp()
        return max(0.,result) if math.isfinite(result) and result>=-2 else None
    except (ValueError,TypeError,AttributeError,OverflowError):return None


def obj(value):return value if isinstance(value,dict) else {}
def number(value):return type(value) in (int,float) and math.isfinite(value)


def stage(row,fresh,bound):
    if row.get('expected_active') is False:return 'standby'
    if not bound:return 'binding'
    if not fresh:return 'stale'
    reason=str(row.get('reason',''))
    if row.get('error_stage')=='publication' or 'io_error' in reason:return 'publication'
    if row.get('capture_ok'):
        return 'analysis' if row.get('clock_state') in ('WARMUP','OCR_UNCERTAIN','UNKNOWN') else 'none'
    if any(s in reason for s in ('decode','ppm','frame_format')):return 'decode'
    if any(s in reason for s in ('roi','ocr','analysis','resource')):return 'analysis'
    return 'transport'


def histogram(writer,name,value,labels):
    """Append a classic histogram with its correct TYPE to the existing text writer."""
    if not hasattr(writer,'lines'):return
    bounds=value.get('bounds');counts=value.get('buckets');count=value.get('count');total=value.get('sum')
    if not isinstance(bounds,list) or not isinstance(counts,list) or len(bounds)!=len(counts) or not number(count) or not number(total):return
    if not all(number(v) for v in bounds+counts):return
    if bounds!=sorted(set(bounds)) or any(v<0 or v>count for v in counts) or counts!=sorted(counts):return
    seen=getattr(writer,'seen',set())
    if name not in seen:
        writer.lines.extend([f'# HELP {name} Validated display-clock age at decoded-frame arrival; seconds quantization and decoder queues apply.',f'# TYPE {name} histogram'])
        seen.add(name)
    text=','.join(f'{key}="{value}"' for key,value in sorted(labels.items()))
    for bound,amount in [*zip(bounds,counts),('+Inf',count)]:writer.lines.append(f'{name}_bucket{{{text},le="{bound}"}} {amount}')
    writer.lines.extend([f'{name}_sum{{{text}}} {total}',f'{name}_count{{{text}}} {count}'])


def write_metrics(writer,observation,*,now):
    observation=obj(observation);valid=observation.get('schema')=='stream_v3.oci_monitor_bundle.v1'
    state=obj(observation.get('state')) if valid else {};heartbeat=obj(observation.get('heartbeat')) if valid else {}
    contract=obj(observation.get('observer_contract'));admitted=contract.get('compatible') is True and contract.get('runtime_attested') is True
    hb_age=age(heartbeat.get('checked_at_utc'),now)
    writer.metric('stream_v3_oci_monitor_observer_heartbeat_ok',int(hb_age is not None and hb_age<=30 and heartbeat.get('worker_ok') is True),help_text='Observer process heartbeat; independent of frame or destination health.')
    for key in ('compatible','runtime_attested','configuration_changed'):
        writer.metric('stream_v3_monitor_probe_'+key,int(contract.get(key) is True),help_text='Observer runtime contract and configuration diagnostics.')
    for key in ('unique_observations_total','duplicates_total'):
        value=obj(observation.get('collector')).get(key)
        if number(value):writer.metric('stream_v3_monitor_probe_'+key,value,metric_type='counter',help_text='Collector cycle identity counters; duplicate cycles do not enter sample history.')
    for target,prefix,scope in (('oci_relay','stream_v3_oci_monitor_','dell_oci_relay_dell'),('youtube_fallback','stream_v3_youtube_fallback_monitor_','dell_backup_sender_dell')):
        row=obj(obj(state.get('targets')).get(target));labels={'target':target,'scope':scope}
        checked_age=age(row.get('checked_at_utc'),now);sample_age=age(row.get('captured_at_utc'),now)
        fresh=checked_age is not None and checked_age<=30;bound=admitted and row.get('source_binding_ok') is True
        capture=bound and fresh and row.get('capture_ok') is True and sample_age is not None and sample_age<=30
        if target=='youtube_fallback':capture=capture and row.get('expected_active') is True
        confirmed=capture and row.get('clock_state')=='ADVANCING_CONFIRMED' and row.get('ocr_verified') is True and row.get('clock_pair_consistent') is True
        for suffix,value,description in (
            ('observation_fresh',int(fresh),'Frame observation freshness.'),('capture_ok',int(capture),'Fresh frame with admitted observer and source binding.'),
            ('clock_advancing',int(confirmed),'Clock progression confirmed by paired JST and UTC OCR; not public-platform receipt.'),
            ('clock_ocr_verified',int(capture and row.get('ocr_verified') is True and row.get('clock_pair_consistent') is True),'Current clock reading verified by paired JST and UTC OCR.')):
            writer.metric(prefix+suffix,value,help_text=description)
        if target=='youtube_fallback':
            expected=row.get('expected_active')
            writer.metric(prefix+'expected_active',int(expected) if type(expected) is bool and fresh else -1,help_text='Backup sender requested; -1 unknown.')
            writer.metric(prefix+'producer_ready',int(fresh and row.get('producer_ready') is True and bound),help_text='Current backup producer runtime binding and readiness.')
        if sample_age is not None:writer.metric(prefix+'sample_age_seconds',sample_age,help_text='Latest frame arrival age.')
        current=row.get('clock_state') if capture else 'UNKNOWN'
        if current not in STATES:current='UNKNOWN'
        for value in STATES:writer.metric(prefix+'clock_state',int(current==value),labels={'state':value},help_text='Clock observation state; unavailable observations remain UNKNOWN.')
        classification=row.get('classification') if fresh and bound else 'observation_unknown'
        if row.get('expected_active') is False and fresh and row.get('source_binding_ok') is True:classification='standby'
        for value in CLASSES:writer.metric('stream_v3_monitor_probe_classification',int(classification==value),labels={**labels,'state':value})
        for value in STAGES:writer.metric('stream_v3_monitor_probe_failure_stage',int(stage(row,fresh,bound)==value),labels={**labels,'stage':value})
        reason=row.get('classification_reason') if row.get('classification_reason') in ('relay_not_running','relay_no_publisher') else row.get('reason')
        if not bound:reason='runtime_attestation_unavailable'
        elif not fresh:reason='frame_observation_stale'
        writer.metric('stream_v3_monitor_probe_reason',1,labels={**labels,'reason':reason if reason in REASONS else 'other'},help_text='Current fixed reason code; raw errors and URLs never become labels.')
        writer.metric('stream_v3_monitor_probe_scope_info',1,labels=labels,help_text='Observation boundary excludes YouTube and Twitch public-player receipt.')
        for suffix,key in (('clock_frozen_seconds','clock_frozen_seconds'),('clock_frozen_observed_seconds','clock_frozen_observed_seconds'),('cadence_error_seconds','cadence_error_seconds'),('display_age_seconds','display_age_seconds'),('receiver_frame_age_seconds','receiver_frame_age_seconds')):
            value=row.get(key)
            if number(value) and fresh:writer.metric('stream_v3_monitor_probe_'+suffix,value,labels=labels)
        for suffix,key in (('freeze_has_observation_gap','freeze_has_observation_gap'),('delay_valid','delay_valid'),('measurement_timing_valid','measurement_timing_valid'),('configuration_changed','configuration_changed'),('internal_observation_disagreement','internal_observation_disagreement')):
            enabled=fresh and row.get(key) is True
            if key=='delay_valid':enabled=enabled and confirmed
            writer.metric('stream_v3_monitor_probe_'+suffix,int(enabled),labels=labels)
        valid_age=age(row.get('last_valid_observation_at_utc') or state.get('started_at_utc'),now)
        if valid_age is not None:writer.metric('stream_v3_monitor_probe_seconds_since_last_valid_observation',valid_age,labels=labels)
        delay=row.get('stream_delay_seconds')
        if confirmed and row.get('delay_valid') is True and number(delay):writer.metric('stream_v3_monitor_probe_stream_delay_seconds',delay,labels=labels,help_text='Display-clock age at full decoded-frame arrival; not network-only round-trip time.')
        for key,value in obj(row.get('observation_counters')).items():
            if key in ('observations','ocr_uncertain','cadence_drift','valid_clock') and number(value):
                writer.metric('stream_v3_monitor_probe_analysis_total',value,labels={**labels,'kind':key},metric_type='counter')
        if bound:histogram(writer,'stream_v3_monitor_probe_display_age_seconds_histogram',obj(row.get('delay_histogram')),labels)
    native=obj(observation.get('native_delivery'));native_age=age(native.get('observed_at_utc'),now)
    native_fresh=native.get('fresh') is True and native_age is not None and native_age<=90
    writer.metric('stream_v3_monitor_probe_native_fresh',int(native_fresh))
    if native_age is not None:writer.metric('stream_v3_monitor_probe_native_age_seconds',native_age)
    for suffix,value in [('relay_active',native.get('relay_active')),*[(f'input_{key}',obj(native.get('input')).get(key)) for key in ('available','ready','flowing')]]:
        writer.metric('stream_v3_monitor_probe_native_'+suffix,int(value) if native_fresh and type(value) is bool else -1,help_text='Existing native relay facts; -1 unavailable.')
    for platform in ('youtube','twitch'):
        row=obj(obj(native.get('destinations')).get(platform));labels={'platform':platform,'scope':'oci_forward_sender'}
        classification=row.get('classification') if native_fresh else 'observation_unknown'
        for value in CLASSES:writer.metric('stream_v3_monitor_probe_forward_classification',int(classification==value),labels={**labels,'state':value})
        writer.metric('stream_v3_monitor_probe_forward_flowing',int(row['flowing']) if native_fresh and type(row.get('flowing')) is bool else -1,labels=labels)
        if number(row.get('bytes')) and native_fresh:writer.metric('stream_v3_monitor_probe_forward_bytes',row['bytes'],labels=labels,help_text='Native forward connection byte gauge; connection changes can reset it.')
