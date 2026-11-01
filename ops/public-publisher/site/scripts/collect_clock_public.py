"""Publish arena's on-screen clock (OCR) observations as a fixed, numeric-only projection.

arena's Prometheus is queried read-only over SSH. Only allowlisted states, reason codes and
bounded numbers reach the public tree; a failed fetch keeps the previous artifact and its age.
"""
import json
import math
import os
import subprocess
import tempfile
import time
from pathlib import Path
from public_snapshot_time import PublicTimestampError, validate_generated_at

PUBLIC = Path(__file__).resolve().parents[1] / 'public'
SCHEMA = 'stream.clock_observation_public.v1'
STEP = 600
BUCKETS = 144
# One scrape job only: the clock job scrapes the same exporter, so mixing jobs double-counts ratios.
JOB = 'stream_v3_arena_monitor'

CLOCK_STATES = ('WARMUP', 'ADVANCING_PROBABLE', 'ADVANCING_CONFIRMED', 'STALLED_SUSPECT', 'STALLED_CONFIRMED',
                'BLACK_FRAME', 'OCR_UNCERTAIN', 'UNKNOWN')
CLASSES = ('target_good', 'target_bad', 'observation_unknown', 'standby')
THUMB_STATES = ('FRESH', 'AGING', 'STALE_SUSPECT', 'STALE_CORRELATED', 'UNKNOWN', 'SOURCE_OFFLINE',
                'WAITING_NEW_STREAM_THUMBNAIL', 'WAITING_POLL', 'WAITING_RETRY')
# Fixed reason codes emitted by the exporter (oci_monitor_metrics.REASONS); anything else becomes 'other'.
REASONS = frozenset('''capture_tls_failed capture_connect_failed capture_timeout capture_source_unavailable capture_decode_failed
capture_starting capture_stream_ended capture_frame_stale capture_reader_io_error capture_executable_unavailable capture_probe_failed
snapshot_io_error clock_pair_unreliable clock_cadence_drift clock_time_regressed clock_time_jump ocr_time_unchanged ocr_time_advanced
comparison_unavailable fallback_inactive fallback_stream_not_observed fallback_status_unavailable fallback_producer_identity_changed
fallback_producer_not_ready_or_ambiguous fallback_transport_producer_mismatch relay_not_running relay_no_publisher
runtime_attestation_unavailable frame_observation_stale black_frame observer_analysis_or_resource_error roi_invalid_or_too_small
other'''.split())
# Groups for the 24-hour strip: what each state means for "is the picture moving".
THUMB_GROUP = {'FRESH': 'good', 'AGING': 'good', 'STALE_SUSPECT': 'bad', 'STALE_CORRELATED': 'bad', 'UNKNOWN': 'unknown',
               'SOURCE_OFFLINE': 'standby', 'WAITING_NEW_STREAM_THUMBNAIL': 'standby', 'WAITING_POLL': 'good', 'WAITING_RETRY': 'unknown'}

J = '{job="' + JOB + '"}'
JT = lambda target, extra='': '{job="' + JOB + '",target="' + target + '"' + extra + '}'
INSTANT = {
    'oci_state': 'max by (state) (stream_v3_oci_monitor_clock_state' + J + ')',
    'fb_state': 'max by (state) (stream_v3_youtube_fallback_monitor_clock_state' + J + ')',
    'thumb_state': 'max by (state) (stream_v3_twitch_thumbnail_freshness_state' + J + ')',
    'class': 'max by (target,state) (stream_v3_monitor_probe_classification' + J + ')',
    'reason': 'max by (target,reason) (stream_v3_monitor_probe_reason' + J + ')',
    'scalar': 'max by (__name__) ({__name__=~"stream_v3_(oci_monitor_(observer_heartbeat_ok|sample_age_seconds)'
              '|youtube_fallback_monitor_(expected_active|sample_age_seconds)'
              '|twitch_thumbnail_(api_live|clock_verified|progress_observed|image_duplicate|frame_age_seconds|seconds_since_last_valid_observation))",job="' + JOB + '"})',
    'probe': 'max by (__name__,target) ({__name__=~"stream_v3_monitor_probe_(stream_delay_seconds|seconds_since_last_valid_observation)",job="' + JOB + '"})',
    'reads_24h': 'max by (target,kind) (increase(stream_v3_monitor_probe_analysis_total' + J + '[24h]))',
    'thumb_samples_24h': 'max(increase(stream_v3_twitch_thumbnail_unique_samples_total' + J + '[24h]))',
    'thumb_reads_24h': 'max(increase(stream_v3_twitch_thumbnail_ocr_observations_total' + J + '[24h]))',
    'thumb_retries_24h': 'max(increase(stream_v3_twitch_thumbnail_ocr_binary_retries_total' + J + '[24h]))',
    'delay_p50_24h': 'histogram_quantile(0.5, max by (le) (increase(stream_v3_monitor_probe_display_age_seconds_histogram_bucket' + JT('oci_relay') + '[24h])))',
    'delay_p95_24h': 'histogram_quantile(0.95, max by (le) (increase(stream_v3_monitor_probe_display_age_seconds_histogram_bucket' + JT('oci_relay') + '[24h])))',
}
RANGE = {
    'class': 'max by (target,state) (avg_over_time(stream_v3_monitor_probe_classification' + J + '[10m]))',
    'thumb_state': 'max by (state) (avg_over_time(stream_v3_twitch_thumbnail_freshness_state' + J + '[10m]))',
    'oci_delay': 'max(avg_over_time(stream_v3_monitor_probe_stream_delay_seconds' + JT('oci_relay') + '[10m]))',
    'thumb_age': 'max(max_over_time(stream_v3_twitch_thumbnail_frame_age_seconds' + J + '[10m]))',
}

# Runs on arena with the system python3; reads only arena's loopback Prometheus.
REMOTE = r'''
import json,sys,time,urllib.parse,urllib.request
spec=json.loads(sys.argv[1]);opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
now=time.time();end=int(now//spec['step'])*spec['step'];start=end-spec['step']*(spec['buckets']-1)
def get(path,params):
    with opener.open('http://127.0.0.1:9090/api/v1/'+path+'?'+urllib.parse.urlencode(params),timeout=5) as r:body=json.load(r)
    if body.get('status')!='success':raise RuntimeError('query failed')
    return [[x['metric'],x.get('value',x.get('values'))] for x in body['data']['result']]
out={'now':now,'start':start,'end':end,'instant':{},'range':{}}
for k,q in spec['instant'].items():out['instant'][k]=get('query',{'query':q,'time':now})
for k,q in spec['range'].items():out['range'][k]=get('query_range',{'query':q,'start':start,'end':end,'step':spec['step']})
print(json.dumps(out,separators=(',',':')))
'''


def num(v, upper=1e7):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return round(x, 3) if math.isfinite(x) and 0 <= x <= upper else None


def one_hot(rows, allowed, key='state', where=None):
    """The active label of a 0/1 state family; UNKNOWN when none or several are set."""
    active = [r[0].get(key) for r in rows if (where is None or all(r[0].get(k) == v for k, v in where.items())) and num(r[1][1]) == 1]
    return active[0] if len(active) == 1 and active[0] in allowed else None


def series(rows, start, where=None):
    """Values per bucket end; missing buckets stay None (no data, not failure)."""
    values = [None] * BUCKETS
    for metric, points in rows:
        if where is not None and any(metric.get(k) != v for k, v in where.items()):
            continue
        for t, v in points:
            i = int(round((float(t) - start) / STEP))
            x = num(v)
            if 0 <= i < BUCKETS and x is not None:
                values[i] = x if values[i] is None else max(values[i], x)
    return values


def mix(parts):
    """Per-bucket shares of good/bad/unknown/standby; None when nothing was observed."""
    out = []
    for i in range(BUCKETS):
        cell = {k: sum(p[i] or 0 for p in v) for k, v in parts.items()}
        total = sum(cell.values())
        out.append(None if not total else {k: round(min(1., x / total), 3) for k, x in cell.items()})
    return out


def build(raw, now):
    if not isinstance(raw, dict) or not isinstance(raw.get('instant'), dict) or not isinstance(raw.get('range'), dict):
        raise ValueError('SHAPE')
    observed = num(raw.get('now'), 1e10)
    start = num(raw.get('start'), 1e10)
    if observed is None or start is None:
        raise ValueError('STALE')
    validate_generated_at(observed, now, max_age_seconds=120)
    ins, rng = raw['instant'], raw['range']
    scalar = {r[0].get('__name__'): num(r[1][1]) for r in ins.get('scalar', [])}
    probe = {(r[0].get('__name__'), r[0].get('target')): num(r[1][1]) for r in ins.get('probe', [])}
    reads = {(r[0].get('target'), r[0].get('kind')): num(r[1][1]) for r in ins.get('reads_24h', [])}
    single = lambda k: num(ins[k][0][1][1]) if ins.get(k) else None
    reason = lambda t: next((r[0]['reason'] if r[0].get('reason') in REASONS else 'other'
                             for r in ins.get('reason', []) if r[0].get('target') == t and num(r[1][1]) == 1), None)
    count = lambda v: None if v is None else int(round(v))
    probe_parts = lambda t: mix({g: [series(rng.get('class', []), start, {'target': t, 'state': s})]
                                 for g, s in (('good', 'target_good'), ('bad', 'target_bad'), ('unknown', 'observation_unknown'), ('standby', 'standby'))})
    thumb_groups = {g: [series(rng.get('thumb_state', []), start, {'state': s}) for s, gg in THUMB_GROUP.items() if gg == g]
                    for g in ('good', 'bad', 'unknown', 'standby')}
    sources = [
        {'id': 'oci_relay', 'state': one_hot(ins.get('oci_state', []), CLOCK_STATES) or 'UNKNOWN',
         'classification': one_hot(ins.get('class', []), CLASSES, where={'target': 'oci_relay'}) or 'observation_unknown',
         'reason': reason('oci_relay'), 'heartbeat_ok': scalar.get('stream_v3_oci_monitor_observer_heartbeat_ok') == 1,
         'sample_age_seconds': scalar.get('stream_v3_oci_monitor_sample_age_seconds'),
         'clock_delay_seconds': probe.get(('stream_v3_monitor_probe_stream_delay_seconds', 'oci_relay')),
         'since_valid_seconds': probe.get(('stream_v3_monitor_probe_seconds_since_last_valid_observation', 'oci_relay')),
         'delay_p50_24h': single('delay_p50_24h'), 'delay_p95_24h': single('delay_p95_24h'),
         'reads_24h': {k: count(reads.get(('oci_relay', k))) for k in ('observations', 'valid_clock', 'ocr_uncertain', 'cadence_drift')},
         'timeline': probe_parts('oci_relay'), 'value_series': series(rng.get('oci_delay', []), start)},
        {'id': 'youtube_fallback', 'state': one_hot(ins.get('fb_state', []), CLOCK_STATES) or 'UNKNOWN',
         'classification': one_hot(ins.get('class', []), CLASSES, where={'target': 'youtube_fallback'}) or 'observation_unknown',
         'reason': reason('youtube_fallback'),
         'expected_active': {1: True, 0: False}.get(scalar.get('stream_v3_youtube_fallback_monitor_expected_active')),
         'sample_age_seconds': scalar.get('stream_v3_youtube_fallback_monitor_sample_age_seconds'),
         'since_valid_seconds': probe.get(('stream_v3_monitor_probe_seconds_since_last_valid_observation', 'youtube_fallback')),
         'reads_24h': {k: count(reads.get(('youtube_fallback', k))) for k in ('observations', 'valid_clock', 'ocr_uncertain', 'cadence_drift')},
         'timeline': probe_parts('youtube_fallback')},
        {'id': 'twitch_thumbnail', 'state': one_hot(ins.get('thumb_state', []), THUMB_STATES) or 'UNKNOWN',
         'api_live': scalar.get('stream_v3_twitch_thumbnail_api_live') == 1,
         'clock_verified': scalar.get('stream_v3_twitch_thumbnail_clock_verified') == 1,
         'progress_observed': scalar.get('stream_v3_twitch_thumbnail_progress_observed') == 1,
         'image_unchanged': scalar.get('stream_v3_twitch_thumbnail_image_duplicate') == 1,
         'frame_age_seconds': scalar.get('stream_v3_twitch_thumbnail_frame_age_seconds'),
         'since_valid_seconds': scalar.get('stream_v3_twitch_thumbnail_seconds_since_last_valid_observation'),
         'samples_24h': count(single('thumb_samples_24h')), 'reads_24h': count(single('thumb_reads_24h')),
         'retries_24h': count(single('thumb_retries_24h')),
         'timeline': mix(thumb_groups), 'value_series': series(rng.get('thumb_age', []), start)},
    ]
    return validate({'schema': SCHEMA, 'generated_at': observed, 'bucket_start': start, 'bucket_seconds': STEP, 'sources': sources}, now)


SOURCE_KEYS = {
    'oci_relay': {'id', 'state', 'classification', 'reason', 'heartbeat_ok', 'sample_age_seconds', 'clock_delay_seconds', 'since_valid_seconds',
                  'delay_p50_24h', 'delay_p95_24h', 'reads_24h', 'timeline', 'value_series'},
    'youtube_fallback': {'id', 'state', 'classification', 'reason', 'expected_active', 'sample_age_seconds', 'since_valid_seconds', 'reads_24h', 'timeline'},
    'twitch_thumbnail': {'id', 'state', 'api_live', 'clock_verified', 'progress_observed', 'image_unchanged', 'frame_age_seconds', 'since_valid_seconds',
                         'samples_24h', 'reads_24h', 'retries_24h', 'timeline', 'value_series'},
}


def validate(v, now):
    """Reject anything outside the fixed public shape: unknown keys, free text, non-finite or stale values."""
    def number(x, upper=1e7, nullable=True):
        if x is None and nullable: return
        if type(x) not in (int, float) or not math.isfinite(x) or not 0 <= x <= upper: raise ValueError('NUMBER')
    if not isinstance(v, dict) or set(v) != {'schema', 'generated_at', 'bucket_start', 'bucket_seconds', 'sources'}: raise ValueError('KEYS')
    if v['schema'] != SCHEMA or v['bucket_seconds'] != STEP: raise ValueError('SCHEMA')
    number(v['generated_at'], 1e10, False); number(v['bucket_start'], 1e10, False)
    validate_generated_at(v['generated_at'], now, max_age_seconds=120)
    if v['bucket_start'] > v['generated_at']: raise ValueError('STALE')
    if not isinstance(v['sources'], list) or [s.get('id') if isinstance(s, dict) else None for s in v['sources']] != list(SOURCE_KEYS): raise ValueError('SOURCES')
    for s in v['sources']:
        if set(s) != SOURCE_KEYS[s['id']]: raise ValueError('SOURCE_KEYS')
        if s['state'] not in (THUMB_STATES if s['id'] == 'twitch_thumbnail' else CLOCK_STATES): raise ValueError('STATE')
        if 'classification' in s and s['classification'] not in CLASSES: raise ValueError('CLASS')
        if 'reason' in s and s['reason'] is not None and s['reason'] not in REASONS: raise ValueError('REASON')
        for k, x in s.items():
            if k in ('heartbeat_ok', 'api_live', 'clock_verified', 'progress_observed', 'image_unchanged') and type(x) is not bool: raise ValueError('BOOL')
            if k == 'expected_active' and x not in (True, False, None): raise ValueError('BOOL')
            if k.endswith(('_seconds', '_p50_24h', '_p95_24h')): number(x)
            if k in ('samples_24h', 'retries_24h') or (k == 'reads_24h' and not isinstance(x, dict)):
                if x is not None and type(x) is not int: raise ValueError('COUNT')
                number(x)
        if isinstance(s['reads_24h'], dict):
            if set(s['reads_24h']) != {'observations', 'valid_clock', 'ocr_uncertain', 'cadence_drift'}: raise ValueError('READS')
            for x in s['reads_24h'].values():
                if x is not None and type(x) is not int: raise ValueError('COUNT')
                number(x)
        if not isinstance(s['timeline'], list) or len(s['timeline']) != BUCKETS: raise ValueError('TIMELINE')
        for cell in s['timeline']:
            if cell is None: continue
            if not isinstance(cell, dict) or set(cell) != {'good', 'bad', 'unknown', 'standby'}: raise ValueError('CELL')
            for x in cell.values(): number(x, 1, False)
        if 'value_series' in s:
            if not isinstance(s['value_series'], list) or len(s['value_series']) != BUCKETS: raise ValueError('SERIES')
            for x in s['value_series']: number(x)
    return v


def fetch():
    spec = json.dumps({'step': STEP, 'buckets': BUCKETS, 'instant': INSTANT, 'range': RANGE}, separators=(',', ':'))
    target = os.environ.get('STREAM_V3_CLOCK_SSH_TARGET', os.environ.get('STREAM_V3_RELIABILITY_SSH_TARGET', 'stream-monitor'))
    if not target or target.startswith('-') or any(c.isspace() for c in target): raise ValueError('REMOTE_TARGET')
    r = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', target, 'python3', '-I', '-c',
                        _quote(REMOTE), _quote(spec)], capture_output=True, timeout=30)
    if r.returncode or len(r.stdout) > 1 << 20: raise ValueError('REMOTE')
    return json.loads(r.stdout)


def _quote(text):
    return "'" + text.replace("'", "'\"'\"'") + "'"


def main():
    path = PUBLIC / 'clock-observation.json'
    try:
        value = build(fetch(), time.time())
    except (OSError, ValueError, TypeError, KeyError, IndexError, subprocess.TimeoutExpired) as exc:
        reason=str(exc) if isinstance(exc,PublicTimestampError) else 'FETCH_OR_SCHEMA_INVALID'
        print(json.dumps({'clock_public_source': 'unavailable', 'last_artifact_retained': path.exists(), 'reason': reason}))
        # Retain the original timestamps: a failed fetch never rejuvenates evidence.
        return 0
    fd, name = tempfile.mkstemp(prefix='.clock-', dir=PUBLIC)
    try:
        with os.fdopen(fd, 'w') as out:
            os.fchmod(out.fileno(), 0o644); json.dump(value, out, allow_nan=False, separators=(',', ':')); out.write('\n'); out.flush(); os.fsync(out.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name): os.unlink(name)
    return 0


if __name__ == '__main__': raise SystemExit(main())
