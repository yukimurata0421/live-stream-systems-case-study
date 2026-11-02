#!/usr/bin/env python3
"""Send bounded observation heartbeats to OCI; never control the media path.

The official OCI SDK signs requests. A fresh TLS response Date supplies the metric
and signature timestamp; local producer freshness uses only same-boot boottime.
The credential may write only this namespace in a dedicated compartment.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime, format_datetime
import json
from pathlib import Path
import re
import secrets
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from stream_v3_external_watchdog_heartbeat import check_local, read

NAMESPACE = 'stream_v3_watchdog'
METRIC = 'ObservationHeartbeat'
MAX_CLOCK_RTT = 5


def validate_config(value):
    if not isinstance(value, dict) or value.get('schema') != 'stream.oci_watchdog.v1':
        raise ValueError('config_schema_invalid')
    for name, kind in [('tenancy', 'tenancy'), ('user', 'user'), ('compartment', 'compartment')]:
        if not re.fullmatch(r'ocid1\.' + kind + r'\.oc1\.[a-z0-9.-]+', str(value.get(name, ''))):
            raise ValueError('config_identity_invalid')
    if not re.fullmatch(r'[a-z]{2}-[a-z]+-[1-9][0-9]*', str(value.get('region', ''))):
        raise ValueError('config_region_invalid')
    if not re.fullmatch(r'(?:[0-9a-f]{2}:){15}[0-9a-f]{2}', str(value.get('fingerprint', ''))):
        raise ValueError('config_fingerprint_invalid')
    if not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', str(value.get('monitor', ''))):
        raise ValueError('config_monitor_invalid')
    return value


def cloud_time(session, host, mono=None):
    """Bounded fresh HTTPS exchange; never reuse a previous request's time."""
    if mono is None:
        mono = lambda: time.clock_gettime(time.CLOCK_BOOTTIME)
    started = mono()
    response = session.head('https://' + host + '/?watchdog_nonce=' + secrets.token_hex(16),
                            headers={'Cache-Control': 'no-cache, no-store'},
                            timeout=(2, 3), allow_redirects=False)
    try:
        elapsed = mono() - started
        if (response.status_code not in (200, 400, 401, 403, 404, 405)
                or not 0 <= elapsed <= MAX_CLOCK_RTT
                or response.headers.get('Age', '0') != '0'):
            raise ValueError('cloud_clock_unavailable')
        stamp = parsedate_to_datetime(response.headers['Date'])
        if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
            raise ValueError('cloud_clock_invalid')
        return stamp.astimezone(timezone.utc)
    finally:
        response.close()


def point(config, stamp):
    validate_config(config)
    if stamp.tzinfo is None:
        raise ValueError('timestamp_must_be_aware')
    return {'metricData': [{'compartmentId': config['compartment'], 'namespace': NAMESPACE,
        'name': METRIC, 'dimensions': {'monitor': config['monitor']},
        'datapoints': [{'timestamp': stamp.isoformat(), 'value': 1, 'count': 1}]}]}


def alarm(config, topic_id, *, validation=False):
    validate_config(config)
    if not re.fullmatch(r'ocid1\.onstopic\.oc1\.[a-z0-9.-]+', topic_id):
        raise ValueError('topic_invalid')
    label = '[TEST] ' if validation else ''
    return {'display_name': label + 'stream-v3 observation heartbeat absent',
        'compartment_id': config['compartment'], 'metric_compartment_id': config['compartment'],
        'namespace': NAMESPACE,
        'query': f'{METRIC}[1m]{{monitor="{config["monitor"]}"}}.groupBy(monitor).absent(72h)',
        'pending_duration': 'PT5M', 'severity': 'CRITICAL', 'is_enabled': False,
        'destinations': [topic_id], 'repeat_notification_duration': 'PT1H',
        'message_format': 'ONS_OPTIMIZED',
        'body': label + 'Monitoring heartbeat state notification. OK_TO_FIRING means sustained missing data. Check the observation host, network, credentials and producers. '
                'This does not prove a stream outage and triggers no automatic repair. '
                'FIRING_TO_OK means heartbeat emission resumed; RESET after a long absence is not recovery evidence.',
        'freeform_tags': {'purpose': 'stream-v3-external-watchdog',
                          'mode': 'validation' if validation else 'live'}}


def emit(config, key_path, local_check, *, send=False, session=None, signer_factory=None):
    validate_config(config)
    if not local_check():
        raise ValueError('local_evidence_unavailable')
    if not send:
        return {'status': 'READY_NOT_SENT', 'provider': 'oci'}
    import requests
    if signer_factory is None:
        from oci.signer import Signer
        signer_factory = Signer
    host = 'telemetry-ingestion.' + config['region'] + '.oraclecloud.com'
    with session if session is not None else requests.Session() as transport:
        transport.trust_env = False
        stamp = cloud_time(transport, host)
        # A slow clock request must not turn already stale local evidence into a heartbeat.
        if not local_check():
            raise ValueError('local_evidence_unavailable')
        if key_path.is_symlink() or not key_path.is_file() or key_path.stat().st_size > 16384:
            raise ValueError('credential_invalid')
        signer = signer_factory(config['tenancy'], config['user'], config['fingerprint'], str(key_path))
        payload = json.dumps(point(config, stamp), separators=(',', ':')).encode()
        response = transport.post('https://' + host + '/20180401/metrics', data=payload,
            headers={'Content-Type': 'application/json', 'Date': format_datetime(stamp, usegmt=True)},
            auth=signer, timeout=(2, 5), allow_redirects=False, stream=True)
        try:
            raw = response.raw.read(16385, decode_content=True)
            if response.status_code != 200 or len(raw) > 16384:
                raise ValueError('metric_response_not_accepted')
            value = json.loads(raw)
            if (not isinstance(value, dict) or type(value.get('failedMetricsCount')) is not int
                    or value['failedMetricsCount'] != 0 or value.get('failedMetrics', []) != []):
                raise ValueError('metric_not_accepted')
        finally:
            response.close()
    return {'status': 'POINT_ACCEPTED', 'provider': 'oci', 'notification_verified': False,
            'cloud_timestamp': stamp.isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--key-file', type=Path, required=True)
    parser.add_argument('--notification-state', type=Path, required=True)
    parser.add_argument('--gateway-state', type=Path, default=Path('/run/stream-notification-gateway/status.json'))
    parser.add_argument('--send', action='store_true')
    args = parser.parse_args()
    try:
        result = emit(read(args.config), args.key_file,
                      lambda: check_local(args.gateway_state, args.notification_state), send=args.send)
        print(json.dumps(result))
        return 0
    except Exception as exc:
        # Provider exceptions may contain request details. Never log them or credentials.
        print(json.dumps({'status': 'HEARTBEAT_NOT_CONFIRMED', 'error_class': type(exc).__name__}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
