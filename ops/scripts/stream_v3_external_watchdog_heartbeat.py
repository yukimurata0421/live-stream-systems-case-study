#!/usr/bin/env python3
"""Bounded heartbeat for an independently hosted absence detector.

Without --send this only checks local evidence and prints the proposed point.
Cloud notification channel creation and alert activation are separate operations.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import subprocess
import time
from urllib.request import Request, ProxyHandler, build_opener

METRIC = 'custom.googleapis.com/stream_observation/heartbeat'


def fresh(value, *, boot, mono, limit):
    if not isinstance(value, dict):
        return False
    stamp = value.get('mono')
    return (value.get('boot_id') == boot and type(stamp) in (int, float)
            and math.isfinite(stamp) and 0 <= mono - stamp <= limit)


def ready(gateway, notification, *, boot, mono, prometheus_ready):
    return (prometheus_ready is True and isinstance(gateway, dict) and isinstance(notification, dict)
            and gateway.get('schema') == 'stream.notification_gateway.v1'
            and fresh(gateway, boot=boot, mono=mono, limit=30)
            and fresh(notification.get('notification_clock', {}), boot=boot, mono=mono, limit=300))


def check_local(gateway_path, notification_path):
    """Read current producer evidence; file copies do not renew its age."""
    opener = build_opener(ProxyHandler({}))
    with opener.open('http://127.0.0.1:9090/-/ready', timeout=3) as response:
        prom_ok = response.status == 200 and b'Ready' in response.read(1024)
    return ready(read(gateway_path), read(notification_path),
                 boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                 mono=time.clock_gettime(time.CLOCK_BOOTTIME), prometheus_ready=prom_ok)


def point(project, monitor, wall):
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]', project):
        raise ValueError('project_invalid')
    if not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', monitor):
        raise ValueError('monitor_invalid')
    stamp = datetime.fromtimestamp(wall, timezone.utc).isoformat()
    return {'timeSeries': [{'metric': {'type': METRIC, 'labels': {'monitor': monitor}},
        'resource': {'type': 'global', 'labels': {'project_id': project}},
        'metricKind': 'GAUGE', 'valueType': 'INT64',
        'points': [{'interval': {'endTime': stamp}, 'value': {'int64Value': '1'}}]}]}


def policy(project, monitor, channels):
    point(project, monitor, 0)
    prefix = f'projects/{project}/notificationChannels/'
    if not channels or any(not c.startswith(prefix) or not c[len(prefix):].isdigit() for c in channels):
        raise ValueError('verified_notification_channel_required')
    return {'displayName': 'Observation owner heartbeat absent', 'enabled': True,
        'combiner': 'OR', 'notificationChannels': channels,
        'documentation': {'mimeType': 'text/markdown', 'content':
            'Monitoring evidence stopped arriving. Check the observation owner, network, credentials and clock. This is not a stream outage verdict.'},
        'conditions': [{'displayName': 'Heartbeat absent for five minutes',
            'conditionAbsent': {'filter': f'resource.type="global" AND metric.type="{METRIC}" AND metric.labels.monitor="{monitor}"',
                'duration': '300s', 'trigger': {'count': 1}}}]}


def read(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError('source_invalid')
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError('source_invalid')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', required=True)
    parser.add_argument('--monitor', default='observation-owner')
    parser.add_argument('--notification-state', type=Path, required=True)
    parser.add_argument('--gateway-state', type=Path, default=Path('/run/stream-notification-gateway/status.json'))
    parser.add_argument('--channel', action='append', default=[])
    parser.add_argument('--policy', action='store_true')
    parser.add_argument('--send', action='store_true')
    args = parser.parse_args()
    if args.policy:
        if args.send:
            parser.error('policy generation cannot send or activate anything')
        print(json.dumps(policy(args.project, args.monitor, args.channel), indent=2))
        return 0
    opener = build_opener(ProxyHandler({}))
    try:
        if not check_local(args.gateway_state, args.notification_state):
            raise ValueError('local_evidence_unavailable')
        value = point(args.project, args.monitor, time.time())
        if not args.send:
            print(json.dumps({'status': 'READY_NOT_SENT', 'point': value}))
            return 0
        token = subprocess.run(['/usr/bin/gcloud', 'auth', 'application-default', 'print-access-token'],
            capture_output=True, text=True, timeout=10, check=True).stdout.strip()
        if not token or '\n' in token:
            raise ValueError('credential_unavailable')
        request = Request(f'https://monitoring.googleapis.com/v3/projects/{args.project}/timeSeries',
            data=json.dumps(value).encode(), method='POST',
            headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
        with opener.open(request, timeout=5) as response:
            if response.status != 200:
                raise ValueError('point_not_accepted')
            response.read(4096)
        print(json.dumps({'status': 'POINT_ACCEPTED', 'notification_verified': False}))
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(json.dumps({'status': 'HEARTBEAT_NOT_CONFIRMED', 'error_class': type(exc).__name__}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
