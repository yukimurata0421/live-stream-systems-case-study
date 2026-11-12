#!/usr/bin/env python3
"""Connect fixed read-only delivery/central facts to Prometheus and diagnostic logs."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
import shlex
import subprocess
import sys
import threading
import time
from urllib.parse import urlencode

if __package__:
    from .cra_monitoring_io import append, atomic, get_json, utc
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from cra_monitoring_io import append, atomic, get_json, utc

ROLES = {'dell': 'delivery-host', 'central': 'authority-host'}


def validate(value, role, source_hash, now):
    if (not isinstance(value, dict) or value.get('schema') != 'cra.monitoring_facts.v1'
            or value.get('role') != role or value.get('host_id') != ROLES[role]
            or value.get('source_sha256') != source_hash):
        raise ValueError('facts_identity_mismatch')
    age = now - float(value['collected_at'])
    if not math.isfinite(age) or not -5 <= age <= 30:
        raise ValueError('facts_time_invalid')
    data = value['data']
    required = ('audit_high', 'audit_acked', 'policy_version', 'unknown_actions', 'agent_service_up') if role == 'dell' else ('violations', 'missing_seq', 'unresolved_gaps', 'conflicts', 'policy_version', 'central_service_up')
    if not isinstance(data, dict) or any(type(data.get(k)) is not int or data[k] < 0 for k in required):
        raise ValueError('facts_data_invalid')
    for k in (('status_timestamp', 'observation_timestamp') if role == 'dell' else ('verification_timestamp',)):
        stamp = float(data[k])
        if not math.isfinite(stamp) or stamp > now+5 or stamp <= 0:
            raise ValueError('facts_timestamp_invalid')
    if role == 'dell':
        if data['mode'] not in ('off', 'dry_run', 'enforce') or len(data['audit']) > 200:
            raise ValueError('facts_policy_or_audit_invalid')
        seqs = [r['seq'] for r in data['audit']]
        if seqs != sorted(set(seqs)) or any(type(s) is not int or not 0 < s <= data['audit_high'] for s in seqs):
            raise ValueError('facts_audit_invalid')
    return value


def fetch(role, cfg, after):
    command = ['/usr/bin/python3', '-I', '-B', cfg['helper'], role, '--after', str(after)]
    argv = ['/usr/bin/ssh', '-T', '-oBatchMode=yes', '-oConnectTimeout=3',
            '-oStrictHostKeyChecking=yes', '-oUpdateHostKeys=no', '-oForwardAgent=no',
            '-oClearAllForwardings=yes', '-oControlMaster=no', '-oControlPath=none',
            cfg['ssh_host'], shlex.join(['/usr/bin/sudo', '-n', *command])]
    p = subprocess.run(argv, capture_output=True, timeout=8, check=True)
    if len(p.stdout) > 1048576:
        raise ValueError('facts_response_too_large')
    return validate(json.loads(p.stdout), role, cfg['source_sha256'], time.time())


def metrics(snapshot, now):
    lines = []
    def emit(name, value, labels=''):
        value = float(value)
        if not math.isfinite(value):
            raise ValueError('metric_nonfinite')
        lines.append(f'cra_{name}{labels} {value:.17g}')
    data = {}
    fresh = {}
    for role in ROLES:
        raw = snapshot.get(role) or {}
        value = raw.get('facts')
        label = '{component="'+role+'"}'
        available = bool(value) and raw.get('available') is True
        emit('source_available', int(available), label)
        emit('source_timestamp_seconds', value['collected_at'] if value else 0, label)
        fresh[role] = available and -5 <= now-value['collected_at'] <= 60
        if value:
            data[role] = value['data']
    d, c = data.get('dell', {}), data.get('central', {})
    for field, name in (
        ('agent_service_up', 'agent_service_up'), ('status_timestamp', 'agent_status_timestamp_seconds'),
        ('observation_timestamp', 'target_observation_timestamp_seconds'), ('target_valid', 'target_valid'),
        ('authority_match', 'executor_authority_match'), ('local_stop', 'agent_local_stop'),
        ('in_flight', 'in_flight_requests'), ('unknown_actions', 'unresolved_actions'),
        ('enforce_actions', 'enforce_actions_total'), ('audit_high', 'agent_audit_sequence'),
        ('audit_acked', 'agent_audit_acked_sequence'), ('legacy_timer_active', 'legacy_controller_active')):
        if field in d:
            emit(name, d[field])
    for mode in ('off', 'dry_run', 'enforce'):
        emit('agent_policy_mode', int(d.get('mode') == mode), '{mode="'+mode+'"}')
    for role, value in data.items():
        emit('policy_version', value['policy_version'], '{component="'+role+'"}')
    ob = d.get('observer', {})
    emit('pod_api_observer_available', ob.get('available', 0))
    for field in ('successful_reads', 'failed_reads', 'busy_rejections'):
        if field in ob:
            emit('pod_api_'+field+'_total', ob[field])
    request = ob.get('last_request') or {}
    if 'duration_seconds' in request:
        emit('pod_api_last_request_duration_seconds', request['duration_seconds'])
    for field, name in (
        ('central_service_up', 'central_service_up'), ('verify_timer_up', 'central_verify_timer_up'),
        ('verification_timestamp', 'central_verification_timestamp_seconds'),
        ('violations', 'central_verification_violations'), ('missing_seq', 'central_missing_sequences'),
        ('unresolved_gaps', 'central_unresolved_gaps'), ('conflicts', 'central_conflicts')):
        if field in c:
            emit(name, c[field])
    central_high = None
    if d and c:
        found = [s['high'] for s in c['streams'] if s['agent_id'] == 'delivery-host' and s['instance_id'] == d['instance_id']]
        if len(found) == 1:
            central_high = found[0]
            emit('central_audit_sequence', central_high)
    loki = snapshot.get('loki') or {}
    emit('loki_query_available', int(loki.get('available') is True))
    emit('loki_last_event_timestamp_seconds', loki.get('last_event', 0))
    notifier = snapshot.get('notifier') or {}
    emit('notifier_available', int(notifier.get('available') is True))
    emit('notifier_timestamp_seconds', notifier.get('checked_at', 0))
    deliveries = notifier.get('deliveries') or {}
    emit('notification_unconfirmed', sum(deliveries.get(k, 0) for k in ('UNKNOWN', 'SENDING', 'REJECTED', 'EXHAUSTED')))
    emit('notification_pending', deliveries.get('PENDING', 0))
    issue = (not all(fresh.values()) or not d or not c
             or d.get('agent_service_up') != 1 or (snapshot.get('dell', {}).get('facts') or {}).get('collected_at', now)-d.get('status_timestamp', 0) > 10
             or d.get('target_valid') != 1 or (snapshot.get('dell', {}).get('facts') or {}).get('collected_at', now)-d.get('observation_timestamp', 0) > 10
             or d.get('authority_match') != 1 or d.get('mode') != 'enforce' or d.get('local_stop')
             or d.get('unknown_actions') or d.get('legacy_timer_active')
             or c.get('central_service_up') != 1 or c.get('verify_timer_up') != 1
             or now-c.get('verification_timestamp', 0) > 480
             or any(c.get(k, 1) for k in ('violations', 'missing_seq', 'unresolved_gaps', 'conflicts'))
             or c.get('policy_version') != d.get('policy_version') or central_high is None
             or central_high < d.get('audit_acked', 0)
             or not loki.get('available') or now-loki.get('last_event', 0) > 180
             or not notifier.get('available') or now-notifier.get('checked_at', 0) > 90
             or any(deliveries.get(k, 0) for k in ('UNKNOWN', 'SENDING', 'PENDING', 'REJECTED', 'EXHAUSTED')))
    emit('monitoring_issue', int(bool(issue)))
    emit('last_cycle_timestamp_seconds', snapshot.get('cycle_at', 0))
    return '\n'.join(lines)+'\n'


class Collector:
    def __init__(self, config, *, pull=fetch):
        self.config = config
        self.pull = pull
        self.state_path = Path(config['state_dir'])/'cursor.json'
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {
            'cursor': {'instance': config['initial_instance'], 'seq': config['initial_after_seq']}}
        self.snapshot = {}
        self.lock = threading.Lock()

    def cycle(self, now):
        values, events = {}, []
        cursor = self.state['cursor']
        with ThreadPoolExecutor(max_workers=2) as p:
            jobs = {role: p.submit(self.pull, role, cfg, cursor['seq'] if role == 'dell' else 0)
                    for role, cfg in self.config['sources'].items()}
            for role, job in jobs.items():
                try:
                    facts = job.result()
                    values[role] = {'available': True, 'facts': facts}
                except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
                    values[role] = {'available': False}
        d = (values.get('dell', {}).get('facts') or {}).get('data')
        if d:
            if d['instance_id'] != cursor['instance']:
                cursor = {'instance': d['instance_id'], 'seq': 0}
                events.append({'kind': 'audit_instance_changed', 'event_id': 'instance/'+d['instance_id']})
            else:
                for row in d['audit']:
                    if row['seq'] <= cursor['seq']:
                        continue
                    if row['seq'] != cursor['seq']+1:
                        values['dell']['available'] = False
                        events.append({'kind': 'audit_gap', 'event_id': f"gap/{d['instance_id']}/{cursor['seq']}", 'expected': cursor['seq']+1, 'received': row['seq']})
                        break
                    events.append({'kind': 'agent_audit', 'event_id': f"audit/{d['instance_id']}/{row['seq']}",
                                   'source_ts_utc': row['ts'], 'audit_kind': row['kind'], 'seq': row['seq'], 'payload': row['payload']})
                    cursor = {'instance': d['instance_id'], 'seq': row['seq']}
        c = (values.get('central', {}).get('facts') or {}).get('data')
        if c and c['verification_timestamp'] != self.state.get('central_verify'):
            events.append({'kind': 'central_verification', 'event_id': 'verify/'+str(c['verification_timestamp']),
                           'source_ts_utc': utc(c['verification_timestamp']),
                           **{k: c[k] for k in ('violations', 'violation_codes', 'missing_seq', 'unresolved_gaps', 'conflicts')}})
        if now-self.state.get('heartbeat', 0) >= 60:
            events.append({'kind': 'observation_heartbeat', 'event_id': 'heartbeat/'+str(int(now)),
                           'source_available': {k: v['available'] for k, v in values.items()}})
        for event in events:
            event.update(ts_utc=utc(now), component='cra_observation', schema='cra.monitoring_event.v1')
        append(self.config['log_path'], events)
        # The audit cursor advances only after the diagnostic log is durable.
        self.state['cursor'] = cursor
        if c:
            self.state['central_verify'] = c['verification_timestamp']
        if any(e['kind'] == 'observation_heartbeat' for e in events):
            self.state['heartbeat'] = now
        atomic(self.state_path, self.state)
        try:
            query = urlencode({'query': '{app="cra",source="cra_monitoring"}', 'start': str(int((now-300)*1e9)),
                               'end': str(int(now*1e9)), 'limit': '1', 'direction': 'backward'})
            result = get_json('http://127.0.0.1:3100/loki/api/v1/query_range?'+query)
            if result.get('status') != 'success':
                raise ValueError('loki_query_failed')
            stamps = [int(t)/1e9 for stream in result['data']['result'] for t, _ in stream['values']]
            values['loki'] = {'available': True, 'last_event': max(stamps, default=0)}
        except (OSError, ValueError, KeyError, TypeError):
            values['loki'] = {'available': False, 'last_event': 0}
        values['cycle_at'] = now
        try:
            values['notifier'] = json.loads((Path(self.config['state_dir'])/'notify-status.json').read_text())
        except (OSError, ValueError):
            values['notifier'] = {'available': False, 'checked_at': 0}
        with self.lock:
            self.snapshot = values
        atomic(Path(self.config['state_dir'])/'status.json', values)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--once', action='store_true')
    args = p.parse_args()
    config = json.loads(args.config.read_text())
    collector = Collector(config)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/metrics':
                self.send_error(404)
                return
            with collector.lock:
                raw = metrics(collector.snapshot, time.time()).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', config.get('port', 9130)), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    while True:
        collector.cycle(time.time())
        if args.once:
            break
        time.sleep(15)


if __name__ == '__main__':
    main()
