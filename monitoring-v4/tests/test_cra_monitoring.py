"""Check missing/stale I/O, durable audit cursors and notification deduplication."""
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
import shutil
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from stream_monitoring_v4.compatibility import cra_notify as notify
from stream_monitoring_v4.compatibility import cra_observation as observation
from stream_monitoring_v4.compatibility import cra_monitoring_io as monitor_io
from stream_monitoring_v4.compatibility.twitch_alert_notify import plan

NOW = 1794500000.


def facts(role, seq=10, audit=None):
    data = ({'audit_high': seq, 'audit_acked': seq, 'policy_version': 3,
             'unknown_actions': 0, 'agent_service_up': 1, 'status_timestamp': NOW,
             'observation_timestamp': NOW, 'target_valid': 1, 'authority_match': 1,
             'local_stop': False, 'legacy_timer_active': 0, 'mode': 'enforce',
             'instance_id': 'instance', 'enforce_actions': 0, 'in_flight': 0,
             'audit': audit or [], 'observer': {'available': 1}}
            if role == 'dell' else
            {'violations': 0, 'missing_seq': 0, 'unresolved_gaps': 0, 'conflicts': 0,
             'policy_version': 3, 'central_service_up': 1, 'verify_timer_up': 1,
             'verification_timestamp': NOW, 'violation_codes': [],
             'streams': [{'agent_id': 'delivery-host', 'instance_id': 'instance', 'high': seq}]})
    return {'schema': 'cra.monitoring_facts.v1', 'role': role, 'host_id': observation.ROLES[role],
            'collected_at': NOW, 'source_sha256': 'a'*64, 'boot_id': 'boot', 'data': data}


def rules(firing=False):
    result = {n: {'name': n, 'health': 'ok', 'state': 'inactive',
                  'lastEvaluation': datetime.fromtimestamp(NOW, timezone.utc).isoformat(),
                  'annotations': {'summary': n}, 'alerts': []} for n in notify.NAMES}
    if firing:
        n = 'CRACentralVerificationFailed'
        result[n].update(state='firing', alerts=[{'labels': {'alertname': n},
            'activeAt': datetime.fromtimestamp(NOW-60, timezone.utc).isoformat(), 'state': 'firing'}])
    return result


class FactsTests(unittest.TestCase):
    def test_epoch_timestamp_survives_metrics_serialization(self):
        x = facts('dell'); x['collected_at'] = NOW+717.7493516
        text = observation.metrics({'dell': {'available': True, 'facts': x}}, NOW+717.75)
        value = next(l.split()[-1] for l in text.splitlines() if l.startswith('cra_source_timestamp_seconds{component="dell"}'))
        self.assertEqual(float(value), x['collected_at'])

    def test_truncated_http_is_reported_as_unavailable(self):
        from http.client import IncompleteRead
        with patch.object(monitor_io.urllib.request, 'build_opener') as build:
            build.return_value.open.return_value.__enter__.return_value.read.side_effect = IncompleteRead(b'partial', 40)
            with self.assertRaises(OSError):
                monitor_io.get_json('http://127.0.0.1/fixture')

    def test_rejects_changed_source_wrong_host_future_and_nan(self):
        for key, bad in (('source_sha256', 'b'*64), ('host_id', 'elsewhere'),
                         ('collected_at', NOW+60), ('collected_at', float('nan'))):
            x = facts('dell'); x[key] = bad
            with self.assertRaises(ValueError):
                observation.validate(x, 'dell', 'a'*64, NOW)

    def test_duplicate_or_unordered_audit_is_rejected(self):
        x = facts('dell', audit=[{'seq': 9}, {'seq': 9}])
        with self.assertRaises(ValueError):
            observation.validate(x, 'dell', 'a'*64, NOW)

    def test_stale_facts_cannot_keep_public_health_good(self):
        snap = {r: {'available': True, 'facts': facts(r)} for r in observation.ROLES}
        snap['loki'] = {'available': True, 'last_event': NOW}
        snap['notifier'] = {'available': True, 'checked_at': NOW, 'deliveries': {}}
        self.assertIn('cra_monitoring_issue 0\n', observation.metrics(snap, NOW))
        self.assertIn('cra_monitoring_issue 0\n', observation.metrics(snap, NOW+30))
        self.assertIn('cra_monitoring_issue 1\n', observation.metrics(snap, NOW+61))
        snap['dell'] = {'available': False}
        self.assertIn('cra_monitoring_issue 1\n', observation.metrics(snap, NOW))

    def test_mismatched_instance_does_not_use_another_streams_high_water(self):
        snap = {r: {'available': True, 'facts': facts(r)} for r in observation.ROLES}
        snap['central']['facts']['data']['streams'][0]['instance_id'] = 'old'
        text = observation.metrics(snap, NOW)
        self.assertNotIn('cra_central_audit_sequence ', text)
        self.assertIn('cra_monitoring_issue 1\n', text)

    def test_cursor_advances_only_after_durable_log_and_does_not_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = {'state_dir': temp, 'log_path': str(Path(temp)/'events.jsonl'),
                   'initial_instance': 'instance', 'initial_after_seq': 9,
                   'sources': {'dell': {}, 'central': {}}}
            row = {'seq': 10, 'ts': '2026-11-12T14:30:23Z', 'kind': 'decision',
                   'payload': {'mode': 'enforce', 'reason': 'HEALTHY'}}
            def pull(role, _, after):
                return facts(role, audit=[row] if role == 'dell' else None)
            c = observation.Collector(cfg, pull=pull)
            with patch.object(observation, 'append', side_effect=OSError('fixture')):
                with self.assertRaises(OSError):
                    c.cycle(NOW)
            self.assertFalse(c.state_path.exists())
            with patch.object(observation, 'get_json', return_value={'status': 'success', 'data': {'result': []}}):
                c.cycle(NOW)
                c.cycle(NOW+15)
            logs = [json.loads(l) for l in Path(cfg['log_path']).read_text().splitlines()]
            self.assertEqual(len([e for e in logs if e['kind'] == 'agent_audit']), 1)
            self.assertEqual(json.loads(c.state_path.read_text())['cursor']['seq'], 10)
            event = next(e for e in logs if e['kind'] == 'agent_audit')
            self.assertEqual(event['source_ts_utc'], row['ts'])
            self.assertNotEqual(event['source_ts_utc'], event['ts_utc'])


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = notify.connect(str(self.root/'notify.sqlite'))
        self.log = str(self.root/'events.jsonl')

    def tearDown(self):
        self.db.close(); self.tmp.cleanup()

    def test_both_routes_once_and_one_resolution_each(self):
        sends = []
        def send(route, body):
            sends.append((route, body)); return 'DELIVERED', 204 if route == 'discord' else 200
        for _ in range(2):
            notify.cycle(self.db, rules(True), NOW, sender=send, log_path=self.log)
        self.assertEqual([s[0] for s in sends], ['discord', 'slack'])
        for _ in range(2):
            notify.cycle(self.db, rules(), NOW+15, sender=send, log_path=self.log)
        self.assertEqual(len(sends), 4)

    def test_unknown_is_held_across_process_restart(self):
        calls = []
        def send(route, body):
            calls.append(route); return 'UNKNOWN', 0
        notify.cycle(self.db, rules(True), NOW, sender=send, log_path=self.log)
        self.db.close(); self.db = notify.connect(str(self.root/'notify.sqlite'))
        notify.cycle(self.db, rules(True), NOW+100, sender=send, log_path=self.log)
        self.assertEqual(len(calls), 2)
        self.assertEqual(dict(self.db.execute('SELECT state,count(*) FROM deliveries GROUP BY state')), {'UNKNOWN': 2})

    def test_crash_after_admission_is_not_retried(self):
        def send(route, body):
            raise RuntimeError('fixture crashed after send admission')
        with self.assertRaises(RuntimeError):
            notify.cycle(self.db, rules(True), NOW, sender=send, log_path=self.log)
        self.db.close(); self.db = notify.connect(str(self.root/'notify.sqlite'))
        self.assertEqual(self.db.execute("SELECT count(*) FROM deliveries WHERE state='UNKNOWN'").fetchone()[0], 1)

    def test_missing_stale_or_unhealthy_rules_cannot_resolve(self):
        for bad in ('missing', 'stale', 'health'):
            x = rules()
            if bad == 'missing': x.pop(next(iter(x)))
            elif bad == 'stale': x[next(iter(x))]['lastEvaluation'] = '2020-01-01T00:00:00Z'
            else: x[next(iter(x))]['health'] = 'err'
            with self.assertRaises(ValueError):
                notify.rules_from({'status': 'success', 'data': {'groups': [{'rules': list(x.values())}]}}, NOW)

    def test_existing_notifier_leaves_explicit_cra_owner_to_dual_route_worker(self):
        alert = {'labels': {'alertname': 'CRACentralVerificationFailed', 'notification_owner': 'cra_monitoring'},
                 'state': 'firing', 'activeAt': '2026-11-12T14:30:23Z'}
        messages, _ = plan([alert], {}, NOW, repeat_sec=3600)
        self.assertEqual(messages, [])
        alert['labels'].pop('notification_owner')
        messages, _ = plan([alert], {}, NOW, repeat_sec=3600)
        self.assertEqual(len(messages), 1)


class StagedCliTests(unittest.TestCase):
    def test_staged_cli_loads_shared_gateway_and_preserves_route_deduplication(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        package = Path(__file__).resolve().parents[1]
        public_root = package.parent
        with tempfile.TemporaryDirectory(prefix='cra-stage-') as temporary:
            root = Path(temporary)
            for name in ('cra_notify.py', 'cra_monitoring_io.py'):
                shutil.copy2(package/'src/stream_monitoring_v4/compatibility'/name, root/name)
            shutil.copy2(public_root/'src/stream_core/common/notification_gateway_client.py',
                         root/'notification_gateway_client.py')
            firing = [True]
            received = []
            opened = datetime.fromtimestamp(time.time()-60, timezone.utc).isoformat()

            class Prometheus(BaseHTTPRequestHandler):
                def do_GET(self):
                    values = rules(firing[0])
                    for rule in values.values():
                        rule['lastEvaluation'] = datetime.now(timezone.utc).isoformat()
                        for alert in rule['alerts']:
                            alert['activeAt'] = opened
                    body = json.dumps({'status': 'success', 'data': {
                        'groups': [{'rules': list(values.values())}]}}).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def log_message(self, *_):
                    pass

            class Gateway(BaseHTTPRequestHandler):
                def do_POST(self):
                    value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    received.append(value)
                    body = json.dumps({'route': value['route'], 'status': 'DELIVERED',
                                       'provider_status': 200, 'delivered': True}).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def log_message(self, *_):
                    pass

            class UnixServer(socketserver.ThreadingUnixStreamServer):
                daemon_threads = True

            with ThreadingHTTPServer(('127.0.0.1', 0), Prometheus) as prom, \
                    UnixServer(str(root/'gateway.sock'), Gateway) as gateway:
                threads = [threading.Thread(target=server.serve_forever, daemon=True)
                           for server in (prom, gateway)]
                for thread in threads:
                    thread.start()
                config = root/'config.json'
                config.write_text(json.dumps({'state_dir': str(root),
                    'log_path': str(root/'events.jsonl'), 'gateway_socket': str(root/'gateway.sock'),
                    'prometheus': 'http://127.0.0.1:'+str(prom.server_port)}))
                try:
                    for value in (True, True, False, False):
                        firing[0] = value
                        result = subprocess.run([sys.executable, '-I', '-B', str(root/'cra_notify.py'),
                            '--config', str(config), '--once', '--test-label', 'public-fixture'],
                            capture_output=True, text=True, timeout=10)
                        self.assertEqual(result.returncode, 0, result.stderr)
                finally:
                    prom.shutdown()
                    gateway.shutdown()
                    for thread in threads:
                        thread.join(timeout=2)
            self.assertEqual([row['route'] for row in received], ['discord', 'slack']*2)
            self.assertTrue(all('public-fixture' in row['content'] for row in received))
            with sqlite3.connect(root/'deliveries.sqlite3') as connection:
                self.assertEqual(connection.execute('SELECT state,count(*) FROM deliveries GROUP BY state').fetchall(),
                                 [('DELIVERED', 4)])


if __name__ == '__main__':
    unittest.main()
