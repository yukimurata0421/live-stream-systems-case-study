"""Evaluate actual cloud output facts and continuous-health backup-stop conditions."""
import copy
import json
import sys
import threading
import tempfile
import types
import time
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ops/selfheal'))
import fallback_controller as control
from route_facts_http import FactsServer
import route_facts_http as facts_http
import importlib.util
_observer_path = Path(__file__).resolve().parents[1] / 'monitoring-v4/src/stream_monitoring_v4/compatibility/relay_observe.py'
_observer_spec = importlib.util.spec_from_file_location('public_selfheal_relay_observe', _observer_path)
relay_observe = importlib.util.module_from_spec(_observer_spec)
_observer_spec.loader.exec_module(relay_observe)

MACHINE = 'a' * 64
DESTINATION = 'b' * 64
STAMP = '2026-10-25T00:00:00+00:00'
NOW = datetime.fromisoformat(STAMP).timestamp() + 1


def facts(n, *, session=1, stamp=STAMP):
    ident = f'youtube-forward-123-{session}'
    return {'schema': 'stream_v3.route_facts.v1', 'copy_observation': {'schema':'stream_v3.copy_observation.v1','age_seconds':0.,'confirmed':True,'new_observation':True}, 'relay': {
        'schema': 'stream.relay_raw.v1', 'host_id': control.EXPECTED_HOST_ID, 'machine_id_sha256': MACHINE,
        'boot_id': 'boot', 'observed_at': stamp, 'observed_boottime_seconds':control.clock.mono(), 'monotonic_seconds': 1000 + n * 15,
        'relay': {'active': True, 'pid': 855},
        'input': {'available': True, 'ready': True, 'connection_id': 'input', 'bytes': 10000 + n * 10000},
        'destinations': {'youtube': {'id': ident, 'state': 'forwarding', 'bytes': 1}}},
        'youtube_copy': {'schema': 'stream_v3.youtube_forward.v1', 'pid': 123, 'session': session,
                         'boot_id': 'boot', 'state': 'FLOWING', 'updated_at': datetime.fromtimestamp(datetime.fromisoformat(stamp).timestamp()+n*.00001, timezone.utc).isoformat(),
                         'last_progress_at': stamp, 'destination_fingerprint': DESTINATION,
                         'output_bytes': 1000 + n * 9000, 'video_packets': 1 + n * 75,
                         'audio_packets': 1 + n * 700}}


def clock_model(now=NOW, *, offset=0., uncertainty=0.):
    mark = control.clock.capture()
    remote_minus_local = now + offset - mark['wall']
    return control.clock.Estimate(remote_minus_local-uncertainty, remote_minus_local+uncertainty,
                                  mark['wall'], mark['mono'], 'boot', MACHINE, 'local', 2*uncertainty)


def sample(n, **kwargs):
    return control.copy_route_sample(facts(n, **kwargs), machine=MACHINE, destination=DESTINATION, received=n * 15, source_age=0.0)


class CopyFactsTests(unittest.TestCase):





    def test_observed_millisecond_skew_neither_starts_backup_nor_resets_recovery(self):
        standby, running = control.Controller(), control.Controller()
        for n in range(22):
            now = NOW + n * 15
            ahead = (.003, .065, 0, -.01)[n % 4]
            stamp = datetime.fromtimestamp(now + ahead, timezone.utc).isoformat()
            diagnostic = {}
            value = control.copy_route_sample(facts(n, stamp=stamp), machine=MACHINE, destination=DESTINATION, received=n * 15, diagnostic=diagnostic, source_age=0.0)
            self.assertIsNotNone(value)
            self.assertIsNone(standby.decide(n * 15, standby.healthy(value), 0))
            action = running.decide(n * 15, running.healthy(value), 1)
            self.assertEqual(action, 'STOP' if n == 21 else None)
        self.assertEqual(running.healthy_since, 15)
        self.assertEqual(running.healthy_resets_total, 0)
        self.assertEqual(standby.healthy_resets_total, 0)

    def test_reset_reason_survives_recovery_and_status_round_trip(self):
        c = control.Controller()
        c.decide(0, c.healthy(sample(0)), 1)
        c.decide(15, c.healthy(sample(1)), 1)
        c.decide(30, c.healthy(None, 'facts_timeout'), 1)
        status = control.status_snapshot(c, {'http_status': None}, 30, 1)
        self.assertEqual((status['health'], status['reason'], status['healthy_for_sec']), ('UNKNOWN', 'facts_timeout', 0))
        self.assertEqual((status['healthy_resets_total'], status['last_reset_reason']), (1, 'facts_timeout'))
        c.decide(45, c.healthy(sample(3)), 1)
        c.decide(60, c.healthy(sample(4)), 1)
        status = control.status_snapshot(c, {}, 75, 1)
        self.assertEqual((status['health'], status['healthy_for_sec'], status['last_reset_reason']), ('HEALTHY', 15, 'facts_timeout'))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'status.json'
            control.write_status(status, path)
            self.assertEqual(json.loads(path.read_text()), status)
            self.assertEqual(path.stat().st_mode & 0o777, 0o644)
            self.assertEqual(list(path.parent.iterdir()), [path])

    def test_frozen_output_and_observation_loss_have_distinct_states(self):
        c = control.Controller(); c.healthy(sample(0))
        after = sample(1); after['youtube']['outbound'] = sample(0)['youtube']['outbound']
        self.assertFalse(c.healthy(after))
        self.assertEqual((c.health, c.reason), ('UNHEALTHY', 'output_not_advancing'))
        self.assertFalse(c.healthy(None, 'copy_stale'))
        self.assertEqual((c.health, c.reason), ('UNKNOWN', 'copy_stale'))

    def test_actual_output_is_used_instead_of_loopback_counter(self):
        row = sample(2)
        self.assertEqual(row['youtube']['outbound'], 19000)
        self.assertEqual(row['youtube']['audio_packets'], 1401)
        self.assertEqual(row['bytes_received'], 30000)

    def test_binding_freshness_or_missing_track_fails_closed(self):
        changes = [lambda d: d.update(youtube_copy=None),
                   lambda d: d['relay'].update(machine_id_sha256='other'),
                   lambda d: d['copy_observation'].update(confirmed=False),
                   lambda d: d['copy_observation'].update(age_seconds=21),
                   lambda d: d['youtube_copy'].update(destination_fingerprint='other'),
                   lambda d: d['youtube_copy'].update(boot_id='other'),
                   lambda d: d['youtube_copy'].update(state='bogus'),
                   lambda d: d['youtube_copy'].update(output_bytes=True),
                   lambda d: d['relay']['input'].update(connection_id=''),
                   lambda d: d['relay'].update(monotonic_seconds=float('nan'))]
        for change in changes:
            d = facts(1); change(d)
            with self.subTest(change=change):
                self.assertIsNone(control.copy_route_sample(d, machine=MACHINE, destination=DESTINATION, received=1, source_age=0.0))

    def test_full_300_seconds_follow_first_progress_observation(self):
        c = control.Controller()
        for n in range(21):
            self.assertIsNone(c.decide(n * 15, c.healthy(sample(n)), 1))
        self.assertEqual(c.decide(315, c.healthy(sample(21)), 1), 'STOP')
        self.assertEqual(c.healthy_since, 15)

    def test_unknown_resets_the_stability_window(self):
        c = control.Controller()
        for n in range(20):
            self.assertIsNone(c.decide(n * 15, c.healthy(sample(n)), 1))
        self.assertIsNone(c.decide(300, c.healthy(None), 1))
        for n in range(21, 42):
            self.assertIsNone(c.decide(n * 15, c.healthy(sample(n)), 1))
        self.assertEqual(c.decide(630, c.healthy(sample(42)), 1), 'STOP')

    def test_new_session_gap_or_one_frozen_track_is_not_healthy(self):
        for kind in ('session', 'gap', 'audio', 'video', 'output', 'input'):
            c = control.Controller(); before = sample(1); after = sample(2)
            c.healthy(before)
            if kind == 'session': after = sample(2, session=2)
            elif kind == 'gap': after['source_monotonic'] += 100
            elif kind in ('audio', 'video'): after['youtube'][kind + '_packets'] = before['youtube'][kind + '_packets']
            elif kind == 'output': after['youtube']['outbound'] = before['youtube']['outbound']
            else: after['bytes_received'] = before['bytes_received']
            with self.subTest(kind=kind): self.assertFalse(c.healthy(after))

    def test_missing_observations_without_expired_deadline_do_not_start_backup(self):
        c = control.Controller()
        self.assertIsNone(c.decide(0, c.healthy(None), 0))
        self.assertIsNone(c.decide(29, c.healthy(None), 0))
        self.assertIsNone(c.decide(30, c.healthy(None), 0))

    def test_copy_record_age_adds_to_source_age_and_requires_producer_confirmation(self):
        for change in ({'confirmed':False},{'age_seconds':20},{'confirmed':1},
                       {'age_seconds':float('nan')},{'new_observation':1}):
            row=facts(1);row['copy_observation'].update(change)
            self.assertIsNone(control.copy_route_sample(row,machine=MACHINE,
                destination=DESTINATION,received=15,source_age=1))
        row=facts(1);row.pop('copy_observation')
        self.assertIsNone(control.copy_route_sample(row,machine=MACHINE,
            destination=DESTINATION,received=15,source_age=0))

    def test_source_age_must_be_finite_nonnegative_and_within_budget(self):
        for age in (None,True,-1,20.001,float('nan'),float('inf')):
            diagnostic={}
            self.assertIsNone(control.copy_route_sample(facts(1),machine=MACHINE,
                destination=DESTINATION,received=15,source_age=age,diagnostic=diagnostic))
            self.assertEqual(diagnostic['reason'],'source_freshness_unavailable')
        self.assertIsNotNone(control.copy_route_sample(facts(1),machine=MACHINE,
            destination=DESTINATION,received=15,source_age=20))

    def test_utc_labels_do_not_decide_boot_bound_media_freshness(self):
        for stamp in ('2020-01-01T00:00:00+00:00','2099-01-01T00:00:00+00:00','invalid'):
            row=facts(1);row['relay']['observed_at']=stamp
            row['youtube_copy'].update(updated_at=stamp,last_progress_at=stamp)
            self.assertIsNotNone(control.copy_route_sample(row,machine=MACHINE,
                destination=DESTINATION,received=15,source_age=1))


class FactsHTTPTests(unittest.TestCase):
    def server(self, collector, peer='127.0.0.1'):
        server = FactsServer(('127.0.0.1', 0), collector, peer)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f'http://127.0.0.1:{server.server_port}'

    def test_real_http_observation_accepts_small_remote_clock_skew(self):
        def collect():
            stamp = (datetime.now(timezone.utc) + timedelta(milliseconds=65)).isoformat()
            return facts(1, stamp=stamp)
        url = self.server(collect)
        diagnostic = {}
        capture = facts_http.clock.capture
        def remote_capture():
            value = capture(); value['wall'] += .065; return value
        with mock.patch.multiple(control, FACTS_URL=url + '/facts', EXPECTED_MACHINE=MACHINE,
                                 EXPECTED_DESTINATION=DESTINATION), mock.patch.object(facts_http.clock, 'capture', remote_capture):
            self.assertIsNotNone(control.observe(diagnostic))
        self.assertEqual(diagnostic['reason'], 'baseline_required')
        self.assertGreaterEqual(diagnostic['http_elapsed_seconds'], 0)
        self.assertLessEqual(datetime.fromisoformat(diagnostic['http_started_at']),
                             datetime.fromisoformat(diagnostic['http_completed_at']))
        status = control.status_snapshot(control.Controller(), diagnostic, 0, 0)
        self.assertEqual(status['http_elapsed_seconds'], diagnostic['http_elapsed_seconds'])
        self.assertGreater(status['controller_pid'], 0)

    def test_old_server_never_downgrades_to_unverified_timestamps(self):
        for reject_query in (False,True):
            class LegacyHandler(facts_http.FactsHandler):
                def do_GET(self):
                    if reject_query and '?' in self.path:
                        self.reply(404,{'error':'not_found'})
                    else:
                        self.reply(200,facts(1,stamp=datetime.now(timezone.utc).isoformat()))
            server=facts_http.HTTPServer(('127.0.0.1',0),LegacyHandler)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                diagnostic={}
                with mock.patch.multiple(control,FACTS_URL=f'http://127.0.0.1:{server.server_port}/facts',
                                         EXPECTED_MACHINE=MACHINE,EXPECTED_DESTINATION=DESTINATION):
                    value=control.observe(diagnostic)
                self.assertIsNone(value)
                reason='facts_http_unavailable' if reject_query else 'facts_invalid'
                self.assertEqual(diagnostic['reason'],reason)
                c=control.Controller()
                self.assertIsNone(c.decide(0,c.healthy(value,reason),0))
                self.assertIsNone(c.decide(30,c.healthy(value,reason),0))
                self.assertEqual(c.health,'UNKNOWN')
            finally:server.shutdown();server.server_close()

    def test_real_http_read_uses_bound_facts_and_has_no_write_method(self):
        count = []
        def collect():
            count.append(1)
            return facts(len(count), stamp=datetime.now(timezone.utc).isoformat())
        url = self.server(collect)
        with mock.patch.multiple(control, FACTS_URL=url + '/facts', EXPECTED_MACHINE=MACHINE,
                                 EXPECTED_DESTINATION=DESTINATION):
            value = control.observe()
        self.assertIsNotNone(value)
        self.assertEqual(value['youtube']['outbound'], 10000)
        req = urllib.request.Request(url + '/facts', data=b'{}', method='POST')
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(req, timeout=2)
        self.assertEqual(error.exception.code, 405)
        error.exception.close()
        self.assertEqual(len(count), 1)

    def test_other_peer_cannot_read_or_trigger_collection(self):
        collector = mock.Mock(return_value=facts(1))
        url = self.server(collector, peer='192.0.2.1')
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(url + '/facts', timeout=2)
        self.assertEqual(error.exception.code, 403)
        error.exception.close()
        collector.assert_not_called()

    def test_unavailable_or_oversized_facts_never_return_success(self):
        for collector in (mock.Mock(side_effect=ValueError()), lambda: {'large': 'x' * 20000}):
            url = self.server(collector)
            with self.assertRaises(urllib.error.HTTPError) as error:
                control.get_url(url + '/facts')
            self.assertEqual(error.exception.code, 503)
            error.exception.close()

    def test_observation_failure_keeps_sanitized_http_reason(self):
        url = self.server(mock.Mock(side_effect=ValueError('private-source-error')))
        diagnostic = {}
        with mock.patch.object(control, 'FACTS_URL', url + '/facts'):
            self.assertIsNone(control.observe(diagnostic))
        self.assertEqual(diagnostic['reason'], 'facts_http_unavailable')
        self.assertEqual(diagnostic['http_status'], 503)
        self.assertGreaterEqual(diagnostic['http_elapsed_seconds'], 0)
        self.assertIsNotNone(diagnostic['http_completed_at'])
        self.assertNotIn('private-source-error', json.dumps(diagnostic))


class CollectorBindingTests(unittest.TestCase):
    def test_fresh_bound_copy_error_survives_idle_bridge_and_can_confirm_input_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            module,replies=self.fixture(directory)
            replies[0].update(ready=False,online=False,source=None)
            replies[1]['items'][0].update(state='idle',lastError='source unavailable')
            cp=json.loads(module.YOUTUBE_STATUS.read_text());cp['state']='ERROR'
            cp['last_progress_at']=(datetime.now(timezone.utc)-timedelta(seconds=45)).isoformat()
            module.YOUTUBE_STATUS.write_text(json.dumps(cp))
            with mock.patch.object(facts_http,'unit_pid',side_effect=[855,123,855,123]), \
                 mock.patch.object(facts_http,'api',side_effect=replies):
                value=facts_http.collect(module)
            self.assertIsNotNone(value['youtube_copy'])
            self.assertEqual(value['relay']['destinations']['youtube']['state'],'unknown')
            c=control.Controller()
            for n in (0,15,30):
                diagnostic={};mark=control.clock.capture();model=control.clock.Estimate(
                    -.005,.005,mark['wall'],mark['mono'],value['relay']['boot_id'],
                    value['relay']['machine_id_sha256'],control.clock.identity()['boot_id'],.01)
                value['copy_observation']['confirmed']=True
                sample=control.copy_route_sample(value, machine=value['relay']['machine_id_sha256'], destination=DESTINATION, received=n, diagnostic=diagnostic, source_age=0.0)
                self.assertIsNotNone(sample)
                self.assertEqual(c.decide(n,c.healthy(sample,diagnostic['reason']),0),'START' if n==30 else None)

    def test_idle_bridge_does_not_bypass_copy_identity_or_freshness(self):
        for changed in ({'pid':999},{'destination_fingerprint':'other'},
                        {'updated_at':(datetime.now(timezone.utc)-timedelta(seconds=60)).isoformat()}):
            with self.subTest(changed=changed),tempfile.TemporaryDirectory() as directory:
                module,replies=self.fixture(directory);replies[0].update(ready=False,online=False,source=None)
                replies[1]['items'][0]['state']='idle'
                cp=json.loads(module.YOUTUBE_STATUS.read_text());cp.update(state='ERROR',**changed)
                module.YOUTUBE_STATUS.write_text(json.dumps(cp))
                with mock.patch.object(facts_http,'unit_pid',side_effect=[855,123,855,123]), \
                     mock.patch.object(facts_http,'api',side_effect=replies):
                    value=facts_http.collect(module)
                self.assertIsNone(value['youtube_copy'])

    def fixture(self, directory):
        d = Path(directory)
        cp = facts(1, stamp=datetime.now(timezone.utc).isoformat())['youtube_copy']
        cp['boot_id'] = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        (d / 'status.json').write_text(json.dumps(cp))
        (d / 'fingerprint').write_text(DESTINATION)
        module = types.SimpleNamespace(YOUTUBE_STATUS=d / 'status.json', YOUTUBE_FINGERPRINT=d / 'fingerprint',
            YOUTUBE_BRIDGE='rtmp://127.0.0.1:19350/live/youtube',
            youtube_forward_facts=relay_observe.youtube_forward_facts, provider=relay_observe.provider,
            input_facts=relay_observe.input_facts)
        path = {'ready': True, 'source': {'id': 'input'}, 'bytesReceived': 10000}
        rows = {'items': [{'conf': {'dest': module.YOUTUBE_BRIDGE}, 'state': 'forwarding'}]}
        return module, [path, rows]

    def test_collector_uses_read_only_sources_and_exact_process_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            module, replies = self.fixture(directory)
            with mock.patch.object(facts_http, 'unit_pid', side_effect=[855, 123, 855, 123]), \
                 mock.patch.object(facts_http, 'api', side_effect=replies):
                value = facts_http.collect(module)
            self.assertEqual(value['youtube_copy']['output_bytes'], 10000)
            self.assertEqual(value['relay']['destinations']['youtube']['id'], 'youtube-forward-123-1')

    def test_process_change_during_collection_cannot_be_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            module, replies = self.fixture(directory)
            with mock.patch.object(facts_http, 'unit_pid', side_effect=[855, 123, 999]), \
                 mock.patch.object(facts_http, 'api', side_effect=replies):
                with self.assertRaisesRegex(ValueError, 'unit_process_changed'):
                    facts_http.collect(module)

    def test_missing_or_duplicate_bridge_is_not_a_route_match(self):
        for count in (0, 2):
            with tempfile.TemporaryDirectory() as directory:
                module, replies = self.fixture(directory)
                replies[1]['items'] *= count
                with mock.patch.object(facts_http, 'unit_pid', side_effect=[855, 123]), \
                     mock.patch.object(facts_http, 'api', side_effect=replies):
                    with self.assertRaisesRegex(ValueError, 'route_binding_mismatch'):
                        facts_http.collect(module)


if __name__ == '__main__':
    unittest.main()
