"""Bind delivery decisions to actual copy output and require continuous health before stopping backup."""
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
from datetime import datetime, timezone
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
    return {'schema': 'stream_v3.route_facts.v1', 'relay': {
        'schema': 'stream.relay_raw.v1', 'host_id': 'relay-host', 'machine_id_sha256': MACHINE,
        'boot_id': 'boot', 'observed_at': stamp, 'monotonic_seconds': 1000 + n * 15,
        'relay': {'active': True, 'pid': 855},
        'input': {'available': True, 'ready': True, 'connection_id': 'input', 'bytes': 10000 + n * 10000},
        'destinations': {'youtube': {'id': ident, 'state': 'forwarding', 'bytes': 1}}},
        'youtube_copy': {'schema': 'stream_v3.youtube_forward.v1', 'pid': 123, 'session': session,
                         'boot_id': 'boot', 'state': 'FLOWING', 'updated_at': stamp,
                         'last_progress_at': stamp, 'destination_fingerprint': DESTINATION,
                         'output_bytes': 1000 + n * 9000, 'video_packets': 1 + n * 75,
                         'audio_packets': 1 + n * 700}}


def sample(n, **kwargs):
    return control.copy_route_sample(facts(n, **kwargs), now=NOW, machine=MACHINE,
                                     destination=DESTINATION, received=n * 15)


class CopyFactsTests(unittest.TestCase):
    def test_actual_output_is_used_instead_of_loopback_counter(self):
        row = sample(2)
        self.assertEqual(row['youtube']['outbound'], 19000)
        self.assertEqual(row['youtube']['audio_packets'], 1401)
        self.assertEqual(row['bytes_received'], 30000)

    def test_binding_freshness_or_missing_track_fails_closed(self):
        changes = [lambda d: d.update(youtube_copy=None),
                   lambda d: d['relay'].update(machine_id_sha256='other'),
                   lambda d: d['relay'].update(observed_at='2026-10-24T23:59:00Z'),
                   lambda d: d['youtube_copy'].update(updated_at='2026-10-25T00:01:00Z'),
                   lambda d: d['youtube_copy'].update(destination_fingerprint='other'),
                   lambda d: d['youtube_copy'].update(boot_id='other'),
                   lambda d: d['youtube_copy'].update(state='ERROR'),
                   lambda d: d['youtube_copy'].update(video_packets=0),
                   lambda d: d['youtube_copy'].update(audio_packets=0),
                   lambda d: d['youtube_copy'].update(output_bytes=True),
                   lambda d: d['relay']['input'].update(connection_id=''),
                   lambda d: d['relay'].update(monotonic_seconds=float('nan'))]
        for change in changes:
            d = facts(1); change(d)
            with self.subTest(change=change):
                self.assertIsNone(control.copy_route_sample(d, now=NOW, machine=MACHINE,
                                                            destination=DESTINATION, received=1))

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

    def test_failure_still_starts_backup_after_30_seconds(self):
        c = control.Controller()
        self.assertIsNone(c.decide(0, c.healthy(None), 0))
        self.assertIsNone(c.decide(29, c.healthy(None), 0))
        self.assertEqual(c.decide(30, c.healthy(None), 0), 'START')


class FactsHTTPTests(unittest.TestCase):
    def server(self, collector, peer='127.0.0.1'):
        server = FactsServer(('127.0.0.1', 0), collector, peer)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f'http://127.0.0.1:{server.server_port}'

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


class CollectorBindingTests(unittest.TestCase):
    def fixture(self, directory):
        d = Path(directory)
        cp = facts(1, stamp=datetime.now(timezone.utc).isoformat())['youtube_copy']
        cp['boot_id'] = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        (d / 'status.json').write_text(json.dumps(cp))
        (d / 'fingerprint').write_text(DESTINATION)
        module = types.SimpleNamespace(YOUTUBE_STATUS=d / 'status.json', YOUTUBE_FINGERPRINT=d / 'fingerprint',
            YOUTUBE_BRIDGE='rtmp://127.0.0.1:19350/live/youtube',
            youtube_forward_facts=relay_observe.youtube_forward_facts, provider=relay_observe.provider)
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
