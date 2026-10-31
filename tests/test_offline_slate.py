"""Check slate candidates, preview boundaries and source-loss classification."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ops/selfheal'))
import offline_slate
from test_fallback_copy_route import relay_observe
input_facts = relay_observe.input_facts
import fallback_controller as control
from test_fallback_copy_route import clock_model, facts, NOW, MACHINE, DESTINATION


class OfflineConfigTests(unittest.TestCase):
    def test_candidate_changes_only_offline_fields_and_preserves_both_forwards(self):
        base = {'rtmpAddress': '127.0.0.1:1935', 'authInternalUsers': [{'pass': 'private-fixture'}],
                'paths': {'relay': {'source': 'publisher', 'forward': [{'dest': 'youtube-fixture'},
                                                                          {'dest': 'twitch-fixture'}]},
                          'other': {'source': 'publisher'}}}
        original = copy.deepcopy(base)
        candidate = offline_slate.candidate_config(base)
        self.assertEqual(base, original)
        for key in offline_slate.path_patch():
            candidate['paths']['relay'].pop(key)
        self.assertEqual(candidate, original)

    def test_wrong_source_or_ambiguous_runtime_path_is_rejected(self):
        for path in ('relative.mp4', '/tmp/video.flv', '/tmp/line\nbreak.mp4'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                offline_slate.path_patch(path)
        with self.assertRaises(ValueError):
            offline_slate.candidate_config({'paths': {'relay': {'source': 'rtsp://fixture', 'forward': []}}})

    def test_private_candidate_is_exclusive_and_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            offline_slate.write_json(path, {'private': 'fixture'}, private=True)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                offline_slate.write_json(path, {'replaced': True})
            self.assertEqual(json.loads(path.read_text()), {'private': 'fixture'})


class OfflineObservationTests(unittest.TestCase):
    def test_ready_slate_with_increasing_bytes_is_not_a_live_input(self):
        value = input_facts({'ready': True, 'online': False, 'source': None, 'bytesReceived': 10000})
        self.assertTrue(value['stream_available'])
        self.assertFalse(value['ready'])
        self.assertEqual(value['content_state'], 'OFFLINE_SLATE')
        self.assertIsNone(value['connection_id'])

    def test_real_publisher_and_legacy_api_have_source_binding(self):
        for online in ({'online': True}, {}):
            value = input_facts({'ready': True, 'source': {'id': 'publisher'}, 'bytesReceived': 1, **online})
            self.assertTrue(value['ready'])
            self.assertEqual(value['content_state'], 'LIVE')
        self.assertFalse(input_facts({'ready': True, 'online': True, 'source': None})['ready'])
        self.assertFalse(input_facts({'ready': True, 'online': 'true', 'source': {'id': 'p'}})['ready'])

    def test_slate_output_cannot_stop_backup_or_hide_sustained_input_loss(self):
        c = control.Controller()
        for n in range(24):
            value = facts(n)
            value['relay']['input'].update(online=False, content_state='OFFLINE_SLATE')
            sample = control.copy_route_sample(value, machine=MACHINE, destination=DESTINATION,
                                               received=n * 15, source_age=0.)
            self.assertFalse(sample["ready"])
            self.assertIsNone(c.decide(n * 15, c.healthy(sample), 1))
        self.assertIsNone(c.healthy_since)
        self.assertEqual(c.decide(360, False, 0), 'START')

    def test_legacy_direct_observer_also_rejects_slate(self):
        with mock.patch.object(control, 'FACTS_URL', ''), mock.patch.object(control, 'get', side_effect=[
                {'ready': True, 'online': False, 'source': None, 'bytesReceived': 99},
                {'items': []}]):
            self.assertFalse(control.observe()['ready'])


class SlatePreviewHTTPTests(unittest.TestCase):
    def test_only_slate_and_static_map_are_exposed(self):
        server = offline_slate.ThreadingHTTPServer(('127.0.0.1', 0), offline_slate.PreviewHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        root = f'http://127.0.0.1:{server.server_port}'
        with urllib.request.urlopen(root, timeout=2) as response:
            self.assertIn('No data received', response.read().decode())
        with urllib.request.urlopen(root + '/adsb-map/airports.geojson', timeout=2) as response:
            self.assertEqual(json.load(response)['type'], 'FeatureCollection')
        for path in ('/data/aircraft.json', '/now_playing.json', '/../../.git/config', '/map/../../AGENTS.md'):
            with self.subTest(path=path), self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(root + path, timeout=2)
            self.assertEqual(error.exception.code, 404)
            error.exception.close()
        request = urllib.request.Request(root + '/render/ready', data=b'{}', method='POST')
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request, timeout=2)
        self.assertEqual(error.exception.code, 405)
        error.exception.close()
