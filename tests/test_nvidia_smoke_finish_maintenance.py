import importlib.util
import hashlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bridge = load('nvidia_finish', ROOT / 'ops/host-maintenance/bin/nvidia_smoke_finish_maintenance.py')
fixture = load('maintenance_fixture_for_nvidia', ROOT / 'tests/test_manual_maintenance.py')
# Always exercise the public core; never import an installed production release.
core = fixture.manual_maintenance


class ConfirmationObservationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = fixture.Store(Path(directory.name) / 'maintenance')
        self.store.initialize()
        self.clock = fixture.Clock()
        self.backend = fixture.Backend(self.store.root, self.clock)
        self.mode = core.Maintenance(self.store, self.backend, fixture.config(), clock=self.clock.time,
                                       sleep=self.clock.sleep, interval=2, stable_seconds=300)
        self.mode.begin(minutes=90, reason='NVIDIA smoke fixture')
        self.mode.confirm_slate(youtube=True, twitch=True)
        self.mode.resume()

    def confirm(self, answer='yes\n', *, fault=None):
        polls = 0

        def wait(streams, *_):
            nonlocal polls
            polls += 1
            self.clock.sleep(5)
            if polls == 8 and fault:
                fault()
            return (streams if polls >= 20 else [], [], [])

        return bridge.confirm_while_observing(self.mode, stream=io.StringIO(answer), output=io.StringIO(), wait=wait)

    def test_legacy_blocking_input_reproduces_gap_and_retains_guard(self):
        def old_confirmation():
            self.clock.sleep(100)
            return True
        with self.assertRaisesRegex(ValueError, 'progress_sample_gap'):
            self.mode.off(confirm=old_confirmation)
        self.assertEqual(self.store.read()['phase'], 'VERIFYING')

    def test_100_second_operator_wait_keeps_real_checks_and_completes(self):
        self.mode.off(confirm=self.confirm)
        self.assertEqual(self.store.read()['phase'], 'NORMAL')
        self.assertEqual(self.backend.calls.count('restore_units'), 1)

    def test_av_failure_during_input_wait_never_releases_guard(self):
        with self.assertRaisesRegex(ValueError, 'audio_video_or_output_stalled'):
            self.mode.off(confirm=lambda: self.confirm(fault=lambda: setattr(self.backend, 'freeze_audio', True)))
        self.assertEqual(self.store.read()['phase'], 'VERIFYING')
        self.assertNotIn('restore_units', self.backend.calls)

    def test_authority_drift_during_wait_never_releases_guard(self):
        with self.assertRaisesRegex(ValueError, 'authority_not_quiescent'):
            self.mode.off(confirm=lambda: self.confirm(fault=lambda: self.backend.ledger.update(unresolved=1)))
        self.assertNotIn('restore_units', self.backend.calls)

    def test_no_and_eof_keep_inhibition(self):
        for answer in ('no\n', ''):
            with self.assertRaisesRegex(ValueError, 'live_playback_not_confirmed'):
                self.mode.off(confirm=lambda: self.confirm(answer))
            self.assertEqual(self.store.read()['phase'], 'VERIFYING')

    def test_public_core_pins_bind_to_the_public_repository(self):
        for name, expected in bridge.PINS.items():
            with self.subTest(name=name):
                self.assertEqual(hashlib.sha256((ROOT / 'ops/selfheal' / name).read_bytes()).hexdigest(), expected)

    def test_cold_cli_rejects_confirmation_flags_without_operating_on_host(self):
        result = subprocess.run(['/usr/bin/python3', '-I', '-B', bridge.__file__, '--yes'],
                                input='', text=True, capture_output=True, cwd='/', timeout=5)
        self.assertEqual(result.returncode, 1)
        self.assertIn('root_terminal_without_confirmation_flags_required', result.stdout)
        self.assertIn('"inhibition_retained": true', result.stdout)


if __name__ == '__main__':
    unittest.main()
