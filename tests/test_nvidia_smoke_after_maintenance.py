import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('after_maintenance', ROOT / 'ops/host-maintenance/bin/nvidia_smoke_after_maintenance.py')
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class SmokeAfterMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'smoke'
        self.maintenance = Path(self.temp.name) / 'session.json'
        self.runner = launcher.Launcher(self.root, self.maintenance, uid=os.getuid())
        self.session = {'schema': 'stream_v3.manual_maintenance.v1', 'phase': 'MAINTENANCE',
                        'session_id': 'new-session', 'generation': 3}
        self.save_session()
        self.active = self.enabled = False
        self.starts = self.enables = 0
        self.start_outcome = 'RUNNING'
        self.timeout = False
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(launcher, 'machine', return_value='machine').start()
        mock.patch.object(launcher.time, 'time', return_value=1000).start()
        original = Path.read_text

        def read(path, *args, **kwargs):
            if str(path) == '/sys/module/nvidia/version':
                return '580.178.04'
            return original(path, *args, **kwargs)

        mock.patch.object(Path, 'read_text', read).start()
        mock.patch.object(launcher, 'command', side_effect=self.command).start()

    def save_session(self):
        self.maintenance.write_text(json.dumps(self.session))
        self.maintenance.chmod(0o600)

    def normal(self):
        self.session.update(phase='NORMAL', completed_at=1100, live_player_confirmation={
            'kind': 'operator_attestation', 'at': 1090, 'youtube': True, 'twitch': True, 'live_data': True})
        self.save_session()

    def create_run(self, status='RUNNING'):
        db = sqlite3.connect(self.root / 'smoke.sqlite3')
        try:
            db.executescript('PRAGMA application_id=1314280241; PRAGMA user_version=1; CREATE TABLE runs(id TEXT,status TEXT);')
            db.execute('INSERT INTO runs VALUES (?,?)', ('run', status))
            db.commit()
        finally:
            db.close()

    def command(self, args, *, timeout=70):
        output = ''
        if 'start' in args:
            self.starts += 1
            if self.timeout:
                raise subprocess.TimeoutExpired(args, timeout)
            self.create_run(self.start_outcome)
        elif 'is-active' in args:
            output = 'active' if self.active else 'inactive'
        elif 'is-enabled' in args:
            output = 'enabled' if self.enabled else 'disabled'
        elif 'enable' in args:
            self.enables += 1
            self.active = self.enabled = True
        return subprocess.CompletedProcess(args, 0, output, '')

    def arm(self):
        return self.runner.arm('run', '580.178.04')

    def test_only_after_confirmed_normal_and_only_once(self):
        self.arm()
        self.assertEqual(self.runner.tick()['waiting_for'], 'NORMAL')
        self.assertEqual(self.starts, 0)
        self.normal()
        self.assertEqual(self.runner.tick()['stage'], 'STARTED')
        self.assertEqual(self.runner.tick()['stage'], 'STARTED')
        self.assertEqual((self.starts, self.enables), (1, 1))

    def test_arm_rejects_normal_and_rearm(self):
        self.normal()
        with self.assertRaisesRegex(ValueError, 'ACTIVE_MAINTENANCE_REQUIRED'):
            self.arm()
        self.session['phase'] = 'MAINTENANCE'
        self.save_session()
        self.arm()
        with self.assertRaisesRegex(ValueError, 'LAUNCH_ALREADY_ARMED'):
            self.arm()

    def test_missing_or_old_public_confirmation_never_starts(self):
        self.arm()
        self.normal()
        for value in ({}, {'kind': 'operator_attestation', 'at': 900, 'youtube': True, 'twitch': True, 'live_data': True}):
            self.session['live_player_confirmation'] = value
            self.save_session()
            with self.assertRaisesRegex(ValueError, 'LIVE_PLAYER_CONFIRMATION_MISSING'):
                self.runner.tick()
        self.assertEqual(self.starts, 0)

    def test_wrong_session_or_generation_never_starts(self):
        self.arm()
        self.normal()
        self.session['session_id'] = 'other-session'
        self.save_session()
        with self.assertRaisesRegex(ValueError, 'MAINTENANCE_SESSION_CHANGED'):
            self.runner.tick()

    def test_wrong_driver_waits_without_creating_run(self):
        self.arm()
        self.normal()
        with mock.patch.object(Path, 'read_text', return_value='580.173.02'):
            self.assertEqual(self.runner.tick()['waiting_for'], 'EXPECTED_DRIVER')
        self.assertEqual(self.starts, 0)

    def test_unknown_start_is_not_resent(self):
        self.arm()
        self.normal()
        self.timeout = True
        self.assertEqual(self.runner.tick()['stage'], 'UNKNOWN')
        self.assertEqual(self.runner.tick()['stage'], 'UNKNOWN')
        self.assertEqual(self.starts, 1)

    def test_recorded_failed_campaign_is_not_restarted(self):
        self.arm()
        self.normal()
        self.start_outcome = 'FAILED'
        self.assertEqual(self.runner.tick()['stage'], 'TERMINAL')
        self.runner.tick()
        self.assertEqual((self.starts, self.enables), (1, 0))

    def test_crash_after_start_reconciles_database_without_resend(self):
        value = self.arm()
        self.create_run()
        value['stage'] = 'START_REQUESTED'
        self.runner.save(value)
        self.assertEqual(self.runner.tick()['stage'], 'STARTED')
        self.assertEqual((self.starts, self.enables), (0, 1))

    def test_unknown_timer_effect_is_not_resent(self):
        value = self.arm()
        self.create_run()
        value.update(stage='START_REQUESTED', timer_requested=True)
        self.runner.save(value)
        self.assertEqual(self.runner.tick()['stage'], 'UNKNOWN')
        self.assertEqual(self.enables, 0)

    def test_unsafe_state_file_is_rejected(self):
        self.maintenance.chmod(0o666)
        with self.assertRaisesRegex(ValueError, 'UNTRUSTED_STATE'):
            self.arm()

    def test_machine_change_is_rejected(self):
        self.arm()
        with mock.patch.object(launcher, 'machine', return_value='other-machine'):
            with self.assertRaisesRegex(ValueError, 'LAUNCH_IDENTITY_CHANGED'):
                self.runner.tick()


if __name__ == '__main__':
    unittest.main()
