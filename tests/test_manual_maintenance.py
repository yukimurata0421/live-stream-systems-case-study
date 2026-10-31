"""Test the real maintenance state machine, durability, backup exclusion and return gates at external I/O boundaries."""
import copy
from contextlib import redirect_stdout
from datetime import datetime, timezone
import json
import io
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ops/selfheal'))
import fallback_controller as control
import manual_maintenance
import maintenance_host as host
from maintenance_state import Store, guard, session_lock, read_json
from manual_maintenance import Maintenance, LabBackend, validate_config


class Clock:
    def __init__(self): self.now = 1000000.
    def time(self): return self.now
    def sleep(self, seconds): self.now += seconds


def config():
    return {'schema': 'stream_v3.manual_maintenance_config.v1', 'oci_host': 'relay-fixture',
        'dell_machine_sha256': 'a' * 64, 'oci_machine_sha256': 'b' * 64,
        'slate_sha256': 'c' * 64, 'youtube_destination_sha256': 'd' * 64,
        'slate_manifest_sha256': 'e' * 64,
        'slate_file': '/opt/maintenance-fixture/offline.mp4', 'effect_ledger_file': '/fixture/ledger.sqlite3'}


class Backend:
    """Only provider/OS I/O is substituted; CLI/state/guard/effect checks remain real."""
    def __init__(self, root, clock):
        self.root, self.clock, self.calls = root, clock, []
        self.replicas, self.extra_backup_pods, self.backup_replicas = 1, 0, 0
        self.boot, self.session, self.freeze_audio = 'dell-boot-1', 1, False
        self.freeze_twitch = False
        self.cloud_boot = 'oci-boot'
        self.unit_state = {host.TIMER: {'active': True, 'enabled': 'enabled'},
                           host.SERVICE: {'active': False, 'enabled': 'static'}}
        self.path_fields = {'alwaysAvailable': False, 'alwaysAvailableFile': '', 'alwaysAvailableTracks': []}
        self.ledger = {'producer_id': 'independent-controller', 'generation': 2, 'authority_version': 1, 'unresolved': 0}
        self.fail_action, self.fail_after = None, False

    def snapshot(self, **_):
        now = self.clock.time()
        stamp = datetime.fromtimestamp(now, timezone.utc).isoformat()
        count = int(now * 100)
        return {'dell': {'identity': {'machine': 'a' * 64, 'boot': self.boot}, 'observed_at': now,
            'main': {'uid': 'main-uid', 'replicas': self.replicas, 'active_pods': self.replicas, 'ready_pods': self.replicas},
            'backup': {'uid': 'backup-uid', 'replicas': self.backup_replicas, 'active_pods': self.extra_backup_pods, 'ready_pods': 0},
            'units': copy.deepcopy(self.unit_state), 'ledger': copy.deepcopy(self.ledger),
            'controller': {'observed_at': stamp, 'health': 'HEALTHY', 'maintenance_guard_supported': True,
                           'maintenance_root': str(self.root)}},
            'oci': {'identity': {'machine': 'b' * 64, 'boot': self.cloud_boot}, 'observed_at': now,
                'config_fingerprint': 'config-fingerprint', 'path_fields': copy.deepcopy(self.path_fields),
                'content': 'LIVE' if self.replicas else 'MAINTENANCE_SLATE' if self.path_fields['alwaysAvailable'] else 'UNKNOWN',
                'input_id': 'publisher' if self.replicas else None, 'tracks': ['H264', 'MPEG-4 Audio'],
                'twitch': {'id': 'tw-1', 'state': 'forwarding', 'error': False, 'bytes': 1 if self.freeze_twitch else count},
                'copy': {'schema': 'stream_v3.youtube_forward.v1', 'boot_id': self.cloud_boot, 'pid': 20,
                    'session': self.session, 'state': 'FLOWING', 'destination_fingerprint': 'd' * 64,
                    'updated_at': stamp, 'last_progress_at': stamp, 'output_bytes': count,
                    'video_packets': count, 'audio_packets': 1 if self.freeze_audio else count}}}

    def request(self, role, action, **request):
        self.calls.append(action)
        if action == self.fail_action and not self.fail_after:
            raise TimeoutError('fixture_before_effect')
        if action == 'quiesce':
            self.unit_state[host.TIMER] = {'active': False, 'enabled': 'disabled'}
            self.unit_state[host.SERVICE]['active'] = False
        elif action == 'restore_units': self.unit_state = copy.deepcopy(request['restore'])
        elif action == 'path_patch': self.path_fields = copy.deepcopy(request['after'])
        elif action == 'scale_main': self.replicas = request['after']
        else: raise AssertionError(action)
        if action == self.fail_action:
            raise TimeoutError('fixture_ack_lost_after_effect')
        return self.snapshot()[role]


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Store(Path(directory.name) / 'maintenance')
        self.store.initialize()
        self.clock = Clock()
        self.backend = Backend(self.store.root, self.clock)
        self.reload()

    def reload(self):
        self.mode = Maintenance(self.store, self.backend, config(), clock=self.clock.time,
                                sleep=self.clock.sleep, interval=1, stable_seconds=3)

    def begin(self): self.mode.begin(minutes=10, reason='Security updates')

    def test_full_window_survives_dell_reboot_and_restores_original_settings(self):
        original = self.backend.snapshot()
        self.begin()
        self.assertEqual(self.mode.state['phase'], 'AWAITING_SLATE_CONFIRMATION')
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])
        with self.assertRaisesRegex(ValueError, 'both_public'):
            self.mode.confirm_slate(youtube=True, twitch=False)
        self.mode.confirm_slate(youtube=True, twitch=True)
        self.assertEqual(self.mode.state['slate_player_confirmation']['kind'], 'operator_attestation')
        self.backend.boot = 'dell-after-reboot'
        self.reload()
        self.mode.resume()
        self.assertEqual(self.mode.state['phase'], 'VERIFYING')
        self.mode.verify(timeout=6)
        self.clock.sleep(1)
        self.mode.complete(youtube=True, twitch=True, live_data=True)
        self.assertEqual(self.mode.state['phase'], 'NORMAL')
        self.assertFalse(guard(self.store.root, uid=os.geteuid())['inhibited'])
        self.assertEqual(self.backend.unit_state, original['dell']['units'])
        self.assertEqual(self.backend.path_fields, original['oci']['path_fields'])
        self.assertEqual(self.backend.replicas, 1)
        self.assertNotIn('backup_scale', self.backend.calls)
        finished = self.mode.state['session_id']
        self.begin()
        self.assertEqual(self.mode.state['generation'], 2)
        self.assertTrue((self.store.root / 'history' / (finished + '.json')).is_file())

    def test_active_backup_even_with_zero_desired_replicas_blocks_entry_without_effects(self):
        self.backend.extra_backup_pods = 1
        with self.assertRaisesRegex(ValueError, 'backup_sender'):
            self.begin()
        self.assertEqual(self.backend.calls, [])
        self.assertEqual(self.store.read()['phase'], 'NORMAL')

    def test_existing_fence_or_different_authority_is_never_bypassed(self):
        for changes in ({'unresolved': 1}, {'producer_id': 'cra-agent'}):
            self.backend.ledger.update(changes)
            with self.assertRaisesRegex(ValueError, 'authority_not_quiescent'):
                self.begin()
            self.backend.ledger.update(unresolved=0, producer_id='independent-controller')
        self.assertEqual(self.backend.calls, [])

    def test_audio_must_advance_before_source_can_be_stopped(self):
        self.backend.freeze_audio = True
        with self.assertRaisesRegex(ValueError, 'audio_video'):
            self.begin()
        self.assertEqual(self.backend.calls, [])

    def test_lost_ack_is_durable_reconciled_read_only_and_never_resent(self):
        self.backend.fail_action, self.backend.fail_after = 'path_patch', True
        with self.assertRaises(TimeoutError): self.begin()
        self.reload()
        self.assertEqual(self.mode.state['effects'][-1]['result'], 'OUTCOME_UNKNOWN')
        with self.assertRaisesRegex(ValueError, 'reconcile_pending'):
            self.mode.advance()
        self.backend.fail_action = None
        self.mode.reconcile()
        self.mode.advance()
        self.assertEqual(self.backend.calls.count('path_patch'), 1)
        self.assertEqual(self.mode.state['phase'], 'AWAITING_SLATE_CONFIRMATION')

    def test_unapplied_unknown_retains_inhibition_and_rejects_retry_or_resume(self):
        self.backend.fail_action = 'path_patch'
        with self.assertRaises(TimeoutError): self.begin()
        with self.assertRaisesRegex(ValueError, 'outcome_unknown'):
            self.mode.reconcile()
        with self.assertRaisesRegex(ValueError, 'reconcile_pending'):
            self.mode.resume()
        self.assertEqual(self.backend.calls.count('path_patch'), 1)
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])

    def test_cancel_after_partial_entry_uses_original_source_and_targeted_restore(self):
        self.backend.fail_action, self.backend.fail_after = 'path_patch', True
        with self.assertRaises(TimeoutError): self.begin()
        self.backend.fail_action = None
        self.mode.reconcile()
        self.mode.resume()
        self.assertNotIn('scale_main', self.backend.calls)
        self.assertFalse(self.backend.path_fields['alwaysAvailable'])
        self.assertEqual(self.mode.state['phase'], 'VERIFYING')

    def test_expiry_does_not_release_gate_or_claim_live_recovery(self):
        self.begin()
        self.clock.sleep(601)
        value = guard(self.store.root, uid=os.geteuid(), now=self.clock.time())
        self.assertTrue(value['overdue'])
        self.assertTrue(value['inhibited'])
        with self.assertRaisesRegex(ValueError, 'window_expired'):
            self.mode.confirm_slate(youtube=True, twitch=True)

    def test_live_stability_and_explicit_player_data_confirmation_are_required(self):
        self.begin(); self.mode.resume()
        with self.assertRaisesRegex(ValueError, 'live_stability'):
            self.mode.complete(youtube=True, twitch=True, live_data=True)
        self.mode.verify(timeout=6)
        with self.assertRaisesRegex(ValueError, 'public_av'):
            self.mode.complete(youtube=True, twitch=True, live_data=False)
        self.assertEqual(self.backend.calls.count('restore_units'), 0)

    def test_frozen_audio_new_connection_and_observation_gap_reset_stability(self):
        self.begin(); self.mode.resume(); self.mode.verify(timeout=6)
        self.backend.freeze_audio = True
        self.clock.sleep(1)
        with self.assertRaisesRegex(ValueError, 'stalled'):
            self.mode.verification_sample()
        self.assertIsNone(self.mode.state['stability'])
        self.backend.freeze_audio = False
        self.mode.verify(timeout=6)
        self.backend.session += 1
        self.clock.sleep(1)
        with self.assertRaisesRegex(ValueError, 'connection_changed'):
            self.mode.verification_sample()
        self.mode.verify(timeout=6)
        self.clock.sleep(46)
        with self.assertRaisesRegex(ValueError, 'sample_gap'):
            self.mode.complete(youtube=True, twitch=True, live_data=True)
        self.assertEqual(self.backend.calls.count('restore_units'), 0)

    def test_changed_cloud_boot_or_authority_blocks_return(self):
        self.begin()
        self.backend.cloud_boot = 'other-boot'
        with self.assertRaisesRegex(ValueError, 'oci_rebooted'):
            self.mode.resume()
        self.assertEqual(self.backend.calls.count('scale_main'), 1)

    def test_reenabled_recovery_timer_blocks_later_effects(self):
        self.begin()
        self.backend.unit_state[host.TIMER]['enabled'] = 'enabled'
        before = len(self.backend.calls)
        with self.assertRaisesRegex(ValueError, 'quiescence_lost'):
            self.mode.resume()
        self.assertEqual(len(self.backend.calls), before)
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])

    def test_verification_timeout_keeps_gate_and_recovery_quiesced(self):
        self.begin(); self.mode.resume()
        with self.assertRaisesRegex(ValueError, 'live_stability_deadline'):
            self.mode.verify(timeout=1)
        self.assertEqual(self.mode.state['phase'], 'VERIFYING')
        self.assertFalse(self.backend.unit_state[host.TIMER]['active'])
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])

    def test_sender_check_does_not_accept_youtube_progress_if_twitch_is_frozen(self):
        self.begin()
        self.backend.freeze_twitch = True
        before = len(self.backend.calls)
        with self.assertRaisesRegex(ValueError, 'content_progress_deadline'):
            self.mode.check()
        self.assertEqual(len(self.backend.calls), before)
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])

    def test_changed_session_config_is_rejected_before_an_effect(self):
        self.begin()
        changed = config(); changed['slate_sha256'] = 'e' * 64
        candidate = Maintenance(self.store, self.backend, changed, clock=self.clock.time, sleep=self.clock.sleep)
        with self.assertRaisesRegex(ValueError, 'session_config_changed'):
            candidate.resume()
        self.assertEqual(self.backend.calls.count('scale_main'), 1)

    def test_on_observes_slate_before_prompt_and_establishes_maintenance_in_one_call(self):
        def confirm():
            self.assertEqual(self.mode.state['phase'], 'AWAITING_SLATE_CONFIRMATION')
            self.assertEqual(self.backend.replicas, 0)
            self.assertNotIn('slate_player_confirmation', self.store.read())
            self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])
            return True
        self.mode.on(minutes=10, reason='Security updates', confirm=confirm)
        self.assertEqual(self.store.read()['phase'], 'MAINTENANCE')
        self.assertEqual(self.backend.calls, ['quiesce', 'path_patch', 'scale_main'])
        self.assertEqual(self.store.read()['slate_player_confirmation']['kind'], 'operator_attestation')

    def test_on_after_decline_reuses_the_window_without_resending_effects(self):
        with self.assertRaisesRegex(ValueError, 'playback_not_confirmed'):
            self.mode.on(minutes=10, reason='Security updates', confirm=lambda: False)
        before = self.store.read()
        self.assertEqual(before['phase'], 'AWAITING_SLATE_CONFIRMATION')
        self.reload()
        self.mode.on(minutes=90, reason='Retry', confirm=lambda: True)
        self.assertEqual(self.mode.state['phase'], 'MAINTENANCE')
        self.assertEqual(self.mode.state['deadline'], before['deadline'])
        self.assertEqual(self.mode.state['session_id'], before['session_id'])
        self.assertEqual(self.backend.calls, ['quiesce', 'path_patch', 'scale_main'])

    def test_on_after_terminal_eof_keeps_inhibition_and_can_continue_after_reload(self):
        def eof(): raise EOFError()
        with self.assertRaises(EOFError):
            self.mode.on(minutes=10, reason='Security updates', confirm=eof)
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])
        self.reload()
        self.mode.on(minutes=10, reason='Security updates', confirm=lambda: True)
        self.assertEqual(self.mode.state['phase'], 'MAINTENANCE')
        self.assertEqual(self.backend.calls.count('scale_main'), 1)

    def test_on_when_already_active_refreshes_confirmation_without_new_effect_or_deadline(self):
        self.mode.on(minutes=10, reason='Security updates', confirm=lambda: True)
        original = self.store.read()
        self.reload()
        self.mode.on(minutes=90, reason='Other', confirm=lambda: True)
        self.assertEqual(self.mode.state['deadline'], original['deadline'])
        self.assertEqual(self.mode.state['generation'], original['generation'])
        self.assertGreater(self.mode.state['slate_player_confirmation']['at'], original['slate_player_confirmation']['at'])
        self.assertEqual(len(self.backend.calls), 3)

    def test_on_refuses_unknown_until_reconcile_then_continues_known_preparing_stages(self):
        self.backend.fail_action, self.backend.fail_after = 'path_patch', True
        with self.assertRaises(TimeoutError): self.begin()
        self.reload()
        confirmation = mock.Mock(return_value=True)
        with self.assertRaisesRegex(ValueError, 'reconcile_pending'):
            self.mode.on(minutes=10, reason='Security updates', confirm=confirmation)
        confirmation.assert_not_called()
        self.backend.fail_action = None
        self.mode.reconcile()
        self.mode.on(minutes=10, reason='Security updates', confirm=confirmation)
        self.assertEqual(self.mode.state['phase'], 'MAINTENANCE')
        self.assertEqual(self.backend.calls.count('path_patch'), 1)

    def test_on_during_return_does_not_stop_live_source_again(self):
        self.begin(); self.mode.resume()
        before = list(self.backend.calls)
        confirmation = mock.Mock(return_value=True)
        with self.assertRaisesRegex(ValueError, 'return_in_progress'):
            self.mode.on(minutes=10, reason='Security updates', confirm=confirmation)
        confirmation.assert_not_called()
        self.assertEqual(self.backend.calls, before)
        self.assertEqual(self.mode.state['phase'], 'VERIFYING')

    def test_on_does_not_accept_confirmation_after_window_expires_during_prompt(self):
        def confirm(): self.clock.sleep(601); return True
        with self.assertRaisesRegex(ValueError, 'window_expired'):
            self.mode.on(minutes=10, reason='Security updates', confirm=confirm)
        self.assertEqual(self.mode.state['phase'], 'AWAITING_SLATE_CONFIRMATION')
        self.assertNotIn('slate_player_confirmation', self.store.read())
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])


    def test_off_runs_full_return_and_keeps_the_production_stability_window(self):
        self.mode.on(minutes=10, reason='Security updates', confirm=lambda: True)
        self.backend.boot = 'dell-after-reboot'
        self.reload()
        self.mode.stable_seconds = manual_maintenance.STABLE_SECONDS
        started = self.clock.time()
        def confirm():
            self.assertGreaterEqual(self.clock.time() - started, 300)
            self.assertEqual(self.backend.replicas, 1)
            self.assertFalse(self.backend.path_fields['alwaysAvailable'])
            self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])
            self.assertNotIn('restore_units', self.backend.calls)
            return True
        self.mode.off(confirm=confirm)
        self.assertEqual(self.store.read()['phase'], 'NORMAL')
        self.assertEqual(self.backend.calls, ['quiesce', 'path_patch', 'scale_main',
                                             'scale_main', 'path_patch', 'restore_units'])
        self.assertFalse(guard(self.store.root, uid=os.geteuid())['inhibited'])

    def test_off_after_decline_continues_saved_verification_without_replaying_effects(self):
        self.begin()
        with self.assertRaisesRegex(ValueError, 'live_playback_not_confirmed'):
            self.mode.off(confirm=lambda: False)
        original = self.store.read()
        calls = list(self.backend.calls)
        self.assertEqual(original['phase'], 'VERIFYING')
        self.assertNotIn('restore_units', calls)
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])
        self.reload()
        self.mode.off(confirm=lambda: True)
        self.assertEqual(self.mode.state['phase'], 'NORMAL')
        self.assertEqual(self.mode.state['session_id'], original['session_id'])
        self.assertEqual(self.mode.state['generation'], original['generation'])
        self.assertEqual(self.mode.state['deadline'], original['deadline'])
        self.assertEqual(self.backend.calls, calls + ['restore_units'])

    def test_off_eof_or_interrupt_retains_guard_and_is_resumable_after_reload(self):
        for error in (EOFError, KeyboardInterrupt):
            with self.subTest(error=error.__name__):
                self.begin()
                def interrupted(): raise error()
                with self.assertRaises(error): self.mode.off(confirm=interrupted)
                self.assertEqual(self.store.read()['phase'], 'VERIFYING')
                self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])
                calls = list(self.backend.calls)
                self.reload()
                self.mode.off(confirm=lambda: True)
                self.assertEqual(self.backend.calls, calls + ['restore_units'])
                self.assertEqual(self.store.read()['phase'], 'NORMAL')

    def test_off_verification_failure_does_not_prompt_or_release_guard(self):
        self.begin()
        self.mode.resume()
        self.backend.freeze_audio = True
        confirm = mock.Mock(return_value=True)
        with self.assertRaisesRegex(ValueError, 'live_stability_deadline'):
            self.mode.off(confirm=confirm)
        confirm.assert_not_called()
        self.assertEqual(self.store.read()['phase'], 'VERIFYING')
        self.assertNotIn('restore_units', self.backend.calls)
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])

    def test_off_after_long_player_confirmation_requires_fresh_stability(self):
        self.begin()
        def delayed_confirm(): self.clock.sleep(46); return True
        with self.assertRaisesRegex(ValueError, 'sample_gap'):
            self.mode.off(confirm=delayed_confirm)
        self.assertEqual(self.store.read()['phase'], 'VERIFYING')
        self.assertNotIn('restore_units', self.backend.calls)
        self.reload()
        self.mode.off(confirm=lambda: True)
        self.assertEqual(self.store.read()['phase'], 'NORMAL')

    def test_off_in_normal_is_a_noop_without_a_player_prompt_or_effect(self):
        before = self.store.read()
        confirm = mock.Mock(return_value=True)
        self.mode.off(confirm=confirm)
        confirm.assert_not_called()
        self.assertEqual(self.store.read(), before)
        self.assertEqual(self.backend.calls, [])

    def test_off_never_retries_unknown_source_resume_before_read_only_reconcile(self):
        self.begin()
        self.backend.fail_action, self.backend.fail_after = 'scale_main', True
        confirm = mock.Mock(return_value=True)
        with self.assertRaises(TimeoutError): self.mode.off(confirm=confirm)
        self.assertEqual(self.store.read()['phase'], 'RESUMING')
        calls = list(self.backend.calls)
        self.reload()
        with self.assertRaisesRegex(ValueError, 'reconcile_pending'):
            self.mode.off(confirm=confirm)
        self.assertEqual(self.backend.calls, calls)
        confirm.assert_not_called()
        self.backend.fail_action = None
        self.mode.reconcile()
        self.mode.off(confirm=confirm)
        self.assertEqual(self.backend.calls.count('scale_main'), 2)
        self.assertEqual(self.store.read()['phase'], 'NORMAL')

    def test_off_restoring_unknown_is_reconciled_without_restoring_timer_twice(self):
        self.begin()
        self.backend.fail_action, self.backend.fail_after = 'restore_units', True
        with self.assertRaises(TimeoutError): self.mode.off(confirm=lambda: True)
        self.assertEqual(self.store.read()['phase'], 'RESTORING')
        self.reload()
        with self.assertRaisesRegex(ValueError, 'reconcile_pending'):
            self.mode.off(confirm=lambda: True)
        self.backend.fail_action = None
        self.mode.reconcile()
        self.mode.off(confirm=lambda: True)
        self.assertEqual(self.backend.calls.count('restore_units'), 1)
        self.assertEqual(self.store.read()['phase'], 'NORMAL')

    def test_off_cancels_known_partial_entry_without_stopping_main_again(self):
        self.backend.fail_action, self.backend.fail_after = 'path_patch', True
        with self.assertRaises(TimeoutError): self.begin()
        self.backend.fail_action = None
        self.mode.reconcile()
        self.mode.off(confirm=lambda: True)
        self.assertNotIn('scale_main', self.backend.calls)
        self.assertEqual(self.store.read()['phase'], 'NORMAL')

    def test_off_rechecks_live_state_after_prompt_before_releasing_guard(self):
        self.begin()
        def confirm(): self.backend.session += 1; return True
        with self.assertRaisesRegex(ValueError, 'connection_changed'):
            self.mode.off(confirm=confirm)
        self.assertEqual(self.store.read()['phase'], 'VERIFYING')
        self.assertNotIn('restore_units', self.backend.calls)
        self.assertTrue(guard(self.store.root, uid=os.geteuid())['inhibited'])



class GuardTests(unittest.TestCase):
    def test_missing_installation_is_normal_but_partial_or_corrupt_store_is_held(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'maintenance'
            self.assertFalse(guard(root, uid=os.geteuid())['inhibited'])
            root.mkdir()
            self.assertTrue(guard(root, uid=os.geteuid())['inhibited'])
            (root / 'session.json').write_text('{broken')
            self.assertTrue(guard(root, uid=os.geteuid())['inhibited'])
            (root / 'session.json').unlink()
            (root / 'session.json').symlink_to('/dev/null')
            self.assertTrue(guard(root, uid=os.geteuid())['inhibited'])

    def test_real_shared_lock_closes_scale_race_and_expiry_keeps_both_actions_inhibited(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'maintenance'); store.initialize()
            reader = lambda root: guard(root, uid=os.geteuid(), now=100)
            with mock.patch.object(control, 'maintenance_guard', reader), mock.patch.object(control, 'kubectl') as scale:
                with store.lock():
                    self.assertEqual(control.scale_if_allowed('START', store.root), 'INHIBITED')
                value = store.read(); value.update(phase='MAINTENANCE', session_id='session', deadline=1)
                store.write(value)
                for action in ('START', 'STOP'):
                    self.assertEqual(control.scale_if_allowed(action, store.root), 'INHIBITED')
                scale.assert_not_called()
                mode = control.Controller()
                self.assertIsNone(mode.decide(999, False, 0, maintenance=reader(store.root)))
                self.assertIsNone(mode.decide(999, True, 1, maintenance=reader(store.root)))
                value['phase'] = 'NORMAL'; store.write(value)
                mode.domain='MEDIA_FAILURE'
                self.assertIsNone(mode.decide(1000, False, 0, maintenance=reader(store.root)))
                self.assertEqual(mode.decide(1030, False, 0, maintenance=reader(store.root)), 'START')
                self.assertEqual(control.scale_if_allowed('START', store.root), 'SUCCEEDED')
                scale.assert_called_once()

    def test_lock_is_exclusive_and_state_replacement_is_atomic_and_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'maintenance'); store.initialize()
            with store.lock(), self.assertRaises(BlockingIOError):
                with session_lock(store.root, shared=True): pass
            self.assertEqual((store.root / 'session.json').stat().st_mode & 0o777, 0o644)
            self.assertEqual(read_json(store.root / 'session.json')['phase'], 'NORMAL')
            with self.assertRaises(FileExistsError): store.initialize()


class HostBoundaryTests(unittest.TestCase):
    def test_read_only_snapshot_waits_for_a_natural_oneshot_transition(self):
        busy = subprocess.CompletedProcess([], 1, stdout='{"ok":false,"reason":"recovery_unit_transitioning"}', stderr='')
        ready = subprocess.CompletedProcess([], 0, stdout='{"ok":true,"value":{"ready":true}}', stderr='')
        with mock.patch.object(manual_maintenance.subprocess, 'run', side_effect=[busy, ready]) as command, \
                mock.patch.object(manual_maintenance.time, 'sleep') as sleep:
            result = manual_maintenance.HostBackend(config()).request('dell', 'snapshot')
        self.assertEqual(result, {'ready': True})
        self.assertEqual(command.call_count, 2)
        sleep.assert_called_once_with(.25)

    def test_effect_and_expired_readiness_deadline_are_never_automatically_retried(self):
        busy = subprocess.CompletedProcess([], 1, stdout='{"ok":false,"reason":"recovery_unit_transitioning"}', stderr='')
        for action in ('quiesce', 'snapshot'):
            with mock.patch.object(manual_maintenance.subprocess, 'run', return_value=busy) as command, \
                    mock.patch.object(manual_maintenance.time, 'monotonic', side_effect=[0, 6]), \
                    mock.patch.object(manual_maintenance.time, 'sleep') as sleep:
                with self.assertRaisesRegex(ValueError, 'host_adapter_failed'):
                    manual_maintenance.HostBackend(config()).request('dell', action)
                command.assert_called_once()
                sleep.assert_not_called()

    def test_manifest_and_media_are_both_pinned_without_a_remote_decoder(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'offline.mp4'; path.write_bytes(b'fixture-media')
            path.chmod(0o644)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest = {'schema': 'stream_v3.offline_slate_bundle.v1', 'content_state': 'MAINTENANCE_SLATE',
                'language': 'en', 'files': {'offline.mp4': digest}, 'media': {'video': 'H264', 'audio': 'AAC',
                'width': 1920, 'height': 1080, 'fps': 5, 'sample_rate': 48000, 'channels': 2, 'duration_seconds': 4.0}}
            meta = path.with_name('manifest.json'); meta.write_text(json.dumps(manifest)); meta.chmod(0o644)
            expected = hashlib.sha256(meta.read_bytes()).hexdigest()
            with mock.patch.object(host, 'run') as command:
                host.verify_asset(path, digest, expected, trusted_uid=os.geteuid())
                command.assert_not_called()
            meta.write_text(json.dumps({**manifest, 'language': 'other'}))
            with self.assertRaisesRegex(ValueError, 'manifest_hash_mismatch'):
                host.verify_asset(path, digest, expected, trusted_uid=os.geteuid())
            path.write_bytes(b'changed-media')
            with self.assertRaisesRegex(ValueError, 'slate_hash_mismatch'):
                host.verify_asset(path, digest, expected, trusted_uid=os.geteuid())

    def test_ssh_uses_the_configured_existing_operator_identity(self):
        value = config(); value['ssh_operator'] = 'operator'
        validate_config(value)
        reply = subprocess.CompletedProcess([], 0, stdout='{"ok":true,"value":{}}', stderr='')
        with mock.patch.object(manual_maintenance.subprocess, 'run', return_value=reply) as command:
            manual_maintenance.HostBackend(value).request('oci', 'snapshot')
        self.assertEqual(command.call_args.args[0][:5], ['/usr/sbin/runuser', '-u', 'operator', '--', 'ssh'])
        value['ssh_operator'] = 'operator;command'
        with self.assertRaisesRegex(ValueError, 'invalid_ssh_operator'):
            validate_config(value)

    def test_executable_symlink_from_another_cwd_rejects_piped_yes_before_effects(self):
        script = Path(__file__).resolve().parents[1] / 'ops/selfheal/stream-v3-maintenance'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'maintenance'; store = Store(root); store.initialize()
            link = Path(directory) / 'maintenance-command'; link.symlink_to(script)
            for action in ('on', 'off'):
                with self.subTest(action=action):
                    result = subprocess.run([str(link), '--root', str(root), '--lab-api', 'http://127.0.0.1:1', action],
                        input='yes\n', capture_output=True, text=True, cwd=directory, timeout=5)
                    self.assertEqual(result.returncode, 1)
                    self.assertEqual(json.loads(result.stdout)['reason'], 'terminal_required_for_playback_confirmation')
                    self.assertFalse(json.loads(result.stdout)['safe_to_update_dell'])
                    self.assertEqual(store.read()['phase'], 'NORMAL')

    def test_on_and_off_reject_preconfirmation_flags_even_in_a_terminal(self):
        for action, reason in (('on', 'on_requires_confirmation_after_slate_output'),
                               ('off', 'off_requires_confirmation_after_live_verification')):
            for flag in ('--confirm-youtube', '--confirm-twitch', '--confirm-live-data'):
                with self.subTest(action=action, flag=flag), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory) / 'maintenance'; store = Store(root); store.initialize()
                    stream = io.StringIO()
                    argv = ['maintenance', '--root', str(root), '--lab-api', 'http://127.0.0.1:1', action, flag]
                    with mock.patch.object(sys, 'argv', argv), mock.patch.object(sys, 'stdin') as stdin, \
                            mock.patch.object(sys, 'stderr') as stderr, redirect_stdout(stream):
                        stdin.isatty.return_value = stderr.isatty.return_value = True
                        self.assertEqual(manual_maintenance.main(), 1)
                    self.assertEqual(json.loads(stream.getvalue())['reason'], reason)
                    self.assertEqual(store.read()['phase'], 'NORMAL')

    def test_endpoint_and_host_config_reject_shell_and_nonloopback_lab_access(self):
        for url in ('https://127.0.0.1:80', 'http://localhost:80', 'http://127.0.0.1:80@elsewhere', 'http://192.0.2.1:80'):
            with self.assertRaises(ValueError): LabBackend(url)
        for name in ('-oProxyCommand=fixture', 'host;echo', 'host$(id)'):
            value = config(); value['oci_host'] = name
            with self.assertRaises(ValueError): validate_config(value)

    def test_real_read_only_fence_query_preserves_retired_history_and_blocks_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger.sqlite3'
            connection = sqlite3.connect(path)
            connection.executescript("""
                CREATE TABLE runtime_authority(singleton,producer_id,producer_generation,authority_version);
                INSERT INTO runtime_authority VALUES(1,'independent-controller',2,4);
                CREATE TABLE effect_scope_fences(owner_request_id,state);
                CREATE TABLE effect_requests(request_id,state);
                CREATE TABLE typed_effect_requests(request_id,state);
                INSERT INTO effect_requests VALUES('retired','OUTCOME_UNKNOWN');
                INSERT INTO effect_scope_fences VALUES('retired','RETIRED_TARGET_OUTCOME_UNKNOWN');
            """)
            connection.commit()
            before = path.read_bytes()
            self.assertEqual(host.ledger(path)['unresolved'], 0)
            self.assertEqual(path.read_bytes(), before)
            connection.execute("INSERT INTO typed_effect_requests VALUES('new','ACCEPTED')")
            connection.commit()
            self.assertGreater(host.ledger(path)['unresolved'], 0)
            connection.close()

    def test_cli_cold_init_and_saved_status_do_not_claim_current_delivery(self):
        script = Path(__file__).resolve().parents[1] / 'ops/selfheal/manual_maintenance.py'
        with tempfile.TemporaryDirectory() as directory:
            root = str(Path(directory) / 'maintenance')
            command = [sys.executable, '-B', str(script), '--root', root, '--lab-api', 'http://127.0.0.1:1']
            subprocess.run(command + ['init'], check=True, capture_output=True)
            result = subprocess.run(command + ['status'], check=True, capture_output=True, text=True)
            self.assertFalse(json.loads(result.stdout)['safe_to_update_dell'])
            self.assertEqual(json.loads(result.stdout)['phase'], 'NORMAL')
