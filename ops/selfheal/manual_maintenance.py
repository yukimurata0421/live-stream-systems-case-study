#!/usr/bin/env python3
"""Send a cloud-hosted slate during planned sender maintenance and return after operator checks."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid

from maintenance_state import ROOT, SCHEMA, Store, guard, read_json

STABLE_SECONDS = 300
MAX_GAP = 45
DEFAULT_CONFIG = Path('/etc/stream-v3-manual-maintenance/config.json')


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def epoch(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(stamp.tzinfo is not None, 'timezone_required')
    return stamp.timestamp()


def confirm_public_slate():
    print('The maintenance slate is flowing. Check the public YouTube and Twitch players.', file=sys.stderr, flush=True)
    print('Are both players showing the maintenance slate? [yes/no] ',
          file=sys.stderr, end='', flush=True)
    return input().strip().casefold() in ('yes', 'y', '\u306f\u3044')


def confirm_public_live():
    print('Live output has passed the stability check. Inspect both public players.', file=sys.stderr, flush=True)
    print('Are YouTube and Twitch playing live video/audio, with live data updating? [yes/no] ', file=sys.stderr, end='', flush=True)
    return input().strip().casefold() in ('yes', 'y', '\u306f\u3044')


def validate_config(config):
    require(config.get('schema') == 'stream_v3.manual_maintenance_config.v1', 'config_schema_invalid')
    for key in ('dell_machine_sha256', 'oci_machine_sha256', 'slate_sha256', 'slate_manifest_sha256', 'youtube_destination_sha256'):
        require(isinstance(config.get(key), str) and re.fullmatch('[a-f0-9]{64}', config[key]), 'config_hash_invalid')
    require(config['dell_machine_sha256'] != config['oci_machine_sha256'], 'maintenance_sender_must_be_independent')
    require(re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.@-]{0,100}', config.get('oci_host', '')), 'invalid_ssh_host')
    if config.get('ssh_operator') is not None:
        require(re.fullmatch('[a-z_][a-z0-9_-]{0,31}', config['ssh_operator']), 'invalid_ssh_operator')
    for key in ('slate_file', 'effect_ledger_file'):
        require(isinstance(config.get(key), str) and Path(config[key]).is_absolute()
                and not any(c in config[key] for c in '\r\n'), 'absolute_config_path_required')
    require(Path(config['slate_file']).suffix == '.mp4', 'mp4_required')
    return config


class HostBackend:
    def __init__(self, config):
        self.config = config

    def request(self, role, action, **payload):
        request = {'config': self.config, 'role': role, 'action': action, **payload}
        helper = Path(__file__).with_name('maintenance_host.py')
        if role == 'dell':
            command = ['/usr/bin/python3', '-B', str(helper)]
        else:
            # SSH receives one quoted fixed Python program. No stream keys, shell
            # interpolation, arbitrary commands or inherited remote environment.
            remote = 'sudo -n /usr/bin/python3 -B -c ' + shlex.quote(helper.read_text())
            command = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
                       self.config['oci_host'], remote]
            if self.config.get('ssh_operator'):
                command = ['/usr/sbin/runuser', '-u', self.config['ssh_operator'], '--', *command]
        deadline = time.monotonic() + 5
        while True:
            result = subprocess.run(command, input=json.dumps(request), text=True, capture_output=True, timeout=60)
            require(len(result.stdout) <= 65536, 'oversized_host_reply')
            reply = json.loads(result.stdout)
            # A naturally running oneshot may be in systemd's activating state.
            # Wait only for this read-only observation; never retry an effect.
            if (role == 'dell' and action in ('snapshot', 'preflight') and reply.get('ok') is False
                    and reply.get('reason') == 'recovery_unit_transitioning' and time.monotonic() < deadline):
                time.sleep(.25)
                continue
            require(result.returncode == 0 and reply.get('ok') is True, 'host_adapter_failed')
            return reply['value']

    def snapshot(self, *, preflight=False):
        action = 'preflight' if preflight else 'snapshot'
        return {'dell': self.request('dell', action), 'oci': self.request('oci', action)}


class LabBackend:
    """Isolated entrypoint fixture: literal loopback only; never invokes the host helper."""
    def __init__(self, url):
        parsed = urllib.parse.urlsplit(url)
        require(parsed.scheme == 'http' and parsed.hostname == '127.0.0.1' and parsed.port
                and parsed.path in ('', '/') and not parsed.username and not parsed.query and not parsed.fragment,
                'literal_loopback_lab_endpoint_required')
        self.url = url.rstrip('/')
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(self, value):
        request = urllib.request.Request(self.url, data=json.dumps(value).encode(),
                                          headers={'Content-Type': 'application/json'})
        with self.opener.open(request, timeout=20) as response:
            raw = response.read(65537)
        require(len(raw) <= 65536, 'oversized_lab_reply')
        return json.loads(raw)

    def snapshot(self, *, preflight=False):
        return self.call({'action': 'snapshot', 'preflight': preflight})

    def request(self, role, action, **payload):
        return self.call({'role': role, 'action': action, **payload})


class Maintenance:
    def __init__(self, store, backend, config, *, clock=time.time, sleep=time.sleep, interval=2,
                 stable_seconds=STABLE_SECONDS):
        self.store, self.backend, self.config = store, backend, config
        self.clock, self.sleep, self.interval, self.stable_seconds = clock, sleep, interval, stable_seconds
        self.state = store.read()

    def save(self, phase=None):
        if phase is not None and self.state['phase'] != phase:
            self.state['history'].append({'at': self.clock(), 'phase': phase})
            self.state['phase'] = phase
        self.state['updated_at'] = self.clock()
        self.store.write(self.state)

    def bound(self, snapshot):
        now = self.clock()
        for role in ('dell', 'oci'):
            value = snapshot[role]
            require(value['identity']['machine'] == self.config[role + '_machine_sha256'], 'host_identity_drift')
            require(0 <= now - value['observed_at'] <= 20, 'host_observation_stale')
        require(snapshot['dell']['backup']['replicas'] == 0 and snapshot['dell']['backup']['active_pods'] == 0,
                'backup_sender_is_active_or_unknown')
        require(snapshot['dell']['ledger']['unresolved'] == 0
                and snapshot['dell']['ledger']['producer_id'] == 'independent-controller', 'existing_authority_not_quiescent')
        if self.state['phase'] != 'NORMAL':
            before = self.state['before']
            require(self.state['config_sha256'] == fingerprint(self.config), 'session_config_changed')
            require(snapshot['oci']['identity'] == before['oci']['identity'], 'oci_rebooted_or_changed')
            require(snapshot['oci']['config_fingerprint'] == before['oci']['config_fingerprint'], 'relay_config_drift')
            require(snapshot['dell']['main']['uid'] == before['dell']['main']['uid'], 'main_workload_replaced')
            require(snapshot['dell']['backup']['uid'] == before['dell']['backup']['uid'], 'backup_workload_replaced')
            require(snapshot['dell']['ledger'] == before['dell']['ledger'], 'authority_changed')
            if (self.state['phase'] != 'RESTORING'
                    and any(e['name'] == 'quiesce' and e['result'] == 'APPLIED' for e in self.state['effects'])):
                from maintenance_host import TIMER, SERVICE
                units = snapshot['dell']['units']
                require(not units[TIMER]['active'] and units[TIMER]['enabled'] == 'disabled'
                        and not units[SERVICE]['active'], 'recovery_quiescence_lost')
        return snapshot

    def snapshot(self, *, preflight=False):
        return self.bound(self.backend.snapshot(preflight=preflight))

    def progress(self, snapshot, content, previous=None):
        cloud, local = snapshot['oci'], snapshot['dell']
        cp, now = cloud['copy'], self.clock()
        tw = cloud['twitch']
        require(cloud['content'] == content, 'expected_content_not_observed')
        require(cp['schema'] == 'stream_v3.youtube_forward.v1' and cp['boot_id'] == cloud['identity']['boot']
                and cp['destination_fingerprint'] == self.config['youtube_destination_sha256']
                and cp['state'] == 'FLOWING', 'youtube_copy_not_bound_or_flowing')
        require(0 <= now - epoch(cp['updated_at']) <= 20
                and 0 <= now - epoch(cp['last_progress_at']) <= 20, 'copy_observation_stale')
        require('H264' in cloud['tracks'] and 'MPEG-4 Audio' in cloud['tracks'], 'both_tracks_required')
        require(tw.get('id') and tw.get('state') == 'forwarding' and tw.get('error') is False
                and type(tw.get('bytes')) is int and tw['bytes'] > 0, 'twitch_output_not_flowing')
        for key in ('pid', 'session', 'output_bytes', 'video_packets', 'audio_packets'):
            require(type(cp[key]) is int and cp[key] > 0, 'copy_counter_invalid')
        if content == 'LIVE':
            require(local['main']['replicas'] == 1 and local['main']['ready_pods'] == 1 and local['main']['active_pods'] == 1
                    and cloud['input_id'], 'main_not_ready')
        else:
            require(local['main']['replicas'] == 0 and local['main']['active_pods'] == 0
                    and cloud['path_fields'] == self.state['armed_fields'], 'source_not_stopped_or_slate_not_armed')
        if previous is not None:
            before = previous['oci']['copy']
            require(0 < cloud['observed_at'] - previous['oci']['observed_at'] <= MAX_GAP, 'progress_sample_gap')
            require(all(cp[k] == before[k] for k in ('pid', 'session', 'boot_id', 'destination_fingerprint'))
                    and cloud['input_id'] == previous['oci']['input_id'], 'connection_changed')
            require(all(cp[k] > before[k] for k in ('output_bytes', 'video_packets', 'audio_packets')),
                    'audio_video_or_output_stalled')
            old_tw = previous['oci']['twitch']
            require(tw['id'] == old_tw['id'] and tw['bytes'] > old_tw['bytes'], 'twitch_output_not_advancing')

    def wait_content(self, content, *, timeout=90):
        deadline, previous = self.clock() + timeout, None
        while self.clock() <= deadline:
            sample = self.snapshot()
            try:
                self.progress(sample, content, previous)
                if previous is not None:
                    return sample
                previous = sample
            except (ValueError, TypeError, KeyError):
                previous = None
            self.sleep(self.interval)
        raise ValueError('content_progress_deadline')

    @staticmethod
    def applied(effect, snapshot):
        value, request = snapshot[effect['role']], effect['request']
        if effect['action'] == 'path_patch':
            return value['path_fields'] == request['after']
        if effect['action'] == 'scale_main':
            return value['main']['uid'] == request['uid'] and value['main']['replicas'] == request['after']
        from maintenance_host import TIMER, SERVICE
        if effect['action'] == 'quiesce':
            return (value['units'][TIMER]['enabled'] == 'disabled' and not value['units'][TIMER]['active']
                    and not value['units'][SERVICE]['active'])
        if effect['action'] == 'restore_units':
            return value['units'][TIMER] == request['restore'][TIMER]
        return False

    def no_unknown(self):
        require(all(e['result'] == 'APPLIED' for e in self.state['effects']), 'reconcile_pending_effect_first')

    def effect(self, name, role, action, **request):
        previous = next((e for e in self.state['effects'] if e['name'] == name), None)
        if previous is not None:
            require(previous['result'] == 'APPLIED', 'reconcile_pending_effect_first')
            return
        self.no_unknown()
        current = self.snapshot()
        effect = {'name': name, 'role': role, 'action': action, 'request': request, 'result': 'PENDING',
                  'attempt': 1, 'at': self.clock()}
        self.state['effects'].append(effect)
        self.save()
        try:
            self.backend.request(role, action, expected_boot=current[role]['identity']['boot'], **request)
            require(self.applied(effect, self.snapshot()), 'effect_not_confirmed')
            effect['result'] = 'APPLIED'
            self.save()
        except BaseException:
            effect['result'] = 'OUTCOME_UNKNOWN'
            self.save()
            raise

    def reconcile(self):
        sample = self.snapshot()
        for effect in self.state['effects']:
            if effect['result'] != 'APPLIED':
                require(self.applied(effect, sample), 'outcome_unknown_keep_inhibition_no_retry')
                effect['result'] = 'APPLIED'
                effect['reconciled_at'] = self.clock()
        self.save()

    def preflight(self):
        sample = self.snapshot(preflight=True)
        status = sample['dell']['controller']
        require(status.get('maintenance_guard_supported') is True
                and status.get('maintenance_root') == str(self.store.root)
                and 0 <= self.clock() - epoch(status['observed_at']) <= 30, 'updated_fallback_guard_not_observed')
        require(sample['dell']['main']['replicas'] == 1 and sample['dell']['main']['ready_pods'] == 1
                and sample['dell']['main']['active_pods'] == 1,
                'single_live_main_required')
        require(sample['oci']['path_fields']['alwaysAvailable'] is False, 'existing_slate_or_other_owner')
        self.progress(sample, 'LIVE')
        self.sleep(self.interval)
        self.progress(self.snapshot(), 'LIVE', sample)
        return sample

    def begin(self, *, minutes, reason):
        require(self.state['phase'] == 'NORMAL', 'maintenance_already_started')
        require(1 <= minutes <= 1440 and 0 < len(reason) <= 120 and '\n' not in reason, 'invalid_window_or_reason')
        before = self.preflight()
        if self.state['session_id']:
            archive = self.store.root / 'history'
            archive.mkdir(mode=0o755, exist_ok=True)
            path = archive / (self.state['session_id'] + '.json')
            require(not path.exists(), 'existing_session_archive_preserved')
            from maintenance_state import atomic_json
            atomic_json(path, self.state)
        generation = self.state['generation'] + 1
        self.state = {'schema': SCHEMA, 'phase': 'PREPARING', 'session_id': str(uuid.uuid4()),
            'generation': generation, 'deadline': self.clock() + minutes * 60, 'reason': reason,
            'config_sha256': fingerprint(self.config), 'before': before, 'effects': [], 'history': [],
            'armed_fields': {'alwaysAvailable': True, 'alwaysAvailableFile': self.config['slate_file'], 'alwaysAvailableTracks': []}}
        self.save()
        self.advance()

    def advance(self):
        require(self.state['phase'] == 'PREPARING', 'not_preparing')
        self.no_unknown()
        before = self.state['before']
        self.effect('quiesce', 'dell', 'quiesce', before=before['dell']['units'], ledger=before['dell']['ledger'])
        self.effect('arm_slate', 'oci', 'path_patch', before=before['oci']['path_fields'],
                    after=self.state['armed_fields'], config_fingerprint=before['oci']['config_fingerprint'])
        # A PATCH may rebuild the path. Confirm the original live output before
        # intentionally stopping Dell; do not assume a zero-gap hot change.
        if not any(e['name'] == 'source_stop' for e in self.state['effects']):
            self.wait_content('LIVE')
        self.effect('source_stop', 'dell', 'scale_main', uid=before['dell']['main']['uid'],
                    before=1, after=0, ledger=before['dell']['ledger'])
        self.wait_content('MAINTENANCE_SLATE')
        self.save('AWAITING_SLATE_CONFIRMATION')

    def confirm_slate(self, *, youtube, twitch):
        require(self.state['phase'] in ('AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE'), 'not_awaiting_slate_confirmation')
        require(youtube and twitch, 'both_public_players_must_be_confirmed')
        require(self.clock() < self.state['deadline'], 'maintenance_entry_window_expired')
        self.no_unknown()
        self.wait_content('MAINTENANCE_SLATE')
        require(self.clock() < self.state['deadline'], 'maintenance_entry_window_expired')
        self.state['slate_player_confirmation'] = {'kind': 'operator_attestation', 'at': self.clock(),
                                                   'youtube': True, 'twitch': True}
        self.save('MAINTENANCE')

    def on(self, *, minutes, reason, confirm):
        """Combine entry, interrupted-session continuation and player confirmation in one call."""
        self.no_unknown()
        phase = self.state['phase']
        if phase == 'NORMAL':
            self.begin(minutes=minutes, reason=reason)
        elif phase == 'PREPARING':
            self.advance()
        else:
            require(phase in ('AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE'), 'return_in_progress_cannot_start')
        require(self.clock() < self.state['deadline'], 'maintenance_entry_window_expired')
        self.check()
        # A sender observation never fabricates a public-player confirmation.
        # The prompt happens after the actual slate output has been observed.
        require(confirm() is True, 'playback_not_confirmed_maintenance_retained')
        self.confirm_slate(youtube=True, twitch=True)

    def off(self, *, confirm):
        """Resume the saved return stages and release inhibition after operator checks."""
        self.no_unknown()
        phase = self.state['phase']
        if phase == 'NORMAL':
            return
        if phase in ('PREPARING', 'AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE', 'RESUMING'):
            self.resume()
        else:
            require(phase in ('VERIFYING', 'RESTORING'), 'not_resumable')
        self.verify()
        require(confirm() is True, 'live_playback_not_confirmed_inhibition_retained')
        self.complete(youtube=True, twitch=True, live_data=True)

    def resume(self):
        require(self.state['phase'] in ('PREPARING', 'AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE', 'RESUMING'), 'not_resumable')
        self.no_unknown()
        self.save('RESUMING')
        before = self.state['before']
        current = self.snapshot()
        if current['dell']['main']['replicas'] == 0:
            self.effect('source_resume', 'dell', 'scale_main', uid=before['dell']['main']['uid'],
                        before=0, after=1, ledger=before['dell']['ledger'])
        self.wait_content('LIVE')
        current = self.snapshot()
        if current['oci']['path_fields'] == self.state['armed_fields']:
            self.effect('restore_path', 'oci', 'path_patch', before=self.state['armed_fields'],
                        after=before['oci']['path_fields'], config_fingerprint=before['oci']['config_fingerprint'])
        require(self.snapshot()['oci']['path_fields'] == before['oci']['path_fields'], 'path_restore_not_confirmed')
        self.wait_content('LIVE')
        self.state['stability'] = None
        self.save('VERIFYING')

    def verification_sample(self):
        sample = self.snapshot()
        require(sample['oci']['path_fields'] == self.state['before']['oci']['path_fields'], 'path_not_restored')
        stability = self.state.get('stability')
        try:
            self.progress(sample, 'LIVE', stability['sample'] if stability else None)
        except (ValueError, TypeError, KeyError):
            stability = None
            self.state['stability'] = None
            self.save()
            raise
        since = stability['since'] if stability else self.clock()
        self.state['stability'] = {'since': since, 'sample': sample}
        self.save()
        return self.clock() - since

    def verify(self, *, timeout=450):
        require(self.state['phase'] in ('VERIFYING', 'RESTORING'), 'not_verifying')
        self.no_unknown()
        deadline = self.clock() + timeout
        while self.clock() <= deadline:
            try:
                if self.verification_sample() >= self.stable_seconds:
                    return
            except ValueError as error:
                # Identity, authority and backup conflicts are hard stops.
                if str(error) not in ('copy_observation_stale', 'expected_content_not_observed',
                    'youtube_copy_not_bound_or_flowing', 'main_not_ready', 'connection_changed',
                    'audio_video_or_output_stalled', 'progress_sample_gap', 'both_tracks_required',
                    'twitch_output_not_flowing', 'twitch_output_not_advancing'):
                    raise
            self.sleep(self.interval)
        raise ValueError('live_stability_deadline_inhibition_retained')

    def check(self):
        self.no_unknown()
        require(self.state['phase'] in ('MAINTENANCE', 'AWAITING_SLATE_CONFIRMATION', 'VERIFYING', 'RESTORING'),
                'no_active_content_to_check')
        content = 'MAINTENANCE_SLATE' if self.state['phase'] in ('MAINTENANCE', 'AWAITING_SLATE_CONFIRMATION') else 'LIVE'
        self.wait_content(content)

    def complete(self, *, youtube, twitch, live_data):
        require(self.state['phase'] in ('VERIFYING', 'RESTORING'), 'not_ready_to_complete')
        require(youtube and twitch and live_data, 'public_av_and_live_data_confirmation_required')
        self.no_unknown()
        self.sleep(self.interval)
        require(self.verification_sample() >= self.stable_seconds, 'live_stability_not_yet_confirmed')
        self.state['live_player_confirmation'] = {'kind': 'operator_attestation', 'at': self.clock(),
                                                  'youtube': True, 'twitch': True, 'live_data': True}
        self.save('RESTORING')
        self.effect('restore_units', 'dell', 'restore_units', restore=self.state['before']['dell']['units'],
                    ledger=self.state['before']['dell']['ledger'])
        self.state['completed_at'] = self.clock()
        self.save('NORMAL')

    def status(self):
        state = self.state
        return {'schema': SCHEMA, 'phase': state['phase'], 'session_id': state['session_id'],
            'generation': state['generation'], 'guard': guard(self.store.root, uid=os.geteuid(), now=self.clock()),
            'safe_to_update_dell': False,
            'pending_effects': [e['name'] for e in state['effects'] if e['result'] != 'APPLIED'],
            'stable_seconds': max(0, state['stability']['sample']['oci']['observed_at'] - state['stability']['since']) if state.get('stability') else 0,
            'player_verification': 'operator_attestation_required'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG, help='configuration (default: %(default)s)')
    parser.add_argument('--lab-api', help='isolated literal loopback adapter; never uses host helper')
    parser.add_argument('--lab-stability-seconds', type=float, default=2)
    parser.add_argument('command', choices=('init', 'preflight', 'begin', 'advance', 'confirm-slate', 'resume',
                                           'verify', 'complete', 'reconcile', 'status', 'check', 'on', 'off'))
    parser.add_argument('--minutes', type=int, default=60)
    parser.add_argument('--reason', default='Scheduled maintenance')
    parser.add_argument('--confirm-youtube', action='store_true')
    parser.add_argument('--confirm-twitch', action='store_true')
    parser.add_argument('--confirm-live-data', action='store_true')
    args = parser.parse_args()
    try:
        require(args.root.is_absolute() and args.root.resolve() == args.root, 'canonical_absolute_state_root_required')
        if not args.lab_api:
            require(os.geteuid() == 0, 'root_required_for_production_adapter')
        if args.command in ('on', 'off'):
            require(sys.stdin.isatty() and sys.stderr.isatty(), 'terminal_required_for_playback_confirmation')
            require(not (args.confirm_youtube or args.confirm_twitch or args.confirm_live_data),
                    'on_requires_confirmation_after_slate_output' if args.command == 'on'
                    else 'off_requires_confirmation_after_live_verification')
        store = Store(args.root)
        if args.command == 'init':
            store.initialize()
            print(json.dumps({'result': 'initialized', 'phase': 'NORMAL'}))
            return 0
        require(guard(args.root, uid=os.geteuid())['reason'] != 'maintenance_state_invalid', 'untrusted_session_store')
        if args.command == 'status':
            state = store.read()
            print(json.dumps({'phase': state['phase'], 'session_id': state['session_id'],
                'guard': guard(args.root, uid=os.geteuid()), 'safe_to_update_dell': False,
                'note': 'read-only saved state; run confirm-slate for fresh entry verification'}))
            return 0
        require(args.config is not None, 'config_required')
        config = validate_config(read_json(args.config, uid=os.geteuid()))
        backend = LabBackend(args.lab_api) if args.lab_api else HostBackend(config)
        with store.lock():
            maintenance = Maintenance(store, backend, config, interval=1.2 if args.lab_api else 2,
                stable_seconds=args.lab_stability_seconds if args.lab_api else STABLE_SECONDS)
            if args.command == 'preflight': maintenance.preflight()
            elif args.command == 'begin': maintenance.begin(minutes=args.minutes, reason=args.reason)
            elif args.command == 'advance': maintenance.advance()
            elif args.command == 'confirm-slate':
                maintenance.confirm_slate(youtube=args.confirm_youtube, twitch=args.confirm_twitch)
            elif args.command == 'resume': maintenance.resume()
            elif args.command == 'verify': maintenance.verify()
            elif args.command == 'complete':
                maintenance.complete(youtube=args.confirm_youtube, twitch=args.confirm_twitch, live_data=args.confirm_live_data)
            elif args.command == 'reconcile': maintenance.reconcile()
            elif args.command == 'check': maintenance.check()
            elif args.command == 'on':
                maintenance.on(minutes=args.minutes, reason=args.reason, confirm=confirm_public_slate)
            elif args.command == 'off':
                if maintenance.state['phase'] != 'NORMAL':
                    print('Restoring live output; requiring 300 seconds of continuous progress before confirmation.',
                          file=sys.stderr, flush=True)
                maintenance.off(confirm=confirm_public_live)
            output = maintenance.status()
            output['safe_to_update_dell'] = args.command in ('confirm-slate', 'on')
            print(json.dumps(output, allow_nan=False))
        return 0
    except (OSError, ValueError, KeyError, TypeError, EOFError, KeyboardInterrupt, subprocess.SubprocessError) as error:
        # Full state/effect history remains on disk; never echo config/SSH stderr.
        reason = str(error) if isinstance(error, ValueError) and re.fullmatch('[a-z_]{1,100}', str(error)) else 'operation_not_confirmed'
        if isinstance(error, (EOFError, KeyboardInterrupt)):
            reason = 'confirmation_interrupted_maintenance_retained'
        print(json.dumps({'result': 'HELD', 'reason': reason,
                          'safe_to_update_dell': False,
                          'detail': 'Inspect saved state; reconcile UNKNOWN before another effect.'}))
        return 130 if isinstance(error, KeyboardInterrupt) else 1


if __name__ == '__main__':
    sys.exit(main())
