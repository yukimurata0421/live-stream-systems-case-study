#!/usr/bin/env python3
"""Start the armed 48-hour smoke once after confirmed normal-delivery recovery."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import tempfile
import time

ROOT = Path('/var/lib/stream-v3-nvidia-smoke')
MAINTENANCE = Path('/var/lib/stream-v3-manual-maintenance/session.json')
SMOKE = Path(__file__).resolve().with_name('nvidia_driver_smoke.py')


def read_json(path, *, uid=0):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != uid or info.st_mode & 0o022 or info.st_size > 65536:
            raise ValueError('UNTRUSTED_STATE')
        return json.loads(handle.read(65537), parse_constant=lambda _: (_ for _ in ()).throw(ValueError('NONFINITE')))


def write_json(path, value):
    raw = json.dumps(value, sort_keys=True, allow_nan=False).encode() + b'\n'
    if len(raw) > 65536:
        raise ValueError('STATE_TOO_LARGE')
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.launch-')
    try:
        with os.fdopen(fd, 'wb') as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def machine():
    return hashlib.sha256(Path('/etc/machine-id').read_text().strip().encode()).hexdigest()


def command(args, *, timeout=70):
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=False)


class Launcher:
    def __init__(self, root=ROOT, maintenance=MAINTENANCE, *, uid=0):
        self.root, self.maintenance, self.uid = Path(root), Path(maintenance), uid
        self.path = self.root / 'launch.json'

    def save(self, value):
        write_json(self.path, value)

    def arm(self, run_id, expected):
        if self.path.exists() or self.path.is_symlink():
            raise ValueError('LAUNCH_ALREADY_ARMED')
        session = read_json(self.maintenance, uid=self.uid)
        if session.get('schema') != 'stream_v3.manual_maintenance.v1' or session.get('phase') not in {
                'AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE'}:
            raise ValueError('ACTIVE_MAINTENANCE_REQUIRED')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        value = {'schema': 'stream_v3.nvidia_smoke_launch.v1', 'stage': 'ARMED',
                 'run_id': run_id, 'expected_driver': expected, 'armed_at': time.time(),
                 'session_id': session['session_id'], 'generation': session['generation'], 'machine': machine(),
                 'smoke_sha256': hashlib.sha256(SMOKE.read_bytes()).hexdigest(), 'timer_requested': False}
        self.save(value)
        return value

    def run_status(self, run_id):
        path = self.root / 'smoke.sqlite3'
        if not path.exists():
            return None
        if path.is_symlink():
            raise ValueError('DATABASE_SYMLINK')
        db = sqlite3.connect(path.absolute().as_uri() + '?mode=ro', uri=True, timeout=1)
        try:
            if db.execute('PRAGMA application_id').fetchone()[0] != 0x4E565331 or db.execute('PRAGMA user_version').fetchone()[0] != 1:
                raise ValueError('FOREIGN_SMOKE_DATABASE')
            return db.execute('SELECT status FROM runs WHERE id=?', (run_id,)).fetchone()
        finally:
            db.close()

    def tick(self):
        value = read_json(self.path, uid=self.uid)
        if value['stage'] in {'STARTED', 'TERMINAL', 'UNKNOWN'}:
            return value
        if value['machine'] != machine() or value['smoke_sha256'] != hashlib.sha256(SMOKE.read_bytes()).hexdigest():
            raise ValueError('LAUNCH_IDENTITY_CHANGED')
        if value['stage'] == 'ARMED':
            session = read_json(self.maintenance, uid=self.uid)
            if (session.get('schema') != 'stream_v3.manual_maintenance.v1'
                    or session.get('session_id') != value['session_id']
                    or session.get('generation') != value['generation']):
                raise ValueError('MAINTENANCE_SESSION_CHANGED')
            attestation = session.get('live_player_confirmation', {})
            if session['phase'] != 'NORMAL':
                return {**value, 'waiting_for': 'NORMAL'}
            if (attestation.get('kind') != 'operator_attestation'
                    or any(attestation.get(k) is not True for k in ('youtube', 'twitch', 'live_data'))
                    or not value['armed_at'] <= attestation.get('at', 0) <= session.get('completed_at', 0)):
                raise ValueError('LIVE_PLAYER_CONFIRMATION_MISSING')
            if Path('/sys/module/nvidia/version').read_text().strip() != value['expected_driver']:
                return {**value, 'waiting_for': 'EXPECTED_DRIVER'}
            # Persist the intent before invoking the existing CLI. Never resend an unknown start.
            value['stage'] = 'START_REQUESTED'
            self.save(value)
            try:
                result = command(['/usr/bin/python3', '-I', '-B', str(SMOKE), '--state-dir', str(self.root),
                                  'start', '--run-id', value['run_id'], '--expected-driver', value['expected_driver']])
                value['start_exit'] = result.returncode
            except (OSError, subprocess.TimeoutExpired) as exc:
                value['start_error'] = type(exc).__name__
            self.save(value)
        found = self.run_status(value['run_id'])
        if not found:
            value.update(stage='UNKNOWN', reason='START_OUTCOME_NOT_FOUND_NO_RETRY')
        elif found[0] != 'RUNNING':
            value.update(stage='TERMINAL', reason='RECORDED_' + found[0])
        else:
            active = command(['/usr/bin/systemctl', 'is-active', 'stream-v3-nvidia-smoke.timer'], timeout=10)
            enabled = command(['/usr/bin/systemctl', 'is-enabled', 'stream-v3-nvidia-smoke.timer'], timeout=10)
            if active.stdout.strip() != 'active' or enabled.stdout.strip() != 'enabled':
                if value['timer_requested']:
                    value.update(stage='UNKNOWN', reason='TIMER_OUTCOME_UNKNOWN_NO_RETRY')
                else:
                    value['timer_requested'] = True
                    self.save(value)
                    command(['/usr/bin/systemctl', 'enable', '--now', 'stream-v3-nvidia-smoke.timer'], timeout=20)
                    active = command(['/usr/bin/systemctl', 'is-active', 'stream-v3-nvidia-smoke.timer'], timeout=10)
                    enabled = command(['/usr/bin/systemctl', 'is-enabled', 'stream-v3-nvidia-smoke.timer'], timeout=10)
                    value['stage'] = 'STARTED' if active.stdout.strip() == 'active' and enabled.stdout.strip() == 'enabled' else 'UNKNOWN'
            else:
                value['stage'] = 'STARTED'
        self.save(value)
        return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    arm = sub.add_parser('arm')
    arm.add_argument('--run-id', required=True)
    arm.add_argument('--expected-driver', required=True)
    sub.add_parser('tick')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('root required')
    if args.command == 'arm' and (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', args.run_id)
                                  or not re.fullmatch(r'580\.\d+(?:\.\d+)?', args.expected_driver)):
        parser.error('invalid run ID or R580 version')
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(ROOT / 'launch.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        runner = Launcher()
        result = runner.arm(args.run_id, args.expected_driver) if args.command == 'arm' else runner.tick()
        print(json.dumps({k: result[k] for k in ('stage', 'run_id', 'expected_driver', 'waiting_for', 'reason') if k in result}))
        return 1 if result['stage'] in {'UNKNOWN', 'TERMINAL'} else 0
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.TimeoutExpired) as exc:
        print(json.dumps({'stage': 'ERROR', 'error': type(exc).__name__}))
        return 2
    finally:
        os.close(fd)


if __name__ == '__main__':
    raise SystemExit(main())
