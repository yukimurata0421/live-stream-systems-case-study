"""Durable manual-maintenance state and the exclusion contract shared with fallback."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import math
import os
from pathlib import Path
import stat
import tempfile
import time

ROOT = Path('/var/lib/stream-v3-manual-maintenance')
SCHEMA = 'stream_v3.manual_maintenance.v1'
PHASES = {'NORMAL', 'PREPARING', 'AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE',
          'RESUMING', 'VERIFYING', 'RESTORING'}


def read_json(path, *, uid=None, limit=65536):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_size > limit or info.st_mode & 0o022
                or (uid is not None and info.st_uid != uid)):
            raise ValueError('untrusted_state_file')
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError('oversized_state')
    return json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite_json')))


def atomic_json(path, value):
    raw = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode() + b'\n'
    if len(raw) > 65536:
        raise ValueError('state_too_large')
    fd, temporary = tempfile.mkstemp(prefix='.session-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            os.fchmod(handle.fileno(), 0o644)
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


def guard(root=ROOT, *, uid=0, now=None):
    """Keep inhibition on expiry or unreadable state; only a missing installation is normal."""
    result = {'inhibited': True, 'reason': 'maintenance_state_invalid', 'phase': 'UNKNOWN',
              'session_id': None, 'overdue': False}
    try:
        info = root.lstat()
    except FileNotFoundError:
        return {**result, 'inhibited': False, 'reason': 'not_installed', 'phase': 'NORMAL'}
    except OSError:
        return result
    try:
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o022 or info.st_uid != uid:
            return result
        value = read_json(root / 'session.json', uid=uid)
        phase = value['phase']
        if value['schema'] != SCHEMA or phase not in PHASES or type(value['generation']) is not int:
            return result
        if phase != 'NORMAL':
            deadline = value['deadline']
            if (not isinstance(value['session_id'], str) or not value['session_id']
                    or type(deadline) not in (int, float) or not math.isfinite(deadline)):
                return result
            result['overdue'] = (time.time() if now is None else now) >= deadline
        return {**result, 'inhibited': phase != 'NORMAL', 'reason': 'manual_maintenance' if phase != 'NORMAL' else 'normal',
                'phase': phase, 'session_id': value.get('session_id'), 'generation': value['generation']}
    except (OSError, ValueError, TypeError, KeyError):
        return result


@contextmanager
def session_lock(root=ROOT, *, shared=False):
    # The fallback DynamicUser opens the root-owned 0644 lock read-only.
    # A nonblocking shared lock never stalls observation behind a manual operation.
    fd = os.open(root / 'lock', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError('invalid_session_lock')
        fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


class Store:
    def __init__(self, root=ROOT):
        self.root = Path(root)

    def initialize(self):
        self.root.mkdir(mode=0o755, parents=True, exist_ok=False)
        with (self.root / 'lock').open('xb') as handle:
            os.fchmod(handle.fileno(), 0o644)
            handle.flush()
            os.fsync(handle.fileno())
        self.write({'schema': SCHEMA, 'phase': 'NORMAL', 'generation': 0, 'session_id': None,
                    'effects': [], 'history': []})

    def read(self):
        value = read_json(self.root / 'session.json')
        if value.get('schema') != SCHEMA or value.get('phase') not in PHASES:
            raise ValueError('invalid_session')
        return value

    def write(self, value):
        atomic_json(self.root / 'session.json', value)

    def lock(self):
        return session_lock(self.root)
