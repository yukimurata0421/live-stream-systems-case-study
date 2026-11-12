"""Bounded monitoring I/O; generated artifacts never become CRA control inputs."""
from __future__ import annotations

from datetime import datetime, timezone
from http.client import HTTPException
import fcntl
import json
from pathlib import Path
import time
import urllib.request


def utc(stamp=None):
    return datetime.fromtimestamp(time.time() if stamp is None else stamp, timezone.utc).isoformat()


def get_json(url, limit=1048576):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=4) as response:
            raw = response.read(limit + 1)
    except HTTPException:
        raise OSError('http_transport_unavailable') from None
    if len(raw) > limit:
        raise ValueError('response_too_large')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('response_shape_invalid')
    return value


def atomic(path, value):
    path = Path(path)
    tmp = path.with_suffix('.pending')
    with tmp.open('w') as f:
        json.dump(value, f, ensure_ascii=False, allow_nan=False)
        f.flush()
    tmp.replace(path)


def append(path, values):
    """Persist logs before the cursor; a crash may replay the same event_id."""
    path = Path(path)
    if not values:
        return
    raw = ''.join(json.dumps(v, ensure_ascii=False, separators=(',', ':'), allow_nan=False)+'\n' for v in values)
    if len(raw.encode()) > 1048576:
        raise ValueError('log_batch_too_large')
    with path.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if path.exists() and path.stat().st_size > 10*1024*1024:
            for i in range(6, 0, -1):
                source = path.with_name(f'events.{i}.jsonl')
                if source.exists():
                    source.replace(path.with_name(f'events.{i+1}.jsonl'))
            path.replace(path.with_name('events.1.jsonl'))
        with path.open('a') as f:
            f.write(raw)
            f.flush()
            import os
            os.fsync(f.fileno())
