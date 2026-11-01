"""Bounded producer-time diagnostics; no raw payloads, providers or effects."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import stat

from stream_core.common import observation_time as timing


def record(root, source, payload, *, now=None, result=None, max_bytes=2 * 1024 * 1024):
    now = timing.capture() if now is None else now
    metadata = payload.get("observation_time", {})
    if not isinstance(metadata, dict):
        metadata = {}
    qualified = timing.qualify(metadata, now=now, source=source, max_age=180.)
    row = {"source": source, "checked_at_utc": now.wall, "receiver_boot_id": now.boot,
           "receiver_boottime": now.mono, "status": qualified.status, "reason": qualified.reason,
           "age_upper_sec": qualified.age_upper, "utc_status": qualified.utc_status,
           "source_boot_id": metadata.get("boot_id"), "sample_id": metadata.get("sample_id"),
           "observed_at_utc": metadata.get("observed_at_utc"),
           "observed_boottime": metadata.get("observed_boottime")}
    upstream = payload.get("upstream_clock_host")
    row["upstream_clock_host"] = upstream[:128] if isinstance(upstream, str) else None
    model = timing.upstream_clock_model(payload, now=now)
    if model is not None:
        row["upstream_clock"] = model.diagnostic(now={"wall": now.wall, "mono": now.mono})
    for field in ("sample_ts", "source_now"):
        value = payload.get(field)
        row[field] = value if timing.finite(value) else None
    if result:
        row["source_timing"] = {k: result.get(k) for k in
            ("qualification", "sample_age", "heartbeat_age_at_sample", "source_lag_bounds_sec", "capture_fresh", "heartbeat_fresh")}
    # Every scalar is selected explicitly; reason/source cannot contain payloads.
    encoded = (json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n").encode()
    if len(encoded) > min(max_bytes, 8192):
        raise ValueError("notification_diagnostic_record_too_large")
    root = Path(root)
    if root.is_symlink():
        raise ValueError("notification_diagnostic_symlink")
    root.mkdir(parents=True, exist_ok=True)
    lock = os.open(root / ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(lock).st_mode):
            raise ValueError("notification_diagnostic_not_regular")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        latest, log, previous = root / "latest.json", root / "events.jsonl", root / "events.jsonl.1"
        if any(p.is_symlink() for p in (latest, log, previous)):
            raise ValueError("notification_diagnostic_symlink")
        try:
            fd = os.open(latest, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as handle:
                if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                    raise ValueError("notification_diagnostic_not_regular")
                raw = handle.read(8193)
            old = json.loads(raw) if len(raw) <= 8192 else {}
            old = old if isinstance(old, dict) else {}
        except FileNotFoundError:
            old = {}
        except json.JSONDecodeError:
            old = {}
        if old.get("sample_id") == row["sample_id"] and row["sample_id"] is not None:
            return False
        fd = os.open(log, os.O_CREAT | os.O_WRONLY | os.O_APPEND | os.O_NONBLOCK | os.O_NOFOLLOW, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("notification_diagnostic_not_regular")
            if os.fstat(fd).st_size + len(encoded) > max_bytes:
                os.close(fd)
                fd = -1
                log.replace(previous)
                fd = os.open(log, os.O_CREAT | os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, 0o600)
            os.write(fd, encoded)
        finally:
            if fd >= 0:
                os.close(fd)
        temporary = root / "latest.tmp"
        fd = os.open(temporary, os.O_CREAT | os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
        temporary.replace(latest)
        return True
    finally:
        os.close(lock)
