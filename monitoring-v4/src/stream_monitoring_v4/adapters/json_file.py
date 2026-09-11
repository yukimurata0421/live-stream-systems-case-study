from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.time import parse_utc, utc_text


class SnapshotReadError(RuntimeError):
    def __init__(self, reason_code: str, detail: str, *, payload_sha256: str = "") -> None:
        super().__init__(detail)
        self.reason_code = reason_code
        self.detail = detail
        self.payload_sha256 = payload_sha256


def strict_json_loads(raw: bytes | str) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number is not supported: {value}")

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError(f"non-finite JSON number is not supported: {value}")
        return parsed

    def bounded_int(value: str) -> int:
        parsed = int(value)
        if not -(2**63) <= parsed <= 2**63 - 1:
            raise ValueError("JSON integer is outside the signed 64-bit boundary")
        return parsed

    return json.loads(
        raw,
        parse_constant=reject_constant,
        parse_float=finite_float,
        parse_int=bounded_int,
    )


@dataclass(frozen=True)
class JsonSnapshot:
    payload: Mapping[str, Any]
    sha256: str
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int
    mode: int
    uid: int
    gid: int
    received_at: str


def read_json_snapshot(
    path: Path,
    *,
    max_bytes: int = 2 * 1024 * 1024,
    received_at: str | None = None,
) -> JsonSnapshot:
    """Read one stable JSON snapshot and timestamp its actual receipt.

    ``received_at`` is only an injected deterministic clock for replay/tests.
    Live callers leave it unset, so the receipt is captured after the second
    stat and JSON validation rather than at scheduler/process start.
    """

    if received_at is not None:
        parse_utc(received_at, field="received_at")
    limit = max(1, int(max_bytes))
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as exc:
        raise SnapshotReadError("source_missing", f"source file missing: {path.name}") from exc
    except OSError as exc:
        reason = "source_symlink_rejected" if path.is_symlink() else "source_open_failed"
        raise SnapshotReadError(reason, f"source open failed: {type(exc).__name__}") from exc
    try:
        before = os.fstat(descriptor)
    except OSError as exc:
        os.close(descriptor)
        raise SnapshotReadError("source_stat_failed", f"source stat failed: {type(exc).__name__}") from exc
    if not stat.S_ISREG(before.st_mode):
        os.close(descriptor)
        raise SnapshotReadError("source_not_regular", "source must be a regular file")
    if before.st_size > limit:
        os.close(descriptor)
        raise SnapshotReadError("source_too_large", f"source exceeds {limit} bytes")
    try:
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            try:
                raw = stream.read(limit + 1)
            except OSError as exc:
                raise SnapshotReadError(
                    "source_read_failed",
                    f"source read failed: {type(exc).__name__}",
                ) from exc
            after = os.fstat(stream.fileno())
    except SnapshotReadError:
        raise
    except OSError as exc:
        raise SnapshotReadError("source_stat_failed", f"source stat failed: {type(exc).__name__}") from exc
    if len(raw) > limit:
        raise SnapshotReadError("source_too_large", f"source exceeds {limit} bytes")
    digest = hashlib.sha256(raw).hexdigest()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise SnapshotReadError(
            "source_changed_during_read",
            "source identity, size, or mtime changed during read",
            payload_sha256=digest,
        )
    try:
        payload = strict_json_loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise SnapshotReadError("source_json_invalid", f"invalid JSON: {type(exc).__name__}", payload_sha256=digest) from exc
    if not isinstance(payload, dict):
        raise SnapshotReadError("source_not_object", "source JSON root must be an object", payload_sha256=digest)
    receipt = received_at or utc_text(int(time.time()))
    return JsonSnapshot(
        payload=payload,
        sha256=digest,
        size=len(raw),
        mtime_ns=after.st_mtime_ns,
        ctime_ns=after.st_ctime_ns,
        device=after.st_dev,
        inode=after.st_ino,
        mode=stat.S_IMODE(after.st_mode),
        uid=after.st_uid,
        gid=after.st_gid,
        received_at=receipt,
    )
