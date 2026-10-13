"""Bounded append-only JSONL I/O shared by the minimal observation releases."""

from __future__ import annotations

import errno
import json
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cra_dell_recovery.json_input import load_object

MAXIMUM_LINE_BYTES = 8 * 1024 * 1024


def _serialized_line(value: Mapping[str, Any]) -> bytes:
    raw = (json.dumps(dict(value), separators=(",", ":"), sort_keys=True) + "\n").encode()
    if len(raw) > MAXIMUM_LINE_BYTES:
        raise ValueError("RESILIENT_SOAK_SAMPLE_TOO_LARGE")
    return raw


def _write_all(handle: Any, raw: bytes) -> None:
    """Write one complete frame or fail without claiming progress."""

    remaining = memoryview(raw)
    while remaining:
        written = handle.write(remaining)
        if written is None or written <= 0:
            raise OSError(errno.EIO, "RESILIENT_SOAK_EVIDENCE_WRITE_NO_PROGRESS")
        remaining = remaining[written:]


def _append_line(path: Path, value: Mapping[str, Any], *, maximum_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    raw = _serialized_line(value)
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("RESILIENT_SOAK_EVIDENCE_NOT_REGULAR_FILE")
        if metadata.st_size + len(raw) > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_CAPACITY_EXHAUSTED")
        with os.fdopen(descriptor, "ab", buffering=0) as handle:
            descriptor = -1
            _write_all(handle, raw)
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _complete_pending_line(
    path: Path, value: Mapping[str, Any], *, maximum_bytes: int, committed_bytes: int | None = None
) -> None:
    """Complete only a byte-for-byte prefix of a durably reserved frame.

    No suffix is discarded. A conflicting or overlong tail remains untouched
    for forensic review. The caller owns the journal lock and validates the
    pending packet/sample and its previous committed identity first.
    """
    expected = _serialized_line(value)
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("RESILIENT_SOAK_EVIDENCE_NOT_REGULAR_FILE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_TOO_LARGE")
        if committed_bytes is not None:
            if not 0 <= committed_bytes <= metadata.st_size:
                raise ValueError("RESILIENT_SOAK_PENDING_PREFIX_CONFLICT")
            suffix_size = metadata.st_size - committed_bytes
            if suffix_size >= len(expected):
                raise ValueError("RESILIENT_SOAK_PENDING_PREFIX_CONFLICT")
            os.lseek(descriptor, committed_bytes, os.SEEK_SET)
            suffix = os.read(descriptor, suffix_size)
        else:
            read_size = min(metadata.st_size, len(expected))
            os.lseek(descriptor, metadata.st_size - read_size, os.SEEK_SET)
            tail = os.read(descriptor, read_size)
            suffix = tail.rsplit(b"\n", 1)[-1]
        if not suffix or not expected.startswith(suffix) or len(suffix) >= len(expected):
            raise ValueError("RESILIENT_SOAK_PENDING_PREFIX_CONFLICT")
        if metadata.st_size + len(expected) - len(suffix) > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_CAPACITY_EXHAUSTED")
        os.lseek(descriptor, 0, os.SEEK_END)
        with os.fdopen(descriptor, "r+b", buffering=0) as handle:
            descriptor = -1
            _write_all(handle, expected[len(suffix):])
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _last_line(path: Path, *, maximum_bytes: int) -> dict[str, Any] | None:
    if not path.exists():
        return None
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("RESILIENT_SOAK_EVIDENCE_NOT_REGULAR_FILE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_TOO_LARGE")
        if metadata.st_size == 0:
            return None
        read_size = min(metadata.st_size, 4 * 1024 * 1024)
        os.lseek(descriptor, metadata.st_size - read_size, os.SEEK_SET)
        raw = os.read(descriptor, read_size)
    finally:
        os.close(descriptor)
    if not raw.endswith(b"\n"):
        raise ValueError("RESILIENT_SOAK_EVIDENCE_PARTIAL_TAIL")
    lines = raw.rstrip(b"\n").splitlines()
    if not lines:
        return None
    try:
        return load_object(lines[-1], maximum_bytes=read_size)
    except ValueError as error:
        if str(error) == "JSON_INPUT_NOT_OBJECT":
            raise ValueError("RESILIENT_SOAK_SAMPLE_NOT_OBJECT") from error
        raise


def _last_committed_line(path: Path, *, committed_bytes: int, maximum_bytes: int) -> dict[str, Any] | None:
    """Read the last frame ending at a state-owned committed byte watermark."""
    if committed_bytes == 0:
        return None
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum_bytes or committed_bytes > metadata.st_size:
            raise ValueError("RESILIENT_SOAK_COMMITTED_PREFIX_INVALID")
        read_size = min(committed_bytes, MAXIMUM_LINE_BYTES)
        os.lseek(descriptor, committed_bytes - read_size, os.SEEK_SET)
        raw = os.read(descriptor, read_size)
    finally:
        os.close(descriptor)
    if len(raw) != read_size or not raw.endswith(b"\n"):
        raise ValueError("RESILIENT_SOAK_COMMITTED_PREFIX_INVALID")
    lines = raw.rstrip(b"\n").splitlines()
    if not lines:
        raise ValueError("RESILIENT_SOAK_COMMITTED_PREFIX_INVALID")
    line = lines[-1]
    return load_object(line, maximum_bytes=MAXIMUM_LINE_BYTES)


def _last_complete_before_partial(path: Path, *, maximum_bytes: int) -> tuple[dict[str, Any] | None, bytes]:
    """Read a bounded incomplete tail without treating it as committed evidence."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_FILE_INVALID")
        read_size = min(metadata.st_size, 2 * MAXIMUM_LINE_BYTES + 1)
        raw = os.pread(descriptor, read_size, metadata.st_size - read_size)
    finally:
        os.close(descriptor)
    if not raw or raw.endswith(b"\n"):
        raise ValueError("RESILIENT_SOAK_EVIDENCE_PARTIAL_TAIL_REQUIRED")
    if b"\n" not in raw:
        if metadata.st_size > MAXIMUM_LINE_BYTES:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_PARTIAL_TAIL_OVERSIZE")
        return None, raw
    previous, partial = raw.rsplit(b"\n", 1)
    if len(partial) >= MAXIMUM_LINE_BYTES:
        raise ValueError("RESILIENT_SOAK_EVIDENCE_PARTIAL_TAIL_OVERSIZE")
    committed = previous.rsplit(b"\n", 1)[-1]
    if not committed or len(committed) > MAXIMUM_LINE_BYTES:
        raise ValueError("RESILIENT_SOAK_COMMITTED_PREFIX_INVALID")
    return load_object(committed, maximum_bytes=MAXIMUM_LINE_BYTES), partial
