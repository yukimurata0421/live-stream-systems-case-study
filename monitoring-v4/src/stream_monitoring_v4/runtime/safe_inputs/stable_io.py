from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from pathlib import Path

from stream_monitoring_v4.adapters.json_file import SnapshotReadError

from .constants import (
    LIFECYCLE_MAX_LINE_BYTES,
    LIFECYCLE_MAX_LINES,
    LIFECYCLE_TAIL_BYTES,
)


def stable_text_lines(
    path: Path,
    *,
    required: bool,
    max_tail_bytes: int = LIFECYCLE_TAIL_BYTES,
    max_line_bytes: int = LIFECYCLE_MAX_LINE_BYTES,
    max_lines: int = LIFECYCLE_MAX_LINES,
) -> Iterator[str]:
    """Yield a stable bounded tail of a potentially large rotated JSONL file."""

    if not path.exists():
        if required:
            raise SnapshotReadError(
                "source_missing",
                f"source file missing: {path.name}",
            )
        return
    if path.is_symlink():
        raise SnapshotReadError(
            "source_symlink_rejected",
            "allowlisted source is a symlink",
        )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SnapshotReadError(
            "source_read_failed",
            f"source read failed: {type(exc).__name__}",
        ) from exc
    try:
        before = os.fstat(descriptor)
    except OSError as exc:
        os.close(descriptor)
        raise SnapshotReadError(
            "source_stat_failed",
            f"source stat failed: {type(exc).__name__}",
        ) from exc
    if not stat.S_ISREG(before.st_mode):
        os.close(descriptor)
        raise SnapshotReadError(
            "source_not_regular",
            "JSONL source must be a regular file",
        )
    with os.fdopen(descriptor, "rb", closefd=True) as handle:
        start = max(0, before.st_size - max(1, int(max_tail_bytes)))
        handle.seek(start)
        if start:
            partial = handle.readline(max(1, int(max_line_bytes)) + 1)
            if len(partial) > max_line_bytes and not partial.endswith(b"\n"):
                raise SnapshotReadError(
                    "source_line_too_large",
                    f"JSONL line exceeds {max_line_bytes} bytes",
                )
        line_count = 0
        while True:
            raw = handle.readline(max(1, int(max_line_bytes)) + 1)
            if not raw:
                break
            line_count += 1
            if line_count > max(1, int(max_lines)):
                raise SnapshotReadError(
                    "source_line_count_exceeded",
                    f"JSONL tail exceeds {max_lines} lines",
                )
            if len(raw) > max_line_bytes and not raw.endswith(b"\n"):
                raise SnapshotReadError(
                    "source_line_too_large",
                    f"JSONL line exceeds {max_line_bytes} bytes",
                )
            try:
                yield raw.decode("utf-8").rstrip("\r\n")
            except UnicodeDecodeError as exc:
                raise SnapshotReadError(
                    "source_json_invalid",
                    "JSONL source is not UTF-8",
                ) from exc
        after_fd = os.fstat(handle.fileno())
    try:
        after_path = path.lstat()
    except OSError as exc:
        raise SnapshotReadError(
            "source_changed_during_read",
            f"source path changed: {type(exc).__name__}",
        ) from exc

    def identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            stat.S_IFMT(value.st_mode),
        )

    if (
        not stat.S_ISREG(after_path.st_mode)
        or identity(before) != identity(after_fd)
        or identity(before) != identity(after_path)
    ):
        raise SnapshotReadError(
            "source_changed_during_read",
            "source changed during stable JSONL read",
        )
