from __future__ import annotations

import os
import re
import stat
from pathlib import Path


UNKNOWN_SOURCE_REVISION = "unknown-source-revision"
MAX_SOURCE_REVISION_LENGTH = 240
_SAFE_SOURCE_REVISION = re.compile(
    rf"^[A-Za-z0-9._@-]{{1,{MAX_SOURCE_REVISION_LENGTH}}}$"
)
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9._-]{1,160}$")
_SAFE_COMMIT = re.compile(r"^[0-9A-Fa-f]{7,64}$")
_ALLOWED_KEYS = frozenset(
    {
        "STREAM_V3_DEPLOYED_REVISION",
        "STREAM_V3_DEPLOYED_COMMIT",
        "STREAM_V3_DEPLOYED_HOTFIX",
    }
)


def normalize_source_revision(value: str) -> str:
    candidate = str(value).strip()
    if not _SAFE_SOURCE_REVISION.fullmatch(candidate):
        return UNKNOWN_SOURCE_REVISION
    return candidate


def read_source_revision(path: Path, *, max_bytes: int = 64 * 1024) -> str:
    """Read only allowlisted, secret-free v3 deployment identity fields."""

    path = Path(path)
    limit = max(1, int(max_bytes))
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            os.close(descriptor)
            return UNKNOWN_SOURCE_REVISION
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            raw = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(raw) > limit or (
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            return UNKNOWN_SOURCE_REVISION
        lines = raw.decode("utf-8").splitlines()
    except (OSError, UnicodeError):
        return UNKNOWN_SOURCE_REVISION

    values: dict[str, str] = {}
    for line in lines:
        key, separator, raw = line.partition("=")
        if separator and key.strip() in _ALLOWED_KEYS:
            candidate = raw.strip()
            valid = (
                _SAFE_COMMIT.fullmatch(candidate)
                if key.strip() == "STREAM_V3_DEPLOYED_COMMIT"
                else _SAFE_LABEL.fullmatch(candidate)
            )
            if valid:
                values[key.strip()] = candidate

    effective = values.get("STREAM_V3_DEPLOYED_HOTFIX") or values.get(
        "STREAM_V3_DEPLOYED_REVISION"
    )
    commit = values.get("STREAM_V3_DEPLOYED_COMMIT")
    if effective and commit:
        return f"{effective}@{commit}"
    return effective or commit or UNKNOWN_SOURCE_REVISION
