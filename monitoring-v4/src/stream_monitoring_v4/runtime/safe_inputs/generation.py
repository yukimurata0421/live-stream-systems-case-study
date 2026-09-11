from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from stream_contracts.monitoring_v4.ids import stable_id
from stream_monitoring_v4.adapters.json_file import (
    SnapshotReadError,
    read_json_snapshot,
)
from stream_monitoring_v4.runtime.atomic_file import atomic_write_bytes
from stream_monitoring_v4.runtime.source_revision import (
    MAX_SOURCE_REVISION_LENGTH,
    UNKNOWN_SOURCE_REVISION,
    normalize_source_revision,
    read_source_revision,
)

from .constants import (
    SAFE_JSON_FILES,
    SAFE_OUTBOX_FILE,
    SAFE_LIFECYCLE_FILE,
    SAFE_REVISION_FILE,
    SAFE_ROLLOUT_FILE,
)


SAFE_GENERATION_MANIFEST = ".projection-generation.json"
SAFE_GENERATION_SCHEMA = "monitoring_v4.safe_input_generation.v2"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MANIFEST_MAX_BYTES = 64 * 1024
_DEFAULT_FILE_MAX_BYTES = 4 * 1024 * 1024
_LIFECYCLE_FILE_MAX_BYTES = 20 * 1024 * 1024
_REVISION_FILE_MAX_BYTES = 4096


def projected_file_names() -> tuple[str, ...]:
    return (
        *SAFE_JSON_FILES,
        SAFE_OUTBOX_FILE,
        SAFE_LIFECYCLE_FILE,
        SAFE_ROLLOUT_FILE,
        SAFE_REVISION_FILE,
    )


def _file_limit(name: str) -> int:
    if name == SAFE_LIFECYCLE_FILE:
        return _LIFECYCLE_FILE_MAX_BYTES
    if name == SAFE_REVISION_FILE:
        return _REVISION_FILE_MAX_BYTES
    return _DEFAULT_FILE_MAX_BYTES


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        stat.S_IFMT(value.st_mode),
    )


def _read_regular(path: Path, *, max_bytes: int) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError(f"safe input is not a regular file: {path.name}")
        if stat.S_IMODE(before.st_mode) & 0o077:
            raise RuntimeError(f"safe input permissions are too broad: {path.name}")
        if before.st_size > max_bytes:
            raise RuntimeError(f"safe input exceeds its size boundary: {path.name}")
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = -1
            content = stream.read(max_bytes + 1)
            after = os.fstat(stream.fileno())
        if len(content) > max_bytes or _identity(before) != _identity(after):
            raise RuntimeError(f"safe input changed during validation: {path.name}")
        final = path.lstat()
        if _identity(before) != _identity(final):
            raise RuntimeError(f"safe input path changed during validation: {path.name}")
        return content
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _file_metadata(contents: Mapping[str, bytes]) -> dict[str, dict[str, object]]:
    expected = set(projected_file_names())
    if set(contents) != expected:
        missing = sorted(expected - set(contents))
        unexpected = sorted(set(contents) - expected)
        raise ValueError(
            f"safe input generation is incomplete: missing={missing} unexpected={unexpected}"
        )
    return {
        name: {
            "sha256": hashlib.sha256(contents[name]).hexdigest(),
            "size_bytes": len(contents[name]),
        }
        for name in projected_file_names()
    }


def _generation_id(source_revision: str, files: Mapping[str, object]) -> str:
    return stable_id(
        "sig",
        source_revision,
        [[name, files[name]] for name in projected_file_names()],
    )


def publish_safe_input_generation(
    target_root: Path,
    *,
    contents: Mapping[str, bytes],
    source_revision: str,
) -> str:
    """Publish the digest manifest last, after every generation member."""

    if (
        not isinstance(source_revision, str)
        or not source_revision
        or source_revision == UNKNOWN_SOURCE_REVISION
        or len(source_revision) > MAX_SOURCE_REVISION_LENGTH
        or normalize_source_revision(source_revision) != source_revision
    ):
        raise ValueError("safe input generation source revision is invalid")
    files = _file_metadata(contents)
    generation_id = _generation_id(source_revision, files)
    payload = {
        "schema": SAFE_GENERATION_SCHEMA,
        "generation_id": generation_id,
        "source_revision": source_revision,
        "files": files,
    }
    rendered = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    atomic_write_bytes(
        Path(target_root) / SAFE_GENERATION_MANIFEST,
        rendered,
        mode=0o600,
        skip_if_unchanged=True,
    )
    return generation_id


@dataclass(frozen=True)
class ValidatedSafeInputGeneration:
    generation_id: str
    source_revision: str


def validate_safe_input_generation(root: Path) -> ValidatedSafeInputGeneration:
    """Fail closed unless every projected file matches the last commit manifest."""

    root = Path(root)
    if root.is_symlink():
        raise RuntimeError("safe input generation root must not be a symlink")
    manifest_path = root / SAFE_GENERATION_MANIFEST
    try:
        manifest = read_json_snapshot(
            manifest_path,
            max_bytes=_MANIFEST_MAX_BYTES,
        ).payload
    except SnapshotReadError as exc:
        raise RuntimeError(
            f"safe input generation manifest is unavailable: {exc.reason_code}"
        ) from exc
    manifest_mode = stat.S_IMODE(manifest_path.lstat().st_mode)
    if manifest_mode & 0o077:
        raise RuntimeError("safe input generation manifest permissions are too broad")
    if manifest.get("schema") != SAFE_GENERATION_SCHEMA:
        raise RuntimeError("safe input generation manifest schema is unsupported")
    source_revision = manifest.get("source_revision")
    generation_id = manifest.get("generation_id")
    files = manifest.get("files")
    if (
        not isinstance(source_revision, str)
        or not source_revision
        or source_revision == UNKNOWN_SOURCE_REVISION
        or len(source_revision) > MAX_SOURCE_REVISION_LENGTH
        or normalize_source_revision(source_revision) != source_revision
    ):
        raise RuntimeError("safe input generation source revision is invalid")
    if not isinstance(generation_id, str) or not generation_id:
        raise RuntimeError("safe input generation id is invalid")
    if not isinstance(files, dict) or set(files) != set(projected_file_names()):
        raise RuntimeError("safe input generation file set is incomplete")
    normalized: dict[str, dict[str, object]] = {}
    for name in projected_file_names():
        item = files[name]
        if not isinstance(item, dict) or set(item) != {"sha256", "size_bytes"}:
            raise RuntimeError(f"safe input generation metadata is invalid: {name}")
        digest = item.get("sha256")
        size = item.get("size_bytes")
        if (
            not isinstance(digest, str)
            or _SHA256.fullmatch(digest) is None
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or size > _file_limit(name)
        ):
            raise RuntimeError(f"safe input generation metadata is invalid: {name}")
        content = _read_regular(root / name, max_bytes=_file_limit(name))
        if len(content) != size or hashlib.sha256(content).hexdigest() != digest:
            raise RuntimeError(f"safe input generation digest mismatch: {name}")
        normalized[name] = {"sha256": digest, "size_bytes": size}
    expected_id = _generation_id(source_revision, normalized)
    if generation_id != expected_id:
        raise RuntimeError("safe input generation id does not match its members")
    if read_source_revision(root / SAFE_REVISION_FILE) != source_revision:
        raise RuntimeError("safe input generation source revision does not match its file")
    return ValidatedSafeInputGeneration(generation_id, source_revision)
