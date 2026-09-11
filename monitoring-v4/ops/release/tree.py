from __future__ import annotations

import contextlib
import ctypes
import errno
import fcntl
import hashlib
import os
import secrets
import shutil
import stat
from collections.abc import Iterator
from pathlib import Path

from .model import (
    ALLOWED_TOP_LEVEL,
    DIRECTORY_MODE,
    FILE_MODE,
    GENERATED_DIRECTORY_NAMES,
    GENERATED_FILE_NAMES,
    GENERATED_FILE_SUFFIXES,
    LOCK_MODE,
    MAX_DIRECTORIES,
    MAX_FILE_BYTES,
    MAX_FILES,
    MAX_PATH_BYTES,
    MAX_TREE_BYTES,
    REQUIRED_PATHS,
    TreeRecord,
    existing_real_directory,
)


_AT_FDCWD = -100
_RENAME_NOREPLACE = 1


def _validate_relative_path(relative: str) -> bytes:
    encoded = relative.encode("utf-8")
    if not relative or len(encoded) > MAX_PATH_BYTES or any(
        ord(character) < 32 or ord(character) == 127 for character in relative
    ):
        raise RuntimeError("release source contains an invalid path")
    parts = Path(relative).parts
    if any(part in GENERATED_DIRECTORY_NAMES for part in parts) or (
        parts[-1] in GENERATED_FILE_NAMES
        or parts[-1].endswith(GENERATED_FILE_SUFFIXES)
    ):
        raise RuntimeError(f"release tree contains a generated artifact: {relative}")
    return encoded


def _stable_file_record(root: Path, path: Path) -> TreeRecord:
    relative = path.relative_to(root).as_posix()
    _validate_relative_path(relative)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError(f"release source file cannot be opened safely: {relative}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise RuntimeError(f"release source must contain only single-link files: {relative}")
        if before.st_size < 0 or before.st_size > MAX_FILE_BYTES:
            raise RuntimeError(f"release source file is outside the size bound: {relative}")
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_FILE_BYTES:
                raise RuntimeError(f"release source file exceeded the size bound: {relative}")
            digest.update(chunk)
        after = os.fstat(descriptor)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
            before.st_mode,
            before.st_uid,
            before.st_gid,
            before.st_nlink,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
            after.st_mode,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
        )
        if total != before.st_size or before_identity != after_identity:
            raise RuntimeError(f"release source changed while it was read: {relative}")
        return TreeRecord(relative=relative, size=total, digest=digest.digest())
    finally:
        os.close(descriptor)


def tree_records(
    root: Path,
    *,
    require_source_layout: bool,
    exclude_rendered: bool = False,
) -> tuple[TreeRecord, ...]:
    records: list[TreeRecord] = []
    top_level: set[str] = set()
    directory_count = 0
    for directory, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current = Path(directory)
        if exclude_rendered and current == root / "deploy":
            directory_names[:] = [name for name in directory_names if name != "k3s-rendered"]
        for name in sorted(directory_names):
            child = current / name
            metadata = os.lstat(child)
            if not stat.S_ISDIR(metadata.st_mode):
                raise RuntimeError("release tree must not contain directory symlinks")
            relative = child.relative_to(root)
            _validate_relative_path(relative.as_posix())
            directory_count += 1
            if directory_count > MAX_DIRECTORIES:
                raise RuntimeError("release tree exceeds the directory-count bound")
            if len(relative.parts) == 1:
                top_level.add(name)
        for name in sorted(file_names):
            child = current / name
            if len(child.relative_to(root).parts) == 1:
                top_level.add(name)
            records.append(_stable_file_record(root, child))
            if len(records) > MAX_FILES:
                raise RuntimeError("release tree exceeds the file-count bound")
    total_bytes = sum(record.size for record in records)
    if total_bytes > MAX_TREE_BYTES:
        raise RuntimeError("release tree exceeds the total-size bound")
    if require_source_layout:
        unexpected = sorted(top_level - ALLOWED_TOP_LEVEL)
        if unexpected:
            raise RuntimeError(f"release source has unexpected top-level entries: {unexpected}")
        if not exclude_rendered and (
            (root / "deploy/k3s-rendered").exists()
            or (root / "deploy/k3s-rendered").is_symlink()
        ):
            raise RuntimeError("release source must not contain a pre-rendered manifest tree")
        missing = [relative for relative in REQUIRED_PATHS if not (root / relative).is_file()]
        if missing:
            raise RuntimeError(f"release source is missing required paths: {missing}")
    return tuple(sorted(records, key=lambda item: item.relative))


def records_digest(records: tuple[TreeRecord, ...]) -> str:
    digest = hashlib.sha256()
    for record in records:
        relative = record.relative.encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(record.size.to_bytes(8, "big"))
        digest.update(record.digest)
    return digest.hexdigest()


@contextlib.contextmanager
def release_lock(release_base: Path) -> Iterator[None]:
    base = existing_real_directory(release_base, label="release base")
    lock_path = base / ".release.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, LOCK_MODE)
    except OSError as exc:
        raise RuntimeError("release lock cannot be opened safely") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid not in {0, os.getuid()}
        ):
            raise RuntimeError("release lock has an unsafe identity")
        os.fchmod(descriptor, LOCK_MODE)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another release operation holds the release lock") from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def copy_source(source: Path, staging: Path) -> None:
    # Preserve a raced symlink so post-copy validation rejects it instead of dereferencing it.
    shutil.copytree(source, staging, copy_function=shutil.copyfile, symlinks=True)
    for directory, _, file_names in os.walk(staging, topdown=False, followlinks=False):
        current = Path(directory)
        for name in file_names:
            os.chmod(current / name, 0o644, follow_symlinks=False)
        os.chmod(current, 0o755, follow_symlinks=False)


def freeze_tree(root: Path) -> None:
    directories: list[Path] = []
    for directory, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current = Path(directory)
        directories.append(current)
        for name in directory_names:
            if (current / name).is_symlink():
                raise RuntimeError("release tree must not contain symlinks")
        for name in file_names:
            path = current / name
            if path.is_symlink() or not path.is_file():
                raise RuntimeError("release tree must contain only regular files")
            os.chmod(path, FILE_MODE, follow_symlinks=False)
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    for directory in reversed(directories):
        os.chmod(directory, DIRECTORY_MODE, follow_symlinks=False)
        descriptor = os.open(
            directory,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def validate_frozen_tree(root: Path) -> tuple[int, int]:
    records = tree_records(root, require_source_layout=False)
    for directory, _, file_names in os.walk(root, topdown=True, followlinks=False):
        current = Path(directory)
        if stat.S_IMODE(os.lstat(current).st_mode) != DIRECTORY_MODE:
            raise RuntimeError("release directory permissions are not immutable")
        for name in file_names:
            if stat.S_IMODE(os.lstat(current / name).st_mode) != FILE_MODE:
                raise RuntimeError("release file permissions are not immutable")
    return len(records), sum(record.size for record in records)


def rename_noreplace(source: Path, target: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise RuntimeError("atomic no-replace rename is unavailable")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(target),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        code = ctypes.get_errno()
        if code == errno.EEXIST:
            raise RuntimeError("release target already exists")
        raise OSError(code, os.strerror(code), os.fspath(target))


def fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_staging(staging: Path) -> None:
    if staging.is_symlink() or not staging.exists():
        return
    for directory, directory_names, file_names in os.walk(
        staging,
        topdown=False,
        followlinks=False,
    ):
        current = Path(directory)
        os.chmod(current, 0o700, follow_symlinks=False)
        for name in file_names:
            (current / name).unlink()
        for name in directory_names:
            child = current / name
            if child.is_symlink():
                child.unlink()
            else:
                child.rmdir()
    staging.rmdir()
