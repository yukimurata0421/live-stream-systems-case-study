from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path


def atomic_write_bytes(
    path: Path,
    content: bytes,
    *,
    mode: int = 0o600,
    skip_if_unchanged: bool = False,
) -> bool:
    """Replace one regular file durably without following target/temp symlinks."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    desired_mode = int(mode) & 0o777
    if skip_if_unchanged:
        try:
            descriptor = os.open(target, os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0))
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise OSError(f"atomic target cannot be opened safely: {target.name}") from exc
        else:
            with os.fdopen(descriptor, "rb", closefd=True) as current:
                metadata = os.fstat(current.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise OSError(f"atomic target is not a regular file: {target.name}")
                if current.read(len(content) + 1) == content:
                    mode_changed = stat.S_IMODE(metadata.st_mode) != desired_mode
                    if mode_changed:
                        os.fchmod(current.fileno(), desired_mode)
                        os.fsync(current.fileno())
                    return mode_changed

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, desired_mode)
        os.replace(temporary, target)
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(target.parent, directory_flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return True


def atomic_write_text(
    path: Path,
    content: str,
    *,
    encoding: str = "utf-8",
    mode: int = 0o600,
    skip_if_unchanged: bool = False,
) -> bool:
    return atomic_write_bytes(
        path,
        content.encode(encoding),
        mode=mode,
        skip_if_unchanged=skip_if_unchanged,
    )
