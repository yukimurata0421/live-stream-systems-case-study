from __future__ import annotations

import fcntl
import math
import os
import stat
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def projection_lock(
    path: Path,
    *,
    exclusive: bool,
    timeout_sec: float,
) -> Iterator[None]:
    """Bound readers to one complete safe-input projection generation."""

    timeout = float(timeout_sec)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("projection lock timeout must be finite and non-negative")
    path = Path(path)
    flags = (
        (os.O_RDWR | os.O_CREAT if exclusive else os.O_RDONLY)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise RuntimeError("projection lock path must be a regular file")
        if exclusive:
            os.fchmod(descriptor, 0o600)
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("safe-input projection lock deadline exceeded")
                time.sleep(min(0.05, remaining))
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)
