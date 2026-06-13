from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

try:
    import fcntl
except ModuleNotFoundError:  # pragma: no cover - exercised on Windows
    fcntl = None  # type: ignore[assignment]

try:
    import msvcrt
except ModuleNotFoundError:  # pragma: no cover - exercised on POSIX
    msvcrt = None  # type: ignore[assignment]


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def lock_holder_pid(runtime_state_file: Path) -> Optional[int]:
    if runtime_state_file.exists():
        try:
            data = json.loads(runtime_state_file.read_text(encoding="utf-8"))
            pid = int(str(data.get("stream_pid", "")).strip())
            if pid > 1:
                return pid
        except Exception:
            return None
    return None


def try_acquire_lock(lock_path: Path) -> Optional[object]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fp = lock_path.open("a+")
    if try_lock_file_handle(fp):
        return fp
    fp.close()
    return None


def try_lock_file_handle(fp: object) -> bool:
    if fcntl is not None:
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (BlockingIOError, OSError):
            return False
    if msvcrt is not None:
        try:
            fp.seek(0)
            if not fp.read(1):
                fp.write("0")
                fp.flush()
            fp.seek(0)
            msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    return True


def display_capture_lock_path(stream_lock_dir: Path, display_name: str) -> Path:
    display_key = "".join(ch if ch.isalnum() else "_" for ch in display_name)
    return stream_lock_dir / f"adsb-stream-new-capture-{display_key}.lock"
