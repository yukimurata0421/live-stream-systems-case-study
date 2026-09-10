"""Process-scoped, credential-free registry for owner-spawned children.

The registry closes the gap between ``Popen`` returning and procfs sampling.
It never stores argv, environment, file descriptors inherited by the child, or
any stream credential. Unknown launch/exec identity remains unproven.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

MAXIMUM_REGISTERED_CHILDREN = 256


class ChildLifecycleRegistry:
    def __init__(self, *, proc_root: Path = Path("/proc")) -> None:
        self.proc_root = proc_root
        self.owner_pid = os.getpid()
        self._lock = threading.RLock()
        self._entries: dict[int, dict[str, Any]] = {}
        self._pidfds: dict[int, int] = {}
        self._original_popen: Any = None
        self._wrapper: Any = None
        self._installed = False
        self._owner_start_ticks = self._proc_stat(self.owner_pid)[2]

    def _proc_stat(self, pid: int) -> tuple[str, int, int]:
        raw = (self.proc_root / str(pid) / "stat").read_text(encoding="ascii")
        fields = raw[raw.rfind(")") + 2 :].split()
        if not raw.startswith(f"{pid} (") or len(fields) < 20 or int(fields[19]) <= 0:
            raise ValueError("CHILD_REGISTRY_PROC_STAT_INVALID")
        return fields[0], int(fields[1]), int(fields[19])

    def _resolved_launch_identity(self, popenargs: tuple[Any, ...], kwargs: Mapping[str, Any]) -> tuple[str, int, int] | None:
        executable = kwargs.get("executable")
        if kwargs.get("shell"):
            executable = executable or "/bin/sh"
        elif executable is None and popenargs:
            command = popenargs[0]
            if isinstance(command, (list, tuple)) and command:
                executable = command[0]
            elif isinstance(command, (str, bytes, os.PathLike)):
                executable = command
        if isinstance(executable, bytes):
            executable = os.fsdecode(executable)
        elif isinstance(executable, os.PathLike):
            executable = os.fspath(executable)
        if not isinstance(executable, str) or not executable:
            return None
        environment = kwargs.get("env")
        search_path = None
        if isinstance(environment, Mapping):
            raw_path = environment.get("PATH")
            if isinstance(raw_path, bytes):
                search_path = os.fsdecode(raw_path)
            elif isinstance(raw_path, str):
                search_path = raw_path
        resolved: str | None
        if os.sep in executable:
            candidate = Path(executable)
            if not candidate.is_absolute():
                cwd = kwargs.get("cwd")
                base = Path(os.fsdecode(cwd)) if isinstance(cwd, (str, bytes, os.PathLike)) else Path.cwd()
                candidate = base / candidate
            resolved = str(candidate)
        else:
            resolved = shutil.which(executable, path=search_path)
        if not resolved:
            return None
        try:
            meta = os.stat(resolved)
        except OSError:
            return None
        if not stat.S_ISREG(meta.st_mode):
            return None
        role = "DELIVERY_CANDIDATE" if Path(resolved).name == "ffmpeg" else "AUXILIARY"
        return role, meta.st_dev, meta.st_ino

    def _proc_executable(self, pid: int) -> tuple[str, int, int]:
        path = self.proc_root / str(pid) / "exe"
        meta = path.stat()
        if not stat.S_ISREG(meta.st_mode):
            raise ValueError("CHILD_REGISTRY_EXECUTABLE_INVALID")
        try:
            name = Path(os.readlink(path)).name
        except OSError:
            name = ""
        role = "DELIVERY_CANDIDATE" if name == "ffmpeg" else "AUXILIARY"
        return role, meta.st_dev, meta.st_ino

    def _drop_locked(self, pid: int) -> None:
        self._entries.pop(pid, None)
        pidfd = self._pidfds.pop(pid, None)
        if pidfd is not None:
            with suppress(OSError):
                os.close(pidfd)

    def _prune_locked(self) -> None:
        for pid, entry in list(self._entries.items()):
            try:
                _, parent, ticks = self._proc_stat(pid)
            except (OSError, ValueError):
                self._drop_locked(pid)
                continue
            if parent != self.owner_pid or ticks != entry["start_ticks"]:
                self._drop_locked(pid)

    def _register_locked(self, process: subprocess.Popen[Any], launch: tuple[str, int, int] | None) -> None:
        pid = int(process.pid)
        try:
            state, parent, ticks = self._proc_stat(pid)
        except (OSError, ValueError):
            return
        if parent != self.owner_pid or state not in {"R", "S", "D", "I", "T", "t", "Z"}:
            return
        try:
            role, device, inode = self._proc_executable(pid)
            identity_source = "PROC_EXE"
        except (OSError, ValueError):
            if state != "Z" or launch is None:
                return
            role, device, inode = launch
            identity_source = "RESOLVED_LAUNCH"
        self._drop_locked(pid)
        self._entries[pid] = {
            "pid": pid,
            "parent_pid": parent,
            "start_ticks": ticks,
            "owner_pid": self.owner_pid,
            "owner_start_ticks": self._owner_start_ticks,
            "role": role,
            "executable_device": device,
            "executable_inode": inode,
            "identity_source": identity_source,
            "registered_post_exec": True,
        }
        open_pidfd = getattr(os, "pidfd_open", None)
        if callable(open_pidfd):
            with suppress(OSError):
                self._pidfds[pid] = int(open_pidfd(pid))
        self._prune_locked()
        if len(self._entries) > MAXIMUM_REGISTERED_CHILDREN:
            self._drop_locked(pid)

    def install(self) -> None:
        with self._lock:
            if self._installed:
                return
            original = subprocess.Popen
            if not isinstance(original, type):
                raise RuntimeError("CHILD_REGISTRY_POPEN_ALREADY_INTERPOSED")
            self._original_popen = original

            def tracked_popen(*popenargs: Any, **kwargs: Any) -> subprocess.Popen[Any]:
                with self._lock:
                    launch = self._resolved_launch_identity(popenargs, kwargs)
                    process = original(*popenargs, **kwargs)
                    self._register_locked(process, launch)
                    return process

            self._wrapper = tracked_popen
            subprocess.Popen = tracked_popen  # type: ignore[misc, assignment]
            self._installed = True

    @contextmanager
    def guard(self) -> Iterator[None]:
        with self._lock:
            yield

    def snapshot_locked(self) -> dict[int, dict[str, Any]]:
        """Return the pre-prune identity set while the caller holds ``guard``.

        The caller enumerates procfs children immediately before this method.
        A registered short-lived child can be reaped between that enumeration
        and pruning.  Keep its prior identity in this one returned snapshot so
        the caller can classify the failed stat as a bounded snapshot race.
        It is still removed from the live registry before this method returns.
        """
        snapshot = {pid: dict(value) for pid, value in self._entries.items()}
        self._prune_locked()
        return snapshot

    def close(self) -> None:
        with self._lock:
            if self._installed and subprocess.Popen is self._wrapper:
                subprocess.Popen = self._original_popen  # type: ignore[misc]
            self._installed = False
            self._wrapper = None
            self._original_popen = None
            for pid in list(self._entries):
                self._drop_locked(pid)
