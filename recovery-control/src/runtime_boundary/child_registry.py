"""Process-scoped, credential-free registry for owner-spawned children.

The registry closes the gap between ``Popen`` returning and procfs sampling.
It never stores argv, environment, file descriptors inherited by the child, or
any stream credential. Unknown launch/exec identity remains unproven.
"""

from __future__ import annotations

import os
import hashlib
import shutil
import stat
import subprocess
import threading
import time
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
            subprocess.Popen = tracked_popen  # type: ignore[assignment]
            self._installed = True

    def spawn_pipe_encoder(self, command: list[str], **kwargs: Any) -> subprocess.Popen[Any]:
        """単一pipe outputのencoderを、観測lock内で明示的に束縛する。"""
        if (not isinstance(command, list) or not command or Path(command[0]).name != "ffmpeg"
            or command[-1] != "pipe:1" or command.count("pipe:1") != 1
            or kwargs.get("stdout") != subprocess.PIPE
            or any(str(arg).startswith(("rtmp:", "rtmps:", "http:", "https:", "tcp:", "udp:", "srt:")) for arg in command)):
            raise ValueError("ENCODER_PIPE_COMMAND_INVALID")
        with self._lock:
            process = subprocess.Popen(command, **kwargs)
            try:
                entry = self._entries.get(process.pid)
                if entry is None or entry["role"] != "DELIVERY_CANDIDATE" or process.stdout is None:
                    raise ValueError("ENCODER_PIPE_POST_EXEC_UNPROVEN")
                raw = b"\0".join(os.fsencode(arg) for arg in command) + b"\0"
                entry.update(role="ENCODER_PIPE", encoder_command_sha256=hashlib.sha256(raw).hexdigest(),
                             encoder_pipe_inode=os.fstat(process.stdout.fileno()).st_ino)
                if not self.pipe_encoder_proven(process.pid, entry):
                    raise ValueError("ENCODER_PIPE_BINDING_UNPROVEN")
                return process
            except BaseException:
                # Popen created this child. Reap it before dropping its pidfd.
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=1)
                self._drop_locked(process.pid)
                raise

    def pipe_encoder_proven(self, pid: int, entry: Mapping[str, Any]) -> bool:
        """argvは保存せずdigestを照合し、pipeとIP socket不在を前後で再読する。"""
        deadline = time.clock_gettime(time.CLOCK_BOOTTIME) + .5
        for _ in range(50):
            try:
                return self._pipe_encoder_proven_once(pid, entry)
            except FileNotFoundError:
                # Library loading can close an FD between listing and readlink.
                # Discard that whole observation; identity checks remain exact.
                if time.clock_gettime(time.CLOCK_BOOTTIME) >= deadline:
                    break
                time.sleep(.01)
        return False

    def _pipe_encoder_proven_once(self, pid: int, entry: Mapping[str, Any]) -> bool:
        if entry.get("role") != "ENCODER_PIPE":
            return False
        root = self.proc_root / str(pid)
        for _ in range(2):
            if self._proc_stat(self.owner_pid)[2] != entry.get("owner_start_ticks"):
                return False
            state, parent, ticks = self._proc_stat(pid)
            if state not in {"R", "S", "D", "I", "T", "t"} or (parent, ticks) != (self.owner_pid, entry.get("start_ticks")):
                return False
            _, dev, ino = self._proc_executable(pid)
            if (dev, ino) != (entry.get("executable_device"), entry.get("executable_inode")):
                return False
            raw = (root / "cmdline").read_bytes()
            if len(raw) > 32768 or hashlib.sha256(raw).hexdigest() != entry.get("encoder_command_sha256"):
                return False
            pipe_meta = (root / "fd/1").stat()
            if not stat.S_ISFIFO(pipe_meta.st_mode) or pipe_meta.st_ino != entry.get("encoder_pipe_inode"):
                return False
            fd_paths = list((root / "fd").iterdir())
            if len(fd_paths) > 256:
                return False
            sockets = set()
            for fd in fd_paths:
                link = os.readlink(fd)
                if link.startswith("socket:["):
                    sockets.add(link[8:-1])
            for table in ("tcp", "tcp6", "udp", "udp6", "raw", "raw6"):
                text = (root / "net" / table).read_text()
                if len(text) > 131072:
                    return False
                for line in text.splitlines()[1:]:
                    fields = line.split()
                    if len(fields) < 10 or fields[9] in sockets:
                        return False
        return True

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
                subprocess.Popen = self._original_popen
            self._installed = False
            self._wrapper = None
            self._original_popen = None
            for pid in list(self._entries):
                self._drop_locked(pid)
