"""Optional owner-cycle evidence export; no daemon, effect or signing path.

The existing runtime owner passes its ledger and process supplier. A separate
short-lived query-only snapshot never changes the writer's connection modes.
Failure leaves the previous export untouched, so consumers see expiry.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import stat
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import closing, contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_dell_recovery.json_input import load_object
from cra_dell_recovery.owner_diagnostics import diagnostic_event, empty_diagnostics
from cra_dell_recovery.recovery_history import validate_policy

from .child_registry import ChildLifecycleRegistry
from .ledger import EffectLedger
from .model import parse_utc, validate_target
from .recovery_evidence import INDEPENDENT_SCHEMA, LIFECYCLE_SCHEMA, SCHEMA, owner_evidence, source_hash

MAXIMUM_BYTES = 64 * 1024
QUERY_SECONDS = 0.5
INTERVAL_SECONDS = 5.0
PROC_SCAN_ATTEMPTS = 3
PROC_SCAN_SECONDS = 0.1
CONFIG_SCHEMA = "runtime.recovery_evidence_config.v1"
LOGGER = logging.getLogger(__name__)


class _ProcSnapshotRace(ValueError):
    """A changing auxiliary process/thread set requires a fresh full scan."""

    def __init__(self, message: str, *, process: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.process = process


class _ProcReadUnproven(ValueError):
    """A read failed with a bounded credential-free process capsule."""

    def __init__(self, message: str, *, process: dict[str, Any]) -> None:
        super().__init__(message)
        self.process = process


class _ClockRegression(ValueError):
    """A clock discontinuity invalidates the whole evidence snapshot."""


def _read_clock(not_before: datetime) -> datetime:
    current = datetime.now(UTC)
    if current < not_before:
        raise _ClockRegression("RECOVERY_OWNER_CLOCK_REGRESSION")
    return current


@contextmanager
def owned_directory(path: Path, *, owner_uid: int = 0) -> Iterator[int]:
    """Walk descriptors: reject symlinks and replaceable parent directories."""
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("RECOVERY_OWNER_PATH_INVALID")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open("/", flags)
    try:
        for index, part in enumerate(path.parts[1:]):
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
            meta = os.fstat(fd)
            final = index == len(path.parts) - 2
            sticky_ancestor = not final and bool(meta.st_mode & stat.S_ISVTX)
            if meta.st_uid not in {0, owner_uid} or (meta.st_mode & 0o022 and not sticky_ancestor):
                raise ValueError("RECOVERY_OWNER_DIRECTORY_UNSAFE")
        if os.fstat(fd).st_uid != owner_uid:
            raise ValueError("RECOVERY_OWNER_DIRECTORY_OWNER_INVALID")
        yield fd
    finally:
        os.close(fd)


def owned_json(path: Path, *, owner_uid: int = 0) -> dict[str, Any]:
    with owned_directory(path.parent, owner_uid=owner_uid) as directory:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory)
        with os.fdopen(fd, "rb") as source:
            meta = os.fstat(source.fileno())
            if not stat.S_ISREG(meta.st_mode) or meta.st_uid != owner_uid or meta.st_mode & 0o022 or meta.st_nlink != 1:
                raise ValueError("RECOVERY_OWNER_SOURCE_UNSAFE")
            raw = source.read(MAXIMUM_BYTES + 1)
    if len(raw) > MAXIMUM_BYTES:
        raise ValueError("RECOVERY_OWNER_SOURCE_TOO_LARGE")

    return load_object(raw, maximum_bytes=MAXIMUM_BYTES)


def write_export(path: Path, value: dict[str, Any], *, owner_uid: int = 0) -> None:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    if len(raw) > MAXIMUM_BYTES:
        raise ValueError("RECOVERY_OWNER_EXPORT_TOO_LARGE")
    with owned_directory(path.parent, owner_uid=owner_uid) as directory:
        try:
            meta = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            meta = None
        if meta is not None and (not stat.S_ISREG(meta.st_mode) or meta.st_uid != owner_uid or meta.st_mode & 0o022 or meta.st_nlink != 1):
            raise ValueError("RECOVERY_OWNER_OUTPUT_UNSAFE")
        name = f".{path.name}.{uuid.uuid4().hex}.tmp"
        try:
            fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o644, dir_fd=directory)
            with os.fdopen(fd, "wb") as output:
                os.fchmod(output.fileno(), 0o644)
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
            os.replace(name, path.name, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(name, dir_fd=directory)


def _managed_command(command: bytes) -> bool:
    args = command.split(b"\0")
    return args[0].rsplit(b"/", 1)[-1] == b"ffmpeg" and any(a.startswith((b"rtmp://", b"rtmps://")) for a in args)


class RecoveryEvidencePublisher:
    def __init__(
        self,
        *,
        ledger: EffectLedger,
        config: dict[str, Any],
        target_path: Path,
        process_supplier: Callable[[], Mapping[str, Any]],
        child_registry: ChildLifecycleRegistry | None = None,
        proc_root: Path = Path("/proc"),
        owner_uid: int = 0,
    ) -> None:
        expected = {
            "schema",
            "host_id",
            "target_host_id",
            "release_id",
            "source_commit",
            "stream_id",
            "allowed_producers",
            "output_file",
            "effect_history_policy",
        }
        strings = expected - {"allowed_producers", "effect_history_policy"}
        target_owner_uid = config.get("target_snapshot_owner_uid", owner_uid)
        if (
            not expected <= set(config) <= expected | {"target_snapshot_owner_uid", "evidence_schema"}
            or config.get("evidence_schema", SCHEMA) not in (SCHEMA, INDEPENDENT_SCHEMA, LIFECYCLE_SCHEMA)
            or any(not isinstance(config.get(k), str) or not config[k] for k in strings)
            or config["schema"] != CONFIG_SCHEMA
            or re.fullmatch(r"[0-9a-f]{40}", config["source_commit"]) is None
            or not isinstance(config["allowed_producers"], list)
            or not config["allowed_producers"]
            or any(not isinstance(p, str) or not p for p in config["allowed_producers"])
            or type(target_owner_uid) is not int
            or target_owner_uid < 0
        ):
            raise ValueError("RECOVERY_OWNER_CONFIG_INVALID")
        self.config = dict(config)
        self.config["effect_history_policy"] = validate_policy(config["effect_history_policy"])
        self.path = Path(config["output_file"])
        if (
            not self.path.is_absolute()
            or not target_path.is_absolute()
            or self.path.resolve() in {ledger.path.resolve(), target_path.resolve()}
            or self.path.parent.resolve() in {ledger.path.parent.resolve(), target_path.parent.resolve()}
        ):
            raise ValueError("RECOVERY_OWNER_PATH_COLLISION_OR_RELATIVE")
        self.ledger = ledger
        self.target_path = target_path
        self.process_supplier = process_supplier
        self.child_registry = child_registry
        self.proc_root = proc_root
        self.owner_uid = owner_uid
        self.target_owner_uid = target_owner_uid
        self._next_cycle = 0.0
        self.last_error_code: str | None = None
        # Identity only. Activation and child liveness are read afresh on every
        # export. This anchor cannot survive an owner process or boot change.
        self._child_anchor: tuple[dict[str, Any], tuple[int, str, int], int] | None = None
        self._anchor_invalid = False
        self._read_diagnostics = empty_diagnostics()
        self._read_stage = "owner_stat"
        self._last_error_log_at = float("-inf")
        self._last_logged_error: str | None = None

    def _proc_stat(self, pid: int) -> tuple[str, int, int]:
        raw = (self.proc_root / str(pid) / "stat").read_text()
        fields = raw[raw.rfind(")") + 2 :].split()
        if not raw.startswith(f"{pid} (") or len(fields) < 20 or int(fields[19]) <= 0:
            raise ValueError("RECOVERY_OWNER_PROC_STAT_INVALID")
        return fields[0], int(fields[1]), int(fields[19])

    def _record_read_failure(self, error: BaseException) -> None:
        events = self._read_diagnostics["events"]
        if len(events) >= 16:
            raise ValueError("RECOVERY_OWNER_DIAGNOSTIC_LIMIT")
        process = getattr(error, "process", None)
        events.append(diagnostic_event(self._read_stage, error, process=process if isinstance(process, dict) else None))

    def _owner_state_matches_anchor(self) -> bool:
        anchor = self._child_anchor
        if anchor is None:
            return False
        try:
            process = dict(self.process_supplier())
        except (OSError, ValueError, KeyError, TypeError):
            return False
        return (
            process.get("local_ffmpeg_pid") == anchor[1][0]
            and process.get("ffmpeg_running") is True
            and process.get("managed_child_cardinality") == "1"
            and process.get("managed_child_pids") == [anchor[1][0]]
            and process.get("pod_uid") == anchor[0]["pod_uid"]
        )

    def _process_capsule(
        self,
        *,
        pid: int,
        state: str,
        parent: int,
        ticks: int,
        executable_relation: str,
        owner_state_relation: str,
    ) -> dict[str, Any]:
        anchor = self._child_anchor
        return {
            "pid": pid,
            "state": state,
            "parent_pid": parent,
            "start_ticks": ticks,
            "command_bytes": 0,
            "anchor_relation": ("UNPROVEN" if anchor is None else "MANAGED" if pid == anchor[1][0] else "AUXILIARY"),
            "executable_relation": executable_relation,
            "anchor_target_sha256": source_hash(anchor[0]) if anchor is not None else None,
            "owner_state_relation": owner_state_relation,
        }

    def _executable_identity(self, pid: int) -> tuple[int, int]:
        meta = (self.proc_root / str(pid) / "exe").stat()
        if not stat.S_ISREG(meta.st_mode):
            raise ValueError("RECOVERY_OWNER_CHILD_EXECUTABLE_INVALID")
        return meta.st_dev, meta.st_ino

    def _retry_anchor(self) -> tuple[int, int, int] | None:
        """Only a continuously re-proven owner and exact live child may retry."""
        anchor = self._child_anchor
        if anchor is None or self._anchor_invalid:
            return None
        self._read_stage = "retry_anchor"
        state, _, owner_ticks = self._proc_stat(os.getpid())
        child_state, parent, ticks = self._proc_stat(anchor[1][0])
        if (
            state not in {"R", "S", "D", "I"}
            or owner_ticks != anchor[2]
            or child_state not in {"R", "S", "D", "I"}
            or parent != os.getpid()
            or ticks != anchor[1][2]
            or (self.proc_root / "sys/kernel/random/boot_id").read_text().strip() != anchor[0]["host_boot_id"]
            or not self._owner_state_matches_anchor()
        ):
            raise ValueError("RECOVERY_OWNER_RETRY_ANCHOR_CHANGED")
        return owner_ticks, anchor[1][0], ticks

    def _strict_children(self) -> list[int]:
        """Retry only proven auxiliary churn, never an unreadable managed child."""
        until = time.monotonic() + PROC_SCAN_SECONDS
        try:
            anchor = self._retry_anchor()
        except (OSError, ValueError):
            # Stable absence still needs the full scan and owner absence proof.
            # Without a live anchor, no interrupted scan may be retried.
            anchor = None
        for attempt in range(PROC_SCAN_ATTEMPTS):
            self._read_diagnostics["proc_scan_attempt_count"] += 1
            try:
                return self._scan_children()
            except _ProcSnapshotRace as error:
                self._record_read_failure(error)
                if anchor is None or self._retry_anchor() != anchor:
                    raise ValueError("RECOVERY_OWNER_RETRY_ANCHOR_CHANGED") from error
                if attempt + 1 == PROC_SCAN_ATTEMPTS:
                    raise ValueError("RECOVERY_OWNER_PROC_RETRY_EXHAUSTED") from error
                if time.monotonic() >= until:
                    raise ValueError("RECOVERY_OWNER_PROC_RETRY_DEADLINE") from error
                self._read_diagnostics["proc_scan_retry_count"] += 1
        raise AssertionError("unreachable")

    def _strict_registered_children(self, process: Mapping[str, Any]) -> list[int]:
        until = time.monotonic() + PROC_SCAN_SECONDS
        for attempt in range(PROC_SCAN_ATTEMPTS):
            self._read_diagnostics["proc_scan_attempt_count"] += 1
            try:
                return self._scan_registered_children(process)
            except _ProcSnapshotRace as error:
                self._record_read_failure(error)
                if attempt + 1 == PROC_SCAN_ATTEMPTS:
                    raise ValueError("RECOVERY_OWNER_PROC_RETRY_EXHAUSTED") from error
                if time.monotonic() >= until:
                    raise ValueError("RECOVERY_OWNER_PROC_RETRY_DEADLINE") from error
                self._read_diagnostics["proc_scan_retry_count"] += 1
        raise AssertionError("unreachable")

    def _scan_registered_children(self, process: Mapping[str, Any]) -> list[int]:
        """Classify only spawn-registered identities while creation is locked."""
        if self.child_registry is None:
            raise ValueError("RECOVERY_OWNER_CHILD_REGISTRY_UNAVAILABLE")
        tasks = self.proc_root / str(os.getpid()) / "task"
        self._read_stage = "task_list"
        before = sorted(path.name for path in tasks.iterdir())
        if not before or len(before) > 256 or any(not thread.isdecimal() for thread in before):
            raise ValueError("RECOVERY_OWNER_TASK_SET_INVALID")

        def children() -> set[int]:
            result: set[int] = set()
            for thread in before:
                self._read_stage = "thread_children"
                try:
                    raw = (tasks / thread / "children").read_text()
                except FileNotFoundError as error:
                    if not (tasks / thread).exists():
                        raise _ProcSnapshotRace("RECOVERY_OWNER_TASK_RETIRED") from error
                    raise
                if len(raw) > 4096:
                    raise ValueError("RECOVERY_OWNER_CHILD_SET_TOO_LARGE")
                result.update(int(value) for value in raw.split())
            if len(result) > 256 or any(pid <= 1 for pid in result):
                raise ValueError("RECOVERY_OWNER_CHILD_SET_INVALID")
            return result

        pids = children()
        entries = self.child_registry.snapshot_locked()
        managed_pid = process.get("local_ffmpeg_pid") if process.get("ffmpeg_running") is True else 0
        managed_executable = None
        if managed_pid:
            self._read_stage = "child_identity"
            managed_executable = self._executable_identity(managed_pid)
        managed: list[int] = []
        for pid in sorted(pids):
            self._read_stage = "child_registry"
            entry = entries.get(pid)
            try:
                state, parent, ticks = self._proc_stat(pid)
            except (FileNotFoundError, ProcessLookupError) as error:
                if entry is not None:
                    raise _ProcSnapshotRace("RECOVERY_OWNER_REGISTERED_CHILD_DISAPPEARED") from error
                raise
            capsule = self._process_capsule(
                pid=pid,
                state=state,
                parent=parent,
                ticks=ticks,
                executable_relation="UNAVAILABLE",
                owner_state_relation="MATCHED" if entry is not None else "UNPROVEN",
            )
            if entry is None:
                raise _ProcReadUnproven("RECOVERY_OWNER_CHILD_UNREGISTERED", process=capsule)
            if (
                entry.get("pid") != pid
                or entry.get("parent_pid") != parent
                or entry.get("start_ticks") != ticks
                or entry.get("owner_pid") != os.getpid()
                or entry.get("owner_start_ticks") != self._proc_stat(os.getpid())[2]
                or entry.get("registered_post_exec") is not True
            ):
                raise _ProcReadUnproven("RECOVERY_OWNER_CHILD_REGISTRY_IDENTITY_INVALID", process=capsule)
            role = entry.get("role")
            if role not in {"AUXILIARY", "DELIVERY_CANDIDATE"}:
                raise _ProcReadUnproven("RECOVERY_OWNER_CHILD_REGISTRY_IDENTITY_INVALID", process=capsule)
            if role == "DELIVERY_CANDIDATE":
                managed.append(pid)
                if pid != managed_pid and process.get("runtime_lifecycle_state") != "STARTING_FFMPEG":
                    raise _ProcReadUnproven("RECOVERY_OWNER_DELIVERY_CHILD_UNBOUND", process=capsule)
                continue
            if state == "Z":
                continue
            try:
                executable = self._executable_identity(pid)
            except (OSError, ValueError) as error:
                raise _ProcReadUnproven("RECOVERY_OWNER_AUXILIARY_EXECUTABLE_UNAVAILABLE", process=capsule) from error
            try:
                stable_state, stable_parent, stable_ticks = self._proc_stat(pid)
                stable_executable = self._executable_identity(pid)
            except (OSError, ValueError) as error:
                raise _ProcSnapshotRace("RECOVERY_OWNER_AUXILIARY_EXECUTABLE_CHANGED") from error
            if (stable_parent, stable_ticks) != (parent, ticks):
                raise _ProcReadUnproven(
                    "RECOVERY_OWNER_CHILD_REGISTRY_IDENTITY_INVALID",
                    process=self._process_capsule(
                        pid=pid,
                        state=stable_state,
                        parent=stable_parent,
                        ticks=stable_ticks,
                        executable_relation="UNAVAILABLE",
                        owner_state_relation="UNPROVEN",
                    ),
                )
            if stable_state == "Z":
                raise _ProcSnapshotRace(
                    "RECOVERY_OWNER_AUXILIARY_EXITED",
                    process=self._process_capsule(
                        pid=pid,
                        state=stable_state,
                        parent=stable_parent,
                        ticks=stable_ticks,
                        executable_relation="UNAVAILABLE",
                        owner_state_relation="MATCHED",
                    ),
                )
            if state not in {"R", "S", "D", "I", "T", "t"} or stable_state not in {
                "R",
                "S",
                "D",
                "I",
                "T",
                "t",
            }:
                raise _ProcReadUnproven(
                    "RECOVERY_OWNER_CHILD_STATE_INVALID",
                    process=self._process_capsule(
                        pid=pid,
                        state=stable_state,
                        parent=stable_parent,
                        ticks=stable_ticks,
                        executable_relation="UNAVAILABLE",
                        owner_state_relation="MATCHED",
                    ),
                )
            if stable_executable != executable:
                raise _ProcSnapshotRace("RECOVERY_OWNER_AUXILIARY_EXECUTABLE_CHANGED")
            # R/S/D/I/T/t are scheduler states, not process identity. A helper can
            # move between them during two adjacent procfs reads while its
            # parent, start ticks and executable remain exact.  Treat only the
            # identity fields above as drift; otherwise routine helper work can
            # incorrectly erase a simultaneously proven managed FFmpeg.
            if managed_executable is not None and stable_executable == managed_executable:
                capsule["executable_relation"] = "SAME"
                raise _ProcReadUnproven("RECOVERY_OWNER_AUXILIARY_MATCHES_DELIVERY_EXECUTABLE", process=capsule)

        self._read_stage = "task_list_final"
        if sorted(path.name for path in tasks.iterdir()) != before:
            raise _ProcSnapshotRace("RECOVERY_OWNER_TASK_SET_CHANGED")
        self._read_stage = "child_set_final"
        if children() != pids:
            raise _ProcSnapshotRace("RECOVERY_OWNER_CHILD_SET_CHANGED")
        return managed

    def _empty_child_command(self, pid: int, parent: int, ticks: int) -> None:
        """An empty read is retryable only after proving this auxiliary exited."""
        stage = self._read_stage
        self._read_stage = "child_stat_final"
        final_state, final_parent, final_ticks = self._proc_stat(pid)
        self._read_stage = stage
        anchor = self._child_anchor
        owner_state_matched = self._owner_state_matches_anchor()
        capsule = self._process_capsule(
            pid=pid,
            state=final_state,
            parent=final_parent,
            ticks=final_ticks,
            executable_relation="UNAVAILABLE",
            owner_state_relation="MATCHED" if owner_state_matched else "UNPROVEN",
        )
        if (
            final_state == "Z"
            and (final_parent, final_ticks) == (parent, ticks)
            and anchor is not None
            and pid != anchor[1][0]
            and owner_state_matched
        ):
            # A zombie retains procfs entries but has no command bytes. The
            # outer retry loop must still re-prove the owner and managed child.
            raise _ProcSnapshotRace("RECOVERY_OWNER_AUXILIARY_EXITED", process=capsule)
        if (
            final_state in {"R", "S", "D", "I"}
            and (final_parent, final_ticks) == (parent, ticks)
            and anchor is not None
            and pid != anchor[1][0]
            and owner_state_matched
        ):
            try:
                executable = self._executable_identity(pid)
                anchor_executable = self._executable_identity(anchor[1][0])
                stable_state, stable_parent, stable_ticks = self._proc_stat(pid)
                stable_executable = self._executable_identity(pid)
            except (OSError, ValueError):
                pass
            else:
                executable_relation = "SAME" if executable == anchor_executable else "DIFFERENT"
                capsule = self._process_capsule(
                    pid=pid,
                    state=stable_state,
                    parent=stable_parent,
                    ticks=stable_ticks,
                    executable_relation=executable_relation,
                    owner_state_relation="MATCHED",
                )
                if (
                    (stable_state, stable_parent, stable_ticks) == (final_state, final_parent, final_ticks)
                    and stable_executable == executable
                    and executable_relation == "DIFFERENT"
                ):
                    # The stable executable is distinct from the anchored
                    # delivery child. Re-scan from the owner/child anchor so a
                    # later exec to FFmpeg or a second managed child is still
                    # detected instead of being inferred away.
                    raise _ProcSnapshotRace("RECOVERY_OWNER_AUXILIARY_COMMAND_UNAVAILABLE", process=capsule)
        raise _ProcReadUnproven("RECOVERY_OWNER_CHILD_COMMAND_INVALID", process=capsule)

    def _scan_children(self) -> list[int]:
        """Unreadable children/threads are UNKNOWN, never proof of absence."""
        tasks = self.proc_root / str(os.getpid()) / "task"
        self._read_stage = "task_list"
        before = sorted(p.name for p in tasks.iterdir())
        if not before or len(before) > 256 or any(not p.isdecimal() for p in before):
            raise ValueError("RECOVERY_OWNER_TASK_SET_INVALID")

        def children() -> set[int]:
            result: set[int] = set()
            for thread in before:
                self._read_stage = "thread_children"
                try:
                    raw = (tasks / thread / "children").read_text()
                except FileNotFoundError as error:
                    # A missing file in a still-present task is an observation
                    # failure. Only a retired thread can be retried.
                    if not (tasks / thread).exists():
                        raise _ProcSnapshotRace("RECOVERY_OWNER_TASK_RETIRED") from error
                    raise
                if len(raw) > 4096:
                    raise ValueError("RECOVERY_OWNER_CHILD_SET_TOO_LARGE")
                result.update(int(p) for p in raw.split())
            if len(result) > 256 or any(p <= 1 for p in result):
                raise ValueError("RECOVERY_OWNER_CHILD_SET_INVALID")
            return result

        pids = children()
        managed = []
        for pid in sorted(pids):
            is_managed = False
            self._read_stage = "child_stat"
            try:
                state, parent, ticks = self._proc_stat(pid)
                if parent != os.getpid():
                    raise ValueError("RECOVERY_OWNER_CHILD_PARENT_CHANGED")
                self._read_stage = "child_command"
                with (self.proc_root / str(pid) / "cmdline").open("rb") as source:
                    command = source.read(128 * 1024 + 1)
                if len(command) > 128 * 1024:
                    raise ValueError("RECOVERY_OWNER_CHILD_COMMAND_INVALID")
                if not command and state != "Z":
                    self._empty_child_command(pid, parent, ticks)
                is_managed = _managed_command(command)
                if is_managed:
                    if state in {"R", "S", "D", "I"}:
                        managed.append(pid)
                    elif state != "Z":
                        raise ValueError("RECOVERY_OWNER_CHILD_STATE_INVALID")
                self._read_stage = "child_stat_final"
                final_state, final_parent, final_ticks = self._proc_stat(pid)
                if (final_parent, final_ticks) != (parent, ticks) or (final_state == "Z") != (state == "Z"):
                    error_type = ValueError if is_managed else _ProcSnapshotRace
                    raise error_type("RECOVERY_OWNER_CHILD_CHANGED_DURING_READ")
                self._read_stage = "child_command_final"
                with (self.proc_root / str(pid) / "cmdline").open("rb") as source:
                    final_command = source.read(128 * 1024 + 1)
                if len(final_command) > 128 * 1024:
                    raise ValueError("RECOVERY_OWNER_CHILD_COMMAND_INVALID")
                if not final_command and final_state != "Z":
                    self._empty_child_command(pid, final_parent, final_ticks)
                if final_command != command:
                    error_type = ValueError if is_managed or _managed_command(final_command) else _ProcSnapshotRace
                    raise error_type("RECOVERY_OWNER_CHILD_COMMAND_CHANGED")
            except (FileNotFoundError, ProcessLookupError) as error:
                # procfs may open successfully and return ESRCH on read after
                # the process exits. Preserve the errno before revalidating.
                self._record_read_failure(error)
                if is_managed:
                    raise ValueError("RECOVERY_OWNER_MANAGED_CHILD_DISAPPEARED") from error
                if self._child_anchor is not None and pid != self._child_anchor[1][0]:
                    raise _ProcSnapshotRace("RECOVERY_OWNER_AUXILIARY_DISAPPEARED") from error
                raise
        self._read_stage = "task_list_final"
        if sorted(p.name for p in tasks.iterdir()) != before:
            raise _ProcSnapshotRace("RECOVERY_OWNER_TASK_SET_CHANGED")
        self._read_stage = "child_set_final"
        if children() != pids:
            raise _ProcSnapshotRace("RECOVERY_OWNER_CHILD_SET_CHANGED")
        return managed

    def _lifecycle_context(self, now: datetime) -> tuple[dict[str, Any] | None, tuple[int, str, int] | None, dict[str, Any]]:
        proof: dict[str, Any] = {
            "schema": "runtime.child_lifecycle.v2",
            "state": "UNKNOWN",
            "reason": "READ_UNAVAILABLE",
            "observed_at": now.isoformat(),
            "anchor_target": None,
            "owner_pid": None,
            "owner_start_ticks": None,
            "child_pid": None,
            "child_start_ticks": None,
        }
        events_before = len(self._read_diagnostics["events"])
        if self._anchor_invalid:
            self._read_stage = "anchor_runtime"
            self._record_read_failure(ValueError("RECOVERY_OWNER_ANCHOR_INVALID"))
            return None, None, proof
        try:
            self._read_stage = "owner_stat"
            owner_state, _, owner_ticks = self._proc_stat(os.getpid())
            if owner_state not in {"R", "S", "D", "I"}:
                raise ValueError("RECOVERY_OWNER_NOT_LIVE")
            self._read_stage = "process_supplier"
            if self.child_registry is None:
                process = dict(self.process_supplier())
                children = self._strict_children()
            else:
                with self.child_registry.guard():
                    process = dict(self.process_supplier())
                    children = self._strict_registered_children(process)
            self._read_stage = "anchor_runtime"
            anchor = self._child_anchor
            if anchor is not None and (
                anchor[2] != owner_ticks
                or process.get("pod_uid") != anchor[0]["pod_uid"]
                or (self.proc_root / "sys/kernel/random/boot_id").read_text().strip() != anchor[0]["host_boot_id"]
            ):
                self._child_anchor = None
                self._anchor_invalid = True
                raise ValueError("RECOVERY_OWNER_ANCHOR_RUNTIME_CHANGED")
            self._read_stage = "child_identity"
            if children and (len(children) != 1 or process.get("managed_child_pids") != children):
                raise ValueError("RECOVERY_OWNER_CHILD_CARDINALITY_INVALID")
            proof.update(owner_pid=os.getpid(), owner_start_ticks=owner_ticks)
            if children:
                pid = children[0]
                state, parent, ticks = self._proc_stat(pid)
                if (
                    process.get("local_ffmpeg_pid") == 0
                    and process.get("ffmpeg_running") is False
                    and process.get("runtime_lifecycle_state") == "STARTING_FFMPEG"
                    and process.get("managed_child_pids") == children
                    and anchor is not None
                ):
                    proof.update(
                        state="TRANSITION",
                        reason="CHILD_CHANGED",
                        child_pid=pid,
                        child_start_ticks=ticks,
                        anchor_target=dict(anchor[0]),
                    )
                    proof["observed_at"] = _read_clock(now).isoformat()
                    return None, None, proof
                if process.get("local_ffmpeg_pid") != pid or process.get("ffmpeg_running") is not True or parent != os.getpid():
                    raise ValueError("RECOVERY_OWNER_CHILD_IDENTITY_INVALID")
                proof.update(child_pid=pid, child_start_ticks=ticks)
                # The host namespace identity is admitted once, then each claim
                # re-proves the same live direct child and owner incarnation.
                # No previous activation value or old observation time is used.
                if anchor is not None and (pid, ticks) == (anchor[1][0], anchor[1][2]):
                    target = dict(anchor[0])
                else:
                    try:
                        self._read_stage = "target_snapshot"
                        target = self._target(now)
                    except _ClockRegression:
                        raise
                    except (OSError, ValueError) as error:
                        self._record_read_failure(error)
                        target = None
                if target is not None:
                    try:
                        self._read_stage = "process_identity"
                        exact = self._process(target)
                    except ValueError as error:
                        self._record_read_failure(error)
                        exact = None
                    if exact is not None and (exact[0], exact[2]) == (pid, ticks):
                        self._child_anchor = (dict(target), exact, owner_ticks)
                        proof.update(state="RUNNING", reason="EXACT_CHILD", anchor_target=dict(target))
                        proof["observed_at"] = _read_clock(now).isoformat()
                        return target, exact, proof
                if anchor is not None and (pid, ticks) != (anchor[1][0], anchor[1][2]):
                    proof.update(state="TRANSITION", reason="CHILD_CHANGED", anchor_target=dict(anchor[0]))
                else:
                    proof.update(child_pid=None, child_start_ticks=None)
            elif (
                anchor is not None
                and process.get("ffmpeg_running") is False
                and process.get("local_ffmpeg_pid") == 0
                and process.get("managed_child_pids") == []
                and process.get("managed_child_cardinality") == "0"
                and process.get("runtime_lifecycle_state") in {"FFMPEG_EXITED", "RESTART_DELAY", "CONNECTIVITY_WAIT", "STARTING_FFMPEG"}
            ):
                self._read_stage = "absence_check"
                try:
                    old_state, old_parent, old_ticks = self._proc_stat(anchor[1][0])
                except FileNotFoundError:
                    pass
                else:
                    if old_state != "Z" or old_parent != os.getpid() or old_ticks != anchor[1][2]:
                        raise ValueError("RECOVERY_OWNER_FALSE_CHILD_ABSENCE")
                proof.update(state="ABSENT", reason="NO_MANAGED_CHILD", anchor_target=dict(anchor[0]))
        except _ClockRegression:
            raise
        except (OSError, ValueError, KeyError, TypeError) as error:
            self._record_read_failure(error)
            proof.update(state="UNKNOWN", reason="READ_UNAVAILABLE", anchor_target=None, child_pid=None, child_start_ticks=None)
        if proof["state"] == "UNKNOWN" and len(self._read_diagnostics["events"]) == events_before:
            self._read_stage = "child_identity"
            self._record_read_failure(
                ValueError("RECOVERY_OWNER_CHILD_NOT_ANCHORED" if self._child_anchor is None else "RECOVERY_OWNER_CHILD_STATE_UNPROVEN")
            )
        proof["observed_at"] = _read_clock(now).isoformat()
        return None, None, proof

    def _target(self, now: datetime) -> dict[str, Any]:
        # The target publisher is a separate, explicitly admitted owner.
        # Never infer trust from the file's current uid or chown its source.
        raw = owned_json(self.target_path, owner_uid=self.target_owner_uid)
        # An atomic producer refresh may occur after read-start. Validate
        # against read-completion, without granting future-clock tolerance.
        now = _read_clock(now)
        if (
            raw.get("schema") != "cra_dell_recovery.target_snapshot.v1"
            or raw.get("status") != "VALID"
            or not 0 <= (now - parse_utc(raw.get("observed_at"))).total_seconds() <= 45
            or parse_utc(raw.get("valid_until")) <= now
        ):
            raise ValueError("RECOVERY_OWNER_TARGET_STALE_OR_INVALID")
        target = validate_target(raw.get("target_identity"))
        boot = (self.proc_root / "sys/kernel/random/boot_id").read_text().strip()
        if target["host_id"] != self.config["target_host_id"] or target["host_boot_id"] != boot:
            raise ValueError("RECOVERY_OWNER_HOST_OR_BOOT_MISMATCH")
        return target

    def _process(self, target: dict[str, Any]) -> tuple[int, str, int]:
        process = dict(self.process_supplier())
        pid = process.get("local_ffmpeg_pid")
        if (
            type(pid) is not int
            or pid <= 1
            or process.get("ffmpeg_running") is not True
            or process.get("managed_child_cardinality") != "1"
            or process.get("managed_child_pids") != [pid]
            or process.get("pod_uid") != target["pod_uid"]
            or not isinstance(process.get("ffmpeg_generation"), str)
            or not process["ffmpeg_generation"]
        ):
            raise ValueError("RECOVERY_OWNER_PROCESS_IDENTITY_INVALID")
        raw = (self.proc_root / str(pid) / "stat").read_text()
        fields = raw[raw.rfind(")") + 2 :].split()
        if (
            not raw.startswith(f"{pid} (")
            or len(fields) < 20
            or fields[0] not in {"R", "S", "D", "I"}
            or int(fields[1]) != os.getpid()
            or int(fields[19]) <= 0
        ):
            raise ValueError("RECOVERY_OWNER_NOT_LIVE_DIRECT_CHILD")
        ticks = int(fields[19])
        expected = hashlib.sha256(f"{target['pod_uid']}:{target['container_id']}:{target['ffmpeg_pid']}:{ticks}".encode()).hexdigest()[:32]
        if target["ffmpeg_generation"] != f"ffmpeg-{expected}":
            raise ValueError("RECOVERY_OWNER_PID_NAMESPACE_OR_GENERATION_MISMATCH")
        return pid, process["ffmpeg_generation"], ticks

    def publish(self) -> dict[str, Any]:
        started = time.monotonic()
        self._read_diagnostics = empty_diagnostics()
        now = datetime.now(UTC)
        lifecycle_enabled = self.config.get("evidence_schema") == LIFECYCLE_SCHEMA
        independent = self.config.get("evidence_schema") in (INDEPENDENT_SCHEMA, LIFECYCLE_SCHEMA)
        self._read_stage = "boot_snapshot"
        boot = (self.proc_root / "sys/kernel/random/boot_id").read_text().strip()
        if not boot:
            raise ValueError("RECOVERY_OWNER_PHYSICAL_BOOT_MISSING")
        lifecycle = None
        if lifecycle_enabled:
            target, process, lifecycle = self._lifecycle_context(now)
        else:
            target, process = self._target_context(now, independent=independent)
        now = _read_clock(now)
        self._read_stage = "ledger_snapshot"
        if self.ledger.path.is_symlink() or not self.ledger.path.is_file():
            raise ValueError("RECOVERY_OWNER_LEDGER_INVALID")
        # The owner supplies the existing ledger. No effect connection, DDL,
        # writer lock, checkpoint or retry is used for this evidence snapshot.
        uri = self.ledger.path.as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=0.05, isolation_level=None)) as db:
            db.execute("PRAGMA query_only=ON")
            db.set_progress_handler(lambda: int(time.monotonic() - started >= QUERY_SECONDS), 500)
            db.execute("BEGIN")
            value = owner_evidence(
                db,
                target_identity=target,
                release_id=self.config["release_id"],
                source_commit=self.config["source_commit"],
                host_id=self.config["host_id"],
                stream_id=self.config["stream_id"],
                allowed_producers=self.config["allowed_producers"],
                now=now,
                local_pid=process[0] if process is not None else None,
                proc_root=self.proc_root,
                history_policy=self.config["effect_history_policy"],
                independent_effects=independent,
                host_boot_id=boot,
            )
            db.rollback()
        if (self.proc_root / "sys/kernel/random/boot_id").read_text().strip() != boot:
            raise ValueError("RECOVERY_OWNER_PHYSICAL_BOOT_CHANGED")
        checked_at = _read_clock(now)
        final_lifecycle = None
        if lifecycle_enabled:
            final_target, final_process, final_lifecycle = self._lifecycle_context(checked_at)
            final_context = (final_target, final_process)
        else:
            final_context = self._target_context(checked_at, independent=independent)
        _read_clock(checked_at)
        if final_context == (target, process) and lifecycle_enabled:
            lifecycle = final_lifecycle
        if final_context != (target, process):
            if lifecycle_enabled:
                self._read_stage = "snapshot_consistency"
                self._record_read_failure(ValueError("RECOVERY_OWNER_CONTEXT_CHANGED"))
            if not independent:
                raise ValueError("RECOVERY_OWNER_TARGET_CHANGED_DURING_EXPORT")
            # The consistent ledger snapshot remains valid. Target-specific
            # claims do not survive a process transition during that snapshot.
            value["target_sha256"] = None
            value["effects"].update(target_status="UNKNOWN", current_target_sha256=None, current_target_unresolved_scope_count=None)
            value["activation"] = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
            target = None
            if lifecycle_enabled and final_lifecycle is not None:
                prior_lifecycle = lifecycle
                lifecycle = final_lifecycle
                if lifecycle["state"] == "RUNNING":
                    if prior_lifecycle is not None and prior_lifecycle["state"] in {"RUNNING", "ABSENT", "TRANSITION"}:
                        lifecycle = {**lifecycle, "state": "TRANSITION", "reason": "CHILD_CHANGED"}
                    else:
                        lifecycle = {
                            **lifecycle,
                            "state": "UNKNOWN",
                            "reason": "READ_UNAVAILABLE",
                            "anchor_target": None,
                            "child_pid": None,
                            "child_start_ticks": None,
                        }
        if lifecycle_enabled:
            assert lifecycle is not None
            if final_lifecycle is not None and final_lifecycle["state"] == "UNKNOWN":
                lifecycle = final_lifecycle
                target = None
                value["target_sha256"] = None
                value["effects"].update(target_status="UNKNOWN", current_target_sha256=None, current_target_unresolved_scope_count=None)
                value["activation"] = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
            if target is not None and any(v is None for v in value["activation"].values()):
                self._read_stage = "activation"
                self._record_read_failure(ValueError("RECOVERY_OWNER_ACTIVATION_UNAVAILABLE"))
            lifecycle["read_diagnostics"] = self._read_diagnostics
            value.update(schema=LIFECYCLE_SCHEMA, lifecycle=lifecycle, target_identity=target)
        self._read_stage = "snapshot_deadline"
        if time.monotonic() - started >= QUERY_SECONDS:
            raise ValueError("RECOVERY_OWNER_SNAPSHOT_DEADLINE")
        self._read_stage = "export_write"
        write_export(self.path, value, owner_uid=self.owner_uid)
        return value

    def _target_context(self, now: datetime, *, independent: bool) -> tuple[dict[str, Any] | None, tuple[int, str, int] | None]:
        try:
            target = self._target(now)
            return target, self._process(target)
        except _ClockRegression:
            raise
        except (OSError, ValueError):
            if not independent:
                raise
            return None, None

    def publish_if_due(self) -> None:
        current = time.monotonic()
        if current < self._next_cycle:
            return
        self._next_cycle = current + INTERVAL_SECONDS
        try:
            value = self.publish()
        except (OSError, ValueError, sqlite3.Error) as error:
            # An export failure cannot write its own diagnostic to that export.
            # Emit only a bounded allowlisted code through the owner's logger.
            self.last_error_code = "RECOVERY_OWNER_EXPORT_UNAVAILABLE"
            if len(self._read_diagnostics["events"]) < 16:
                self._record_read_failure(ValueError("SQL_READ_FAILED") if isinstance(error, sqlite3.Error) else error)
        else:
            self.last_error_code = "RECOVERY_OWNER_LIFECYCLE_UNKNOWN" if value.get("lifecycle", {}).get("state") == "UNKNOWN" else None
        if self.last_error_code is not None and (
            self.last_error_code != self._last_logged_error or current - self._last_error_log_at >= 60
        ):
            LOGGER.warning("%s %s", self.last_error_code, json.dumps(self._read_diagnostics, sort_keys=True))
            self._last_error_log_at = current
        self._last_logged_error = self.last_error_code
