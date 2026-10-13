from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc
from cra_dell_recovery.transport_resilience import FailureClassification

LEGACY_STATE_SCHEMA = "cra.transport_recovery_state.v1"
STATE_SCHEMA = "cra.transport_recovery_state.v2"
EVENT_SCHEMA = "cra.transport_recovery_event.v1"
ZERO_HASH = "0" * 64
# The journal may outlive one seven-day qualification. Recovery work is
# bounded to the committed byte watermark plus a small suffix, so the total
# file size is no longer also (incorrectly) used as the replay budget.
MAXIMUM_EVENT_JOURNAL_BYTES = 1024 * 1024 * 1024
MAXIMUM_EVENT_REPLAY_BYTES = 64 * 1024 * 1024
MAXIMUM_EVENT_LINE_BYTES = 1024 * 1024


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("RECOVERY_HEALTH_TIME_NAIVE")
    return parsed.astimezone(UTC)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_state(path: Path) -> dict[str, Any] | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ValueError("RECOVERY_HEALTH_STATE_FILE_UNSAFE")
        if metadata.st_size > 1024 * 1024:
            raise ValueError("RECOVERY_HEALTH_STATE_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            value = json.load(handle)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not isinstance(value, dict) or value.get("schema") not in {LEGACY_STATE_SCHEMA, STATE_SCHEMA}:
        raise ValueError("RECOVERY_HEALTH_STATE_INVALID")
    return dict(value)


def _event_hash(event: dict[str, Any]) -> str:
    value = dict(event)
    claimed = str(value.pop("event_hash", ""))
    previous = str(value.get("previous_event_hash", ""))
    canonical = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    calculated = hashlib.sha256(previous.encode() + b"\0" + canonical).hexdigest()
    if claimed != calculated:
        raise ValueError("RECOVERY_HEALTH_EVENT_HASH_INVALID")
    return calculated


def _open_event_file(path: Path) -> tuple[int, os.stat_result] | tuple[None, None]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None, None
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        os.close(descriptor)
        raise ValueError("RECOVERY_HEALTH_EVENT_FILE_UNSAFE")
    if metadata.st_size > MAXIMUM_EVENT_JOURNAL_BYTES:
        os.close(descriptor)
        raise ValueError("RECOVERY_HEALTH_EVENT_FILE_TOO_LARGE")
    return descriptor, metadata


def _verify_event_file_unchanged(path: Path, descriptor: int, opened: os.stat_result) -> None:
    """Keep one immutable journal snapshot across validation and replay."""

    current = os.fstat(descriptor)
    try:
        visible = os.stat(path, follow_symlinks=False)
    except FileNotFoundError as error:
        raise ValueError("RECOVERY_HEALTH_EVENT_FILE_CHANGED_DURING_READ") from error
    identity = (opened.st_dev, opened.st_ino)
    if (
        (current.st_dev, current.st_ino) != identity
        or (visible.st_dev, visible.st_ino) != identity
        or current.st_size != opened.st_size
        or current.st_mtime_ns != opened.st_mtime_ns
        or current.st_ctime_ns != opened.st_ctime_ns
    ):
        raise ValueError("RECOVERY_HEALTH_EVENT_FILE_CHANGED_DURING_READ")


def _frozen_event_lines(handle: Any, *, start: int, end: int) -> Iterator[bytes]:
    """Yield newline-terminated frames only inside the captured byte prefix."""

    if not 0 <= start <= end:
        raise ValueError("RECOVERY_HEALTH_EVENT_PREFIX_INVALID")
    handle.seek(start)
    consumed = start
    while consumed < end:
        maximum = min(MAXIMUM_EVENT_LINE_BYTES + 1, end - consumed)
        raw = handle.readline(maximum)
        if not raw or len(raw) > MAXIMUM_EVENT_LINE_BYTES or not raw.endswith(b"\n"):
            raise ValueError("RECOVERY_HEALTH_EVENT_INVALID")
        consumed += len(raw)
        yield raw
    if consumed != end:
        raise ValueError("RECOVERY_HEALTH_EVENT_PREFIX_INVALID")


def _event_tail(path: Path, *, committed_bytes: int | None = None) -> dict[str, Any] | None:
    descriptor, metadata = _open_event_file(path)
    if descriptor is None or metadata is None:
        return None
    try:
        size = metadata.st_size if committed_bytes is None else committed_bytes
        if not 0 <= size <= metadata.st_size:
            raise ValueError("RECOVERY_HEALTH_EVENT_PREFIX_INVALID")
        if size == 0:
            return None
        start = max(0, size - MAXIMUM_EVENT_LINE_BYTES)
        os.lseek(descriptor, start, os.SEEK_SET)
        raw = os.read(descriptor, size - start)
        _verify_event_file_unchanged(path, descriptor, metadata)
    finally:
        os.close(descriptor)
    if start > 0:
        separator = raw.find(b"\n")
        if separator < 0:
            raise ValueError("RECOVERY_HEALTH_EVENT_LINE_TOO_LARGE")
        raw = raw[separator + 1 :]
    lines = [line for line in raw.splitlines() if line]
    if not lines:
        return None
    if len(lines[-1]) > MAXIMUM_EVENT_LINE_BYTES:
        raise ValueError("RECOVERY_HEALTH_EVENT_LINE_TOO_LARGE")
    try:
        value = json.loads(lines[-1])
    except json.JSONDecodeError as error:
        raise ValueError("RECOVERY_HEALTH_EVENT_TAIL_INVALID") from error
    if not isinstance(value, dict):
        raise ValueError("RECOVERY_HEALTH_EVENT_TAIL_INVALID")
    return value


def _apply_replayed_event(state: dict[str, Any], event: dict[str, Any]) -> None:
    kind = str(event.get("event", ""))
    observed_at = str(event.get("observed_at", ""))
    observed = _parse_utc(observed_at)
    if event.get("control_capability_count") != 0 or event.get("physical_effect_count") != 0:
        raise ValueError("RECOVERY_HEALTH_EVENT_CAPABILITY_INVALID")
    if kind == "FAILURE_OBSERVED":
        retryable = event.get("retryable")
        exhausted = event.get("exhausted")
        if not isinstance(retryable, bool) or not isinstance(exhausted, bool):
            raise ValueError("RECOVERY_HEALTH_FAILURE_EVENT_INVALID")
        episode_id = str(event.get("episode_id", ""))
        episode = state.get("current_episode")
        if episode is None:
            episode = {
                "episode_id": episode_id,
                "started_at": observed_at,
                "ended_at": None,
                "state": "RECOVERING" if retryable else "TERMINAL",
                "category": str(event.get("category", "")),
                "last_reason_code": str(event.get("reason_code", "")),
                "failure_attempt_count": 0,
                "retry_attempt_count": 0,
                "exhausted_invocation_count": 0,
                "duration_ms": None,
            }
            state["current_episode"] = episode
            state["episode_count"] = int(state["episode_count"]) + 1
        if not episode_id or episode.get("episode_id") != episode_id:
            raise ValueError("RECOVERY_HEALTH_EVENT_EPISODE_INVALID")
        episode["failure_attempt_count"] = int(episode["failure_attempt_count"]) + 1
        episode["last_reason_code"] = str(event.get("reason_code", ""))
        episode["category"] = str(event.get("category", ""))
        state["failure_attempt_count"] = int(state["failure_attempt_count"]) + 1
        state["retry_attempt_count"] = int(state["retry_attempt_count"]) + int(retryable and not exhausted)
        state["exhausted_invocation_count"] = int(state["exhausted_invocation_count"]) + int(exhausted)
        if retryable:
            episode["state"] = "RECOVERING"
            episode["retry_attempt_count"] = int(episode["retry_attempt_count"]) + int(not exhausted)
            episode["exhausted_invocation_count"] = int(episode["exhausted_invocation_count"]) + int(exhausted)
            state["current_state"] = "RECOVERING"
        else:
            episode["state"] = "TERMINAL"
            episode["ended_at"] = observed_at
            episode["duration_ms"] = max(
                0,
                int((observed - _parse_utc(str(episode["started_at"]))).total_seconds() * 1000),
            )
            state["last_episode"] = dict(episode)
            state["current_episode"] = None
            state["terminal_episode_count"] = int(state["terminal_episode_count"]) + 1
            state["current_state"] = "SAFE_BLOCKED"
    elif kind == "RECOVERED":
        episode = state.get("current_episode")
        if not isinstance(episode, dict) or episode.get("episode_id") != event.get("episode_id"):
            raise ValueError("RECOVERY_HEALTH_RECOVERY_EVENT_INVALID")
        duration = max(
            0,
            int((observed - _parse_utc(str(episode["started_at"]))).total_seconds() * 1000),
        )
        episode["state"] = "RECOVERED"
        episode["ended_at"] = observed_at
        episode["duration_ms"] = duration
        state["last_episode"] = dict(episode)
        state["current_episode"] = None
        state["recovered_episode_count"] = int(state["recovered_episode_count"]) + 1
        state["maximum_recovery_duration_ms"] = max(int(state["maximum_recovery_duration_ms"]), duration)
        state["current_state"] = "READY"
    elif kind == "READY_RESTORED":
        if state.get("current_episode") is not None or state.get("current_state") != "SAFE_BLOCKED":
            raise ValueError("RECOVERY_HEALTH_READY_RESTORED_INVALID")
        state["current_state"] = "READY"
    else:
        raise ValueError("RECOVERY_HEALTH_EVENT_KIND_INVALID")
    state["updated_at"] = observed_at


def _replay_events(path: Path, *, component: str, release_id: str, now: datetime) -> dict[str, Any]:
    state = _new_state(component, release_id, now)
    descriptor, metadata = _open_event_file(path)
    if descriptor is None:
        return state
    assert metadata is not None
    if metadata.st_size > MAXIMUM_EVENT_REPLAY_BYTES:
        os.close(descriptor)
        raise ValueError("RECOVERY_HEALTH_FULL_REPLAY_TOO_LARGE")
    previous = ZERO_HASH
    try:
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            for sequence, raw in enumerate(
                _frozen_event_lines(handle, start=0, end=metadata.st_size), start=1
            ):
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError as error:
                    raise ValueError("RECOVERY_HEALTH_EVENT_INVALID") from error
                if not isinstance(event, dict):
                    raise ValueError("RECOVERY_HEALTH_EVENT_INVALID")
                if (
                    event.get("schema") != EVENT_SCHEMA
                    or event.get("component") != component
                    or event.get("release_id") != release_id
                    or event.get("sequence") != sequence
                    or event.get("previous_event_hash") != previous
                ):
                    raise ValueError("RECOVERY_HEALTH_EVENT_CHAIN_INVALID")
                previous = _event_hash(event)
                _apply_replayed_event(state, event)
                state["event_count"] = sequence
                state["last_event_hash"] = previous
                state["event_verified_bytes"] += len(raw)
            _verify_event_file_unchanged(path, handle.fileno(), metadata)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return state


@contextmanager
def _locked(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = path.with_name(f".{path.name}.lock")
    descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ValueError("RECOVERY_HEALTH_LOCK_UNSAFE")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _new_state(component: str, release_id: str, now: datetime) -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "component": component,
        "release_id": release_id,
        "current_state": "READY",
        "current_episode": None,
        "last_episode": None,
        "episode_count": 0,
        "recovered_episode_count": 0,
        "terminal_episode_count": 0,
        "failure_attempt_count": 0,
        "retry_attempt_count": 0,
        "exhausted_invocation_count": 0,
        "maximum_recovery_duration_ms": 0,
        "event_count": 0,
        "last_event_hash": ZERO_HASH,
        "event_verified_bytes": 0,
        "pending_event": None,
        "updated_at": isoformat_utc(now),
    }


def _event_bytes(event: dict[str, Any]) -> bytes:
    raw = (json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n").encode()
    if len(raw) > MAXIMUM_EVENT_LINE_BYTES:
        raise ValueError("RECOVERY_HEALTH_EVENT_LINE_TOO_LARGE")
    return raw


def _write_all(descriptor: int, raw: bytes) -> None:
    remaining = memoryview(raw)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("RECOVERY_HEALTH_EVENT_WRITE_NO_PROGRESS")
        remaining = remaining[written:]


def _event_file_size(path: Path) -> int:
    descriptor, metadata = _open_event_file(path)
    if descriptor is None or metadata is None:
        return 0
    os.close(descriptor)
    return metadata.st_size


def _committed_event_matches(path: Path, state: dict[str, Any]) -> bool:
    count = state.get("event_count")
    event_hash = state.get("last_event_hash")
    committed_bytes = state.get("event_verified_bytes")
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        or not isinstance(event_hash, str)
        or isinstance(committed_bytes, bool)
        or not isinstance(committed_bytes, int)
        or committed_bytes < 0
    ):
        return False
    tail = _event_tail(path, committed_bytes=committed_bytes)
    if count == 0:
        return committed_bytes == 0 and event_hash == ZERO_HASH and tail is None
    return (
        isinstance(tail, dict)
        and tail.get("sequence") == count
        and tail.get("event_hash") == event_hash
        and _event_hash(tail) == event_hash
    )


def _append_reserved_event(path: Path, event: dict[str, Any], *, committed_bytes: int) -> int:
    """Complete only the exact reserved frame after a committed prefix."""

    expected = _event_bytes(event)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ValueError("RECOVERY_HEALTH_EVENT_FILE_UNSAFE")
        if metadata.st_size > MAXIMUM_EVENT_JOURNAL_BYTES:
            raise ValueError("RECOVERY_HEALTH_EVENT_FILE_TOO_LARGE")
        suffix_size = metadata.st_size - committed_bytes
        if suffix_size < 0 or suffix_size > len(expected):
            raise ValueError("RECOVERY_HEALTH_PENDING_EVENT_CONFLICT")
        suffix = os.pread(descriptor, suffix_size, committed_bytes)
        if suffix != expected[:suffix_size]:
            raise ValueError("RECOVERY_HEALTH_PENDING_EVENT_CONFLICT")
        if committed_bytes + len(expected) > MAXIMUM_EVENT_JOURNAL_BYTES:
            raise ValueError("RECOVERY_HEALTH_EVENT_FILE_TOO_LARGE")
        os.lseek(descriptor, 0, os.SEEK_END)
        _write_all(descriptor, expected[suffix_size:])
        os.fsync(descriptor)
        if os.fstat(descriptor).st_size != committed_bytes + len(expected):
            raise ValueError("RECOVERY_HEALTH_PENDING_EVENT_CONFLICT")
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)
    return committed_bytes + len(expected)


def _replay_event_suffix(
    path: Path,
    state: dict[str, Any],
    *,
    component: str,
    release_id: str,
) -> dict[str, Any]:
    committed_bytes = int(state["event_verified_bytes"])
    descriptor, metadata = _open_event_file(path)
    if descriptor is None or metadata is None:
        if committed_bytes:
            raise ValueError("RECOVERY_HEALTH_EVENT_JOURNAL_TRUNCATED")
        return state
    size = metadata.st_size
    if size < committed_bytes:
        os.close(descriptor)
        raise ValueError("RECOVERY_HEALTH_EVENT_JOURNAL_TRUNCATED")
    if size - committed_bytes > MAXIMUM_EVENT_REPLAY_BYTES:
        os.close(descriptor)
        raise ValueError("RECOVERY_HEALTH_SUFFIX_REPLAY_TOO_LARGE")
    if size == committed_bytes:
        os.close(descriptor)
        return state
    replayed = copy.deepcopy(state)
    replayed["pending_event"] = None
    previous = str(replayed["last_event_hash"])
    sequence = int(replayed["event_count"])
    try:
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            for raw in _frozen_event_lines(handle, start=committed_bytes, end=size):
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError as error:
                    raise ValueError("RECOVERY_HEALTH_EVENT_INVALID") from error
                sequence += 1
                if (
                    not isinstance(event, dict)
                    or event.get("schema") != EVENT_SCHEMA
                    or event.get("component") != component
                    or event.get("release_id") != release_id
                    or event.get("sequence") != sequence
                    or event.get("previous_event_hash") != previous
                ):
                    raise ValueError("RECOVERY_HEALTH_EVENT_CHAIN_INVALID")
                previous = _event_hash(event)
                _apply_replayed_event(replayed, event)
                replayed["event_count"] = sequence
                replayed["last_event_hash"] = previous
                replayed["event_verified_bytes"] += len(raw)
            _verify_event_file_unchanged(path, handle.fileno(), metadata)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return replayed


def _episode_id(component: str, release_id: str, started_at: str, ordinal: int) -> str:
    return hashlib.sha256(f"{component}\0{release_id}\0{started_at}\0{ordinal}".encode()).hexdigest()


class RecoveryHealthStore:
    """Durable current state plus an append-only hash-chained recovery journal."""

    def __init__(self, *, state_file: Path, event_file: Path, component: str, release_id: str) -> None:
        if not component or not release_id:
            raise ValueError("RECOVERY_HEALTH_IDENTITY_MISSING")
        self.state_file = state_file
        self.event_file = event_file
        self.component = component
        self.release_id = release_id

    def _load(self, now: datetime) -> dict[str, Any]:
        state = _read_state(self.state_file)
        if state is None:
            if _event_file_size(self.event_file):
                state = _replay_events(
                    self.event_file,
                    component=self.component,
                    release_id=self.release_id,
                    now=now,
                )
                _atomic_json(self.state_file, state)
                return state
            return _new_state(self.component, self.release_id, now)
        if state.get("component") != self.component or state.get("release_id") != self.release_id:
            raise ValueError("RECOVERY_HEALTH_IDENTITY_MISMATCH")

        if state.get("schema") == LEGACY_STATE_SCHEMA:
            legacy_fields = set(_new_state(self.component, self.release_id, now)) - {
                "event_verified_bytes",
                "pending_event",
            }
            if set(state) != legacy_fields:
                raise ValueError("RECOVERY_HEALTH_STATE_INVALID")
            tail = _event_tail(self.event_file)
            size = _event_file_size(self.event_file)
            if tail is None:
                if state.get("event_count") != 0 or state.get("last_event_hash") != ZERO_HASH:
                    raise ValueError("RECOVERY_HEALTH_EVENT_JOURNAL_MISSING")
            elif (
                state.get("event_count") != tail.get("sequence")
                or state.get("last_event_hash") != _event_hash(tail)
            ):
                replayed = _replay_events(
                    self.event_file,
                    component=self.component,
                    release_id=self.release_id,
                    now=now,
                )
                if int(state.get("event_count", 0)) > int(replayed["event_count"]):
                    raise ValueError("RECOVERY_HEALTH_STATE_AHEAD_OF_JOURNAL")
                _atomic_json(self.state_file, replayed)
                return replayed
            state.update(
                schema=STATE_SCHEMA,
                event_verified_bytes=size,
                pending_event=None,
            )
            _atomic_json(self.state_file, state)
            return state

        if set(state) != set(_new_state(self.component, self.release_id, now)):
            raise ValueError("RECOVERY_HEALTH_STATE_INVALID")
        try:
            committed_matches = _committed_event_matches(self.event_file, state)
        except ValueError:
            # Preserve the historical tamper classification when a rewrite
            # changed the serialized length of the committed tail.
            current_tail = _event_tail(self.event_file)
            if isinstance(current_tail, dict) and current_tail.get("sequence") == state.get("event_count"):
                _event_hash(current_tail)
            raise
        if not committed_matches:
            raise ValueError("RECOVERY_HEALTH_COMMITTED_PREFIX_INVALID")
        pending = state["pending_event"]
        if pending is not None:
            if (
                not isinstance(pending, dict)
                or pending.get("schema") != EVENT_SCHEMA
                or pending.get("component") != self.component
                or pending.get("release_id") != self.release_id
                or pending.get("sequence") != int(state["event_count"]) + 1
                or pending.get("previous_event_hash") != state["last_event_hash"]
                or _event_hash(pending) != pending.get("event_hash")
            ):
                raise ValueError("RECOVERY_HEALTH_PENDING_EVENT_INVALID")
            state["event_verified_bytes"] = _append_reserved_event(
                self.event_file,
                pending,
                committed_bytes=int(state["event_verified_bytes"]),
            )
            state["event_count"] = pending["sequence"]
            state["last_event_hash"] = pending["event_hash"]
            state["pending_event"] = None
            _atomic_json(self.state_file, state)
            return state

        replayed = _replay_event_suffix(
            self.event_file,
            state,
            component=self.component,
            release_id=self.release_id,
        )
        if replayed is not state:
            _atomic_json(self.state_file, replayed)
        return replayed

    def _append_event(self, state: dict[str, Any], payload: dict[str, Any]) -> None:
        previous = str(state["last_event_hash"])
        event = {
            "schema": EVENT_SCHEMA,
            "component": self.component,
            "release_id": self.release_id,
            "sequence": int(state["event_count"]) + 1,
            "previous_event_hash": previous,
            **payload,
        }
        canonical = json.dumps(event, separators=(",", ":"), sort_keys=True).encode()
        event_hash = hashlib.sha256(previous.encode() + b"\0" + canonical).hexdigest()
        event["event_hash"] = event_hash
        state["pending_event"] = event
        _atomic_json(self.state_file, state)
        state["event_verified_bytes"] = _append_reserved_event(
            self.event_file,
            event,
            committed_bytes=int(state["event_verified_bytes"]),
        )
        state["event_count"] = event["sequence"]
        state["last_event_hash"] = event_hash
        state["pending_event"] = None

    def record_failure(
        self,
        classification: FailureClassification,
        *,
        attempt: int,
        exhausted: bool,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        observed_at = isoformat_utc(current)
        with _locked(self.state_file):
            state = self._load(current)
            episode = state.get("current_episode")
            if episode is None:
                ordinal = int(state["episode_count"]) + 1
                episode = {
                    "episode_id": _episode_id(self.component, self.release_id, observed_at, ordinal),
                    "started_at": observed_at,
                    "ended_at": None,
                    "state": "RECOVERING" if classification.retryable else "TERMINAL",
                    "category": classification.category,
                    "last_reason_code": classification.reason_code,
                    "failure_attempt_count": 0,
                    "retry_attempt_count": 0,
                    "exhausted_invocation_count": 0,
                    "duration_ms": None,
                }
                state["current_episode"] = episode
                state["episode_count"] = ordinal
            episode["failure_attempt_count"] = int(episode["failure_attempt_count"]) + 1
            episode["last_reason_code"] = classification.reason_code
            episode["category"] = classification.category
            if classification.retryable:
                episode["retry_attempt_count"] = int(episode["retry_attempt_count"]) + int(not exhausted)
                episode["exhausted_invocation_count"] = int(episode["exhausted_invocation_count"]) + int(exhausted)
                state["current_state"] = "RECOVERING"
            else:
                episode["state"] = "TERMINAL"
                episode["ended_at"] = observed_at
                episode["duration_ms"] = max(
                    0,
                    int((current - _parse_utc(str(episode["started_at"]))).total_seconds() * 1000),
                )
                state["last_episode"] = dict(episode)
                state["current_episode"] = None
                state["terminal_episode_count"] = int(state["terminal_episode_count"]) + 1
                state["current_state"] = "SAFE_BLOCKED"
            state["failure_attempt_count"] = int(state["failure_attempt_count"]) + 1
            state["retry_attempt_count"] = int(state["retry_attempt_count"]) + int(classification.retryable and not exhausted)
            state["exhausted_invocation_count"] = int(state["exhausted_invocation_count"]) + int(exhausted)
            state["updated_at"] = observed_at
            self._append_event(
                state,
                {
                    "event": "FAILURE_OBSERVED",
                    "episode_id": episode["episode_id"],
                    "category": classification.category,
                    "reason_code": classification.reason_code,
                    "retryable": classification.retryable,
                    "attempt": attempt,
                    "exhausted": exhausted,
                    "observed_at": observed_at,
                    "control_capability_count": 0,
                    "physical_effect_count": 0,
                },
            )
            _atomic_json(self.state_file, state)
            return state

    def record_success(
        self,
        *,
        attempt_count: int,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        observed_at = isoformat_utc(current)
        with _locked(self.state_file):
            state = self._load(current)
            episode = state.get("current_episode")
            if isinstance(episode, dict):
                episode_id = str(episode["episode_id"])
                duration = max(0, int((current - _parse_utc(str(episode["started_at"]))).total_seconds() * 1000))
                episode["state"] = "RECOVERED"
                episode["ended_at"] = observed_at
                episode["duration_ms"] = duration
                state["last_episode"] = dict(episode)
                state["current_episode"] = None
                state["recovered_episode_count"] = int(state["recovered_episode_count"]) + 1
                state["maximum_recovery_duration_ms"] = max(int(state["maximum_recovery_duration_ms"]), duration)
                self._append_event(
                    state,
                    {
                        "event": "RECOVERED",
                        "episode_id": episode_id,
                        "attempt_count": attempt_count,
                        "observed_at": observed_at,
                        "control_capability_count": 0,
                        "physical_effect_count": 0,
                    },
                )
            elif state["current_state"] == "SAFE_BLOCKED":
                self._append_event(
                    state,
                    {
                        "event": "READY_RESTORED",
                        "episode_id": None if state["last_episode"] is None else state["last_episode"].get("episode_id"),
                        "attempt_count": attempt_count,
                        "observed_at": observed_at,
                        "control_capability_count": 0,
                        "physical_effect_count": 0,
                    },
                )
            state["current_state"] = "READY"
            state["updated_at"] = observed_at
            # Healthy timer cycles update the compact current-state file but do
            # not append an event.  The journal records transitions, not every
            # poll, so a permanent 2-second timer cannot grow it without bound.
            _atomic_json(self.state_file, state)
            return state

    def snapshot(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        with _locked(self.state_file):
            state = self._load(current)
        episode = state.get("current_episode")
        age_ms = 0
        if isinstance(episode, dict):
            age_ms = max(0, int((current - _parse_utc(str(episode["started_at"]))).total_seconds() * 1000))
        return {
            # The signed/published projection stays v1-compatible. The v2
            # schema is the private cache transaction format and adds no
            # remotely trusted health claim.
            "schema": LEGACY_STATE_SCHEMA,
            "component": state["component"],
            "release_id": state["release_id"],
            "current_state": state["current_state"],
            "current_episode_id": None if episode is None else episode["episode_id"],
            "current_episode_age_ms": age_ms,
            "last_episode": state["last_episode"],
            "episode_count": state["episode_count"],
            "recovered_episode_count": state["recovered_episode_count"],
            "terminal_episode_count": state["terminal_episode_count"],
            "failure_attempt_count": state["failure_attempt_count"],
            "retry_attempt_count": state["retry_attempt_count"],
            "exhausted_invocation_count": state["exhausted_invocation_count"],
            "maximum_recovery_duration_ms": state["maximum_recovery_duration_ms"],
            "event_count": state["event_count"],
            "last_event_hash": state["last_event_hash"],
            "updated_at": state["updated_at"],
        }
