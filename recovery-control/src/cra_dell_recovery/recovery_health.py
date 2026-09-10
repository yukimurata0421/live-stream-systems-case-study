from __future__ import annotations

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

STATE_SCHEMA = "cra.transport_recovery_state.v1"
EVENT_SCHEMA = "cra.transport_recovery_event.v1"
ZERO_HASH = "0" * 64
MAXIMUM_EVENT_JOURNAL_BYTES = 64 * 1024 * 1024
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
    if not isinstance(value, dict) or value.get("schema") != STATE_SCHEMA:
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


def _event_tail(path: Path) -> dict[str, Any] | None:
    descriptor, metadata = _open_event_file(path)
    if descriptor is None or metadata is None:
        return None
    try:
        if metadata.st_size == 0:
            return None
        start = max(0, metadata.st_size - MAXIMUM_EVENT_LINE_BYTES)
        os.lseek(descriptor, start, os.SEEK_SET)
        raw = os.read(descriptor, metadata.st_size - start)
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
    descriptor, _metadata = _open_event_file(path)
    if descriptor is None:
        return state
    previous = ZERO_HASH
    try:
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            for sequence, line in enumerate(handle, start=1):
                if len(line.encode()) > MAXIMUM_EVENT_LINE_BYTES:
                    raise ValueError("RECOVERY_HEALTH_EVENT_LINE_TOO_LARGE")
                try:
                    event = json.loads(line)
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
        "updated_at": isoformat_utc(now),
    }


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
        state = _read_state(self.state_file) or _new_state(self.component, self.release_id, now)
        if state.get("component") != self.component or state.get("release_id") != self.release_id:
            raise ValueError("RECOVERY_HEALTH_IDENTITY_MISMATCH")
        tail = _event_tail(self.event_file)
        if tail is None:
            if state.get("event_count") != 0 or state.get("last_event_hash") != ZERO_HASH:
                raise ValueError("RECOVERY_HEALTH_EVENT_JOURNAL_MISSING")
            return state
        tail_hash = _event_hash(tail)
        if state.get("event_count") == tail.get("sequence") and state.get("last_event_hash") == tail_hash:
            return state
        # Event append is fsynced before the derived current-state cache is
        # replaced.  A crash in that window leaves the journal ahead.  Replay
        # the authoritative chain and repair the cache instead of appending a
        # duplicate sequence/hash branch.
        replayed = _replay_events(self.event_file, component=self.component, release_id=self.release_id, now=now)
        if int(state.get("event_count", 0)) > int(replayed["event_count"]):
            raise ValueError("RECOVERY_HEALTH_STATE_AHEAD_OF_JOURNAL")
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
        self.event_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(
            self.event_file,
            os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW,
            0o600,
        )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise ValueError("RECOVERY_HEALTH_EVENT_FILE_UNSAFE")
            with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
                descriptor = -1
                handle.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        _fsync_directory(self.event_file.parent)
        state["event_count"] = event["sequence"]
        state["last_event_hash"] = event_hash

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
            "schema": STATE_SCHEMA,
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
