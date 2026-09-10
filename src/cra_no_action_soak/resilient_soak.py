from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import stat
import time
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_dell_recovery.canonical import KeyRing, canonical_json
from cra_dell_recovery.json_input import load_object
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc

from .host_status import atomic_write_json, read_object
from .host_status_pull import _public_key
from .resilient_host_status import ResilientHostStatusContract

CONFIG_SCHEMA = "cra.resilient_status_soak_config.v2"
SAMPLE_SCHEMA = "cra.resilient_status_soak_sample.v2"
STATE_SCHEMA = "cra.resilient_status_soak_state.v1"
GATE_SCHEMA = "cra.resilient_status_soak_gate.v2"
WATCHDOG_SCHEMA = "cra.resilient_status_soak_watchdog.v2"
MAXIMUM_SAMPLE_BYTES = 8 * 1024 * 1024
EVIDENCE_CAPACITY_SAFETY_FACTOR = 1.5
MAXIMUM_GATE_AGE_SECONDS = 2 * 3600
MINIMUM_FILESYSTEM_FREE_BYTES = 5 * 1024 * 1024 * 1024
MINIMUM_FILESYSTEM_FREE_INODES = 10_000
DEGRADED_STATES = frozenset({"DEGRADED_SOURCE", "DEGRADED_OBSERVABILITY", "RECOVERING", "COMMUNICATION_DEGRADED"})
PULL_RECOVERY_STATE_SCHEMA = "cra.transport_recovery_state.v1"
PULL_RECOVERY_COMPONENT = "cra_arena_resilient_status_pull"
PULL_RECOVERY_COUNTER_FIELDS = (
    "episode_count",
    "event_count",
    "exhausted_invocation_count",
    "failure_attempt_count",
    "maximum_recovery_duration_ms",
    "recovered_episode_count",
    "retry_attempt_count",
    "terminal_episode_count",
)
PULL_RECOVERY_FIELDS = frozenset(
    {
        "schema",
        "component",
        "release_id",
        "current_state",
        "current_episode",
        "last_episode",
        *PULL_RECOVERY_COUNTER_FIELDS,
        "last_event_hash",
        "updated_at",
    }
)
PENDING_ELIGIBILITY_BLOCKERS = frozenset({"SAMPLE_COUNT_INSUFFICIENT", "SOAK_DURATION_INSUFFICIENT"})


class SoakConfig:
    def __init__(self, value: dict[str, Any], path: Path) -> None:
        self.value = value
        self.path = path

    @classmethod
    def load(cls, path: Path) -> SoakConfig:
        value = read_object(path, maximum_bytes=128 * 1024)
        required = {
            "schema",
            "cra_release_id",
            "resilient_host_status_schema_file",
            "arena_key_id",
            "arena_public_key_file",
            "arena_host_id",
            "arena_release_id",
            "dell_key_id",
            "dell_public_key_file",
            "dell_host_id",
            "dell_release_id",
            "arena_status_inbox_file",
            "cra_pull_status_file",
            "cra_pull_recovery_state_file",
            "evidence_file",
            "state_file",
            "gate_file",
            "minimum_duration_seconds",
            "maximum_sample_gap_seconds",
            "maximum_recovery_episode_seconds",
            "maximum_report_age_seconds",
            "maximum_clock_uncertainty_ms",
            "maximum_evidence_bytes",
        }
        if set(value) != required or value.get("schema") != CONFIG_SCHEMA:
            raise ValueError("RESILIENT_SOAK_CONFIG_FIELDS_INVALID")
        for name in (
            "cra_release_id",
            "arena_key_id",
            "arena_host_id",
            "arena_release_id",
            "dell_key_id",
            "dell_host_id",
            "dell_release_id",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).strip():
                raise ValueError(f"RESILIENT_SOAK_{name.upper()}_INVALID")
        require_runtime_release(str(value["cra_release_id"]), "RESILIENT_SOAK_RUNTIME_RELEASE_MISMATCH")
        for name in (
            "resilient_host_status_schema_file",
            "arena_public_key_file",
            "dell_public_key_file",
            "arena_status_inbox_file",
            "cra_pull_status_file",
            "cra_pull_recovery_state_file",
            "evidence_file",
            "state_file",
            "gate_file",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).startswith("/"):
                raise ValueError(f"RESILIENT_SOAK_{name.upper()}_INVALID")
        for name, lower, upper in (
            ("minimum_duration_seconds", 60, 7 * 86400),
            ("maximum_sample_gap_seconds", 5, 600),
            ("maximum_recovery_episode_seconds", 1, 3600),
            ("maximum_report_age_seconds", 1, 300),
            ("maximum_clock_uncertainty_ms", 1, 60_000),
            ("maximum_evidence_bytes", 1024 * 1024, 2 * 1024 * 1024 * 1024),
        ):
            raw = value[name]
            if (
                isinstance(raw, bool)
                or not isinstance(raw, (int, float))
                or not math.isfinite(float(raw))
                or not lower <= float(raw) <= upper
            ):
                raise ValueError(f"RESILIENT_SOAK_{name.upper()}_INVALID")
        return cls(value, path)


def _contracts(config: SoakConfig) -> tuple[ResilientHostStatusContract, ResilientHostStatusContract]:
    value = config.value
    schema = Path(value["resilient_host_status_schema_file"])
    dell = ResilientHostStatusContract(
        schema,
        KeyRing({str(value["dell_key_id"]): _public_key(Path(value["dell_public_key_file"]))}),
        key_id=str(value["dell_key_id"]),
        expected_role="dell",
        expected_host_id=str(value["dell_host_id"]),
        expected_release_id=str(value["dell_release_id"]),
        maximum_report_lease_seconds=300,
        maximum_clock_skew_seconds=5,
    )
    arena = ResilientHostStatusContract(
        schema,
        KeyRing({str(value["arena_key_id"]): _public_key(Path(value["arena_public_key_file"]))}),
        key_id=str(value["arena_key_id"]),
        expected_role="arena",
        expected_host_id=str(value["arena_host_id"]),
        expected_release_id=str(value["arena_release_id"]),
        maximum_report_lease_seconds=300,
        maximum_clock_skew_seconds=5,
        upstream_contract=dell,
    )
    return arena, dell


def pull_recovery_path(config: SoakConfig) -> Path:
    return Path(str(config.value["cra_pull_recovery_state_file"]))


def _validate_pull_recovery_state(
    config: SoakConfig,
    state: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    if set(state) != PULL_RECOVERY_FIELDS:
        raise ValueError("RESILIENT_SOAK_CRA_PULL_RECOVERY_FIELDS_INVALID")
    if (
        state.get("schema") != PULL_RECOVERY_STATE_SCHEMA
        or state.get("component") != PULL_RECOVERY_COMPONENT
        or state.get("release_id") != config.value["cra_release_id"]
        or state.get("current_state") not in {"READY", "RECOVERING", "SAFE_BLOCKED"}
        or not re.fullmatch(r"[0-9a-f]{64}", str(state.get("last_event_hash", "")))
        or (state.get("current_episode") is not None and not isinstance(state["current_episode"], dict))
        or (state.get("last_episode") is not None and not isinstance(state["last_episode"], dict))
    ):
        raise ValueError("RESILIENT_SOAK_CRA_PULL_RECOVERY_STATE_INVALID")
    for field in PULL_RECOVERY_COUNTER_FIELDS:
        counter = state.get(field)
        if isinstance(counter, bool) or not isinstance(counter, int) or counter < 0:
            raise ValueError("RESILIENT_SOAK_CRA_PULL_RECOVERY_COUNTER_INVALID")
    completed_episodes = int(state["recovered_episode_count"]) + int(state["terminal_episode_count"])
    active_episode_count = int(state["current_episode"] is not None)
    if (
        int(state["episode_count"]) != completed_episodes + active_episode_count
        or int(state["failure_attempt_count"]) < int(state["episode_count"])
        or int(state["retry_attempt_count"]) > int(state["failure_attempt_count"])
        or int(state["exhausted_invocation_count"]) > int(state["failure_attempt_count"])
        or int(state["event_count"]) < int(state["failure_attempt_count"])
        or (int(state["event_count"]) == 0) != (state["last_event_hash"] == "0" * 64)
        or (state["current_state"] == "RECOVERING") != (state["current_episode"] is not None)
        or (state["current_state"] == "SAFE_BLOCKED" and int(state["terminal_episode_count"]) == 0)
    ):
        raise ValueError("RESILIENT_SOAK_CRA_PULL_RECOVERY_COUNTER_INCONSISTENT")
    updated_at = parse_utc(str(state.get("updated_at", "")))
    age = (now - updated_at).total_seconds()
    if age < -5 or age > float(config.value["maximum_report_age_seconds"]):
        raise ValueError("RESILIENT_SOAK_CRA_PULL_RECOVERY_STATE_STALE")
    return state


def _pull_recovery_state(
    config: SoakConfig,
    *,
    now: datetime,
) -> dict[str, Any]:
    state = read_object(pull_recovery_path(config), maximum_bytes=1024 * 1024)
    return _validate_pull_recovery_state(config, state, now=now)


def _sample_hash(value: Mapping[str, Any]) -> str:
    unsigned = {name: item for name, item in value.items() if name != "sample_hash"}
    return hashlib.sha256(canonical_json(unsigned)).hexdigest()


def _gate_status(blockers: set[str]) -> str:
    if not blockers:
        return "PASS"
    if blockers <= PENDING_ELIGIBILITY_BLOCKERS:
        return "NOT_YET_ELIGIBLE"
    return "FAIL"


def _serialized_line(value: Mapping[str, Any]) -> bytes:
    raw = (json.dumps(dict(value), separators=(",", ":"), sort_keys=True) + "\n").encode()
    if len(raw) > MAXIMUM_SAMPLE_BYTES:
        raise ValueError("RESILIENT_SOAK_SAMPLE_TOO_LARGE")
    return raw


def _write_all(handle: Any, raw: bytes) -> None:
    """Write one complete frame or fail without claiming progress."""

    remaining = memoryview(raw)
    while remaining:
        written = handle.write(remaining)
        if written is None or written <= 0:
            raise OSError(errno.EIO, "RESILIENT_SOAK_EVIDENCE_WRITE_NO_PROGRESS")
        remaining = remaining[written:]


def _append_line(path: Path, value: Mapping[str, Any], *, maximum_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    raw = _serialized_line(value)
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("RESILIENT_SOAK_EVIDENCE_NOT_REGULAR_FILE")
        if metadata.st_size + len(raw) > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_CAPACITY_EXHAUSTED")
        with os.fdopen(descriptor, "ab", buffering=0) as handle:
            descriptor = -1
            _write_all(handle, raw)
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _last_line(path: Path, *, maximum_bytes: int) -> dict[str, Any] | None:
    if not path.exists():
        return None
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("RESILIENT_SOAK_EVIDENCE_NOT_REGULAR_FILE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_TOO_LARGE")
        if metadata.st_size == 0:
            return None
        read_size = min(metadata.st_size, 4 * 1024 * 1024)
        os.lseek(descriptor, metadata.st_size - read_size, os.SEEK_SET)
        raw = os.read(descriptor, read_size)
    finally:
        os.close(descriptor)
    if not raw.endswith(b"\n"):
        raise ValueError("RESILIENT_SOAK_EVIDENCE_PARTIAL_TAIL")
    lines = raw.rstrip(b"\n").splitlines()
    if not lines:
        return None
    try:
        return load_object(lines[-1], maximum_bytes=read_size)
    except ValueError as error:
        if str(error) == "JSON_INPUT_NOT_OBJECT":
            raise ValueError("RESILIENT_SOAK_SAMPLE_NOT_OBJECT") from error
        raise


def _state(path: Path, evidence: Path, *, maximum_bytes: int) -> dict[str, Any]:
    last = _last_line(evidence, maximum_bytes=maximum_bytes)
    if not path.exists():
        if last is not None:
            raise ValueError("RESILIENT_SOAK_STATE_MISSING")
        return {"schema": STATE_SCHEMA, "sample_count": 0, "last_sample_hash": None, "last_observed_at": None}
    state = read_object(path, maximum_bytes=64 * 1024)
    if set(state) != {"schema", "sample_count", "last_sample_hash", "last_observed_at"} or state["schema"] != STATE_SCHEMA:
        raise ValueError("RESILIENT_SOAK_STATE_INVALID")
    if last is None and int(state["sample_count"]) != 0:
        raise ValueError("RESILIENT_SOAK_EVIDENCE_MISSING")
    if last is not None:
        last_count = int(last.get("sample_sequence", -1))
        state_count = int(state["sample_count"])
        if last_count == state_count + 1 and last.get("previous_sample_hash") == state["last_sample_hash"]:
            state = {
                "schema": STATE_SCHEMA,
                "sample_count": last_count,
                "last_sample_hash": last.get("sample_hash"),
                "last_observed_at": last.get("observed_at"),
            }
            atomic_write_json(path, state)
        elif (
            last_count != state_count
            or last.get("sample_hash") != state["last_sample_hash"]
            or last.get("observed_at") != state["last_observed_at"]
        ):
            raise ValueError("RESILIENT_SOAK_STATE_EVIDENCE_FORK")
    return state


def collect_sample(config: SoakConfig, *, now: datetime | None = None) -> dict[str, Any]:
    value = config.value
    current = (now or datetime.now(UTC)).astimezone(UTC)
    arena_contract, _ = _contracts(config)
    arena = arena_contract.decode(read_object(Path(value["arena_status_inbox_file"])), now=current)
    upstream = arena["upstream_status"]
    dell = dict(upstream) if isinstance(upstream, dict) else None
    pull = read_object(Path(value["cra_pull_status_file"]), maximum_bytes=128 * 1024)
    if (
        pull.get("schema") != "cra.resilient_host_status_pull_status.v2"
        or pull.get("status") != "READY"
        or pull.get("source_producer_instance_id") != arena["producer_instance_id"]
        or pull.get("source_producer_sequence") != arena["producer_sequence"]
    ):
        raise ValueError("RESILIENT_SOAK_CRA_PULL_BINDING_INVALID")
    pull_observed = parse_utc(str(pull.get("observed_at", "")))
    if pull_observed > current or (current - pull_observed).total_seconds() > float(value["maximum_report_age_seconds"]):
        raise ValueError("RESILIENT_SOAK_CRA_PULL_STATUS_STALE")
    pull_recovery = _pull_recovery_state(config, now=current)
    evidence = Path(value["evidence_file"])
    state_file = Path(value["state_file"])
    state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = os.open(state_file.with_suffix(".lock"), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = _state(
            state_file,
            evidence,
            maximum_bytes=int(value["maximum_evidence_bytes"]),
        )
        previous_observed = state["last_observed_at"]
        if previous_observed is not None and parse_utc(str(previous_observed)) >= current:
            raise ValueError("RESILIENT_SOAK_SAMPLE_TIME_REGRESSION")
        sample: dict[str, Any] = {
            "schema": SAMPLE_SCHEMA,
            "sample_sequence": int(state["sample_count"]) + 1,
            "observed_at": isoformat_utc(current),
            "previous_sample_hash": state["last_sample_hash"],
            "cra_release_id": value["cra_release_id"],
            "arena_status": arena,
            "dell_status_payload_sha256": dell["payload_sha256"] if dell is not None else None,
            "cra_pull_status": pull,
            "cra_pull_recovery_state": pull_recovery,
        }
        sample["sample_hash"] = _sample_hash(sample)
        # Make the one-frame recovery rule valid for the first append too.
        # A missing state beside pre-existing evidence remains fail-closed in
        # _state(); only this known empty head may precede a first frame.
        if not state_file.exists():
            atomic_write_json(state_file, state)
        _append_line(
            evidence,
            sample,
            maximum_bytes=int(value["maximum_evidence_bytes"]),
        )
        next_state = {
            "schema": STATE_SCHEMA,
            "sample_count": sample["sample_sequence"],
            "last_sample_hash": sample["sample_hash"],
            "last_observed_at": sample["observed_at"],
        }
        atomic_write_json(state_file, next_state)
        return sample
    finally:
        os.close(lock)


def read_samples(path: Path, *, maximum_bytes: int) -> list[dict[str, Any]]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if metadata.st_size > maximum_bytes:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes or (raw and not raw.endswith(b"\n")):
        raise ValueError("RESILIENT_SOAK_EVIDENCE_INVALID")
    result: list[dict[str, Any]] = []
    for line in raw.splitlines():
        item = json.loads(line)
        if not isinstance(item, dict):
            raise ValueError("RESILIENT_SOAK_SAMPLE_NOT_OBJECT")
        result.append(dict(item))
    return result


@dataclass
class EvidenceSnapshot(Iterable[dict[str, Any]]):
    path: Path
    maximum_bytes: int
    expected_count: int
    expected_last_hash: str | None
    expected_last_observed_at: str | None
    file_size_at_open: int = 0
    bytes_read: int = 0

    def __iter__(self) -> Iterator[dict[str, Any]]:
        if self.bytes_read != 0:
            raise ValueError("RESILIENT_SOAK_SNAPSHOT_ALREADY_CONSUMED")
        if not self.path.exists():
            if self.expected_count != 0:
                raise ValueError("RESILIENT_SOAK_EVIDENCE_MISSING")
            return
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        last: dict[str, Any] | None = None
        try:
            metadata = os.fstat(descriptor)
            self.file_size_at_open = metadata.st_size
            if metadata.st_size > self.maximum_bytes:
                raise ValueError("RESILIENT_SOAK_EVIDENCE_TOO_LARGE")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                for _ in range(self.expected_count):
                    raw = handle.readline(MAXIMUM_SAMPLE_BYTES + 1)
                    if not raw:
                        raise ValueError("RESILIENT_SOAK_EVIDENCE_TRUNCATED")
                    if len(raw) > MAXIMUM_SAMPLE_BYTES:
                        raise ValueError("RESILIENT_SOAK_SAMPLE_TOO_LARGE")
                    if not raw.endswith(b"\n"):
                        raise ValueError("RESILIENT_SOAK_EVIDENCE_PARTIAL_TAIL")
                    self.bytes_read += len(raw)
                    item = json.loads(raw)
                    if not isinstance(item, dict):
                        raise ValueError("RESILIENT_SOAK_SAMPLE_NOT_OBJECT")
                    last = dict(item)
                    yield last
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        if self.expected_count == 0:
            if self.expected_last_hash is not None or self.expected_last_observed_at is not None:
                raise ValueError("RESILIENT_SOAK_STATE_INVALID")
            return
        if last is None:
            raise ValueError("RESILIENT_SOAK_EVIDENCE_TRUNCATED")
        if last.get("sample_hash") != self.expected_last_hash or last.get("observed_at") != self.expected_last_observed_at:
            raise ValueError("RESILIENT_SOAK_STATE_EVIDENCE_FORK")


def evidence_snapshot(config: SoakConfig) -> EvidenceSnapshot:
    value = config.value
    state_path = Path(value["state_file"])
    evidence_path = Path(value["evidence_file"])
    if not state_path.exists():
        if evidence_path.exists() and evidence_path.stat().st_size != 0:
            raise ValueError("RESILIENT_SOAK_STATE_MISSING")
        return EvidenceSnapshot(
            evidence_path,
            int(value["maximum_evidence_bytes"]),
            0,
            None,
            None,
        )
    state = read_object(state_path, maximum_bytes=64 * 1024)
    if set(state) != {"schema", "sample_count", "last_sample_hash", "last_observed_at"}:
        raise ValueError("RESILIENT_SOAK_STATE_INVALID")
    count = state.get("sample_count")
    if (
        state.get("schema") != STATE_SCHEMA
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        or (count == 0 and (state.get("last_sample_hash") is not None or state.get("last_observed_at") is not None))
        or (count > 0 and (not isinstance(state.get("last_sample_hash"), str) or not isinstance(state.get("last_observed_at"), str)))
    ):
        raise ValueError("RESILIENT_SOAK_STATE_INVALID")
    return EvidenceSnapshot(
        evidence_path,
        int(value["maximum_evidence_bytes"]),
        count,
        state.get("last_sample_hash"),
        state.get("last_observed_at"),
    )


def _new_transition_events(previous: Mapping[str, Any] | None, current: Mapping[str, Any]) -> list[dict[str, Any]]:
    transition = dict(current["transition"])
    if previous is None:
        return list(transition["event_tail"])
    prior = dict(previous["transition"])
    if transition["journal_id"] != prior["journal_id"]:
        raise ValueError("TRANSITION_JOURNAL_ID_CHANGED")
    if int(transition["high_watermark"]) < int(prior["high_watermark"]):
        raise ValueError("TRANSITION_HIGH_WATERMARK_REGRESSION")
    if transition["high_watermark"] == prior["high_watermark"]:
        if transition["last_event_hash"] != prior["last_event_hash"]:
            raise ValueError("TRANSITION_SAME_WATERMARK_CONFLICT")
        return []
    expected = int(prior["high_watermark"]) + 1
    events = [event for event in transition["event_tail"] if int(event["sequence"]) >= expected]
    if not events or int(events[0]["sequence"]) != expected or events[0]["previous_event_hash"] != prior["last_event_hash"]:
        raise ValueError("TRANSITION_EVENT_GAP")
    return events


def _new_health_events(previous: Mapping[str, Any] | None, current: Mapping[str, Any]) -> list[dict[str, Any]]:
    if previous is None:
        return []
    health = dict(current["health_transition"])
    prior = dict(previous["health_transition"])
    if health["journal_id"] != prior["journal_id"]:
        if current["host_boot_id"] == previous["host_boot_id"] or current["producer_instance_id"] == previous["producer_instance_id"]:
            raise ValueError("HEALTH_JOURNAL_ID_CHANGED_WITHOUT_BOOT")
        events = list(health["event_tail"])
        if (
            not events
            or int(events[0]["sequence"]) != 1
            or events[0]["previous_event_hash"] is not None
            or events[0]["previous_state"] is not None
        ):
            raise ValueError("HEALTH_BOOT_EVENT_GAP")
        return events
    if int(health["high_watermark"]) < int(prior["high_watermark"]):
        raise ValueError("HEALTH_HIGH_WATERMARK_REGRESSION")
    if health["high_watermark"] == prior["high_watermark"]:
        if health["last_event_hash"] != prior["last_event_hash"]:
            raise ValueError("HEALTH_SAME_WATERMARK_CONFLICT")
        if current["state"] != previous["state"] or current["reason_codes"] != previous["reason_codes"]:
            raise ValueError("HEALTH_TRANSITION_MISSING")
        return []
    expected = int(prior["high_watermark"]) + 1
    events = [event for event in health["event_tail"] if int(event["sequence"]) >= expected]
    if not events or int(events[0]["sequence"]) != expected or events[0]["previous_event_hash"] != prior["last_event_hash"]:
        raise ValueError("HEALTH_EVENT_GAP")
    if events[-1]["state"] != current["state"] or events[-1]["reason_codes"] != current["reason_codes"]:
        raise ValueError("HEALTH_EVENT_CURRENT_MISMATCH")
    return events


def evaluate_samples(
    samples: Iterable[Mapping[str, Any]],
    *,
    config: SoakConfig,
    evaluated_at: datetime | None = None,
) -> dict[str, Any]:
    value = config.value
    evaluation_time = evaluated_at.astimezone(UTC) if evaluated_at is not None else None
    blockers: set[str] = set()
    arena_contract, _ = _contracts(config)
    previous_hash: str | None = None
    previous_time: datetime | None = None
    previous_status: dict[str, dict[str, Any] | None] = {"arena": None, "dell": None}
    previous_effect: int | None = None
    episode_started: dict[str, datetime | None] = {"arena": None, "dell": None}
    maximum_episode = {"arena": 0.0, "dell": 0.0}
    ready_count = {"arena": 0, "dell": 0}
    state_sample_count: dict[str, dict[str, int]] = {"arena": {}, "dell": {}}
    maximum_gap = 0.0
    gap_count = 0
    gap_total = 0.0
    sample_gaps: list[float] = []
    gaps_over_15_seconds = 0
    gaps_over_30_seconds = 0
    gaps_over_limit_seconds = 0
    new_event_count = 0
    exact_event_count = 0
    dell_available_final = False
    sample_count = 0
    first_time: datetime | None = None
    maximum_report_age = {"arena": 0.0, "dell": 0.0}
    maximum_clock_uncertainty = {"arena": 0, "dell": 0}
    producer_rotations = {"arena": 0, "dell": 0}
    health_journal_rotations = {"arena": 0, "dell": 0}
    previous_target_identity: str | None = None
    target_identity_change_count = 0
    physical_effect_baseline: int | None = None
    physical_effect_final: int | None = None
    previous_pull_recovery: dict[str, Any] | None = None
    pull_recovery_baseline: dict[str, int] | None = None
    pull_recovery_final: dict[str, int] | None = None
    publisher_failure_event_count = {"arena": 0, "dell": 0}
    for index, raw in enumerate(samples):
        sample_count += 1
        sample = dict(raw)
        if set(sample) != {
            "schema",
            "sample_sequence",
            "observed_at",
            "previous_sample_hash",
            "cra_release_id",
            "arena_status",
            "dell_status_payload_sha256",
            "cra_pull_status",
            "cra_pull_recovery_state",
            "sample_hash",
        }:
            blockers.add(f"SAMPLE_FIELDS_INVALID:{index}")
            continue
        if (
            sample["schema"] != SAMPLE_SCHEMA
            or sample["sample_sequence"] != index + 1
            or sample["previous_sample_hash"] != previous_hash
            or sample["sample_hash"] != _sample_hash(sample)
        ):
            blockers.add(f"SAMPLE_HASH_CHAIN_INVALID:{index}")
            continue
        current_time = parse_utc(str(sample["observed_at"]))
        if first_time is None:
            first_time = current_time
        if previous_time is not None:
            gap = (current_time - previous_time).total_seconds()
            if gap <= 0:
                blockers.add(f"SAMPLE_TIME_REGRESSION:{index}")
            gap_count += 1
            gap_total += gap
            sample_gaps.append(gap)
            gaps_over_15_seconds += int(gap > 15)
            gaps_over_30_seconds += int(gap > 30)
            gaps_over_limit_seconds += int(gap > float(value["maximum_sample_gap_seconds"]))
            maximum_gap = max(maximum_gap, gap)
            if gap > float(value["maximum_sample_gap_seconds"]):
                blockers.add(f"SAMPLE_GAP_EXCEEDED:{index}")
        previous_time = current_time
        previous_hash = str(sample["sample_hash"])
        if sample["cra_release_id"] != value["cra_release_id"]:
            blockers.add(f"CRA_RELEASE_MISMATCH:{index}")
        try:
            arena = arena_contract.decode(dict(sample["arena_status"]), now=current_time)
            nested = arena["upstream_status"]
            dell = dict(nested) if isinstance(nested, dict) else None
        except (TypeError, ValueError) as error:
            blockers.add(f"SIGNED_STATUS_INVALID:{index}:{error}")
            continue
        if sample["dell_status_payload_sha256"] != (dell["payload_sha256"] if dell is not None else None):
            blockers.add(f"NESTED_DELL_DIGEST_MISMATCH:{index}")
        pull = sample["cra_pull_status"]
        if (
            not isinstance(pull, dict)
            or pull.get("status") != "READY"
            or pull.get("source_producer_instance_id") != arena["producer_instance_id"]
            or pull.get("source_producer_sequence") != arena["producer_sequence"]
        ):
            blockers.add(f"CRA_PULL_BINDING_INVALID:{index}")
        try:
            pull_recovery = _validate_pull_recovery_state(
                config,
                dict(sample["cra_pull_recovery_state"]),
                now=current_time,
            )
        except (TypeError, ValueError) as error:
            blockers.add(f"CRA_PULL_RECOVERY_STATE_INVALID:{index}:{error}")
        else:
            counters = {field: int(pull_recovery[field]) for field in PULL_RECOVERY_COUNTER_FIELDS}
            if pull_recovery_baseline is None:
                pull_recovery_baseline = counters
            pull_recovery_final = counters
            if previous_pull_recovery is not None:
                for field in PULL_RECOVERY_COUNTER_FIELDS:
                    if int(pull_recovery[field]) < int(previous_pull_recovery[field]):
                        blockers.add(f"CRA_PULL_RECOVERY_COUNTER_REGRESSION:{field}:{index}")
                if (
                    pull_recovery["event_count"] == previous_pull_recovery["event_count"]
                    and pull_recovery["last_event_hash"] != previous_pull_recovery["last_event_hash"]
                ):
                    blockers.add(f"CRA_PULL_RECOVERY_EVENT_CONFLICT:{index}")
            if pull_recovery["current_state"] != "READY":
                blockers.add(f"CRA_PULL_RECOVERY_NOT_READY:{index}")
            if int(pull_recovery["terminal_episode_count"]) > 0:
                blockers.add("CRA_PULL_TERMINAL_EPISODE_OBSERVED")
            if int(pull_recovery["maximum_recovery_duration_ms"]) > int(float(value["maximum_recovery_episode_seconds"]) * 1000):
                blockers.add("CRA_PULL_RECOVERY_EPISODE_TOO_LONG")
            previous_pull_recovery = pull_recovery
        new_events_by_host: dict[str, list[dict[str, Any]]] = {}
        statuses: list[tuple[str, dict[str, Any]]] = [("arena", arena)]
        if dell is not None:
            statuses.append(("dell", dell))
            dell_available_final = True
        else:
            dell_available_final = False
            state_sample_count["dell"]["UNAVAILABLE"] = state_sample_count["dell"].get("UNAVAILABLE", 0) + 1
            if episode_started["dell"] is None:
                episode_started["dell"] = current_time
        for name, status in statuses:
            state_name = str(status["state"])
            state_sample_count[name][state_name] = state_sample_count[name].get(state_name, 0) + 1
            prior = previous_status[name]
            if prior is not None:
                if status["identity"] != prior["identity"]:
                    blockers.add(f"{name.upper()}_IDENTITY_CHANGED:{index}")
                if status["producer_instance_id"] == prior["producer_instance_id"]:
                    if status["host_boot_id"] != prior["host_boot_id"]:
                        blockers.add(f"{name.upper()}_BOOT_CHANGED_WITHOUT_INSTANCE:{index}")
                    sequence = int(status["producer_sequence"])
                    prior_sequence = int(prior["producer_sequence"])
                    if sequence < prior_sequence:
                        blockers.add(f"{name.upper()}_SEQUENCE_REGRESSION:{index}")
                    elif sequence == prior_sequence and status["payload_sha256"] != prior["payload_sha256"]:
                        blockers.add(f"{name.upper()}_SEQUENCE_CONFLICT:{index}")
                elif status["host_boot_id"] == prior["host_boot_id"]:
                    blockers.add(f"{name.upper()}_INSTANCE_CHANGED_WITHOUT_BOOT:{index}")
                else:
                    producer_rotations[name] += 1
                if status["health_transition"]["journal_id"] != prior["health_transition"]["journal_id"]:
                    health_journal_rotations[name] += 1
            try:
                events = _new_transition_events(prior, status)
            except ValueError as error:
                blockers.add(f"{name.upper()}_{error}:{index}")
                events = []
            try:
                health_events = _new_health_events(prior, status)
            except ValueError as error:
                blockers.add(f"{name.upper()}_{error}:{index}")
                health_events = []
            new_event_count += len(events)
            exact_event_count += sum(event["causal_state"] == "EXACT" for event in events)
            failure_scan = health_events if prior is not None else list(status["health_transition"]["event_tail"])
            publisher_failure_event_count[name] += sum("PUBLISHER_INVOCATION_FAILED" in event["reason_codes"] for event in failure_scan)
            if publisher_failure_event_count[name] > 0:
                blockers.add(f"{name.upper()}_PUBLISHER_INVOCATION_FAILED")
            new_events_by_host[name] = events
            uncertainty = status["clock"]["uncertainty_ms"]
            if uncertainty is None or int(uncertainty) > int(value["maximum_clock_uncertainty_ms"]):
                blockers.add(f"{name.upper()}_CLOCK_UNCERTAIN:{index}")
            if uncertainty is not None:
                maximum_clock_uncertainty[name] = max(maximum_clock_uncertainty[name], int(uncertainty))
            report_age = (current_time - parse_utc(str(status["observed_at"]))).total_seconds()
            maximum_report_age[name] = max(maximum_report_age[name], report_age)
            if report_age < -5 or report_age > float(value["maximum_report_age_seconds"]):
                blockers.add(f"{name.upper()}_REPORT_AGE_INVALID:{index}")
            if status["state"] == "SOURCE_UNREACHABLE":
                blockers.add(f"{name.upper()}_SOURCE_UNREACHABLE:{index}")
            if status["state"] == "READY":
                ready_count[name] += 1
            health_points = (
                [
                    (
                        parse_utc(str(event["observed_at"])),
                        str(event["state"]),
                        int(event["sequence"]),
                    )
                    for event in health_events
                ]
                if prior is not None
                else [(parse_utc(str(status["observed_at"])), str(status["state"]), 0)]
            )
            for health_time, health_state, health_sequence in health_points:
                if health_state == "SOURCE_UNREACHABLE":
                    blockers.add(f"{name.upper()}_SOURCE_UNREACHABLE_HISTORY:{health_sequence}")
                if health_state == "READY":
                    started = episode_started[name]
                    if started is not None:
                        maximum_episode[name] = max(
                            maximum_episode[name],
                            (health_time - started).total_seconds(),
                        )
                        episode_started[name] = None
                elif health_state in DEGRADED_STATES and episode_started[name] is None:
                    episode_started[name] = health_time
            previous_status[name] = status
            if name == "dell":
                current_target = status.get("current")
                target_identity = (
                    str(current_target.get("target_identity_sha256"))
                    if isinstance(current_target, dict) and current_target.get("target_identity_sha256") is not None
                    else None
                )
                if previous_target_identity is not None and target_identity is not None and target_identity != previous_target_identity:
                    target_identity_change_count += 1
                previous_target_identity = target_identity
        effect = dell["physical_effect_count"] if dell is not None else None
        if effect is not None:
            effect_value = int(effect)
            if physical_effect_baseline is None:
                physical_effect_baseline = effect_value
            physical_effect_final = effect_value
            if previous_effect is not None:
                if effect_value < previous_effect:
                    blockers.add(f"PHYSICAL_EFFECT_COUNT_REGRESSION:{index}")
                elif effect_value > previous_effect and (
                    effect_value - previous_effect != 1
                    or not any(event["causal_state"] == "EXACT" for event in new_events_by_host.get("dell", []))
                ):
                    blockers.add(f"PHYSICAL_EFFECT_WITHOUT_EXACT_TRANSITION:{index}")
            previous_effect = effect_value
    duration = 0.0
    if first_time is not None and previous_time is not None:
        duration = (previous_time - first_time).total_seconds()
    final_sample_age: float | None = None
    if previous_time is not None and evaluation_time is not None:
        final_sample_age = (evaluation_time - previous_time).total_seconds()
        if final_sample_age < -5:
            blockers.add("FINAL_SAMPLE_FROM_FUTURE")
        elif final_sample_age > float(value["maximum_sample_gap_seconds"]):
            blockers.add("FINAL_SAMPLE_STALE")
    if sample_count < 2:
        blockers.add("SAMPLE_COUNT_INSUFFICIENT")
    if duration < float(value["minimum_duration_seconds"]):
        blockers.add("SOAK_DURATION_INSUFFICIENT")
    for name in ("arena", "dell"):
        started = episode_started[name]
        final = previous_status[name]
        if started is not None and final is not None:
            maximum_episode[name] = max(
                maximum_episode[name],
                (parse_utc(str(final["observed_at"])) - started).total_seconds(),
            )
        if maximum_episode[name] > float(value["maximum_recovery_episode_seconds"]):
            blockers.add(f"{name.upper()}_RECOVERY_EPISODE_TOO_LONG")
        if final is None or final["state"] != "READY" or (name == "dell" and not dell_available_final):
            blockers.add(f"{name.upper()}_FINAL_STATE_NOT_READY")
        if final is not None and final["transition"]["open_episode_count"] != 0:
            blockers.add(f"{name.upper()}_UNRESOLVED_TRANSITION")
    gap_p95 = 0.0
    if sample_gaps:
        ordered_gaps = sorted(sample_gaps)
        position = 0.95 * (len(ordered_gaps) - 1)
        lower = math.floor(position)
        upper = math.ceil(position)
        gap_p95 = ordered_gaps[lower] + (ordered_gaps[upper] - ordered_gaps[lower]) * (position - lower)
    return {
        "schema": GATE_SCHEMA,
        "status": _gate_status(blockers),
        "eligible": not blockers,
        "sample_count": sample_count,
        "duration_seconds": round(duration, 3),
        "final_sample_age_seconds": round(final_sample_age, 3) if final_sample_age is not None else None,
        "maximum_sample_gap_seconds": round(maximum_gap, 3),
        "sample_gap_summary": {
            "average_seconds": round(gap_total / gap_count, 3) if gap_count else 0.0,
            "p95_seconds": round(gap_p95, 3),
            "count": gap_count,
            "over_15_seconds": gaps_over_15_seconds,
            "over_30_seconds": gaps_over_30_seconds,
            "over_maximum_seconds": gaps_over_limit_seconds,
        },
        "maximum_recovery_episode_seconds": {name: round(item, 3) for name, item in maximum_episode.items()},
        "maximum_report_age_seconds_observed": {name: round(item, 3) for name, item in maximum_report_age.items()},
        "maximum_clock_uncertainty_ms_observed": maximum_clock_uncertainty,
        "ready_sample_count": ready_count,
        "state_sample_count": state_sample_count,
        "transition_event_count": new_event_count,
        "exact_transition_event_count": exact_event_count,
        "producer_rotation_count": producer_rotations,
        "health_journal_rotation_count": health_journal_rotations,
        "target_identity_change_count": target_identity_change_count,
        "physical_effect_count_baseline": physical_effect_baseline,
        "physical_effect_count_final": physical_effect_final,
        "physical_effect_count_delta": (
            physical_effect_final - physical_effect_baseline
            if physical_effect_final is not None and physical_effect_baseline is not None
            else None
        ),
        "publisher_failure_event_count": publisher_failure_event_count,
        "cra_pull_recovery_counters_baseline": pull_recovery_baseline,
        "cra_pull_recovery_counters_final": pull_recovery_final,
        "cra_pull_recovery_counters_delta": (
            {field: pull_recovery_final[field] - pull_recovery_baseline[field] for field in PULL_RECOVERY_COUNTER_FIELDS}
            if pull_recovery_final is not None and pull_recovery_baseline is not None
            else None
        ),
        "blockers": sorted(blockers),
        "control_capability_count": 0,
        "evaluated_at": isoformat_utc(evaluation_time or datetime.now(UTC)),
    }


def add_evidence_capacity_assessment(
    result: dict[str, Any],
    *,
    snapshot: EvidenceSnapshot,
    config: SoakConfig,
) -> None:
    count = int(result["sample_count"])
    duration = float(result["duration_seconds"])
    limit = int(config.value["maximum_evidence_bytes"])
    minimum_duration = float(config.value["minimum_duration_seconds"])
    average_sample_bytes = round(snapshot.bytes_read / count, 3) if count else None
    projected_bytes: int | None = None
    projected_with_margin: int | None = None
    seconds_to_capacity: float | None = None
    assessment = "INSUFFICIENT_DATA"
    if count >= 2 and duration > 0 and average_sample_bytes is not None:
        samples_per_second = (count - 1) / duration
        projected_samples = 1 + minimum_duration * samples_per_second
        projected_bytes = math.ceil(average_sample_bytes * projected_samples)
        projected_with_margin = math.ceil(projected_bytes * EVIDENCE_CAPACITY_SAFETY_FACTOR)
        bytes_per_second = average_sample_bytes * samples_per_second
        remaining = max(0, limit - snapshot.file_size_at_open)
        seconds_to_capacity = round(remaining / bytes_per_second, 3) if bytes_per_second > 0 else None
        assessment = "SUFFICIENT" if projected_with_margin <= limit else "INSUFFICIENT"
    capacity = {
        "assessment": assessment,
        "average_sample_bytes": average_sample_bytes,
        "evidence_bytes_at_snapshot": snapshot.file_size_at_open,
        "evidence_bytes_verified": snapshot.bytes_read,
        "evidence_limit_bytes": limit,
        "evidence_utilization_ratio": round(snapshot.file_size_at_open / limit, 6),
        "projected_bytes_at_minimum_duration": projected_bytes,
        "projected_bytes_with_safety_margin": projected_with_margin,
        "safety_factor": EVIDENCE_CAPACITY_SAFETY_FACTOR,
        "seconds_to_capacity_at_observed_rate": seconds_to_capacity,
    }
    result["evidence_capacity"] = capacity
    blockers = set(result["blockers"])
    if assessment == "INSUFFICIENT":
        blockers.add("EVIDENCE_CAPACITY_INSUFFICIENT")
    result["blockers"] = sorted(blockers)
    result["eligible"] = not blockers
    result["status"] = _gate_status(blockers)


def watchdog_path(config: SoakConfig) -> Path:
    return Path(config.value["gate_file"]).with_name("resilient-status-soak-watchdog.json")


def evaluate_watchdog(config: SoakConfig, *, now: datetime | None = None) -> dict[str, Any]:
    value = config.value
    current = (now or datetime.now(UTC)).astimezone(UTC)
    blockers: set[str] = set()
    pull_recovery: dict[str, Any] | None = None
    pull_recovery_age: float | None = None
    try:
        state = _state(
            Path(value["state_file"]),
            Path(value["evidence_file"]),
            maximum_bytes=int(value["maximum_evidence_bytes"]),
        )
    except (OSError, TypeError, ValueError):
        state = {
            "schema": STATE_SCHEMA,
            "sample_count": 0,
            "last_sample_hash": None,
            "last_observed_at": None,
        }
        blockers.add("COLLECTOR_STATE_INVALID")
    last_observed = state["last_observed_at"]
    collector_age: float | None = None
    if last_observed is None:
        blockers.add("COLLECTOR_SAMPLE_MISSING")
    else:
        collector_age = (current - parse_utc(str(last_observed))).total_seconds()
        if collector_age < -5:
            blockers.add("COLLECTOR_SAMPLE_FROM_FUTURE")
        elif collector_age > float(value["maximum_sample_gap_seconds"]):
            blockers.add("COLLECTOR_STATE_STALE")
    gate_path = Path(value["gate_file"])
    gate: dict[str, Any] | None = None
    gate_age: float | None = None
    if not gate_path.exists():
        blockers.add("FORMAL_GATE_MISSING")
    else:
        try:
            gate = read_object(gate_path, maximum_bytes=256 * 1024)
            if gate.get("schema") != GATE_SCHEMA:
                blockers.add("FORMAL_GATE_SCHEMA_INVALID")
            else:
                gate_age = (current - parse_utc(str(gate.get("evaluated_at", "")))).total_seconds()
                if gate_age < -5:
                    blockers.add("FORMAL_GATE_FROM_FUTURE")
                elif gate_age > MAXIMUM_GATE_AGE_SECONDS:
                    blockers.add("FORMAL_GATE_STALE")
                capacity = gate.get("evidence_capacity")
                if isinstance(capacity, dict) and capacity.get("assessment") == "INSUFFICIENT":
                    blockers.add("EVIDENCE_CAPACITY_INSUFFICIENT")
        except (OSError, TypeError, ValueError):
            gate = None
            blockers.add("FORMAL_GATE_INVALID")
    evidence_path = Path(value["evidence_file"])
    evidence_bytes = evidence_path.stat().st_size if evidence_path.exists() else 0
    evidence_limit = int(value["maximum_evidence_bytes"])
    utilization = evidence_bytes / evidence_limit
    if utilization >= 0.9:
        blockers.add("EVIDENCE_CAPACITY_CRITICAL")
    try:
        pull_recovery = _pull_recovery_state(config, now=current)
        pull_recovery_age = (current - parse_utc(str(pull_recovery["updated_at"]))).total_seconds()
        if pull_recovery["current_state"] != "READY":
            blockers.add("CRA_PULL_RECOVERY_NOT_READY")
        if int(pull_recovery["terminal_episode_count"]) > 0:
            blockers.add("CRA_PULL_TERMINAL_EPISODE_OBSERVED")
        if int(pull_recovery["maximum_recovery_duration_ms"]) > int(float(value["maximum_recovery_episode_seconds"]) * 1000):
            blockers.add("CRA_PULL_RECOVERY_EPISODE_TOO_LONG")
    except (OSError, TypeError, ValueError):
        blockers.add("CRA_PULL_RECOVERY_STATE_INVALID")
    filesystem = os.statvfs(evidence_path.parent)
    filesystem_free_bytes = filesystem.f_bavail * filesystem.f_frsize
    filesystem_free_inodes = filesystem.f_favail
    if filesystem_free_bytes < max(MINIMUM_FILESYSTEM_FREE_BYTES, evidence_limit):
        blockers.add("FILESYSTEM_HEADROOM_LOW")
    if filesystem_free_inodes < MINIMUM_FILESYSTEM_FREE_INODES:
        blockers.add("FILESYSTEM_INODE_HEADROOM_LOW")
    return {
        "schema": WATCHDOG_SCHEMA,
        "status": "READY" if not blockers else "AT_RISK",
        "blockers": sorted(blockers),
        "observed_at": isoformat_utc(current),
        "cra_release_id": value["cra_release_id"],
        "collector_sample_count": state["sample_count"],
        "collector_last_observed_at": last_observed,
        "collector_age_seconds": round(collector_age, 3) if collector_age is not None else None,
        "formal_gate_status": gate.get("status") if gate is not None else None,
        "formal_gate_eligible": gate.get("eligible") if gate is not None else None,
        "formal_gate_blockers": gate.get("blockers") if gate is not None else None,
        "formal_gate_evaluated_at": gate.get("evaluated_at") if gate is not None else None,
        "formal_gate_age_seconds": round(gate_age, 3) if gate_age is not None else None,
        "cra_pull_recovery_state": (pull_recovery.get("current_state") if pull_recovery is not None else None),
        "cra_pull_recovery_updated_at": (pull_recovery.get("updated_at") if pull_recovery is not None else None),
        "cra_pull_recovery_age_seconds": (round(pull_recovery_age, 3) if pull_recovery_age is not None else None),
        "cra_pull_recovery_counters": (
            {field: int(pull_recovery[field]) for field in PULL_RECOVERY_COUNTER_FIELDS} if pull_recovery is not None else None
        ),
        "evidence_bytes": evidence_bytes,
        "evidence_limit_bytes": evidence_limit,
        "evidence_utilization_ratio": round(utilization, 6),
        "filesystem_free_bytes": filesystem_free_bytes,
        "filesystem_free_inodes": filesystem_free_inodes,
        "control_capability_count": 0,
        "physical_effect_count": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect or evaluate the resilient signed-status soak")
    parser.add_argument("--config", type=Path, required=True)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--evaluate", action="store_true")
    action.add_argument("--watchdog", action="store_true")
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = SoakConfig.load(args.config)
    if args.check_config:
        _contracts(config)
        print(json.dumps({"config": "VALID", "schema": CONFIG_SCHEMA}, sort_keys=True))
        return
    if args.watchdog:
        result = evaluate_watchdog(config)
        atomic_write_json(watchdog_path(config), result, mode=0o640)
    elif args.evaluate:
        started = time.monotonic()
        snapshot = evidence_snapshot(config)
        result = evaluate_samples(snapshot, config=config, evaluated_at=datetime.now(UTC))
        add_evidence_capacity_assessment(result, snapshot=snapshot, config=config)
        result["evaluation_elapsed_seconds"] = round(time.monotonic() - started, 6)
        atomic_write_json(Path(config.value["gate_file"]), result, mode=0o640)
    else:
        sample = collect_sample(config)
        result = {
            "schema": SAMPLE_SCHEMA,
            "sample_sequence": sample["sample_sequence"],
            "observed_at": sample["observed_at"],
            "arena_state": sample["arena_status"]["state"],
            "dell_state": (
                sample["arena_status"]["upstream_status"]["state"]
                if isinstance(sample["arena_status"]["upstream_status"], dict)
                else "UNAVAILABLE"
            ),
            "control_capability_count": 0,
        }
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
