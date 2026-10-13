"""Recovery-aware seven-day soak: local inbox -> durable samples -> pure gate.

This is a new epoch/schema, not a reinterpretation of resilient-soak v1/v2.
Unavailable inputs are recorded; invalid signatures/history never become
successful recovery. No public/Pi observation can authorize an action.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import heapq
import json
import math
import os
import re
import stat
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from itertools import groupby
from pathlib import Path
from typing import Any, cast

from cra_dell_recovery.canonical import KeyRing, canonical_json
from cra_dell_recovery.errors import RecoveryControlError, SignatureValidationError
from cra_dell_recovery.recovery_history import history_hash, validate_effect_history, validate_policy
from cra_dell_recovery.time import isoformat_utc

from .append_only_jsonl import _append_line, _complete_pending_line, _last_committed_line, _last_line, _serialized_line
from .host_status import atomic_write_json
from .host_status_pull import _public_key
from .operator_status import classify_gate, operator_report
from .recovery_facts import (
    ACTION_TABLES,
    ROLES,
    AdmissionCache,
    HostBinding,
    admit_facts,
    nonnegative_integer,
    source_fresh,
    strict_json_object,
    strict_object,
    timestamp,
)
from .recovery_live import advance, observation
from .recovery_window import Health, RecoveryWindow

CONFIG_SCHEMA = "cra.recovery_soak_config.v2"
HISTORY_CONFIG_SCHEMA = "cra.recovery_soak_config.v3"
SAMPLE_SCHEMA = "cra.recovery_soak_sample.v5"
STATE_SCHEMA = "cra.recovery_soak_state.v6"
GATE_SCHEMA = "cra.recovery_soak_gate.v4"
HISTORY_GATE_SCHEMA = "cra.recovery_soak_gate.v9"
WATCHDOG_SCHEMA = "cra.recovery_soak_watchdog.v6"
TRANSPORT_ORDER_UNKNOWN = "TRANSPORT_TARGET_ORDER_UNVERIFIED"
LIVE_UNKNOWN_RESOLUTION_SCHEMA = "cra.live_unknown_resolution.v1"
MAXIMUM_BYTES = 2 * 1024 * 1024 * 1024
_POSITIVE_SAFETY_BLOCKERS = frozenset({
    "CRA_NO_ACTION_SAFETY_VIOLATED", "DELL_EFFECT_SAFETY_VIOLATED", "NO_ACTION_PHYSICAL_EFFECT_DETECTED",
    "BOUNDED_TERMINATION_NOT_ACTIVE",
})

_HISTORY_MODEL_ERRORS = frozenset(
    {
        "RECOVERY_HISTORY_FILE_CHANGED_DURING_READ",
        "RECOVERY_HISTORY_INTERVAL_LIMIT",
        "RECOVERY_HISTORY_RAW_PREFIX_CHANGED",
        "RECOVERY_HISTORY_RECONSTRUCTION_LIMIT",
        "RECOVERY_HISTORY_REPLAY_UNTRUSTED",
    }
)
_HISTORY_EVIDENCE_ERRORS = frozenset(
    {
        "RECOVERY_HISTORY_BOUNDARY_CONFLICT",
        "RECOVERY_HISTORY_FILE_INVALID",
        "RECOVERY_HISTORY_FRAME_INVALID",
        "RECOVERY_HISTORY_GAP_BOUNDARY_MISSING",
        "RECOVERY_HISTORY_GAP_INCOMPLETE",
        "RECOVERY_HISTORY_LATE_PACKET_EXPIRED",
        "RECOVERY_HISTORY_PRODUCER_ROTATED",
        "RECOVERY_HISTORY_SEQUENCE_OR_ROLE_INVALID",
        "RECOVERY_HISTORY_SIGNATURE_INVALID",
        "RECOVERY_HISTORY_SIGNED_PACKET_INVALID",
        "RECOVERY_HISTORY_TIME_OUTSIDE_RAW_INTERVAL",
        "RECOVERY_HISTORY_TIME_REGRESSION",
    }
)
_SAFE_FAILURE_CODE = re.compile(r"[A-Z][A-Z0-9_]{2,95}\Z")


class _HistoryRoleFailures(ValueError):
    """Bounded diagnostics after inspecting each role's valid interval prefix."""

    def __init__(self, failures: list[dict[str, Any]]) -> None:
        self.failures = failures
        self.primary = next(
            (item for item in failures if item["harness_classification"] == "HARNESS_FAILURE"), failures[0],
        )
        super().__init__(self.primary["reason_code"])


def _historical_failure(error: Exception) -> tuple[str, str]:
    """Return a non-secret reason and failure owner; never serialize exception text."""
    if isinstance(error, _HistoryRoleFailures):
        return error.primary["reason_code"], error.primary["harness_classification"]
    if isinstance(error, OSError):
        return "RECOVERY_HISTORY_READ_FAILED", "MISSING_EVIDENCE"
    code = str(error)
    if code in _HISTORY_MODEL_ERRORS:
        return code, "HARNESS_FAILURE"
    if code in _HISTORY_EVIDENCE_ERRORS or code.startswith(
        ("RECOVERY_ENVELOPE_", "RECOVERY_FACT_", "RECOVERY_SOURCE_", "OWNER_LIFECYCLE_", "RUNTIME_EVIDENCE_")
    ):
        return code if code in _HISTORY_EVIDENCE_ERRORS else "RECOVERY_HISTORY_SIGNED_PACKET_INVALID", "MISSING_EVIDENCE"
    return "RECOVERY_HISTORY_EVALUATION_FAILED", "HARNESS_FAILURE"


def _evaluation_failure_code(error: Exception) -> str:
    """Expose only stable machine codes, never arbitrary exception payloads."""
    code = str(error)
    if _SAFE_FAILURE_CODE.fullmatch(code) and code.startswith(("RECOVERY_SOAK_", "RECOVERY_HISTORY_")):
        return code
    return "EVIDENCE_EVALUATION_FAILED"
MAXIMUM_FRAME_BYTES = 128 * 1024
MAXIMUM_HISTORY_PACKETS_PER_INTERVAL = 4096
MAXIMUM_HISTORY_PACKETS_PER_ROLE = 1_000_000
MAXIMUM_RECONSTRUCTED_SAMPLES = 1_000_000
CAPACITY_PROJECTION_WARMUP_SECONDS = 300


def digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json({"frame": {k: v for k, v in value.items() if k != "sample_hash"}})).hexdigest()


def gate_schema(config: Config) -> str:
    return HISTORY_GATE_SCHEMA if config.value.get("schema") == HISTORY_CONFIG_SCHEMA else GATE_SCHEMA


@dataclass
class Config:
    value: dict[str, Any]
    bindings: dict[str, HostBinding]
    keys: KeyRing
    key_fingerprints: dict[str, str] = field(default_factory=dict)

    @property
    def identity(self) -> str:
        return digest({"config": self.value, "key_fingerprints": self.key_fingerprints})

    @classmethod
    def load(cls, path: Path) -> Config:
        value = strict_object(path, maximum_bytes=128 * 1024)
        fields = {
            "schema",
            "epoch_id",
            "hosts",
            "evidence_file",
            "state_file",
            "gate_file",
            "maximum_evidence_bytes",
            "minimum_duration_seconds",
            "maximum_sample_gap_seconds",
            "recovery_deadline_seconds",
            "effect_history_policy",
        }
        history_enabled = value.get("schema") == HISTORY_CONFIG_SCHEMA
        if (
            not fields
            <= set(value)
            <= fields
            | {
                "require_independent_effect_evidence",
                "require_owner_lifecycle_evidence",
                "require_network_episode_evidence",
                "require_rtmps_episode_evidence",
            }
            | ({"history_files"} if history_enabled else set())
            or value.get("schema") not in (CONFIG_SCHEMA, HISTORY_CONFIG_SCHEMA)
            or ("require_independent_effect_evidence" in value and value["require_independent_effect_evidence"] is not True)
            or (
                "require_owner_lifecycle_evidence" in value
                and (value["require_owner_lifecycle_evidence"] is not True or value.get("require_independent_effect_evidence") is not True)
            )
            or ("require_network_episode_evidence" in value and value["require_network_episode_evidence"] is not True)
            or ("require_rtmps_episode_evidence" in value and value["require_rtmps_episode_evidence"] is not True)
        ):
            raise ValueError("RECOVERY_SOAK_CONFIG_INVALID")
        validate_policy(value["effect_history_policy"])
        if (
            not isinstance(value["epoch_id"], str)
            or not 1 <= len(value["epoch_id"]) <= 128
            or value["minimum_duration_seconds"] != 604800
            or value["maximum_sample_gap_seconds"] != 45
            or value["recovery_deadline_seconds"] != 600
        ):
            raise ValueError("RECOVERY_SOAK_SEVEN_DAY_600_SECOND_CONTRACT_REQUIRED")
        limit = value["maximum_evidence_bytes"]
        if not nonnegative_integer(limit) or not 1024 * 1024 <= limit <= MAXIMUM_BYTES:
            raise ValueError("RECOVERY_SOAK_CAPACITY_INVALID")
        paths: list[str] = []
        for name in ("evidence_file", "state_file", "gate_file"):
            item = value[name]
            if not isinstance(item, str) or not Path(item).is_absolute():
                raise ValueError("RECOVERY_SOAK_PATH_INVALID")
            paths.append(str(Path(item).resolve()))
        paths.extend(str(Path(value["state_file"]).with_suffix(suffix).resolve()) for suffix in (".lock", ".watchdog.json"))
        if history_enabled:
            histories = value.get("history_files")
            if not isinstance(histories, dict) or set(histories) != {"dell", "arena", "cra"}:
                raise ValueError("RECOVERY_SOAK_HISTORY_ROLES_INVALID")
            if any(not isinstance(path, str) or not Path(path).is_absolute() for path in histories.values()):
                raise ValueError("RECOVERY_SOAK_HISTORY_PATH_INVALID")
            paths.extend(str(Path(path).resolve()) for path in histories.values())
        if len(set(paths)) != len(paths):
            raise ValueError("RECOVERY_SOAK_PATH_COLLISION")
        if not isinstance(value["hosts"], dict) or set(value["hosts"]) != set(ROLES):
            raise ValueError("RECOVERY_SOAK_HOSTS_INVALID")
        bindings: dict[str, HostBinding] = {}
        keys = {}
        fingerprints = {}
        for role, host in value["hosts"].items():
            # Pi is advisory-only. An explicitly unconfigured publisher is
            # UNKNOWN, not a fabricated release/key and never a core bypass.
            if role == "raspi" and host is None:
                continue
            if not isinstance(host, dict) or set(host) != {
                "host_id",
                "release_id",
                "source_commit",
                "stream_id",
                "key_id",
                "public_key_file",
                "inbox_file",
            }:
                raise ValueError("RECOVERY_SOAK_HOST_CONFIG_INVALID")
            binding = HostBinding(role=role, **{k: host[k] for k in ("host_id", "release_id", "source_commit", "stream_id", "key_id")})
            if binding.key_id in keys:
                raise ValueError("RECOVERY_SOAK_HOST_KEYS_MUST_BE_DISTINCT")
            for name in ("public_key_file", "inbox_file"):
                if not isinstance(host[name], str) or not Path(host[name]).is_absolute():
                    raise ValueError("RECOVERY_SOAK_HOST_PATH_INVALID")
                if str(Path(host[name]).resolve()) in paths:
                    raise ValueError("RECOVERY_SOAK_INPUT_OUTPUT_COLLISION")
            keys[binding.key_id] = _public_key(Path(host["public_key_file"]))
            fingerprints[binding.key_id] = hashlib.sha256(keys[binding.key_id].public_bytes_raw()).hexdigest()
            bindings[role] = binding
        if len(set(fingerprints.values())) != len(fingerprints):
            raise ValueError("RECOVERY_SOAK_HOST_KEYS_MUST_BE_DISTINCT")
        if len({bindings[r].stream_id for r in ("dell", "arena", "cra")}) != 1:
            raise ValueError("RECOVERY_SOAK_STREAM_BINDING_MISMATCH")
        return cls(value, bindings, KeyRing(keys), fingerprints)


def _initial_state(config: Config) -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "epoch_id": config.value["epoch_id"],
        "config_sha256": config.identity,
        "sample_count": 0,
        "last_sample_hash": None,
        "last_observed_at": None,
        "verified_bytes": 0,
        "live_recovery": None,
        "pending_sample": None,
    }


def _head(config: Config) -> dict[str, Any]:
    state_path = Path(config.value["state_file"])
    evidence_path = Path(config.value["evidence_file"])
    if not state_path.exists():
        if evidence_path.exists() and evidence_path.stat().st_size:
            raise ValueError("RECOVERY_SOAK_STATE_MISSING")
        return _initial_state(config)
    state = strict_object(state_path, maximum_bytes=256 * 1024)
    if (
        set(state) != set(_initial_state(config))
        or state.get("schema") != STATE_SCHEMA
        or state.get("epoch_id") != config.value["epoch_id"]
        or state.get("config_sha256") != config.identity
        or not nonnegative_integer(state.get("sample_count"))
        or not nonnegative_integer(state.get("verified_bytes"))
    ):
        raise ValueError("RECOVERY_SOAK_STATE_INVALID_OR_EPOCH_CHANGED")
    pending = state["pending_sample"]
    if pending is not None and (
        not isinstance(pending, dict)
        or len(canonical_json(pending)) > MAXIMUM_FRAME_BYTES
        or pending.get("schema") != SAMPLE_SCHEMA
        or pending.get("epoch_id") != config.value["epoch_id"]
        or pending.get("config_sha256") != config.identity
        or pending.get("sample_sequence") != state["sample_count"] + 1
        or pending.get("previous_sample_hash") != state["last_sample_hash"]
        or pending.get("sample_hash") != digest(pending)
    ):
        raise ValueError("RECOVERY_SOAK_PENDING_SAMPLE_INVALID")
    return state


def _next_state(config: Config, sample: dict[str, Any], size: int) -> dict[str, Any]:
    return {
        **_initial_state(config),
        "sample_count": sample["sample_sequence"],
        "last_sample_hash": sample["sample_hash"],
        "last_observed_at": sample["observed_at"],
        "verified_bytes": size,
        "live_recovery": sample["live_recovery"],
        "pending_sample": None,
    }


def collect(config: Config, *, now: datetime | None = None) -> dict[str, Any]:
    state_path = Path(config.value["state_file"])
    evidence_path = Path(config.value["evidence_file"])
    state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(state_path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        head = _head(config)
        pending = head["pending_sample"]
        if pending is not None:
            committed_tail = _last_committed_line(
                evidence_path, committed_bytes=head["verified_bytes"],
                maximum_bytes=int(config.value["maximum_evidence_bytes"]),
            )
            if (committed_tail is None) != (head["sample_count"] == 0) or (
                committed_tail is not None and (
                    committed_tail.get("sample_hash") != digest(committed_tail)
                    or committed_tail.get("sample_hash") != head["last_sample_hash"]
                    or committed_tail.get("sample_sequence") != head["sample_count"]
                    or committed_tail.get("epoch_id") != config.value["epoch_id"]
                    or committed_tail.get("config_sha256") != config.identity
                    or committed_tail.get("observed_at") != head["last_observed_at"]
                    or committed_tail.get("live_recovery") != head["live_recovery"]
                )
            ):
                raise ValueError("RECOVERY_SOAK_PENDING_PREFIX_CONFLICT")
            expected = _serialized_line(pending)
            current_size = evidence_path.stat().st_size if evidence_path.exists() else 0
            if current_size > head["verified_bytes"] + len(expected):
                raise ValueError("RECOVERY_SOAK_PENDING_PREFIX_CONFLICT")
            if current_size == head["verified_bytes"]:
                _append_line(evidence_path, pending, maximum_bytes=int(config.value["maximum_evidence_bytes"]))
            elif current_size > head["verified_bytes"]:
                if current_size < head["verified_bytes"] + len(expected):
                    _complete_pending_line(
                        evidence_path, pending, committed_bytes=head["verified_bytes"],
                        maximum_bytes=int(config.value["maximum_evidence_bytes"]),
                    )
            else:
                raise ValueError("RECOVERY_SOAK_PENDING_PREFIX_CONFLICT")
            frame_descriptor = os.open(evidence_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                frame_metadata = os.fstat(frame_descriptor)
                if (
                    not stat.S_ISREG(frame_metadata.st_mode)
                    or frame_metadata.st_size != head["verified_bytes"] + len(expected)
                    or os.pread(frame_descriptor, len(expected), head["verified_bytes"]) != expected
                ):
                    raise ValueError("RECOVERY_SOAK_PENDING_PREFIX_CONFLICT")
            finally:
                os.close(frame_descriptor)
            head = _next_state(config, pending, evidence_path.stat().st_size)
            atomic_write_json(state_path, head)
        tail = _last_line(evidence_path, maximum_bytes=int(config.value["maximum_evidence_bytes"]))
        if tail is not None:
            if (
                tail.get("sample_hash") != digest(tail)
                or tail.get("epoch_id") != config.value["epoch_id"]
                or tail.get("config_sha256") != config.identity
            ):
                raise ValueError("RECOVERY_SOAK_TAIL_INVALID")
            size = evidence_path.stat().st_size
            # One append may have committed before the atomic state replacement.
            if tail.get("sample_sequence") == head["sample_count"] + 1 and tail.get("previous_sample_hash") == head["last_sample_hash"]:
                head = _next_state(config, tail, size)
            if (
                tail.get("sample_sequence") != head["sample_count"]
                or tail.get("sample_hash") != head["last_sample_hash"]
                or size != head["verified_bytes"]
                or tail.get("live_recovery") != head["live_recovery"]
                or tail.get("observed_at") != head["last_observed_at"]
            ):
                raise ValueError("RECOVERY_SOAK_STATE_EVIDENCE_FORK")
        elif head["sample_count"] != 0:
            raise ValueError("RECOVERY_SOAK_EVIDENCE_MISSING")
        inputs: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for role in ROLES:
            if role == "raspi" and config.value["hosts"][role] is None:
                inputs[role] = None
                errors[role] = "ADVISORY_NOT_CONFIGURED"
                continue
            try:
                inputs[role] = strict_object(Path(config.value["hosts"][role]["inbox_file"]), maximum_bytes=24 * 1024)
            except OSError:
                inputs[role] = None
                errors[role] = "INPUT_UNAVAILABLE"
            except ValueError:
                inputs[role] = None
                errors[role] = "INPUT_INVALID"
        # The read-after time prevents normal atomic producer updates from
        # being mislabeled future relative to a collector cycle-start time.
        # Persisted timestamps have millisecond precision. The live checkpoint
        # must use that exact value too, otherwise replay differs only because
        # the in-memory datetime retained sub-millisecond precision.
        current = timestamp(isoformat_utc(now or datetime.now(UTC)))
        validate_policy(config.value.get("effect_history_policy"), now=current)
        if head["last_observed_at"] is not None and current <= timestamp(head["last_observed_at"]):
            raise ValueError("RECOVERY_SOAK_COLLECTOR_CLOCK_REGRESSION")
        sample: dict[str, Any] = {
            "schema": SAMPLE_SCHEMA,
            "epoch_id": config.value["epoch_id"],
            "config_sha256": config.identity,
            "sample_sequence": head["sample_count"] + 1,
            "previous_sample_hash": head["last_sample_hash"],
            "observed_at": isoformat_utc(current),
            "inputs": inputs,
            "input_errors": errors,
            "live_recovery": advance(
                head["live_recovery"],
                inputs,
                bindings=config.bindings,
                keys=config.keys,
                now=current,
                history_policy=config.value.get("effect_history_policy"),
            ),
        }
        sample["sample_hash"] = digest(sample)
        if len(canonical_json(sample)) > MAXIMUM_FRAME_BYTES:
            raise ValueError("RECOVERY_SOAK_FRAME_TOO_LARGE")
        # Establish the empty committed head durably before the first append.
        # If the following state replacement fails, the existing one-frame
        # recovery rule also works on the very first sample. A missing state
        # beside existing evidence remains an integrity failure in _head().
        if not state_path.exists():
            atomic_write_json(state_path, head)
        atomic_write_json(state_path, {**head, "pending_sample": sample})
        _append_line(evidence_path, sample, maximum_bytes=int(config.value["maximum_evidence_bytes"]))
        atomic_write_json(state_path, _next_state(config, sample, evidence_path.stat().st_size))
        return sample
    finally:
        os.close(descriptor)


def snapshot(config: Config, *, prefix: dict[str, Any] | None = None) -> tuple[dict[str, Any], Iterator[dict[str, Any]]]:
    """Pin a committed prefix under the collector lock; replay outside it."""
    state_path = Path(config.value["state_file"])
    lock = os.open(state_path.with_suffix(".lock"), os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        current_head = _head(config)
        head = prefix or current_head
        if prefix is not None and (
            head.get("config_sha256") != current_head["config_sha256"]
            or head.get("epoch_id") != current_head["epoch_id"]
            or head.get("sample_count", -1) > current_head["sample_count"]
            or head.get("verified_bytes", -1) > current_head["verified_bytes"]
        ):
            raise ValueError("RECOVERY_SOAK_PREFIX_NOT_COMMITTED")
        descriptor = os.open(config.value["evidence_file"], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or (
                metadata.st_size < head["verified_bytes"]
                if prefix is not None or current_head["pending_sample"] is not None
                else metadata.st_size != head["verified_bytes"]
            )
            or metadata.st_size > int(config.value["maximum_evidence_bytes"])
        ):
            os.close(descriptor)
            raise ValueError("RECOVERY_SOAK_PREFIX_NOT_COMMITTED")
    finally:
        os.close(lock)

    def rows() -> Iterator[dict[str, Any]]:
        with os.fdopen(descriptor, "rb") as stream:
            remaining = int(head["verified_bytes"])
            while remaining:
                raw = stream.readline(min(MAXIMUM_FRAME_BYTES + 1, remaining))
                if not raw.endswith(b"\n") or len(raw) > MAXIMUM_FRAME_BYTES:
                    raise ValueError("RECOVERY_SOAK_PARTIAL_OR_OVERSIZE_FRAME")
                remaining -= len(raw)
                yield strict_json_object(raw)

    return head, rows()


def _health(fact: object, now: datetime, maximum_age: int) -> Health:
    if not isinstance(fact, dict) or not source_fresh(fact.get("observed_at"), now, maximum_age):
        return "UNKNOWN"
    return cast(Health, fact["state"]) if fact.get("state") in {"UP", "DOWN", "UNKNOWN"} else "UNKNOWN"


def _cra_safety_state(safety: dict[str, Any]) -> str:
    """Classify positive violations separately from unavailable evidence.

    A signed wrapper proves who emitted the nested fact, not that every nested
    field was observed.  Explicit unsafe values are retained even when another
    field is missing, while missing/malformed/UNKNOWN values pause
    certification instead of manufacturing a positive CRA safety violation.
    """

    delivery = safety.get("command_delivery_enabled")
    mode = safety.get("runtime_operating_mode")
    capability_count = safety.get("control_capability_count")
    integrity = safety.get("integrity")
    counts = safety.get("action_table_counts")
    evidence_status = safety.get("evidence_status")

    if delivery is True:
        return "VIOLATED"
    if isinstance(mode, str) and mode not in {"", "UNKNOWN", "NO_ACTION"}:
        return "VIOLATED"
    if type(capability_count) is int and capability_count > 0:
        return "VIOLATED"
    if isinstance(integrity, str) and integrity not in {"", "UNKNOWN", "ok"}:
        return "VIOLATED"
    if isinstance(counts, dict) and any(
        table in counts and type(counts[table]) is int and counts[table] > 0 for table in ACTION_TABLES
    ):
        return "VIOLATED"

    complete_safe = (
        delivery is False
        and mode == "NO_ACTION"
        and type(capability_count) is int
        and capability_count == 0
        and integrity == "ok"
        and isinstance(counts, dict)
        and set(counts) == set(ACTION_TABLES)
        and all(type(value) is int and value == 0 for value in counts.values())
        and evidence_status in {None, "OBSERVED"}
    )
    return "SAFE" if complete_safe else "UNKNOWN"


def _dell_effect_safety_violated(effects: dict[str, Any]) -> bool:
    return (
        not nonnegative_integer(effects.get("physical_attempt_count"))
        or effects.get("integrity") != "ok"
        or effects.get("duplicate_attempt_count") != 0
        or effects.get("unauthorized_effect_count") != 0
    )


def _dell_activation_safety_violated(facts: dict[str, Any]) -> bool:
    """A signed, target-bound false activation is a positive SUT violation."""
    transport = facts.get("transport")
    activation = facts.get("activation")
    target = transport.get("target") if isinstance(transport, dict) else None
    return (
        isinstance(target, str)
        and isinstance(activation, dict)
        and set(activation) == {"bounded_termination_enabled", "rw_timeout_enabled"}
        and any(activation.get(name) is False for name in ("bounded_termination_enabled", "rw_timeout_enabled"))
    )


def evaluate(
    samples: Iterable[Mapping[str, Any]], *, config: Config, now: datetime, logical_replay: bool = False,
    positive_blockers: set[str] | None = None,
    admission_cache: AdmissionCache | None = None,
) -> dict[str, Any]:
    window = RecoveryWindow()
    blockers: set[str] = set()
    unknowns: set[str] = set()
    oracle_errors: set[str] = set()
    previous_packets: dict[str, dict[str, Any]] = {}
    previous_time: datetime | None = None
    previous_hash: str | None = None
    previous_effects: int | None = None
    previous_target: str | None = None
    previous_effect_counter: int | None = None
    pending_effect_transition: tuple[str, str] | None = None
    target_transition_count = 0
    no_effect_target_transition_count = 0
    effect_bound_target_transition_count = 0
    unclassified_target_transition_count = 0
    initial_effects: int | None = None
    effect_attempt_increase_count = 0
    activation_unknown_sample_count = 0
    activation_unavailable_sample_count = 0
    owner_lifecycle_sample_counts = dict.fromkeys(("RUNNING", "ABSENT", "TRANSITION", "UNKNOWN", "MISSING"), 0)
    owner_read_diagnostic_sample_count = 0
    owner_read_retry_sample_count = 0
    owner_read_diagnostic_code_counts: dict[str, int] = {}
    owner_unverified_diagnostic_code_counts: dict[str, int] = {}
    source_failure_events = dict.fromkeys(("dell", "arena", "cra"), 0)
    source_failure_events_during_recovery = dict.fromkeys(("dell", "arena", "cra"), 0)
    count = 0
    maximum_gap = 0.0
    gap_sum = 0.0
    gap_count = 0
    gap_histogram: dict[int, int] = {}
    gap_exceeded = {"15": 0, "30": 0, "45": 0}
    domains = dict.fromkeys(
        (
            "dell_network_down",
            "arena_network_down",
            "both_network_down",
            "dell_only_network_down",
            "arena_only_network_down",
            "platform_ingest_down",
            "control_path_down",
            "cra_runtime_safe_blocked",
        ),
        0,
    )
    first_time: datetime | None = None
    source_unavailable_count = dict.fromkeys(ROLES, 0)
    pi_advisories: set[str] = set()
    healthy_final = False
    live_checkpoint: dict[str, Any] | None = None
    history_policy = validate_policy(config.value.get("effect_history_policy"))
    packet_cache = admission_cache or AdmissionCache()
    final_effect_history: dict[str, Any] | None = None
    recovery_source_gaps: list[datetime] = []
    owner_lifecycle_unknown_gaps: list[datetime] = []
    rtmps_evidence_ranges: list[tuple[datetime, datetime]] = []
    fields = {
        "schema",
        "epoch_id",
        "config_sha256",
        "sample_sequence",
        "previous_sample_hash",
        "observed_at",
        "inputs",
        "input_errors",
        "sample_hash",
        "live_recovery",
    }
    for index, raw in enumerate(samples, start=1):
        count += 1
        sample = dict(raw)
        if (
            set(sample) != fields
            or sample.get("schema") != SAMPLE_SCHEMA
            or sample.get("epoch_id") != config.value["epoch_id"]
            or sample.get("config_sha256") != config.identity
            or sample.get("sample_sequence") != index
            or sample.get("sample_hash") != digest(sample)
            or sample.get("previous_sample_hash") != previous_hash
        ):
            oracle_errors.add("SAMPLE_CHAIN_OR_EPOCH_INVALID")
            continue
        current = timestamp(sample["observed_at"])
        try:
            validate_policy(history_policy, now=current)
        except ValueError:
            blockers.add("EFFECT_HISTORY_FREEZE_AFTER_EPOCH")
        if current > now:
            blockers.add("SAMPLE_FROM_FUTURE")
        if previous_time is not None:
            gap = (current - previous_time).total_seconds()
            maximum_gap = max(maximum_gap, gap)
            if gap > 0:
                gap_count += 1
                gap_sum += gap
                bucket = math.ceil(gap * 1000)
                gap_histogram[bucket] = gap_histogram.get(bucket, 0) + 1
                for threshold in gap_exceeded:
                    gap_exceeded[threshold] += int(gap > int(threshold))
            if gap < 0 or (gap == 0 and not logical_replay):
                oracle_errors.add("COLLECTOR_CLOCK_REGRESSION")
            elif gap > config.value["maximum_sample_gap_seconds"]:
                # An interruption of the collector is not positive evidence
                # that the stream failed.  Until a complete, independently
                # verified historical replay exists, formal certification is
                # paused rather than declaring either SUT failure or PASS.
                unknowns.add("COLLECTOR_GAP_UNRECONSTRUCTED")
        first_time = first_time or current
        previous_time, previous_hash = current, sample["sample_hash"]
        if not isinstance(sample["inputs"], dict) or set(sample["inputs"]) != set(ROLES):
            blockers.add("SAMPLE_INPUT_ROLES_INVALID")
            continue
        live_checkpoint = advance(
            live_checkpoint, sample["inputs"], bindings=config.bindings, keys=config.keys, now=current,
            history_policy=history_policy, replay_step=index if logical_replay else 0,
            admission_cache=packet_cache,
        )
        if sample["live_recovery"] != live_checkpoint:
            oracle_errors.add("LIVE_CHECKPOINT_REPLAY_MISMATCH")
        admitted: dict[str, dict[str, Any]] = {}
        source_failure_deltas_this_sample: dict[str, int] = {}
        outage_source_gaps: set[str] = set()
        for role in ROLES:
            packet = sample["inputs"][role]
            if packet is None:
                if isinstance(sample["input_errors"], dict) and sample["input_errors"].get(role) == "INPUT_INVALID":
                    (pi_advisories if role == "raspi" else blockers).add(f"{role.upper()}_INPUT_INVALID")
                source_unavailable_count[role] += 1
                if role == "raspi":
                    pi_advisories.add("PI_EVIDENCE_UNAVAILABLE")
                elif role == "cra":
                    unknowns.add(f"{role.upper()}_EVIDENCE_UNAVAILABLE_OUTSIDE_KNOWN_OUTAGE")
                elif window.active is None:
                    outage_source_gaps.add(f"{role.upper()}_EVIDENCE_UNAVAILABLE_OUTSIDE_KNOWN_OUTAGE")
                continue
            try:
                if role == "raspi" and role not in config.bindings:
                    pi_advisories.add("PI_UNCONFIGURED_INPUT_REJECTED")
                    continue
                checked = packet_cache.admit(packet, binding=config.bindings[role], keys=config.keys, now=current)
            except (ValueError, TypeError, KeyError, RecoveryControlError) as error:
                if str(error) == "RECOVERY_ENVELOPE_STALE":
                    source_unavailable_count[role] += 1
                    if role == "raspi":
                        pi_advisories.add("PI_EVIDENCE_STALE")
                    elif role == "cra":
                        unknowns.add(f"{role.upper()}_EVIDENCE_STALE_OUTSIDE_KNOWN_OUTAGE")
                    elif window.active is None:
                        outage_source_gaps.add(f"{role.upper()}_EVIDENCE_STALE_OUTSIDE_KNOWN_OUTAGE")
                    continue
                if role == "raspi":
                    pi_advisories.add("PI_EVIDENCE_INVALID")
                else:
                    blockers.add(f"{role.upper()}_SIGNED_EVIDENCE_INVALID")
                continue
            previous = previous_packets.get(role)
            if previous is not None:
                if checked["source_failure_count"] < previous["source_failure_count"]:
                    (pi_advisories if role == "raspi" else blockers).add(role.upper() + "_SOURCE_FAILURE_COUNTER_REGRESSION")
                elif checked["source_failure_count"] > previous["source_failure_count"]:
                    delta = checked["source_failure_count"] - previous["source_failure_count"]
                    if role == "raspi":
                        pi_advisories.add("PI_SOURCE_READ_FAILURE_HISTORY")
                    else:
                        source_failure_events[role] += delta
                        source_failure_deltas_this_sample[role] = delta
                if (checked["host_boot_id"], checked["producer_id"]) != (previous["host_boot_id"], previous["producer_id"]):
                    if role == "raspi":
                        pi_advisories.add("PI_PRODUCER_ROTATED")
                    else:
                        blockers.add(f"{role.upper()}_BOOT_OR_PRODUCER_ROTATED")
                elif checked["sequence"] < previous["sequence"] or (
                    checked["sequence"] == previous["sequence"] and checked["payload_sha256"] != previous["payload_sha256"]
                ):
                    if role == "raspi":
                        pi_advisories.add("PI_SEQUENCE_CONFLICT")
                    else:
                        blockers.add(f"{role.upper()}_SEQUENCE_REGRESSION_OR_CONFLICT")
            previous_packets[role] = checked
            admitted[role] = checked["facts"]
        dell = admitted.get("dell", {})
        arena = admitted.get("arena", {})
        safety = admitted.get("cra", {}).get("safety", {})
        effects = dell.get("effects", {})
        final_effect_history = None
        if not isinstance(safety, dict) or not source_fresh(safety.get("observed_at"), current, 45):
            unknowns.add("CRA_SAFETY_EVIDENCE_MISSING_OR_STALE")
        else:
            cra_safety_state = _cra_safety_state(safety)
            if cra_safety_state == "VIOLATED":
                blockers.add("CRA_NO_ACTION_SAFETY_VIOLATED")
                if positive_blockers is not None:
                    positive_blockers.add("CRA_NO_ACTION_SAFETY_VIOLATED")
            elif cra_safety_state == "UNKNOWN":
                unknowns.add("CRA_SAFETY_EVIDENCE_MISSING_OR_STALE")
        if not source_fresh(safety.get("runtime_observed_at") if isinstance(safety, dict) else None, current, 45):
            unknowns.add("CRA_RUNTIME_STATUS_STALE_OR_UNKNOWN")
        current_physical: int | None = None
        independent_required = config.value.get("require_independent_effect_evidence") is True
        if independent_required and isinstance(effects, dict) and effects.get("target_status") not in ("VALID", "UNKNOWN"):
            unknowns.add("DELL_INDEPENDENT_EFFECT_EVIDENCE_REQUIRED")
        if not isinstance(effects, dict) or not source_fresh(effects.get("observed_at"), current, 45):
            if independent_required or window.active is None or previous_effects is None:
                unknowns.add("DELL_EFFECT_EVIDENCE_MISSING_OR_STALE")
            effects = {}
        else:
            try:
                transport_target = dell.get("transport", {})
                validate_effect_history(
                    effects,
                    policy=history_policy,
                    target=transport_target.get("target") if isinstance(transport_target, dict) else None,
                    now=current,
                    allow_unknown_target=effects.get("target_status") == "UNKNOWN",
                )
            except (ValueError, TypeError):
                blockers.add("DELL_EFFECT_HISTORY_INVALID")
            else:
                final_effect_history = {
                    k: effects[k]
                    for k in (
                        "observed_at",
                        "unresolved_scope_count_total",
                        "unresolved_scope_count",
                        "current_target_unresolved_scope_count",
                        "current_target_sha256",
                    )
                }
            physical = effects.get("physical_attempt_count")
            if _dell_effect_safety_violated(effects):
                blockers.add("DELL_EFFECT_SAFETY_VIOLATED")
                if positive_blockers is not None:
                    positive_blockers.add("DELL_EFFECT_SAFETY_VIOLATED")
            elif previous_effects is not None and nonnegative_integer(physical) and physical < previous_effects:
                blockers.add("DELL_EFFECT_COUNTER_REGRESSION")
            elif nonnegative_integer(physical):
                current_physical = physical
                if initial_effects is None:
                    initial_effects = physical
                if previous_effects is not None and physical > previous_effects:
                    effect_attempt_increase_count += physical - previous_effects
                    blockers.add("NO_ACTION_PHYSICAL_EFFECT_DETECTED")
                    if positive_blockers is not None:
                        positive_blockers.add("NO_ACTION_PHYSICAL_EFFECT_DETECTED")
                previous_effects = physical
        network = dell.get("network", {})
        transport = dell.get("transport", {})
        platform = arena.get("platform", {})
        control = arena.get("control_path", {})
        if not all(isinstance(v, dict) for v in (network, transport, platform, control)):
            blockers.add("SOURCE_FACT_STRUCTURE_INVALID")
            continue
        if config.value.get("require_network_episode_evidence") is True and "episode" not in network:
            unknowns.add("DELL_NETWORK_EPISODE_EVIDENCE_REQUIRED")
        if config.value.get("require_rtmps_episode_evidence") is True and "rtmps_episode" not in transport:
            unknowns.add("DELL_RTMPS_EPISODE_EVIDENCE_REQUIRED")
        source_health_unknown = any(
            _health(fact, current, age) == "UNKNOWN" for fact, age in ((network, 45), (platform, 120), (control, 45))
        )
        target = transport.get("target")
        if target is not None and not isinstance(target, str):
            blockers.add("TRANSPORT_TARGET_INVALID")
            target = None
        if previous_target is not None and target is not None and previous_target != target:
            if pending_effect_transition is not None:
                unclassified_target_transition_count += 1
                pending_effect_transition = None
            target_transition_count += 1
            effect_delta = (
                current_physical - previous_effect_counter if current_physical is not None and previous_effect_counter is not None else None
            )
            proof = effects.get("last_reconciliation", {})
            effect_bound = (
                effect_delta == 1
                and isinstance(proof, dict)
                and (proof.get("before_target"), proof.get("after_target")) == (previous_target, target)
                and proof.get("resolution") == "EFFECT_OBSERVED"
                and isinstance(proof.get("action_id"), str)
                and bool(proof["action_id"])
            )
            if effect_delta == 0:
                # A child-process lifecycle transition is not an authority
                # effect. Its success is assessed by the bounded recovery
                # oracle, without fabricating an EffectLedger attempt.
                no_effect_target_transition_count += 1
            elif effect_bound:
                effect_bound_target_transition_count += 1
                pending_effect_transition = None
            elif effect_delta == 1:
                pending_effect_transition = (previous_target, target)
            else:
                unclassified_target_transition_count += 1
                pending_effect_transition = None
        proof = effects.get("last_reconciliation", {})
        if (
            pending_effect_transition is not None
            and isinstance(proof, dict)
            and (proof.get("before_target"), proof.get("after_target")) == pending_effect_transition
            and proof.get("resolution") == "EFFECT_OBSERVED"
            and isinstance(proof.get("action_id"), str)
            and bool(proof["action_id"])
        ):
            effect_bound_target_transition_count += 1
            pending_effect_transition = None
        if target is not None and target != previous_target:
            previous_target = target
            previous_effect_counter = current_physical
        had_baseline = window.baseline_at is not None
        was_recovering = window.active is not None
        live_observation = observation(admitted, stream_id=config.bindings["dell"].stream_id, now=current, history_policy=history_policy)
        if logical_replay:
            live_observation = replace(live_observation, replay_step=index)
        if live_observation.rtmps_episode_started_at is not None and live_observation.rtmps_episode_detected_at is not None:
            rtmps_evidence_ranges.append(
                (
                    live_observation.rtmps_episode_started_at,
                    live_observation.rtmps_episode_recovered_at or current,
                )
            )
        dell_network, arena_network = _health(network, current, 45), _health(arena.get("network"), current, 45)
        domains["dell_network_down"] += int(dell_network == "DOWN")
        domains["arena_network_down"] += int(arena_network == "DOWN")
        domains["both_network_down"] += int(dell_network == arena_network == "DOWN")
        domains["dell_only_network_down"] += int(dell_network == "DOWN" and arena_network == "UP")
        domains["arena_only_network_down"] += int(arena_network == "DOWN" and dell_network == "UP")
        domains["platform_ingest_down"] += int(live_observation.platform == "DOWN")
        domains["control_path_down"] += int(live_observation.control_path == "DOWN")
        domains["cra_runtime_safe_blocked"] += int(live_observation.cra_readiness == "DOWN")
        window.observe(live_observation)
        # Both paths consumed this exact logical observation.  A matching
        # serialized live checkpoint does not prove continuity across its
        # roundtrips; compare with the continuously held formal window.
        if live_checkpoint is not None and window.checkpoint() != live_checkpoint["window"]:
            oracle_errors.add("RECOVERY_WINDOW_PARITY_MISMATCH")
        if config.value.get("require_owner_lifecycle_evidence") is True:
            if live_observation.child_lifecycle is None:
                unknowns.add("DELL_OWNER_LIFECYCLE_EVIDENCE_REQUIRED")
            elif live_observation.child_lifecycle == "UNKNOWN" and not was_recovering and window.active is None:
                if config.value.get("require_rtmps_episode_evidence") is True:
                    owner_lifecycle_unknown_gaps.append(current)
                else:
                    unknowns.add("DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY")
        if not was_recovering and window.active is None:
            unknowns.update(outage_source_gaps)
        # A confirmed outage in this sample opens its own recovery window.
        # UNKNOWN alone never opens one, and a later DOWN cannot erase an
        # earlier unexplained gap. Ledger and CRA safety checks stay separate.
        if had_baseline and not was_recovering and window.active is None and source_health_unknown:
            unknowns.add("SOURCE_EVIDENCE_UNKNOWN_OUTSIDE_KNOWN_OUTAGE")
        lifecycle_transport = dell.get("transport")
        lifecycle = lifecycle_transport.get("lifecycle", {}) if isinstance(lifecycle_transport, dict) else {}
        state = lifecycle.get("state", "MISSING") if isinstance(lifecycle, dict) else "MISSING"
        owner_lifecycle_sample_counts[state if state in owner_lifecycle_sample_counts else "MISSING"] += 1
        diagnostics = lifecycle.get("read_diagnostics", {}) if isinstance(lifecycle, dict) else {}
        owner_read_diagnostic_sample_count += int(bool(diagnostics.get("events")))
        owner_read_retry_sample_count += int(diagnostics.get("proc_scan_retry_count", 0) > 0)
        events = diagnostics.get("events", []) if isinstance(diagnostics, dict) else []
        for event in events if isinstance(events, list) else []:
            code = event.get("code") if isinstance(event, dict) else None
            if isinstance(code, str):
                owner_read_diagnostic_code_counts[code] = owner_read_diagnostic_code_counts.get(code, 0) + 1
                if state in {"UNKNOWN", "MISSING"}:
                    owner_unverified_diagnostic_code_counts[code] = owner_unverified_diagnostic_code_counts.get(code, 0) + 1
        activation = dell.get("activation", {})
        # Count every unavailable activation observation, including target-less
        # samples. Keep the historical target-bound gate/counter unchanged.
        if "dell" in admitted and (
            not isinstance(activation, dict)
            or set(activation) != {"bounded_termination_enabled", "rw_timeout_enabled"}
            or any(type(value) is not bool for value in activation.values())
        ):
            activation_unavailable_sample_count += 1
        if target is not None and "dell" in admitted:
            if not isinstance(activation, dict) or set(activation) != {"bounded_termination_enabled", "rw_timeout_enabled"}:
                unknowns.add("DELL_ACTIVATION_EVIDENCE_INVALID")
                activation_unknown_sample_count += 1
            elif _dell_activation_safety_violated(dell):
                blockers.add("BOUNDED_TERMINATION_NOT_ACTIVE")
                if positive_blockers is not None:
                    positive_blockers.add("BOUNDED_TERMINATION_NOT_ACTIVE")
            elif any(activation.get(name) is not True for name in ("bounded_termination_enabled", "rw_timeout_enabled")):
                independent_onset = (
                    effects.get("target_status") == "UNKNOWN"
                    and source_fresh(effects.get("observed_at"), current, 45)
                    and window.active is not None
                )
                if not was_recovering and not independent_onset:
                    unknowns.add("DELL_ACTIVATION_EVIDENCE_UNKNOWN_OUTSIDE_RECOVERY")
                activation_unknown_sample_count += 1
        for role, delta in source_failure_deltas_this_sample.items():
            if was_recovering or window.active is not None:
                source_failure_events_during_recovery[role] += delta
            else:
                unknowns.add(role.upper() + "_SOURCE_READ_FAILURE_OUTSIDE_RECOVERY")
        if had_baseline and window.active is None and window.current_stream_health == "UNKNOWN":
            if config.value.get("require_rtmps_episode_evidence") is True:
                recovery_source_gaps.append(current)
            else:
                unknowns.add("RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE")
        healthy_final = window.current_health == "READY" and all(r in admitted for r in ("dell", "arena", "cra"))
        publication = admitted.get("raspi", {}).get("publication", {})
        if isinstance(publication, dict) and "raspi" in admitted:
            if publication.get("control_input_allowed") is not False:
                pi_advisories.add("PI_CONTROL_BOUNDARY_INVALID")
            reasons = publication.get("reason_codes", [])
            for reason in reasons if isinstance(reasons, list) else []:
                if isinstance(reason, str):
                    pi_advisories.add(reason)
    recovery = window.finish(now)
    unknowns.update(recovery["unknown_reasons"])
    covered_recovery_gaps = sum(any(started <= gap <= ended for started, ended in rtmps_evidence_ranges) for gap in recovery_source_gaps)
    covered_owner_unknowns = sum(
        any(started <= gap <= ended for started, ended in rtmps_evidence_ranges) for gap in owner_lifecycle_unknown_gaps
    )
    if covered_recovery_gaps != len(recovery_source_gaps):
        unknowns.add("RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE")
    if covered_owner_unknowns != len(owner_lifecycle_unknown_gaps):
        unknowns.add("DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY")
    if pending_effect_transition is not None:
        unclassified_target_transition_count += 1
    blockers.update(recovery["blockers"])
    duration = (previous_time - first_time).total_seconds() if first_time is not None and previous_time is not None else 0
    verified_duration = (
        (previous_time - window.baseline_at).total_seconds() if previous_time is not None and window.baseline_at is not None else 0
    )
    final_age = (now - previous_time).total_seconds() if previous_time else None
    if final_age is None or not 0 <= final_age <= 45:
        unknowns.add("FINAL_SAMPLE_MISSING_OR_STALE")
    pending: set[str] = set()
    if verified_duration < int(config.value["minimum_duration_seconds"]):
        pending.add("SOAK_DURATION_INSUFFICIENT")
    if window.network_recovered_count == 0:
        pending.add("RECOVERY_NOT_EXERCISED")
    if not healthy_final or window.active is not None:
        pending.add("FINAL_RECOVERY_NOT_CONFIRMED")
    harness_classification = (
        "HARNESS_FAILURE" if oracle_errors else "MISSING_EVIDENCE" if unknowns else "SUT_FAILURE" if blockers else "PASS"
    )
    formal_evaluation_performed = harness_classification in {"PASS", "SUT_FAILURE"}
    soak_status = None if not formal_evaluation_performed else "FAIL" if blockers else "NOT_YET_ELIGIBLE" if pending else "PASS"
    # `status=UNKNOWN` remains only as a compatibility projection. The v2
    # formal vocabulary is `soak_status`, and is withheld when the harness or
    # required evidence is not trustworthy.
    status = soak_status or "UNKNOWN"
    rank, cumulative, p95 = math.ceil(gap_count * 0.95), 0, None
    for bucket, frequency in sorted(gap_histogram.items()):
        cumulative += frequency
        if cumulative >= rank:
            p95 = bucket / 1000
            break
    result = {
        "schema": gate_schema(config),
        "epoch_id": config.value["epoch_id"],
        "config_sha256": config.identity,
        "status": status,
        "soak_status": soak_status,
        "formal_evaluation_performed": formal_evaluation_performed,
        "harness_classification": harness_classification,
        "oracle_errors": sorted(oracle_errors),
        "eligible": soak_status == "PASS",
        "live_health": recovery["live_health"] if final_age is not None and 0 <= final_age <= 45 else "UNKNOWN",
        "sample_count": count,
        "last_sample_hash": previous_hash,
        "duration_seconds": duration,
        "verified_duration_seconds": verified_duration,
        "final_sample_age_seconds": final_age,
        "maximum_sample_gap_seconds": maximum_gap,
        "first_sample_at": isoformat_utc(first_time) if first_time else None,
        "last_sample_at": isoformat_utc(previous_time) if previous_time else None,
        "sample_gaps": {
            "count": gap_count,
            "average_seconds": gap_sum / gap_count if gap_count else None,
            "p95_upper_bound_seconds": p95,
            "p95_resolution_seconds": 0.001,
            "exceeded_seconds_count": gap_exceeded,
        },
        "failure_domain_sample_counts": domains,
        "source_unavailable_sample_count": source_unavailable_count,
        "source_failure_events": {
            "total": source_failure_events,
            "during_recovery": source_failure_events_during_recovery,
            "outside_recovery": {
                role: source_failure_events[role] - source_failure_events_during_recovery[role] for role in source_failure_events
            },
        },
        "target_transitions": {
            "total": target_transition_count,
            "no_authority_effect": no_effect_target_transition_count,
            "effect_bound": effect_bound_target_transition_count,
            "unclassified": unclassified_target_transition_count,
        },
        "activation_unknown_sample_count": activation_unknown_sample_count,
        "activation_unavailable_sample_count": activation_unavailable_sample_count,
        "owner_lifecycle_sample_counts": owner_lifecycle_sample_counts,
        "owner_read_diagnostic_sample_count": owner_read_diagnostic_sample_count,
        "owner_read_retry_sample_count": owner_read_retry_sample_count,
        "owner_read_diagnostic_code_counts": dict(sorted(owner_read_diagnostic_code_counts.items())),
        "owner_unverified_diagnostic_code_counts": dict(sorted(owner_unverified_diagnostic_code_counts.items())),
        "rtmps_episode_coverage": {
            "transport_gap_sample_count": len(recovery_source_gaps),
            "explained_transport_gap_sample_count": covered_recovery_gaps,
            "owner_unknown_sample_count": len(owner_lifecycle_unknown_gaps),
            "explained_owner_unknown_sample_count": covered_owner_unknowns,
        },
        "physical_attempt_baseline": initial_effects,
        "physical_attempt_final": previous_effects,
        "physical_attempt_delta": (
            previous_effects - initial_effects if previous_effects is not None and initial_effects is not None else None
        ),
        "physical_attempt_increase_count": effect_attempt_increase_count,
        "blockers": sorted(blockers),
        "unknown_reasons": sorted(unknowns),
        "pending_reasons": sorted(pending),
        "recovery": recovery,
        "pi_advisories": sorted(pi_advisories),
        "pi_is_control_input": False,
        "control_capability_count": 0,
        "effect_history": {
            "policy_sha256": history_hash(history_policy),
            "frozen_at": history_policy["frozen_at"],
            "historical_retired_unknown": history_policy["retired_unknown"],
            "final_observation": final_effect_history,
            "historical_outcome_resolved": False,
        },
        "evaluated_at": isoformat_utc(now),
    }
    result["operator_status"] = classify_gate(result)
    return result


class _HistoryCursor:
    """Verify a frozen append-only prefix with bounded frame memory."""

    def __init__(self, config: Config, role: str, *, admission_cache: AdmissionCache | None = None) -> None:
        self.config = config
        self.role = role
        self.admission_cache = admission_cache
        descriptor = os.open(config.value["history_files"][role], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            self.metadata = os.fstat(descriptor)
            if not stat.S_ISREG(self.metadata.st_mode) or self.metadata.st_size > MAXIMUM_BYTES:
                raise ValueError("RECOVERY_HISTORY_FILE_INVALID")
            # A bounded readline on an unbuffered FileIO issues a syscall for
            # each byte.  BufferedReader preserves the frame limit below.
            self.stream = os.fdopen(descriptor, "rb", buffering=64 * 1024)
        except BaseException:
            os.close(descriptor)
            raise
        self.read_bytes = 0
        self.sequence = 0
        self.packet: dict[str, Any] | None = None
        self.previous_time: datetime | None = None
        self.identity: tuple[str, str] | None = None
        self.selected_packets = 0
        self.checksum = hashlib.sha256()

    def close(self) -> None:
        self.stream.close()

    def mark(self) -> tuple[Any, ...]:
        """Save a verified interval boundary for a bounded second pass."""
        return (
            self.stream.tell(), self.read_bytes, self.sequence, self.packet,
            self.previous_time, self.identity, self.selected_packets, self.checksum.copy(),
        )

    def restore(self, mark: tuple[Any, ...]) -> None:
        self.stream.seek(mark[0])
        (
            self.read_bytes, self.sequence, self.packet, self.previous_time,
            self.identity, self.selected_packets, self.checksum,
        ) = mark[1:]

    def _next(self) -> dict[str, Any]:
        if self.read_bytes >= self.metadata.st_size:
            raise ValueError("RECOVERY_HISTORY_GAP_INCOMPLETE")
        raw = self.stream.readline(min(MAXIMUM_FRAME_BYTES + 1, self.metadata.st_size - self.read_bytes))
        self.read_bytes += len(raw)
        if not raw or len(raw) > MAXIMUM_FRAME_BYTES or not raw.endswith(b"\n"):
            raise ValueError("RECOVERY_HISTORY_FRAME_INVALID")
        self.checksum.update(raw)
        try:
            packet = strict_json_object(raw)
        except (ValueError, TypeError) as error:
            raise ValueError("RECOVERY_HISTORY_FRAME_INVALID") from error
        sequence = packet.get("sequence")
        if packet.get("role") != self.role or type(sequence) is not int or sequence != self.sequence + 1:
            raise ValueError("RECOVERY_HISTORY_SEQUENCE_OR_ROLE_INVALID")
        try:
            at = timestamp(packet["observed_at"])
        except (ValueError, TypeError, KeyError) as error:
            raise ValueError("RECOVERY_HISTORY_SIGNED_PACKET_INVALID") from error
        try:
            if self.admission_cache is None:
                admit_facts(packet, binding=self.config.bindings[self.role], keys=self.config.keys, now=at)
            else:
                self.admission_cache.admit(packet, binding=self.config.bindings[self.role], keys=self.config.keys, now=at)
        except SignatureValidationError as error:
            raise ValueError("RECOVERY_HISTORY_SIGNATURE_INVALID") from error
        except (ValueError, TypeError, KeyError, RecoveryControlError) as error:
            raise ValueError("RECOVERY_HISTORY_SIGNED_PACKET_INVALID") from error
        identity = (packet["host_boot_id"], packet["producer_id"])
        if self.identity is not None and identity != self.identity:
            raise ValueError("RECOVERY_HISTORY_PRODUCER_ROTATED")
        if self.previous_time is not None and at <= self.previous_time:
            raise ValueError("RECOVERY_HISTORY_TIME_REGRESSION")
        self.identity = identity
        self.previous_time = at
        self.sequence = sequence
        self.packet = packet
        return packet

    def through(
        self, sequence: object, *, on_packet: Callable[[dict[str, Any]], None] | None = None
    ) -> list[dict[str, Any]]:
        if not isinstance(sequence, int) or type(sequence) is not int or sequence < self.sequence:
            raise ValueError("RECOVERY_HISTORY_GAP_BOUNDARY_MISSING")
        packets = []
        while self.sequence < sequence:
            packet = self._next()
            if on_packet is not None:
                on_packet(packet)
            packets.append(packet)
            if len(packets) > MAXIMUM_HISTORY_PACKETS_PER_INTERVAL:
                raise ValueError("RECOVERY_HISTORY_INTERVAL_LIMIT")
        self.selected_packets += len(packets)
        return packets

    def iter_through(
        self, sequence: object, *, on_packet: Callable[[dict[str, Any]], None] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Stream a long interval without a per-interval packet allocation."""
        if type(sequence) is not int or sequence < self.sequence:
            raise ValueError("RECOVERY_HISTORY_GAP_BOUNDARY_MISSING")
        while self.sequence < sequence:
            packet = self._next()
            self.selected_packets += 1
            if self.selected_packets > MAXIMUM_HISTORY_PACKETS_PER_ROLE:
                raise ValueError("RECOVERY_HISTORY_RECONSTRUCTION_LIMIT")
            if on_packet is not None:
                on_packet(packet)
            yield packet

    def align(self, sequence: int) -> dict[str, Any] | None:
        """Verify the pre-soak prefix without retaining it as one interval."""
        if type(sequence) is not int or sequence < self.sequence:
            raise ValueError("RECOVERY_HISTORY_GAP_BOUNDARY_MISSING")
        while self.sequence < sequence:
            self._next()
        return self.packet

    def finish(self) -> dict[str, Any]:
        # The raw snapshot commits only the sequence reached by replay.
        # A producer may have appended an incomplete later frame concurrently;
        # that tail is not evidence for (or against) this frozen replay.
        current = os.fstat(self.stream.fileno())
        path = os.stat(self.config.value["history_files"][self.role], follow_symlinks=False)
        if (
            current.st_dev != self.metadata.st_dev
            or current.st_ino != self.metadata.st_ino
            or path.st_dev != self.metadata.st_dev
            or path.st_ino != self.metadata.st_ino
            or current.st_size < self.read_bytes
        ):
            raise ValueError("RECOVERY_HISTORY_FILE_CHANGED_DURING_READ")
        return {
            "bytes": self.read_bytes,
            "sha256": self.checksum.hexdigest(),
            "device": self.metadata.st_dev,
            "inode": self.metadata.st_ino,
            "archive_size_at_open": self.metadata.st_size,
            "unverified_tail_bytes": max(0, current.st_size - self.read_bytes),
            "committed_sequence": self.sequence,
            "selected_packets": self.selected_packets,
        }


def _reconstructed_samples(
    samples: Iterable[dict[str, Any]],
    config: Config,
    histories: dict[str, _HistoryCursor],
    *,
    original_head: dict[str, Any],
    timing: dict[str, Any] | None = None,
    positive_blockers: set[str] | None = None,
    admission_cache: AdmissionCache | None = None,
) -> Iterator[dict[str, Any]]:
    """Keep the raw chain separate; create a deterministic formal-only replay."""
    prior_raw: dict[str, Any] | None = None
    previous_hash: str | None = None
    checkpoint: dict[str, Any] | None = None
    sequence = 0
    virtual_count = 0
    raw_count = 0
    raw_previous_hash: str | None = None
    # One verified high-water mark spans every raw interval. A regressed
    # counter is evidence of a regression, not a new baseline for an effect.
    physical_seen: list[int | None] = [None]

    def frame(inputs: dict[str, Any], current: datetime, errors: dict[str, Any]) -> dict[str, Any]:
        nonlocal sequence, previous_hash, checkpoint
        sequence += 1
        checkpoint = advance(
            checkpoint,
            inputs,
            bindings=config.bindings,
            keys=config.keys,
            now=current,
            history_policy=config.value.get("effect_history_policy"),
            replay_step=sequence,
            admission_cache=admission_cache,
        )
        value = {
            "schema": SAMPLE_SCHEMA,
            "epoch_id": config.value["epoch_id"],
            "config_sha256": config.identity,
            "sample_sequence": sequence,
            "previous_sample_hash": previous_hash,
            "observed_at": isoformat_utc(current),
            "inputs": inputs,
            "input_errors": errors,
            "live_recovery": checkpoint,
        }
        value["sample_hash"] = digest(value)
        previous_hash = value["sample_hash"]
        return value

    for current_raw in samples:
        raw_count += 1
        if timing is not None:
            timing["raw_sequence"] = raw_count
        if (
            current_raw.get("sample_sequence") != raw_count
            or current_raw.get("previous_sample_hash") != raw_previous_hash
            or current_raw.get("sample_hash") != digest(current_raw)
        ):
            raise ValueError("RECOVERY_HISTORY_RAW_PREFIX_CHANGED")
        raw_previous_hash = current_raw["sample_hash"]
        current = timestamp(current_raw["observed_at"])
        if prior_raw is None:
            for role, cursor in histories.items():
                packet = current_raw["inputs"].get(role)
                if not isinstance(packet, dict) or type(packet.get("sequence")) is not int:
                    raise ValueError("RECOVERY_HISTORY_GAP_BOUNDARY_MISSING")
                if cursor.align(packet["sequence"]) != packet:
                    raise ValueError("RECOVERY_HISTORY_BOUNDARY_CONFLICT")
                if role == "dell":
                    # Cursor alignment verifies the packet at its own source
                    # time. The counter baseline must also be admissible at
                    # this first raw observation, just as evaluate() requires.
                    try:
                        admitted_first = (
                            admission_cache.admit(packet, binding=config.bindings["dell"], keys=config.keys, now=current)
                            if admission_cache is not None
                            else admit_facts(packet, binding=config.bindings["dell"], keys=config.keys, now=current)
                        )
                    except (ValueError, TypeError, KeyError, RecoveryControlError):
                        continue
                    effects = admitted_first["facts"].get("effects")
                    if isinstance(effects, dict) and source_fresh(effects.get("observed_at"), current, 45):
                        physical = effects.get("physical_attempt_count")
                        if nonnegative_integer(physical):
                            physical_seen[0] = physical
        else:
            previous = timestamp(prior_raw["observed_at"])
            # The signed source time and the CRA visibility interval are two
            # different axes. A late packet has one lower-bound visibility
            # millisecond, but does not need an otherwise empty millisecond of
            # its own. Source time and per-role sequence order its logical
            # transitions within that millisecond.
            def confirm_signed_safety(
                packet: dict[str, Any], packet_role: str, physical_seen: list[int | None],
                *, prior_at: datetime = previous, raw_at: datetime = current,
            ) -> None:
                if positive_blockers is None:
                    return
                source_at = timestamp(packet["observed_at"])
                replay_at = max(source_at, prior_at + timedelta(milliseconds=1))
                if source_at > raw_at or replay_at >= timestamp(packet["valid_until"]):
                    return
                facts = packet.get("facts", {})
                if packet_role == "cra":
                    safety = facts.get("safety")
                    if (
                        isinstance(safety, dict)
                        and source_fresh(safety.get("observed_at"), replay_at, 45)
                        and _cra_safety_state(safety) == "VIOLATED"
                    ):
                        positive_blockers.add("CRA_NO_ACTION_SAFETY_VIOLATED")
                elif packet_role == "dell":
                    if _dell_activation_safety_violated(facts):
                        positive_blockers.add("BOUNDED_TERMINATION_NOT_ACTIVE")
                    effects = facts.get("effects")
                    if not isinstance(effects, dict) or not source_fresh(effects.get("observed_at"), replay_at, 45):
                        return
                    if _dell_effect_safety_violated(effects):
                        positive_blockers.add("DELL_EFFECT_SAFETY_VIOLATED")
                    physical = effects.get("physical_attempt_count")
                    if nonnegative_integer(physical):
                        if physical_seen[0] is not None and physical > physical_seen[0]:
                            positive_blockers.add("NO_ACTION_PHYSICAL_EFFECT_DETECTED")
                        if physical_seen[0] is None or physical > physical_seen[0]:
                            physical_seen[0] = physical

            # Stream each role once. A failing role stops at its first invalid
            # packet, while heapq.merge continues consuming the other roles so
            # independently signed safety violations remain retained. The
            # interval is rejected only after every valid role prefix reaches
            # its raw boundary. This preserves cross-role diagnostics without
            # verifying every signature twice on long gaps.
            boundaries: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
            role_failures: list[dict[str, Any]] = []
            for role in ("cra", "dell", "arena"):
                cursor = histories[role]
                first = prior_raw["inputs"].get(role)
                last = current_raw["inputs"].get(role)
                if not isinstance(first, dict) or not isinstance(last, dict) or cursor.packet != first:
                    role_failures.append({
                        "source_role": role,
                        "source_sequence": cursor.sequence,
                        "source_observed_at": None,
                        "reason_code": "RECOVERY_HISTORY_GAP_BOUNDARY_MISSING",
                        "harness_classification": "MISSING_EVIDENCE",
                    })
                    continue
                boundaries[role] = first, last

            def source_events(
                packet_role: str, cursor: _HistoryCursor, last: dict[str, Any],
                *, prior_at: datetime = previous, raw_at: datetime = current,
                failures: list[dict[str, Any]] = role_failures,
            ) -> Iterator[tuple[datetime, datetime, str, dict[str, Any] | None]]:
                reading_packets = False
                try:
                    reading_packets = True
                    for packet in cursor.iter_through(last.get("sequence")):
                        confirm_signed_safety(packet, packet_role, physical_seen)
                        source_at = timestamp(packet["observed_at"])
                        if timing is not None:
                            timing["role"] = packet_role
                            timing["packet_sequence"] = packet["sequence"]
                            timing["source_at"] = packet["observed_at"]
                        if source_at > raw_at:
                            raise ValueError("RECOVERY_HISTORY_TIME_OUTSIDE_RAW_INTERVAL")
                        if source_at == raw_at:
                            # The original raw boundary already carries this
                            # packet; it is still checked by iter_through().
                            continue
                        replay_at = source_at
                        if source_at <= prior_at:
                            replay_at = prior_at + timedelta(milliseconds=1)
                            if replay_at >= timestamp(packet["valid_until"]):
                                raise ValueError("RECOVERY_HISTORY_LATE_PACKET_EXPIRED")
                            if timing is not None:
                                timing["late_packet_count"] += 1
                                timing["maximum_lateness_seconds"] = max(
                                    timing["maximum_lateness_seconds"], (prior_at - source_at).total_seconds()
                                )
                        yield replay_at, source_at, packet_role, packet
                    reading_packets = False
                    if cursor.packet != last:
                        raise ValueError("RECOVERY_HISTORY_BOUNDARY_CONFLICT")
                except (OSError, ValueError, TypeError, KeyError, RecoveryControlError) as error:
                    reason_code, classification = _historical_failure(error)
                    failures.append({
                        "source_role": packet_role,
                        "source_sequence": cursor.sequence + int(reading_packets),
                        # A failed read has not authenticated the next source
                        # timestamp; never reuse the previous role's time.
                        "source_observed_at": None,
                        "reason_code": reason_code,
                        "harness_classification": classification,
                    })

            def filler_events(
                *, prior_at: datetime = previous, raw_at: datetime = current,
            ) -> Iterator[tuple[datetime, datetime, str, dict[str, Any] | None]]:
                if (raw_at - prior_at).total_seconds() > config.value["maximum_sample_gap_seconds"]:
                    step = prior_at + timedelta(seconds=15)
                    while step < raw_at:
                        yield step, step, "", None
                        step += timedelta(seconds=15)

            events = heapq.merge(
                *(source_events(role, histories[role], boundaries[role][1]) for role in ("cra", "dell", "arena") if role in boundaries),
                filler_events(), key=lambda event: (event[0], event[1], event[2]),
            )
            inputs = dict(prior_raw["inputs"])
            preceding_virtual_at: datetime | None = None
            for (moment, _), group in groupby(events, key=lambda event: (event[0], event[1])):
                for _, _, role, packet in group:
                    if role:
                        inputs[role] = packet
                if preceding_virtual_at == moment and timing is not None:
                    timing["logical_tie_count"] += 1
                preceding_virtual_at = moment
                virtual_count += 1
                if virtual_count > MAXIMUM_RECONSTRUCTED_SAMPLES:
                    raise ValueError("RECOVERY_HISTORY_RECONSTRUCTION_LIMIT")
                yield frame(
                    dict(inputs), moment,
                    {role: "HISTORICAL_PACKET_MISSING" for role, packet in inputs.items() if packet is None},
                )
            if role_failures:
                raise _HistoryRoleFailures(role_failures)
        yield frame(current_raw["inputs"], current, current_raw["input_errors"])
        prior_raw = current_raw
    if raw_count != original_head["sample_count"] or raw_previous_hash != original_head["last_sample_hash"]:
        raise ValueError("RECOVERY_HISTORY_RAW_PREFIX_CHANGED")


def gate(config: Config, *, now: datetime | None = None) -> dict[str, Any]:
    replay_started = time.monotonic()
    current = now or datetime.now(UTC)
    head, samples = snapshot(config)
    gap_count = 0
    prior: dict[str, Any] | None = None

    def raw_frames() -> Iterator[dict[str, Any]]:
        nonlocal prior, gap_count
        for sample in samples:
            if prior is not None and (timestamp(sample["observed_at"]) - timestamp(prior["observed_at"])).total_seconds() > 45:
                gap_count += 1
            yield sample
            prior = sample

    result = evaluate(raw_frames(), config=config, now=current)
    raw_transport_order_revision = (
        result.get("recovery", {}).get("observation_revision", {}).get("transport_target_order")
        if isinstance(result.get("recovery"), dict)
        else None
    )
    raw_transport_order_unknown = TRANSPORT_ORDER_UNKNOWN in result.get("unknown_reasons", [])
    if result["sample_count"] != head["sample_count"] or result["last_sample_hash"] != head["last_sample_hash"]:
        raise ValueError("RECOVERY_SOAK_REPLAY_HEAD_MISMATCH")
    # Window parity is a model defect after the raw sample and signed safety
    # facts have been admitted.  Preserve those independent positive facts;
    # any other raw-chain error still makes this fast path untrusted.
    raw_positive_blockers = (
        set(result["blockers"]) & _POSITIVE_SAFETY_BLOCKERS
        if set(result["oracle_errors"]) <= {"RECOVERY_WINDOW_PARITY_MISMATCH"} else set()
    )
    confirmed_blockers: set[str] = set(raw_positive_blockers)
    historical_status: dict[str, Any] = {"status": "NOT_NEEDED"}
    if config.value.get("schema") == HISTORY_CONFIG_SCHEMA and head["sample_count"] and not result["oracle_errors"]:
        stage = "OPEN_HISTORY"
        timing: dict[str, Any] = {"late_packet_count": 0, "maximum_lateness_seconds": 0.0, "logical_tie_count": 0}
        try:
            with ExitStack() as stack:
                histories: dict[str, _HistoryCursor] = {}
                admission_cache = AdmissionCache()
                for role in ("dell", "arena", "cra"):
                    timing["role"] = role
                    cursor = _HistoryCursor(config, role, admission_cache=admission_cache)
                    stack.callback(cursor.close)
                    histories[role] = cursor
                stage = "RECONSTRUCT"
                replay_head, replay_rows = snapshot(config, prefix=head)
                reconstructed = evaluate(
                    _reconstructed_samples(
                        replay_rows, config, histories, original_head=replay_head,
                        timing=timing, positive_blockers=confirmed_blockers,
                        admission_cache=admission_cache,
                    ),
                    config=config,
                    now=current,
                    logical_replay=True,
                    positive_blockers=confirmed_blockers,
                    admission_cache=admission_cache,
                )
                if not reconstructed["oracle_errors"] and "COLLECTOR_GAP_UNRECONSTRUCTED" not in reconstructed["unknown_reasons"]:
                    confirmed_blockers.update(set(reconstructed["blockers"]) & _POSITIVE_SAFETY_BLOCKERS)
                stage = "VERIFY_TAIL"
                proofs = {}
                for role, cursor in histories.items():
                    timing["role"] = role
                    timing["packet_sequence"] = cursor.sequence
                    timing["source_at"] = isoformat_utc(cursor.previous_time) if cursor.previous_time is not None else None
                    proofs[role] = cursor.finish()
            if reconstructed["oracle_errors"] or "COLLECTOR_GAP_UNRECONSTRUCTED" in reconstructed["unknown_reasons"]:
                raise ValueError("RECOVERY_HISTORY_REPLAY_UNTRUSTED")
            replay_sample_count = reconstructed["sample_count"]
            reconstructed.update(
                sample_count=result["sample_count"],
                last_sample_hash=result["last_sample_hash"],
                maximum_sample_gap_seconds=result["maximum_sample_gap_seconds"],
                sample_gaps=result["sample_gaps"],
            )
            result = reconstructed
            historical_status = {
                "status": "RECONSTRUCTED",
                "order_model": "source_time_visibility_interval_logical_step.v1",
                "gap_count": gap_count,
                "replay_sample_count": replay_sample_count,
                "late_packet_count": timing["late_packet_count"],
                "maximum_lateness_seconds": timing["maximum_lateness_seconds"],
                "logical_tie_count": timing["logical_tie_count"],
                "sources": proofs,
            }
        except (OSError, ValueError, TypeError, KeyError, RecoveryControlError) as error:
            reason_code, classification = _historical_failure(error)
            historical_status = {
                "status": "UNAVAILABLE_OR_INVALID" if classification == "MISSING_EVIDENCE" else "MODEL_ERROR",
                "gap_count": gap_count,
                "reason_code": reason_code,
                "stage": stage,
                "raw_sequence": timing.get("raw_sequence"),
                "source_role": timing.get("role"),
                "source_sequence": timing.get("packet_sequence"),
                "source_observed_at": timing.get("source_at"),
            }
            if isinstance(error, _HistoryRoleFailures):
                historical_status.update({
                    key: error.primary[key]
                    for key in ("source_role", "source_sequence", "source_observed_at")
                })
                historical_status["role_failures"] = error.failures
                if any(item["harness_classification"] == "MISSING_EVIDENCE" for item in error.failures):
                    result["unknown_reasons"] = sorted(set(result["unknown_reasons"]) | {"HISTORICAL_EVIDENCE_UNAVAILABLE"})
                result["oracle_errors"] = sorted(set(result["oracle_errors"]) | {
                    item["reason_code"] for item in error.failures if item["harness_classification"] == "HARNESS_FAILURE"
                })
            if classification == "MISSING_EVIDENCE":
                result["unknown_reasons"] = sorted(set(result["unknown_reasons"]) | {"HISTORICAL_EVIDENCE_UNAVAILABLE"})
            else:
                result["oracle_errors"] = sorted(set(result["oracle_errors"]) | {reason_code})
            result.update(
                status="UNKNOWN",
                soak_status=None,
                eligible=False,
                formal_evaluation_performed=False,
                harness_classification=classification,
            )
    elif gap_count:
        historical_status = {"status": "RAW_UNTRUSTED" if result["oracle_errors"] else "NOT_CONFIGURED", "gap_count_at_least": gap_count}
    result["historical_replay"] = historical_status
    if head["pending_sample"] is not None:
        result["unknown_reasons"] = sorted(set(result["unknown_reasons"]) | {"EVIDENCE_APPEND_PENDING"})
        if result["soak_status"] != "FAIL":
            result.update(
                status="UNKNOWN", soak_status=None, eligible=False,
                formal_evaluation_performed=False, harness_classification="MISSING_EVIDENCE",
            )
    result["evidence_verified_bytes"] = head["verified_bytes"]
    result["evidence_limit_bytes"] = config.value["maximum_evidence_bytes"]
    duration = float(result["duration_seconds"])
    warmup = min(CAPACITY_PROJECTION_WARMUP_SECONDS, int(config.value["minimum_duration_seconds"]))
    result["capacity_projection_warmup_seconds"] = warmup
    # An immediate timer cycle can make two otherwise normal samples only a
    # second apart. Extrapolating that startup burst to seven days is not a
    # capacity measurement and must not become an environment failure.
    projection = head["verified_bytes"] / duration * 604800 if duration >= warmup else None
    result["projected_seven_day_bytes"] = projection
    result["projected_safety_margin_bytes"] = projection * 1.5 if projection is not None else None
    if projection is None:
        if result["soak_status"] == "PASS":
            result["status"] = result["soak_status"] = "NOT_YET_ELIGIBLE"
        result["eligible"] = False
        result["pending_reasons"].append("EVIDENCE_CAPACITY_NOT_YET_MEASURABLE")
    elif projection * 1.5 > config.value["maximum_evidence_bytes"]:
        result["unknown_reasons"] = sorted(set(result["unknown_reasons"]) | {"EVIDENCE_CAPACITY_NOT_PROVEN"})
        if result["soak_status"] != "FAIL":
            result["status"], result["soak_status"], result["eligible"] = "UNKNOWN", None, False
            result["formal_evaluation_performed"] = False
            result["harness_classification"] = "ENVIRONMENT_FAILURE"
    if confirmed_blockers:
        # Proven signed safety violations are monotonic across a later
        # missing source, stale final sample, archive failure, or pending tail.
        # Keep those evidence defects visible, but never let them erase FAIL.
        result["blockers"] = sorted(set(result["blockers"]) | confirmed_blockers)
        result.update(
            status="FAIL",
            soak_status="FAIL",
            eligible=False,
            formal_evaluation_performed=True,
            harness_classification="SUT_FAILURE",
        )
    transport_order_resolved = (
        raw_transport_order_unknown
        and isinstance(raw_transport_order_revision, dict)
        and historical_status.get("status") == "RECONSTRUCTED"
        and TRANSPORT_ORDER_UNKNOWN not in result.get("unknown_reasons", [])
        and not result.get("oracle_errors")
        and result.get("harness_classification") in {"PASS", "SUT_FAILURE"}
    )
    result["live_unknown_resolutions"] = {
        TRANSPORT_ORDER_UNKNOWN: {
            "schema": LIVE_UNKNOWN_RESOLUTION_SCHEMA,
            "reason_code": TRANSPORT_ORDER_UNKNOWN,
            "status": "RESOLVED" if transport_order_resolved else "NOT_RESOLVED",
            "observation_revision": raw_transport_order_revision,
            "epoch_id": config.value["epoch_id"],
            "config_sha256": config.identity,
            "sample_count": head["sample_count"],
            "last_sample_hash": head["last_sample_hash"],
            "evidence_verified_bytes": head["verified_bytes"],
            "order_model": historical_status.get("order_model"),
        }
    }
    result["operator_status"] = classify_gate(result)
    result["replay_elapsed_seconds"] = max(0.0, time.monotonic() - replay_started)
    sources = historical_status.get("sources")
    result["history_verified_bytes"] = (
        sum(int(proof["bytes"]) for proof in sources.values()) if isinstance(sources, dict) else None
    )
    result["projected_seven_day_replay_seconds"] = (
        result["replay_elapsed_seconds"] / duration * 604800
        if isinstance(sources, dict) and duration >= warmup and duration > 0 else None
    )
    return result


def watchdog(config: Config, *, now: datetime | None = None) -> dict[str, Any]:
    """O(1) local check. Formal history replay remains a separate operation."""
    current = now or datetime.now(UTC)
    blockers: set[str] = set()
    head = _head(config)
    evidence_path = Path(config.value["evidence_file"])
    pending = head["pending_sample"] is not None
    if pending:
        blockers.add("EVIDENCE_APPEND_PENDING")
    try:
        tail = _last_committed_line(
            evidence_path, committed_bytes=head["verified_bytes"],
            maximum_bytes=int(config.value["maximum_evidence_bytes"]),
        )
        size = evidence_path.stat().st_size
    except (OSError, ValueError):
        tail, size = None, -1
    # A racing append is not interpreted as a healthy synchronized snapshot;
    # the next cycle can converge without modifying the history.
    if (
        tail is None
        or tail.get("sample_hash") != head["last_sample_hash"]
        or tail.get("sample_hash") != digest(tail)
        or tail.get("live_recovery") != head["live_recovery"]
        or size < head["verified_bytes"]
        or (size != head["verified_bytes"] and not pending)
    ):
        blockers.add("LIVE_STATE_EVIDENCE_NOT_SYNCHRONIZED")
    if not source_fresh(head["last_observed_at"], current, 45):
        blockers.add("COLLECTOR_STALE")
    recovery: dict[str, Any] = {"live_health": "UNKNOWN"}
    live_unknowns: set[str] = set()
    resolved_live_unknowns: set[str] = set()
    if head["live_recovery"] is None:
        blockers.add("LIVE_RECOVERY_CHECKPOINT_MISSING")
    else:
        checkpoint = head["live_recovery"]
        window = RecoveryWindow.restore(checkpoint["window"])
        recovery = window.finish(current)
        blockers.update(checkpoint["integrity_blockers"])
        blockers.update(recovery["blockers"])
        live_unknowns.update(recovery["unknown_reasons"])
    if "COLLECTOR_STALE" in blockers:
        recovery["live_health"] = "UNKNOWN"
    formal_status = "NOT_EVALUATED"
    formal_age = None
    try:
        formal = strict_object(Path(config.value["gate_file"]))
        formal_age = (current - timestamp(formal["evaluated_at"])).total_seconds()
        formal_count = formal.get("sample_count")
        formal_bytes = formal.get("evidence_verified_bytes")
        formal_hash = formal.get("last_sample_hash")
        formal_tail = (
            _last_committed_line(
                evidence_path,
                committed_bytes=formal_bytes,
                maximum_bytes=int(config.value["maximum_evidence_bytes"]),
            )
            if nonnegative_integer(formal_bytes) and formal_bytes > 0
            else None
        )
        formal_prefix_bound = (
            nonnegative_integer(formal_count)
            and nonnegative_integer(formal_bytes)
            and formal_count <= head["sample_count"]
            and formal_bytes <= head["verified_bytes"]
            and (
                (formal_count == 0 and formal_bytes == 0 and formal_hash is None)
                or (
                    formal_count > 0
                    and formal_bytes > 0
                    and isinstance(formal_tail, dict)
                    and formal_tail.get("sample_sequence") == formal_count
                    and formal_tail.get("sample_hash") == formal_hash
                    and formal_tail.get("sample_hash") == digest(formal_tail)
                    and formal_tail.get("epoch_id") == config.value["epoch_id"]
                    and formal_tail.get("config_sha256") == config.identity
                )
            )
        )
        if (
            formal.get("schema") != gate_schema(config)
            or formal.get("config_sha256") != config.identity
            or formal.get("epoch_id") != config.value["epoch_id"]
            or not 0 <= formal_age <= 7200
            or not formal_prefix_bound
        ):
            blockers.add("FORMAL_GATE_STALE_OR_UNBOUND")
        else:
            formal_status = formal.get("soak_status") or "NOT_EVALUATED"
            historical = formal.get("historical_replay", {})
            if isinstance(historical, dict) and historical.get("status") == "RECONSTRUCTED":
                sources = historical.get("sources", {})
                if config.value.get("schema") != HISTORY_CONFIG_SCHEMA or not isinstance(sources, dict):
                    blockers.add("HISTORICAL_EVIDENCE_UNAVAILABLE")
                else:
                    for role in ("dell", "arena", "cra"):
                        proof = sources.get(role, {})
                        try:
                            metadata = os.stat(config.value["history_files"][role], follow_symlinks=False)
                            if (
                                not isinstance(proof, dict)
                                or not stat.S_ISREG(metadata.st_mode)
                                or metadata.st_dev != proof["device"]
                                or metadata.st_ino != proof["inode"]
                                or metadata.st_size < proof["bytes"]
                            ):
                                blockers.add("HISTORICAL_EVIDENCE_UNAVAILABLE")
                        except (OSError, KeyError, TypeError):
                            blockers.add("HISTORICAL_EVIDENCE_UNAVAILABLE")
            if formal.get("harness_classification") not in {"PASS", "SUT_FAILURE"}:
                blockers.add("FORMAL_GATE_EVALUATION_UNTRUSTED")
            elif formal_status == "FAIL":
                blockers.add("FORMAL_GATE_NOT_HEALTHY")
            resolutions = formal.get("live_unknown_resolutions", {})
            resolution = resolutions.get(TRANSPORT_ORDER_UNKNOWN) if isinstance(resolutions, dict) else None
            current_revision = recovery.get("observation_revision", {}).get("transport_target_order")
            if (
                TRANSPORT_ORDER_UNKNOWN in live_unknowns
                and isinstance(resolution, dict)
                and set(resolution)
                == {
                    "schema", "reason_code", "status", "observation_revision", "epoch_id", "config_sha256",
                    "sample_count", "last_sample_hash", "evidence_verified_bytes", "order_model",
                }
                and resolution.get("schema") == LIVE_UNKNOWN_RESOLUTION_SCHEMA
                and resolution.get("reason_code") == TRANSPORT_ORDER_UNKNOWN
                and resolution.get("status") == "RESOLVED"
                and resolution.get("observation_revision") == current_revision
                and resolution.get("epoch_id") == config.value["epoch_id"]
                and resolution.get("config_sha256") == config.identity
                and resolution.get("sample_count") == formal_count
                and resolution.get("last_sample_hash") == formal_hash
                and resolution.get("evidence_verified_bytes") == formal_bytes
                and resolution.get("order_model") == "source_time_visibility_interval_logical_step.v1"
                and TRANSPORT_ORDER_UNKNOWN not in formal.get("unknown_reasons", [])
                and not formal.get("oracle_errors")
                and formal.get("harness_classification") in {"PASS", "SUT_FAILURE"}
            ):
                resolved_live_unknowns.add(TRANSPORT_ORDER_UNKNOWN)
    except (OSError, ValueError, KeyError, TypeError):
        blockers.add("FORMAL_GATE_UNAVAILABLE")
    blockers.update(live_unknowns - resolved_live_unknowns)
    filesystem = os.statvfs(evidence_path.parent)
    free_bytes, free_inodes = filesystem.f_bavail * filesystem.f_frsize, filesystem.f_favail
    if free_bytes < 5 * 1024**3 or free_inodes < 10_000:
        blockers.add("FILESYSTEM_HEADROOM_LOW")
    if head["verified_bytes"] >= config.value["maximum_evidence_bytes"]:
        blockers.add("EVIDENCE_CAPACITY_EXHAUSTED")
    result = {
        "schema": WATCHDOG_SCHEMA,
        "epoch_id": config.value["epoch_id"],
        "config_sha256": config.identity,
        "status": "AT_RISK" if blockers or recovery["live_health"] != "READY" else "READY",
        "live_health": recovery["live_health"],
        "formal_status": formal_status,
        "formal_gate_age_seconds": formal_age,
        "sample_count": head["sample_count"],
        "recovery": recovery,
        "blockers": sorted(blockers),
        "resolved_live_unknowns": sorted(resolved_live_unknowns),
        "filesystem_free_bytes": free_bytes,
        "filesystem_free_inodes": free_inodes,
        "evidence_verified_bytes": head["verified_bytes"],
        "evidence_limit_bytes": config.value["maximum_evidence_bytes"],
        "observed_at": isoformat_utc(current),
        "control_capability_count": 0,
    }
    result["operator_status"] = classify_gate(result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only recovery-aware 7-day / 600-second soak")
    parser.add_argument("--config", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--gate", action="store_true")
    mode.add_argument("--watchdog", action="store_true")
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument("--operator-report", action="store_true")
    args = parser.parse_args()
    if args.operator_report and not (args.gate or args.watchdog):
        parser.error("--operator-report requires --gate or --watchdog")
    config = Config.load(args.config)
    if args.check_config:
        print(json.dumps({"schema": CONFIG_SCHEMA, "config": "VALID", "config_sha256": config.identity}))
        return
    if args.watchdog or args.gate:
        try:
            result = watchdog(config) if args.watchdog else gate(config)
        except (OSError, ValueError, TypeError, KeyError, RecoveryControlError) as error:
            result = {
                "schema": WATCHDOG_SCHEMA if args.watchdog else gate_schema(config),
                "epoch_id": config.value["epoch_id"],
                "config_sha256": config.identity,
                "status": "AT_RISK" if args.watchdog else "UNKNOWN",
                "soak_status": None,
                "formal_evaluation_performed": False,
                "harness_classification": "HARNESS_FAILURE",
                "oracle_errors": ["EVIDENCE_EVALUATION_FAILED"],
                "eligible": False,
                "blockers": ["EVIDENCE_EVALUATION_FAILED"] if args.watchdog else [],
                "evaluated_at": isoformat_utc(datetime.now(UTC)),
                "control_capability_count": 0,
                "evaluation_error": {
                    "code": _evaluation_failure_code(error),
                    "exception_type": type(error).__name__,
                },
            }
            result["operator_status"] = classify_gate(result)
        output = Path(config.value["state_file"]).with_suffix(".watchdog.json") if args.watchdog else Path(config.value["gate_file"])
        atomic_write_json(output, result)
    else:
        sample = collect(config)
        result = {k: sample[k] for k in ("schema", "sample_sequence", "observed_at", "sample_hash")}
    print(json.dumps(operator_report(result) if args.operator_report else result, sort_keys=True))


if __name__ == "__main__":
    main()
