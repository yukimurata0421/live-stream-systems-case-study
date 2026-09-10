"""Recovery-aware seven-day soak: local inbox -> durable samples -> pure gate.

This is a new epoch/schema, not a reinterpretation of resilient-soak v1/v2.
Unavailable inputs are recorded; invalid signatures/history never become
successful recovery. No public/Pi observation can authorize an action.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import stat
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from cra_dell_recovery.canonical import KeyRing, canonical_json
from cra_dell_recovery.errors import RecoveryControlError
from cra_dell_recovery.recovery_history import history_hash, validate_effect_history, validate_policy
from cra_dell_recovery.time import isoformat_utc

from .host_status import atomic_write_json
from .host_status_pull import _public_key
from .operator_status import classify_gate, operator_report
from .recovery_facts import (
    ACTION_TABLES,
    ROLES,
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
from .resilient_soak import _append_line, _last_line

CONFIG_SCHEMA = "cra.recovery_soak_config.v2"
SAMPLE_SCHEMA = "cra.recovery_soak_sample.v2"
STATE_SCHEMA = "cra.recovery_soak_state.v2"
GATE_SCHEMA = "cra.recovery_soak_gate.v3"
WATCHDOG_SCHEMA = "cra.recovery_soak_watchdog.v3"
MAXIMUM_BYTES = 2 * 1024 * 1024 * 1024
MAXIMUM_FRAME_BYTES = 128 * 1024


def digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json({"frame": {k: v for k, v in value.items() if k != "sample_hash"}})).hexdigest()


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
        if (
            not fields
            <= set(value)
            <= fields | {"require_independent_effect_evidence", "require_owner_lifecycle_evidence", "require_network_episode_evidence"}
            or value.get("schema") != CONFIG_SCHEMA
            or ("require_independent_effect_evidence" in value and value["require_independent_effect_evidence"] is not True)
            or (
                "require_owner_lifecycle_evidence" in value
                and (value["require_owner_lifecycle_evidence"] is not True or value.get("require_independent_effect_evidence") is not True)
            )
            or ("require_network_episode_evidence" in value and value["require_network_episode_evidence"] is not True)
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
    }


def _head(config: Config) -> dict[str, Any]:
    state_path = Path(config.value["state_file"])
    evidence_path = Path(config.value["evidence_file"])
    if not state_path.exists():
        if evidence_path.exists() and evidence_path.stat().st_size:
            raise ValueError("RECOVERY_SOAK_STATE_MISSING")
        return _initial_state(config)
    state = strict_object(state_path, maximum_bytes=64 * 1024)
    if (
        set(state) != set(_initial_state(config))
        or state.get("schema") != STATE_SCHEMA
        or state.get("epoch_id") != config.value["epoch_id"]
        or state.get("config_sha256") != config.identity
        or not nonnegative_integer(state.get("sample_count"))
        or not nonnegative_integer(state.get("verified_bytes"))
    ):
        raise ValueError("RECOVERY_SOAK_STATE_INVALID_OR_EPOCH_CHANGED")
    return state


def _next_state(config: Config, sample: dict[str, Any], size: int) -> dict[str, Any]:
    return {
        **_initial_state(config),
        "sample_count": sample["sample_sequence"],
        "last_sample_hash": sample["sample_hash"],
        "last_observed_at": sample["observed_at"],
        "verified_bytes": size,
        "live_recovery": sample["live_recovery"],
    }


def collect(config: Config, *, now: datetime | None = None) -> dict[str, Any]:
    state_path = Path(config.value["state_file"])
    evidence_path = Path(config.value["evidence_file"])
    state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(state_path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        head = _head(config)
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
        _append_line(evidence_path, sample, maximum_bytes=int(config.value["maximum_evidence_bytes"]))
        atomic_write_json(state_path, _next_state(config, sample, evidence_path.stat().st_size))
        return sample
    finally:
        os.close(descriptor)


def snapshot(config: Config) -> tuple[dict[str, Any], Iterator[dict[str, Any]]]:
    """Pin a committed prefix under the collector lock; replay outside it."""
    state_path = Path(config.value["state_file"])
    lock = os.open(state_path.with_suffix(".lock"), os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        head = _head(config)
        descriptor = os.open(config.value["evidence_file"], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size != head["verified_bytes"]
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


def evaluate(samples: Iterable[Mapping[str, Any]], *, config: Config, now: datetime) -> dict[str, Any]:
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
        ),
        0,
    )
    first_time: datetime | None = None
    source_unavailable_count = dict.fromkeys(ROLES, 0)
    pi_advisories: set[str] = set()
    healthy_final = False
    live_checkpoint: dict[str, Any] | None = None
    history_policy = validate_policy(config.value.get("effect_history_policy"))
    final_effect_history: dict[str, Any] | None = None
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
                bucket = min(45001, math.ceil(gap * 1000))
                gap_histogram[bucket] = gap_histogram.get(bucket, 0) + 1
                for threshold in gap_exceeded:
                    gap_exceeded[threshold] += int(gap > int(threshold))
            if gap <= 0 or gap > 45:
                blockers.add("COLLECTOR_GAP_OR_CLOCK_REGRESSION")
        first_time = first_time or current
        previous_time, previous_hash = current, sample["sample_hash"]
        if not isinstance(sample["inputs"], dict) or set(sample["inputs"]) != set(ROLES):
            blockers.add("SAMPLE_INPUT_ROLES_INVALID")
            continue
        live_checkpoint = advance(
            live_checkpoint, sample["inputs"], bindings=config.bindings, keys=config.keys, now=current, history_policy=history_policy
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
                checked = admit_facts(packet, binding=config.bindings[role], keys=config.keys, now=current)
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
        elif (
            safety.get("command_delivery_enabled") is not False
            or safety.get("runtime_operating_mode") != "NO_ACTION"
            or type(safety.get("control_capability_count")) is not int
            or safety.get("control_capability_count") != 0
            or safety.get("integrity") != "ok"
            or not isinstance(safety.get("action_table_counts"), dict)
            or set(safety["action_table_counts"]) != set(ACTION_TABLES)
            or any(type(v) is not int or v != 0 for v in safety["action_table_counts"].values())
        ):
            blockers.add("CRA_NO_ACTION_SAFETY_VIOLATED")
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
            if (
                not nonnegative_integer(physical)
                or effects.get("integrity") != "ok"
                or effects.get("duplicate_attempt_count") != 0
                or effects.get("unauthorized_effect_count") != 0
            ):
                blockers.add("DELL_EFFECT_SAFETY_VIOLATED")
            elif previous_effects is not None and physical < previous_effects:
                blockers.add("DELL_EFFECT_COUNTER_REGRESSION")
            elif nonnegative_integer(physical):
                current_physical = physical
                if initial_effects is None:
                    initial_effects = physical
                if previous_effects is not None and physical > previous_effects:
                    effect_attempt_increase_count += physical - previous_effects
                    blockers.add("NO_ACTION_PHYSICAL_EFFECT_DETECTED")
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
        dell_network, arena_network = _health(network, current, 45), _health(arena.get("network"), current, 45)
        domains["dell_network_down"] += int(dell_network == "DOWN")
        domains["arena_network_down"] += int(arena_network == "DOWN")
        domains["both_network_down"] += int(dell_network == arena_network == "DOWN")
        domains["dell_only_network_down"] += int(dell_network == "DOWN" and arena_network == "UP")
        domains["arena_only_network_down"] += int(arena_network == "DOWN" and dell_network == "UP")
        domains["platform_ingest_down"] += int(live_observation.platform == "DOWN")
        domains["control_path_down"] += int(live_observation.control_path == "DOWN")
        window.observe(live_observation)
        if config.value.get("require_owner_lifecycle_evidence") is True:
            if live_observation.child_lifecycle is None:
                unknowns.add("DELL_OWNER_LIFECYCLE_EVIDENCE_REQUIRED")
            elif live_observation.child_lifecycle == "UNKNOWN" and not was_recovering and window.active is None:
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
            elif any(activation.get(name) is False for name in ("bounded_termination_enabled", "rw_timeout_enabled")):
                blockers.add("BOUNDED_TERMINATION_NOT_ACTIVE")
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
        if had_baseline and window.active is None and window.current_health == "UNKNOWN":
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
            p95 = bucket / 1000 if bucket <= 45000 else None
            break
    result = {
        "schema": GATE_SCHEMA,
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


def gate(config: Config, *, now: datetime | None = None) -> dict[str, Any]:
    head, samples = snapshot(config)
    result = evaluate(samples, config=config, now=now or datetime.now(UTC))
    if result["sample_count"] != head["sample_count"] or result["last_sample_hash"] != head["last_sample_hash"]:
        raise ValueError("RECOVERY_SOAK_REPLAY_HEAD_MISMATCH")
    result["evidence_verified_bytes"] = head["verified_bytes"]
    result["evidence_limit_bytes"] = config.value["maximum_evidence_bytes"]
    duration = float(result["duration_seconds"])
    projection = head["verified_bytes"] / duration * 604800 if duration > 0 else None
    result["projected_seven_day_bytes"] = projection
    result["projected_safety_margin_bytes"] = projection * 1.5 if projection is not None else None
    if projection is None:
        if result["soak_status"] == "PASS":
            result["status"] = result["soak_status"] = "NOT_YET_ELIGIBLE"
        result["eligible"] = False
        result["pending_reasons"].append("EVIDENCE_CAPACITY_NOT_YET_MEASURABLE")
    elif projection * 1.5 > config.value["maximum_evidence_bytes"]:
        result["status"], result["soak_status"], result["eligible"] = "UNKNOWN", None, False
        result["formal_evaluation_performed"] = False
        result["harness_classification"] = "ENVIRONMENT_FAILURE"
        result["blockers"].append("EVIDENCE_CAPACITY_NOT_PROVEN")
    result["operator_status"] = classify_gate(result)
    return result


def watchdog(config: Config, *, now: datetime | None = None) -> dict[str, Any]:
    """O(1) local check. Formal history replay remains a separate operation."""
    current = now or datetime.now(UTC)
    blockers: set[str] = set()
    head = _head(config)
    evidence_path = Path(config.value["evidence_file"])
    tail = _last_line(evidence_path, maximum_bytes=int(config.value["maximum_evidence_bytes"]))
    # A racing append is not interpreted as a healthy synchronized snapshot;
    # the next cycle can converge without modifying the history.
    if (
        tail is None
        or tail.get("sample_hash") != head["last_sample_hash"]
        or tail.get("sample_hash") != digest(tail)
        or tail.get("live_recovery") != head["live_recovery"]
        or evidence_path.stat().st_size != head["verified_bytes"]
    ):
        blockers.add("LIVE_STATE_EVIDENCE_NOT_SYNCHRONIZED")
    if not source_fresh(head["last_observed_at"], current, 45):
        blockers.add("COLLECTOR_STALE")
    recovery: dict[str, Any] = {"live_health": "UNKNOWN"}
    if head["live_recovery"] is None:
        blockers.add("LIVE_RECOVERY_CHECKPOINT_MISSING")
    else:
        checkpoint = head["live_recovery"]
        window = RecoveryWindow.restore(checkpoint["window"])
        recovery = window.finish(current)
        blockers.update(checkpoint["integrity_blockers"])
        blockers.update(recovery["blockers"])
    if "COLLECTOR_STALE" in blockers:
        recovery["live_health"] = "UNKNOWN"
    formal_status = "NOT_EVALUATED"
    formal_age = None
    try:
        formal = strict_object(Path(config.value["gate_file"]))
        formal_age = (current - timestamp(formal["evaluated_at"])).total_seconds()
        if (
            formal.get("schema") != GATE_SCHEMA
            or formal.get("config_sha256") != config.identity
            or formal.get("epoch_id") != config.value["epoch_id"]
            or not 0 <= formal_age <= 7200
            or formal.get("sample_count", -1) > head["sample_count"]
            or (formal.get("sample_count") == head["sample_count"] and formal.get("last_sample_hash") != head["last_sample_hash"])
        ):
            blockers.add("FORMAL_GATE_STALE_OR_UNBOUND")
        else:
            formal_status = formal.get("soak_status") or "NOT_EVALUATED"
            if formal.get("harness_classification") not in {"PASS", "SUT_FAILURE"}:
                blockers.add("FORMAL_GATE_EVALUATION_UNTRUSTED")
            elif formal_status == "FAIL":
                blockers.add("FORMAL_GATE_NOT_HEALTHY")
    except (OSError, ValueError, KeyError, TypeError):
        blockers.add("FORMAL_GATE_UNAVAILABLE")
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
        except (OSError, ValueError, TypeError, KeyError, RecoveryControlError):
            result = {
                "schema": WATCHDOG_SCHEMA if args.watchdog else GATE_SCHEMA,
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
