from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator, FormatChecker

from cra_dell_recovery.canonical import KeyRing, Signer, canonical_json
from cra_dell_recovery.time import isoformat_utc, parse_utc

from .host_status import atomic_write_json, read_object, read_regular_bytes

SCHEMA = "cra.resilient_host_status.v3"
STATE_SCHEMA = "cra.resilient_host_status_state.v2"
EVENT_SCHEMA = "cra.resilient_host_status_transition_event.v1"
HEALTH_EVENT_SCHEMA = "cra.resilient_host_status_health_transition.v1"
STATUS_PATH = "/v3/no-action-soak-status/latest"
REASON = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
COMPONENT_STATES = frozenset({"FRESH", "INVALID", "STALE", "MISSING", "ERROR"})
REPORT_STATES = frozenset(
    {
        "READY",
        "DEGRADED_SOURCE",
        "DEGRADED_OBSERVABILITY",
        "RECOVERING",
        "MAINTENANCE",
        "COMMUNICATION_DEGRADED",
        "SOURCE_UNREACHABLE",
    }
)
PUBLISHER_FAILURE_REASON = "PUBLISHER_INVOCATION_FAILED"


@dataclass(frozen=True)
class ComponentStatus:
    name: str
    state: str
    reason_code: str
    observed_at: datetime | None = None
    valid_until: datetime | None = None
    identity_sha256: str | None = None

    def value(self, *, now: datetime) -> dict[str, Any]:
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", self.name):
            raise ValueError("RESILIENT_STATUS_COMPONENT_NAME_INVALID")
        if self.state not in COMPONENT_STATES:
            raise ValueError("RESILIENT_STATUS_COMPONENT_STATE_INVALID")
        if REASON.fullmatch(self.reason_code) is None:
            raise ValueError("RESILIENT_STATUS_COMPONENT_REASON_INVALID")
        if self.identity_sha256 is not None and SHA256.fullmatch(self.identity_sha256) is None:
            raise ValueError("RESILIENT_STATUS_COMPONENT_IDENTITY_INVALID")
        if (self.observed_at is None) != (self.valid_until is None):
            raise ValueError("RESILIENT_STATUS_COMPONENT_TIME_PARTIAL")
        if self.observed_at is not None and self.valid_until is not None:
            observed = self.observed_at.astimezone(UTC)
            valid_until = self.valid_until.astimezone(UTC)
            if observed > now or observed >= valid_until:
                raise ValueError("RESILIENT_STATUS_COMPONENT_TIME_INVALID")
            if self.state == "FRESH" and valid_until <= now:
                raise ValueError("RESILIENT_STATUS_COMPONENT_FRESH_EXPIRED")
            if self.state == "STALE" and valid_until > now:
                raise ValueError("RESILIENT_STATUS_COMPONENT_STALE_FUTURE")
        elif self.state in {"FRESH", "STALE"}:
            raise ValueError("RESILIENT_STATUS_COMPONENT_TIME_REQUIRED")
        return {
            "name": self.name,
            "state": self.state,
            "reason_code": self.reason_code,
            "observed_at": isoformat_utc(self.observed_at) if self.observed_at is not None else None,
            "valid_until": isoformat_utc(self.valid_until) if self.valid_until is not None else None,
            "identity_sha256": self.identity_sha256,
        }


def _component_value_for_report(component: ComponentStatus, *, now: datetime) -> dict[str, Any]:
    """Keep a single future-dated component from suppressing the whole report."""

    if component.observed_at is not None and component.observed_at.astimezone(UTC) > now:
        return ComponentStatus(
            component.name,
            "INVALID",
            f"{component.name.upper()}_OBSERVED_IN_FUTURE",
            identity_sha256=component.identity_sha256,
        ).value(now=now)
    return component.value(now=now)


def evidence_valid_until(value: Mapping[str, Any]) -> datetime:
    """Return the earliest deadline required to keep a signed report decodable.

    Only evidence represented as current is part of the deadline. STALE/ERROR
    components remain transportable as degraded evidence instead of making
    their outer diagnostic report immediately expired.
    """

    observed_at = parse_utc(str(value["observed_at"]))
    deadlines = [parse_utc(str(value["report_lease_until"]))]
    for component in value["components"]:
        if component["state"] == "FRESH" and component["valid_until"] is not None:
            deadlines.append(parse_utc(str(component["valid_until"])))
    current = value["current"]
    if current["source_valid_until"] is not None:
        source_deadline = parse_utc(str(current["source_valid_until"]))
        if source_deadline > observed_at:
            deadlines.append(source_deadline)
    upstream = value.get("upstream_status")
    if isinstance(upstream, Mapping):
        deadlines.append(evidence_valid_until(upstream))
    return min(deadlines)


@dataclass(frozen=True)
class ExactTransitionBinding:
    effect_request_id: str
    effect_scope_id: str
    before_target_identity_sha256: str | None
    after_target_identity_sha256: str | None

    def __post_init__(self) -> None:
        if not self.effect_request_id or len(self.effect_request_id) > 256:
            raise ValueError("RESILIENT_STATUS_EFFECT_REQUEST_ID_INVALID")
        if not self.effect_scope_id or len(self.effect_scope_id) > 256:
            raise ValueError("RESILIENT_STATUS_EFFECT_SCOPE_ID_INVALID")
        for value in (self.before_target_identity_sha256, self.after_target_identity_sha256):
            if value is not None and SHA256.fullmatch(value) is None:
                raise ValueError("RESILIENT_STATUS_EFFECT_TARGET_IDENTITY_INVALID")


def _empty_last_good() -> dict[str, Any]:
    return {
        "present": False,
        "diagnostic_only": True,
        "observed_at": None,
        "valid_until": None,
        "retained_until": None,
        "target_identity_sha256": None,
        "source_payload_sha256": None,
        "origin_host_id": None,
    }


def _initial_state(host_id: str, boot_id: str) -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "host_id": host_id,
        "host_boot_id": boot_id,
        "producer_instance_id": f"resilient-status-{uuid.uuid4()}",
        "producer_sequence": 0,
        "journal_id": f"resilient-transition-{uuid.uuid4()}",
        "transition_high_watermark": 0,
        "last_event_hash": None,
        "last_target_identity_sha256": None,
        "open_episodes": [],
        "last_good": _empty_last_good(),
        "health_journal_id": f"resilient-health-{uuid.uuid4()}",
        "health_high_watermark": 0,
        "last_health_event_hash": None,
        "last_report_state": None,
        "last_report_reason_codes": [],
    }


def _new_boot_state(state: Mapping[str, Any], boot_id: str) -> dict[str, Any]:
    result = dict(state)
    result["host_boot_id"] = boot_id
    result["producer_instance_id"] = f"resilient-status-{uuid.uuid4()}"
    result["producer_sequence"] = 0
    result["health_journal_id"] = f"resilient-health-{uuid.uuid4()}"
    result["health_high_watermark"] = 0
    result["last_health_event_hash"] = None
    result["last_report_state"] = None
    result["last_report_reason_codes"] = []
    return result


def _validate_state(value: Mapping[str, Any], *, host_id: str, boot_id: str) -> dict[str, Any]:
    required = {
        "schema",
        "host_id",
        "host_boot_id",
        "producer_instance_id",
        "producer_sequence",
        "journal_id",
        "transition_high_watermark",
        "last_event_hash",
        "last_target_identity_sha256",
        "open_episodes",
        "last_good",
        "health_journal_id",
        "health_high_watermark",
        "last_health_event_hash",
        "last_report_state",
        "last_report_reason_codes",
    }
    state = dict(value)
    if set(state) != required or state.get("schema") != STATE_SCHEMA or state.get("host_id") != host_id:
        raise ValueError("RESILIENT_STATUS_STATE_IDENTITY_INVALID")
    if state.get("host_boot_id") != boot_id:
        state = _new_boot_state(state, boot_id)
    for name in ("producer_sequence", "transition_high_watermark", "health_high_watermark"):
        raw = state.get(name)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise ValueError("RESILIENT_STATUS_STATE_COUNTER_INVALID")
    for name in ("producer_instance_id", "journal_id", "health_journal_id"):
        if not isinstance(state.get(name), str) or not state[name]:
            raise ValueError("RESILIENT_STATUS_STATE_VALUE_INVALID")
    for name in ("last_event_hash", "last_target_identity_sha256", "last_health_event_hash"):
        raw = state.get(name)
        if raw is not None and (not isinstance(raw, str) or SHA256.fullmatch(raw) is None):
            raise ValueError("RESILIENT_STATUS_STATE_DIGEST_INVALID")
    if not isinstance(state.get("last_good"), dict):
        raise ValueError("RESILIENT_STATUS_LAST_GOOD_STATE_INVALID")
    if state.get("last_report_state") is not None and state["last_report_state"] not in REPORT_STATES:
        raise ValueError("RESILIENT_STATUS_LAST_REPORT_STATE_INVALID")
    last_reasons = state.get("last_report_reason_codes")
    if not isinstance(last_reasons, list) or any(REASON.fullmatch(str(reason)) is None for reason in last_reasons):
        raise ValueError("RESILIENT_STATUS_LAST_REPORT_REASONS_INVALID")
    open_episodes = state.get("open_episodes")
    if not isinstance(open_episodes, list) or len(open_episodes) > 64 or not all(isinstance(item, dict) for item in open_episodes):
        raise ValueError("RESILIENT_STATUS_OPEN_EPISODES_INVALID")
    episode_ids = [str(item.get("episode_id")) for item in open_episodes]
    if len(episode_ids) != len(set(episode_ids)):
        raise ValueError("RESILIENT_STATUS_OPEN_EPISODE_DUPLICATE")
    return state


def _append_event(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(json.dumps(dict(value), separators=(",", ":"), sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _event_hash(value: Mapping[str, Any]) -> str:
    unsigned = {name: item for name, item in value.items() if name != "event_hash"}
    return hashlib.sha256(canonical_json(unsigned)).hexdigest()


def _journal_events(path: Path, *, journal_id: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("RESILIENT_STATUS_JOURNAL_NOT_REGULAR")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("RESILIENT_STATUS_JOURNAL_PERMISSIONS_UNSAFE")
        if metadata.st_size > 64 * 1024 * 1024:
            raise ValueError("RESILIENT_STATUS_JOURNAL_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(64 * 1024 * 1024 + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > 64 * 1024 * 1024:
        raise ValueError("RESILIENT_STATUS_JOURNAL_TOO_LARGE")
    if raw and not raw.endswith(b"\n"):
        raise ValueError("RESILIENT_STATUS_JOURNAL_PARTIAL_TAIL")
    result: list[dict[str, Any]] = []
    previous_hash: str | None = None
    for sequence, raw_line in enumerate(raw.splitlines(), start=1):
        try:
            value = json.loads(raw_line)
        except json.JSONDecodeError as error:
            raise ValueError("RESILIENT_STATUS_JOURNAL_JSON_INVALID") from error
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "journal_id",
            "sequence",
            "observed_at",
            "previous_event_hash",
            "event_action",
            "episode",
            "causal_state",
            "event_hash",
        }:
            raise ValueError("RESILIENT_STATUS_JOURNAL_EVENT_FIELDS_INVALID")
        if (
            value.get("schema") != EVENT_SCHEMA
            or value.get("journal_id") != journal_id
            or value.get("sequence") != sequence
            or value.get("previous_event_hash") != previous_hash
            or value.get("event_action") not in {"OBSERVED", "CAUSAL_BOUND"}
            or value.get("causal_state") not in {"EXACT", "UNBOUND"}
            or value.get("event_hash") != _event_hash(value)
            or not isinstance(value.get("episode"), dict)
        ):
            raise ValueError("RESILIENT_STATUS_JOURNAL_CHAIN_INVALID")
        parse_utc(str(value.get("observed_at")))
        result.append(dict(value))
        previous_hash = str(value["event_hash"])
    return result


def _health_journal_events(path: Path, *, journal_id: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_NOT_REGULAR")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_PERMISSIONS_UNSAFE")
        if metadata.st_size > 64 * 1024 * 1024:
            raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(64 * 1024 * 1024 + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > 64 * 1024 * 1024:
        raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_TOO_LARGE")
    if raw and not raw.endswith(b"\n"):
        raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_PARTIAL_TAIL")
    result: list[dict[str, Any]] = []
    previous_hash: str | None = None
    previous_state: str | None = None
    for sequence, raw_line in enumerate(raw.splitlines(), start=1):
        try:
            value = json.loads(raw_line)
        except json.JSONDecodeError as error:
            raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_JSON_INVALID") from error
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "journal_id",
            "sequence",
            "producer_sequence",
            "observed_at",
            "previous_event_hash",
            "previous_state",
            "state",
            "reason_codes",
            "event_hash",
        }:
            raise ValueError("RESILIENT_STATUS_HEALTH_EVENT_FIELDS_INVALID")
        reasons = value.get("reason_codes")
        if (
            value.get("schema") != HEALTH_EVENT_SCHEMA
            or value.get("journal_id") != journal_id
            or value.get("sequence") != sequence
            or not isinstance(value.get("producer_sequence"), int)
            or isinstance(value.get("producer_sequence"), bool)
            or int(value["producer_sequence"]) < 1
            or value.get("previous_event_hash") != previous_hash
            or value.get("previous_state") != previous_state
            or value.get("state") not in REPORT_STATES
            or not isinstance(reasons, list)
            or not reasons
            or len(reasons) != len(set(str(reason) for reason in reasons))
            or any(REASON.fullmatch(str(reason)) is None for reason in reasons)
            or value.get("event_hash") != _event_hash(value)
        ):
            raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_CHAIN_INVALID")
        parse_utc(str(value.get("observed_at")))
        result.append(dict(value))
        previous_hash = str(value["event_hash"])
        previous_state = str(value["state"])
    return result


def _reconcile_health_journal(state: dict[str, Any], journal: Path) -> None:
    events = _health_journal_events(journal, journal_id=str(state["health_journal_id"]))
    watermark = int(state["health_high_watermark"])
    if watermark > len(events):
        raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_REGRESSION")
    if watermark and events[watermark - 1]["event_hash"] != state["last_health_event_hash"]:
        raise ValueError("RESILIENT_STATUS_HEALTH_JOURNAL_STATE_FORK")
    for event in events[watermark:]:
        state["health_high_watermark"] = int(event["sequence"])
        state["last_health_event_hash"] = str(event["event_hash"])
        state["last_report_state"] = str(event["state"])
        state["last_report_reason_codes"] = list(event["reason_codes"])


def _append_health_event(
    state: dict[str, Any],
    journal: Path,
    *,
    producer_sequence: int,
    now: datetime,
    report_state: str,
    reason_codes: Sequence[str],
) -> None:
    reasons = sorted(set(str(reason) for reason in reason_codes))
    event: dict[str, Any] = {
        "schema": HEALTH_EVENT_SCHEMA,
        "journal_id": state["health_journal_id"],
        "sequence": int(state["health_high_watermark"]) + 1,
        "producer_sequence": producer_sequence,
        "observed_at": isoformat_utc(now),
        "previous_event_hash": state["last_health_event_hash"],
        "previous_state": state["last_report_state"],
        "state": report_state,
        "reason_codes": reasons,
    }
    event["event_hash"] = _event_hash(event)
    _append_event(journal, event)
    state["health_high_watermark"] = int(event["sequence"])
    state["last_health_event_hash"] = str(event["event_hash"])
    state["last_report_state"] = report_state
    state["last_report_reason_codes"] = reasons


def record_publisher_failure(
    *,
    state_file: Path,
    host_id: str,
    host_boot_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Durably retain a failed publisher invocation for the next signed report.

    A failed invocation cannot safely replace the last signed output. Recording it
    in the producer's hash-chained health journal makes the failure visible after
    the next successful publication instead of letting last-known-good hide it.
    """

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = state_file.with_name(f"{state_file.name}.lock")
    lock_descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = (
            _validate_state(
                read_object(state_file, maximum_bytes=512 * 1024),
                host_id=host_id,
                boot_id=host_boot_id,
            )
            if state_file.exists()
            else _initial_state(host_id, host_boot_id)
        )
        health_journal = state_file.with_name(f"{state_file.stem}-{state['health_journal_id']}.jsonl")
        _reconcile_health_journal(state, health_journal)
        next_producer_sequence = int(state["producer_sequence"]) + 1
        reasons = [PUBLISHER_FAILURE_REASON]
        if state["last_report_state"] != "DEGRADED_OBSERVABILITY" or state["last_report_reason_codes"] != reasons:
            _append_health_event(
                state,
                health_journal,
                producer_sequence=next_producer_sequence,
                now=current_time,
                report_state="DEGRADED_OBSERVABILITY",
                reason_codes=reasons,
            )
        state["producer_sequence"] = next_producer_sequence
        atomic_write_json(state_file, state, mode=0o600)
        return state
    finally:
        os.close(lock_descriptor)


def _apply_event(state: dict[str, Any], event: Mapping[str, Any]) -> None:
    episode = dict(event["episode"])
    if event["event_action"] == "OBSERVED":
        state["last_target_identity_sha256"] = episode.get("after_target_identity_sha256")
        if event["causal_state"] == "UNBOUND":
            if any(item.get("episode_id") == episode.get("episode_id") for item in state["open_episodes"]):
                raise ValueError("RESILIENT_STATUS_OPEN_EPISODE_DUPLICATE")
            state["open_episodes"].append(episode)
    else:
        matches = [item for item in state["open_episodes"] if item.get("episode_id") == episode.get("episode_id")]
        if len(matches) != 1 or event["causal_state"] != "EXACT":
            raise ValueError("RESILIENT_STATUS_CAUSAL_BINDING_ORPHANED")
        state["open_episodes"] = [item for item in state["open_episodes"] if item.get("episode_id") != episode.get("episode_id")]
    state["transition_high_watermark"] = int(event["sequence"])
    state["last_event_hash"] = str(event["event_hash"])


def _reconcile_journal(state: dict[str, Any], journal: Path) -> None:
    events = _journal_events(journal, journal_id=str(state["journal_id"]))
    watermark = int(state["transition_high_watermark"])
    if watermark > len(events):
        raise ValueError("RESILIENT_STATUS_JOURNAL_REGRESSION")
    if watermark and events[watermark - 1]["event_hash"] != state["last_event_hash"]:
        raise ValueError("RESILIENT_STATUS_JOURNAL_STATE_FORK")
    for event in events[watermark:]:
        _apply_event(state, event)


def _append_state_event(
    state: dict[str, Any],
    journal: Path,
    *,
    now: datetime,
    event_action: str,
    episode: Mapping[str, Any],
    causal_state: str,
) -> None:
    event: dict[str, Any] = {
        "schema": EVENT_SCHEMA,
        "journal_id": state["journal_id"],
        "sequence": int(state["transition_high_watermark"]) + 1,
        "observed_at": isoformat_utc(now),
        "previous_event_hash": state["last_event_hash"],
        "event_action": event_action,
        "episode": dict(episode),
        "causal_state": causal_state,
    }
    event["event_hash"] = _event_hash(event)
    _append_event(journal, event)
    _apply_event(state, event)


def _transition_event(
    state: dict[str, Any],
    journal: Path,
    *,
    now: datetime,
    before: str | None,
    after: str | None,
    binding: ExactTransitionBinding | None,
) -> None:
    if before is None and after is not None:
        event_type = "TARGET_RECOVERED"
    elif before is not None and after is None:
        event_type = "TARGET_LOST"
    else:
        event_type = "TARGET_CHANGED"
    episode = {
        "episode_id": f"target-transition-{uuid.uuid4()}",
        "event_type": event_type,
        "started_at": isoformat_utc(now),
        "before_target_identity_sha256": before,
        "after_target_identity_sha256": after,
        "effect_request_id": binding.effect_request_id if binding is not None else None,
        "effect_scope_id": binding.effect_scope_id if binding is not None else None,
    }
    _append_state_event(
        state,
        journal,
        now=now,
        event_action="OBSERVED",
        episode=episode,
        causal_state="EXACT" if binding is not None else "UNBOUND",
    )


def _resolve_open_episode(
    state: dict[str, Any],
    journal: Path,
    *,
    now: datetime,
    binding: ExactTransitionBinding,
) -> bool:
    matches = [
        item
        for item in state["open_episodes"]
        if item.get("before_target_identity_sha256") == binding.before_target_identity_sha256
        and item.get("after_target_identity_sha256") == binding.after_target_identity_sha256
    ]
    if len(matches) != 1:
        return False
    episode = {
        **matches[0],
        "effect_request_id": binding.effect_request_id,
        "effect_scope_id": binding.effect_scope_id,
    }
    _append_state_event(
        state,
        journal,
        now=now,
        event_action="CAUSAL_BOUND",
        episode=episode,
        causal_state="EXACT",
    )
    return True


def _current_value(
    *,
    source_observed_at: datetime | None,
    source_valid_until: datetime | None,
    target_identity_sha256: str | None,
    source_payload_sha256: str | None,
    origin_host_id: str | None,
) -> dict[str, Any]:
    for value in (target_identity_sha256, source_payload_sha256):
        if value is not None and SHA256.fullmatch(value) is None:
            raise ValueError("RESILIENT_STATUS_CURRENT_DIGEST_INVALID")
    if (source_observed_at is None) != (source_valid_until is None):
        raise ValueError("RESILIENT_STATUS_CURRENT_TIME_PARTIAL")
    return {
        "source_observed_at": isoformat_utc(source_observed_at) if source_observed_at is not None else None,
        "source_valid_until": isoformat_utc(source_valid_until) if source_valid_until is not None else None,
        "target_identity_sha256": target_identity_sha256,
        "source_payload_sha256": source_payload_sha256,
        "origin_host_id": origin_host_id,
    }


def _retained_last_good(value: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    last_good = dict(value)
    if not last_good.get("present"):
        return _empty_last_good()
    try:
        retained_until = parse_utc(str(last_good["retained_until"]))
    except (KeyError, TypeError, ValueError):
        raise ValueError("RESILIENT_STATUS_LAST_GOOD_STATE_INVALID") from None
    return last_good if retained_until > now else _empty_last_good()


def build_resilient_status(
    *,
    state_file: Path,
    transition_journal: Path,
    schema_file: Path,
    private_key: Ed25519PrivateKey,
    key_id: str,
    role: str,
    host_id: str,
    release_id: str,
    host_boot_id: str,
    identity: Mapping[str, str],
    components: Sequence[ComponentStatus],
    source_observed_at: datetime | None,
    source_valid_until: datetime | None,
    target_identity_sha256: str | None,
    source_payload_sha256: str | None,
    origin_host_id: str | None,
    physical_effect_count: int | None,
    exact_transition_binding: ExactTransitionBinding | None = None,
    upstream_status: Mapping[str, Any] | None = None,
    track_target_transitions: bool = True,
    explicit_state: str | None = None,
    explicit_reason_codes: Sequence[str] = (),
    reporter_lease_seconds: float = 45.0,
    last_good_retention_seconds: float = 300.0,
    clock_state: str = "SYNCED",
    clock_uncertainty_ms: int | None = 0,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    if role not in {"dell", "arena"}:
        raise ValueError("RESILIENT_STATUS_ROLE_INVALID")
    if (role == "dell" and upstream_status is not None) or (upstream_status is not None and not isinstance(upstream_status, Mapping)):
        raise ValueError("RESILIENT_STATUS_UPSTREAM_INVALID")
    if not host_id or not release_id or not host_boot_id or not key_id:
        raise ValueError("RESILIENT_STATUS_IDENTITY_INVALID")
    if not 15 <= reporter_lease_seconds <= 300 or not 60 <= last_good_retention_seconds <= 86400:
        raise ValueError("RESILIENT_STATUS_RETENTION_INVALID")
    if clock_state not in {"SYNCED", "UNCERTAIN"}:
        raise ValueError("RESILIENT_STATUS_CLOCK_STATE_INVALID")
    if clock_uncertainty_ms is not None and (isinstance(clock_uncertainty_ms, bool) or not 0 <= clock_uncertainty_ms <= 86_400_000):
        raise ValueError("RESILIENT_STATUS_CLOCK_UNCERTAINTY_INVALID")
    if physical_effect_count is not None and (
        isinstance(physical_effect_count, bool) or not isinstance(physical_effect_count, int) or physical_effect_count < 0
    ):
        raise ValueError("RESILIENT_STATUS_PHYSICAL_EFFECT_COUNT_INVALID")
    required_identity = {
        "release_manifest_sha256",
        "runtime_manifest_sha256",
        "configuration_set_sha256",
        "maintenance_restart_policy_sha256",
    }
    if set(identity) != required_identity or any(SHA256.fullmatch(str(value)) is None for value in identity.values()):
        raise ValueError("RESILIENT_STATUS_RELEASE_IDENTITY_INVALID")
    component_values = [_component_value_for_report(component, now=current_time) for component in components]
    names = [str(value["name"]) for value in component_values]
    if not names or len(names) != len(set(names)):
        raise ValueError("RESILIENT_STATUS_COMPONENT_SET_INVALID")
    source_observed_in_future = source_observed_at is not None and source_observed_at.astimezone(UTC) > current_time
    if (
        source_valid_until is not None
        and source_observed_at is not None
        and source_valid_until.astimezone(UTC) <= source_observed_at.astimezone(UTC)
    ):
        raise ValueError("RESILIENT_STATUS_SOURCE_TIME_INVALID")

    state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = state_file.with_name(f"{state_file.name}.lock")
    lock_descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = (
            _validate_state(read_object(state_file, maximum_bytes=512 * 1024), host_id=host_id, boot_id=host_boot_id)
            if state_file.exists()
            else _initial_state(host_id, host_boot_id)
        )
        health_journal = state_file.with_name(f"{state_file.stem}-{state['health_journal_id']}.jsonl")
        _reconcile_journal(state, transition_journal)
        _reconcile_health_journal(state, health_journal)
        before_target = state.get("last_target_identity_sha256")
        transition_observable = not source_observed_in_future
        effective_target_identity_sha256 = target_identity_sha256 if transition_observable else None
        transition_recorded = False
        binding_resolved = False
        if exact_transition_binding is not None and transition_observable and before_target == effective_target_identity_sha256:
            binding_resolved = _resolve_open_episode(
                state,
                transition_journal,
                now=current_time,
                binding=exact_transition_binding,
            )
            if not binding_resolved:
                raise ValueError("RESILIENT_STATUS_CAUSAL_BINDING_ORPHANED")
        if (
            track_target_transitions
            and transition_observable
            and before_target != effective_target_identity_sha256
            and (before_target is not None or int(state["producer_sequence"]) > 0)
        ):
            if exact_transition_binding is not None and (
                exact_transition_binding.before_target_identity_sha256 != before_target
                or exact_transition_binding.after_target_identity_sha256 != effective_target_identity_sha256
            ):
                raise ValueError("RESILIENT_STATUS_CAUSAL_BINDING_TARGET_MISMATCH")
            _transition_event(
                state,
                transition_journal,
                now=current_time,
                before=before_target,
                after=effective_target_identity_sha256,
                binding=exact_transition_binding,
            )
            transition_recorded = True
        elif transition_observable and (not track_target_transitions or int(state["producer_sequence"]) == 0):
            state["last_target_identity_sha256"] = effective_target_identity_sha256

        current = _current_value(
            source_observed_at=source_observed_at if transition_observable else None,
            source_valid_until=source_valid_until if transition_observable else None,
            target_identity_sha256=effective_target_identity_sha256,
            source_payload_sha256=source_payload_sha256 if transition_observable else None,
            origin_host_id=origin_host_id if transition_observable else None,
        )
        components_ready = all(value["state"] == "FRESH" for value in component_values) and clock_state == "SYNCED"
        source_ready = (
            current["source_observed_at"] is not None
            and current["source_valid_until"] is not None
            and parse_utc(str(current["source_valid_until"])) > current_time
            and current["target_identity_sha256"] is not None
            and current["source_payload_sha256"] is not None
            and current["origin_host_id"] is not None
        )
        open_episodes = list(state["open_episodes"])
        report_state = "READY" if components_ready and source_ready and not open_episodes else "DEGRADED_SOURCE"
        nonfresh_component_names = {str(value["name"]) for value in component_values if value["state"] != "FRESH"}
        if (
            nonfresh_component_names
            and nonfresh_component_names <= {"publisher_resources"}
            and source_ready
            and not open_episodes
            and clock_state == "SYNCED"
        ):
            report_state = "DEGRADED_OBSERVABILITY"
        if open_episodes:
            report_state = "RECOVERING"
        if explicit_state is not None:
            if explicit_state not in REPORT_STATES:
                raise ValueError("RESILIENT_STATUS_EXPLICIT_STATE_INVALID")
            if explicit_state == "READY" and (not components_ready or not source_ready or open_episodes):
                raise ValueError("RESILIENT_STATUS_EXPLICIT_READY_UNSAFE")
            report_state = explicit_state

        reasons = set(explicit_reason_codes)
        reasons.update(value["reason_code"] for value in component_values if value["state"] != "FRESH")
        if source_observed_in_future:
            reasons.add("SOURCE_OBSERVED_IN_FUTURE")
        if open_episodes:
            reasons.add("TARGET_TRANSITION_UNBOUND")
        if report_state == "READY":
            reasons.add("ALL_REQUIRED_SOURCES_FRESH")
        elif not reasons:
            reasons.add("SOURCE_NOT_READY")
        if any(REASON.fullmatch(str(reason)) is None for reason in reasons):
            raise ValueError("RESILIENT_STATUS_REASON_INVALID")

        last_good = _retained_last_good(dict(state["last_good"]), now=current_time)
        if report_state == "READY":
            last_good = {
                "present": True,
                "diagnostic_only": True,
                "observed_at": current["source_observed_at"],
                "valid_until": current["source_valid_until"],
                "retained_until": isoformat_utc(current_time + timedelta(seconds=last_good_retention_seconds)),
                "target_identity_sha256": effective_target_identity_sha256,
                "source_payload_sha256": current["source_payload_sha256"],
                "origin_host_id": current["origin_host_id"],
            }
        state["last_good"] = last_good
        next_producer_sequence = int(state["producer_sequence"]) + 1
        sorted_reasons = sorted(reasons)
        if report_state != state["last_report_state"] or sorted_reasons != state["last_report_reason_codes"]:
            _append_health_event(
                state,
                health_journal,
                producer_sequence=next_producer_sequence,
                now=current_time,
                report_state=report_state,
                reason_codes=sorted_reasons,
            )
        state["producer_sequence"] = next_producer_sequence
        journal_event_tail = _journal_events(transition_journal, journal_id=str(state["journal_id"]))[-32:]
        health_event_tail = _health_journal_events(
            health_journal,
            journal_id=str(state["health_journal_id"]),
        )[-64:]
        causal_state = "UNBOUND" if state["open_episodes"] else ("EXACT" if transition_recorded or binding_resolved else "NONE")
        report_lease_candidates = [current_time + timedelta(seconds=reporter_lease_seconds)]
        report_lease_candidates.extend(
            parse_utc(str(value["valid_until"]))
            for value in component_values
            if value["state"] == "FRESH" and value["valid_until"] is not None
        )
        if current["source_valid_until"] is not None:
            report_lease_candidates.append(parse_utc(str(current["source_valid_until"])))
        if upstream_status is not None:
            report_lease_candidates.append(evidence_valid_until(upstream_status))
        report_lease_until = min(report_lease_candidates)
        if isoformat_utc(report_lease_until) <= isoformat_utc(current_time):
            raise ValueError("RESILIENT_STATUS_REPORT_EVIDENCE_LEASE_EXHAUSTED")
        unsigned: dict[str, Any] = {
            "schema": SCHEMA,
            "role": role,
            "host_id": host_id,
            "release_id": release_id,
            "producer_instance_id": state["producer_instance_id"],
            "producer_sequence": state["producer_sequence"],
            "host_boot_id": host_boot_id,
            "observed_at": isoformat_utc(current_time),
            "report_lease_until": isoformat_utc(report_lease_until),
            "state": report_state,
            "reason_codes": sorted_reasons,
            "clock": {"state": clock_state, "uncertainty_ms": clock_uncertainty_ms},
            "identity": dict(identity),
            "components": sorted(component_values, key=lambda value: str(value["name"])),
            "current": current,
            "last_good": last_good,
            "upstream_status": dict(upstream_status) if upstream_status is not None else None,
            "transition": {
                "journal_id": state["journal_id"],
                "high_watermark": state["transition_high_watermark"],
                "last_event_hash": state["last_event_hash"],
                "causal_state": causal_state,
                "open_episode_count": len(state["open_episodes"]),
                "open_episodes": state["open_episodes"],
                "event_tail": journal_event_tail,
            },
            "health_transition": {
                "journal_id": state["health_journal_id"],
                "high_watermark": state["health_high_watermark"],
                "last_event_hash": state["last_health_event_hash"],
                "event_tail": health_event_tail,
            },
            "control_capability_count": 0,
            "physical_effect_count": physical_effect_count,
            "key_id": key_id,
        }
        signed = Signer(key_id, private_key).sign(unsigned)
        validator = Draft202012Validator(
            json.loads(schema_file.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )
        validator.validate(signed)
        atomic_write_json(state_file, state, mode=0o600)
        return signed
    finally:
        os.close(lock_descriptor)


class ResilientHostStatusContract:
    def __init__(
        self,
        schema_file: Path,
        verifier: KeyRing,
        *,
        key_id: str,
        expected_role: str,
        expected_host_id: str,
        expected_release_id: str,
        maximum_report_lease_seconds: float = 300.0,
        maximum_clock_skew_seconds: float = 5.0,
        upstream_contract: ResilientHostStatusContract | None = None,
    ) -> None:
        self.validator = Draft202012Validator(
            json.loads(schema_file.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )
        self.verifier = verifier
        self.key_id = key_id
        self.expected_role = expected_role
        self.expected_host_id = expected_host_id
        self.expected_release_id = expected_release_id
        self.maximum_report_lease_seconds = maximum_report_lease_seconds
        self.maximum_clock_skew_seconds = maximum_clock_skew_seconds
        self.upstream_contract = upstream_contract

    def decode(self, value: Mapping[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
        payload = dict(value)
        self.validator.validate(payload)
        self.verifier.verify(payload)
        if payload["key_id"] != self.key_id:
            raise ValueError("RESILIENT_STATUS_KEY_ID_MISMATCH")
        if payload["role"] != self.expected_role or payload["host_id"] != self.expected_host_id:
            raise ValueError("RESILIENT_STATUS_HOST_IDENTITY_MISMATCH")
        if payload["release_id"] != self.expected_release_id:
            raise ValueError("RESILIENT_STATUS_RELEASE_ID_MISMATCH")
        current = (now or datetime.now(UTC)).astimezone(UTC)
        observed = parse_utc(str(payload["observed_at"]))
        lease = parse_utc(str(payload["report_lease_until"]))
        if observed > current + timedelta(seconds=self.maximum_clock_skew_seconds):
            raise ValueError("RESILIENT_STATUS_OBSERVED_IN_FUTURE")
        if observed >= lease or (lease - observed).total_seconds() > self.maximum_report_lease_seconds:
            raise ValueError("RESILIENT_STATUS_REPORT_LEASE_INVALID")
        if lease <= current:
            raise ValueError("RESILIENT_STATUS_REPORT_EXPIRED")
        if lease > evidence_valid_until(payload):
            raise ValueError("RESILIENT_STATUS_REPORT_EXCEEDS_EVIDENCE_VALIDITY")
        components = list(payload["components"])
        names = [str(item["name"]) for item in components]
        if len(names) != len(set(names)):
            raise ValueError("RESILIENT_STATUS_COMPONENT_DUPLICATE")
        for component in components:
            component_observed = component["observed_at"]
            component_valid_until = component["valid_until"]
            if (component_observed is None) != (component_valid_until is None):
                raise ValueError("RESILIENT_STATUS_COMPONENT_TIME_PARTIAL")
            if component_observed is not None:
                component_observed_time = parse_utc(str(component_observed))
                component_valid_time = parse_utc(str(component_valid_until))
                if component_observed_time >= component_valid_time:
                    raise ValueError("RESILIENT_STATUS_COMPONENT_TIME_INVALID")
                if component_observed_time > current + timedelta(seconds=self.maximum_clock_skew_seconds):
                    raise ValueError("RESILIENT_STATUS_COMPONENT_OBSERVED_IN_FUTURE")
                if component["state"] == "FRESH" and component_valid_time <= current:
                    raise ValueError("RESILIENT_STATUS_COMPONENT_FRESH_EXPIRED")
                if component["state"] == "STALE" and component_valid_time > current:
                    raise ValueError("RESILIENT_STATUS_COMPONENT_STALE_FUTURE")
        if payload["last_good"]["diagnostic_only"] is not True:
            raise ValueError("RESILIENT_STATUS_LAST_GOOD_AUTHORITY_INVALID")
        current_source = payload["current"]
        source_observed = current_source["source_observed_at"]
        source_valid_until = current_source["source_valid_until"]
        if (source_observed is None) != (source_valid_until is None):
            raise ValueError("RESILIENT_STATUS_CURRENT_TIME_PARTIAL")
        source_fresh = False
        if source_observed is not None:
            source_observed_time = parse_utc(str(source_observed))
            source_valid_time = parse_utc(str(source_valid_until))
            if source_observed_time >= source_valid_time:
                raise ValueError("RESILIENT_STATUS_CURRENT_TIME_INVALID")
            if source_observed_time > current + timedelta(seconds=self.maximum_clock_skew_seconds):
                raise ValueError("RESILIENT_STATUS_CURRENT_OBSERVED_IN_FUTURE")
            source_fresh = source_valid_time > current
        current_complete = all(
            current_source[name] is not None
            for name in ("source_observed_at", "source_valid_until", "target_identity_sha256", "source_payload_sha256", "origin_host_id")
        )
        transition = payload["transition"]
        if transition["open_episode_count"] != len(transition["open_episodes"]):
            raise ValueError("RESILIENT_STATUS_OPEN_EPISODE_COUNT_MISMATCH")
        open_episode_ids = [str(item["episode_id"]) for item in transition["open_episodes"]]
        if len(open_episode_ids) != len(set(open_episode_ids)):
            raise ValueError("RESILIENT_STATUS_OPEN_EPISODE_DUPLICATE")
        if transition["causal_state"] == "UNBOUND" and not transition["open_episodes"]:
            raise ValueError("RESILIENT_STATUS_CAUSAL_STATE_INVALID")
        if transition["open_episodes"] and transition["causal_state"] != "UNBOUND":
            raise ValueError("RESILIENT_STATUS_CAUSAL_STATE_INVALID")
        event_tail = transition["event_tail"]
        if transition["high_watermark"] == 0:
            if event_tail or transition["last_event_hash"] is not None:
                raise ValueError("RESILIENT_STATUS_EVENT_TAIL_INVALID")
        else:
            if (
                not event_tail
                or event_tail[-1]["sequence"] != transition["high_watermark"]
                or event_tail[-1]["event_hash"] != transition["last_event_hash"]
            ):
                raise ValueError("RESILIENT_STATUS_EVENT_TAIL_INVALID")
            expected_start = transition["high_watermark"] - len(event_tail) + 1
            previous_hash = event_tail[0]["previous_event_hash"]
            for sequence, event in enumerate(event_tail, start=expected_start):
                if (
                    event["sequence"] != sequence
                    or event["previous_event_hash"] != previous_hash
                    or event["event_hash"] != _event_hash(event)
                ):
                    raise ValueError("RESILIENT_STATUS_EVENT_TAIL_CHAIN_INVALID")
                previous_hash = event["event_hash"]
        health = payload["health_transition"]
        health_tail = health["event_tail"]
        if health["high_watermark"] == 0:
            raise ValueError("RESILIENT_STATUS_HEALTH_HISTORY_EMPTY")
        if (
            not health_tail
            or health_tail[-1]["sequence"] != health["high_watermark"]
            or health_tail[-1]["event_hash"] != health["last_event_hash"]
            or health_tail[-1]["state"] != payload["state"]
            or health_tail[-1]["reason_codes"] != payload["reason_codes"]
            or int(health_tail[-1]["producer_sequence"]) > int(payload["producer_sequence"])
        ):
            raise ValueError("RESILIENT_STATUS_HEALTH_EVENT_TAIL_INVALID")
        expected_health_start = int(health["high_watermark"]) - len(health_tail) + 1
        previous_health_hash = health_tail[0]["previous_event_hash"]
        previous_health_state = health_tail[0]["previous_state"]
        previous_health_time: datetime | None = None
        previous_producer_sequence = 0
        for sequence, event in enumerate(health_tail, start=expected_health_start):
            event_time = parse_utc(str(event["observed_at"]))
            if (
                event["sequence"] != sequence
                or event["previous_event_hash"] != previous_health_hash
                or event["previous_state"] != previous_health_state
                or event["event_hash"] != _event_hash(event)
                or (previous_health_time is not None and event_time < previous_health_time)
                or event_time > observed
                or int(event["producer_sequence"]) <= previous_producer_sequence
                or int(event["producer_sequence"]) > int(payload["producer_sequence"])
            ):
                raise ValueError("RESILIENT_STATUS_HEALTH_EVENT_TAIL_CHAIN_INVALID")
            previous_health_hash = event["event_hash"]
            previous_health_state = event["state"]
            previous_health_time = event_time
            previous_producer_sequence = int(event["producer_sequence"])
        all_components_fresh = all(item["state"] == "FRESH" for item in components)
        if payload["state"] == "READY" and (
            not all_components_fresh
            or payload["clock"]["state"] != "SYNCED"
            or not current_complete
            or not source_fresh
            or transition["open_episodes"]
        ):
            raise ValueError("RESILIENT_STATUS_READY_INVARIANT_INVALID")
        if payload["state"] == "DEGRADED_OBSERVABILITY" and (
            payload["clock"]["state"] != "SYNCED"
            or not current_complete
            or not source_fresh
            or transition["open_episodes"]
            or not any(item["name"] == "publisher_resources" and item["state"] != "FRESH" for item in components)
            or any(item["name"] != "publisher_resources" and item["state"] != "FRESH" for item in components)
        ):
            raise ValueError("RESILIENT_STATUS_OBSERVABILITY_INVARIANT_INVALID")
        last_good = payload["last_good"]
        last_good_fields = (
            "observed_at",
            "valid_until",
            "retained_until",
            "target_identity_sha256",
            "source_payload_sha256",
            "origin_host_id",
        )
        if last_good["present"] is not all(last_good[name] is not None for name in last_good_fields):
            raise ValueError("RESILIENT_STATUS_LAST_GOOD_COMPLETENESS_INVALID")
        upstream = payload["upstream_status"]
        if payload["role"] == "dell" and upstream is not None:
            raise ValueError("RESILIENT_STATUS_DELL_UPSTREAM_FORBIDDEN")
        if upstream is not None:
            if self.upstream_contract is None:
                raise ValueError("RESILIENT_STATUS_UPSTREAM_CONTRACT_REQUIRED")
            # Revalidate the nested signature and its temporal claims at the
            # signed relay observation time.  The outer envelope is evaluated
            # at the receiver's current time above, so its lease and current
            # source still bound how long an arena assertion can be used.  A
            # short Dell component lease expiring during otherwise healthy
            # transport must not turn a freshly signed arena envelope into a
            # false negative at CRA.
            self.upstream_contract.decode(dict(upstream), now=observed)
        elif payload["role"] == "arena" and payload["state"] == "READY":
            raise ValueError("RESILIENT_STATUS_ARENA_UPSTREAM_REQUIRED")
        return payload


def served_resilient_status_bytes(
    path: Path,
    *,
    expected_role: str,
    expected_host_id: str,
    expected_release_id: str,
    maximum_bytes: int,
    now: datetime | None = None,
) -> bytes:
    raw = read_regular_bytes(path, maximum_bytes=maximum_bytes)
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise ValueError("RESILIENT_STATUS_FILE_SCHEMA_INVALID")
    if value.get("role") != expected_role or value.get("host_id") != expected_host_id:
        raise ValueError("RESILIENT_STATUS_FILE_HOST_MISMATCH")
    if value.get("release_id") != expected_release_id:
        raise ValueError("RESILIENT_STATUS_FILE_RELEASE_MISMATCH")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed = parse_utc(str(value.get("observed_at", "")))
    lease = parse_utc(str(value.get("report_lease_until", "")))
    if observed > current:
        raise ValueError("RESILIENT_STATUS_FILE_OBSERVED_IN_FUTURE")
    if observed >= lease or lease <= current:
        raise ValueError("RESILIENT_STATUS_FILE_REPORT_EXPIRED")
    return raw
