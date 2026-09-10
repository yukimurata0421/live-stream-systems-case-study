"""Signed, read-only recovery facts and raw-source adapters.

These adapters distinguish fresh source evidence from a fresh wrapper. They
never call an effect socket, invoke a command, or use Pi evidence for control.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeGuard

from cra_dell_recovery.canonical import KeyRing, Signer, canonical_json
from cra_dell_recovery.json_input import load_object
from cra_dell_recovery.owner_diagnostics import validate_diagnostics
from cra_dell_recovery.recovery_history import HISTORY_FIELDS, validate_effect_history
from cra_dell_recovery.time import isoformat_utc, parse_utc

from .host_status import read_regular_bytes

FACTS_SCHEMA = "cra.recovery_soak_facts.v1"
ROLES = ("dell", "arena", "cra", "raspi")
FACT_FIELDS = {
    "dell": {"network", "transport", "effects", "activation"},
    "arena": {"network", "platform", "control_path"},
    "cra": {"safety"},
    "raspi": {"publication"},
}
ENVELOPE_FIELDS = {
    "schema",
    "role",
    "host_id",
    "host_boot_id",
    "release_id",
    "source_commit",
    "stream_id",
    "producer_id",
    "sequence",
    "observed_at",
    "valid_until",
    "source_hashes",
    "facts",
    "key_id",
    "payload_sha256",
    "signature",
    "source_failure_count",
}
ACTION_TABLES = ("commands", "recovery_authorizations", "delivery_attempts", "effect_scope_ledger", "effect_reconciliations")
NETWORK_EPISODE_SCHEMA = "stream_v3_network_episode/v1"
NETWORK_EPISODE_ANCHORS = ["cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6"]


def strict_json_object(raw: bytes) -> dict[str, Any]:
    # Bound parser depth before json.loads can raise RecursionError and bypass
    # per-source ValueError isolation. Reject float overflow and invalid Unicode
    # before canonicalization can abort the entire collector/relay invocation.
    return load_object(raw, maximum_bytes=2 * 1024 * 1024)


def strict_object(path: Path, *, maximum_bytes: int = 2 * 1024 * 1024) -> dict[str, Any]:
    return strict_json_object(read_regular_bytes(path, maximum_bytes=maximum_bytes))


def source_hash(value: Mapping[str, Any]) -> str:
    # Preserve signature/digest fields of nested raw evidence as content.
    return hashlib.sha256(canonical_json({"raw": dict(value)})).hexdigest()


def timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("SOURCE_TIMESTAMP_MISSING")
    return parse_utc(value)


def nonnegative_integer(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def source_fresh(value: object, now: datetime, maximum_age: int) -> bool:
    try:
        return 0 <= (now - timestamp(value)).total_seconds() <= maximum_age
    except (TypeError, ValueError, OverflowError):
        return False


@dataclass(frozen=True)
class HostBinding:
    role: str
    host_id: str
    release_id: str
    source_commit: str
    stream_id: str
    key_id: str

    def __post_init__(self) -> None:
        if self.role not in ROLES or not all((self.host_id, self.release_id, self.stream_id, self.key_id)):
            raise ValueError("RECOVERY_HOST_BINDING_INVALID")
        if re.fullmatch(r"[0-9a-f]{40}", self.source_commit) is None:
            raise ValueError("RECOVERY_SOURCE_COMMIT_INVALID")


def sign_facts(
    *,
    binding: HostBinding,
    signer: Signer,
    host_boot_id: str,
    producer_id: str,
    sequence: int,
    now: datetime,
    valid_until: datetime,
    facts: dict[str, Any],
    source_hashes: dict[str, str],
    source_failure_count: int = 0,
) -> dict[str, Any]:
    if set(facts) != FACT_FIELDS[binding.role]:
        raise ValueError("RECOVERY_FACT_FIELDS_INVALID")
    if not host_boot_id or not producer_id or not nonnegative_integer(sequence) or sequence < 1:
        raise ValueError("RECOVERY_PRODUCER_IDENTITY_INVALID")
    return signer.sign(
        {
            "schema": FACTS_SCHEMA,
            **binding.__dict__,
            "host_boot_id": host_boot_id,
            "producer_id": producer_id,
            "sequence": sequence,
            "observed_at": isoformat_utc(now),
            "valid_until": isoformat_utc(valid_until),
            "facts": facts,
            "source_hashes": source_hashes,
            "source_failure_count": source_failure_count,
        }
    )


def admit_facts(packet: dict[str, Any], *, binding: HostBinding, keys: KeyRing, now: datetime) -> dict[str, Any]:
    if set(packet) != ENVELOPE_FIELDS or packet.get("schema") != FACTS_SCHEMA:
        raise ValueError("RECOVERY_ENVELOPE_FIELDS_INVALID")
    for key, expected in binding.__dict__.items():
        if packet.get(key) != expected:
            raise ValueError("RECOVERY_HOST_RELEASE_STREAM_BINDING_INVALID")
    keys.verify(packet)
    observed = timestamp(packet["observed_at"])
    expires = timestamp(packet["valid_until"])
    if observed > now or not 0 < (expires - observed).total_seconds() <= 45:
        raise ValueError("RECOVERY_ENVELOPE_FUTURE_OR_LEASE_INVALID")
    if now >= expires:
        raise ValueError("RECOVERY_ENVELOPE_STALE")
    if not nonnegative_integer(packet["sequence"]) or packet["sequence"] < 1:
        raise ValueError("RECOVERY_SEQUENCE_INVALID")
    if not nonnegative_integer(packet["source_failure_count"]):
        raise ValueError("RECOVERY_SOURCE_FAILURE_COUNTER_INVALID")
    for key in ("host_boot_id", "producer_id"):
        if not isinstance(packet[key], str) or not 1 <= len(packet[key]) <= 128:
            raise ValueError("RECOVERY_PRODUCER_IDENTITY_INVALID")
    if not isinstance(packet["facts"], dict) or set(packet["facts"]) != FACT_FIELDS[binding.role]:
        raise ValueError("RECOVERY_FACT_FIELDS_INVALID")
    # A signature establishes provenance, not the nested producer's types.
    # Reject malformed health fields before set membership in either the live
    # checkpoint or formal replay can abort collection for every role.
    for name in {"dell": ("network",), "arena": ("network", "platform", "control_path")}.get(binding.role, ()):
        fact = packet["facts"][name]
        if not isinstance(fact, dict) or (
            "state" in fact and (not isinstance(fact["state"], str) or fact["state"] not in {"UP", "DOWN", "UNKNOWN"})
        ):
            raise ValueError("RECOVERY_HEALTH_FACT_INVALID")
    if binding.role == "dell":
        network = packet["facts"].get("network")
        if not isinstance(network, dict) or set(network) not in (
            {"state", "observed_at"},
            {"state", "observed_at", "episode"},
        ):
            raise ValueError("RECOVERY_NETWORK_FACT_INVALID")
        if "episode" in network and network["episode"] is not None:
            validate_network_episode(
                network["episode"],
                host_boot_id=packet["host_boot_id"],
                network_observed_at=network.get("observed_at"),
            )
        transport = packet["facts"].get("transport")
        if not isinstance(transport, dict):
            raise ValueError("RECOVERY_TRANSPORT_FACT_INVALID")
        if isinstance(transport, dict) and "lifecycle" in transport:
            lifecycle = validate_lifecycle(transport["lifecycle"], now=timestamp(packet["observed_at"]), boot_id=packet["host_boot_id"])
            if lifecycle["state"] == "RUNNING" and transport.get("target") not in (None, source_hash(lifecycle["anchor_target"])):
                raise ValueError("OWNER_LIFECYCLE_TRANSPORT_TARGET_INVALID")
    hashes = packet["source_hashes"]
    if not isinstance(hashes, dict) or not hashes or len(hashes) > 16:
        raise ValueError("RECOVERY_SOURCE_HASHES_INVALID")
    if any(
        not isinstance(k, str)
        or re.fullmatch(r"[a-z_]{1,48}", k) is None
        or not isinstance(v, str)
        or re.fullmatch(r"[0-9a-f]{64}", v) is None
        for k, v in hashes.items()
    ):
        raise ValueError("RECOVERY_SOURCE_HASHES_INVALID")
    return packet


def network_fact(raw: Mapping[str, Any], *, host_boot_id: str | None = None) -> dict[str, Any]:
    """Existing-connection failure plus successful reconnect is not a WAN down.

    An all-provider failure is DOWN; a single-provider failure is not. The
    source timestamp is cycle-start, so callers do not claim exact downtime.
    """
    probes = raw.get("probes")
    expected = {"cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6"}
    state = "UNKNOWN"
    if isinstance(probes, list) and len(probes) == 4 and all(isinstance(p, dict) and isinstance(p.get("name"), str) for p in probes):
        names = {p.get("name") for p in probes}
        if names == expected and all(type(p.get("ok")) is bool for p in probes):
            good = {p["name"].split("_")[0] for p in probes if p["ok"] or p.get("reconnect_after_failure_ok") is True}
            state = "UP" if good == {"cloudflare", "google"} else "DOWN" if not good else "UNKNOWN"
    result: dict[str, Any] = {"state": state, "observed_at": raw.get("ts_utc")}
    if "network_episode" in raw:
        episode = raw.get("network_episode")
        if episode is not None:
            episode_boot_id = episode.get("host_boot_id") if isinstance(episode, dict) else ""
            effective_boot_id = host_boot_id if host_boot_id is not None else episode_boot_id
            validate_network_episode(
                episode,
                host_boot_id=effective_boot_id if isinstance(effective_boot_id, str) else "",
                network_observed_at=raw.get("ts_utc"),
            )
        result["episode"] = episode
    return result


def validate_network_episode(
    value: object,
    *,
    host_boot_id: str,
    network_observed_at: object,
) -> dict[str, Any]:
    fields = {
        "schema",
        "episode_id",
        "sequence",
        "host_boot_id",
        "state",
        "classification",
        "started_at",
        "last_down_at",
        "recovered_at",
        "anchor_names",
        "provider_count",
        "address_family_count",
    }
    if not isinstance(value, dict) or set(value) != fields or value.get("schema") != NETWORK_EPISODE_SCHEMA:
        raise ValueError("RECOVERY_NETWORK_EPISODE_FIELDS_INVALID")
    episode_id = value.get("episode_id")
    sequence = value.get("sequence")
    if (
        value.get("host_boot_id") != host_boot_id
        or not isinstance(episode_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", episode_id) is None
        or not nonnegative_integer(sequence)
        or sequence < 1
        or value.get("classification") != "FULL_WAN"
        or value.get("anchor_names") != NETWORK_EPISODE_ANCHORS
        or value.get("provider_count") != 2
        or value.get("address_family_count") != 2
    ):
        raise ValueError("RECOVERY_NETWORK_EPISODE_IDENTITY_INVALID")
    state = value.get("state")
    recovered_raw = value.get("recovered_at")
    if state not in {"ACTIVE", "RECOVERED"} or (state == "ACTIVE") != (recovered_raw is None):
        raise ValueError("RECOVERY_NETWORK_EPISODE_STATE_INVALID")
    try:
        observed = timestamp(network_observed_at)
        started = timestamp(value.get("started_at"))
        last_down = timestamp(value.get("last_down_at"))
        recovered = timestamp(recovered_raw) if recovered_raw is not None else None
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("RECOVERY_NETWORK_EPISODE_TIME_INVALID") from error
    if (
        started > last_down
        or last_down > observed
        or (recovered is not None and (last_down > recovered or recovered > observed))
        or episode_id != hashlib.sha256(f"{host_boot_id}:{sequence}:{value['started_at']}".encode()).hexdigest()[:32]
    ):
        raise ValueError("RECOVERY_NETWORK_EPISODE_TIME_OR_DIGEST_INVALID")
    return value


def transport_fact(target: Mapping[str, Any], transport: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    identity = target.get("target_identity")
    metrics = transport.get("metrics")
    result: dict[str, Any] = {"target": None, "bytes_acked": None, "observed_at": transport.get("ts_utc")}
    if not isinstance(identity, dict) or not isinstance(metrics, dict):
        return result
    if (
        target.get("status") != "VALID"
        or not source_fresh(target.get("observed_at"), now, 45)
        or not source_fresh(transport.get("ts_utc"), now, 45)
        or identity.get("ffmpeg_pid") != transport.get("ffmpeg_pid")
        or not nonnegative_integer(metrics.get("bytes_acked"))
    ):
        return result
    try:
        if timestamp(target.get("valid_until")) <= now:
            return result
    except ValueError:
        return result
    required = {"host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id", "ffmpeg_pid", "ffmpeg_generation"}
    if not required <= set(identity) or any(identity.get(k) in (None, "") for k in required):
        return result
    result.update(target=source_hash(identity), bytes_acked=metrics["bytes_acked"])
    return result


def owner_target_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
    """A v3 owner proves its exact child locally on both sides of the DB read."""
    return {
        "status": "VALID" if raw.get("target_identity") is not None else "UNKNOWN",
        "target_identity": raw.get("target_identity"),
        "observed_at": raw["lifecycle"]["observed_at"],
        "valid_until": raw.get("valid_until"),
    }


def validate_lifecycle(value: object, *, now: datetime, boot_id: str) -> dict[str, Any]:
    fields = {
        "schema",
        "state",
        "reason",
        "observed_at",
        "anchor_target",
        "owner_pid",
        "owner_start_ticks",
        "child_pid",
        "child_start_ticks",
    }
    if isinstance(value, dict) and value.get("schema") == "runtime.child_lifecycle.v2":
        fields.add("read_diagnostics")
        validate_diagnostics(value.get("read_diagnostics"))
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("schema") not in ("runtime.child_lifecycle.v1", "runtime.child_lifecycle.v2")
    ):
        raise ValueError("OWNER_LIFECYCLE_FIELDS_INVALID")
    state = value.get("state")
    reasons = {"RUNNING": "EXACT_CHILD", "ABSENT": "NO_MANAGED_CHILD", "TRANSITION": "CHILD_CHANGED", "UNKNOWN": "READ_UNAVAILABLE"}
    if not isinstance(state, str) or state not in reasons or value.get("reason") != reasons[state]:
        raise ValueError("OWNER_LIFECYCLE_STATE_INVALID")
    if not source_fresh(value["observed_at"], now, 45):
        raise ValueError("OWNER_LIFECYCLE_STALE_OR_FUTURE")
    if state == "UNKNOWN":
        if value["schema"] == "runtime.child_lifecycle.v2" and not value["read_diagnostics"]["events"]:
            raise ValueError("OWNER_LIFECYCLE_UNKNOWN_DIAGNOSTIC_REQUIRED")
        if value["anchor_target"] is not None or value["child_pid"] is not None or value["child_start_ticks"] is not None:
            raise ValueError("OWNER_LIFECYCLE_UNKNOWN_IDENTITY_INVALID")
        return value
    anchor = value["anchor_target"]
    required = {"host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id", "ffmpeg_pid", "ffmpeg_generation"}
    if (
        not isinstance(anchor, dict)
        or set(anchor) != required
        or anchor.get("host_boot_id") != boot_id
        or any(not isinstance(anchor[k], str) or not anchor[k] for k in required - {"ffmpeg_pid"})
        or not nonnegative_integer(anchor["ffmpeg_pid"])
        or anchor["ffmpeg_pid"] <= 1
        or any(not nonnegative_integer(value[k]) or value[k] < 1 for k in ("owner_pid", "owner_start_ticks"))
    ):
        raise ValueError("OWNER_LIFECYCLE_RUNTIME_IDENTITY_INVALID")
    if state == "ABSENT":
        if value["child_pid"] is not None or value["child_start_ticks"] is not None:
            raise ValueError("OWNER_LIFECYCLE_ABSENCE_IDENTITY_INVALID")
    else:
        if any(not nonnegative_integer(value[k]) or value[k] < 1 for k in ("child_pid", "child_start_ticks")):
            raise ValueError("OWNER_LIFECYCLE_CHILD_IDENTITY_INVALID")
        if state == "RUNNING":
            generation = hashlib.sha256(
                f"{anchor['pod_uid']}:{anchor['container_id']}:{anchor['ffmpeg_pid']}:{value['child_start_ticks']}".encode()
            ).hexdigest()[:32]
            if anchor["ffmpeg_generation"] != "ffmpeg-" + generation:
                raise ValueError("OWNER_LIFECYCLE_CHILD_GENERATION_INVALID")
    return value


def runtime_evidence_fact(
    raw: dict[str, Any],
    *,
    target: dict[str, Any],
    binding: HostBinding,
    runtime_binding: dict[str, str],
    now: datetime,
    history_policy: dict[str, Any] | None = None,
    physical_boot_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Admit an owner-produced export, without DB/process access or retiming."""
    expected_fields = {
        "schema",
        "host_id",
        "host_boot_id",
        "release_id",
        "source_commit",
        "stream_id",
        "target_sha256",
        "observed_at",
        "valid_until",
        "effects",
        "activation",
    }
    identity = target.get("target_identity")
    schema = runtime_binding.get("evidence_schema", "runtime.recovery_evidence.v1")
    lifecycle_enabled = schema == "runtime.recovery_evidence.v3"
    independent = schema in ("runtime.recovery_evidence.v2", "runtime.recovery_evidence.v3")
    if lifecycle_enabled:
        expected_fields |= {"target_identity", "lifecycle"}
        lifecycle = validate_lifecycle(raw.get("lifecycle"), now=now, boot_id=physical_boot_id or "")
        target = owner_target_snapshot(raw)
        identity = target["target_identity"]
        if (
            # The final kernel read follows the ledger snapshot. Preserve both
            # clocks; allow the 0.5-second read budget plus the ledger clock's
            # millisecond serialization rounding, never a future observation.
            not 0 <= (timestamp(lifecycle["observed_at"]) - timestamp(raw.get("observed_at"))).total_seconds() <= 0.501
            or (
                lifecycle["state"] == "RUNNING"
                and (identity != lifecycle["anchor_target"] or raw.get("target_sha256") != source_hash(identity))
            )
            or (lifecycle["state"] != "RUNNING" and identity is not None)
            or (lifecycle["anchor_target"] is not None and lifecycle["anchor_target"]["host_id"] != runtime_binding.get("target_host_id"))
        ):
            raise ValueError("OWNER_LIFECYCLE_TARGET_BINDING_INVALID")
    try:
        target_bound = (
            isinstance(identity, dict)
            and target.get("status") == "VALID"
            and source_fresh(target.get("observed_at"), now, 45)
            and timestamp(target.get("valid_until")) > now
            and identity.get("host_id") == runtime_binding.get("target_host_id")
            and raw.get("host_boot_id") == identity.get("host_boot_id")
            and raw.get("target_sha256") == source_hash(identity)
        )
    except (TypeError, ValueError):
        target_bound = False
    if (
        set(raw) != expected_fields
        or schema not in ("runtime.recovery_evidence.v1", "runtime.recovery_evidence.v2", "runtime.recovery_evidence.v3")
        or raw.get("schema") != schema
        or (not independent and not target_bound)
        or (independent and (not physical_boot_id or raw.get("host_boot_id") != physical_boot_id))
        or raw.get("host_id") != binding.host_id
        or not raw.get("host_boot_id")
        or raw.get("stream_id") != binding.stream_id
        or any(raw.get(k) != runtime_binding.get(k) for k in ("release_id", "source_commit"))
    ):
        raise ValueError("RUNTIME_EVIDENCE_BINDING_INVALID")
    observed, expires = timestamp(raw["observed_at"]), timestamp(raw["valid_until"])
    if not source_fresh(raw["observed_at"], now, 45) or now >= expires or not 0 < (expires - observed).total_seconds() <= 45:
        raise ValueError("RUNTIME_EVIDENCE_STALE_OR_FUTURE")
    effects, activation = raw["effects"], raw["activation"]
    counters = {"physical_attempt_count", "duplicate_attempt_count", "unresolved_scope_count", "unauthorized_effect_count"}
    if (
        not isinstance(effects, dict)
        or set(effects)
        != counters | HISTORY_FIELDS | {"observed_at", "integrity", "last_reconciliation"} | ({"target_status"} if independent else set())
        or effects.get("observed_at") != raw["observed_at"]
        or any(not nonnegative_integer(effects.get(k)) for k in counters)
        or not isinstance(effects.get("integrity"), str)
        or not isinstance(effects.get("last_reconciliation"), dict)
        or not isinstance(activation, dict)
        or set(activation) != {"bounded_termination_enabled", "rw_timeout_enabled"}
        or any(value is not None and type(value) is not bool for value in activation.values())
    ):
        raise ValueError("RUNTIME_EVIDENCE_FACTS_INVALID")
    if independent and (
        effects.get("target_status") not in ("VALID", "UNKNOWN")
        or raw["target_sha256"] != effects.get("current_target_sha256")
        or (effects["target_status"] == "UNKNOWN" and (raw["target_sha256"] is not None or any(v is not None for v in activation.values())))
    ):
        raise ValueError("RUNTIME_EVIDENCE_TARGET_STATUS_INVALID")
    validate_effect_history(
        effects,
        policy=history_policy,
        target=raw["target_sha256"],
        now=now,
        allow_unknown_target=independent and effects.get("target_status") == "UNKNOWN",
    )
    proof = effects["last_reconciliation"]
    if proof and (
        set(proof) != {"resolution", "action_id", "before_target", "after_target"}
        or proof.get("resolution") not in {"EFFECT_OBSERVED", "NO_EFFECT_PROVEN", "REMAINS_UNKNOWN"}
        or any(not isinstance(v, str) or not v for v in proof.values())
        or any(re.fullmatch(r"[0-9a-f]{64}", proof[k]) is None for k in ("before_target", "after_target"))
    ):
        raise ValueError("RUNTIME_EVIDENCE_RECONCILIATION_INVALID")
    if independent and not target_bound:
        return (
            {**effects, "target_status": "UNKNOWN", "current_target_sha256": None, "current_target_unresolved_scope_count": None},
            {"bounded_termination_enabled": None, "rw_timeout_enabled": None},
        )
    return dict(effects), dict(activation)


def platform_fact(raw: Mapping[str, Any], *, stream_id: str, expected_video_id: str) -> dict[str, Any]:
    # `healthy`, watch-page/live markers and ESTAB are availability-only. Never
    # let their OR expression override OAuth ingest inactive/noData.
    state = "UNKNOWN"
    video = raw.get("oauth_broadcast_id") or raw.get("video_id")
    if expected_video_id and video == expected_video_id and raw.get("oauth_probe_ok") is True:
        active = raw.get("oauth_stream_status")
        health = raw.get("oauth_stream_health_status")
        if active == "active" and health == "good" and raw.get("oauth_healthy") is True:
            state = "UP"
        elif active == "inactive" or health in {"noData", "bad"}:
            state = "DOWN"
    return {"state": state, "observed_at": raw.get("oauth_checked_ts_utc"), "stream_id": stream_id}


def publication_fact(
    *,
    local_generated_at: object,
    remote_generated_at: object,
    upload_completed_at: object,
    upload_succeeded: object,
    network_state: str,
    now: datetime,
) -> dict[str, Any]:
    """Advisory only: distinguishes source stale, uploader failure and mirror lag."""

    def source_time(value: object) -> object:
        if isinstance(value, (float, int)) and not isinstance(value, bool):
            try:
                if not math.isfinite(value):
                    return None
                return isoformat_utc(datetime.fromtimestamp(value, UTC))
            except (ValueError, OverflowError, OSError):
                return None
        return value

    local_generated_at = source_time(local_generated_at)
    remote_generated_at = source_time(remote_generated_at)
    reasons: list[str] = []
    if not source_fresh(local_generated_at, now, 180):
        reasons.append("PUBLIC_SOURCE_STALE_OR_UNKNOWN")
    if upload_succeeded is not True or not source_fresh(upload_completed_at, now, 180):
        reasons.append("PUBLISHER_FAILED_OR_UNKNOWN")
    if not source_fresh(remote_generated_at, now, 180):
        reasons.append("PUBLIC_MIRROR_STALE_OR_UNKNOWN")
    if network_state != "UP":
        reasons.append("PI_NETWORK_UNAVAILABLE_OR_UNKNOWN")
    return {
        "status": "READY" if not reasons else "DEGRADED",
        "reason_codes": reasons,
        "observed_at": isoformat_utc(now),
        "control_input_allowed": False,
    }


def wifi_diagnosis(
    *,
    link_up: bool | None,
    gateway_up: bool | None,
    dns_ok: bool | None,
    independent_https_successes: int | None,
    publisher_ok: bool | None,
) -> dict[str, Any]:
    """A diagnostic decision, never a Wi-Fi disconnect/restart instruction."""
    if link_up is False or gateway_up is False:
        reason = "LOCAL_LINK_OR_GATEWAY_FAILURE"
    elif dns_ok is False:
        reason = "DNS_FAILURE_NOT_PROOF_OF_WIFI_FAILURE"
    elif independent_https_successes == 0:
        reason = "UPSTREAM_UNREACHABLE_NOT_PROOF_OF_WIFI_FAILURE"
    elif independent_https_successes is not None and independent_https_successes >= 2 and publisher_ok is False:
        reason = "PUBLISHER_OR_CREDENTIAL_FAILURE"
    elif link_up is True and gateway_up is True and dns_ok is True and publisher_ok is True:
        reason = "READY"
    else:
        reason = "INSUFFICIENT_INDEPENDENT_EVIDENCE"
    return {"reason_code": reason, "automatic_reassociate": False, "control_capability_count": 0}
