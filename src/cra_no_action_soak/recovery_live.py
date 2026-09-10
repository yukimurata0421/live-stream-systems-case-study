"""Bounded per-sample checkpoint; formal gate replays it independently."""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.errors import RecoveryControlError
from cra_dell_recovery.recovery_history import validate_effect_history

from .recovery_facts import HostBinding, admit_facts, nonnegative_integer, source_fresh, source_hash, timestamp
from .recovery_window import Health, RecoveryObservation, RecoveryWindow


def observation(
    facts: dict[str, dict[str, Any]], *, stream_id: str, now: datetime, history_policy: dict[str, Any] | None = None
) -> RecoveryObservation:
    def part(role: str, name: str) -> dict[str, Any]:
        value = facts.get(role, {}).get(name)
        return value if isinstance(value, dict) else {}

    def at(value: object) -> datetime | None:
        try:
            return timestamp(value)
        except (TypeError, ValueError):
            return None

    def health(value: dict[str, Any], age: int) -> Health:
        if source_fresh(value.get("observed_at"), now, age) and value.get("state") in {"UP", "DOWN", "UNKNOWN"}:
            return cast(Health, value["state"])
        return "UNKNOWN"

    network, transport = part("dell", "network"), part("dell", "transport")
    raw_lifecycle = transport.get("lifecycle")
    lifecycle: dict[str, Any] = raw_lifecycle if isinstance(raw_lifecycle, dict) else {}
    lifecycle_present = isinstance(raw_lifecycle, dict)
    lifecycle_fresh = lifecycle_present and source_fresh(lifecycle.get("observed_at"), now, 45)
    owner_target: str | None = None
    if lifecycle_fresh and lifecycle.get("state") == "RUNNING" and isinstance(lifecycle.get("anchor_target"), dict):
        try:
            owner_target = source_hash(lifecycle["anchor_target"])
        except (TypeError, ValueError):
            # Direct callers still fail closed even though admitted signed
            # packets have already passed validate_lifecycle().
            owner_target = None
    raw_network_episode = network.get("episode")
    network_episode: dict[str, Any] = raw_network_episode if isinstance(raw_network_episode, dict) else {}
    platform, control = part("arena", "platform"), part("arena", "control_path")
    safety = part("cra", "safety")
    control_health = health(control, 45)
    if not source_fresh(safety.get("runtime_observed_at"), now, 45):
        control_health = "UNKNOWN"
    elif safety.get("runtime_readiness") != "NO_ACTION_READY" and control_health == "UP":
        control_health = "DOWN" if safety.get("runtime_readiness") == "SAFE_BLOCKED" else "UNKNOWN"
    effects = part("dell", "effects")
    unresolved = effects.get("unresolved_scope_count") if source_fresh(effects.get("observed_at"), now, 45) else None
    if unresolved is not None:
        try:
            validate_effect_history(effects, policy=history_policy, target=transport.get("target"), now=now)
        except (ValueError, TypeError):
            unresolved = None
    if effects.get("target_status") is not None:
        activation = part("dell", "activation")
        if set(activation) != {"bounded_termination_enabled", "rw_timeout_enabled"} or any(v is not True for v in activation.values()):
            # Global ledger health cannot finish recovery before exact-child
            # activation is observed under the independent evidence contract.
            unresolved = None
    return RecoveryObservation(
        observed_at=now,
        network=health(network, 45),
        network_observed_at=at(network.get("observed_at")),
        target=transport.get("target") if isinstance(transport.get("target"), str) else None,
        transport_observed_at=at(transport.get("observed_at")),
        bytes_acked=transport.get("bytes_acked"),
        stream_id=stream_id,
        platform=health(platform, 120),
        platform_observed_at=at(platform.get("observed_at")),
        platform_stream_id=platform.get("stream_id"),
        control_path=control_health,
        unresolved_effect_count=unresolved if nonnegative_integer(unresolved) else None,
        network_episode_id=(network_episode.get("episode_id") if isinstance(network_episode.get("episode_id"), str) else None),
        network_episode_sequence=(network_episode.get("sequence") if nonnegative_integer(network_episode.get("sequence")) else None),
        network_episode_state=(network_episode.get("state") if network_episode.get("state") in {"ACTIVE", "RECOVERED"} else None),
        network_episode_started_at=at(network_episode.get("started_at")),
        network_episode_recovered_at=at(network_episode.get("recovered_at")),
        child_lifecycle=(lifecycle.get("state") if lifecycle_fresh else "UNKNOWN" if lifecycle_present else None),
        owner_target=owner_target,
    )


def advance(
    previous: dict[str, Any] | None,
    inputs: dict[str, Any],
    *,
    bindings: dict[str, HostBinding],
    keys: KeyRing,
    now: datetime,
    history_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    expected_fields = {"window", "packet_heads", "integrity_blockers", "source_failure_deltas"}
    if previous is not None and set(previous) != expected_fields:
        raise ValueError("RECOVERY_LIVE_CHECKPOINT_FIELDS_INVALID")
    window = RecoveryWindow.restore(previous["window"]) if previous else RecoveryWindow()
    heads = dict(previous["packet_heads"]) if previous else {}
    blockers = set(previous["integrity_blockers"]) if previous else set()
    source_failure_deltas = dict(previous["source_failure_deltas"]) if previous else dict.fromkeys(("dell", "arena", "cra"), 0)
    if set(source_failure_deltas) != {"dell", "arena", "cra"} or any(
        not nonnegative_integer(value) for value in source_failure_deltas.values()
    ):
        raise ValueError("RECOVERY_LIVE_SOURCE_FAILURE_DELTAS_INVALID")
    admitted = {}
    for role in ("dell", "arena", "cra"):
        packet = inputs.get(role)
        if packet is None:
            continue
        try:
            checked = admit_facts(packet, binding=bindings[role], keys=keys, now=now)
        except (ValueError, TypeError, KeyError, RecoveryControlError) as error:
            if str(error) != "RECOVERY_ENVELOPE_STALE":
                blockers.add(role.upper() + "_SIGNED_EVIDENCE_INVALID")
            continue
        head = {k: checked[k] for k in ("host_boot_id", "producer_id", "sequence", "payload_sha256", "source_failure_count")}
        prior = heads.get(role)
        if prior is not None and head["source_failure_count"] < prior["source_failure_count"]:
            blockers.add(role.upper() + "_SOURCE_FAILURE_COUNTER_REGRESSION")
        elif prior is not None and head["source_failure_count"] > prior["source_failure_count"]:
            source_failure_deltas[role] += head["source_failure_count"] - prior["source_failure_count"]
        if prior is not None and (
            (head["host_boot_id"], head["producer_id"]) != (prior["host_boot_id"], prior["producer_id"])
            or head["sequence"] < prior["sequence"]
            or (head["sequence"] == prior["sequence"] and head["payload_sha256"] != prior["payload_sha256"])
        ):
            blockers.add(role.upper() + "_PRODUCER_CONTINUITY_INVALID")
        heads[role] = head
        admitted[role] = checked["facts"]
    dell = admitted.get("dell", {})
    effects = dell.get("effects", {})
    if isinstance(effects, dict) and source_fresh(effects.get("observed_at"), now, 45):
        try:
            transport = dell.get("transport", {})
            target = transport.get("target") if isinstance(transport, dict) else None
            validate_effect_history(
                effects, policy=history_policy, target=target, now=now, allow_unknown_target=effects.get("target_status") == "UNKNOWN"
            )
        except (ValueError, TypeError):
            blockers.add("DELL_EFFECT_HISTORY_INVALID")
    window.observe(observation(admitted, stream_id=bindings["dell"].stream_id, now=now, history_policy=history_policy))
    window.finish(now)
    return {
        "window": window.checkpoint(),
        "packet_heads": heads,
        "integrity_blockers": sorted(blockers),
        "source_failure_deltas": source_failure_deltas,
    }
