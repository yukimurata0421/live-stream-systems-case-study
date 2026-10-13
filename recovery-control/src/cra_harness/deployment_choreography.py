from __future__ import annotations

import itertools
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class DeploymentState(StrEnum):
    OLD_CHAIN_ACTIVE = "OLD_CHAIN_ACTIVE"
    DRAINING_CONSUMER = "DRAINING_CONSUMER"
    CONSUMER_QUIESCED = "CONSUMER_QUIESCED"
    UPSTREAM_SWITCHING = "UPSTREAM_SWITCHING"
    NEW_PROJECTION_FRESH = "NEW_PROJECTION_FRESH"
    NEW_CONSUMER_STARTING = "NEW_CONSUMER_STARTING"
    NEW_CHAIN_READY = "NEW_CHAIN_READY"
    ROLLING_BACK = "ROLLING_BACK"


@dataclass(frozen=True)
class DeploymentStep:
    operation: str
    executable_authorization_count: int = 0
    command_count: int = 0
    effect_count: int = 0


@dataclass
class _Composition:
    state: DeploymentState = DeploymentState.OLD_CHAIN_ACTIVE
    old_timer_active: bool = True
    old_timer_enabled: bool = True
    old_consumer_active: bool = True
    old_consumer_enabled: bool = True
    new_timer_active: bool = False
    new_timer_enabled: bool = False
    new_consumer_active: bool = False
    new_consumer_enabled: bool = False
    dell_new: bool = False
    arena_new: bool = False
    projection_fresh: bool = False
    rollback_started: bool = False
    rollback_completed: bool = False
    reboot_persistence_confirmed: bool = False

    @property
    def new_upstream_active(self) -> bool:
        return self.dell_new or self.arena_new

    @property
    def effective_consumer_count(self) -> int:
        return int(self.old_consumer_active) + int(self.new_consumer_active)

    @property
    def coherent_chain_count(self) -> int:
        old = self.old_consumer_active and not self.new_upstream_active
        new = self.new_consumer_active and self.dell_new and self.arena_new and self.projection_fresh
        return int(old) + int(new)


def _snapshot(composition: _Composition, step: DeploymentStep) -> dict[str, Any]:
    return {
        "operation": step.operation,
        "state": composition.state.value,
        "old_timer_active": composition.old_timer_active,
        "old_timer_enabled": composition.old_timer_enabled,
        "old_consumer_active": composition.old_consumer_active,
        "old_consumer_enabled": composition.old_consumer_enabled,
        "new_timer_active": composition.new_timer_active,
        "new_timer_enabled": composition.new_timer_enabled,
        "new_consumer_active": composition.new_consumer_active,
        "new_consumer_enabled": composition.new_consumer_enabled,
        "dell_new": composition.dell_new,
        "arena_new": composition.arena_new,
        "projection_fresh": composition.projection_fresh,
        "effective_consumer_count": composition.effective_consumer_count,
        "coherent_chain_count": composition.coherent_chain_count,
        "reboot_persistence_confirmed": composition.reboot_persistence_confirmed,
        "executable_authorization_count": step.executable_authorization_count,
        "command_count": step.command_count,
        "effect_count": step.effect_count,
    }


def evaluate_deployment_sequence(steps: tuple[DeploymentStep, ...]) -> dict[str, Any]:
    """Evaluate cutover choreography without contacting or mutating a host."""

    composition = _Composition()
    violations: list[dict[str, Any]] = []
    trace: list[dict[str, Any]] = []

    def add(code: str, index: int, detail: str) -> None:
        violations.append({"code": code, "step_index": index, "detail": detail})

    for index, step in enumerate(steps):
        operation = step.operation
        if operation == "STOP_OLD_TIMER":
            composition.old_timer_active = False
            composition.state = DeploymentState.DRAINING_CONSUMER
        elif operation == "LOSE_TIMER_STOP":
            composition.state = DeploymentState.DRAINING_CONSUMER
            add("OLD_TIMER_STOP_NOT_DURABLE", index, "the old timer remained active")
        elif operation == "DRAIN_OLD_CONSUMER":
            if composition.old_timer_active:
                add("OLD_CONSUMER_DRAIN_WITH_TIMER_ACTIVE", index, "a new oneshot can start during drain")
            else:
                composition.old_consumer_active = False
                composition.state = DeploymentState.CONSUMER_QUIESCED
        elif operation == "DISABLE_OLD_UNITS":
            composition.old_timer_enabled = False
            composition.old_consumer_enabled = False
        elif operation == "LEAVE_OLD_UNITS_ENABLED":
            add("OLD_UNITS_REBOOT_ENABLED", index, "old units would return after a host reboot")
        elif operation == "DELAY_IN_FLIGHT":
            composition.state = DeploymentState.DRAINING_CONSUMER
            add("OLD_CONSUMER_IN_FLIGHT", index, "old consumer did not reach inactive")
        elif operation == "SWITCH_DELL":
            composition.dell_new = True
            composition.state = DeploymentState.UPSTREAM_SWITCHING
        elif operation == "SWITCH_ARENA":
            composition.arena_new = True
            composition.projection_fresh = False
            composition.state = DeploymentState.UPSTREAM_SWITCHING
        elif operation == "CRASH_UPSTREAM_SWITCH":
            composition.dell_new = True
            composition.state = DeploymentState.UPSTREAM_SWITCHING
            add("UPSTREAM_SWITCH_PARTIAL", index, "Dell changed but arena did not")
        elif operation == "CONFIRM_FRESH_PROJECTION":
            if composition.dell_new and composition.arena_new:
                composition.projection_fresh = True
                composition.state = DeploymentState.NEW_PROJECTION_FRESH
            else:
                add("PROJECTION_FRESHNESS_BEFORE_UPSTREAM_READY", index, "freshness was asserted on a partial chain")
        elif operation == "STALE_PROJECTION":
            composition.projection_fresh = False
            add("NEW_PROJECTION_NOT_FRESH", index, "projection is stale or incompatible")
        elif operation == "START_NEW_CONSUMER":
            composition.state = DeploymentState.NEW_CONSUMER_STARTING
            if not (composition.dell_new and composition.arena_new and composition.projection_fresh):
                add("NEW_CONSUMER_STARTED_BEFORE_FRESH_PROJECTION", index, "new consumer started before compatible evidence")
            composition.new_consumer_active = True
        elif operation == "ENABLE_NEW_UNITS":
            composition.new_timer_enabled = True
            composition.new_timer_active = True
            composition.new_consumer_enabled = True
        elif operation == "LEAVE_NEW_UNITS_DISABLED":
            add("NEW_UNITS_REBOOT_DISABLED", index, "new units would not return after a host reboot")
        elif operation == "DUPLICATE_NEW_CONSUMER_START":
            composition.new_consumer_active = True
            add("DUPLICATE_NEW_CONSUMER_START", index, "a second effective start was requested")
        elif operation == "OLD_SERVICE_LATE_WAKEUP":
            composition.old_consumer_active = True
            add("OLD_CONSUMER_LATE_WAKEUP", index, "old consumer became effective after quiesce")
        elif operation == "CONFIRM_NEW_CHAIN":
            if composition.coherent_chain_count == 1 and composition.new_consumer_active:
                if composition.reboot_persistence_confirmed:
                    composition.state = DeploymentState.NEW_CHAIN_READY
                else:
                    add("REBOOT_PERSISTENCE_UNCONFIRMED", index, "active processes do not prove the next-boot chain")
            else:
                add("NEW_CHAIN_NOT_COHERENT", index, "new chain cannot be promoted")
        elif operation == "CONFIRM_REBOOT_PERSISTENCE":
            if composition.old_timer_enabled or composition.old_consumer_enabled:
                add("OLD_UNITS_REBOOT_ENABLED", index, "old unit enablement remains")
            if not composition.new_timer_enabled or not composition.new_consumer_enabled:
                add("NEW_UNITS_REBOOT_DISABLED", index, "new unit enablement is incomplete")
            if not composition.new_timer_active or not composition.new_consumer_active:
                add("NEW_UNITS_NOT_ACTIVE", index, "new enabled units are not currently active")
            if not violations or all(item["step_index"] != index for item in violations):
                composition.reboot_persistence_confirmed = True
        elif operation == "START_ROLLBACK":
            composition.rollback_started = True
            composition.state = DeploymentState.ROLLING_BACK
            composition.new_consumer_active = False
        elif operation == "CRASH_ROLLBACK":
            composition.rollback_started = True
            composition.state = DeploymentState.ROLLING_BACK
            add("ROLLBACK_INCOMPLETE", index, "rollback stopped before a coherent chain was restored")
        elif operation == "COMPLETE_ROLLBACK":
            composition.old_timer_active = True
            composition.old_timer_enabled = True
            composition.old_consumer_active = True
            composition.old_consumer_enabled = True
            composition.new_timer_active = False
            composition.new_timer_enabled = False
            composition.new_consumer_active = False
            composition.new_consumer_enabled = False
            composition.dell_new = False
            composition.arena_new = False
            composition.projection_fresh = False
            composition.rollback_started = False
            composition.rollback_completed = True
            composition.reboot_persistence_confirmed = False
            composition.state = DeploymentState.OLD_CHAIN_ACTIVE
        else:
            add("DEPLOYMENT_OPERATION_UNKNOWN", index, operation)

        if composition.old_consumer_active and composition.new_upstream_active:
            add("OLD_INCOMPATIBLE_CONSUMER_WITH_NEW_UPSTREAM", index, operation)
        if composition.effective_consumer_count > 1:
            add("SIMULTANEOUS_EFFECTIVE_CONSUMERS", index, str(composition.effective_consumer_count))
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (step.executable_authorization_count, step.command_count, step.effect_count)
        ):
            add("CUTOVER_COUNTER_INVALID", index, operation)
        elif step.executable_authorization_count or step.command_count or step.effect_count:
            add("CUTOVER_EXECUTABLE_ACTIVITY_NONZERO", index, operation)
        trace.append(_snapshot(composition, step))

    if composition.state not in {DeploymentState.NEW_CHAIN_READY, DeploymentState.OLD_CHAIN_ACTIVE}:
        add("CUTOVER_TERMINAL_STATE_INCOMPLETE", len(steps), composition.state.value)
    if composition.rollback_completed and composition.coherent_chain_count != 1:
        add("ROLLBACK_COHERENT_CHAIN_COUNT_INVALID", len(steps), str(composition.coherent_chain_count))
    codes = sorted({item["code"] for item in violations})
    return {
        "schema": "cra.deployment_choreography_harness.v1",
        "pass": not violations,
        "final_state": composition.state.value,
        "violation_codes": codes,
        "violations": violations,
        "trace": trace,
        "safety": {
            "production_mutation_count": 0,
            "network_failure_injection_count": 0,
            "physical_effect_count": 0,
        },
    }


FAILED_UPSTREAM_FIRST = (
    DeploymentStep("SWITCH_DELL"),
    DeploymentStep("SWITCH_ARENA"),
    DeploymentStep("STALE_PROJECTION"),
)

CONSUMER_FIRST = (
    DeploymentStep("STOP_OLD_TIMER"),
    DeploymentStep("DRAIN_OLD_CONSUMER"),
    DeploymentStep("DISABLE_OLD_UNITS"),
    DeploymentStep("SWITCH_DELL"),
    DeploymentStep("SWITCH_ARENA"),
    DeploymentStep("CONFIRM_FRESH_PROJECTION"),
    DeploymentStep("START_NEW_CONSUMER"),
    DeploymentStep("ENABLE_NEW_UNITS"),
    DeploymentStep("CONFIRM_REBOOT_PERSISTENCE"),
    DeploymentStep("CONFIRM_NEW_CHAIN"),
)


CHAOS_MUTATIONS = (
    "LOSE_TIMER_STOP",
    "DELAY_IN_FLIGHT",
    "CRASH_UPSTREAM_SWITCH",
    "STALE_PROJECTION",
    "CRASH_ROLLBACK",
    "DUPLICATE_NEW_CONSUMER_START",
    "OLD_SERVICE_LATE_WAKEUP",
    "LEAVE_OLD_UNITS_ENABLED",
    "LEAVE_NEW_UNITS_DISABLED",
)


def _chaos_sequence(faults: frozenset[str]) -> tuple[DeploymentStep, ...]:
    steps = [DeploymentStep("LOSE_TIMER_STOP" if "LOSE_TIMER_STOP" in faults else "STOP_OLD_TIMER")]
    steps.append(DeploymentStep("DELAY_IN_FLIGHT" if "DELAY_IN_FLIGHT" in faults else "DRAIN_OLD_CONSUMER"))
    steps.append(
        DeploymentStep("LEAVE_OLD_UNITS_ENABLED" if "LEAVE_OLD_UNITS_ENABLED" in faults else "DISABLE_OLD_UNITS")
    )
    if "CRASH_UPSTREAM_SWITCH" in faults:
        steps.append(DeploymentStep("CRASH_UPSTREAM_SWITCH"))
    else:
        steps.append(DeploymentStep("SWITCH_DELL"))
    steps.extend((DeploymentStep("SWITCH_ARENA"), DeploymentStep("CONFIRM_FRESH_PROJECTION")))
    if "STALE_PROJECTION" in faults:
        steps.append(DeploymentStep("STALE_PROJECTION"))
    steps.append(DeploymentStep("START_NEW_CONSUMER"))
    steps.append(
        DeploymentStep("LEAVE_NEW_UNITS_DISABLED" if "LEAVE_NEW_UNITS_DISABLED" in faults else "ENABLE_NEW_UNITS")
    )
    if "DUPLICATE_NEW_CONSUMER_START" in faults:
        steps.append(DeploymentStep("DUPLICATE_NEW_CONSUMER_START"))
    if "OLD_SERVICE_LATE_WAKEUP" in faults:
        steps.append(DeploymentStep("OLD_SERVICE_LATE_WAKEUP"))
    steps.append(DeploymentStep("CONFIRM_REBOOT_PERSISTENCE"))
    steps.append(DeploymentStep("CONFIRM_NEW_CHAIN"))
    if "CRASH_ROLLBACK" in faults:
        steps.extend((DeploymentStep("START_ROLLBACK"), DeploymentStep("CRASH_ROLLBACK")))
    return tuple(steps)


def run_deployment_chaos() -> dict[str, Any]:
    """Exhaust every non-empty combination of the seven cutover fault families."""

    cases: list[dict[str, Any]] = []
    for width in range(1, len(CHAOS_MUTATIONS) + 1):
        for combination in itertools.combinations(CHAOS_MUTATIONS, width):
            result = evaluate_deployment_sequence(_chaos_sequence(frozenset(combination)))
            cases.append(
                {
                    "faults": list(combination),
                    "detected": not result["pass"],
                    "violation_codes": result["violation_codes"],
                    "final_state": result["final_state"],
                }
            )
    detected = sum(bool(case["detected"]) for case in cases)
    return {
        "schema": "cra.deployment_choreography_chaos.v1",
        "classification": "PASS" if detected == len(cases) else "HARNESS_FAILURE",
        "scenario_family": "exhaustive_cutover_fault_combinations",
        "fault_family_count": len(CHAOS_MUTATIONS),
        "scenario_count": len(cases),
        "expected_failure_count": len(cases),
        "detected_failure_count": detected,
        "sut_failure_count": 0,
        "harness_failure_count": len(cases) - detected,
        "safety": {
            "production_mutation_count": 0,
            "network_failure_injection_count": 0,
            "physical_effect_count": 0,
            "ffmpeg_signal_count": 0,
        },
        "cases": cases,
    }


def evaluate_recovery_unit_persistence(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a separately captured three-host unit state.

    This comparator never contacts systemd. The operator must capture both
    active state and unit-file enablement from each physical host in one fresh
    pre/post-deployment window.
    """

    issues: list[str] = []
    if set(snapshot) != {"schema", "captured_at", "release_ids", "units", "obsolete_units"} or snapshot.get(
        "schema"
    ) != (
        "cra.recovery_unit_persistence_snapshot.v1"
    ):
        issues.append("UNIT_PERSISTENCE_SNAPSHOT_INVALID")
        rows: list[Any] = []
        obsolete_rows: list[Any] = []
        raw_releases: Any = None
    else:
        captured_at = snapshot.get("captured_at")
        if not isinstance(captured_at, str) or not captured_at:
            issues.append("UNIT_PERSISTENCE_CAPTURE_TIME_INVALID")
        raw_releases = snapshot.get("release_ids")
        raw_rows = snapshot.get("units")
        rows = raw_rows if isinstance(raw_rows, list) else []
        if not rows:
            issues.append("UNIT_PERSISTENCE_UNITS_MISSING")
        raw_obsolete = snapshot.get("obsolete_units")
        obsolete_rows = raw_obsolete if isinstance(raw_obsolete, list) else []
        if not isinstance(raw_obsolete, list):
            issues.append("UNIT_PERSISTENCE_OBSOLETE_INVENTORY_INVALID")

    required_kinds = {
        "dell-stream-runtime": {"tcp_collector", "observer_timer", "serve"},
        "arena-monitoring-facts": {"pull_timer", "observer_timer", "serve"},
        "cra-01-central-authority": {
            "pull_timer",
            "observer_timer",
            "soak_timer",
            "gate_timer",
            "watchdog_timer",
        },
    }
    unit_templates = {
        ("dell-stream-runtime", "tcp_collector"): "stream-v3-tcp-stall-collector@{release}.service",
        ("dell-stream-runtime", "observer_timer"): "dell-recovery-observer-{release}.timer",
        ("dell-stream-runtime", "serve"): "dell-recovery-serve-{release}.service",
        ("arena-monitoring-facts", "pull_timer"): "arena-recovery-pull-{release}.timer",
        ("arena-monitoring-facts", "observer_timer"): "arena-recovery-observer-{release}.timer",
        ("arena-monitoring-facts", "serve"): "arena-recovery-serve-{release}.service",
        ("cra-01-central-authority", "pull_timer"): "cra-recovery-pull-{release}.timer",
        ("cra-01-central-authority", "observer_timer"): "cra-recovery-observer-{release}.timer",
        ("cra-01-central-authority", "soak_timer"): "cra-recovery-soak@{release}.timer",
        ("cra-01-central-authority", "gate_timer"): "cra-recovery-soak-gate@{release}.timer",
        ("cra-01-central-authority", "watchdog_timer"): "cra-recovery-soak-watchdog@{release}.timer",
    }
    persistent_unit_patterns = {
        "dell-stream-runtime": (
            r"stream-v3-tcp-stall-collector@.+\.service",
            r"dell-recovery-observer-.+\.timer",
            r"dell-recovery-serve-.+\.service",
            r"dell-resilient-host-status-publisher@.+\.timer",
        ),
        "arena-monitoring-facts": (
            r"arena-recovery-pull-.+\.timer",
            r"arena-recovery-observer-.+\.timer",
            r"arena-recovery-serve-.+\.service",
            r"monitoring-v4-dell-resilient-status-pull@.+\.timer",
            r"monitoring-v4-resilient-status-publisher@.+\.timer",
        ),
        "cra-01-central-authority": (
            r"cra-recovery-pull-.+\.timer",
            r"cra-recovery-observer-.+\.timer",
            r"cra-recovery-soak@.+\.timer",
            r"cra-recovery-soak-gate@.+\.timer",
            r"cra-recovery-soak-watchdog@.+\.timer",
            r"cra-arena-resilient-status-pull@.+\.timer",
            r"cra-resilient-status-soak@.+\.timer",
            r"cra-resilient-status-soak-gate@.+\.timer",
            r"cra-resilient-status-soak-watchdog@.+\.timer",
        ),
    }
    expected_hosts = set(required_kinds)
    release_ids: dict[str, dict[str, str]] = {}
    if not isinstance(raw_releases, dict) or set(raw_releases) != expected_hosts:
        issues.append("UNIT_PERSISTENCE_RELEASE_SET_INVALID")
    else:
        for expected_host in sorted(expected_hosts):
            generations_value = raw_releases.get(expected_host)
            if not isinstance(generations_value, dict) or set(generations_value) != {"OLD", "NEW"}:
                issues.append("UNIT_PERSISTENCE_RELEASE_SET_INVALID")
                continue
            old = generations_value.get("OLD")
            new = generations_value.get("NEW")
            if (
                not isinstance(old, str)
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", old) is None
                or not isinstance(new, str)
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", new) is None
                or old == new
            ):
                issues.append("UNIT_PERSISTENCE_RELEASE_ID_INVALID")
                continue
            release_ids[expected_host] = {"OLD": old, "NEW": new}
    observed_hosts: set[str] = set()
    generations: dict[str, set[str]] = {host: set() for host in expected_hosts}
    identities: set[tuple[str, str, str]] = set()
    unit_names: set[tuple[str, str]] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "host_id", "unit", "kind", "generation", "active", "enabled",
        }:
            issues.append("UNIT_PERSISTENCE_ROW_INVALID")
            continue
        host = row.get("host_id")
        unit = row.get("unit")
        kind = row.get("kind")
        generation = row.get("generation")
        active = row.get("active")
        enabled = row.get("enabled")
        if (
            host not in expected_hosts
            or not isinstance(unit, str)
            or not unit
            or kind not in required_kinds.get(str(host), set())
            or generation not in {"OLD", "NEW"}
            or not isinstance(active, bool)
            or not isinstance(enabled, bool)
        ):
            issues.append("UNIT_PERSISTENCE_ROW_INVALID")
            continue
        assert isinstance(host, str) and isinstance(kind, str) and isinstance(generation, str)
        release = release_ids.get(host, {}).get(generation)
        if release is None or unit != unit_templates[(host, kind)].format(release=release):
            issues.append("UNIT_PERSISTENCE_UNIT_NAME_INVALID")
            continue
        identity = (host, generation, kind)
        unit_name = (host, unit)
        if identity in identities or unit_name in unit_names:
            issues.append("UNIT_PERSISTENCE_UNIT_DUPLICATE")
            continue
        identities.add(identity)
        unit_names.add(unit_name)
        observed_hosts.add(host)
        generations[host].add(generation)
        if generation == "OLD" and (active or enabled):
            issues.append(f"OLD_UNIT_NOT_QUIESCED:{host}:{unit}")
        if generation == "NEW" and (not active or not enabled):
            issues.append(f"NEW_UNIT_NOT_PERSISTENT:{host}:{unit}")
    if observed_hosts != expected_hosts:
        issues.append("UNIT_PERSISTENCE_HOST_SET_INCOMPLETE")
    if any(generations[host] != {"OLD", "NEW"} for host in expected_hosts):
        issues.append("UNIT_PERSISTENCE_GENERATIONS_INCOMPLETE")
    expected_identities = {
        (host, generation, kind)
        for host, kinds in required_kinds.items()
        for generation in ("OLD", "NEW")
        for kind in kinds
    }
    if identities != expected_identities:
        issues.append("UNIT_PERSISTENCE_INVENTORY_INCOMPLETE")
    obsolete_names: set[tuple[str, str]] = set()
    for row in obsolete_rows:
        if not isinstance(row, dict) or set(row) != {"host_id", "unit", "active", "enabled"}:
            issues.append("UNIT_PERSISTENCE_OBSOLETE_ROW_INVALID")
            continue
        host = row.get("host_id")
        unit = row.get("unit")
        active = row.get("active")
        enabled = row.get("enabled")
        if (
            host not in expected_hosts
            or not isinstance(unit, str)
            or not unit
            or not isinstance(active, bool)
            or not isinstance(enabled, bool)
        ):
            issues.append("UNIT_PERSISTENCE_OBSOLETE_ROW_INVALID")
            continue
        assert isinstance(host, str)
        key = (host, unit)
        if key in unit_names or key in obsolete_names:
            issues.append("UNIT_PERSISTENCE_UNIT_DUPLICATE")
            continue
        obsolete_names.add(key)
        if not any(re.fullmatch(pattern, unit) is not None for pattern in persistent_unit_patterns[host]):
            issues.append("UNIT_PERSISTENCE_OBSOLETE_UNIT_NAME_INVALID")
            continue
        if active or enabled:
            issues.append(f"OBSOLETE_UNIT_NOT_QUIESCED:{host}:{unit}")
    unique = sorted(set(issues))
    return {
        "schema": "cra.recovery_unit_persistence_check.v1",
        "status": "PASS" if not unique else "HOLD",
        "issues": unique,
        "captured_at": snapshot.get("captured_at"),
        "release_ids": release_ids,
        "unit_count": len(identities),
        "obsolete_unit_count": len(obsolete_names),
        "host_count": len(observed_hosts),
        "claim_boundary": "snapshot comparison only; capture freshness and physical host identity remain external prerequisites",
    }
