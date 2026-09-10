from __future__ import annotations

import itertools
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
    old_consumer_active: bool = True
    new_consumer_active: bool = False
    dell_new: bool = False
    arena_new: bool = False
    projection_fresh: bool = False
    rollback_started: bool = False
    rollback_completed: bool = False

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
        "old_consumer_active": composition.old_consumer_active,
        "new_consumer_active": composition.new_consumer_active,
        "dell_new": composition.dell_new,
        "arena_new": composition.arena_new,
        "projection_fresh": composition.projection_fresh,
        "effective_consumer_count": composition.effective_consumer_count,
        "coherent_chain_count": composition.coherent_chain_count,
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
        elif operation == "DUPLICATE_NEW_CONSUMER_START":
            composition.new_consumer_active = True
            add("DUPLICATE_NEW_CONSUMER_START", index, "a second effective start was requested")
        elif operation == "OLD_SERVICE_LATE_WAKEUP":
            composition.old_consumer_active = True
            add("OLD_CONSUMER_LATE_WAKEUP", index, "old consumer became effective after quiesce")
        elif operation == "CONFIRM_NEW_CHAIN":
            if composition.coherent_chain_count == 1 and composition.new_consumer_active:
                composition.state = DeploymentState.NEW_CHAIN_READY
            else:
                add("NEW_CHAIN_NOT_COHERENT", index, "new chain cannot be promoted")
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
            composition.old_consumer_active = True
            composition.new_consumer_active = False
            composition.dell_new = False
            composition.arena_new = False
            composition.projection_fresh = False
            composition.rollback_started = False
            composition.rollback_completed = True
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
    DeploymentStep("SWITCH_DELL"),
    DeploymentStep("SWITCH_ARENA"),
    DeploymentStep("CONFIRM_FRESH_PROJECTION"),
    DeploymentStep("START_NEW_CONSUMER"),
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
)


def _chaos_sequence(faults: frozenset[str]) -> tuple[DeploymentStep, ...]:
    steps = [DeploymentStep("LOSE_TIMER_STOP" if "LOSE_TIMER_STOP" in faults else "STOP_OLD_TIMER")]
    steps.append(DeploymentStep("DELAY_IN_FLIGHT" if "DELAY_IN_FLIGHT" in faults else "DRAIN_OLD_CONSUMER"))
    if "CRASH_UPSTREAM_SWITCH" in faults:
        steps.append(DeploymentStep("CRASH_UPSTREAM_SWITCH"))
    else:
        steps.append(DeploymentStep("SWITCH_DELL"))
    steps.extend((DeploymentStep("SWITCH_ARENA"), DeploymentStep("CONFIRM_FRESH_PROJECTION")))
    if "STALE_PROJECTION" in faults:
        steps.append(DeploymentStep("STALE_PROJECTION"))
    steps.append(DeploymentStep("START_NEW_CONSUMER"))
    if "DUPLICATE_NEW_CONSUMER_START" in faults:
        steps.append(DeploymentStep("DUPLICATE_NEW_CONSUMER_START"))
    if "OLD_SERVICE_LATE_WAKEUP" in faults:
        steps.append(DeploymentStep("OLD_SERVICE_LATE_WAKEUP"))
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
