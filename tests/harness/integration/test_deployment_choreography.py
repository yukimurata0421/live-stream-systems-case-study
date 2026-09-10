from __future__ import annotations

import pytest

from cra_harness.deployment_choreography import (
    CONSUMER_FIRST,
    FAILED_UPSTREAM_FIRST,
    DeploymentStep,
    evaluate_deployment_sequence,
    run_deployment_chaos,
)


def test_production_failed_upstream_first_sequence_is_a_regression_failure() -> None:
    result = evaluate_deployment_sequence(FAILED_UPSTREAM_FIRST)

    assert result["pass"] is False
    assert "OLD_INCOMPATIBLE_CONSUMER_WITH_NEW_UPSTREAM" in result["violation_codes"]
    assert "CUTOVER_TERMINAL_STATE_INCOMPLETE" in result["violation_codes"]
    assert result["safety"]["physical_effect_count"] == 0


def test_consumer_first_sequence_reaches_one_coherent_chain_without_effects() -> None:
    result = evaluate_deployment_sequence(CONSUMER_FIRST)

    assert result["pass"] is True
    assert result["final_state"] == "NEW_CHAIN_READY"
    assert max(item["effective_consumer_count"] for item in result["trace"]) == 1
    assert all(item["effect_count"] == 0 for item in result["trace"])


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("LOSE_TIMER_STOP", "OLD_TIMER_STOP_NOT_DURABLE"),
        ("DELAY_IN_FLIGHT", "OLD_CONSUMER_IN_FLIGHT"),
        ("CRASH_UPSTREAM_SWITCH", "UPSTREAM_SWITCH_PARTIAL"),
        ("STALE_PROJECTION", "NEW_PROJECTION_NOT_FRESH"),
        ("CRASH_ROLLBACK", "ROLLBACK_INCOMPLETE"),
        ("DUPLICATE_NEW_CONSUMER_START", "DUPLICATE_NEW_CONSUMER_START"),
        ("OLD_SERVICE_LATE_WAKEUP", "OLD_CONSUMER_LATE_WAKEUP"),
    ],
)
def test_sequence_mutations_are_detected(mutation: str, expected: str) -> None:
    prefix = [DeploymentStep("STOP_OLD_TIMER"), DeploymentStep("DRAIN_OLD_CONSUMER")]
    if mutation == "LOSE_TIMER_STOP":
        steps = [DeploymentStep(mutation), DeploymentStep("DRAIN_OLD_CONSUMER"), DeploymentStep("SWITCH_DELL")]
    elif mutation == "DELAY_IN_FLIGHT":
        steps = [DeploymentStep("STOP_OLD_TIMER"), DeploymentStep(mutation), DeploymentStep("SWITCH_DELL")]
    elif mutation == "CRASH_UPSTREAM_SWITCH":
        steps = [*prefix, DeploymentStep(mutation)]
    elif mutation == "STALE_PROJECTION":
        steps = [*prefix, DeploymentStep("SWITCH_DELL"), DeploymentStep("SWITCH_ARENA"), DeploymentStep(mutation)]
    elif mutation == "CRASH_ROLLBACK":
        steps = [*prefix, DeploymentStep("SWITCH_DELL"), DeploymentStep("START_ROLLBACK"), DeploymentStep(mutation)]
    elif mutation == "DUPLICATE_NEW_CONSUMER_START":
        steps = [*CONSUMER_FIRST[:-1], DeploymentStep(mutation)]
    else:
        steps = [*CONSUMER_FIRST[:-1], DeploymentStep(mutation)]

    result = evaluate_deployment_sequence(tuple(steps))

    assert result["pass"] is False
    assert expected in result["violation_codes"]


def test_cutover_activity_counter_mutant_is_detected() -> None:
    steps = (*CONSUMER_FIRST[:-1], DeploymentStep("CONFIRM_NEW_CHAIN", command_count=1))
    result = evaluate_deployment_sequence(steps)

    assert "CUTOVER_EXECUTABLE_ACTIVITY_NONZERO" in result["violation_codes"]


def test_all_seven_fault_families_are_detected_in_every_combination() -> None:
    result = run_deployment_chaos()

    assert result["classification"] == "PASS"
    assert result["scenario_count"] == 127
    assert result["detected_failure_count"] == 127
    assert result["safety"]["physical_effect_count"] == 0
