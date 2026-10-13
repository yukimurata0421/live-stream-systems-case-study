from __future__ import annotations

import pytest

from cra_harness.deployment_choreography import (
    CONSUMER_FIRST,
    FAILED_UPSTREAM_FIRST,
    DeploymentStep,
    evaluate_deployment_sequence,
    evaluate_recovery_unit_persistence,
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
        ("LEAVE_OLD_UNITS_ENABLED", "OLD_UNITS_REBOOT_ENABLED"),
        ("LEAVE_NEW_UNITS_DISABLED", "NEW_UNITS_REBOOT_DISABLED"),
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
    elif mutation == "LEAVE_OLD_UNITS_ENABLED":
        steps = [*prefix, DeploymentStep(mutation), *CONSUMER_FIRST[3:]]
    elif mutation == "LEAVE_NEW_UNITS_DISABLED":
        steps = [
            *CONSUMER_FIRST[:7],
            DeploymentStep(mutation),
            *CONSUMER_FIRST[8:],
        ]
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


def test_all_nine_fault_families_are_detected_in_every_combination() -> None:
    result = run_deployment_chaos()

    assert result["classification"] == "PASS"
    assert result["scenario_count"] == 511
    assert result["detected_failure_count"] == 511
    assert result["safety"]["physical_effect_count"] == 0


def _unit_snapshot() -> dict[str, object]:
    units = []
    names = {
        "dell-stream-runtime": {
            "tcp_collector": "stream-v3-tcp-stall-collector@{release}.service",
            "observer_timer": "dell-recovery-observer-{release}.timer",
            "serve": "dell-recovery-serve-{release}.service",
        },
        "arena-monitoring-facts": {
            "pull_timer": "arena-recovery-pull-{release}.timer",
            "observer_timer": "arena-recovery-observer-{release}.timer",
            "serve": "arena-recovery-serve-{release}.service",
        },
        "cra-01-central-authority": {
            "pull_timer": "cra-recovery-pull-{release}.timer",
            "observer_timer": "cra-recovery-observer-{release}.timer",
            "soak_timer": "cra-recovery-soak@{release}.timer",
            "gate_timer": "cra-recovery-soak-gate@{release}.timer",
            "watchdog_timer": "cra-recovery-soak-watchdog@{release}.timer",
        },
    }
    for host, kinds in names.items():
        for generation, release, active, enabled in (
            ("OLD", "old-release", False, False),
            ("NEW", "new-release", True, True),
        ):
            units.extend(
                {
                    "host_id": host,
                    "unit": pattern.format(release=release),
                    "kind": kind,
                    "generation": generation,
                    "active": active,
                    "enabled": enabled,
                }
                for kind, pattern in kinds.items()
            )
    return {
        "schema": "cra.recovery_unit_persistence_snapshot.v1",
        "captured_at": "2026-09-16T04:30:00+09:00",
        "release_ids": {
            host: {"OLD": "old-release", "NEW": "new-release"}
            for host in names
        },
        "units": units,
        "obsolete_units": [],
    }


def test_three_host_reboot_persistence_snapshot_requires_exact_old_and_new_states() -> None:
    result = evaluate_recovery_unit_persistence(_unit_snapshot())

    assert result["status"] == "PASS"
    assert result["host_count"] == 3
    assert result["unit_count"] == 22


@pytest.mark.parametrize(("generation", "field"), [("OLD", "enabled"), ("NEW", "active"), ("NEW", "enabled")])
def test_three_host_reboot_persistence_snapshot_rejects_late_or_nonpersistent_units(
    generation: str,
    field: str,
) -> None:
    snapshot = _unit_snapshot()
    row = next(item for item in snapshot["units"] if item["generation"] == generation)  # type: ignore[index,union-attr]
    row[field] = generation == "OLD"  # type: ignore[index]

    result = evaluate_recovery_unit_persistence(snapshot)

    assert result["status"] == "HOLD"
    assert result["issues"]


def test_three_host_reboot_persistence_snapshot_rejects_one_missing_unit_with_both_generations_present() -> None:
    snapshot = _unit_snapshot()
    snapshot["units"] = [  # type: ignore[index]
        row
        for row in snapshot["units"]  # type: ignore[index,union-attr]
        if not (
            row["host_id"] == "cra-01-central-authority"
            and row["generation"] == "NEW"
            and row["kind"] == "watchdog_timer"
        )
    ]

    result = evaluate_recovery_unit_persistence(snapshot)

    assert result["status"] == "HOLD"
    assert "UNIT_PERSISTENCE_INVENTORY_INCOMPLETE" in result["issues"]


def test_three_host_reboot_persistence_snapshot_rejects_kind_and_unit_name_mismatch() -> None:
    snapshot = _unit_snapshot()
    row = next(  # type: ignore[union-attr]
        item
        for item in snapshot["units"]  # type: ignore[index,union-attr]
        if item["host_id"] == "dell-stream-runtime" and item["kind"] == "serve"
    )
    row["unit"] = "dell-recovery-observer-new-release.timer"

    result = evaluate_recovery_unit_persistence(snapshot)

    assert result["status"] == "HOLD"
    assert "UNIT_PERSISTENCE_UNIT_NAME_INVALID" in result["issues"]


def test_three_host_reboot_persistence_snapshot_binds_exact_release_ids() -> None:
    snapshot = _unit_snapshot()
    snapshot["release_ids"]["dell-stream-runtime"]["NEW"] = "unrelated-release"  # type: ignore[index]

    result = evaluate_recovery_unit_persistence(snapshot)

    assert result["status"] == "HOLD"
    assert "UNIT_PERSISTENCE_UNIT_NAME_INVALID" in result["issues"]


def test_three_host_reboot_persistence_snapshot_rejects_obsolete_enabled_unit() -> None:
    snapshot = _unit_snapshot()
    snapshot["obsolete_units"] = [
        {
            "host_id": "dell-stream-runtime",
            "unit": "dell-recovery-serve-dell-observation-stale.service",
            "active": False,
            "enabled": True,
        }
    ]

    result = evaluate_recovery_unit_persistence(snapshot)

    assert result["status"] == "HOLD"
    assert result["obsolete_unit_count"] == 1
    assert result["issues"] == [
        "OBSOLETE_UNIT_NOT_QUIESCED:dell-stream-runtime:"
        "dell-recovery-serve-dell-observation-stale.service"
    ]
