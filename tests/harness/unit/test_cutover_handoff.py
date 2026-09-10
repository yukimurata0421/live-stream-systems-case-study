from __future__ import annotations

import copy
from datetime import UTC, datetime

import pytest

from cra_harness.cutover_handoff import _valid_fixture, run_cutover_handoff_chaos, validate_no_action_cutover_handoff

NOW = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)


def _validate(fixture: dict) -> dict:
    return validate_no_action_cutover_handoff(
        **fixture,
        expected_arena_release="arena-cra-projection-test",
        expected_cra_release="cra-no-action-test",
        now=NOW,
    )


def test_cutover_handoff_accepts_exact_schema_scoped_evidence() -> None:
    fixture = _valid_fixture(NOW, 20.0)
    assert "control_capability_count" not in fixture["arena_projection"]
    assert "physical_effect_count" not in fixture["arena_projection"]

    report = _validate(fixture)

    assert report["status"] == "PASS"
    assert report["remaining_seconds"] == 20.0
    assert report["transport_retry_count"] == 0


def test_cutover_handoff_rejects_inbox_mutation_without_changing_arena_evidence() -> None:
    fixture = _valid_fixture(NOW, 20.0)
    fixture["cra_projection"] = copy.deepcopy(fixture["arena_projection"])
    fixture["cra_projection"]["projection_id"] = "projection-mutated"

    with pytest.raises(ValueError, match="CRA_INBOX_PROJECTION_MISMATCH"):
        _validate(fixture)


def test_cutover_handoff_rejects_expiring_projection() -> None:
    fixture = _valid_fixture(NOW, 9.999)

    with pytest.raises(ValueError, match="CRA_INBOX_PROJECTION_TTL_INSUFFICIENT"):
        _validate(fixture)


def test_cutover_handoff_chaos_detects_all_mutation_classes() -> None:
    report = run_cutover_handoff_chaos(case_count=4096, seed=20260901)

    assert report["pass"] is True
    assert report["failure_count"] == 0
    assert all(count > 0 for count in report["scenario_detection_count"].values())
    assert report["production_target_touched"] is False
