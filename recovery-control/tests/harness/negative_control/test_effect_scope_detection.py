from __future__ import annotations

from cra_harness.effect_scope import effect_scope_violations


def test_same_scope_different_id_mutant_is_detected() -> None:
    mutant_events = (
        {"request_id": "first", "effect_scope_id": "scope-a", "physical_attempt_count": 1},
        {"request_id": "second", "effect_scope_id": "scope-a", "physical_attempt_count": 1},
    )

    violations = effect_scope_violations(mutant_events)

    assert len(violations) == 1
    assert violations[0]["observed"] == 2


def test_two_generations_are_two_scopes_and_do_not_trigger_oracle() -> None:
    events = (
        {"request_id": "first", "effect_scope_id": "scope-generation-a", "physical_attempt_count": 1},
        {"request_id": "second", "effect_scope_id": "scope-generation-b", "physical_attempt_count": 1},
    )
    assert effect_scope_violations(events) == ()
