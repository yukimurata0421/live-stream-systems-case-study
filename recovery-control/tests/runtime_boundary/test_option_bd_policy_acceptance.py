from __future__ import annotations

import importlib.util
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

MODULE_PATH = Path(__file__).parents[2] / "tools/validate_option_bd_policy_acceptance.py"
SPEC = importlib.util.spec_from_file_location("option_bd_policy_acceptance", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
evaluate = MODULE.evaluate


def valid_policy() -> dict[str, Any]:
    return {
        "complete": True,
        "independent_oracle_pass": True,
        "negative_controls": {"NC-PS-01": True},
        "negative_controls_detected": 1,
        "negative_controls_total": 1,
        "unexplained_drift": 0,
        "intentional_change_not_activated": 1,
        "rows": [
            {
                "scenario_id": "POL-07",
                "parity": "INTENTIONAL_SAFETY_DIFFERENCE_NOT_ACTIVATED",
                "candidate": {
                    "intent_type": "NO_ACTION",
                    "effect_owner": "none",
                    "effect_scope": "none",
                    "failure_domain": "UNKNOWN",
                    "automatic_retry": False,
                },
            }
        ],
    }


def valid_decision() -> dict[str, Any]:
    return {
        "status": "ACCEPTED",
        "decision_id": "DEC-08-SAFE-NO-ACTION-V11",
        "release_id": "runtime-boundary-20260825T0850JST-v11",
        "identity_fallback_allowed": False,
        "maintenance_p2_enforcement_allowed": False,
        "maximum_controlled_recreate_count": 1,
        "missing_stale_unknown_runtime_identity": {
            "automatic_retry": False,
            "effect_scope": "none",
            "intent": "NO_ACTION",
            "physical_effect_count": 0,
        },
    }


def test_exact_accepted_difference_passes() -> None:
    result = evaluate(valid_policy(), valid_decision())
    assert result["pass"] is True
    assert result["accepted_intentional_difference_count"] == 1
    assert result["physical_effect_count"] == 0


@pytest.mark.parametrize(
    ("control_id", "mutate"),
    [
        ("NC-ACT-01", lambda p, d: p.update(unexplained_drift=1)),
        ("NC-ACT-02", lambda p, d: p["rows"][0]["candidate"].update(intent_type="ESCALATE_RUNTIME_RECOVERY")),
        ("NC-ACT-03", lambda p, d: p["rows"][0]["candidate"].update(effect_scope="runtime")),
        ("NC-ACT-04", lambda p, d: p["rows"][0]["candidate"].update(automatic_retry=True)),
        ("NC-ACT-05", lambda p, d: d.update(identity_fallback_allowed=True)),
        ("NC-ACT-06", lambda p, d: d.update(maintenance_p2_enforcement_allowed=True)),
        ("NC-ACT-07", lambda p, d: d.update(maximum_controlled_recreate_count=2)),
        ("NC-ACT-08", lambda p, d: d.update(release_id="different-release")),
        ("NC-ACT-09", lambda p, d: d.update(status="PENDING")),
        (
            "NC-ACT-10",
            lambda p, d: d["missing_stale_unknown_runtime_identity"].update(physical_effect_count=1),
        ),
    ],
)
def test_negative_control_is_rejected(
    control_id: str,
    mutate: Callable[[dict[str, Any], dict[str, Any]], None],
) -> None:
    policy = deepcopy(valid_policy())
    decision = deepcopy(valid_decision())
    mutate(policy, decision)
    assert evaluate(policy, decision)["pass"] is False, control_id
