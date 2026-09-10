from __future__ import annotations

from typing import Any

import pytest

from tools.run_owner_observation_chaos import mutation_detected


@pytest.mark.parametrize(
    "fault", ["none", "passed-mutant", "timeout", "harness-error", "empty", "skipped", "wrong-assertion", "import-error", "duplicate"]
)
def test_negative_control_requires_the_intended_assertion(fault: str) -> None:
    value: dict[str, Any] = {
        "classification": "TEST_FAILURE",
        "counts": {"tests": 1, "failures": 1, "errors": 0, "skipped": 0},
        "collected": 1,
        "failures": [{"message": "assert RUNNING == ABSENT", "text": "AssertionError: RUNNING == ABSENT"}],
    }
    if fault == "passed-mutant":
        value["classification"] = "PASS"
    elif fault == "timeout":
        value = {"classification": "TIMEOUT"}
    elif fault == "harness-error":
        value["counts"]["errors"] = 1
    elif fault == "empty":
        value["collected"] = 0
    elif fault == "skipped":
        value["counts"]["skipped"] = 1
    elif fault == "wrong-assertion":
        value["failures"][0]["message"] = "unrelated assertion"
    elif fault == "import-error":
        value["failures"][0]["text"] = "ImportError: ABSENT"
    elif fault == "duplicate":
        value["failures"] *= 2
    assert mutation_detected(value, "ABSENT") is (fault == "none")
