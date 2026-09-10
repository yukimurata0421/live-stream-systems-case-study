from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum


class Classification(StrEnum):
    PASS = "PASS"
    SUT_FAILURE = "SUT_FAILURE"
    HARNESS_FAILURE = "HARNESS_FAILURE"
    MISSING_EVIDENCE = "MISSING_EVIDENCE"
    UNKNOWN_AMBIGUOUS = "UNKNOWN_AMBIGUOUS"
    EXPECTED_INJECTED_FAILURE = "EXPECTED_INJECTED_FAILURE"
    UNSUPPORTED_CONDITION = "UNSUPPORTED_CONDITION"
    ENVIRONMENT_FAILURE = "ENVIRONMENT_FAILURE"
    SAFETY_GATE_FAILURE = "SAFETY_GATE_FAILURE"


@dataclass(frozen=True)
class ClassificationResult:
    classification: Classification
    reason_codes: tuple[str, ...]
    oracle_violations: tuple[str, ...] = ()
    expected: Classification | None = None

    @property
    def harness_success(self) -> bool:
        return self.classification in {
            Classification.PASS,
            Classification.EXPECTED_INJECTED_FAILURE,
            Classification.UNSUPPORTED_CONDITION,
        }

    @property
    def matches_expectation(self) -> bool:
        return self.expected is None or self.classification == self.expected

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["classification"] = self.classification.value
        value["expected"] = None if self.expected is None else self.expected.value
        value["matches_expectation"] = self.matches_expectation
        return value
