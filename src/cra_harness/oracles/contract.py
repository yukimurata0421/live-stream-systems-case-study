from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cra_harness.observers.evidence import EvidenceBundle
from cra_harness.scenarios.model import InvariantExpectation, InvariantOperator, ScenarioSpec


@dataclass(frozen=True)
class OracleViolation:
    invariant_id: str
    evidence_path: str
    operator: str
    expected: Any
    observed: Any


@dataclass(frozen=True)
class OracleResult:
    evaluated: int
    violations: tuple[OracleViolation, ...]
    errors: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return self.evaluated > 0 and not self.violations and not self.errors


class IndependentOracle:
    """Evaluate immutable expectations against evidence without SUT decision code."""

    def evaluate(self, scenario: ScenarioSpec, evidence: EvidenceBundle) -> OracleResult:
        return self.evaluate_expectations(scenario.expected_invariants, evidence)

    def evaluate_expectations(self, expectations: tuple[InvariantExpectation, ...], evidence: EvidenceBundle) -> OracleResult:
        """Share fixed comparisons with other SUT profiles without inventing protocol events."""
        violations: list[OracleViolation] = []
        errors: list[str] = []
        evaluated = 0
        for expectation in expectations:
            try:
                observed = evidence.scalar(expectation.evidence)
                valid = self._compare(expectation, observed)
                evaluated += 1
                if not valid:
                    violations.append(
                        OracleViolation(
                            expectation.invariant_id,
                            expectation.evidence,
                            expectation.operator.value,
                            expectation.expected,
                            observed,
                        )
                    )
            except (KeyError, TypeError, ValueError) as error:
                errors.append(f"{expectation.invariant_id}:{type(error).__name__}")
        return OracleResult(evaluated, tuple(violations), tuple(errors))

    @staticmethod
    def _compare(expectation: InvariantExpectation, observed: Any) -> bool:
        operator = expectation.operator
        if operator == InvariantOperator.EQ:
            return bool(observed == expectation.expected)
        if operator == InvariantOperator.LTE:
            return bool(observed <= expectation.expected)
        if operator == InvariantOperator.GTE:
            return bool(observed >= expectation.expected)
        if operator == InvariantOperator.IN:
            return bool(observed in expectation.expected)
        if operator == InvariantOperator.CONTAINS:
            return bool(expectation.expected in observed)
        if operator == InvariantOperator.TRUE:
            return observed is True
        if operator == InvariantOperator.FALSE:
            return observed is False
        raise ValueError(f"unsupported operator: {operator}")
