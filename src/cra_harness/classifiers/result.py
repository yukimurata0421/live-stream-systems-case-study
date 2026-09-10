from __future__ import annotations

from cra_harness.classifiers.taxonomy import Classification, ClassificationResult
from cra_harness.observers.evidence import CompletenessResult
from cra_harness.oracles.contract import OracleResult
from cra_harness.scenarios.model import ScenarioSpec


class ResultClassifier:
    def classify(
        self,
        scenario: ScenarioSpec,
        completeness: CompletenessResult,
        oracle: OracleResult | None,
        *,
        harness_errors: tuple[str, ...] = (),
        environment_errors: tuple[str, ...] = (),
        unsupported: tuple[str, ...] = (),
        safety_errors: tuple[str, ...] = (),
        injector_errors: tuple[str, ...] = (),
    ) -> ClassificationResult:
        expected = scenario.expected_terminal_classification
        if safety_errors:
            return ClassificationResult(Classification.SAFETY_GATE_FAILURE, safety_errors, expected=expected)
        if environment_errors:
            return ClassificationResult(Classification.ENVIRONMENT_FAILURE, environment_errors, expected=expected)
        if unsupported:
            return ClassificationResult(Classification.UNSUPPORTED_CONDITION, unsupported, expected=expected)
        combined_harness = completeness.harness_errors + harness_errors + injector_errors
        if combined_harness:
            return ClassificationResult(Classification.HARNESS_FAILURE, combined_harness, expected=expected)
        if completeness.missing:
            return ClassificationResult(Classification.MISSING_EVIDENCE, completeness.missing, expected=expected)
        if oracle is None:
            return ClassificationResult(Classification.UNKNOWN_AMBIGUOUS, ("ORACLE_RESULT_ABSENT",), expected=expected)
        if oracle.errors:
            return ClassificationResult(Classification.HARNESS_FAILURE, oracle.errors, expected=expected)
        violations = tuple(item.invariant_id for item in oracle.violations)
        if scenario.negative_control:
            if violations:
                return ClassificationResult(Classification.EXPECTED_INJECTED_FAILURE, violations, violations, expected)
            return ClassificationResult(Classification.HARNESS_FAILURE, ("NEGATIVE_CONTROL_ESCAPED",), expected=expected)
        if violations:
            return ClassificationResult(Classification.SUT_FAILURE, violations, violations, expected)
        return ClassificationResult(Classification.PASS, ("ALL_INVARIANTS_HOLD",), expected=expected)
