from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from cra_harness.classifiers.taxonomy import Classification

BASE_EVIDENCE_REQUIREMENTS = (
    "central_state",
    "dell_state",
    "heartbeat_events",
    "lease_events",
    "target_before",
    "target_after",
)


class ScenarioSource(StrEnum):
    R7_CONTRACT = "R7_CONTRACT"
    HISTORICAL_EVENT = "HISTORICAL_EVENT"
    REGRESSION = "REGRESSION"
    NEGATIVE_CONTROL = "NEGATIVE_CONTROL"
    EXPLORATORY = "EXPLORATORY"


class InvariantOperator(StrEnum):
    EQ = "eq"
    LTE = "lte"
    GTE = "gte"
    IN = "in"
    CONTAINS = "contains"
    FALSE = "false"
    TRUE = "true"


@dataclass(frozen=True)
class InvariantExpectation:
    evidence: str
    operator: InvariantOperator
    expected: Any
    invariant_id: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> InvariantExpectation:
        return cls(
            evidence=str(value["evidence"]),
            operator=InvariantOperator(str(value["operator"])),
            expected=value.get("expected"),
            invariant_id=str(value["invariant_id"]),
        )


@dataclass(frozen=True)
class ScenarioSpec:
    scenario_id: str
    title: str
    category: str
    description: str
    initial_state: dict[str, Any]
    inputs: dict[str, Any]
    faults: tuple[dict[str, Any], ...]
    timing: dict[str, Any]
    required_evidence: tuple[str, ...]
    expected_invariants: tuple[InvariantExpectation, ...]
    expected_terminal_classification: Classification
    deterministic: bool
    source: ScenarioSource
    profile: str

    @property
    def negative_control(self) -> bool:
        return self.source == ScenarioSource.NEGATIVE_CONTROL

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ScenarioSpec:
        required = {
            "scenario_id",
            "title",
            "category",
            "description",
            "initial_state",
            "inputs",
            "faults",
            "timing",
            "required_evidence",
            "expected_invariants",
            "expected_terminal_classification",
            "deterministic",
            "source",
            "profile",
        }
        unknown = set(value) - required
        missing = required - set(value)
        if unknown or missing:
            raise ValueError(f"scenario keys mismatch: missing={sorted(missing)} unknown={sorted(unknown)}")
        scenario_id = str(value["scenario_id"])
        if not scenario_id or not scenario_id.replace("_", "").replace("-", "").isalnum():
            raise ValueError("scenario_id must be a non-empty stable identifier")
        declared_evidence = tuple(str(item) for item in value["required_evidence"])
        evidence = declared_evidence + tuple(item for item in BASE_EVIDENCE_REQUIREMENTS if item not in declared_evidence)
        if not evidence or len(evidence) != len(set(evidence)):
            raise ValueError("required_evidence must be non-empty and unique")
        invariants = tuple(InvariantExpectation.from_dict(dict(item)) for item in value["expected_invariants"])
        if not invariants:
            raise ValueError("expected_invariants must be non-empty")
        return cls(
            scenario_id=scenario_id,
            title=str(value["title"]),
            category=str(value["category"]),
            description=str(value["description"]),
            initial_state=dict(value["initial_state"]),
            inputs=dict(value["inputs"]),
            faults=tuple(dict(item) for item in value["faults"]),
            timing=dict(value["timing"]),
            required_evidence=evidence,
            expected_invariants=invariants,
            expected_terminal_classification=Classification(str(value["expected_terminal_classification"])),
            deterministic=bool(value["deterministic"]),
            source=ScenarioSource(str(value["source"])),
            profile=str(value["profile"]),
        )
