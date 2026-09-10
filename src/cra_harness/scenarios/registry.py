from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from cra_harness.scenarios.model import ScenarioSpec


class ScenarioRegistry:
    def __init__(self, scenarios: tuple[ScenarioSpec, ...], source_path: Path) -> None:
        identifiers = [item.scenario_id for item in scenarios]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("duplicate scenario_id")
        self.scenarios = scenarios
        self.source_path = source_path
        self.fixture_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()

    @classmethod
    def load(cls, path: Path) -> ScenarioRegistry:
        try:
            value: Any = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"malformed or missing scenario fixture: {path}") from exc
        if not isinstance(value, dict) or set(value) != {"registry_version", "scenarios"}:
            raise ValueError("scenario registry envelope is malformed")
        if value["registry_version"] != 1 or not isinstance(value["scenarios"], list):
            raise ValueError("unsupported scenario registry")
        return cls(tuple(ScenarioSpec.from_dict(dict(item)) for item in value["scenarios"]), path)

    def by_id(self, scenario_id: str) -> ScenarioSpec:
        for scenario in self.scenarios:
            if scenario.scenario_id == scenario_id:
                return scenario
        raise KeyError(scenario_id)

    def profile(self, profile: str) -> tuple[ScenarioSpec, ...]:
        return tuple(item for item in self.scenarios if item.profile == profile)
