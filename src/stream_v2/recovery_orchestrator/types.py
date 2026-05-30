from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GateResult:
    gates: dict[str, Any]
    passed: bool
    blocked_by: list[str]


@dataclass(frozen=True)
class OrchestratorDecision:
    event: dict[str, Any]
    selected_action: str
    execute: bool
    execution_plan: dict[str, Any]
