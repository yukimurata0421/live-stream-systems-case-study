from __future__ import annotations

import ast
from pathlib import Path

import pytest

from cra_harness.runner.p2_disabled import negative_controls, observation, run, snapshot
from maintenance_enforcement import evaluate_enforcement_shadow

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_p2_disabled_harness_is_locally_verified() -> None:
    result = run()
    assert result["status"] == "LOCAL_VERIFIED"
    assert result["deterministic_count"] == 10
    assert result["deterministic_safe"] is True
    assert result["negative_control_count"] == 10
    assert result["negative_control_detected"] == 10
    assert result["independent_oracle"] == "PASS"
    assert result["physical_effect_count"] == 0
    assert result["production_adapter_connected"] is False


def test_enforcement_activation_is_not_available_in_p2() -> None:
    with pytest.raises(ValueError, match="activation is prohibited"):
        evaluate_enforcement_shadow(observation(), snapshot(active=False), enforcement_enabled=True)


def test_negative_control_inventory_is_exact() -> None:
    assert [case["control_id"] for case in negative_controls()] == [f"NC-P2-{number:02d}" for number in range(1, 11)]


def test_enforcement_model_imports_no_production_adapter() -> None:
    path = PROJECT_ROOT / "src/maintenance_enforcement/model.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = ("dell_recovery_agent", "cra_authority", "subprocess", "kubernetes")
    assert not any(name.startswith(prefix) for name in imported for prefix in forbidden)
