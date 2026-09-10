from __future__ import annotations

import ast
from pathlib import Path

from cra_harness.oracles.evidence_binding import evidence_binding_violations, independent_expected
from cra_harness.runner.evidence_binding import negative_controls, run

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_evidence_binding_negative_controls_are_all_detected() -> None:
    result = run()
    assert result["negative_control_count"] == 15
    assert result["negative_control_detected"] == 15
    assert result["negative_control_detection_rate"] == 1.0
    assert result["independent_oracle"] == "PASS"
    assert result["physical_effect_count"] == 0


def test_each_control_is_an_actual_oracle_disagreement() -> None:
    controls = negative_controls()
    assert [case["control_id"] for case in controls] == [f"NC-E{number:02d}" for number in range(1, 16)]
    for case in controls:
        assert evidence_binding_violations(case)
        assert independent_expected(case) != case["observed_result"]


def test_independent_oracle_imports_no_production_binding_implementation() -> None:
    oracle_path = PROJECT_ROOT / "src/cra_harness/oracles/evidence_binding.py"
    tree = ast.parse(oracle_path.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    assert not any(
        name.startswith(prefix)
        for name in imported
        for prefix in ("maintenance_audit", "maintenance_shadow", "dell_recovery_agent", "cra_authority")
    )
