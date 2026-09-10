from __future__ import annotations

import copy
from pathlib import Path

import pytest

from tools.run_postmortem_io_harness import BOUND_INPUT_FILES, EXPECTED_IDS, load_catalog

ROOT = Path(__file__).parents[3]
CATALOG = ROOT / "harness/scenarios/postmortem_io_v1.json"


def test_catalog_closes_all_six_regression_and_negative_control_cells() -> None:
    value = load_catalog(CATALOG, ROOT)
    assert tuple(item["scenario_id"] for item in value["scenarios"]) == EXPECTED_IDS
    assert all(item["regression_nodeids"] for item in value["scenarios"])
    assert all(item["negative_control_nodeids"] for item in value["scenarios"])
    assert value["execution_boundary"]["production_mutation"] is False
    assert value["execution_boundary"]["production_credentials"] is False


@pytest.mark.parametrize("missing", ["regression_nodeids", "negative_control_nodeids", "required_observations"])
def test_catalog_rejects_an_empty_required_cell(tmp_path: Path, missing: str) -> None:
    import json

    value = copy.deepcopy(load_catalog(CATALOG, ROOT))
    value["scenarios"][0][missing] = []
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match=missing.upper()):
        load_catalog(path, ROOT)


def test_catalog_does_not_execute_incident_source_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    import urllib.request

    monkeypatch.setattr(urllib.request, "urlopen", lambda *_args, **_kwargs: pytest.fail("network access attempted"))
    value = load_catalog(CATALOG, ROOT)
    assert sum(len(item["sources"]) for item in value["scenarios"]) >= 6


def test_operator_projection_is_bound_to_postmortem_evidence_identity() -> None:
    assert "src/cra_no_action_soak/operator_status.py" in BOUND_INPUT_FILES


def test_full_regression_runtime_gate_is_bound_to_postmortem_evidence_identity() -> None:
    assert "tools/run_full_regression.sh" in BOUND_INPUT_FILES
    assert "tools/sqlite_runtime/check_fixed.py" in BOUND_INPUT_FILES
    assert "src/cra_harness/controls/sqlite_runtime.py" in BOUND_INPUT_FILES
