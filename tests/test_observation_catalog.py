import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("catalog", Path(__file__).parents[1] / "ops/scripts/check_observation_catalog.py")
catalog = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(catalog)


def fixture(tmp_path):
    folder = tmp_path / "configs/observation"
    folder.mkdir(parents=True)
    (tmp_path / "source.py").write_text("MAX_AGE = 180\n")
    policy = {"schema": "stream.observation_policy_catalog.v1", "entries": [
        {"id": "freshness", "owner": "source", "value": 180, "unit": "seconds", "basis": "cadence policy",
         "source": {"path": "source.py", "symbol": "MAX_AGE"}}]}
    retirement = {"schema": "stream.observation_retirement_plan.v1", "review_dates_are_not_deletion_authority": True,
        "entries": [{"id": "history", "owner": "history", "state": "retained_by_design", "consumers": ["SLI"],
                     "reason": "independent history", "acceptance": "preserve history", "review_due_jst": "2026-11-10"}]}
    (folder / "policy-catalog.json").write_text(json.dumps(policy))
    (folder / "retirement-plan.json").write_text(json.dumps(retirement))
    return folder


def test_catalog_rejects_source_drift(tmp_path):
    fixture(tmp_path)
    assert catalog.check(tmp_path)["status"] == "PASS"
    (tmp_path / "source.py").write_text("MAX_AGE = 1\n")
    with pytest.raises(AssertionError, match="policy/source drift"):
        catalog.check(tmp_path)


def test_review_date_does_not_grant_deletion_authority(tmp_path):
    folder = fixture(tmp_path)
    p = folder / "retirement-plan.json"
    value = json.loads(p.read_text())
    value["review_dates_are_not_deletion_authority"] = False
    p.write_text(json.dumps(value))
    with pytest.raises(AssertionError):
        catalog.check(tmp_path)
