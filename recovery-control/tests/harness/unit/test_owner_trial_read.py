from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tools.verify_owner_trial_read import build_proof, read_object

ROOT = Path(__file__).resolve().parents[3]


def encoded(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def test_build_proof_binds_complete_documents_and_observations() -> None:
    canonical = json.loads((ROOT / "harness/scenarios/owner_observation_chaos_v1.json").read_text())
    trial = copy.deepcopy(canonical)
    omitted = trial["assurance"]["bindings"].pop(2)
    observations = {"native_cycles": 64, "physical_attempt_count": 0}
    proof = build_proof(
        encoded(trial), trial, encoded(canonical), canonical, encoded(observations), observations
    )
    missing = proof["missing_bindings"]
    assert len(missing) == 1
    assert missing[0]["verification_case_id"] == omitted["verification_case_id"]
    assert missing[0]["invariant_id"] == omitted["invariant_id"]
    assert missing[0]["mechanism"] == omitted["mechanism"]
    assert missing[0]["probe_count"] == len(omitted["probes"])
    assert len(missing[0]["binding_sha256"]) == 64
    assert len(missing[0]["nodeids_sha256"]) == 64
    assert proof["extra_binding_count"] == 0
    assert proof["whole_document_equal_after_removing_missing_bindings"] is True
    assert proof["exactly_one_missing_and_no_other_difference"] is True
    assert proof["observations"] == observations
    assert len(json.dumps(proof, ensure_ascii=False).encode()) < 16 * 1024


def test_build_proof_rejects_an_additional_document_difference() -> None:
    canonical = json.loads((ROOT / "harness/scenarios/owner_observation_chaos_v1.json").read_text())
    trial = copy.deepcopy(canonical)
    trial["assurance"]["bindings"].pop(0)
    trial["execution_boundary"]["external_network"] = True
    proof = build_proof(encoded(trial), trial, encoded(canonical), canonical, encoded({}), {})
    assert proof["whole_document_equal_after_removing_missing_bindings"] is False
    assert proof["exactly_one_missing_and_no_other_difference"] is False


def test_read_object_rejects_duplicate_keys(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"key":1,"key":2}\n')
    with pytest.raises(ValueError, match="DUPLICATE_KEY"):
        read_object(path)
