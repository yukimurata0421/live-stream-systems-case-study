from __future__ import annotations

from pathlib import Path

import pytest

from cra_dell_recovery.recovery_verification import RecoveryVerificationContract
from cra_harness.runner.recovery_verification import run_recovery_verification_suite

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize("scenario_id", [f"V-{index:02d}" for index in range(1, 8)])
def test_recovery_verification_contract_scenarios(scenario_id: str) -> None:
    outcomes = {item.scenario_id: item for item in run_recovery_verification_suite(ROOT)}
    assert outcomes[scenario_id].classification == "PASS"


def test_recovery_verification_schema_rejects_payload_hash_conflict() -> None:
    outcome = run_recovery_verification_suite(ROOT)[0]
    changed = {**outcome.verification, "verdict": "FAILED"}
    contract = RecoveryVerificationContract(ROOT / "contracts/monitoring_v4/recovery_verification.v1.schema.json")
    with pytest.raises(ValueError, match="payload_sha256 mismatch"):
        contract.validate(changed)
