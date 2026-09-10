from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.recovery_verification import CHECK_NAMES, RecoveryVerificationContract
from cra_harness.oracles.recovery_verification import IndependentRecoveryVerificationOracle
from cra_harness.verification_contract.fake_monitoring import FakeMonitoringObserver


@dataclass(frozen=True)
class VerificationOutcome:
    scenario_id: str
    classification: str
    expected_verdict: str
    observed_verdict: str
    expected_ingest: str
    observed_ingest: str
    oracle_violations: tuple[str, ...]
    verification: dict[str, Any]
    duration_ms: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "profile": "verification",
            "classification": self.classification,
            "expected_verdict": self.expected_verdict,
            "observed_verdict": self.observed_verdict,
            "expected_ingest": self.expected_ingest,
            "observed_ingest": self.observed_ingest,
            "oracle_violations": list(self.oracle_violations),
            "verification": self.verification,
            "duration_ms": self.duration_ms,
        }


def _checks(mode: str, observed_at: str) -> dict[str, dict[str, str]]:
    values = {name: "PASS" for name in CHECK_NAMES}
    roles = {name: "current_authoritative" for name in CHECK_NAMES}
    roles["external_public_signal"] = "supporting"
    roles["youtube_state"] = "current_correlated"
    if mode == "tcp_stalled":
        values["tcp_flow_healthy"] = "FAIL"
    elif mode == "pod_ready_only":
        values = {name: "UNKNOWN" for name in CHECK_NAMES}
        values["stream_engine_ready"] = "PASS"
    elif mode == "youtube_only":
        values = {name: "UNKNOWN" for name in CHECK_NAMES}
        values["youtube_state"] = "PASS"
    return {
        name: {
            "result": values[name],
            "evidence_ref": f"evidence-{mode}-{name}",
            "observed_at": observed_at,
            "evidence_role": roles[name],
        }
        for name in CHECK_NAMES
    }


def run_recovery_verification_suite(project_root: Path) -> list[VerificationOutcome]:
    registry = json.loads((project_root / "harness/scenarios/recovery_verification_v1.json").read_text(encoding="utf-8"))
    contract = RecoveryVerificationContract(project_root / "contracts/monitoring_v4/recovery_verification.v1.schema.json")
    observer = FakeMonitoringObserver()
    oracle = IndependentRecoveryVerificationOracle()
    before = TargetIdentity("dell", "boot-a", "default", "pod-a", "stream-engine", "container-a", "run-a:0:4100", 4100)
    after = TargetIdentity("dell", "boot-a", "default", "pod-a", "stream-engine", "container-a", "run-a:1:4200", 4200)
    outcomes: list[VerificationOutcome] = []
    for item in registry["scenarios"]:
        started = time.perf_counter_ns()
        scenario_id = str(item["scenario_id"])
        mode = str(item["mode"])
        observed_at = "2026-08-23T04:00:10.000Z"
        fresh_until = "2026-08-23T04:01:10.000Z"
        evaluated_at = "2026-08-23T04:00:20.000Z"
        if mode == "stale":
            fresh_until = "2026-08-23T04:00:15.000Z"
        observed_target = after
        if mode == "unexpected_target":
            observed_target = TargetIdentity(
                "dell", "boot-a", "default", "pod-replaced", "stream-engine", "container-b", "run-b:0:4300", 4300
            )
        command_id = "command-old" if mode == "old_command" else "command-active"
        verification = observer.verify(
            command_id=command_id,
            incident_id="incident-active",
            target_id="stream-target",
            monitoring_cycle_id=f"cycle-{scenario_id}",
            command_state="EFFECT_OBSERVED",
            before_target=before,
            observed_target=observed_target,
            checks=_checks(mode, observed_at),
            observed_at=observed_at,
            evidence_fresh_until=fresh_until,
            evaluated_at=evaluated_at,
            observation_revision=f"observation-{scenario_id}",
        )
        contract.validate(verification)
        expected_verdict = str(item["expected_verdict"])
        violations = oracle.evaluate(
            verification,
            expected_verdict=expected_verdict,
            active_command_id="command-active",
            evaluated_at=evaluated_at,
        )
        expected_ingest = str(item["expected_ingest"])
        if mode == "old_command":
            observed_ingest = "REJECTED_OLD_COMMAND" if violations == ("OLD_COMMAND_VERIFICATION",) else "ACCEPTED"
            passed = observed_ingest == expected_ingest
        else:
            observed_ingest = "RECONCILE" if mode == "unexpected_target" else "ACCEPTED"
            passed = not violations and observed_ingest == expected_ingest
        outcomes.append(
            VerificationOutcome(
                scenario_id,
                "PASS" if passed else "SUT_FAILURE",
                expected_verdict,
                str(verification["verdict"]),
                expected_ingest,
                observed_ingest,
                violations,
                verification,
                round((time.perf_counter_ns() - started) / 1_000_000, 6),
            )
        )
    return outcomes
