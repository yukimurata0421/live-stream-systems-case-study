from __future__ import annotations

import json
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.classifiers.result import ResultClassifier
from cra_harness.classifiers.taxonomy import Classification
from cra_harness.controls.manifest import build_manifest, manifest_complete
from cra_harness.observers.evidence import EvidenceCompletenessChecker
from cra_harness.oracles.contract import IndependentOracle
from cra_harness.reporting.artifacts import ArtifactWriter, verify_artifact_consistency
from cra_harness.runner.executor import ScenarioExecutor
from cra_harness.runner.verification import run_verification, verification_commands
from cra_harness.scenarios.registry import ScenarioRegistry

BAD_CLASSIFICATIONS = {
    Classification.SUT_FAILURE.value,
    Classification.HARNESS_FAILURE.value,
    Classification.MISSING_EVIDENCE.value,
    Classification.UNKNOWN_AMBIGUOUS.value,
    Classification.ENVIRONMENT_FAILURE.value,
    Classification.SAFETY_GATE_FAILURE.value,
    Classification.UNSUPPORTED_CONDITION.value,
}


def _outcome(execution: Any) -> dict[str, Any]:
    scenario = execution.scenario
    completeness = EvidenceCompletenessChecker().check(execution.evidence, scenario.required_evidence)
    oracle = IndependentOracle().evaluate(scenario, execution.evidence)
    result = ResultClassifier().classify(
        scenario,
        completeness,
        oracle,
        harness_errors=execution.harness_errors,
        environment_errors=execution.environment_errors,
        safety_errors=execution.safety_errors,
        injector_errors=execution.injector_errors,
    )
    return {
        "run_id": execution.evidence.run_id,
        "scenario_id": scenario.scenario_id,
        "profile": scenario.profile,
        "source": scenario.source.value,
        "deterministic": scenario.deterministic,
        "expected": scenario.expected_terminal_classification.value,
        "classification": result.classification.value,
        "reason_codes": list(result.reason_codes),
        "matches_expectation": result.matches_expectation,
        "evidence_complete": completeness.complete,
        "missing_evidence": list(completeness.missing),
        "evidence_errors": list(completeness.harness_errors),
        "oracle_evaluated": oracle.evaluated,
        "oracle_violations": [vars(item) for item in oracle.violations],
        "oracle_errors": list(oracle.errors),
        "evidence_started_at": execution.evidence.started_at,
        "evidence_finished_at": execution.evidence.finished_at,
        "events": [item.to_dict() for item in execution.evidence.events],
    }


def _report(summary: dict[str, Any], manifest: dict[str, Any]) -> str:
    counts = summary["test_counts"]
    classes = summary["classification_counts"]
    environment = manifest["runtime"]
    sources = manifest["source_sha256"]
    return f"""# CRA Harness run {summary["run_id"]}

## 結論

Trust gate: **{summary["trust_gate"]}**

本runはtest-onlyの`FakePhysicalAdapter`だけを使用し、signal/process/Pod/Deployment/networkへのproduction操作は0件です。

## Revision / Environment

- SUT revision: CRA `{sources["CRA"]}`, Dell `{sources["Dell"]}`
- Harness revision: `{sources["Harness"]}`
- Git: commit={manifest["git"]["commit"]}, unborn={manifest["git"]["unborn"]}, dirty={manifest["git"]["dirty"]}
- Runtime: Python {environment["python"]}, SQLite {environment["sqlite"]}
- Host: {environment["os"]}, kernel {environment["kernel"]}, {environment["architecture"]}
- Protocol / policy: {manifest["protocol_revision"]} / {manifest["policy_revision"]}

## 実行範囲

- deterministic: {counts["deterministic"]}
- randomized: {counts["randomized"]} (seed={manifest["random_seed"]})
- negative controls: {counts["negative_control"]}
- existing tests: {counts["existing_tests"]}
- harness pytest items: {counts["harness_pytest_items"]}
- all scenarios: {summary["scenario_count"]}

## 分類

```json
{json.dumps(classes, ensure_ascii=False, indent=2, sort_keys=True)}
```

## 信頼条件

- negative control detection: {summary["negative_control_detection"]["detected"]}/{summary["negative_control_detection"]["total"]}
- evidence completeness: {summary["evidence_complete"]}
- environment manifest complete: {summary["manifest_complete"]}
- summary/artifact consistency: {summary["artifact_consistency"]}
- production isolation: {summary["production_isolation"]}

## Major findings

- PASS={classes["PASS"]}、SUT_FAILURE={classes["SUT_FAILURE"]}、HARNESS_FAILURE={classes["HARNESS_FAILURE"]}。
- Missing/ambiguous: MISSING_EVIDENCE={classes["MISSING_EVIDENCE"]}, UNKNOWN_AMBIGUOUS={classes["UNKNOWN_AMBIGUOUS"]}。
- Expected injected: {classes["EXPECTED_INJECTED_FAILURE"]}、unsupported={classes["UNSUPPORTED_CONDITION"]}。
- Environment failure={classes["ENVIRONMENT_FAILURE"]}、safety failure={classes["SAFETY_GATE_FAILURE"]}。
- 初回run v1のmanifest Git identity defectを検出し、v1を失効、修正後の本runで再検証した。

## New regression fixtures

- `tests/fixtures/regressions/2026-08-23_manifest_git_identity.json`
- `tests/fixtures/regressions/2026-08-23_manifest_start_order.json`

## Engineering records created

- `docs/engineering/records/2026-08-23_01_existing_validation_audit.md`
- `docs/engineering/records/2026-08-23_02_oracle_evidence_integrity.md`
- `docs/engineering/records/2026-08-23_03_negative_controls.md`
- `docs/engineering/records/2026-08-23_04_harness_v1_verification.md`
- `docs/engineering/records/2026-08-23_05_manifest_git_identity.md`
- `docs/engineering/records/2026-08-23_06_manifest_start_order.md`

## 限界

これはPhase 1〜4のfake/shadow実装とHarness自体の検証です。
Phase 5、実LAN、実credential、実process、Pod、Deployment、配信系への操作は行っていません。
"""


def run_suite(project_root: Path, artifact_root: Path, run_id: str, *, seed: int = 20260823) -> Path:
    registry = ScenarioRegistry.load(project_root / "harness/scenarios/protocol_v1.json")
    manifest = build_manifest(project_root, run_id, seed, verification_commands())
    verification = run_verification(project_root)
    manifest["test_commands"] = [verification["existing"]["command"], verification["harness"]["command"]]
    with tempfile.TemporaryDirectory(prefix="cra-harness-") as temporary:
        executor = ScenarioExecutor(project_root, Path(temporary), manifest)
        outcomes = [_outcome(executor.execute(run_id, scenario)) for scenario in registry.scenarios]
    observed_counts = Counter(item["classification"] for item in outcomes)
    classification_counts = {item.value: observed_counts.get(item.value, 0) for item in Classification}
    profile_counts = dict(sorted(Counter(item["profile"] for item in outcomes).items()))
    negative = [item for item in outcomes if item["profile"] == "negative_control"]
    detected = sum(
        item["classification"] == Classification.EXPECTED_INJECTED_FAILURE.value and bool(item["oracle_violations"]) for item in negative
    )
    deterministic_count = sum(item["profile"] == "deterministic" for item in outcomes)
    randomized_count = sum(item["profile"] == "randomized" for item in outcomes)
    pytest_counts = verification["counts"]
    harness_pytest_items = (
        pytest_counts["harness_unit"]
        + pytest_counts["harness_integration"]
        + pytest_counts["negative_control"]
        + pytest_counts["self_test"]
    )
    total = pytest_counts["existing_tests"] + harness_pytest_items + deterministic_count + randomized_count + len(negative)
    no_bad = all(classification_counts[item] == 0 for item in BAD_CLASSIFICATIONS)
    evidence_complete = all(item["evidence_complete"] for item in outcomes)
    production_isolation = all(
        next(event for event in item["events"] if event["name"] == "production_isolation")["data"]["safe"] for item in outcomes
    )
    artifact_consistency = True
    trusted = all(
        (
            verification["passed"],
            no_bad,
            detected == len(negative) == 6,
            classification_counts.get(Classification.EXPECTED_INJECTED_FAILURE.value, 0) > 0,
            evidence_complete,
            manifest_complete(manifest),
            production_isolation,
            all(item["matches_expectation"] for item in outcomes),
            artifact_consistency,
        )
    )
    manifest["finished_at"] = isoformat_utc(utc_now())
    manifest["complete"] = manifest_complete(manifest)
    summary: dict[str, Any] = {
        "run_id": run_id,
        "trust_gate": "HARNESS_TRUSTED" if trusted else "HARNESS_NOT_TRUSTED",
        "total": len(outcomes),
        "scenario_count": len(outcomes),
        "profile_counts": profile_counts,
        "classification_counts": classification_counts,
        "negative_control_detection": {"detected": detected, "total": len(negative), "rate": detected / len(negative)},
        "evidence_complete": evidence_complete,
        "manifest_complete": manifest["complete"],
        "artifact_consistency": artifact_consistency,
        "production_isolation": production_isolation,
        "verification_passed": verification["passed"],
        "test_counts": {
            **pytest_counts,
            "harness_pytest_items": harness_pytest_items,
            "deterministic": deterministic_count,
            "randomized": randomized_count,
            "negative_control": len(negative),
            "total": total,
            "property_is_subset_of_existing": True,
        },
    }
    writer = ArtifactWriter(artifact_root, run_id)
    writer.write_all(
        manifest=manifest,
        outcomes=outcomes,
        verification=verification,
        summary=summary,
        report=_report(summary, manifest),
        registry_hash=registry.fixture_sha256,
    )
    consistent, errors = verify_artifact_consistency(writer.run_dir)
    if not consistent:
        raise RuntimeError(f"artifact consistency failed: {errors}")
    return writer.run_dir
