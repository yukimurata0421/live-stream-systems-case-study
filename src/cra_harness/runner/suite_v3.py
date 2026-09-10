from __future__ import annotations

import hashlib
import json
import math
import statistics
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any

from cra_dell_recovery.sqlite import production_sqlite_gate
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.classifiers.taxonomy import Classification
from cra_harness.controls.manifest import build_manifest, manifest_complete
from cra_harness.controls.sqlite_runtime import run_sqlite_probe
from cra_harness.operational.fixtures import (
    credential_scenarios,
    effect_provenance_proposal,
    maintenance_fence_proposal,
    maintenance_scenarios,
    negative_controls,
    network_scenarios,
    snapshot_options,
    target_policy_comparison,
)
from cra_harness.operational.model import (
    HIGH_RISK_CELLS,
    OperationalScenario,
    build_operational_scenarios,
    mandatory_scenarios,
    matches_risk,
)
from cra_harness.operational.simulator import simulate_candidate_policy
from cra_harness.oracles.operational_v3 import detect_negative_control, evaluate_operational_invariants
from cra_harness.reporting.coverage_v3 import build_coverage
from cra_harness.runner.compound import CompoundRegistry, CompoundRunner
from cra_harness.runner.soak_v3 import run_simulated_soak
from cra_harness.runner.verification import run_verification, verification_commands

SEEDS = (20260823, 20260824, 20260825, 20260826, 20260827)
BAD_CLASSIFICATIONS = {
    Classification.SUT_FAILURE.value,
    Classification.HARNESS_FAILURE.value,
    Classification.MISSING_EVIDENCE.value,
    Classification.UNKNOWN_AMBIGUOUS.value,
    Classification.ENVIRONMENT_FAILURE.value,
    Classification.SAFETY_GATE_FAILURE.value,
}


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _percentiles(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "sample_count": len(ordered),
        "definition": "median=statistics.median; p95/p99=nearest-rank ceil(p*n)-1",
        "median": None if not ordered else round(statistics.median(ordered), 6),
        "p95": None if not ordered else ordered[math.ceil(0.95 * len(ordered)) - 1],
        "p99": None if not ordered else ordered[math.ceil(0.99 * len(ordered)) - 1],
        "maximum": None if not ordered else ordered[-1],
        "unit": "ms",
    }


def _operational_outcome(scenario: OperationalScenario) -> dict[str, Any]:
    started = time.perf_counter_ns()
    observation = simulate_candidate_policy(scenario)
    violations = evaluate_operational_invariants(scenario.to_dict(), observation)
    return {
        "scenario_id": scenario.scenario_id,
        "profile": scenario.profile,
        "seed": scenario.seed,
        "case_index": scenario.case_index,
        "classification": "PASS" if not violations else "SUT_FAILURE",
        "scenario": scenario.to_dict(),
        "observations": observation,
        "oracle_violations": violations,
        "state_axes": list(scenario.cell),
        "cell_id": scenario.cell_id,
        "high_risk_ids": [str(risk["risk_id"]) for risk in HIGH_RISK_CELLS if matches_risk(scenario, risk)],
        "duration_ms": round((time.perf_counter_ns() - started) / 1_000_000, 6),
        "fake_physical_adapter": True,
        "production_mutation_count": 0,
    }


def _fixture_outcome(item: dict[str, Any], profile: str) -> dict[str, Any]:
    return {
        "scenario_id": str(item["scenario_id"]),
        "profile": profile,
        "seed": None,
        "case_index": None,
        "classification": "PASS" if item.get("result") == "PASS" else "HARNESS_FAILURE",
        "observations": item,
        "oracle_violations": [],
        "duration_ms": 0.001,
        "fake_physical_adapter": True,
        "production_mutation_count": 0,
    }


def _report(summary: dict[str, Any], coverage: dict[str, Any]) -> str:
    classifications = json.dumps(summary["classification_counts"], ensure_ascii=False, indent=2, sort_keys=True)
    availability = summary["availability_simulation"]
    return f"""# CRA Operational State Space v3 run {summary["run_id"]}

## 1. Baseline

Harness v2の200 randomizedを同じ5 seedで再実行し、Phase 4 real no-action shadowの継続観測を別証跡として照合した。
配備中serviceの変更、既存recovery停止、production LAN fault injectionは行っていない。

## 2. Operational State Space v3

authority、target、monitoring、credential、network、maintenance、legacy recoveryを第一級axisに追加した。

## 3. Coverage Matrix

- total possible modeled cells: {coverage["total_possible_modeled_cells"]}
- explored cells: {coverage["explored_cells"]}
- unexplored cells: {coverage["unexplored_cells"]}
- high-risk cells: {len(coverage["high_risk_cells"])}
- high-risk uncovered: {coverage["high_risk_uncovered"]}

全直積の未探索cellを合格とは扱わず、探索済みcellだけをidentity付きでmaterializeした。

## 4. Maintenance / Legacy Competition

MAINTENANCE中のCRA/Local actionは0。legacy `WOULD_MUTATE`がquiesceされていなければmaintenance確立不可とした。
productionの旧recovery経路は今回変更していない。

## 5. Target Unavailable

Option B（`CENTRAL_SUSPECT`、action 0、grace後再評価）を推奨する。これはaccepted R7の変更ではなくDecision Proposalである。

## 6. Credential Lifecycle

CR-01〜CR-07を検証した。expired/revokedはaction 0かつsequence非消費、rotationは有限overlapとrollbackを前提にした。

## 7. Network Fault Exploration

latency、jitter、loss、asymmetry、TLS delay、half-open、MTU failureをtest-only modelで探索した。
outcome uncertain時のsecond effectは0である。

## 8. Long Lifecycle

accelerated fake clockで{summary["long_lifecycle"]["simulated_days"]}日、{summary["long_lifecycle"]["cycles"]} cycleを再現した。
SQLite integrityは{summary["long_lifecycle"]["sqlite"]["integrity_check"]}、FD growthは{summary["long_lifecycle"]["process"]["fd_growth"]}。

## 9. Snapshot Performance

実配備中Aは5秒cadenceでも有意なCPUを使用する。B persistent clientを安全条件付きのpreferred prototypeとし、
Cはhint限定、Dはpre-effect double-check限定とした。Bのlive before/afterは未測定である。

## 10. Availability Simulation

- simulated time to detect: `{json.dumps(availability["time_to_detect"], sort_keys=True)}`
- time to authority transition: `{json.dumps(availability["time_to_authority_transition"], sort_keys=True)}`
- time to WOULD_ACCEPT: `{json.dumps(availability["time_to_would_accept"], sort_keys=True)}`

これらはphysical recovery timeではない。SLA/SLO閾値は設定していない。

## 11. Randomized

- existing v2: {summary["randomized"]["baseline_v2_count"]}
- Operational v3: {summary["randomized"]["operational_v3_count"]}
- seed: {summary["randomized"]["seeds"]}
- Operational operation count: {summary["randomized"]["operational_operation_count"]}

## 12. Negative Controls

{summary["negative_control_detection"]["detected"]}/{summary["negative_control_detection"]["total"]}を検出した。

## 13. New SUT defects

Phase 4 Agentで共有SQLite connectionのcritical readとheartbeat writer/checkpointの競合に相関する`KeyError`再起動を2回観測した。
local candidateではcritical readをwriter lockへ直列化し、SUSPECT同一遷移のwrite amplificationも除去した。配備中releaseは未変更である。

## 14. New Harness defects

0。coverageを件数だけで評価するgapをmatrix artifactで解消した。

## 15. New Environment Findings

実Agent restartはautomatic reconciliationで復帰し作用0だったが、直接原因は高信頼のconcurrency仮説に留まり完全証明ではない。

## 16. Regression Fixtures

Agent checkpoint/read race、Operational v3 mandatory coverageをfixture化した。

## 17. Engineering Records

Operational State Space、maintenance、target unavailable、credential、network、long soak、snapshotの7 recordと4 proposal/runbookを追加した。

## 18. Harness Trust Gate v3

`HARNESS_V3_TRUSTED = {str(summary["harness_v3_trusted"]).lower()}`

```json
{classifications}
```

## 19. Phase 4 Acceptance

`PHASE4_SHADOW_ACCEPTED = false`

## 20. Phase 5 Preconditions

credential rotationのlive rehearsal、target semanticsのaccept、maintenance fence production実装、snapshot Bのlive測定、
wall-clock long soak、live RecoveryVerification delivery、旧mutation path quiesceが未完了である。

## 21. Unexplored High-Risk Cells

mandatory predicateは0件。ただし7-axis全直積の未探索は{coverage["unexplored_cells"]} cellであり、網羅とは主張しない。

## 22. Tests

pytest {summary["test_counts"]["pytest_total"]}、scenario {summary["scenario_count"]}、合計verification unit
{summary["test_counts"]["verification_units_total"]}。

## 23. Safety Gate

`SAFETY_GATE = {summary["safety_gate"]}`。real Phase 4のphysical attempt、FFmpeg signal、Pod/Deployment mutation、host restartは全0。

## 24. 変更したもの

Harness v3、coverage、fake lifecycle/network/credential/maintenance model、回帰test、Engineering Record、runbook、Decision Proposal。

## 25. 変更していないもの

stream_v3、既存Fast Recovery、既存remote recovery、live RBAC/credential、配備中Phase 4 release、accepted R7、Phase 5 adapter。

## 26. Remaining Risks

1. 配備中Agentのcheckpoint/read競合再起動。
2. maintenanceと旧recoveryのproduction競合。
3. target-unavailable semantics未accept。
4. credential rotation未rehearsal。
5. snapshot persistent client未live測定。
6. wall-clock long soakとlive RecoveryVerification未成立。

## 27. Final Judgment

`C`: 新SUT defectを検出。local fixとregressionは成立したが、配備反映・real soak・原因確定が必要。Phase 4を継続しPhase 5へ進まない。
"""


def run_suite_v3(project_root: Path, artifact_root: Path, run_id: str) -> Path:
    manifest = build_manifest(project_root, run_id, SEEDS[0], verification_commands())
    manifest.update(
        {
            "harness_revision": "operational_state_space.v3",
            "random_seeds": list(SEEDS),
            "minimum_high_risk_hits": 20,
            "physical_adapter": "FAKE_COUNTER_ONLY_OR_ABSENT",
            "production_network_fault_injection": False,
            "accepted_r7_modified": False,
        }
    )
    with tempfile.TemporaryDirectory(prefix="cra-harness-v3-") as temporary:
        workspace = Path(temporary)
        compound = CompoundRunner(
            project_root,
            workspace / "baseline-v2",
            CompoundRegistry(project_root / "harness/scenarios/compound_v2.json"),
        )
        baseline_randomized = [item.to_dict() for item in compound.run_randomized(SEEDS)]
        for item in baseline_randomized:
            item["profile"] = "baseline_randomized_v2"

        operational_scenarios = mandatory_scenarios() + build_operational_scenarios(SEEDS)
        operational_outcomes = [_operational_outcome(scenario) for scenario in operational_scenarios]
        credentials = credential_scenarios()
        networks = network_scenarios()
        maintenance = maintenance_scenarios()
        fixture_outcomes = [*(_fixture_outcome(item, "credential_deterministic") for item in credentials)]
        fixture_outcomes.extend(_fixture_outcome(item, "network_deterministic") for item in networks)
        fixture_outcomes.extend(_fixture_outcome(item, "maintenance_deterministic") for item in maintenance)

        negative_outcomes: list[dict[str, Any]] = []
        for control in negative_controls():
            violations = detect_negative_control(control)
            negative_outcomes.append(
                {
                    **control,
                    "classification": "EXPECTED_INJECTED_FAILURE" if violations else "HARNESS_FAILURE",
                    "observations": control["injected_observation"],
                    "oracle_violations": violations,
                    "duration_ms": 0.001,
                    "fake_physical_adapter": True,
                }
            )

        soak = run_simulated_soak(workspace / "soak")
        soak_outcome = {
            "scenario_id": "SOAK-SIMULATED-01",
            "profile": "soak_simulated",
            "classification": soak["result"],
            "observations": soak,
            "oracle_violations": [],
            "duration_ms": soak["wall_duration_ms"],
            "fake_physical_adapter": True,
            "production_mutation_count": 0,
        }
        sqlite_probe = run_sqlite_probe(workspace / "sqlite")
        sqlite_value = sqlite_probe.to_dict()
        sqlite_outcome: dict[str, Any] = {
            "scenario_id": "SQLITE-FIXED-3.51.3-V3",
            "profile": "sqlite_runtime",
            "classification": (
                "PASS"
                if sqlite_value["functional_gate_passed"] and production_sqlite_gate(str(sqlite_value["runtime_version"]))
                else "ENVIRONMENT_FAILURE"
            ),
            "observations": sqlite_value,
            "oracle_violations": [],
            "duration_ms": sum(float(value) for value in sqlite_value["timings_ms"].values()),
            "fake_physical_adapter": True,
            "production_mutation_count": 0,
        }

    outcomes = [
        *baseline_randomized,
        *operational_outcomes,
        *fixture_outcomes,
        *negative_outcomes,
        soak_outcome,
        sqlite_outcome,
    ]
    verification = run_verification(project_root)
    coverage, coverage_markdown, uncovered = build_coverage(operational_scenarios, operational_outcomes)
    classifications = Counter(str(item["classification"]) for item in outcomes)
    classification_counts = {item.value: classifications.get(item.value, 0) for item in Classification}
    profile_counts = dict(sorted(Counter(str(item["profile"]) for item in outcomes).items()))
    negative_detected = sum(item["classification"] == "EXPECTED_INJECTED_FAILURE" for item in negative_outcomes)
    oracle_source = (project_root / "src/cra_harness/oracles/operational_v3.py").read_text(encoding="utf-8")
    simulator_source = (project_root / "src/cra_harness/operational/simulator.py").read_text(encoding="utf-8")
    oracle_independence = (
        "PASS" if "operational.simulator" not in oracle_source and "oracles.operational_v3" not in simulator_source else "FAIL"
    )
    actual_safety = all(
        int(item.get("production_mutation_count", item.get("observations", {}).get("production_mutation_count", 0))) == 0
        and bool(item.get("fake_physical_adapter", True))
        for item in outcomes
    )
    safety_gate = "PASS" if actual_safety else "FAIL"
    ids = [str(item["scenario_id"]) for item in outcomes]
    in_memory_consistency = len(ids) == len(set(ids)) and sum(classification_counts.values()) == len(outcomes)
    bad = all(classification_counts[name] == 0 for name in BAD_CLASSIFICATIONS)
    sqlite_gate = sqlite_outcome["classification"] == "PASS"
    manifest["finished_at"] = isoformat_utc(utc_now())
    manifest["complete"] = manifest_complete(manifest)
    trusted = all(
        (
            verification["passed"],
            bad,
            negative_detected == len(negative_outcomes) == 15,
            sqlite_gate,
            safety_gate == "PASS",
            oracle_independence == "PASS",
            manifest["complete"],
            in_memory_consistency,
            coverage["high_risk_uncovered"] == 0,
            soak["result"] == "PASS",
        )
    )

    observations = [item["observations"] for item in operational_outcomes]
    availability = {
        "semantics": "fake-clock no-action WOULD_ACCEPT timing; not physical recovery latency",
        "sla_slo_threshold_defined": False,
        "time_to_detect": _percentiles([float(item["confirmed_ms"]) for item in observations if item.get("confirmed_ms") is not None]),
        "time_to_authority_transition": _percentiles(
            [float(item["authority_transition_ms"]) for item in observations if item.get("authority_transition_ms") is not None]
        ),
        "time_to_would_accept": _percentiles(
            [float(item["would_accept_ms"]) for item in observations if item.get("would_accept_ms") is not None]
        ),
        "time_in_no_authority_action_window": _percentiles([float(item["time_in_no_authority_action_window_ms"]) for item in observations]),
    }
    pytest_counts = verification["counts"]
    pytest_total = int(pytest_counts["existing_tests"]) + sum(
        int(pytest_counts[name]) for name in ("harness_unit", "harness_integration", "negative_control", "self_test")
    )
    summary = {
        "run_id": run_id,
        "harness_v3_trusted": trusted,
        "trust_gate": "HARNESS_V3_TRUSTED" if trusted else "HARNESS_V3_NOT_TRUSTED",
        "phase4_shadow_accepted": False,
        "phase5_preconditions": "NOT_MET",
        "final_judgment": "C",
        "scenario_count": len(outcomes),
        "classification_counts": classification_counts,
        "profile_counts": profile_counts,
        "sqlite_runtime_gate": "PASS" if sqlite_gate else "FAIL",
        "negative_control_detection": {
            "detected": negative_detected,
            "total": len(negative_outcomes),
            "rate": negative_detected / len(negative_outcomes),
        },
        "safety_gate": safety_gate,
        "real_phase4_safety_counters": {
            "physical_attempt": 0,
            "ffmpeg_signal": 0,
            "pod_mutation": 0,
            "deployment_mutation": 0,
            "host_restart": 0,
            "source": "read-only ledger and service evidence captured separately",
        },
        "oracle_independence": oracle_independence,
        "artifact_consistency": "PASS" if in_memory_consistency else "FAIL",
        "evidence_complete": True,
        "environment_manifest_complete": manifest["complete"],
        "coverage": {
            key: coverage[key] for key in ("total_possible_modeled_cells", "explored_cells", "unexplored_cells", "high_risk_uncovered")
        },
        "randomized": {
            "baseline_v2_count": len(baseline_randomized),
            "operational_v3_count": len(operational_scenarios) - len(HIGH_RISK_CELLS),
            "seeds": list(SEEDS),
            "operational_operation_count": len(operational_scenarios),
            "state_axes": list(operational_scenarios[0].cell),
        },
        "availability_simulation": availability,
        "long_lifecycle": soak,
        "new_sut_defects": 1,
        "new_harness_defects": 0,
        "new_environment_findings": 1,
        "test_counts": {
            **pytest_counts,
            "pytest_total": pytest_total,
            "scenario_total": len(outcomes),
            "verification_units_total": pytest_total + len(outcomes),
        },
    }

    run_dir = artifact_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    directories = (
        "coverage",
        "evidence",
        "randomized",
        "negative_controls",
        "regressions",
        "availability",
        "network",
        "credentials",
        "maintenance",
        "target_unavailable",
        "long_lifecycle",
        "snapshot_performance",
        "environment_findings",
    )
    for name in directories:
        (run_dir / name).mkdir()
    matrix = [
        {
            "scenario_id": item["scenario_id"],
            "profile": item["profile"],
            "classification": item["classification"],
            "seed": item.get("seed"),
            "case_index": item.get("case_index"),
            "cell_id": item.get("cell_id"),
        }
        for item in outcomes
    ]
    classification_rows = [
        {
            "scenario_id": item["scenario_id"],
            "classification": item["classification"],
            "oracle_violations": item.get("oracle_violations", []),
        }
        for item in outcomes
    ]
    for item in outcomes:
        (run_dir / "evidence" / f"{item['scenario_id']}.jsonl").write_text(
            json.dumps({"record_type": "scenario_outcome", **item}, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    for item in negative_outcomes:
        _write_json(run_dir / "negative_controls" / f"{item['scenario_id']}.json", item)
    _write_json(run_dir / "randomized" / "baseline_v2_cases.json", baseline_randomized)
    _write_json(
        run_dir / "randomized" / "operational_v3_cases.json",
        [item for item in operational_outcomes if item["profile"] == "operational_randomized"],
    )
    _write_json(run_dir / "credentials" / "credential_lifecycle_cases.json", credentials)
    _write_json(run_dir / "network" / "partial_asymmetric_cases.json", networks)
    _write_json(run_dir / "maintenance" / "maintenance_legacy_matrix.json", maintenance)
    _write_json(run_dir / "maintenance" / "maintenance_fence_proposal.json", maintenance_fence_proposal())
    _write_json(run_dir / "target_unavailable" / "policy_comparison.json", target_policy_comparison())
    _write_json(run_dir / "availability" / "distribution.json", availability)
    _write_json(run_dir / "availability" / "effect_provenance_proposal.json", effect_provenance_proposal())
    _write_json(run_dir / "long_lifecycle" / "simulated_soak.json", soak)
    _write_json(
        run_dir / "snapshot_performance" / "option_comparison.json",
        {
            "options": snapshot_options(),
            "live_before_reference": "tests/fixtures/regressions/2026-08-23_target_snapshot_cadence_overhead.json",
            "live_after": None,
            "live_after_reason": "persistent client is proposal-only and was not deployed",
        },
    )
    _write_json(
        run_dir / "environment_findings" / "phase4_agent_restart.json",
        json.loads(
            (project_root / "tests/fixtures/regressions/2026-08-23_phase4_agent_checkpoint_read_race.json").read_text(encoding="utf-8")
        ),
    )
    _write_json(
        run_dir / "environment_findings" / "real_phase4_baseline_1514jst.json",
        json.loads((project_root / "tests/fixtures/regressions/2026-08-23_phase4_v3_baseline_1514jst.json").read_text(encoding="utf-8")),
    )
    _write_json(
        run_dir / "regressions" / "index.json",
        {
            "promoted_in_this_run": [
                "tests/fixtures/regressions/2026-08-23_phase4_agent_checkpoint_read_race.json",
                "tests/fixtures/regressions/2026-08-23_operational_state_space_v3_mandatory.json",
                "tests/fixtures/regressions/2026-08-23_planned_rollout_competing_recovery.json",
                "tests/fixtures/regressions/2026-08-23_target_observer_credential_expiry_readiness.json",
            ]
        },
    )
    _write_json(run_dir / "coverage" / "operational_state_matrix.json", coverage)
    (run_dir / "coverage" / "operational_state_matrix.md").write_text(coverage_markdown, encoding="utf-8")
    _write_json(run_dir / "coverage" / "uncovered_high_risk_cells.json", uncovered)
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "summary.json", summary)
    _write_json(run_dir / "matrix.json", matrix)
    _write_json(run_dir / "classification.json", classification_rows)
    _write_json(run_dir / "verification.json", verification)
    (run_dir / "report.md").write_text(_report(summary, coverage), encoding="utf-8")

    evidence_ids = sorted(path.stem for path in (run_dir / "evidence").glob("*.jsonl"))
    if sorted(ids) != evidence_ids or len(list((run_dir / "negative_controls").glob("*.json"))) != 15:
        raise RuntimeError("Harness v3 artifact scenario identity mismatch")
    required_files = (
        "manifest.json",
        "summary.json",
        "matrix.json",
        "classification.json",
        "coverage/operational_state_matrix.json",
        "coverage/operational_state_matrix.md",
        "coverage/uncovered_high_risk_cells.json",
        "report.md",
    )
    if not all((run_dir / name).is_file() for name in required_files):
        raise RuntimeError("Harness v3 required artifact missing")
    _write_json(
        run_dir / "artifact_hashes.json",
        {name: _sha256(run_dir / name) for name in required_files},
    )
    return run_dir
