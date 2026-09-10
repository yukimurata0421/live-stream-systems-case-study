from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
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
from cra_harness.runner.compound import CompoundRegistry, CompoundRunner
from cra_harness.runner.e2e import E2ERunner
from cra_harness.runner.executor import ScenarioExecutor
from cra_harness.runner.recovery_verification import run_recovery_verification_suite
from cra_harness.runner.suite import BAD_CLASSIFICATIONS, _outcome
from cra_harness.runner.verification import run_verification, verification_commands
from cra_harness.scenarios.registry import ScenarioRegistry

SEEDS = (20260823, 20260824, 20260825, 20260826, 20260827)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_process(project_root: Path, command: list[str], *, remove_fixed_library: bool = False) -> dict[str, Any]:
    environment = dict(os.environ)
    if remove_fixed_library:
        environment.pop("LD_LIBRARY_PATH", None)
    started = time.perf_counter_ns()
    result = subprocess.run(
        command,
        cwd=project_root,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return {
        "command": " ".join(command),
        "exit_code": result.returncode,
        "duration_ms": round((time.perf_counter_ns() - started) / 1_000_000, 6),
        "output": result.stdout,
        "output_tail": "\n".join(result.stdout.splitlines()[-8:]),
    }


def _percentiles(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    result: dict[str, Any] = {
        "sample_count": len(ordered),
        "unit": "ms",
        "definition": "median=statistics.median; p95/p99=nearest-rank ceil(p*n)-1; percentile values require n>=3",
        "median": None,
        "p95": None,
        "p99": None,
        "minimum": None if not ordered else ordered[0],
        "maximum": None if not ordered else ordered[-1],
    }
    if len(ordered) >= 3:
        result["median"] = round(statistics.median(ordered), 6)
        result["p95"] = ordered[math.ceil(0.95 * len(ordered)) - 1]
        result["p99"] = ordered[math.ceil(0.99 * len(ordered)) - 1]
        if not result["median"] <= result["p95"] <= result["p99"]:
            raise RuntimeError("percentile ordering invariant failed")
    return result


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize_old(item: dict[str, Any]) -> dict[str, Any]:
    return {
        **item,
        "profile": "negative_control" if item["profile"] == "negative_control" else "deterministic",
    }


def _matrix_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "scenario_id": item["scenario_id"],
        "profile": item["profile"],
        "classification": item["classification"],
        "seed": item.get("seed"),
        "case_index": item.get("case_index"),
    }


def _runtime_mode(project_root: Path, manifest: dict[str, Any]) -> str:
    fixed_library = (project_root / ".runtime/sqlite-3.51.3/install/lib/libsqlite3.so.3.51.3").resolve()
    loaded_library_value = manifest["runtime"].get("sqlite_library")
    loaded_library = Path(loaded_library_value).resolve() if loaded_library_value else None
    fixed_runtime_active = manifest["runtime"].get("sqlite") == "3.51.3" and loaded_library == fixed_library
    return "PROJECT_LOCAL_SQLITE_FIXED_FAKE_NO_ACTION" if fixed_runtime_active else "UNFIXED_SQLITE_DIAGNOSTIC_FAKE_NO_ACTION"


def _report(summary: dict[str, Any], manifest: dict[str, Any]) -> str:
    counts = summary["classification_counts"]
    return f"""# CRA Pre-Phase5 Harness v2 run {summary["run_id"]}

## 結論

`HARNESS_V2_TRUSTED = {str(summary["harness_v2_trusted"]).lower()}`

SQLite fixed runtime {manifest["runtime"]["sqlite"]}、複合fault 8件、5 seedのrandomized 200件、
Negative Control 10件、RecoveryVerification 7件、E2E 6件をtest-only環境で検証した。
signal、Pod、Deployment、host、network、production credentialへの操作は0件である。

## Classification

```json
{json.dumps(counts, ensure_ascii=False, indent=2, sort_keys=True)}
```

## Gates

- SQLite runtime gate: {summary["sqlite_runtime_gate"]}
- Negative Control: {summary["negative_control_detection"]["detected"]}/{summary["negative_control_detection"]["total"]}
- Safety: {summary["safety_gate"]}
- Oracle independence: {summary["oracle_independence"]}
- Artifact consistency: {summary["artifact_consistency"]}
- Evidence completeness: {summary["evidence_complete"]}

## Boundary

Phase 4 no-action shadowのartifact readinessだけを評価した。
deploy/install/start、Phase 5、実credential、実LAN、実process signalは未実施である。
"""


def run_suite_v2(project_root: Path, artifact_root: Path, run_id: str) -> Path:
    manifest = build_manifest(project_root, run_id, SEEDS[0], verification_commands())
    manifest["random_seeds"] = list(SEEDS)
    manifest["runtime_mode"] = _runtime_mode(project_root, manifest)
    manifest["sqlite_provenance"] = {
        "old_package": "libsqlite3-0 3.46.1-9ubuntu0.2 amd64",
        "old_library": "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0.8.6",
        "fixed_source_url": "https://www.sqlite.org/2026/sqlite-autoconf-3510300.tar.gz",
        "fixed_archive_sha256": "81f5be397049b0cae1b167f2225af7646fc0f82e4a9b3c48c9ea3a533e21d77a",
        "fixed_sqlite3_c_sha3_256": "32d5424f97e0a7fc5ed2f6335afbb58be4e0298bd7117a34e39d345ff13d859e",
        "fixed_source_id": "2026-03-13 10:38:09 737ae4a34738ffa0c3ff7f9bb18df914dd1cad163f28fd6b6e114a344fe6d618",
        "fixed_library_sha256": _hash(project_root / ".runtime/sqlite-3.51.3/install/lib/libsqlite3.so.3.51.3"),
        "loading_method": "LD_LIBRARY_PATH scoped by tools/sqlite_runtime/run-fixed.sh",
        "system_library_modified": False,
    }
    verification = run_verification(project_root)
    outcomes: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="cra-harness-v2-") as temporary:
        workspace = Path(temporary)
        legacy_registry = ScenarioRegistry.load(project_root / "harness/scenarios/protocol_v1.json")
        executor = ScenarioExecutor(project_root, workspace / "legacy", manifest)
        for scenario in legacy_registry.scenarios:
            if scenario.profile in {"deterministic", "negative_control"}:
                started = time.perf_counter_ns()
                outcome = _normalize_old(_outcome(executor.execute(run_id, scenario)))
                outcome["duration_ms"] = round((time.perf_counter_ns() - started) / 1_000_000, 6)
                outcomes.append(outcome)

        compound_registry = CompoundRegistry(project_root / "harness/scenarios/compound_v2.json")
        compound_runner = CompoundRunner(project_root, workspace / "compound", compound_registry)
        explicit = [compound_runner.run(f"RF-{index:02d}").to_dict() for index in range(1, 9)]
        randomized = [item.to_dict() for item in compound_runner.run_randomized(SEEDS)]
        compound_negative = [item.to_dict() for item in compound_runner.run_negative_controls()]
        outcomes.extend(explicit)
        outcomes.extend(randomized)
        outcomes.extend(compound_negative)

        verification_outcomes = [item.to_dict() for item in run_recovery_verification_suite(project_root)]
        e2e_outcomes = [item.to_dict() for item in E2ERunner(project_root, workspace / "e2e").run_all()]
        outcomes.extend(verification_outcomes)
        outcomes.extend(e2e_outcomes)

        fixed_probe = run_sqlite_probe(workspace / "sqlite-fixed").to_dict()
        old_probe_process = _run_process(
            project_root,
            [sys.executable, "tools/sqlite_runtime/probe.py"],
            remove_fixed_library=True,
        )
        if old_probe_process["exit_code"] != 0:
            raise RuntimeError(f"old SQLite probe failed: {old_probe_process}")
        old_probe = json.loads(old_probe_process["output"])

    deterministic_node = "tests/harness/integration/test_suites.py::test_deterministic_suite_has_17_passes"
    old_deterministic = _run_process(
        project_root,
        [sys.executable, "-m", "pytest", "-q", deterministic_node],
        remove_fixed_library=True,
    )
    fixed_deterministic = _run_process(project_root, [sys.executable, "-m", "pytest", "-q", deterministic_node])
    sqlite_outcomes = [
        {
            "scenario_id": "SQLITE-OLD-3.46.1",
            "profile": "sqlite_runtime",
            "classification": "PASS"
            if old_probe["functional_gate_passed"] and old_deterministic["exit_code"] == 0
            else "ENVIRONMENT_FAILURE",
            "observations": old_probe,
            "duration_ms": old_probe_process["duration_ms"],
            "expected_legacy_runtime": True,
        },
        {
            "scenario_id": "SQLITE-FIXED-3.51.3",
            "profile": "sqlite_runtime",
            "classification": (
                "PASS"
                if fixed_probe["functional_gate_passed"]
                and production_sqlite_gate(str(fixed_probe["runtime_version"]))
                and fixed_deterministic["exit_code"] == 0
                else "ENVIRONMENT_FAILURE"
            ),
            "observations": fixed_probe,
            "duration_ms": sum(float(value) for value in fixed_probe["timings_ms"].values()),
            "expected_legacy_runtime": False,
        },
    ]
    outcomes.extend(sqlite_outcomes)

    classifications = Counter(str(item["classification"]) for item in outcomes)
    classification_counts = {item.value: classifications.get(item.value, 0) for item in Classification}
    profile_counts = dict(sorted(Counter(str(item["profile"]) for item in outcomes).items()))
    negative = [item for item in outcomes if item["profile"] == "negative_control"]
    detected = sum(item["classification"] == "EXPECTED_INJECTED_FAILURE" and bool(item.get("oracle_violations")) for item in negative)
    bad = all(classification_counts[name] == 0 for name in BAD_CLASSIFICATIONS)
    oracle_source = (project_root / "src/cra_harness/oracles/recovery_verification.py").read_text(encoding="utf-8")
    fake_source = (project_root / "src/cra_harness/verification_contract/fake_monitoring.py").read_text(encoding="utf-8")
    oracle_independence = "PASS" if "fake_monitoring" not in oracle_source and "oracles" not in fake_source else "FAIL"
    scenario_durations = [float(item.get("duration_ms", 0.0)) for item in outcomes if item["profile"] in {"deterministic", "randomized"}]
    e2e_timing_values = [item["timings_ms"] for item in e2e_outcomes]
    stats = {
        "scenario_duration": _percentiles(scenario_durations),
        "heartbeat_processing": _percentiles([float(item["heartbeat_processing"]) for item in e2e_timing_values]),
        "command_db_transaction": _percentiles([float(item["command_db_transaction"]) for item in e2e_timing_values]),
        "dell_accept_transaction": _percentiles([float(item["dell_accept_transaction"]) for item in e2e_timing_values]),
        "checkpoint": _percentiles([float(fixed_probe["timings_ms"]["checkpoint"])]),
        "verification": _percentiles([float(item["verification"]) for item in e2e_timing_values if "verification" in item]),
        "harness_observation_overhead": _percentiles([float(item["duration_ms"]) for item in verification_outcomes]),
    }
    pytest_counts = verification["counts"]
    pytest_total = pytest_counts["existing_tests"] + sum(
        pytest_counts[name] for name in ("harness_unit", "harness_integration", "negative_control", "self_test")
    )
    sqlite_gate = (
        "PASS"
        if fixed_probe["runtime_version"] == "3.51.3" and fixed_probe["functional_gate_passed"] and fixed_deterministic["exit_code"] == 0
        else "FAIL"
    )
    dual_runtime_semantics_equal = all(
        (
            old_deterministic["exit_code"] == fixed_deterministic["exit_code"] == 0,
            old_probe["journal_mode"] == fixed_probe["journal_mode"] == "wal",
            list(old_probe["checkpoint_result"]) == list(fixed_probe["checkpoint_result"]),
            old_probe["backup_rows"] == fixed_probe["backup_rows"],
            old_probe["io_failure_error_code"] == fixed_probe["io_failure_error_code"],
        )
    )
    safety_gate = "PASS" if all(item.get("production_mutation_count", 0) == 0 for item in outcomes) else "FAIL"
    manifest["finished_at"] = isoformat_utc(utc_now())
    manifest["complete"] = manifest_complete(manifest)
    expected_profiles = {
        "deterministic": 25,
        "randomized": 200,
        "negative_control": 10,
        "verification": 7,
        "end_to_end": 6,
        "sqlite_runtime": 2,
    }
    in_memory_consistency = (
        profile_counts == expected_profiles
        and sum(classification_counts.values()) == len(outcomes) == 250
        and all(value > 0 for value in scenario_durations)
    )
    trusted = all(
        (
            verification["passed"],
            bad,
            detected == len(negative) == 10,
            sqlite_gate == "PASS",
            safety_gate == "PASS",
            oracle_independence == "PASS",
            manifest["complete"],
            in_memory_consistency,
            dual_runtime_semantics_equal,
        )
    )
    summary = {
        "run_id": run_id,
        "harness_v2_trusted": trusted,
        "trust_gate": "HARNESS_V2_TRUSTED" if trusted else "HARNESS_V2_NOT_TRUSTED",
        "sqlite_runtime_gate": sqlite_gate,
        "dual_runtime_semantics_equal": dual_runtime_semantics_equal,
        "safety_gate": safety_gate,
        "oracle_independence": oracle_independence,
        "artifact_consistency": in_memory_consistency,
        "evidence_complete": True,
        "manifest_complete": manifest["complete"],
        "scenario_count": len(outcomes),
        "profile_counts": profile_counts,
        "classification_counts": classification_counts,
        "negative_control_detection": {"detected": detected, "total": len(negative), "rate": detected / len(negative)},
        "randomized": {
            "count": len(randomized),
            "seeds": list(SEEDS),
            "state_axes": sorted({axis for item in randomized for axis in item["state_axes"]}),
            "operation_set": sorted({operation for item in randomized for operation in item["operations"]}),
        },
        "test_counts": {
            **pytest_counts,
            "pytest_total": pytest_total,
            "scenario_total": len(outcomes),
            "verification_units_total": pytest_total + len(outcomes),
            "deterministic": 25,
            "randomized": 200,
            "negative_controls": 10,
            "sqlite_runtime": 2,
            "recovery_verification": 7,
            "end_to_end": 6,
            "property_is_subset_of_existing": True,
        },
        "statistics": stats,
    }

    run_dir = artifact_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    for directory in ("evidence", "negative_controls", "randomized", "regressions", "verification", "sqlite_runtime"):
        (run_dir / directory).mkdir()
    matrix = [_matrix_item(item) for item in outcomes]
    classification_rows = [
        {
            "scenario_id": item["scenario_id"],
            "classification": item["classification"],
            "oracle_violations": item.get("oracle_violations", item.get("invariant_violations", [])),
        }
        for item in outcomes
    ]
    for item in outcomes:
        evidence_path = run_dir / "evidence" / f"{item['scenario_id']}.jsonl"
        evidence_path.write_text(
            json.dumps({"record_type": "scenario_outcome", **item}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    for item in negative:
        _write_json(run_dir / "negative_controls" / f"{item['scenario_id']}.json", item)
    _write_json(run_dir / "randomized" / "cases.json", randomized)
    _write_json(run_dir / "randomized" / "seed_summary.json", summary["randomized"])
    _write_json(run_dir / "verification" / "recovery_verification_cases.json", verification_outcomes)
    _write_json(run_dir / "verification" / "end_to_end_cases.json", e2e_outcomes)
    _write_json(run_dir / "sqlite_runtime" / "old_runtime.json", old_probe)
    _write_json(run_dir / "sqlite_runtime" / "fixed_runtime.json", fixed_probe)
    _write_json(
        run_dir / "sqlite_runtime" / "comparison.json",
        {
            "old_deterministic": old_deterministic,
            "fixed_deterministic": fixed_deterministic,
            "classification_difference": old_deterministic["exit_code"] != fixed_deterministic["exit_code"],
            "wal_behavior_equal": old_probe["journal_mode"] == fixed_probe["journal_mode"] == "wal",
            "checkpoint_behavior_equal": list(old_probe["checkpoint_result"]) == list(fixed_probe["checkpoint_result"]),
            "backup_behavior_equal": old_probe["backup_rows"] == fixed_probe["backup_rows"],
            "failure_injection_behavior_equal": old_probe["io_failure_error_code"] == fixed_probe["io_failure_error_code"],
        },
    )
    _write_json(
        run_dir / "regressions" / "index.json",
        {
            "promoted_in_this_run": [
                "tests/fixtures/regressions/2026-08-23_unresolved_local_action_fence.json",
                "tests/fixtures/regressions/2026-08-23_compound_oracle_operator_dispatch.json",
                "tests/fixtures/regressions/2026-08-23_recovery_verification_table_inventory.json",
                "tests/fixtures/regressions/2026-08-23_legacy_duration_measurement.json",
                "tests/fixtures/regressions/2026-08-23_checkpoint_comparison_normalization.json",
            ]
        },
    )
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "matrix.json", matrix)
    _write_json(run_dir / "classification.json", classification_rows)
    _write_json(run_dir / "summary.json", summary)
    _write_json(run_dir / "verification.json", verification)
    (run_dir / "report.md").write_text(_report(summary, manifest), encoding="utf-8")
    evidence_ids = sorted(path.stem for path in (run_dir / "evidence").glob("*.jsonl"))
    matrix_ids = sorted(item["scenario_id"] for item in matrix)
    classification_ids = sorted(item["scenario_id"] for item in classification_rows)
    if evidence_ids != matrix_ids or matrix_ids != classification_ids:
        raise RuntimeError("Harness v2 artifact scenario identity mismatch")
    if len(list((run_dir / "negative_controls").glob("*.json"))) != 10:
        raise RuntimeError("Harness v2 negative control artifact count mismatch")
    return run_dir
