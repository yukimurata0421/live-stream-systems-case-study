from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.run_candidate_full_validation import classify_test_result

SCHEMA = "cra.postmortem_io_fault_catalog.v1"
SUMMARY_SCHEMA = "cra.postmortem_io_harness_summary.v1"
EXPECTED_IDS = tuple(f"PF-{index:02d}" for index in range(1, 7))
BOUND_INPUT_FILES = (
    "tools/run_postmortem_io_harness.py",
    "tools/run_candidate_full_validation.py",
    "tools/run_full_regression.sh",
    "tools/sqlite_runtime/run-fixed.sh",
    "tools/sqlite_runtime/check_fixed.py",
    "constraints/sqlite-runtime.json",
    "src/cra_harness/controls/sqlite_runtime.py",
    "src/cra_dell_recovery/owner_diagnostics.py",
    "src/cra_no_action_soak/recovery_observer.py",
    "src/cra_no_action_soak/recovery_live.py",
    "src/cra_no_action_soak/recovery_window.py",
    "src/cra_no_action_soak/operator_status.py",
    "src/cra_dell_recovery/recovery_history.py",
    "pyproject.toml",
)
SCENARIO_FIELDS = {
    "scenario_id",
    "fault_family",
    "sources",
    "hypothesis",
    "target_modules",
    "injection_points",
    "required_observations",
    "invariants",
    "regression_nodeids",
    "negative_control_nodeids",
    "unproven_scope",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _project_path(root: Path, relative: str) -> Path:
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        raise ValueError("POSTMORTEM_CATALOG_PATH_INVALID")
    path = (root / relative).resolve()
    if root.resolve() not in path.parents or path.is_symlink() or not path.is_file():
        raise ValueError(f"POSTMORTEM_CATALOG_FILE_INVALID:{relative}")
    return path


def load_catalog(path: Path, project_root: Path) -> dict[str, Any]:
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict) or set(value) != {"schema", "catalog_id", "execution_boundary", "scenarios"}:
        raise ValueError("POSTMORTEM_CATALOG_FIELDS_INVALID")
    if value["schema"] != SCHEMA or not isinstance(value["catalog_id"], str):
        raise ValueError("POSTMORTEM_CATALOG_IDENTITY_INVALID")
    boundary = value["execution_boundary"]
    if (
        not isinstance(boundary, dict)
        or boundary.get("production_credentials") is not False
        or boundary.get("production_mutation") is not False
    ):
        raise ValueError("POSTMORTEM_CATALOG_PRODUCTION_BOUNDARY_INVALID")
    scenarios = value["scenarios"]
    if not isinstance(scenarios, list) or tuple(item.get("scenario_id") for item in scenarios if isinstance(item, dict)) != EXPECTED_IDS:
        raise ValueError("POSTMORTEM_CATALOG_SCENARIOS_INCOMPLETE")
    nodeids: set[str] = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict) or set(scenario) != SCENARIO_FIELDS:
            raise ValueError("POSTMORTEM_CATALOG_SCENARIO_FIELDS_INVALID")
        for field in (
            "target_modules",
            "injection_points",
            "required_observations",
            "invariants",
            "regression_nodeids",
            "negative_control_nodeids",
            "unproven_scope",
        ):
            if (
                not isinstance(scenario[field], list)
                or not scenario[field]
                or not all(isinstance(item, str) and item for item in scenario[field])
            ):
                raise ValueError(f"POSTMORTEM_CATALOG_{field.upper()}_INVALID")
        if not isinstance(scenario["sources"], list) or not scenario["sources"]:
            raise ValueError("POSTMORTEM_CATALOG_SOURCES_INVALID")
        for source in scenario["sources"]:
            if not isinstance(source, dict) or set(source) != {"incident_date", "name", "url"} or not source["url"].startswith("https://"):
                raise ValueError("POSTMORTEM_CATALOG_SOURCE_INVALID")
        for relative in scenario["target_modules"]:
            _project_path(project_root, relative)
        for nodeid in scenario["regression_nodeids"] + scenario["negative_control_nodeids"]:
            relative, separator, test_name = nodeid.partition("::")
            if (
                not separator
                or not relative.startswith("tests/harness/")
                or not test_name.startswith("test_")
                or any(character in nodeid for character in "\n\r\0")
            ):
                raise ValueError("POSTMORTEM_CATALOG_NODEID_INVALID")
            _project_path(project_root, relative)
            if nodeid in nodeids:
                raise ValueError("POSTMORTEM_CATALOG_NODEID_DUPLICATE")
            nodeids.add(nodeid)
    return value


def _junit_counts(path: Path) -> dict[str, int]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    return {name: sum(int(suite.attrib.get(name, "0")) for suite in suites) for name in ("tests", "failures", "errors", "skipped")}


def _git(project_root: Path, *arguments: str) -> str:
    return subprocess.run(["git", *arguments], cwd=project_root, check=True, capture_output=True, text=True).stdout.strip()


def _git_bytes(project_root: Path, *arguments: str) -> bytes:
    return subprocess.run(["git", *arguments], cwd=project_root, check=True, capture_output=True).stdout


def run_catalog(catalog_path: Path, output: Path, project_root: Path, *, timeout_seconds: float = 120.0) -> dict[str, Any]:
    project_root = project_root.resolve()
    catalog_path = catalog_path.resolve()
    output = output.resolve()
    catalog = load_catalog(catalog_path, project_root)
    if output.exists():
        raise FileExistsError("POSTMORTEM_HARNESS_OUTPUT_EXISTS")
    status_before = _git_bytes(project_root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    output.parent.mkdir(parents=True, exist_ok=True)
    started_at = _timestamp()
    inputs = {str(catalog_path.relative_to(project_root)): _sha256(catalog_path)}
    for relative in BOUND_INPUT_FILES:
        inputs[relative] = _sha256(_project_path(project_root, relative))
    for scenario in catalog["scenarios"]:
        for relative in scenario["target_modules"]:
            inputs.setdefault(relative, _sha256(_project_path(project_root, relative)))
        for nodeid in scenario["regression_nodeids"] + scenario["negative_control_nodeids"]:
            relative = nodeid.partition("::")[0]
            inputs.setdefault(relative, _sha256(_project_path(project_root, relative)))
    events: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        shutil.copyfile(catalog_path, temporary / "catalog.json")
        for scenario in catalog["scenarios"]:
            scenario_id = str(scenario["scenario_id"])
            nodeids = scenario["regression_nodeids"] + scenario["negative_control_nodeids"]
            events.append({"event": "REQUESTED", "observed_at": _timestamp(), "scenario_id": scenario_id})
            collect = subprocess.run(
                [sys.executable, "-m", "pytest", "--collect-only", "-q", *nodeids],
                cwd=project_root,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
            collected = [line for line in collect.stdout.splitlines() if "::test_" in line]
            if collect.returncode != 0 or len(collected) < len(nodeids):
                results.append(
                    {
                        "scenario_id": scenario_id,
                        "classification": "HARNESS_FAILURE",
                        "collected_test_count": len(collected),
                        "returncode": collect.returncode,
                        "reason": "COLLECTION_INCOMPLETE",
                    }
                )
                events.append({"event": "COLLECTION_FAILED", "observed_at": _timestamp(), "scenario_id": scenario_id})
                continue
            events.append(
                {
                    "event": "ARMED",
                    "observed_at": _timestamp(),
                    "scenario_id": scenario_id,
                    "collected_test_count": len(collected),
                }
            )
            junit = temporary / f"{scenario_id.lower()}.xml"
            try:
                run = subprocess.run(
                    [sys.executable, "-m", "pytest", "-q", f"--junitxml={junit}", *nodeids],
                    cwd=project_root,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                )
                timed_out = False
            except subprocess.TimeoutExpired as error:
                run = None
                timed_out = True
                (temporary / f"{scenario_id.lower()}.timeout.txt").write_text(str(error) + "\n", encoding="utf-8")
            result: dict[str, Any]
            if timed_out or run is None or not junit.is_file():
                result = {
                    "scenario_id": scenario_id,
                    "classification": "TIMEOUT" if timed_out else "HARNESS_FAILURE",
                    "collected_test_count": len(collected),
                    "returncode": None if run is None else run.returncode,
                    "reason": "TEST_TIMEOUT_OR_JUNIT_MISSING",
                }
            else:
                counts = _junit_counts(junit)
                classification = classify_test_result(run.returncode, counts, artifacts_complete=counts["tests"] >= len(collected))
                passed = classification == "PASS"
                result = {
                    "scenario_id": scenario_id,
                    "classification": classification,
                    "collected_test_count": len(collected),
                    "returncode": run.returncode,
                    "junit": junit.name,
                    "junit_counts": counts,
                    "fault_reachability_asserted_by_tests": passed,
                    "negative_control_detected": passed,
                }
                (temporary / f"{scenario_id.lower()}.stdout.txt").write_text(run.stdout, encoding="utf-8")
                (temporary / f"{scenario_id.lower()}.stderr.txt").write_text(run.stderr, encoding="utf-8")
            results.append(result)
            events.append(
                {
                    "event": "COMPLETED",
                    "observed_at": _timestamp(),
                    "scenario_id": scenario_id,
                    "classification": result["classification"],
                }
            )
        ending_inputs = {relative: _sha256(_project_path(project_root, relative)) for relative in inputs}
        status_after = _git_bytes(project_root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        source_stable = ending_inputs == inputs and status_after == status_before
        trusted = source_stable and len(results) == len(EXPECTED_IDS) and all(result["classification"] == "PASS" for result in results)
        failures = {result["classification"] for result in results} - {"PASS"}
        classification = (
            "SOURCE_DRIFT"
            if not source_stable
            else next(
                (kind for kind in ("TIMEOUT", "HARNESS_FAILURE", "TEST_FAILURE", "INCOMPLETE_TEST_SELECTION") if kind in failures),
                "PASS" if trusted else "HARNESS_FAILURE",
            )
        )
        summary = {
            "schema": SUMMARY_SCHEMA,
            "catalog_id": catalog["catalog_id"],
            "classification": classification,
            "trusted": trusted,
            "deployable_clean_identity": status_before == b"",
            "production_mutation": False,
            "physical_effect_count": 0,
            "started_at": started_at,
            "finished_at": _timestamp(),
            "source_stable": source_stable,
            "scenario_results": results,
            "claim_boundary": [
                "local temporary files, loopback mTLS, and pytest-owned child processes only",
                "not physical LAN, power-loss, host-restore, or production action evidence",
                "not a seven-day soak verdict",
            ],
        }
        _json(temporary / "summary.json", summary)
        with (temporary / "events.jsonl").open("w", encoding="utf-8") as stream:
            for event in events:
                stream.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
        _json(
            temporary / "manifest.json",
            {
                "schema": "cra.postmortem_io_harness_manifest.v1",
                "catalog_sha256": _sha256(catalog_path),
                "git_head": _git(project_root, "rev-parse", "HEAD"),
                "git_status_porcelain_sha256": hashlib.sha256(status_before).hexdigest(),
                "input_sha256": inputs,
                "python": sys.version,
            },
        )
        artifacts = {
            path.name: _sha256(path) for path in sorted(temporary.iterdir()) if path.is_file() and path.name != "artifact_hashes.json"
        }
        _json(temporary / "artifact_hashes.json", artifacts)
        os.replace(temporary, output)
        return summary
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the isolated postmortem-derived PF-01..06 harness")
    parser.add_argument("--catalog", type=Path, default=Path("harness/scenarios/postmortem_io_v1.json"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    catalog = load_catalog(args.catalog, args.project_root)
    if args.validate_only:
        print(json.dumps({"catalog_id": catalog["catalog_id"], "scenario_count": len(catalog["scenarios"]), "valid": True}))
        return
    if args.output is None:
        parser.error("--output is required unless --validate-only is used")
    summary = run_catalog(args.catalog, args.output, args.project_root, timeout_seconds=args.timeout_seconds)
    print(json.dumps(summary, sort_keys=True))
    if not summary["trusted"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
