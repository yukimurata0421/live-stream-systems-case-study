from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = "cra.candidate_full_validation.v1"
COVERAGE_MODULES = (
    "cra_dell_recovery.json_input",
    "cra_dell_recovery.owner_diagnostics",
    "cra_dell_recovery.bounded_http",
    "cra_no_action_soak.recovery_facts",
    "cra_no_action_soak.recovery_transport",
    "cra_no_action_soak.operator_status",
    "cra_no_action_soak.recovery_soak",
    "cra_no_action_soak.resilient_soak",
    "cra_no_action_soak.recovery_observer",
    "cra_no_action_soak.recovery_live",
    "cra_no_action_soak.recovery_window",
    "runtime_boundary.recovery_publisher",
    "runtime_boundary.recovery_evidence",
)
COVERAGE_FILES = tuple("src/" + module.replace(".", "/") + ".py" for module in COVERAGE_MODULES)


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_bytes(root: Path, *arguments: str) -> bytes:
    return subprocess.run(["git", *arguments], cwd=root, check=True, capture_output=True).stdout


def _junit_counts(path: Path) -> dict[str, int]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    return {name: sum(int(suite.attrib.get(name, "0")) for suite in suites) for name in ("tests", "failures", "errors", "skipped")}


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def classify_test_result(
    returncode: int | None,
    counts: dict[str, int],
    *,
    source_stable: bool = True,
    artifacts_complete: bool = True,
    timed_out: bool = False,
) -> str:
    """A failed assertion is not proof that the harness itself is broken."""
    if not source_stable:
        return "SOURCE_DRIFT"
    if timed_out:
        return "TIMEOUT"
    if not artifacts_complete or returncode not in (0, 1) or counts["tests"] <= 0 or counts["errors"]:
        return "HARNESS_FAILURE"
    if returncode == 1 or counts["failures"]:
        return "TEST_FAILURE"
    if counts["skipped"]:
        return "INCOMPLETE_TEST_SELECTION"
    return "PASS"


def run_validation(project_root: Path, output: Path, *, timeout_seconds: float = 1200.0) -> dict[str, Any]:
    project_root, output = project_root.resolve(), output.resolve()
    if output.exists():
        raise FileExistsError("CANDIDATE_VALIDATION_OUTPUT_EXISTS")
    if not all((project_root / relative).is_file() for relative in ("tools/sqlite_runtime/run-fixed.sh", "tools/run_full_regression.sh")):
        raise ValueError("CANDIDATE_VALIDATION_PROJECT_ROOT_INVALID")
    output.parent.mkdir(parents=True, exist_ok=True)
    status_before = _git_bytes(project_root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    head = _git_bytes(project_root, "rev-parse", "HEAD").decode().strip()
    tracked = sorted(
        {item.decode() for item in _git_bytes(project_root, "ls-files", "-co", "--exclude-standard", "-z").split(b"\0") if item}
    )
    source_hashes_before = {
        relative: _sha256(project_root / relative)
        for relative in tracked
        if (project_root / relative).is_file() and not (project_root / relative).is_symlink()
    }
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    started_at = _timestamp()
    try:
        junit = temporary / "full.xml"
        coverage = temporary / "coverage.json"
        command = [
            str(project_root / "tools/run_full_regression.sh"),
            "-q",
            f"--junitxml={junit}",
            "--cov-branch",
            *[f"--cov={module}" for module in COVERAGE_MODULES],
            f"--cov-report=json:{coverage}",
            "--cov-report=term",
        ]
        # Each run owns its instrumentation, including subprocess coverage. Keep
        # transient parallel data out of the source tree observed by other runs.
        environment = {
            name: value for name, value in os.environ.items() if not name.startswith("COV_CORE_") and name != "COVERAGE_PROCESS_START"
        }
        environment["COVERAGE_FILE"] = str(temporary / ".coverage")
        try:
            run = subprocess.run(command, cwd=project_root, env=environment, capture_output=True, text=True, timeout=timeout_seconds)
            timed_out = False
        except subprocess.TimeoutExpired as error:
            run = None
            timed_out = True
            (temporary / "timeout.txt").write_text(str(error) + "\n", encoding="utf-8")
        status_after = _git_bytes(project_root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        source_hashes_after = {relative: _sha256(project_root / relative) for relative in source_hashes_before}
        source_stable = status_after == status_before and source_hashes_after == source_hashes_before
        counts = _junit_counts(junit) if junit.is_file() else {name: 0 for name in ("tests", "failures", "errors", "skipped")}
        coverage_value = json.loads(coverage.read_bytes()) if coverage.is_file() else {}
        coverage_files = coverage_value.get("files", {}) if isinstance(coverage_value, dict) else {}
        missing_coverage_files = sorted(set(COVERAGE_FILES) - set(coverage_files))
        selected_summaries = [coverage_files[path].get("summary", {}) for path in COVERAGE_FILES if path in coverage_files]
        totals: dict[str, int | float] = {
            name: sum(int(summary.get(name, 0)) for summary in selected_summaries)
            for name in (
                "covered_lines",
                "num_statements",
                "missing_lines",
                "excluded_lines",
                "covered_branches",
                "num_branches",
                "missing_branches",
                "num_partial_branches",
            )
        }
        denominator = totals["num_statements"] + totals["num_branches"]
        totals["percent_covered"] = (totals["covered_lines"] + totals["covered_branches"]) / denominator * 100 if denominator else 100.0
        totals["percent_statements_covered"] = (
            totals["covered_lines"] / totals["num_statements"] * 100 if totals["num_statements"] else 100.0
        )
        totals["percent_branches_covered"] = totals["covered_branches"] / totals["num_branches"] * 100 if totals["num_branches"] else 100.0
        branch_coverage_measured = (
            coverage_value.get("meta", {}).get("branch_coverage") is True
            and isinstance(totals.get("num_branches"), int)
            and totals["num_branches"] > 0
            and not missing_coverage_files
        )
        classification = classify_test_result(
            None if run is None else run.returncode,
            counts,
            source_stable=source_stable,
            artifacts_complete=junit.is_file() and branch_coverage_measured,
            timed_out=timed_out,
        )
        trusted = classification == "PASS"
        summary = {
            "schema": SCHEMA,
            "classification": classification,
            "trusted": trusted,
            "deployable_clean_identity": status_before == b"",
            "git_head": head,
            "git_status_porcelain_sha256": hashlib.sha256(status_before).hexdigest(),
            "source_stable": source_stable,
            "started_at": started_at,
            "finished_at": _timestamp(),
            "timed_out": timed_out,
            "returncode": None if run is None else run.returncode,
            "junit_counts": counts,
            "branch_coverage_measured": branch_coverage_measured,
            "coverage_files": list(COVERAGE_FILES),
            "missing_coverage_files": missing_coverage_files,
            "coverage_totals": totals,
            "production_mutation": False,
            "physical_effect_count": 0,
            "claim_boundary": [
                "full repository regression with branch measurement for declared production modules",
                "coverage percentage is not a reliability guarantee",
                "no production fault injection or deployment",
            ],
        }
        if run is not None:
            (temporary / "stdout.txt").write_text(run.stdout, encoding="utf-8")
            (temporary / "stderr.txt").write_text(run.stderr, encoding="utf-8")
        _write_json(temporary / "summary.json", summary)
        _write_json(temporary / "source-before.json", source_hashes_before)
        artifacts = {
            path.name: _sha256(path) for path in sorted(temporary.iterdir()) if path.is_file() and path.name != "artifact_hashes.json"
        }
        _write_json(temporary / "artifact_hashes.json", artifacts)
        os.replace(temporary, output)
        return summary
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full candidate regression and declared-module branch measurement")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args()
    result = run_validation(args.project_root, args.output, timeout_seconds=args.timeout_seconds)
    print(json.dumps(result, sort_keys=True))
    if not result["trusted"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
