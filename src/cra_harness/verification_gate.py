"""Provider-independent acceptance gate for source-bound owner verification."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cra_harness.traceability import check_traceability

GATE_SCHEMA = "cra.owner_verification_gate.v1"
EXIT_PASS = 0
EXIT_VERIFICATION_FAILURE = 1
EXIT_HARNESS_ERROR = 2


def _environment_status(report: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = {}
    for case in report.get("cases", []):
        for environment, status in case.get("environment_coverage", {}).items():
            counts.setdefault(str(environment), Counter())[str(status)] += 1
    return {environment: dict(sorted(statuses.items())) for environment, statuses in sorted(counts.items())}


def _result(
    *,
    result_directory: Path | None,
    execution_status: str,
    verification_status: str,
    failure_owner: str,
    exit_code: int,
    traceability: Mapping[str, Any] | None,
    issues: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "schema": GATE_SCHEMA,
        "status": "PASS" if exit_code == EXIT_PASS else "FAIL",
        "execution_status": execution_status,
        "verification_status": verification_status,
        "failure_owner": failure_owner,
        "exit_code": exit_code,
        "result_directory": None if result_directory is None else str(result_directory),
        "traceability_status": "NOT_EXECUTED" if traceability is None else str(traceability.get("status", "FAIL")),
        "environment_status": {} if traceability is None else _environment_status(traceability),
        "issues": issues,
        "traceability": traceability,
    }


def evaluate_owner_gate(root: Path, *, result_directory: Path | None) -> dict[str, Any]:
    """Evaluate one immutable owner artifact without depending on a CI provider."""

    if result_directory is None or not result_directory.exists():
        issue = {
            "severity": "ERROR",
            "code": "EXECUTION_ARTIFACT_REQUIRED",
            "subject": "owner-profile",
            "detail": "a source-bound owner harness artifact is required",
        }
        return _result(
            result_directory=result_directory,
            execution_status="NOT_EXECUTED",
            verification_status="FAIL",
            failure_owner="HARNESS",
            exit_code=EXIT_VERIFICATION_FAILURE,
            traceability=None,
            issues=[issue],
        )
    if result_directory.is_symlink() or not result_directory.is_dir():
        issue = {
            "severity": "ERROR",
            "code": "EXECUTION_ARTIFACT_INVALID",
            "subject": str(result_directory),
            "detail": "artifact path must be a non-symlink directory",
        }
        return _result(
            result_directory=result_directory,
            execution_status="ERROR",
            verification_status="INCONCLUSIVE",
            failure_owner="HARNESS",
            exit_code=EXIT_HARNESS_ERROR,
            traceability=None,
            issues=[issue],
        )

    report = check_traceability(root, result_directory=result_directory)
    issues = [dict(item) for item in report.get("issues", [])]
    codes = {str(item.get("code")) for item in issues}
    if "EXECUTION_ARTIFACT_INVALID" in codes:
        return _result(
            result_directory=result_directory,
            execution_status="ERROR",
            verification_status="INCONCLUSIVE",
            failure_owner="HARNESS",
            exit_code=EXIT_HARNESS_ERROR,
            traceability=report,
            issues=issues,
        )
    if report.get("status") == "PASS":
        return _result(
            result_directory=result_directory,
            execution_status="EXECUTED",
            verification_status="PASS",
            failure_owner="NONE",
            exit_code=EXIT_PASS,
            traceability=report,
            issues=issues,
        )

    inconclusive = "VERIFICATION_INCONCLUSIVE" in codes
    stale = report.get("status") == "STALE"
    harness_codes = codes - {"VERIFICATION_FAILED"}
    return _result(
        result_directory=result_directory,
        execution_status="EXECUTED",
        verification_status="STALE" if stale else ("INCONCLUSIVE" if inconclusive else "FAIL"),
        failure_owner="HARNESS" if harness_codes else "SUT",
        exit_code=EXIT_VERIFICATION_FAILURE,
        traceability=report,
        issues=issues,
    )


def render_owner_gate(result: Mapping[str, Any]) -> str:
    lines = [
        f"VERIFICATION_GATE_STATUS={result['status']}",
        f"EXECUTION_STATUS={result['execution_status']}",
        f"VERIFICATION_STATUS={result['verification_status']}",
        f"FAILURE_OWNER={result['failure_owner']}",
        f"TRACEABILITY_STATUS={result['traceability_status']}",
    ]
    for environment, statuses in result.get("environment_status", {}).items():
        rendered = ",".join(f"{status}={count}" for status, count in statuses.items())
        lines.append(f"ENVIRONMENT {environment} {rendered}")
    for issue in result.get("issues", []):
        lines.append(f"{issue['severity']} {issue['code']} {issue['subject']}: {issue['detail']}")
    return "\n".join(lines) + "\n"


def dump_owner_gate(result: Mapping[str, Any]) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2) + "\n"
