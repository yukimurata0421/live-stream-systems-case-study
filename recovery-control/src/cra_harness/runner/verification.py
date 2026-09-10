from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc, utc_now

EXISTING_PATHS = [
    "tests/unit",
    "tests/protocol",
    "tests/integration",
    "tests/heartbeat",
    "tests/reconciliation",
    "tests/failure_injection",
    "tests/replay",
    "tests/property",
]
HARNESS_GROUPS = {
    "harness_unit": "tests/harness/unit",
    "harness_integration": "tests/harness/integration",
    "negative_control": "tests/harness/negative_control",
    "self_test": "tests/harness/selftest",
}


def verification_commands() -> list[str]:
    return [
        " ".join([sys.executable, "-m", "pytest", "-q", *EXISTING_PATHS]),
        " ".join([sys.executable, "-m", "pytest", "-q", "tests/harness"]),
    ]


def _execute(project_root: Path, paths: list[str]) -> dict[str, Any]:
    command = [sys.executable, "-m", "pytest", "-q", *paths]
    result = subprocess.run(command, cwd=project_root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    return {
        "command": " ".join(command),
        "exit_code": result.returncode,
        "output_tail": "\n".join(result.stdout.splitlines()[-8:]),
    }


def _collect(project_root: Path, path: str) -> int:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", path],
        cwd=project_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pytest collection failed for {path}: {result.stdout[-1000:]}")
    match = re.search(r"(\d+) tests? collected", result.stdout)
    if match is None:
        node_ids = [line for line in result.stdout.splitlines() if "::" in line]
        return len(node_ids)
    return int(match.group(1))


def run_verification(project_root: Path) -> dict[str, Any]:
    started_at = isoformat_utc(utc_now())
    existing = _execute(project_root, EXISTING_PATHS)
    harness = _execute(project_root, ["tests/harness"])
    group_counts = {name: _collect(project_root, path) for name, path in HARNESS_GROUPS.items()}
    property_count = _collect(project_root, "tests/property")
    return {
        "started_at": started_at,
        "finished_at": isoformat_utc(utc_now()),
        "passed": existing["exit_code"] == 0 and harness["exit_code"] == 0,
        "existing": existing,
        "harness": harness,
        "counts": {
            "existing_tests": _collect(project_root, "tests/unit")
            + _collect(project_root, "tests/protocol")
            + _collect(project_root, "tests/integration")
            + _collect(project_root, "tests/heartbeat")
            + _collect(project_root, "tests/reconciliation")
            + _collect(project_root, "tests/failure_injection")
            + _collect(project_root, "tests/replay")
            + property_count,
            **group_counts,
            "property": property_count,
        },
    }
