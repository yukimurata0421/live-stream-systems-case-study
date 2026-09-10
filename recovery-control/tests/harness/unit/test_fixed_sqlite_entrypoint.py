from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from cra_dell_recovery.sqlite import MINIMUM_PRODUCTION_SQLITE
from cra_harness.controls.sqlite_runtime import (
    FIXED_SQLITE_LIBRARY,
    FIXED_SQLITE_VERSION,
    evaluate_fixed_sqlite_runtime,
    fixed_sqlite_failure_message,
    is_full_regression_selection,
)

ROOT = Path(__file__).resolve().parents[3]


def _fixed_library(project_root: Path) -> Path:
    library = project_root / FIXED_SQLITE_LIBRARY
    library.parent.mkdir(parents=True)
    library.touch()
    return library.resolve()


def test_fixed_runtime_identity_requires_version_and_loaded_library(tmp_path: Path) -> None:
    library = _fixed_library(tmp_path)
    identity = evaluate_fixed_sqlite_runtime(
        tmp_path,
        runtime_version=FIXED_SQLITE_VERSION,
        loaded_library=str(library),
    )
    assert identity.passed
    assert identity.classification == "PASS"


def test_fixed_runtime_pin_matches_production_minimum() -> None:
    assert tuple(int(part) for part in FIXED_SQLITE_VERSION.split(".")) == MINIMUM_PRODUCTION_SQLITE


def test_fixed_runtime_identity_rejects_version_only_match(tmp_path: Path) -> None:
    _fixed_library(tmp_path)
    system_library = tmp_path / "usr/lib/libsqlite3.so.0"
    system_library.parent.mkdir(parents=True)
    system_library.touch()
    identity = evaluate_fixed_sqlite_runtime(
        tmp_path,
        runtime_version=FIXED_SQLITE_VERSION,
        loaded_library=str(system_library),
    )
    assert not identity.passed
    assert identity.classification == "ENVIRONMENT_FAILURE"


def test_fixed_runtime_identity_rejects_library_only_match(tmp_path: Path) -> None:
    library = _fixed_library(tmp_path)
    identity = evaluate_fixed_sqlite_runtime(
        tmp_path,
        runtime_version="3.46.1",
        loaded_library=str(library),
    )
    assert not identity.passed
    assert "required SQLite=3.51.3" in fixed_sqlite_failure_message(identity)
    assert "actual SQLite=3.46.1" in fixed_sqlite_failure_message(identity)


def test_fixed_runtime_identity_rejects_missing_expected_library(tmp_path: Path) -> None:
    identity = evaluate_fixed_sqlite_runtime(
        tmp_path,
        runtime_version=FIXED_SQLITE_VERSION,
        loaded_library=str(tmp_path / FIXED_SQLITE_LIBRARY),
    )
    assert not identity.passed
    assert not identity.expected_library_regular


def test_only_full_repository_selection_requires_fixed_runtime(tmp_path: Path) -> None:
    tests = tmp_path / "tests"
    tests.mkdir()
    assert is_full_regression_selection(tmp_path, [])
    assert is_full_regression_selection(tmp_path, ["tests"])
    assert is_full_regression_selection(tmp_path, [str(tests)])
    assert not is_full_regression_selection(tmp_path, ["tests/unit/test_one.py"])
    assert not is_full_regression_selection(tmp_path, ["tests/unit/test_one.py::test_one"])
    assert not is_full_regression_selection(tmp_path, ["tests/unit", "tests/harness"])


def test_plain_full_suite_stops_before_collection_with_actionable_environment_failure() -> None:
    environment = os.environ.copy()
    environment.pop("LD_LIBRARY_PATH", None)
    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--collect-only"],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
    )
    output = run.stdout + run.stderr
    assert run.returncode == 4
    assert "CRA_FULL_REGRESSION_ENVIRONMENT_FAILURE" in output
    assert "Run tools/run_full_regression.sh" in output
    assert "collected" not in output
