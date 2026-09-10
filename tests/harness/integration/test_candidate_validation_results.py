"""Exercise the validation runner with real pytest outcomes in a tiny test repo."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from tools.run_candidate_full_validation import COVERAGE_MODULES, run_validation


@pytest.mark.parametrize("outcome,expected", [("pass", "PASS"), ("assertion", "TEST_FAILURE"), ("skip", "INCOMPLETE_TEST_SELECTION")])
def test_real_pytest_failure_is_not_reported_as_a_harness_defect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    expected: str,
) -> None:
    project = tmp_path / "runner-fixture"
    project.mkdir()
    for name in os.environ:
        if name.startswith("COV_CORE_") or name == "COVERAGE_PROCESS_START":
            monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    (project / "pyproject.toml").write_text('[tool.pytest.ini_options]\npythonpath=["src"]\ntestpaths=["tests"]\n')
    (project / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n.coverage\n")
    wrapper = project / "tools/sqlite_runtime/run-fixed.sh"
    wrapper.parent.mkdir(parents=True)
    # This fixture tests orchestration, not SQLite compatibility.
    wrapper.write_text('#!/bin/sh\nexec "$@"\n')
    wrapper.chmod(0o755)
    full_runner = project / "tools/run_full_regression.sh"
    full_runner.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -m pytest "$@"\n')
    full_runner.chmod(0o755)
    for module in COVERAGE_MODULES:
        path = project / "src" / (module.replace(".", "/") + ".py")
        path.parent.mkdir(parents=True, exist_ok=True)
        (path.parent / "__init__.py").touch()
        path.write_text("def choose(value):\n    if value:\n        return True\n    return False\n")
    tests = project / "tests"
    tests.mkdir()
    body = "import importlib\nimport pytest\n\ndef test_contract():\n"
    for module in COVERAGE_MODULES:
        body += f"    module = importlib.import_module({module!r})\n    assert module.choose(True)\n    assert not module.choose(False)\n"
    if outcome == "assertion":
        body += '    assert False, "deliberate assertion in the runner fixture"\n'
    elif outcome == "skip":
        body += '    pytest.skip("deliberate incomplete selection")\n'
    (tests / "test_contract.py").write_text(body)
    for args in [
        ("init", "-q"),
        ("add", "."),
        ("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"),
    ]:
        subprocess.run(["git", *args], cwd=project, check=True, capture_output=True)
    output = tmp_path / "result"
    result = run_validation(project, output, timeout_seconds=30)
    assert result["classification"] == expected, (result, (output / "stdout.txt").read_text())
    assert result["source_stable"]
    assert not list(project.glob(".coverage*"))
    assert (output / ".coverage").is_file()
    assert result["trusted"] is (expected == "PASS")
    saved = json.loads((output / "summary.json").read_bytes())
    assert saved["classification"] == expected
    assert result["junit_counts"]["failures"] == int(outcome == "assertion")
