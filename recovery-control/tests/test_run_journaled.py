from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "tools" / "run_journaled.py"
SPEC = importlib.util.spec_from_file_location("run_journaled", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_redact_removes_assignment_and_url_credentials() -> None:
    value = "token=abc password=def rtmp://user:credential@example.invalid/live"
    redacted = MODULE.redact(value)
    assert "abc" not in redacted
    assert "def" not in redacted
    assert "credential" not in redacted
    assert redacted.count("REDACTED") == 3


def test_command_start_failure_is_journaled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = tmp_path / "commands.jsonl"
    output = tmp_path / "output"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_journaled.py",
            "--journal",
            str(journal),
            "--output-dir",
            str(output),
            "--cwd",
            str(tmp_path),
            "--classification",
            "TEST_FAILURE",
            "--",
            "definitely-not-an-executable",
        ],
    )
    assert MODULE.main() == 127
    entry = json.loads(journal.read_text(encoding="utf-8"))
    assert entry["exit_code"] == 127
    assert entry["classification"] == "TEST_FAILURE"
    assert Path(entry["output_artifact"]).read_text(encoding="utf-8") == "COMMAND_START_FAILED:FileNotFoundError\n"
