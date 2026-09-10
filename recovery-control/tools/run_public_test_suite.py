#!/usr/bin/env python3
"""Run the repository-only test surface without private evidence dependencies."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "tests/public_ci_exclusions.json"


def selected_tests() -> tuple[list[str], list[dict[str, str]]]:
    document = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if document.get("schema") != "stream_recovery_control.public_ci_exclusions.v1":
        raise ValueError("PUBLIC_CI_EXCLUSION_SCHEMA")
    excluded = document.get("excluded")
    if not isinstance(excluded, list) or not excluded:
        raise ValueError("PUBLIC_CI_EXCLUSIONS_REQUIRED")
    excluded_paths: set[str] = set()
    for item in excluded:
        if not isinstance(item, dict) or set(item) != {"path", "reason"}:
            raise ValueError("PUBLIC_CI_EXCLUSION_INVALID")
        path = str(item["path"])
        reason = str(item["reason"])
        candidate = ROOT / path
        if not path.startswith("tests/") or not reason.strip() or not candidate.is_file():
            raise ValueError(f"PUBLIC_CI_EXCLUSION_INVALID:{path}")
        excluded_paths.add(path)
    if len(excluded_paths) != len(excluded):
        raise ValueError("PUBLIC_CI_EXCLUSION_DUPLICATE")
    discovered = sorted(path.relative_to(ROOT).as_posix() for path in (ROOT / "tests").rglob("test_*.py"))
    selected = [path for path in discovered if path not in excluded_paths]
    if not selected or excluded_paths - set(discovered):
        raise ValueError("PUBLIC_CI_SELECTION_INVALID")
    return selected, excluded


def main() -> int:
    selected, excluded = selected_tests()
    print(
        json.dumps(
            {
                "schema": "stream_recovery_control.public_ci_selection.v1",
                "selected_files": len(selected),
                "excluded_files": len(excluded),
                "exclusions": excluded,
            },
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )
    return subprocess.call([sys.executable, "-m", "pytest", "-q", *selected], cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
