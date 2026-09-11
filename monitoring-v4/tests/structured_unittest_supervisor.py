#!/usr/bin/env python3
"""Run an existing unittest suite under a parent-owned structured event log."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


class EventResult(unittest.TextTestResult):
    def __init__(self, *args: object, event_path: Path, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.event_path = event_path

    def _event(self, kind: str, test: unittest.case.TestCase) -> None:
        with self.event_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"kind": kind, "test": self.getDescription(test)}, sort_keys=True) + "\n")

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        super().addSuccess(test)
        self._event("passed", test)

    def addSkip(self, test: unittest.case.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self._event("skipped", test)

    def addFailure(self, test: unittest.case.TestCase, err: object) -> None:
        super().addFailure(test, err)
        self._event("failed", test)

    def addError(self, test: unittest.case.TestCase, err: object) -> None:
        super().addError(test, err)
        self._event("errors", test)


def child(event_path: Path, start_dir: str) -> int:
    project_root = str(Path(__file__).resolve().parents[1])
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    suite = unittest.defaultTestLoader.discover(start_dir)
    result_class = lambda *args, **kwargs: EventResult(*args, event_path=event_path, **kwargs)
    result = unittest.TextTestRunner(stream=open(os.devnull, "w"), verbosity=0, resultclass=result_class).run(suite)
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"kind": "summary", "discovered": suite.countTestCases(), "exit_code": 0 if result.wasSuccessful() else 1}, sort_keys=True) + "\n")
    return 0 if result.wasSuccessful() else 1


def supervise(start_dir: str, *, python: str | None = None) -> dict[str, object]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="stream-v4-unittest-events-") as root:
        event_path = Path(root) / "events.jsonl"
        completed = subprocess.run([python or sys.executable, str(Path(__file__).resolve()), "--child", "--events", str(event_path), "--start-dir", start_dir], text=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()] if event_path.exists() else []
    totals = {kind: sum(event.get("kind") == kind for event in events) for kind in ("passed", "skipped", "failed", "errors")}
    summary = next((event for event in reversed(events) if event.get("kind") == "summary"), {})
    return {"discovered": summary.get("discovered", 0), **totals, "exit_code": completed.returncode, "duration_sec": round(time.monotonic() - started, 6), "event_count": len(events)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--events", type=Path)
    parser.add_argument("--start-dir", default="tests")
    args = parser.parse_args(argv)
    if args.child:
        if args.events is None:
            raise SystemExit("--events is required for child mode")
        return child(args.events, args.start_dir)
    print(json.dumps(supervise(args.start_dir), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
