#!/usr/bin/env python3
"""Capture an end manifest and compare it with an iteration baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

EXCLUDED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".runtime",
    ".state",
    ".venv",
    "__pycache__",
    "node_modules",
    "venv",
}


def metadata(path: Path, root: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": path.relative_to(root).as_posix(),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def capture(root: Path, excluded_prefixes: tuple[Path, ...]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(part in EXCLUDED_PARTS for part in relative.parts):
            continue
        if any(relative == prefix or prefix in relative.parents for prefix in excluded_prefixes):
            continue
        if not path.is_file():
            continue
        item = metadata(path, root)
        result[str(item["path"])] = item
    return result


def load_jsonl(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                item = json.loads(line)
                result[str(item["path"])] = item
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--end-manifest", type=Path, required=True)
    parser.add_argument("--change-ledger", type=Path, required=True)
    parser.add_argument("--exclude-prefix", action="append", default=[])
    args = parser.parse_args()
    root = args.root.resolve()
    baseline = load_jsonl(args.baseline)
    excluded_prefixes = tuple(Path(value) for value in args.exclude_prefix)
    current = capture(root, excluded_prefixes)
    changes: list[dict[str, Any]] = []
    counts = {"created": 0, "modified": 0, "deleted": 0, "unchanged_pre_existing": 0}
    for path in sorted(set(baseline) | set(current)):
        before = baseline.get(path)
        after = current.get(path)
        if before is None:
            status = "created"
        elif after is None:
            status = "deleted"
        elif before["sha256"] != after["sha256"]:
            status = "modified"
        else:
            counts["unchanged_pre_existing"] += 1
            continue
        counts[status] += 1
        changes.append({"path": path, "status": status, "before": before, "after": after})
    args.end_manifest.parent.mkdir(parents=True, exist_ok=True)
    with args.end_manifest.open("w", encoding="utf-8") as handle:
        for item in current.values():
            handle.write(json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n")
    payload = {
        "schema_version": "recovery_control.file_change_ledger.v1",
        "root": str(root),
        "excluded_prefixes": [str(value) for value in excluded_prefixes],
        "baseline_file_count": len(baseline),
        "end_file_count": len(current),
        "counts": counts,
        "changes": changes,
        "secret_values_recorded": False,
    }
    args.change_ledger.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in payload.items() if key != "changes"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
