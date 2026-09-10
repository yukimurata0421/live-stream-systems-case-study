#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def optional_git(root: Path, *args: str) -> str | None:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def state(name: str, root: Path) -> dict[str, object]:
    head = optional_git(root, "rev-parse", "HEAD")
    branch = optional_git(root, "symbolic-ref", "--short", "HEAD")
    status = git(root, "status", "--porcelain=v1", "--untracked-files=normal")
    lines = status.splitlines()
    return {
        "repository": name,
        "root": str(root.resolve()),
        "head": head,
        "branch": branch,
        "unborn_head": head is None,
        "dirty": bool(lines),
        "entry_count": len(lines),
        "status_sha256": hashlib.sha256(status.encode()).hexdigest(),
        "status": lines,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("repository", nargs="+", help="NAME=PATH")
    args = parser.parse_args()
    repositories: dict[str, Path] = {}
    for item in args.repository:
        name, separator, raw_path = item.partition("=")
        if not separator or not name or not raw_path:
            parser.error(f"invalid repository mapping: {item!r}")
        repositories[name] = Path(raw_path).resolve()
    payload = {
        "schema_version": "recovery_control.git_states.v1",
        "repositories": {name: state(name, root) for name, root in repositories.items()},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
