#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_PARTS = {
    ".git",
    ".venv",
    ".runtime",
    ".hypothesis",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
}
FORBIDDEN_TOP = {"artifacts", ".agents", ".forgejo"}
FORBIDDEN_SUFFIXES = {".pem", ".key", ".sqlite3", ".sqlite3-wal", ".sqlite3-shm", ".pcap", ".log"}
SECRET_PATTERNS = {
    "api_key": re.compile(b"s" + b"k-(?:proj-)?[A-Za-z0-9_-]{20,}"),
    "github_token": re.compile(b"gh[pousr]_[A-Za-z0-9]{20,}"),
    "google_api_key": re.compile(b"AIza[0-9A-Za-z_-]{30,}"),
    "private_key": re.compile(b"-----BEGIN " + b"(?:RSA |EC |OPENSSH )?" + b"PRIVATE KEY-----"),
    "youtube_ingest_key": re.compile(b"rtmps?://[^\\s]+/(?:live2|app)/[A-Za-z0-9_-]{12,}"),
}


def candidate_files(root: Path) -> list[Path]:
    return sorted(
        path for path in root.rglob("*") if path.is_file() and not any(part in SKIP_PARTS for part in path.relative_to(root).parts)
    )


def validate(root: Path) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    required = ["README.md", "LICENSE", "pyproject.toml", "src", "contracts", "migrations", "policy", "tests"]
    for relative in required:
        if not (root / relative).exists():
            issues.append({"path": relative, "reason": "required_path_missing"})
    for path in candidate_files(root):
        relative = path.relative_to(root)
        if relative.parts[0] in FORBIDDEN_TOP:
            issues.append({"path": relative.as_posix(), "reason": "forbidden_top_level"})
            continue
        name = path.name.lower()
        if any(name.endswith(suffix) for suffix in FORBIDDEN_SUFFIXES):
            issues.append({"path": relative.as_posix(), "reason": "runtime_or_secret_artifact"})
            continue
        payload = path.read_bytes()
        if b"/home/" + b"yuki/" in payload:
            issues.append({"path": relative.as_posix(), "reason": "private_workspace_path"})
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(payload):
                issues.append({"path": relative.as_posix(), "reason": label})
    return issues


def main() -> int:
    issues = validate(ROOT)
    result = {
        "schema": "stream_recovery_control.public_snapshot_validation.v1",
        "file_count": len(candidate_files(ROOT)),
        "issue_count": len(issues),
        "issues": issues,
        "ok": not issues,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
