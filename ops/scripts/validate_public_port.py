#!/usr/bin/env python3
"""Verify the selected public port without reading live state or using a network."""
from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
MAP = ROOT / "publication/source-map.json"
ALLOWLIST = ROOT / "publication/private-path-allowlist.json"
PRIVATE_HOME = re.compile("/home/" + r"yuki(?![A-Za-z0-9_.-])")
PRIVATE_ADDRESS = re.compile(r"\b(?:10\.(?:\d{1,3}\.){2}\d{1,3}|192\.168\.(?:\d{1,3}\.)\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b")
PRIVATE_KEY = re.compile("BEGIN " + r"(?:RSA |EC |OPENSSH )?PRIVATE KEY")
FORBIDDEN_SUFFIXES = (".pyc", ".sqlite3", ".sqlite3-wal", ".sqlite3-shm", ".pcap", ".log", ".pem", ".key")


def validate() -> list[dict[str, str]]:
    document = json.loads(MAP.read_text(encoding="utf-8"))
    if document.get("schema") != "stream.public_source_port.v1":
        raise ValueError("PUBLIC_SOURCE_MAP_SCHEMA_INVALID")
    issues: list[dict[str, str]] = []
    seen: set[str] = set()
    allowlisted = {entry["path"] for entry in json.loads(ALLOWLIST.read_text(encoding="utf-8"))["entries"]}
    for row in document["files"]:
        relative = row["path"]
        parts = PurePosixPath(relative)
        if parts.is_absolute() or ".." in parts.parts or relative in seen:
            issues.append({"path": relative, "reason": "invalid_or_duplicate_path"})
            continue
        seen.add(relative)
        path = ROOT / relative
        if path.is_symlink() or not path.is_file():
            issues.append({"path": relative, "reason": "file_missing_or_symlink"})
            continue
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != row["public_sha256"]:
            issues.append({"path": relative, "reason": "source_digest_mismatch"})
        if len(raw) > 1_000_000 or relative.endswith(FORBIDDEN_SUFFIXES):
            issues.append({"path": relative, "reason": "runtime_artifact_or_size_limit"})
        text = raw.decode("utf-8")
        for name, pattern in (("private_workspace_path", PRIVATE_HOME), ("private_address", PRIVATE_ADDRESS), ("private_key", PRIVATE_KEY)):
            if name == "private_workspace_path" and relative in allowlisted:
                continue
            if pattern.search(text):
                issues.append({"path": relative, "reason": name})
        if path.suffix == ".py":
            try:
                ast.parse(text, filename=relative)
            except SyntaxError:
                issues.append({"path": relative, "reason": "syntax_invalid"})
    return issues


def tracked_files() -> list[str]:
    """Git-tracked paths; falls back to a filesystem walk outside a Git checkout."""
    try:
        output = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT, check=True, capture_output=True, timeout=30
        ).stdout
        return sorted(item.decode("utf-8") for item in output.split(b"\0") if item)
    except (OSError, subprocess.SubprocessError):
        skip = {".git", ".venv", ".runtime", "node_modules", "__pycache__"}
        return sorted(
            path.relative_to(ROOT).as_posix()
            for path in ROOT.rglob("*")
            if path.is_file() and not skip.intersection(path.relative_to(ROOT).parts)
        )


def validate_private_paths() -> list[dict[str, str]]:
    """Every tracked file naming the private workspace must be allowlisted with a reason."""
    document = json.loads(ALLOWLIST.read_text(encoding="utf-8"))
    if document.get("schema") != "stream.public_private_path_allowlist.v1":
        raise ValueError("PRIVATE_PATH_ALLOWLIST_SCHEMA_INVALID")
    allowed: dict[str, str] = {}
    issues: list[dict[str, str]] = []
    for entry in document["entries"]:
        if not str(entry.get("reason") or "").strip() or entry.get("path") in allowed:
            issues.append({"path": str(entry.get("path")), "reason": "invalid_private_path_allowlist_entry"})
            continue
        allowed[entry["path"]] = entry["reason"]
    found: set[str] = set()
    for relative in tracked_files():
        path = ROOT / relative
        if path.is_symlink() or not path.is_file():
            continue
        raw = path.read_bytes()
        if b"\0" in raw[:8192]:
            continue
        if PRIVATE_HOME.search(raw.decode("utf-8", errors="replace")):
            found.add(relative)
            if relative not in allowed:
                issues.append({"path": relative, "reason": "private_workspace_path"})
    for relative in sorted(set(allowed) - found):
        issues.append({"path": relative, "reason": "stale_private_path_allowlist_entry"})
    return issues


def main() -> int:
    issues = validate() + validate_private_paths()
    print(json.dumps({"schema": "stream.public_port_check.v1", "issue_count": len(issues), "issues": issues}, indent=2))
    return int(bool(issues))


if __name__ == "__main__":
    raise SystemExit(main())
