#!/usr/bin/env python3
"""Verify the selected public port without reading live state or using a network."""
from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
MAP = ROOT / "publication/source-map.json"
PRIVATE_HOME = re.compile("/home/" + "yuki(?:/|$)")
PRIVATE_ADDRESS = re.compile(r"\b(?:10\.(?:\d{1,3}\.){2}\d{1,3}|192\.168\.(?:\d{1,3}\.)\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b")
PRIVATE_KEY = re.compile("BEGIN " + r"(?:RSA |EC |OPENSSH )?PRIVATE KEY")
FORBIDDEN_SUFFIXES = (".pyc", ".sqlite3", ".sqlite3-wal", ".sqlite3-shm", ".pcap", ".log", ".pem", ".key")


def validate() -> list[dict[str, str]]:
    document = json.loads(MAP.read_text(encoding="utf-8"))
    if document.get("schema") != "stream.public_source_port.v1":
        raise ValueError("PUBLIC_SOURCE_MAP_SCHEMA_INVALID")
    issues: list[dict[str, str]] = []
    seen: set[str] = set()
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
            if pattern.search(text):
                issues.append({"path": relative, "reason": name})
        if path.suffix == ".py":
            try:
                ast.parse(text, filename=relative)
            except SyntaxError:
                issues.append({"path": relative, "reason": "syntax_invalid"})
    return issues


def main() -> int:
    issues = validate()
    print(json.dumps({"schema": "stream.public_port_check.v1", "issue_count": len(issues), "issues": issues}, indent=2))
    return int(bool(issues))


if __name__ == "__main__":
    raise SystemExit(main())
