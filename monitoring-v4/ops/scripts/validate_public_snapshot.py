#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MAX_PUBLIC_FILE_BYTES = 1_000_000

FORBIDDEN_DIRECTORY_PARTS = {
    ".state",
    ".venv",
    ".pytest_cache",
    "__pycache__",
    "build",
    "dist",
}
FORBIDDEN_SUFFIXES = {
    ".db",
    ".dump",
    ".jsonl",
    ".key",
    ".log",
    ".p12",
    ".pem",
    ".pfx",
    ".pyc",
    ".sqlite3",
}
FORBIDDEN_EXACT_PATHS = {
    "config/monitoring_v4_inventory.json",
}
FORBIDDEN_PATH_PREFIXES = (
    "docs/50_ops_logs/",
)
FORBIDDEN_TEXT = {
    "private_home_path": re.compile(re.escape("/home/" + "yuki")),
    "private_archive_path": re.compile(re.escape("/mnt/" + "archive")),
    "private_ipv4_10": re.compile(r"\b" + "10" + r"\.(?:\d{1,3}\.){2}\d{1,3}\b"),
    "private_ipv4_172": re.compile(
        r"\b" + "172" + r"\.(?:1[6-9]|2\d|3[01])\.(?:\d{1,3}\.)\d{1,3}\b"
    ),
    "private_ipv4_192": re.compile(
        r"\b" + "192" + r"\.168\.(?:\d{1,3}\.)\d{1,3}\b"
    ),
    "private_key_material": re.compile(
        "BEGIN " + r"(?:RSA |EC |OPENSSH )?PRIVATE KEY"
    ),
}
GENERIC_SECURITY_LITERALS = {
    "deploy/k3s/network-policy.yaml": (
        "10" + ".0.0.0/8",
        "172" + ".16.0.0/12",
        "192" + ".168.0.0/16",
    ),
}


def public_paths() -> list[Path]:
    completed = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return sorted(
        ROOT / item.decode("utf-8")
        for item in completed.stdout.split(b"\0")
        if item
    )


def validate() -> dict[str, object]:
    issues: list[dict[str, object]] = []
    paths = public_paths()
    for path in paths:
        relative = path.relative_to(ROOT).as_posix()
        parts = set(path.relative_to(ROOT).parts)
        if relative in FORBIDDEN_EXACT_PATHS:
            issues.append({"path": relative, "code": "forbidden_exact_path"})
        if any(relative.startswith(prefix) for prefix in FORBIDDEN_PATH_PREFIXES):
            issues.append({"path": relative, "code": "forbidden_path_prefix"})
        if parts & FORBIDDEN_DIRECTORY_PARTS:
            issues.append({"path": relative, "code": "forbidden_directory"})
        if path.suffix.lower() in FORBIDDEN_SUFFIXES or path.name == ".env":
            issues.append({"path": relative, "code": "forbidden_artifact_type"})
        if not path.is_file():
            issues.append({"path": relative, "code": "not_regular_file"})
            continue
        size = path.stat().st_size
        if size > MAX_PUBLIC_FILE_BYTES:
            issues.append(
                {
                    "path": relative,
                    "code": "file_too_large",
                    "size_bytes": size,
                }
            )
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            issues.append({"path": relative, "code": "non_utf8_file"})
            continue
        scan_text = text
        # Canonical RFC1918 blocks in an ipBlock ``except`` list prevent the
        # API collector from reaching private networks; they are not endpoint
        # data. All other private addresses remain forbidden.
        for literal in GENERIC_SECURITY_LITERALS.get(relative, ()):
            scan_text = scan_text.replace(literal, "")
        for code, pattern in FORBIDDEN_TEXT.items():
            match = pattern.search(scan_text)
            if match:
                issues.append(
                    {
                        "path": relative,
                        "code": code,
                        "line": scan_text.count("\n", 0, match.start()) + 1,
                    }
                )
    return {
        "schema": "monitoring_v4.public_snapshot_validation.v1",
        "file_count": len(paths),
        "issue_count": len(issues),
        "issues": issues,
        "ok": not issues,
    }


def main() -> int:
    payload = validate()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
