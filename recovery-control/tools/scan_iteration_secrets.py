#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

PATTERNS = {
    "openai_key": re.compile(rb"sk-(?:proj-)?[A-Za-z0-9_-]{20,}"),
    "github_token": re.compile(rb"gh[pousr]_[A-Za-z0-9]{20,}"),
    "google_api_key": re.compile(rb"AIza[0-9A-Za-z_-]{30,}"),
    "private_key": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "youtube_ingest_key": re.compile(rb"rtmps?://[^\s]+/(?:live2|app)/[A-Za-z0-9_-]{12,}"),
}


def files_under(paths: list[Path]) -> list[Path]:
    result: list[Path] = []
    for path in paths:
        if path.is_file():
            result.append(path)
            continue
        result.extend(item for item in path.rglob("*") if item.is_file() and ".git" not in item.parts)
    return sorted(set(result))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()
    findings: list[dict[str, str]] = []
    scanned = 0
    for path in files_under(args.paths):
        try:
            payload = path.read_bytes()
        except OSError:
            continue
        scanned += 1
        for name, pattern in PATTERNS.items():
            if pattern.search(payload):
                findings.append({"path": str(path), "pattern": name})
    result = {
        "schema_version": "policy_semantics.secret_scan.v1",
        "files_scanned": scanned,
        "finding_count": len(findings),
        "findings_without_values": findings,
        "secret_values_recorded": False,
        "pass": not findings,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
