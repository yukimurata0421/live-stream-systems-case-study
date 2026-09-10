#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path

PRIVATE_ADDRESS = re.compile(
    r"(?<![\d.])(?:10(?:\.\d{1,3}){3}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2}|192\.168(?:\.\d{1,3}){2})(?![\d.])"
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--journal", type=Path, required=True)
    args = parser.parse_args()
    project = args.project_root.resolve()
    rows = [json.loads(line) for line in args.journal.read_text(encoding="utf-8").splitlines() if line.strip()]
    sanitized: list[str] = []
    replacement_counts: dict[str, int] = {}
    for row in rows:
        raw = Path(str(row["output_artifact"]))
        output = raw if raw.is_absolute() else project / raw
        if not output.is_file():
            continue
        value = output.read_text(encoding="utf-8")
        updated, count = PRIVATE_ADDRESS.subn("PRIVATE_ADDRESS_REDACTED", value)
        if count:
            output.write_text(updated, encoding="utf-8")
            row["output_sha256"] = sha256(output)
            row["output_sanitization"] = "private address redacted after terminal artifact audit"
            sanitized.append(str(row["command_id"]))
            replacement_counts[str(row["command_id"])] = count
    temporary = args.journal.with_name(f".{args.journal.name}.{os.getpid()}.tmp")
    temporary.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    os.replace(temporary, args.journal)
    print(
        json.dumps(
            {"sanitized_command_ids": sanitized, "replacement_counts": replacement_counts},
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
