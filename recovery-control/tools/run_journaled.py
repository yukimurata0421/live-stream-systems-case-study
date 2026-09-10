#!/usr/bin/env python3
"""Run a command and append a redacted, digest-bound execution journal entry."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

SECRET_PATTERNS = (
    (re.compile(r"(?i)(password|token|secret|private[_-]?key)=([^\s]+)"), r"\1=REDACTED"),
    (re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)"), r"\1REDACTED"),
    (re.compile(r"(?i)(rtmps?://[^\s/:]+:)([^@\s]+)(@)"), r"\1REDACTED\3"),
    (
        re.compile(r"(?<![\d.])(?:10(?:\.\d{1,3}){3}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2}|192\.168(?:\.\d{1,3}){2})(?![\d.])"),
        "PRIVATE_ADDRESS_REDACTED",
    ),
)


def utc_text() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def redact(value: str) -> str:
    redacted = value
    for pattern, replacement in SECRET_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument(
        "--host",
        default=socket.gethostname(),
        help="Host where the journaled command executes (defaults to the local hostname)",
    )
    parser.add_argument("--classification", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a command is required after --")

    command_id = f"cmd-{uuid.uuid4()}"
    started_at = utc_text()
    try:
        result = subprocess.run(
            command,
            cwd=args.cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            env=os.environ.copy(),
        )
        return_code = result.returncode
        raw_output = result.stdout
    except OSError as exc:
        return_code = 127
        raw_output = f"COMMAND_START_FAILED:{type(exc).__name__}\n"
    finished_at = utc_text()
    output = redact(raw_output)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / f"{command_id}.log"
    output_path.write_text(output, encoding="utf-8")
    output_sha256 = hashlib.sha256(output_path.read_bytes()).hexdigest()
    entry = {
        "schema_version": "recovery_control.command_journal.v1",
        "command_id": command_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "host": args.host,
        "cwd": str(args.cwd.resolve()),
        "command": redact(subprocess.list2cmdline(command)),
        "exit_code": return_code,
        "classification": args.classification,
        "output_artifact": str(output_path),
        "output_sha256": output_sha256,
        "secret_values_recorded": False,
    }
    args.journal.parent.mkdir(parents=True, exist_ok=True)
    with args.journal.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(output, end="")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
