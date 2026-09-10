#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--local-host", required=True)
    args = parser.parse_args()
    rows = [json.loads(line) for line in args.journal.read_text(encoding="utf-8").splitlines() if line.strip()]
    corrected: list[str] = []
    for row in rows:
        command = str(row.get("command") or "")
        if not row.get("host"):
            row["host"] = args.local_host
            row["host_correction_reason"] = "journal bootstrap entry written before host field was introduced"
            corrected.append(str(row.get("command_id") or ""))
        elif row.get("host") == "arena-server" and command.startswith("kubectl "):
            row["host"] = args.local_host
            row["host_correction_reason"] = "kubectl used the local k3s context on node yuki; no SSH was invoked"
            corrected.append(str(row.get("command_id") or ""))
    temporary = args.journal.with_name(f".{args.journal.name}.{os.getpid()}.tmp")
    temporary.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    os.replace(temporary, args.journal)
    print(json.dumps({"corrected_count": len(corrected), "command_ids": corrected}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
