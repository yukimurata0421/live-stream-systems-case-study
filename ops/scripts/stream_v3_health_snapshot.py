#!/usr/bin/env python3
"""Write last-known-good observability snapshots for the stream_v3 exporter."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from stream_v3_prometheus_exporter import (  # noqa: E402
    HEALTH_SUMMARY_SNAPSHOT,
    OBJECTIVE_SLI_SNAPSHOT,
    default_repo_root,
    default_state_root,
    run_json,
    stream_cli,
)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def snapshot_payload(payload: dict[str, Any], *, command: list[str], snapshot_ts: str) -> dict[str, Any]:
    result = dict(payload)
    result["_snapshot"] = {
        "command": command,
        "snapshot_source": "stream_v3_health_snapshot",
        "snapshot_ts_utc": snapshot_ts,
    }
    return result


def build_snapshots(
    *,
    repo_root: Path,
    state_root: Path,
    windows: str,
    timeout_sec: float,
) -> dict[str, dict[str, Any]]:
    cli = stream_cli(repo_root)
    health_command = [str(cli), "health-summary", "--windows", windows, "--json"]
    objective_command = [str(cli), "objective-sli", "--json", "--no-record"]
    snapshot_ts = iso_now()
    health = run_json(repo_root, state_root, health_command, timeout_sec=timeout_sec)
    objective = run_json(repo_root, state_root, objective_command, timeout_sec=timeout_sec)
    return {
        HEALTH_SUMMARY_SNAPSHOT: snapshot_payload(health, command=health_command, snapshot_ts=snapshot_ts),
        OBJECTIVE_SLI_SNAPSHOT: snapshot_payload(objective, command=objective_command, snapshot_ts=snapshot_ts),
    }


def write_snapshots(output_dir: Path, snapshots: dict[str, dict[str, Any]]) -> list[Path]:
    written: list[Path] = []
    for name, payload in snapshots.items():
        path = output_dir / name
        atomic_write_json(path, payload)
        written.append(path)
    return written


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo_root = default_repo_root()
    state_root = default_state_root(repo_root)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=repo_root)
    parser.add_argument("--state-root", type=Path, default=state_root)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--windows", default=os.environ.get("STREAM_V3_HEALTH_SNAPSHOT_WINDOWS", "1,8,24"))
    parser.add_argument("--timeout-sec", type=float, default=float(os.environ.get("STREAM_V3_HEALTH_SNAPSHOT_TIMEOUT_SEC", "45")))
    parser.add_argument("--json", action="store_true", help="print a compact JSON result")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.expanduser()
    state_root = args.state_root.expanduser()
    output_dir = (args.output_dir or state_root).expanduser()
    snapshots = build_snapshots(
        repo_root=repo_root,
        state_root=state_root,
        windows=str(args.windows),
        timeout_sec=float(args.timeout_sec),
    )
    written = write_snapshots(output_dir, snapshots)
    result = {
        "ok": True,
        "repo_root": str(repo_root),
        "state_root": str(state_root),
        "written": [str(path) for path in written],
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    else:
        print("stream_v3 health snapshots written: " + ", ".join(str(path) for path in written))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
