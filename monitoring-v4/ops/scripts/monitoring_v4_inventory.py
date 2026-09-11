#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_REPO = Path(
    os.environ.get("STREAM_V3_SOURCE_REPO", "/opt/stream_v3")
).expanduser()
sys.path.insert(0, str(ROOT / "src"))

from stream_monitoring_v4.architecture import inventory_report
from stream_monitoring_v4.inventory.contract import load_live_unit_contract
from stream_monitoring_v4.inventory.systemd import (
    collection_failure_code,
    collect_systemd_snapshot,
    load_systemd_snapshot,
)


COLLECT_SPEC = re.compile(r"^(?P<role>[A-Za-z0-9_.-]{1,96})=(?P<target>[A-Za-z0-9_.@-]{1,160})$")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Build the read-only Monitoring v4 R0 inventory report")
    result.add_argument("--implementation-root", type=Path, default=ROOT)
    result.add_argument(
        "--source-repo",
        type=Path,
        default=DEFAULT_SOURCE_REPO,
        help="read-only stream_v3 migration source repository",
    )
    result.add_argument(
        "--manifest",
        type=Path,
        default=ROOT / "config" / "monitoring_v4_inventory.example.json",
    )
    result.add_argument(
        "--live-unit-contract",
        type=Path,
        default=ROOT / "config" / "monitoring_v4_live_unit_contract.json",
        help="exact unit ownership/state contract",
    )
    result.add_argument(
        "--systemd-snapshot",
        type=Path,
        action="append",
        default=[],
        help="previously captured secret-free systemd snapshot; repeat for multiple hosts",
    )
    result.add_argument(
        "--collect-systemd",
        action="append",
        default=[],
        metavar="HOST_ROLE=TARGET",
        help="collect read-only live state; TARGET is local or a validated SSH alias",
    )
    result.add_argument(
        "--require-live-completeness",
        action="store_true",
        help="return non-zero unless every required host has an exact, healthy live snapshot",
    )
    result.add_argument("--output", type=Path, default=None, help="optional explicit output path; stdout otherwise")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    contract = load_live_unit_contract(args.live_unit_contract.resolve())
    host_contracts = {str(item["host_role"]): item for item in contract["hosts"]}
    snapshots = [load_systemd_snapshot(path) for path in args.systemd_snapshot]
    collection_errors: list[str] = []
    for raw in args.collect_systemd:
        match = COLLECT_SPEC.fullmatch(raw)
        if match is None:
            raise SystemExit("--collect-systemd must be HOST_ROLE=local or HOST_ROLE=SSH_ALIAS")
        role = match.group("role")
        target = match.group("target")
        host = host_contracts.get(role)
        if host is None:
            raise SystemExit(f"unknown live-unit host role: {role}")
        prefix: tuple[str, ...]
        if target == "local":
            prefix = ()
        else:
            prefix = ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", target)
        try:
            snapshot = collect_systemd_snapshot(
                role,
                host["patterns"],
                command_prefix=prefix,
            )
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
            collection_errors.append(collection_failure_code(role, exc))
            sys.stderr.write(
                "warning: systemd collection failed; "
                f"role={role} error={type(exc).__name__}; partial evidence retained\n"
            )
        else:
            snapshots.append(snapshot)
    payload = inventory_report(
        args.implementation_root.resolve(),
        args.source_repo.resolve(),
        args.manifest.resolve(),
        live_unit_contract=args.live_unit_contract.resolve(),
        systemd_snapshots=snapshots,
        systemd_collection_errors=collection_errors,
    )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        sys.stdout.write(rendered)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    required_ok = payload["evidence_complete"] if args.require_live_completeness else payload["ok"]
    return 0 if required_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
