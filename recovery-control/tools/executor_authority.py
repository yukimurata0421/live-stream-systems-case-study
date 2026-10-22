#!/usr/bin/env python3
"""Show or hand over the in-Pod Executor's authority without changing the Pod.

The running Executor reads `runtime_authority` from its SQLite ledger on every request, so a
compare-and-set on that row is a handover. This tool reuses the ledger's own
`EffectLedger.switch_authority` (expected producer check and zero unresolved fences inside one
`BEGIN IMMEDIATE` transaction) but never runs the ledger constructor, whose migrations and
backfills would write to a live ledger. `show` opens the file read-only.

Run on the delivery host as root, with --runtime-src pointing at the `runtime_boundary` source
that matches the running Executor image.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
from pathlib import Path
from typing import Any


def _ledger_class(runtime_src: Path) -> Any:
    sys.path.insert(0, str(runtime_src))
    from runtime_boundary.ledger import EffectLedger  # type: ignore[import-not-found]

    return EffectLedger


def _attach(ledger_class: Any, connection: sqlite3.Connection) -> Any:
    ledger = ledger_class.__new__(ledger_class)  # no constructor: no migration or backfill writes
    ledger.connection = connection
    ledger._lock = threading.RLock()
    return ledger


def _connect(path: Path, *, read_only: bool) -> sqlite3.Connection:
    if not path.is_file():
        raise SystemExit(f"ledger not found: {path}")
    uri = f"file:{path}?mode={'ro' if read_only else 'rw'}"
    connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def show(ledger_class: Any, path: Path) -> dict[str, Any]:
    connection = _connect(path, read_only=True)
    try:
        ledger = _attach(ledger_class, connection)
        row = connection.execute("SELECT * FROM runtime_authority WHERE singleton=1").fetchone()
        return {
            "producer_id": row["producer_id"],
            "producer_generation": row["producer_generation"],
            "authority_version": row["authority_version"],
            "unresolved_count": ledger.unresolved_count(),
            "unresolved_scopes": [
                {k: scope[k] for k in ("effect_scope_id", "owner_request_id", "state")} for scope in ledger.unresolved_scopes()
            ],
        }
    finally:
        connection.close()


def switch(ledger_class: Any, path: Path, expect: tuple[str, int], to: tuple[str, int]) -> dict[str, Any]:
    connection = _connect(path, read_only=False)
    try:
        decision = _attach(ledger_class, connection).switch_authority(
            expected_producer_id=expect[0],
            expected_generation=expect[1],
            new_producer_id=to[0],
            new_generation=to[1],
        )
        return {
            "accepted": decision.accepted,
            "reason": decision.reason,
            "producer_id": decision.producer_id,
            "producer_generation": decision.producer_generation,
            "authority_version": decision.authority_version,
        }
    finally:
        connection.close()


def _producer(value: str) -> tuple[str, int]:
    name, _, generation = value.rpartition(":")
    if not name or not generation.isdigit() or int(generation) < 1:
        raise argparse.ArgumentTypeError("expected PRODUCER_ID:GENERATION")
    return name, int(generation)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="executor_authority")
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--runtime-src", type=Path, required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("show", help="read-only: current authority and unresolved fences")
    handover = sub.add_parser("switch", help="compare-and-set; refused while any fence is unresolved")
    handover.add_argument("--expect", type=_producer, required=True)
    handover.add_argument("--to", type=_producer, required=True)
    args = parser.parse_args(argv)
    ledger_class = _ledger_class(args.runtime_src)
    if args.command == "show":
        result = show(ledger_class, args.ledger)
        print(json.dumps(result, indent=1))
        return 0
    result = switch(ledger_class, args.ledger, args.expect, args.to)
    print(json.dumps(result, indent=1))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    sys.exit(main())
