from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from runtime_boundary.ledger import EffectLedger
from runtime_boundary.legacy_reconciliation import find_legacy_effect_pairs, reconcile_legacy_effect_pairs


def _require_inactive(units: list[str]) -> None:
    for unit in units:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", unit],
            check=False,
            stdin=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            raise RuntimeError(f"MUTATOR_UNIT_ACTIVE:{unit}")


def _backup_database(source: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("BACKUP_DESTINATION_EXISTS")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
        destination_connection.execute("PRAGMA wal_checkpoint(FULL)")
    finally:
        destination_connection.close()
        source_connection.close()
    os.chmod(destination, 0o600)
    descriptor = os.open(destination, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _authority(path: Path) -> tuple[str, int]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = connection.execute("SELECT producer_id,producer_generation FROM runtime_authority WHERE singleton=1").fetchone()
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("RUNTIME_AUTHORITY_MISSING")
    return str(row[0]), int(row[1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Append-only reconciliation for exact legacy effect pairs")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--require-unit-inactive", action="append", default=[])
    args = parser.parse_args()
    database = args.database.resolve(strict=True)
    producer_id, producer_generation = _authority(database)

    if args.apply:
        if args.backup is None:
            raise ValueError("APPLY_REQUIRES_BACKUP")
        if not args.require_unit_inactive:
            raise ValueError("APPLY_REQUIRES_MUTATOR_UNIT_GUARD")
        _require_inactive(args.require_unit_inactive)
        _backup_database(database, args.backup)
        working = database
    else:
        temporary_directory = Path(tempfile.mkdtemp(prefix="effect-reconciliation-"))
        working = temporary_directory / "ledger.sqlite3"
        _backup_database(database, working)

    try:
        ledger = EffectLedger(
            working,
            initial_producer_id=producer_id,
            initial_producer_generation=producer_generation,
            allow_initialize=False,
        )
        try:
            if args.apply:
                report = reconcile_legacy_effect_pairs(ledger)
            else:
                pairs = find_legacy_effect_pairs(ledger)
                report = {
                    "schema": "runtime.legacy_effect_pair_reconciliation_dry_run.v1",
                    "candidate_pair_count": len(pairs),
                    "candidate_scope_ids": [pair.effect_scope_id for pair in pairs],
                    "raw_outcome_unknown_count": ledger.raw_unresolved_count(),
                    "unresolved_scope_count_before": ledger.unresolved_count(),
                }
        finally:
            ledger.close()
    finally:
        if not args.apply:
            shutil.rmtree(temporary_directory)
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
