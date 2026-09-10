from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from cra_authority.storage import CentralStore


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="CRA no-action shadow host process")
    value.add_argument("--database", type=Path, required=True)
    value.add_argument("--migration", type=Path, required=True)
    value.add_argument("--watch", action="store_true")
    return value


def main() -> None:
    args = parser().parse_args()
    store = CentralStore(args.database, args.migration)
    status = {
        "component": "cra-authority",
        "mode": "NO_ACTION_SHADOW",
        "sqlite_version": store.status.version,
        "production_sqlite_version_gate": store.status.production_gate,
        "journal_mode": store.status.journal_mode,
        "readiness": "SAFE_BLOCKED_UNPROVISIONED"
        if store.connection.execute("SELECT count(*) FROM control_plane_identity").fetchone()[0] == 0
        else "LEDGER_READY",
    }
    print(json.dumps(status, sort_keys=True), flush=True)
    while args.watch:
        time.sleep(10)
        print(json.dumps(status, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
