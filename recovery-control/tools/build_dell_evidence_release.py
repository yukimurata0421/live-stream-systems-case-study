#!/usr/bin/env python3
"""Build Evidence Binding release from the prior Dell live base only."""

from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-release", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    old = args.old_release.resolve()
    source = args.source.resolve()
    output = args.output
    if output.exists():
        raise FileExistsError(output)
    shutil.copytree(old, output, symlinks=True)

    for relative in (
        "src/maintenance_audit/__init__.py",
        "src/dell_recovery_agent/server.py",
        "src/dell_recovery_agent/maintenance_snapshot.py",
        "src/maintenance_shadow/__init__.py",
        "src/maintenance_shadow/snapshot.py",
        "src/maintenance_shadow/store.py",
        "contracts/maintenance/v2/audit_state_snapshot.v2.schema.json",
        "contracts/maintenance/v2/audit_event.v2.schema.json",
    ):
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, destination)

    old_cli = (old / "src/dell_recovery_agent/cli.py").read_text(encoding="utf-8")
    cli = old_cli.replace(
        "from dell_recovery_agent.execution import AgentService, FakePhysicalAdapter\n",
        "from dell_recovery_agent.execution import AgentService, FakePhysicalAdapter\n"
        "from dell_recovery_agent.maintenance_snapshot import ShadowMaintenanceSnapshotCache\n",
    )
    marker = "        authority_lease=authority_lease,\n"
    replacement = marker + (
        "        maintenance_snapshot_cache=(\n"
        "            ShadowMaintenanceSnapshotCache(\n"
        '                Path(config["maintenance_snapshot_cache"]),\n'
        '                Path(config["maintenance_snapshot_schema"]) if config.get("maintenance_snapshot_schema") else None,\n'
        "            )\n"
        '            if config.get("maintenance_snapshot_cache")\n'
        "            else None\n"
        "        ),\n"
    )
    if cli.count(marker) != 1:
        raise ValueError("old CLI server marker drift")
    (output / "src/dell_recovery_agent/cli.py").write_text(cli.replace(marker, replacement), encoding="utf-8")

    protected = (
        "src/dell_recovery_agent/execution.py",
        "src/dell_recovery_agent/authority.py",
        "src/dell_recovery_agent/reconciliation.py",
        "src/dell_recovery_agent/storage.py",
    )
    for relative in protected:
        if digest(output / relative) != digest(old / relative):
            raise ValueError(f"deadline-sensitive live-base drift: {relative}")
    if (output / "src/dell_recovery_agent/deadline.py").exists():
        raise ValueError("deadline guard must not be present in Evidence Binding-only release")
    print(f"output={output} protected_live_base_files={len(protected)} db_deadline_guard_present=false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
