#!/usr/bin/env python3
"""Build the DB deadline shadow change set from an Evidence Binding-only base."""

from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-base", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    base = args.evidence_base.resolve()
    source = args.source.resolve()
    output = args.output
    if output.exists():
        raise FileExistsError(output)
    shutil.copytree(base, output, symlinks=True)
    deadline_files = (
        "src/cra_dell_recovery/errors.py",
        "src/dell_recovery_agent/deadline.py",
        "src/dell_recovery_agent/execution.py",
        "src/dell_recovery_agent/authority.py",
        "src/dell_recovery_agent/reconciliation.py",
        "src/dell_recovery_agent/storage.py",
    )
    for relative in deadline_files:
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, destination)

    cli_path = output / "src/dell_recovery_agent/cli.py"
    cli = cli_path.read_text(encoding="utf-8")
    service_marker = "        FakePhysicalAdapter(),\n"
    lease_marker = '        lease_ttl_seconds=float(config.get("lease_ttl_seconds", 15.0)),\n'
    if cli.count(service_marker) != 1 or cli.count(lease_marker) != 1:
        raise ValueError("Evidence Binding CLI marker drift")
    cli = cli.replace(
        service_marker,
        service_marker + '        critical_db_deadline_seconds=float(config.get("critical_db_deadline_seconds", 3.0)),\n',
    )
    cli = cli.replace(
        lease_marker,
        lease_marker + '        critical_db_deadline_seconds=float(config.get("critical_db_deadline_seconds", 3.0)),\n',
    )
    cli_path.write_text(cli, encoding="utf-8")

    evidence_only = (
        "src/maintenance_audit/__init__.py",
        "src/dell_recovery_agent/server.py",
        "src/dell_recovery_agent/maintenance_snapshot.py",
    )
    for relative in evidence_only:
        if digest(output / relative) != digest(base / relative):
            raise ValueError(f"Evidence Binding base drift: {relative}")
    for relative in deadline_files:
        if digest(output / relative) != digest(source / relative):
            raise ValueError(f"deadline overlay copy failed: {relative}")
    print(
        f"output={output} deadline_files={len(deadline_files)} evidence_files_preserved={len(evidence_only)} "
        "physical_adapter=FAKE_COUNTER_ONLY"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
