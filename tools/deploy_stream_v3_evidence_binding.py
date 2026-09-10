#!/usr/bin/env python3
"""Bind arena audit-only services to an immutable stream_v3 release."""

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
from pathlib import Path


def install_dropin(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write("\n".join(lines) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--stage", choices=("C", "D"), required=True)
    args = parser.parse_args()
    release = args.release.resolve()
    remote = release / "ops/scripts/stream_v3_remote_recovery.py"
    scoped = release / "ops/scripts/stream_v3_scoped_recovery.py"
    audit = release / "src/maintenance_audit/__init__.py"
    for required in (remote, scoped, audit):
        if not required.is_file():
            raise FileNotFoundError(required)
    common = [
        "Environment=MAINTENANCE_AUDIT_ENABLED=1",
        "Environment=MAINTENANCE_AUDIT_HOST_ID=arena-server",
        "Environment=MAINTENANCE_AUDIT_BIND_SOURCE_TARGET=1",
        "Environment=MAINTENANCE_AUDIT_STATE_FILE=/var/lib/stream-recovery-control/maintenance-audit-cache/maintenance-audit-state.json",
    ]
    if args.stage == "C":
        dropin = Path("/etc/systemd/system/stream-v3-remote-recovery.service.d/p1-audit-only.conf")
        install_dropin(
            dropin,
            [
                "[Service]",
                f"WorkingDirectory={release}",
                *common,
                "Environment=MAINTENANCE_AUDIT_EVENT_FILE=/var/lib/stream-v3/p1-audit/remote-recovery.jsonl",
                "Environment=MAINTENANCE_AUDIT_HEALTH_FILE=/var/lib/stream-v3/p1-audit/remote-recovery-health.json",
                f"Environment=STREAM_V3_SCOPED_RECOVERY_SCRIPT={scoped}",
                "ExecStart=",
                f"ExecStart=/usr/bin/python3 {remote}",
            ],
        )
        unit = "stream-v3-remote-recovery.service"
    else:
        dropin = Path("/etc/systemd/system/stream-v3-arena-monitor.service.d/p1-audit-only.conf")
        install_dropin(
            dropin,
            [
                "[Service]",
                *common,
                f"Environment=PYTHONPATH={release}/src",
                "Environment=MAINTENANCE_AUDIT_EVENT_FILE=/var/lib/stream-v3/p1-audit/arena-monitor.jsonl",
                "Environment=MAINTENANCE_AUDIT_HEALTH_FILE=/var/lib/stream-v3/p1-audit/arena-monitor-health.json",
            ],
        )
        unit = "stream-v3-arena-monitor.service"
    subprocess.run(("/usr/bin/systemctl", "daemon-reload"), check=True)
    service_restart_count = 0
    if args.stage == "D":
        subprocess.run(("/usr/bin/systemctl", "restart", unit), check=True)
        service_restart_count = 1
    print(
        f"stage={args.stage} unit={unit} new_release={release} "
        "shared_current_modified=false production_unit_configuration_modified=true "
        f"service_restart_count={service_restart_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
