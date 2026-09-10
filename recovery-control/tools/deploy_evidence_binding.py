#!/usr/bin/env python3
"""Deploy audit-only Evidence Binding stages without production mutation access."""

from __future__ import annotations

import argparse
import grp
import json
import os
import pwd
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

# The maintenance fact crosses arena -> Dell -> arena -> CRA.  Twenty seconds
# leaves a bounded reserve for the measured serialized hop latency and scheduler
# jitter while the one-second producer cadence still detects a stopped source.
MAINTENANCE_EVIDENCE_TTL_SECONDS = 20.0


def run(*command: str) -> None:
    subprocess.run(command, check=True)


def atomic_json_update(path: Path, updates: dict[str, Any]) -> None:
    original = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(original, dict):
        raise ValueError(f"configuration is not an object: {path}")
    original.update(updates)
    stat = path.stat()
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(original, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, stat.st_mode & 0o777)
    os.chown(temporary, stat.st_uid, stat.st_gid)
    os.replace(temporary, path)


def atomic_json_install(path: Path, payload: dict[str, Any], *, mode: int = 0o640) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, mode)
    os.replace(temporary, path)


def switch_release(release: Path) -> str:
    if not release.is_dir():
        raise FileNotFoundError(release)
    link = Path("/opt/stream-recovery-control")
    old = str(link.resolve()) if link.exists() else "MISSING"
    temporary = link.with_name(f".{link.name}.evidence-binding.tmp")
    if temporary.exists() or temporary.is_symlink():
        temporary.unlink()
    temporary.symlink_to(release)
    os.replace(temporary, link)
    return old


def install_unit(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    os.chmod(destination, 0o644)


def ensure_maintenance_user() -> None:
    try:
        pwd.getpwnam("maintenance-shadow")
        return
    except KeyError:
        pass
    group = grp.getgrnam("stream-recovery")
    run(
        "/usr/sbin/useradd",
        "--system",
        "--no-create-home",
        "--home-dir",
        "/nonexistent",
        "--shell",
        "/usr/sbin/nologin",
        "--gid",
        str(group.gr_gid),
        "maintenance-shadow",
    )


def stage_a_arena(release: Path) -> dict[str, Any]:
    ensure_maintenance_user()
    maintenance_user = pwd.getpwnam("maintenance-shadow")
    recovery_group = grp.getgrnam("stream-recovery")
    evidence_dir = Path("/var/lib/stream-recovery-control/cra/maintenance-evidence")
    shadow_dir = Path("/var/lib/stream-recovery-control/maintenance-shadow")
    audit_cache_dir = Path("/var/lib/stream-recovery-control/maintenance-audit-cache")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    audit_cache_dir.mkdir(parents=True, exist_ok=True)
    os.chown(evidence_dir, pwd.getpwnam("stream-recovery").pw_uid, recovery_group.gr_gid)
    os.chown(shadow_dir, maintenance_user.pw_uid, recovery_group.gr_gid)
    os.chown(audit_cache_dir, maintenance_user.pw_uid, grp.getgrnam("yuki").gr_gid)
    os.chmod(evidence_dir, 0o750)
    os.chmod(shadow_dir, 0o750)
    os.chmod(audit_cache_dir, 0o750)
    old_release = switch_release(release)
    config = {
        "database": "/var/lib/stream-recovery-control/maintenance-shadow/maintenance-shadow.db",
        "migration": "/opt/stream-recovery-control/migrations/maintenance_shadow/001_initial.sql",
        "producer_id": "arena-maintenance-shadow",
        "target_snapshot": "/var/lib/stream-recovery-control/cra/maintenance-evidence/target_snapshot.json",
        "authority_projection": "/var/lib/stream-recovery-control/cra/maintenance-evidence/authority_projection.json",
        "output_snapshot": "/var/lib/stream-recovery-control/maintenance-audit-cache/maintenance-audit-state.json",
        "ttl_seconds": MAINTENANCE_EVIDENCE_TTL_SECONDS,
        "interval_seconds": 1.0,
    }
    shadow_config = Path("/etc/stream-recovery-control/maintenance-shadow.json")
    atomic_json_install(shadow_config, config)
    os.chown(shadow_config, 0, recovery_group.gr_gid)
    atomic_json_update(
        Path("/etc/stream-recovery-control/cra-shadow.json"),
        {
            "target_snapshot_cache": config["target_snapshot"],
            "authority_projection": config["authority_projection"],
            "maintenance_snapshot_source": config["output_snapshot"],
        },
    )
    install_unit(
        release / "ops/systemd/maintenance-coordinator-shadow.service",
        Path("/etc/systemd/system/maintenance-coordinator-shadow.service"),
    )
    cra_projection_dropin = Path("/etc/systemd/system/cra-authority-shadow.service.d/evidence-binding-projection.conf")
    cra_projection_dropin.parent.mkdir(parents=True, exist_ok=True)
    install_unit(release / "ops/systemd/evidence-binding-cra-projection.conf", cra_projection_dropin)
    for projection in evidence_dir.glob("*.json"):
        os.chmod(projection, 0o640)
    run("/usr/bin/systemctl", "daemon-reload")
    run("/usr/bin/systemctl", "enable", "--now", "maintenance-coordinator-shadow.service")
    run("/usr/bin/systemctl", "restart", "maintenance-coordinator-shadow.service")
    run("/usr/bin/systemctl", "restart", "cra-authority-shadow.service")
    return {"role": "arena", "stage": "A", "old_release": old_release, "new_release": str(release)}


def stage_a_dell(release: Path) -> dict[str, Any]:
    old_release = switch_release(release)
    atomic_json_update(
        Path("/etc/stream-recovery-control/dell-shadow.json"),
        {
            "maintenance_snapshot_cache": "/var/lib/stream-recovery-control/dell/p1-live/maintenance-audit-state.json",
            "maintenance_snapshot_schema": "/opt/stream-recovery-control/contracts/maintenance/v2/audit_state_snapshot.v2.schema.json",
        },
    )
    run("/usr/bin/systemctl", "restart", "dell-recovery-agent-shadow.service")
    return {"role": "dell", "stage": "A", "old_release": old_release, "new_release": str(release)}


def stage_b(role: str, release: Path) -> dict[str, Any]:
    if role == "arena":
        unit = "cra-authority-shadow.service"
        source = release / "ops/systemd/p1-audit-cra-shadow.conf"
        destination = Path("/etc/systemd/system/cra-authority-shadow.service.d/p1-audit-only.conf")
    else:
        unit = "dell-recovery-agent-shadow.service"
        source = release / "ops/systemd/p1-audit-dell-shadow.conf"
        destination = Path("/etc/systemd/system/dell-recovery-agent-shadow.service.d/p1-audit-only.conf")
    destination.parent.mkdir(parents=True, exist_ok=True)
    install_unit(source, destination)
    run("/usr/bin/systemctl", "daemon-reload")
    run("/usr/bin/systemctl", "restart", unit)
    return {"role": role, "stage": "B", "release": str(release), "unit": unit}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=("arena", "dell"), required=True)
    parser.add_argument("--stage", choices=("A", "B"), required=True)
    parser.add_argument("--release", type=Path, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise PermissionError("deployment must run as root")
    result = (
        stage_b(args.role, args.release)
        if args.stage == "B"
        else (stage_a_arena(args.release) if args.role == "arena" else stage_a_dell(args.release))
    )
    print(json.dumps({**result, "production_behavior_modified": False, "physical_effect_count": 0}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
