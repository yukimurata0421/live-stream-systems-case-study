#!/usr/bin/env python3
"""Build an immutable, read-only RuntimeIdentity snapshot producer release."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def copy_file(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise SystemExit(f"missing candidate dependency: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def tree_manifest(root: Path) -> dict[str, dict[str, Any]]:
    return {
        str(path.relative_to(root)): {"sha256": sha256(path), "size": path.stat().st_size}
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--recovery-root", type=Path, required=True)
    parser.add_argument("--current-runtime", type=Path, required=True)
    args = parser.parse_args()
    if args.release_dir.exists():
        raise SystemExit(f"release already exists: {args.release_dir}")

    current_runtime = args.current_runtime.resolve()
    release_root = args.release_dir / "root"
    sources = {
        "src/cra_dell_recovery/__init__.py": args.recovery_root / "src/cra_dell_recovery/__init__.py",
        "src/cra_dell_recovery/canonical.py": args.recovery_root / "src/cra_dell_recovery/canonical.py",
        "src/cra_dell_recovery/errors.py": args.recovery_root / "src/cra_dell_recovery/errors.py",
        "src/cra_dell_recovery/models.py": args.recovery_root / "src/cra_dell_recovery/models.py",
        "src/cra_dell_recovery/schema.py": args.recovery_root / "src/cra_dell_recovery/schema.py",
        "src/cra_dell_recovery/time.py": args.recovery_root / "src/cra_dell_recovery/time.py",
        "src/dell_recovery_agent/__init__.py": args.recovery_root / "src/dell_recovery_agent/__init__.py",
        "src/dell_recovery_agent/target_snapshot.py": args.recovery_root / "src/dell_recovery_agent/target_snapshot.py",
        "tools/sqlite_runtime/run-fixed.sh": current_runtime / "tools/sqlite_runtime/run-fixed.sh",
        ".runtime/sqlite-3.51.3/install/lib/libsqlite3.so.0": current_runtime / ".runtime/sqlite-3.51.3/install/lib/libsqlite3.so.0",
        "vendor/rfc8785/__init__.py": current_runtime / "vendor/rfc8785/__init__.py",
        "vendor/rfc8785/_impl.py": current_runtime / "vendor/rfc8785/_impl.py",
        "vendor/rfc8785/py.typed": current_runtime / "vendor/rfc8785/py.typed",
    }
    for relative, source in sources.items():
        copy_file(source, release_root / relative)

    target_source = sources["src/dell_recovery_agent/target_snapshot.py"]
    forbidden_tokens = (
        "os.kill(",
        ".terminate(",
        "rollout restart",
        "kubectl delete",
        "kubectl patch",
        "kubectl scale",
        "systemctl",
    )
    scanned_source_text = "\n".join(
        source.read_text(encoding="utf-8") for relative, source in sources.items() if relative.endswith(".py") or relative.endswith(".sh")
    )
    present_forbidden = [token for token in forbidden_tokens if token in scanned_source_text]

    opt_path = f"/opt/{args.release_id}"
    exec_start = " ".join(
        (
            f"{opt_path}/tools/sqlite_runtime/run-fixed.sh",
            "/usr/bin/python3",
            "-m dell_recovery_agent.target_snapshot",
            "--output /var/lib/stream-recovery-control/dell/target_snapshot.json",
            "--journal /var/lib/stream-recovery-control/dell/target_snapshots.jsonl",
            "--host-id dell-yuki",
            "--namespace stream-v3",
            "--label-selector app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
            "--container-name stream-engine",
            "--kubeconfig /etc/stream-recovery-control/dell-target-observer.kubeconfig",
            "--ttl-seconds 10",
            "--interval-seconds 5",
        )
    )
    unit = f"""
[Unit]
Description=Dell stream-v3 read-only RuntimeIdentity and FFmpeg target snapshot producer
After=network-online.target k3s.service
Wants=network-online.target
ConditionPathExists=/etc/stream-recovery-control/dell-target-observer.kubeconfig

[Service]
Type=simple
User=stream-recovery
Group=stream-recovery
Environment=PYTHONPATH={opt_path}/src:{opt_path}/vendor
ExecStart={exec_start}
Restart=on-failure
RestartSec=5s
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadOnlyPaths=/proc /etc/stream-recovery-control {opt_path}
ReadWritePaths=/var/lib/stream-recovery-control/dell
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
CapabilityBoundingSet=
LockPersonality=true
"""
    write_text(args.release_dir / "systemd/dell-target-snapshot-shadow.service", unit)

    root_files = tree_manifest(release_root)
    expected_files = sorted(sources)
    actual_files = sorted(root_files)
    previous_target = current_runtime / "src/dell_recovery_agent/target_snapshot.py"
    manifest = {
        "schema_version": "runtime_identity.shadow_release.v1",
        "release_id": args.release_id,
        "created_at": utc_text(),
        "release_path": opt_path,
        "current_runtime_path": str(current_runtime),
        "rollback_unit_source_path": "/opt/stream-recovery-control",
        "expected_files": expected_files,
        "actual_files": actual_files,
        "unexpected_files": sorted(set(actual_files) - set(expected_files)),
        "missing_files": sorted(set(expected_files) - set(actual_files)),
        "files": root_files,
        "candidate_target_snapshot_sha256": sha256(release_root / "src/dell_recovery_agent/target_snapshot.py"),
        "local_verified_source_sha256": sha256(target_source),
        "previous_target_snapshot_sha256": sha256(previous_target),
        "source_identity_match": sha256(release_root / "src/dell_recovery_agent/target_snapshot.py") == sha256(target_source),
        "target_snapshot_change_expected": sha256(target_source) != sha256(previous_target),
        "physical_mutation_tokens_present": present_forbidden,
        "kubernetes_api_scope": "pods GET only",
        "credential_scope_change": False,
        "rbac_change": False,
        "maintenance_enforcement_enabled": False,
        "production_behavior_modified": False,
        "complete": not present_forbidden
        and actual_files == expected_files
        and sha256(release_root / "src/dell_recovery_agent/target_snapshot.py") == sha256(target_source),
    }
    write_text(args.release_dir / "release_manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
    print(json.dumps(manifest, sort_keys=True))
    return 0 if manifest["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
