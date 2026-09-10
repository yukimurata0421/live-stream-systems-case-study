#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--v3-root", type=Path, required=True)
    parser.add_argument("--recovery-root", type=Path, required=True)
    parser.add_argument("--base-image", required=True)
    parser.add_argument("--base-image-digest", required=True)
    parser.add_argument("--rollback-image", required=True)
    args = parser.parse_args()

    expected = {
        "src/maintenance_audit/__init__.py": args.v3_root / "src/maintenance_audit/__init__.py",
        "src/maintenance_enforcement/__init__.py": args.v3_root / "src/maintenance_enforcement/__init__.py",
        "src/maintenance_enforcement/model.py": args.v3_root / "src/maintenance_enforcement/model.py",
        "src/watchers/fast_recovery.py": args.v3_root / "src/watchers/fast_recovery.py",
    }
    overlay_root = args.release_dir / "overlay/app"
    if args.release_dir.exists():
        raise SystemExit(f"release directory already exists: {args.release_dir}")
    for relative, source in expected.items():
        if not source.is_file():
            raise SystemExit(f"missing candidate source: {source}")
        destination = overlay_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    actual = sorted(str(path.relative_to(overlay_root)) for path in overlay_root.rglob("*") if path.is_file())
    expected_paths = sorted(expected)
    unexpected = sorted(set(actual) - set(expected_paths))
    missing = sorted(set(expected_paths) - set(actual))
    if unexpected or missing:
        raise SystemExit(f"candidate contamination: unexpected={unexpected} missing={missing}")

    dockerfile = args.release_dir / "Containerfile"
    dockerfile.write_text(
        "\n".join(
            [
                f"FROM {args.base_image}",
                "COPY overlay/app/src/maintenance_audit/__init__.py /app/src/maintenance_audit/__init__.py",
                "COPY overlay/app/src/maintenance_enforcement/__init__.py /app/src/maintenance_enforcement/__init__.py",
                "COPY overlay/app/src/maintenance_enforcement/model.py /app/src/maintenance_enforcement/model.py",
                "COPY overlay/app/src/watchers/fast_recovery.py /app/src/watchers/fast_recovery.py",
                "ENV MAINTENANCE_AUDIT_ENABLED=1 \\",
                "    MAINTENANCE_AUDIT_HOST_ID=dell-yuki \\",
                "    MAINTENANCE_AUDIT_BIND_SOURCE_TARGET=1 \\",
                "    MAINTENANCE_AUDIT_STATE_FILE=/state/p1-audit/mp03/maintenance-audit-state.json \\",
                "    MAINTENANCE_AUDIT_EVENT_FILE=/state/p1-audit/mp03/maintenance-audit.jsonl \\",
                "    MAINTENANCE_AUDIT_HEALTH_FILE=/state/p1-audit/mp03/maintenance-audit-health.json \\",
                "    MAINTENANCE_ENFORCEMENT_ENABLED=0 \\",
                f"    MAINTENANCE_RUNTIME_RELEASE_ID={args.release_id}",
                f'LABEL org.stream-v3.release-id="{args.release_id}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    schema_files = [
        args.recovery_root / "contracts/maintenance/v2/audit_event.schema.json",
        args.recovery_root / "contracts/maintenance/v2/audit_event.v2.schema.json",
        args.recovery_root / "contracts/maintenance/v2/audit_state_snapshot.v2.schema.json",
    ]
    manifest = {
        "schema_version": "recovery_control.r1_mp03_candidate.v1",
        "release_id": args.release_id,
        "created_at": utc_now(),
        "base_image": args.base_image,
        "base_image_digest": args.base_image_digest,
        "rollback_image": args.rollback_image,
        "expected_files": expected_paths,
        "actual_files": actual,
        "unexpected_files": unexpected,
        "missing_files": missing,
        "source_files": {
            relative: {"source": str(expected[relative]), "sha256": sha256(overlay_root / relative)} for relative in expected_paths
        },
        "contract_hashes": {str(path): sha256(path) for path in schema_files},
        "containerfile_sha256": sha256(dockerfile),
        "enforcement_enabled": False,
        "production_adapter_connected": False,
        "production_behavior_modified": False,
        "physical_effect_count": 0,
        "change_set_contamination": len(unexpected),
    }
    (args.release_dir / "candidate_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
