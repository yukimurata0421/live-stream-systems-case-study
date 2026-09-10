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


def tree_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(value for value in root.rglob("*") if value.is_file()):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


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
        "src/snapshot_projection/__init__.py": args.recovery_root / "src/snapshot_projection/__init__.py",
        "src/snapshot_projection/model.py": args.recovery_root / "src/snapshot_projection/model.py",
        "src/stream_core/engine/ffmpeg_lifecycle.py": args.v3_root / "src/stream_core/engine/ffmpeg_lifecycle.py",
        "src/stream_core/engine/process_discovery.py": args.v3_root / "src/stream_core/engine/process_discovery.py",
        "src/stream_core/engine/rendering_boot.py": args.v3_root / "src/stream_core/engine/rendering_boot.py",
        "src/stream_core/stream_engine.py": args.v3_root / "src/stream_core/stream_engine.py",
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

    containerfile = args.release_dir / "Containerfile"
    copy_lines = [f"COPY overlay/app/{relative} /app/{relative}" for relative in expected_paths]
    containerfile.write_text(
        "\n".join(
            [
                f"FROM {args.base_image}",
                *copy_lines,
                "ENV MAINTENANCE_AUDIT_ENABLED=1 \\",
                "    MAINTENANCE_AUDIT_HOST_ID=dell-yuki \\",
                "    MAINTENANCE_AUDIT_BIND_SOURCE_TARGET=1 \\",
                "    MAINTENANCE_AUDIT_STATE_FILE=/projection/maintenance-snapshot.json \\",
                "    MAINTENANCE_AUDIT_PROJECTION_ENABLED=1 \\",
                "    MAINTENANCE_AUDIT_PROJECTION_PRODUCER_ID=dell-maintenance-snapshot-projector \\",
                "    MAINTENANCE_AUDIT_SOURCE_PRODUCER_ID=arena-maintenance-shadow \\",
                "    MAINTENANCE_AUDIT_PROJECTION_HIGH_WATER_FILE=/state/p1-audit/mp10/projection-high-water.json \\",
                "    MAINTENANCE_AUDIT_EVENT_FILE=/state/p1-audit/mp10/maintenance-audit.jsonl \\",
                "    MAINTENANCE_AUDIT_HEALTH_FILE=/state/p1-audit/mp10/maintenance-audit-health.json \\",
                "    MAINTENANCE_ENFORCEMENT_ENABLED=0 \\",
                f"    MAINTENANCE_RUNTIME_RELEASE_ID={args.release_id}",
                f'LABEL org.stream-v3.release-id="{args.release_id}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    schema_files = [
        args.recovery_root / "contracts/maintenance/v2/audit_event.v2.schema.json",
        args.recovery_root / "contracts/maintenance/v2/audit_state_snapshot.v2.schema.json",
        args.recovery_root / "contracts/maintenance/v2/snapshot_projection.v1.schema.json",
    ]
    manifest = {
        "schema_version": "recovery_control.mp10_candidate.v1",
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
        "containerfile_sha256": sha256(containerfile),
        "overlay_tree_sha256": tree_sha256(overlay_root),
        "enforcement_enabled": False,
        "production_branch_signal": None,
        "production_adapter_connected": False,
        "production_behavior_modified": False,
        "physical_effect_count": 0,
        "change_set_contamination": len(unexpected),
        "effect_boundaries": {
            "process_start": "audit immediately before subprocess.Popen",
            "process_term": "fail-isolated callback/audit immediately before terminate or SIGTERM",
            "process_kill": "fail-isolated callback/audit immediately before kill or SIGKILL",
        },
        "snapshot_projection": {
            "path": "/projection/maintenance-snapshot.json",
            "read_only_mount_required": True,
            "consumer_write_credential_required": False,
        },
        "in_flight_truth": "PROPOSED_NOT_DURABLE",
        "native_generation_mapping": "PROPOSED_RUN_ID_RESTART_COUNT_NOT_MAINTENANCE_GENERATION",
        "runtime_deployed": False,
        "production_deploy_approved": False,
        "release_gate_status": "READY_FOR_LOCAL_VALIDATION",
        "release_gate_blockers": [
            "CURRENT_KUBERNETES_TOPOLOGY_REQUIRES_WHOLE_POD_REPLACEMENT",
            "STREAM_ENGINE_REPLACEMENT_AND_FFMPEG_IDENTITY_CHANGE_NOT_AUTHORIZED",
            "READ_ONLY_PROJECTION_MOUNT_REQUIRES_POD_TEMPLATE_CHANGE",
        ],
    }
    (args.release_dir / "candidate_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
