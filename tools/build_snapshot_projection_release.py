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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--install-path", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"release output already exists: {args.output}")
    sources = {
        "src/snapshot_projection/__init__.py": args.project_root / "src/snapshot_projection/__init__.py",
        "src/snapshot_projection/model.py": args.project_root / "src/snapshot_projection/model.py",
        "src/snapshot_projection/service.py": args.project_root / "src/snapshot_projection/service.py",
        "contracts/maintenance/v2/snapshot_projection.v1.schema.json": (
            args.project_root / "contracts/maintenance/v2/snapshot_projection.v1.schema.json"
        ),
    }
    for relative, source in sources.items():
        destination = args.output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    template = (args.project_root / "ops/systemd/maintenance-snapshot-projection-shadow.service.in").read_text(encoding="utf-8")
    unit = template.replace("@RELEASE_PATH@", str(args.install_path))
    unit_path = args.output / "ops/systemd/maintenance-snapshot-projection-shadow.service"
    unit_path.parent.mkdir(parents=True, exist_ok=True)
    unit_path.write_text(unit, encoding="utf-8")
    actual = sorted(str(path.relative_to(args.output)) for path in args.output.rglob("*") if path.is_file())
    expected = sorted([*sources, "ops/systemd/maintenance-snapshot-projection-shadow.service"])
    manifest = {
        "schema_version": "recovery_control.snapshot_projection_release.v1",
        "release_id": args.release_id,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "install_path": str(args.install_path),
        "expected_files": expected,
        "actual_files": actual,
        "unexpected_files": sorted(set(actual) - set(expected)),
        "missing_files": sorted(set(expected) - set(actual)),
        "file_hashes": {relative: sha256(args.output / relative) for relative in actual},
        "consumer_specific_release": True,
        "mutable_pointer_used": False,
        "production_behavior_modified": False,
        "physical_effect_count": 0,
    }
    (args.output / "release_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, sort_keys=True))
    return 0 if not manifest["unexpected_files"] and not manifest["missing_files"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
