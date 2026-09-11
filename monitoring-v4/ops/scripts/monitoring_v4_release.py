#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


sys.dont_write_bytecode = True


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.release.cluster import validate_cluster_unchanged
from ops.release.model import existing_real_directory
from ops.release.service import prepare_release, promote_release
from ops.release.tree import records_digest, tree_records
from ops.scripts.monitoring_v4_source_identity import content_identity


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Prepare and atomically promote an immutable Monitoring v4 arena release"
    )
    subparsers = result.add_subparsers(dest="command", required=True)
    snapshot = subparsers.add_parser("snapshot")
    snapshot.add_argument("--source-root", type=Path, required=True)
    for name in ("prepare", "promote"):
        command = subparsers.add_parser(name)
        command.add_argument("--release-base", type=Path, required=True)
        command.add_argument("--release-name", required=True)
        command.add_argument("--expected-source-identity", required=True)
        command.add_argument("--expected-snapshot-sha256", required=True)
        command.add_argument("--app-image", required=True)
        command.add_argument("--app-image-id", required=True)
    prepare = subparsers.choices["prepare"]
    prepare.add_argument("--source-root", type=Path, required=True)
    promote = subparsers.choices["promote"]
    promote.add_argument("--expected-current-release", required=True)
    promote.add_argument("--kubectl", default="/usr/local/bin/k3s")
    promote.add_argument("--sudo", default="/usr/bin/sudo")
    return result


def main(arguments: list[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    if options.command == "snapshot":
        source = existing_real_directory(options.source_root, label="release source")
        records = tree_records(source, require_source_layout=True)
        payload = {
            "action": "snapshot",
            "source_identity": content_identity(source),
            "snapshot_sha256": records_digest(records),
            "file_count": len(records),
            "total_bytes": sum(record.size for record in records),
        }
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return 0
    common = {
        "release_base": options.release_base,
        "release_name": options.release_name,
        "expected_identity": options.expected_source_identity,
        "expected_snapshot_sha256": options.expected_snapshot_sha256,
        "app_image": options.app_image,
        "app_image_id": options.app_image_id,
    }
    if options.command == "prepare":
        result = prepare_release(source_root=options.source_root, **common)
        payload = {
            "action": "prepared",
            "release": os.fspath(result.release),
            "source_identity": result.source_identity,
            "snapshot_sha256": result.snapshot_sha256,
            "file_count": result.file_count,
            "total_bytes": result.total_bytes,
            "current_changed": False,
        }
    else:
        promoted = promote_release(
            expected_current_release=options.expected_current_release,
            cluster_validator=lambda rendered: validate_cluster_unchanged(
                rendered,
                kubectl=options.kubectl,
                sudo=options.sudo,
            ),
            **common,
        )
        payload = {
            "action": "promoted",
            "release": os.fspath(promoted),
            "source_identity": options.expected_source_identity,
            "cluster_diff": "zero",
            "current_changed": True,
        }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
