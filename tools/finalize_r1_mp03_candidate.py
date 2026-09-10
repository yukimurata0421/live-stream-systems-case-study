#!/usr/bin/env python3
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def tree_hash(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(value for value in root.rglob("*") if value.is_file()):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--release-gate-status", required=True)
    parser.add_argument("--blocker", action="append", default=[])
    parser.add_argument("--running-fast-recovery", type=Path)
    args = parser.parse_args()
    manifest_path = args.release_dir / "candidate_manifest.json"
    manifest: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
    inspection = subprocess.run(
        ["docker", "image", "inspect", args.image],
        check=True,
        capture_output=True,
        text=True,
    )
    image = json.loads(inspection.stdout)[0]
    running_diff: dict[str, Any] | None = None
    if args.running_fast_recovery is not None:
        candidate_source = args.release_dir / "overlay/app/src/watchers/fast_recovery.py"
        diff_lines = list(
            difflib.unified_diff(
                args.running_fast_recovery.read_text(encoding="utf-8").splitlines(keepends=True),
                candidate_source.read_text(encoding="utf-8").splitlines(keepends=True),
                fromfile="running:/app/src/watchers/fast_recovery.py",
                tofile=f"candidate:{manifest['release_id']}/app/src/watchers/fast_recovery.py",
            )
        )
        diff_path = args.release_dir / "running_to_candidate_fast_recovery.diff"
        diff_path.write_text("".join(diff_lines), encoding="utf-8")
        running_diff = {
            "artifact": str(diff_path),
            "sha256": hashlib.sha256(diff_path.read_bytes()).hexdigest(),
            "added_lines": sum(line.startswith("+") and not line.startswith("+++") for line in diff_lines),
            "removed_lines": sum(line.startswith("-") and not line.startswith("---") for line in diff_lines),
        }
    python_result = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--entrypoint", "python3", args.image, "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    manifest.update(
        {
            "finalized_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "image": args.image,
            "image_id": image["Id"],
            "image_repo_digests": image.get("RepoDigests") or [],
            "overlay_tree_sha256": tree_hash(args.release_dir / "overlay"),
            "runtime_contract": {
                "container": "fast-recovery-loop",
                "command": ["/bin/sh", "-lc"],
                "args": ["PYTHONPATH=/app/src python3 -m stream_v3.control_loop --mode streaming"],
                "state_mount": "/state",
                "image_pull_policy": "IfNotPresent",
            },
            "dependency_manifest": {
                "python": (python_result.stdout or python_result.stderr).strip(),
                "base_image": manifest["base_image"],
                "base_image_digest": manifest["base_image_digest"],
                "network_required_for_candidate_test": False,
            },
            "release_gate_status": args.release_gate_status,
            "release_gate_blockers": args.blocker,
            "runtime_deployed": False,
            "k3s_image_staged": True,
            "running_to_candidate_diff": running_diff,
        }
    )
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
