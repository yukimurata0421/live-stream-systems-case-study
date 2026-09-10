"""Build only the four already committed OAuth observation fixes on a live base."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

COMMIT = "20d0c7b653c670528a835f8932405cbeee38f799"
FILES = (
    "watchers/youtube_watchdog.py",
    "watchers/youtube_watchdog_core/cache.py",
    "watchers/youtube_watchdog_core/status.py",
    "watchers/youtube_monitor/stats_writer.py",
)


def build(base: Path, repository: Path, output: Path) -> dict[str, object]:
    patch = subprocess.run(
        ["git", "show", "--format=", COMMIT, "--", *["src/" + p for p in FILES]],
        cwd=repository,
        capture_output=True,
        check=True,
        timeout=10,
    ).stdout
    if not patch:
        raise ValueError("OAUTH_OVERLAY_PATCH_MISSING")
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    before = {}
    for name in FILES:
        source = base / name
        if not source.is_file() or source.is_symlink():
            raise ValueError("OAUTH_OVERLAY_BASE_INVALID")
        raw = source.read_bytes()
        before[name] = hashlib.sha256(raw).hexdigest()
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        target.chmod(0o644)
    subprocess.run(["git", "apply", "--check", "-p2", "-"], cwd=output, input=patch, check=True, timeout=10)
    subprocess.run(["git", "apply", "-p2", "-"], cwd=output, input=patch, check=True, timeout=10)
    after = {}
    for name in FILES:
        raw = (output / name).read_bytes()
        compile(raw, name, "exec")
        after[name] = hashlib.sha256(raw).hexdigest()
    manifest: dict[str, object] = {
        "schema": "recovery.oauth_observation_overlay.v1",
        "source_commit": COMMIT,
        "base_files": before,
        "candidate_files": after,
        "patch_sha256": hashlib.sha256(patch).hexdigest(),
        "mode": "BUILD_ONLY",
        "control_policy_changed": False,
    }
    (output / "overlay-manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-src", type=Path, required=True)
    parser.add_argument("--v3-repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    print(json.dumps(build(args.base_src, args.v3_repository, args.output), sort_keys=True))


if __name__ == "__main__":
    main()
