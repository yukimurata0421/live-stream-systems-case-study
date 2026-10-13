#!/usr/bin/env python3
"""Build a minimal immutable TCP stall collector release tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from tcp_stall_collector.service import CONFIG_SCHEMA, load_config, strict_json_bytes

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_FILES = (
    Path("src/tcp_stall_collector/__init__.py"),
    Path("src/tcp_stall_collector/service.py"),
)
UNIT_FILE = Path("ops/systemd/stream-v3-tcp-stall-collector@.service")
CONFIG_TEMPLATE = Path("ops/systemd/tcp-stall-collector.example.json")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"


def _safe_release_id(value: str) -> str:
    if not value or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for character in value):
        raise ValueError("invalid release id")
    return value


def source_tree_sha256(source_root: Path = ROOT) -> str:
    digest = hashlib.sha256()
    for relative in (*RUNTIME_FILES, UNIT_FILE, CONFIG_TEMPLATE):
        raw = (source_root / relative).read_bytes()
        digest.update(str(relative).encode("utf-8") + b"\0" + hashlib.sha256(raw).digest())
    return digest.hexdigest()


def _git_commit(source_root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=source_root,
        text=True,
        capture_output=True,
        check=True,
    )
    value = completed.stdout.strip()
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("invalid source git commit")
    return value


def build_release(*, source_root: Path, staging_root: Path, release_id: str) -> dict[str, object]:
    release_id = _safe_release_id(release_id)
    release_dir = staging_root / "opt/stream-recovery-control/tcp-stall/releases" / release_id
    config_dir = staging_root / "etc/stream-recovery-control/tcp-stall/releases" / release_id
    unit_dir = staging_root / "etc/systemd/system"
    if release_dir.exists() or config_dir.exists():
        raise FileExistsError("release staging target already exists")
    release_dir.mkdir(parents=True, mode=0o755)
    config_dir.mkdir(parents=True, mode=0o750)
    unit_dir.mkdir(parents=True, mode=0o755, exist_ok=True)

    source_hashes: dict[str, str] = {}
    for relative in RUNTIME_FILES:
        raw = (source_root / relative).read_bytes()
        destination = release_dir / relative
        destination.parent.mkdir(parents=True, mode=0o755, exist_ok=True)
        destination.write_bytes(raw)
        os.chmod(destination, 0o644)
        source_hashes[str(relative)] = _sha256_bytes(raw)

    template_raw = (source_root / CONFIG_TEMPLATE).read_bytes()
    config = strict_json_bytes(template_raw)
    if not isinstance(config, dict) or config.get("schema") != CONFIG_SCHEMA:
        raise ValueError("invalid config template")
    config["release_id"] = release_id
    config_raw = _canonical(config)
    config_path = config_dir / "collector.json"
    config_path.write_bytes(config_raw)
    os.chmod(config_path, 0o640)
    loaded = load_config(config_path)
    if loaded.release_id != release_id:
        raise ValueError("generated config release mismatch")

    unit_raw = (source_root / UNIT_FILE).read_bytes()
    unit_path = unit_dir / UNIT_FILE.name
    unit_path.write_bytes(unit_raw)
    os.chmod(unit_path, 0o644)
    manifest: dict[str, object] = {
        "schema": "stream_v3.tcp_stall_collector_release.v1",
        "release_id": release_id,
        "created_at": datetime.now(tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source_git_commit": _git_commit(source_root),
        "source_tree_sha256": source_tree_sha256(source_root),
        "runtime_file_sha256": source_hashes,
        "config_sha256": _sha256_bytes(config_raw),
        "unit_sha256": _sha256_bytes(unit_raw),
    }
    manifest_path = release_dir / "release_manifest.json"
    manifest_path.write_bytes(_canonical(manifest))
    os.chmod(manifest_path, 0o644)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--print-source-sha256", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.print_source_sha256:
        print(source_tree_sha256(args.source_root))
        return 0
    manifest = build_release(source_root=args.source_root, staging_root=args.staging_root, release_id=args.release_id)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
