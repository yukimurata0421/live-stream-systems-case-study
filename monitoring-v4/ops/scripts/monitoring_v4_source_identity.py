#!/usr/bin/env python3
from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def included_paths(root: Path) -> tuple[Path, ...]:
    root = Path(root)
    paths = (
        root / ".dockerignore",
        root / "Dockerfile",
        root / "pyproject.toml",
        *sorted((root / "requirements").glob("*.lock")),
        *sorted((root / "src").rglob("*.py")),
        *sorted((root / "src").rglob("*.json")),
        *sorted((root / "deploy" / "k3s").glob("*.yaml")),
        root / "ops" / "scripts" / "monitoring_v4_bootstrap_db_secrets.py",
        root / "ops" / "scripts" / "monitoring_v4_bootstrap_youtube_api_secret.py",
        root / "ops" / "scripts" / "monitoring_v4_k3s_sentinel.py",
        root / "ops" / "scripts" / "monitoring_v4_source_identity.py",
        root / "ops" / "systemd" / "stream-monitoring-v4-k3s-sentinel.service",
        root / "ops" / "systemd" / "stream-monitoring-v4-k3s-sentinel.timer",
    )
    missing = [path.relative_to(root).as_posix() for path in paths if not path.is_file()]
    if missing:
        raise RuntimeError(f"source identity inputs are missing: {missing}")
    return tuple(paths)


def content_identity(root: Path = ROOT) -> str:
    root = Path(root)
    digest = hashlib.sha256()
    for path in included_paths(root):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()[:40]


def main() -> int:
    print(content_identity())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
