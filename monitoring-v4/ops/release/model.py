from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path


MAX_FILES = 10_000
MAX_DIRECTORIES = 2_000
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_TREE_BYTES = 512 * 1024 * 1024
MAX_PATH_BYTES = 4096
FILE_MODE = 0o444
DIRECTORY_MODE = 0o555
LOCK_MODE = 0o600
RELEASE_NAME = re.compile(r"^k3s-postgres-([0-9a-f]{40})-r([1-9][0-9]*)$")
IDENTITY = re.compile(r"^[0-9a-f]{40}$")
IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
SNAPSHOT_SHA256 = re.compile(r"^[0-9a-f]{64}$")
GENERATED_DIRECTORY_NAMES = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".nox",
        ".pytest_cache",
        ".ruff_cache",
        ".state",
        ".tox",
        ".venv",
        "__pycache__",
        "node_modules",
        "venv",
    }
)
GENERATED_FILE_NAMES = frozenset({".coverage", ".DS_Store", "coverage.xml"})
GENERATED_FILE_SUFFIXES = (".pyc", ".pyo")
ALLOWED_TOP_LEVEL = frozenset(
    {
        ".dockerignore",
        ".github",
        ".gitignore",
        "AGENTS.md",
        "Dockerfile",
        "LICENSE",
        "NOTICE",
        "README.md",
        "config",
        "deploy",
        "docs",
        "ops",
        "pyproject.toml",
        "requirements",
        "src",
        "tests",
    }
)
REQUIRED_PATHS = (
    ".dockerignore",
    "Dockerfile",
    "pyproject.toml",
    "deploy/k3s/kustomization.yaml",
    "ops/scripts/monitoring_v4_source_identity.py",
    "src/stream_monitoring_v4/__init__.py",
)


@dataclass(frozen=True)
class TreeRecord:
    relative: str
    size: int
    digest: bytes


@dataclass(frozen=True)
class ReleaseResult:
    release: Path
    source_identity: str
    snapshot_sha256: str
    file_count: int
    total_bytes: int


def plain_absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def existing_real_directory(path: Path, *, label: str) -> Path:
    absolute = plain_absolute(path)
    try:
        metadata = os.lstat(absolute)
    except OSError as exc:
        raise RuntimeError(f"{label} is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"{label} must be a non-symlink directory")
    resolved = absolute.resolve(strict=True)
    if resolved != absolute:
        raise RuntimeError(f"{label} or one of its parents must not be a symlink")
    return absolute


def validate_release_name(name: str, expected_identity: str) -> None:
    match = RELEASE_NAME.fullmatch(name)
    if match is None or match.group(1) != expected_identity:
        raise RuntimeError("release name must contain the exact source identity and revision")


def validate_render_values(
    expected_identity: str,
    app_image: str,
    app_image_id: str,
) -> None:
    if IDENTITY.fullmatch(expected_identity) is None:
        raise RuntimeError("expected source identity must be 40 lowercase hex characters")
    if app_image != f"stream-monitoring-v4:{expected_identity}":
        raise RuntimeError("application image tag must contain the exact source identity")
    if IMAGE_ID.fullmatch(app_image_id) is None:
        raise RuntimeError("application image ID must be a sha256 digest")


def validate_snapshot_sha256(value: str) -> None:
    if SNAPSHOT_SHA256.fullmatch(value) is None:
        raise RuntimeError("expected snapshot SHA-256 must be 64 lowercase hex characters")
