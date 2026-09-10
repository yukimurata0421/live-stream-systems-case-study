from __future__ import annotations

import hashlib
import json
import os
import platform
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.controls.sqlite_runtime import loaded_sqlite_library

REQUIRED_MANIFEST_FIELDS = {
    "run_id",
    "started_at",
    "finished_at",
    "git",
    "runtime",
    "protocol_revision",
    "policy_revision",
    "ddl_sha256",
    "source_sha256",
    "fixture_sha256",
    "random_seed",
    "test_commands",
    "fake_adapter_confirmation",
    "production_credentials_absent_confirmation",
    "secret_values_recorded",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree_hash(root: Path, patterns: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    files = sorted({path for pattern in patterns for path in root.glob(pattern) if path.is_file()})
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _git(project_root: Path) -> dict[str, Any]:
    def run(*args: str) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=project_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else ""

    status = "\n".join(
        line
        for line in run("status", "--porcelain=v1", "--untracked-files=all").splitlines()
        if not line.removeprefix("?? ").startswith("artifacts/")
    )
    commit = run("rev-parse", "HEAD") or None
    diff = run("diff", "--binary", "--no-ext-diff", "HEAD") if commit else ""
    workspace_digest = hashlib.sha256()
    for name in run("ls-files", "-co", "--exclude-standard").splitlines():
        path = project_root / name
        if not path.is_file() or name.startswith(("artifacts/", ".venv/", ".pytest_cache/")):
            continue
        workspace_digest.update(name.encode())
        workspace_digest.update(b"\0")
        workspace_digest.update(path.read_bytes())
        workspace_digest.update(b"\0")
    identity = hashlib.sha256((diff + "\n" + status + "\n" + workspace_digest.hexdigest()).encode()).hexdigest()
    return {
        "commit": commit,
        "unborn": commit is None,
        "dirty": bool(status),
        "diff_status_sha256": identity,
        "workspace_content_sha256": workspace_digest.hexdigest(),
    }


def build_manifest(project_root: Path, run_id: str, random_seed: int, test_commands: list[str]) -> dict[str, Any]:
    os_release: dict[str, str] = {}
    release_path = Path("/etc/os-release")
    if release_path.exists():
        for line in release_path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator:
                os_release[key] = value.strip('"')
    credential_names = {
        "CRA_TLS_CLIENT_KEY",
        "CRA_TLS_SERVER_KEY",
        "CRA_SIGNING_PRIVATE_KEY",
        "DELL_AGENT_PRIVATE_KEY",
        "STREAM_RECOVERY_PRODUCTION_CREDENTIAL",
    }
    present_names = sorted(credential_names & set(os.environ))
    ddl = {path.relative_to(project_root).as_posix(): _sha256(path) for path in sorted((project_root / "migrations").glob("**/*.sql"))}
    fixture_paths = sorted((project_root / "harness/scenarios").glob("*.json"))
    fixture_paths.extend(sorted((project_root / "tests/fixtures/regressions").glob("*.json")))
    fixture_paths.append(project_root / "tests/fixtures/2026-08-21_replay.json")
    fixtures = {path.relative_to(project_root).as_posix(): _sha256(path) for path in sorted(set(fixture_paths))}
    recovery_schema = project_root / "contracts/monitoring_v4/recovery_verification.v1.schema.json"
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "started_at": isoformat_utc(utc_now()),
        "finished_at": None,
        "git": _git(project_root),
        "runtime": {
            "python": platform.python_version(),
            "python_executable": str(Path(sys.executable).resolve()),
            "sqlite": sqlite3.sqlite_version,
            "sqlite_library": loaded_sqlite_library(),
            "kernel": platform.release(),
            "os": os_release.get("PRETTY_NAME", platform.system()),
            "architecture": platform.machine(),
        },
        "protocol_revision": "cra_dell_recovery.v1",
        "policy_revision": "shadow-policy-v1",
        "ddl_sha256": ddl,
        "source_sha256": {
            "CRA": _tree_hash(project_root, ("src/cra_authority/**/*.py", "src/cra_dell_recovery/**/*.py")),
            "Dell": _tree_hash(project_root, ("src/dell_recovery_agent/**/*.py",)),
            "Harness": _tree_hash(project_root, ("src/cra_harness/**/*.py", "harness/scenarios/*.json")),
        },
        "fixture_sha256": fixtures,
        "recovery_verification_schema_sha256": _sha256(recovery_schema) if recovery_schema.exists() else None,
        "random_seed": random_seed,
        "test_commands": list(test_commands),
        "fake_adapter_confirmation": True,
        "production_credentials_absent_confirmation": not present_names,
        "production_credential_variable_names_present": present_names,
        "production_credential_count": len(present_names),
        "runtime_mode": "ISOLATED_FAKE_NO_ACTION",
        "secret_values_recorded": False,
    }
    manifest["complete"] = manifest_complete(manifest)
    return manifest


def manifest_complete(manifest: dict[str, Any]) -> bool:
    if not set(manifest) >= REQUIRED_MANIFEST_FIELDS:
        return False
    if not all(manifest[field] is not None for field in REQUIRED_MANIFEST_FIELDS - {"finished_at"}):
        return False
    git = manifest.get("git")
    if not isinstance(git, dict):
        return False
    commit = git.get("commit")
    unborn = git.get("unborn")
    git_identity_valid = (commit is None and unborn is True) or (isinstance(commit, str) and len(commit) == 40 and unborn is False)
    return bool(
        git_identity_valid
        and manifest["fake_adapter_confirmation"] is True
        and manifest["production_credentials_absent_confirmation"] is True
        and manifest["secret_values_recorded"] is False
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
