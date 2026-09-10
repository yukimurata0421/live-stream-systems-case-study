from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
from pathlib import Path
from typing import Any

RELEASE_ID = re.compile(r"^cra-no-action-[a-z0-9][a-z0-9.-]{7,119}$")
COMMIT_ID = re.compile(r"^[0-9a-f]{40}$")
RECOVERY_SOAK_FILES = frozenset(
    {"ops/systemd/cra-recovery-soak.example.json"}
    | {
        f"ops/systemd/{name}@.{kind}"
        for name in ("cra-recovery-soak", "cra-recovery-soak-gate", "cra-recovery-soak-watchdog")
        for kind in ("service", "timer")
    }
)
ALLOWED_ROOT_FILES = frozenset({"README.md"})
ALLOWED_FILES = frozenset(
    {
        "src/cra_dell_recovery/recovery_history.py",
        "constraints/runtime.txt",
        "constraints/runtime-wheels-cp314-linux-x86_64.json",
        "constraints/sqlite-runtime.json",
        "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        "contracts/cra_no_action_soak/host_status.v1.schema.json",
        "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json",
        "ops/systemd/cra-arena-host-status-pull.example.json",
        "ops/systemd/cra-arena-host-status-pull@.service",
        "ops/systemd/cra-arena-host-status-pull@.timer",
        "ops/systemd/cra-arena-resilient-status-pull.example.json",
        "ops/systemd/cra-arena-resilient-status-pull@.service",
        "ops/systemd/cra-arena-resilient-status-pull@.timer",
        "ops/systemd/cra-monitoring-projection-pull@.service",
        "ops/systemd/cra-monitoring-projection-pull@.timer",
        "ops/systemd/cra-monitoring-pull.example.json",
        "ops/systemd/cra-provisioning.example.json",
        "ops/systemd/cra-runtime-no-action@.service",
        "ops/systemd/cra-runtime.example.json",
        "ops/systemd/cra-no-action-soak-local-collector@.service",
        "ops/systemd/cra-no-action-soak-local-collector@.timer",
        "ops/systemd/cra-no-action-soak-local-profile.example.json",
        "ops/systemd/cra-resilient-status-soak.example.json",
        "ops/systemd/cra-resilient-status-soak@.service",
        "ops/systemd/cra-resilient-status-soak@.timer",
        "ops/systemd/cra-resilient-status-soak-gate@.service",
        "ops/systemd/cra-resilient-status-soak-gate@.timer",
        "ops/systemd/cra-resilient-status-soak-watchdog@.service",
        "ops/systemd/cra-resilient-status-soak-watchdog@.timer",
        "src/cra_authority/__init__.py",
        "src/cra_authority/authorizer.py",
        "src/cra_authority/json_input.py",
        "src/cra_dell_recovery/json_input.py",
        "src/cra_dell_recovery/owner_diagnostics.py",
        "src/cra_authority/monitoring_evidence.py",
        "src/cra_authority/no_action_store.py",
        "src/cra_authority/recovery_safety.py",
        "src/cra_authority/projection_pull.py",
        "src/cra_authority/provision.py",
        "src/cra_authority/py.typed",
        "src/cra_authority/retention.py",
        "src/cra_authority/runtime.py",
        "src/cra_authority/schema_contract.py",
        "src/cra_authority/verifier.py",
        "src/cra_no_action_soak/__init__.py",
        "src/cra_no_action_soak/gate.py",
        "src/cra_no_action_soak/host_status.py",
        "src/cra_no_action_soak/host_status_pull.py",
        "src/cra_no_action_soak/operator_status.py",
        "src/cra_no_action_soak/parity.py",
        "src/cra_no_action_soak/resilient_host_status.py",
        "src/cra_no_action_soak/resilient_host_status_pull.py",
        "src/cra_no_action_soak/resilient_soak.py",
        "src/cra_no_action_soak/recovery_facts.py",
        "src/cra_no_action_soak/recovery_live.py",
        "src/cra_no_action_soak/recovery_observer.py",
        "src/cra_no_action_soak/recovery_transport.py",
        "src/cra_no_action_soak/recovery_soak.py",
        "src/cra_no_action_soak/recovery_window.py",
        "src/cra_no_action_soak/py.typed",
        "src/cra_no_action_soak/sample.py",
        "src/cra_no_action_soak/time.py",
        "tools/install_immutable_runtime_release.py",
        "tools/build_recovery_soak_units.py",
        "tools/freeze_recovery_soak_checkpoint.py",
        "tools/collect_cra_no_action_soak.py",
    }
)
ALLOWED_PREFIXES = (
    "migrations/central/",
    "src/cra_dell_recovery/",
    "src/maintenance_audit/",
    "tools/sqlite_runtime/",
)
ALLOWED_FILES |= RECOVERY_SOAK_FILES
REQUIRED_FILES = frozenset(
    {
        "src/cra_dell_recovery/recovery_history.py",
        "constraints/runtime.txt",
        "constraints/runtime-wheels-cp314-linux-x86_64.json",
        "constraints/sqlite-runtime.json",
        "pyproject.toml",
        "src/cra_authority/authorizer.py",
        "src/cra_authority/json_input.py",
        "src/cra_dell_recovery/json_input.py",
        "src/cra_dell_recovery/owner_diagnostics.py",
        "src/cra_authority/monitoring_evidence.py",
        "src/cra_authority/no_action_store.py",
        "src/cra_authority/recovery_safety.py",
        "src/cra_authority/provision.py",
        "src/cra_authority/runtime.py",
        "src/cra_authority/schema_contract.py",
        "src/cra_authority/projection_pull.py",
        "src/cra_no_action_soak/gate.py",
        "src/cra_no_action_soak/host_status.py",
        "src/cra_no_action_soak/host_status_pull.py",
        "src/cra_no_action_soak/operator_status.py",
        "src/cra_no_action_soak/resilient_host_status.py",
        "src/cra_no_action_soak/resilient_host_status_pull.py",
        "src/cra_no_action_soak/resilient_soak.py",
        "src/cra_no_action_soak/recovery_facts.py",
        "src/cra_no_action_soak/recovery_live.py",
        "src/cra_no_action_soak/recovery_observer.py",
        "src/cra_no_action_soak/recovery_soak.py",
        "src/cra_no_action_soak/recovery_window.py",
        "src/cra_no_action_soak/parity.py",
        "src/cra_no_action_soak/sample.py",
        "src/cra_no_action_soak/time.py",
        "src/cra_dell_recovery/sqlite.py",
        "src/maintenance_audit/__init__.py",
        "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json",
        "migrations/central/001_initial.sql",
        "ops/systemd/cra-runtime-no-action@.service",
        "ops/systemd/cra-arena-host-status-pull@.service",
        "ops/systemd/cra-arena-resilient-status-pull@.service",
        "ops/systemd/cra-resilient-status-soak@.service",
        "ops/systemd/cra-resilient-status-soak-gate@.service",
        "ops/systemd/cra-resilient-status-soak-watchdog@.service",
        "ops/systemd/cra-no-action-soak-local-collector@.service",
        "tools/collect_cra_no_action_soak.py",
        "tools/install_immutable_runtime_release.py",
    }
)
REQUIRED_FILES |= RECOVERY_SOAK_FILES
REQUIRED_FILES |= {"tools/freeze_recovery_soak_checkpoint.py"}
FORBIDDEN_SUFFIXES = (
    ".db",
    ".key",
    ".log",
    ".pem",
    ".sqlite",
    ".sqlite3",
    ".sqlite3-shm",
    ".sqlite3-wal",
)
SECRET_VALUE = re.compile(rb"(?i)(?:password|token|secret|private[_-]?key)\s*=\s*(?!REDACTED\b|replace-with)[^\s\"']+")
COMPONENT_PYPROJECT = b"""[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "stream-recovery-control-cra-no-action"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "cryptography==46.0.7",
  "jsonschema==4.26.0",
  "rfc8785==0.1.4",
]

[project.scripts]
cra-runtime = "cra_authority.runtime:main"
cra-provision = "cra_authority.provision:main"
cra-monitoring-pull = "cra_authority.projection_pull:main"
cra-no-action-soak-gate = "cra_no_action_soak.gate:main"
cra-no-action-soak-sample = "cra_no_action_soak.sample:main"
cra-no-action-host-status-pull = "cra_no_action_soak.host_status_pull:main"
cra-resilient-host-status-pull = "cra_no_action_soak.resilient_host_status_pull:main"
cra-resilient-status-soak = "cra_no_action_soak.resilient_soak:main"
cra-recovery-soak = "cra_no_action_soak.recovery_soak:main"
cra-recovery-observer = "cra_no_action_soak.recovery_observer:main"

[tool.setuptools.packages.find]
where = ["src"]
"""


def _git(source: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", *arguments],
        cwd=source,
        check=True,
        capture_output=True,
    ).stdout


def _tracked_files(source: Path) -> list[str]:
    return sorted(item.decode() for item in _git(source, "ls-files", "-z").split(b"\0") if item)


def _allowed(relative: str) -> bool:
    return relative in ALLOWED_ROOT_FILES or relative in ALLOWED_FILES or relative.startswith(ALLOWED_PREFIXES)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _manifest_bytes(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _tar_info(name: str, data: bytes, *, executable: bool = False) -> tuple[tarfile.TarInfo, io.BytesIO]:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = 0o755 if executable else 0o644
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    return info, io.BytesIO(data)


def build_release(source: Path, output: Path, release_id: str) -> dict[str, Any]:
    source = source.resolve()
    if not RELEASE_ID.fullmatch(release_id):
        raise ValueError("RELEASE_ID_INVALID")
    commit = _git(source, "rev-parse", "HEAD").decode().strip()
    if not COMMIT_ID.fullmatch(commit):
        raise ValueError("SOURCE_COMMIT_NOT_IMMUTABLE")
    if _git(source, "status", "--porcelain=v1", "--untracked-files=all"):
        raise ValueError("SOURCE_WORKTREE_NOT_CLEAN")
    tracked = _tracked_files(source)
    for relative in tracked:
        if relative.lower().endswith(FORBIDDEN_SUFFIXES):
            raise ValueError(f"RELEASE_FORBIDDEN_FILE:{relative}")
    selected = [relative for relative in tracked if _allowed(relative)]
    missing = sorted(REQUIRED_FILES - set(tracked))
    if missing:
        raise ValueError(f"RELEASE_REQUIRED_FILES_MISSING:{','.join(missing)}")
    files: dict[str, dict[str, Any]] = {}
    contents: dict[str, bytes] = {}
    for relative in selected:
        path = source / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"RELEASE_NON_REGULAR_FILE:{relative}")
        data = path.read_bytes()
        secret_assignment = path.suffix != ".py" and SECRET_VALUE.search(data)
        private_marker = b"-----BEGIN " + b"PRIVATE KEY-----"
        if private_marker in data or secret_assignment:
            raise ValueError(f"RELEASE_SECRET_LIKE_VALUE:{relative}")
        contents[relative] = data
        files[relative] = {
            "sha256": _sha256(data),
            "size": len(data),
            "executable": bool(path.stat().st_mode & 0o111),
            "origin": "tracked_source",
        }
    contents["pyproject.toml"] = COMPONENT_PYPROJECT
    files["pyproject.toml"] = {
        "sha256": _sha256(COMPONENT_PYPROJECT),
        "size": len(COMPONENT_PYPROJECT),
        "executable": False,
        "origin": "generated_component_manifest",
    }
    tree_sha256 = _sha256(b"".join(f"{relative}\0{files[relative]['sha256']}\0".encode() for relative in sorted(files)))
    manifest = {
        "schema": "cra.no_action_release_manifest.v1",
        "release_id": release_id,
        "systemd_instance": release_id,
        "expected_install_root": f"/opt/stream-recovery-control/releases/{release_id}",
        "source_commit": commit,
        "source_tree_sha256": tree_sha256,
        "operating_mode": "NO_ACTION",
        "component": "cra-authority",
        "production_action_enabled": False,
        "control_capability_count": 0,
        "physical_effect_count": 0,
        "central_command_store_packaged": False,
        "effect_adapter_packaged": False,
        "dell_agent_packaged": False,
        "monitoring_private_key_packaged": False,
        "file_count": len(files),
        "files": files,
        "excluded_runtime_state": True,
        "secret_values_recorded": False,
    }
    manifest_data = _manifest_bytes(manifest)
    prefix = f"stream-recovery-control-{release_id}"
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for relative in sorted(contents):
            info, payload = _tar_info(
                f"{prefix}/{relative}",
                contents[relative],
                executable=bool(files[relative]["executable"]),
            )
            archive.addfile(info, payload)
        info, payload = _tar_info(f"{prefix}/release_manifest.json", manifest_data)
        archive.addfile(info, payload)
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            compressed.write(tar_buffer.getvalue())
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(temporary, output)
    os.chmod(output, 0o600)
    manifest_path = output.with_suffix(output.suffix + ".manifest.json")
    manifest_path.write_bytes(manifest_data)
    os.chmod(manifest_path, 0o600)
    return {
        **manifest,
        "archive_sha256": _sha256(output.read_bytes()),
        "archive_path": str(output),
        "manifest_path": str(manifest_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a clean immutable CRA NO_ACTION release archive")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    args = parser.parse_args()
    print(json.dumps(build_release(args.source, args.output, args.release_id), sort_keys=True))


if __name__ == "__main__":
    main()
