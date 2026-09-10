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

COMMIT_ID = re.compile(r"^[0-9a-f]{40}$")
RELEASE_IDS = {
    "dell": re.compile(r"^dell-observation-[a-z0-9][a-z0-9.-]{7,119}$"),
    "arena": re.compile(r"^arena-cra-projection-[a-z0-9][a-z0-9.-]{7,119}$"),
}
INSTALL_ROOTS = {
    "dell": "/opt/stream-recovery-observation/releases",
    "arena": "/opt/stream-monitoring-cra-projection/releases",
}
COMMON_FILES = frozenset(
    {
        "constraints/runtime.txt",
        "constraints/runtime-wheels-cp314-linux-x86_64.json",
        "contracts/cra_dell_recovery/target_snapshot.v1.schema.json",
        "contracts/cra_dell_recovery/v1/observation_bundle.schema.json",
        "contracts/cra_no_action_soak/host_status.v1.schema.json",
        "contracts/cra_no_action_soak/resilient_host_status.v3.schema.json",
        "src/cra_dell_recovery/__init__.py",
        "src/cra_dell_recovery/bounded_http.py",
        "src/cra_dell_recovery/canonical.py",
        "src/cra_dell_recovery/json_input.py",
        "src/cra_dell_recovery/owner_diagnostics.py",
        "src/cra_dell_recovery/effect_scope.py",
        "src/cra_dell_recovery/errors.py",
        "src/cra_dell_recovery/models.py",
        "src/cra_dell_recovery/observation.py",
        "src/cra_dell_recovery/py.typed",
        "src/cra_dell_recovery/release_identity.py",
        "src/cra_dell_recovery/recovery_health.py",
        "src/cra_dell_recovery/reloading_tls_server.py",
        "src/cra_dell_recovery/schema.py",
        "src/cra_dell_recovery/time.py",
        "src/cra_dell_recovery/tls_credentials.py",
        "src/cra_dell_recovery/transport_resilience.py",
        "src/cra_dell_recovery/recovery_history.py",
        "src/cra_no_action_soak/__init__.py",
        "src/cra_no_action_soak/host_status.py",
        "src/cra_no_action_soak/recovery_facts.py",
        "src/cra_no_action_soak/recovery_observer.py",
        "src/cra_no_action_soak/recovery_transport.py",
        "src/cra_no_action_soak/resilient_host_status.py",
        "src/cra_no_action_soak/py.typed",
        "tools/install_immutable_runtime_release.py",
        "tools/build_recovery_soak_units.py",
    }
)
COMPONENT_FILES = {
    "dell": frozenset(
        {
            "ops/systemd/dell-observation-publisher.example.json",
            "ops/systemd/recovery-observer-dell.example.json",
            "ops/systemd/dell-observation-server.example.json",
            "ops/systemd/dell-observation-server@.service",
            "ops/systemd/dell-no-action-host-status-publisher.example.json",
            "ops/systemd/dell-no-action-host-status-publisher@.service",
            "ops/systemd/dell-no-action-host-status-publisher@.timer",
            "ops/systemd/dell-clock-status-probe@.service",
            "ops/systemd/dell-resilient-host-status-publisher.example.json",
            "ops/systemd/dell-resilient-host-status-publisher@.service",
            "ops/systemd/dell-resilient-host-status-publisher@.timer",
            "src/cra_no_action_soak/host_status_publisher.py",
            "src/cra_no_action_soak/clock_status_probe.py",
            "src/cra_no_action_soak/resilient_host_status_publisher.py",
            "src/dell_recovery_agent/__init__.py",
            "src/dell_recovery_agent/observation_publisher.py",
            "src/dell_recovery_agent/observation_server.py",
            "src/dell_recovery_agent/py.typed",
        }
    ),
    "arena": frozenset(
        {
            "contracts/monitoring_v4/cra_fact_bundle.v1.schema.json",
            "contracts/monitoring_v4/evidence_projection.v1.schema.json",
            "ops/postgresql/cra_projection_readonly.sql",
            "ops/systemd/dell-observation-pull.example.json",
            "ops/systemd/monitoring-v4-dell-host-status-pull.example.json",
            "ops/systemd/monitoring-live-adapter.example.json",
            "ops/systemd/monitoring-live-projection.example.json",
            "ops/systemd/monitoring-projection-server.example.json",
            "ops/systemd/monitoring-v4-cra-live-adapter@.service",
            "ops/systemd/monitoring-v4-cra-live-adapter@.timer",
            "ops/systemd/monitoring-v4-clock-status-probe@.service",
            "ops/systemd/monitoring-v4-cra-projection-server@.service",
            "ops/systemd/monitoring-v4-cra-projection@.service",
            "ops/systemd/monitoring-v4-cra-projection@.timer",
            "ops/systemd/monitoring-v4-dell-host-status-pull@.service",
            "ops/systemd/monitoring-v4-dell-host-status-pull@.timer",
            "ops/systemd/monitoring-v4-dell-resilient-status-pull.example.json",
            "ops/systemd/monitoring-v4-dell-resilient-status-pull@.service",
            "ops/systemd/monitoring-v4-dell-resilient-status-pull@.timer",
            "ops/systemd/monitoring-v4-dell-observation-pull@.service",
            "ops/systemd/monitoring-v4-dell-observation-pull@.timer",
            "ops/systemd/monitoring-v4-no-action-host-status-publisher.example.json",
            "ops/systemd/monitoring-v4-no-action-host-status-publisher@.service",
            "ops/systemd/monitoring-v4-no-action-host-status-publisher@.timer",
            "ops/systemd/monitoring-v4-resilient-status-publisher.example.json",
            "ops/systemd/monitoring-v4-resilient-status-publisher@.service",
            "ops/systemd/monitoring-v4-resilient-status-publisher@.timer",
            "src/cra_authority/__init__.py",
            "src/cra_authority/json_input.py",
            "src/cra_authority/monitoring_evidence.py",
            "src/cra_authority/py.typed",
            "src/monitoring_projection/__init__.py",
            "src/monitoring_projection/dell_observation_pull.py",
            "src/monitoring_projection/live_adapter.py",
            "src/monitoring_projection/producer.py",
            "src/monitoring_projection/py.typed",
            "src/monitoring_projection/server.py",
            "src/cra_no_action_soak/host_status_publisher.py",
            "src/cra_no_action_soak/host_status_pull.py",
            "src/cra_no_action_soak/clock_status_probe.py",
            "src/cra_no_action_soak/resilient_host_status_pull.py",
            "src/cra_no_action_soak/resilient_host_status_publisher.py",
            "src/cra_no_action_soak/resilient_host_status_relay.py",
        }
    ),
}
REQUIRED_FILES = {name: COMMON_FILES | files for name, files in COMPONENT_FILES.items()}
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
SECRET_VALUE = re.compile(rb"(?i)(?:password|token|secret)\s*=\s*(?!REDACTED\b|replace-with)[^\s\"']+")
PYPROJECTS = {
    "dell": b"""[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "stream-recovery-observation-dell"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "cryptography==46.0.7",
  "jsonschema==4.26.0",
  "rfc8785==0.1.4",
]

[project.scripts]
dell-observation-serve = "dell_recovery_agent.observation_server:main"
cra-no-action-host-status-publish = "cra_no_action_soak.host_status_publisher:main"
cra-resilient-host-status-publish = "cra_no_action_soak.resilient_host_status_publisher:main"
cra-recovery-observer = "cra_no_action_soak.recovery_observer:main"

[tool.setuptools.packages.find]
where = ["src"]
""",
    "arena": b"""[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "stream-monitoring-cra-projection"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "cryptography==46.0.7",
  "jsonschema==4.26.0",
  "psycopg[binary]==3.3.4",
  "rfc8785==0.1.4",
]

[project.scripts]
cra-dell-observation-pull = "monitoring_projection.dell_observation_pull:main"
cra-monitoring-live-adapt = "monitoring_projection.live_adapter:main"
cra-monitoring-project = "monitoring_projection.producer:main"
cra-monitoring-serve = "monitoring_projection.server:main"
cra-no-action-host-status-publish = "cra_no_action_soak.host_status_publisher:main"
cra-no-action-host-status-pull = "cra_no_action_soak.host_status_pull:main"
cra-resilient-host-status-pull = "cra_no_action_soak.resilient_host_status_pull:main"
cra-resilient-host-status-relay = "cra_no_action_soak.resilient_host_status_relay:main"
cra-recovery-observer = "cra_no_action_soak.recovery_observer:main"

[tool.setuptools.packages.find]
where = ["src"]
""",
}


def _git(source: Path, *arguments: str) -> bytes:
    return subprocess.run(["git", *arguments], cwd=source, check=True, capture_output=True).stdout


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


def build_release(source: Path, output: Path, release_id: str, component: str) -> dict[str, Any]:
    source = source.resolve()
    if component not in RELEASE_IDS:
        raise ValueError("OBSERVATION_RELEASE_COMPONENT_INVALID")
    if not RELEASE_IDS[component].fullmatch(release_id):
        raise ValueError("OBSERVATION_RELEASE_ID_INVALID")
    if output.exists() or output.with_suffix(output.suffix + ".manifest.json").exists():
        raise ValueError("OBSERVATION_RELEASE_OUTPUT_EXISTS")
    commit = _git(source, "rev-parse", "HEAD").decode().strip()
    if not COMMIT_ID.fullmatch(commit):
        raise ValueError("OBSERVATION_RELEASE_SOURCE_COMMIT_NOT_IMMUTABLE")
    if _git(source, "status", "--porcelain=v1", "--untracked-files=all"):
        raise ValueError("OBSERVATION_RELEASE_SOURCE_WORKTREE_NOT_CLEAN")
    tracked = {item.decode() for item in _git(source, "ls-files", "-z").split(b"\0") if item}
    for relative in tracked:
        if relative.lower().endswith(FORBIDDEN_SUFFIXES):
            raise ValueError(f"OBSERVATION_RELEASE_FORBIDDEN_FILE:{relative}")
    missing = sorted(REQUIRED_FILES[component] - tracked)
    if missing:
        raise ValueError(f"OBSERVATION_RELEASE_REQUIRED_FILES_MISSING:{','.join(missing)}")

    contents: dict[str, bytes] = {}
    files: dict[str, dict[str, Any]] = {}
    for relative in sorted(REQUIRED_FILES[component]):
        if relative.lower().endswith(FORBIDDEN_SUFFIXES):
            raise ValueError(f"OBSERVATION_RELEASE_FORBIDDEN_FILE:{relative}")
        path = source / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"OBSERVATION_RELEASE_NON_REGULAR_FILE:{relative}")
        data = path.read_bytes()
        secret_assignment = path.suffix != ".py" and SECRET_VALUE.search(data)
        private_marker = b"-----BEGIN " + b"PRIVATE KEY-----"
        if private_marker in data or secret_assignment:
            raise ValueError(f"OBSERVATION_RELEASE_SECRET_LIKE_VALUE:{relative}")
        contents[relative] = data
        files[relative] = {
            "sha256": _sha256(data),
            "size": len(data),
            "executable": bool(path.stat().st_mode & 0o111),
            "origin": "tracked_source",
        }
    contents["pyproject.toml"] = PYPROJECTS[component]
    files["pyproject.toml"] = {
        "sha256": _sha256(PYPROJECTS[component]),
        "size": len(PYPROJECTS[component]),
        "executable": False,
        "origin": "generated_component_manifest",
    }
    tree_sha256 = _sha256(b"".join(f"{relative}\0{files[relative]['sha256']}\0".encode() for relative in sorted(files)))
    manifest = {
        "schema": "cra.observation_plane_release_manifest.v1",
        "release_id": release_id,
        "source_commit": commit,
        "source_tree_sha256": tree_sha256,
        "component": component,
        "expected_install_root": f"{INSTALL_ROOTS[component]}/{release_id}",
        "production_action_enabled": False,
        "control_capability_count": 0,
        "physical_effect_count": 0,
        "command_route_packaged": False,
        "central_command_store_packaged": False,
        "private_key_packaged": False,
        "runtime_state_packaged": False,
        "dependency_constraints_packaged": True,
        "file_count": len(files),
        "files": files,
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
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                compressed.write(tar_buffer.getvalue())
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()
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
    parser = argparse.ArgumentParser(description="Build an immutable Dell or arena observation-plane release")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--component", choices=sorted(RELEASE_IDS), required=True)
    args = parser.parse_args()
    print(json.dumps(build_release(args.source, args.output, args.release_id, args.component), sort_keys=True))


if __name__ == "__main__":
    main()
