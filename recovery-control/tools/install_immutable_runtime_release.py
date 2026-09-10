from __future__ import annotations

import argparse
import grp
import hashlib
import io
import json
import os
import platform
import pwd
import re
import shutil
import stat
import subprocess
import sys
import sysconfig
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any

BUNDLE_SCHEMA = "cra.immutable_runtime_bundle.v1"
HOST_CONTRACT = Path("/etc/stream-recovery-control/host-contract.json")
INSTALL_ROOTS = {
    "cra": Path("/opt/stream-recovery-control/releases"),
    "dell": Path("/opt/stream-recovery-observation/releases"),
    "arena": Path("/opt/stream-monitoring-cra-projection/releases"),
}
RUNTIME_ROOTS = {
    "cra": Path("/opt/stream-recovery-control/runtimes"),
    "dell": Path("/opt/stream-recovery-observation/runtimes"),
    "arena": Path("/opt/stream-monitoring-cra-projection/runtimes"),
}
EXPECTED_HOST_IDS = {
    "cra": "cra-01-central-authority",
    "dell": "dell-stream-runtime",
    "arena": "arena-monitoring-facts",
}
RUNTIME_STATE_LAYOUTS = {
    "cra": (
        Path("stream-recovery-control/monitoring-inbox/releases"),
        Path("stream-recovery-control/cra/releases"),
    ),
    "dell": (Path("stream-recovery-observation/releases"),),
    "arena": (
        Path("stream-monitoring-v4/cra-dell-observation/releases"),
        Path("stream-monitoring-v4/cra-facts/releases"),
        Path("stream-monitoring-v4/cra-projection/releases"),
        Path("stream-monitoring-v4/cra-target-inbox/releases"),
    ),
}
RUNTIME_STATE_ACCOUNTS = {
    "cra": ("stream-recovery", "stream-recovery"),
    "dell": ("stream-recovery-observer", "stream-recovery"),
    "arena": ("stream-monitoring-projection", "stream-monitoring-projection"),
}
RELEASE_SCHEMAS = {
    "cra": ("cra.no_action_release_manifest.v1", "cra-authority"),
    "dell": ("cra.observation_plane_release_manifest.v1", "dell"),
    "arena": ("cra.observation_plane_release_manifest.v1", "arena"),
}
IMPORTS = {
    "cra": (
        "cra_authority.runtime",
        "cra_authority.provision",
        "cra_authority.projection_pull",
        "cra_no_action_soak.gate",
        "cra_no_action_soak.host_status",
        "cra_no_action_soak.host_status_pull",
        "cra_no_action_soak.resilient_host_status",
        "cra_no_action_soak.resilient_host_status_pull",
        "cra_no_action_soak.resilient_soak",
        "cra_no_action_soak.sample",
    ),
    "dell": (
        "cra_dell_recovery.observation",
        "cra_no_action_soak.clock_status_probe",
        "cra_no_action_soak.host_status",
        "cra_no_action_soak.host_status_publisher",
        "cra_no_action_soak.resilient_host_status",
        "cra_no_action_soak.resilient_host_status_publisher",
        "dell_recovery_agent.observation_server",
    ),
    "arena": (
        "monitoring_projection.dell_observation_pull",
        "monitoring_projection.live_adapter",
        "monitoring_projection.producer",
        "monitoring_projection.server",
        "cra_no_action_soak.host_status",
        "cra_no_action_soak.host_status_publisher",
        "cra_no_action_soak.host_status_pull",
        "cra_no_action_soak.clock_status_probe",
        "cra_no_action_soak.resilient_host_status",
        "cra_no_action_soak.resilient_host_status_pull",
        "cra_no_action_soak.resilient_host_status_publisher",
        "cra_no_action_soak.resilient_host_status_relay",
    ),
}
CLI_MODULES = {
    "cra": (
        "cra_authority.runtime",
        "cra_authority.provision",
        "cra_authority.projection_pull",
        "cra_no_action_soak.gate",
        "cra_no_action_soak.sample",
        "cra_no_action_soak.host_status_pull",
        "cra_no_action_soak.resilient_host_status_pull",
        "cra_no_action_soak.resilient_soak",
    ),
    "dell": (
        "dell_recovery_agent.observation_server",
        "cra_no_action_soak.clock_status_probe",
        "cra_no_action_soak.host_status_publisher",
        "cra_no_action_soak.resilient_host_status_publisher",
    ),
    "arena": (
        "monitoring_projection.dell_observation_pull",
        "monitoring_projection.live_adapter",
        "monitoring_projection.producer",
        "monitoring_projection.server",
        "cra_no_action_soak.host_status_publisher",
        "cra_no_action_soak.host_status_pull",
        "cra_no_action_soak.clock_status_probe",
        "cra_no_action_soak.resilient_host_status_pull",
        "cra_no_action_soak.resilient_host_status_relay",
    ),
}
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SAFE_RELEASE_ID = re.compile(r"^[a-z0-9][a-z0-9.-]{7,127}$")
MAX_BUNDLE_BYTES = 256 * 1024 * 1024
MAX_EXPANDED_BYTES = 384 * 1024 * 1024


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _secure_read(path: Path, *, maximum_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("IMMUTABLE_INSTALL_INPUT_NOT_REGULAR")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("IMMUTABLE_INSTALL_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size < 1 or metadata.st_size > maximum_bytes:
            raise ValueError("IMMUTABLE_INSTALL_INPUT_SIZE_INVALID")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            data = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(data) > maximum_bytes:
        raise ValueError("IMMUTABLE_INSTALL_INPUT_TOO_LARGE")
    return data


def _archive_contents(data: bytes, *, minimum_parts: int = 2) -> tuple[str, dict[str, bytes]]:
    contents: dict[str, bytes] = {}
    total = 0
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            for member in archive.getmembers():
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or len(path.parts) < minimum_parts:
                    raise ValueError("IMMUTABLE_INSTALL_ARCHIVE_PATH_INVALID")
                if member.name in contents or not member.isfile() or member.size < 0:
                    raise ValueError("IMMUTABLE_INSTALL_ARCHIVE_MEMBER_INVALID")
                total += member.size
                if total > MAX_EXPANDED_BYTES:
                    raise ValueError("IMMUTABLE_INSTALL_ARCHIVE_EXPANDED_TOO_LARGE")
                handle = archive.extractfile(member)
                if handle is None:
                    raise ValueError("IMMUTABLE_INSTALL_ARCHIVE_MEMBER_UNREADABLE")
                contents[member.name] = handle.read()
    except tarfile.TarError as error:
        raise ValueError("IMMUTABLE_INSTALL_ARCHIVE_INVALID") from error
    roots = {PurePosixPath(name).parts[0] for name in contents}
    if len(roots) != 1:
        raise ValueError("IMMUTABLE_INSTALL_ARCHIVE_ROOT_NOT_EXACT")
    return roots.pop(), contents


def _runtime_platform() -> dict[str, Any]:
    libc_name, libc_version = platform.libc_ver()
    return {
        "python_implementation": sys.implementation.name,
        "python_version": platform.python_version(),
        "python_major_minor": f"{sys.version_info.major}.{sys.version_info.minor}",
        "python_soabi": sysconfig.get_config_var("SOABI"),
        "machine": platform.machine(),
        "libc": libc_name,
        "libc_version": libc_version,
    }


def _verify_source_release(component: str, data: bytes, release_id: str) -> tuple[dict[str, Any], dict[str, bytes]]:
    root, contents = _archive_contents(data)
    manifest_name = f"{root}/release_manifest.json"
    if manifest_name not in contents:
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_MANIFEST_MISSING")
    manifest = json.loads(contents[manifest_name])
    if not isinstance(manifest, dict):
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_MANIFEST_INVALID")
    expected_schema, expected_component = RELEASE_SCHEMAS[component]
    if manifest.get("schema") != expected_schema or manifest.get("component") != expected_component:
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_COMPONENT_MISMATCH")
    if manifest.get("release_id") != release_id:
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_ID_MISMATCH")
    if manifest.get("expected_install_root") != f"{INSTALL_ROOTS[component]}/{release_id}":
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_ROOT_MISMATCH")
    if manifest.get("production_action_enabled") is not False:
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_ACTION_ENABLED")
    if type(manifest.get("control_capability_count")) is not int or manifest["control_capability_count"] != 0:
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_CONTROL_CAPABILITY_PRESENT")
    if type(manifest.get("physical_effect_count")) is not int or manifest["physical_effect_count"] != 0:
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_PHYSICAL_EFFECT_PRESENT")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_FILES_INVALID")
    relative_contents = {str(PurePosixPath(name).relative_to(root)): value for name, value in contents.items() if name != manifest_name}
    if set(relative_contents) != set(files):
        raise ValueError("IMMUTABLE_INSTALL_RELEASE_FILE_SET_MISMATCH")
    for relative, identity in files.items():
        if not isinstance(identity, dict):
            raise ValueError("IMMUTABLE_INSTALL_RELEASE_FILE_IDENTITY_INVALID")
        value = relative_contents[relative]
        if identity.get("sha256") != _sha256(value) or identity.get("size") != len(value):
            raise ValueError(f"IMMUTABLE_INSTALL_RELEASE_FILE_HASH_MISMATCH:{relative}")
    return manifest, relative_contents


def verify_bundle(
    bundle_path: Path,
    expected_bundle_sha256: str,
    *,
    host_contract_path: Path = HOST_CONTRACT,
    _installer_path: Path | None = None,
) -> dict[str, Any]:
    if not SHA256.fullmatch(expected_bundle_sha256):
        raise ValueError("IMMUTABLE_INSTALL_EXPECTED_SHA256_INVALID")
    bundle_data = _secure_read(bundle_path, maximum_bytes=MAX_BUNDLE_BYTES)
    if _sha256(bundle_data) != expected_bundle_sha256:
        raise ValueError("IMMUTABLE_INSTALL_BUNDLE_SHA256_MISMATCH")
    root, contents = _archive_contents(bundle_data)
    manifest_name = f"{root}/runtime_bundle_manifest.json"
    if manifest_name not in contents:
        raise ValueError("IMMUTABLE_INSTALL_BUNDLE_MANIFEST_MISSING")
    manifest = json.loads(contents[manifest_name])
    if not isinstance(manifest, dict) or manifest.get("schema") != BUNDLE_SCHEMA:
        raise ValueError("IMMUTABLE_INSTALL_BUNDLE_MANIFEST_INVALID")
    component = manifest.get("component")
    release_id = manifest.get("release_id")
    if component not in INSTALL_ROOTS or not isinstance(release_id, str) or not SAFE_RELEASE_ID.fullmatch(release_id):
        raise ValueError("IMMUTABLE_INSTALL_BUNDLE_IDENTITY_INVALID")
    if manifest.get("expected_host_id") != EXPECTED_HOST_IDS[component]:
        raise ValueError("IMMUTABLE_INSTALL_EXPECTED_HOST_ID_INVALID")
    if manifest.get("expected_install_root") != f"{INSTALL_ROOTS[component]}/{release_id}":
        raise ValueError("IMMUTABLE_INSTALL_EXPECTED_RELEASE_ROOT_INVALID")
    if manifest.get("expected_runtime_root") != f"{RUNTIME_ROOTS[component]}/{release_id}":
        raise ValueError("IMMUTABLE_INSTALL_EXPECTED_RUNTIME_ROOT_INVALID")
    if manifest.get("runtime_platform") != _runtime_platform():
        raise ValueError("IMMUTABLE_INSTALL_RUNTIME_PLATFORM_MISMATCH")
    if (
        manifest.get("production_action_enabled") is not False
        or type(manifest.get("control_capability_count")) is not int
        or manifest["control_capability_count"] != 0
        or type(manifest.get("physical_effect_count")) is not int
        or manifest["physical_effect_count"] != 0
        or manifest.get("starts_service") is not False
        or manifest.get("installs_config") is not False
        or manifest.get("installs_credential") is not False
    ):
        raise ValueError("IMMUTABLE_INSTALL_BUNDLE_CAPABILITY_UNSAFE")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError("IMMUTABLE_INSTALL_BUNDLE_FILES_INVALID")
    relative_contents = {str(PurePosixPath(name).relative_to(root)): value for name, value in contents.items() if name != manifest_name}
    if set(relative_contents) != set(files) or manifest.get("file_count") != len(files):
        raise ValueError("IMMUTABLE_INSTALL_BUNDLE_FILE_SET_MISMATCH")
    for relative, identity in files.items():
        value = relative_contents.get(relative)
        if not isinstance(identity, dict) or value is None:
            raise ValueError("IMMUTABLE_INSTALL_BUNDLE_FILE_IDENTITY_INVALID")
        if identity.get("sha256") != _sha256(value) or identity.get("size") != len(value):
            raise ValueError(f"IMMUTABLE_INSTALL_BUNDLE_FILE_HASH_MISMATCH:{relative}")
    release_data = relative_contents.get("source-release.tar.gz")
    if release_data is None or manifest.get("source_release_archive_sha256") != _sha256(release_data):
        raise ValueError("IMMUTABLE_INSTALL_SOURCE_RELEASE_ARCHIVE_MISMATCH")
    release_manifest, release_files = _verify_source_release(component, release_data, release_id)
    if manifest.get("source_release_manifest_sha256") != _sha256(_json_bytes(release_manifest)):
        raise ValueError("IMMUTABLE_INSTALL_SOURCE_RELEASE_MANIFEST_MISMATCH")
    if manifest.get("source_commit") != release_manifest.get("source_commit"):
        raise ValueError("IMMUTABLE_INSTALL_SOURCE_COMMIT_MISMATCH")
    if manifest.get("source_tree_sha256") != release_manifest.get("source_tree_sha256"):
        raise ValueError("IMMUTABLE_INSTALL_SOURCE_TREE_MISMATCH")
    wheel_lock = release_manifest["files"].get("constraints/runtime-wheels-cp314-linux-x86_64.json")
    if not isinstance(wheel_lock, dict) or manifest.get("wheel_lock_sha256") != wheel_lock.get("sha256"):
        raise ValueError("IMMUTABLE_INSTALL_WHEEL_LOCK_MISMATCH")
    installer_hash = _sha256(_secure_read(_installer_path or Path(__file__), maximum_bytes=1024 * 1024))
    if manifest.get("installer_sha256") != installer_hash:
        raise ValueError("IMMUTABLE_INSTALL_INSTALLER_HASH_MISMATCH")
    host_contract = json.loads(_secure_read(host_contract_path, maximum_bytes=64 * 1024))
    if not isinstance(host_contract, dict) or host_contract.get("host_id") != manifest["expected_host_id"]:
        raise ValueError("IMMUTABLE_INSTALL_HOST_CONTRACT_MISMATCH")
    wheel_files = sorted(relative for relative in relative_contents if relative.startswith("wheelhouse/"))
    wheels = manifest.get("wheels")
    if not isinstance(wheels, list) or {f"wheelhouse/{item.get('filename')}" for item in wheels} != set(wheel_files):
        raise ValueError("IMMUTABLE_INSTALL_WHEEL_SET_MISMATCH")
    for item in wheels:
        if not isinstance(item, dict):
            raise ValueError("IMMUTABLE_INSTALL_WHEEL_IDENTITY_INVALID")
        filename = item.get("filename")
        if not isinstance(filename, str) or Path(filename).name != filename or not filename.endswith(".whl"):
            raise ValueError("IMMUTABLE_INSTALL_WHEEL_FILENAME_INVALID")
        relative = f"wheelhouse/{filename}"
        value = relative_contents[relative]
        if item.get("sha256") != _sha256(value) or item.get("size") != len(value):
            raise ValueError("IMMUTABLE_INSTALL_WHEEL_HASH_MISMATCH")
    sqlite_identity = manifest.get("sqlite")
    if component == "cra":
        if not isinstance(sqlite_identity, dict):
            raise ValueError("IMMUTABLE_INSTALL_SQLITE_IDENTITY_MISSING")
        sqlite_relative = f"sqlite/{sqlite_identity.get('filename')}"
        sqlite_data = relative_contents.get(sqlite_relative)
        if sqlite_data is None or sqlite_identity.get("sha256") != _sha256(sqlite_data):
            raise ValueError("IMMUTABLE_INSTALL_SQLITE_HASH_MISMATCH")
    elif sqlite_identity is not None:
        raise ValueError("IMMUTABLE_INSTALL_SQLITE_UNEXPECTED")
    return {
        "manifest": manifest,
        "bundle_sha256": expected_bundle_sha256,
        "payloads": relative_contents,
        "release_manifest": release_manifest,
        "release_files": release_files,
    }


def _ensure_directory(path: Path, *, owner_uid: int) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            current.mkdir(mode=0o755)
            metadata = current.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise ValueError("IMMUTABLE_INSTALL_PARENT_NOT_DIRECTORY")
        if metadata.st_uid not in {0, owner_uid} or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("IMMUTABLE_INSTALL_PARENT_PERMISSIONS_UNSAFE")


def _write_file(path: Path, data: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _runtime_state_identity(component: str, *, testing: bool) -> tuple[int, int]:
    if testing:
        return os.geteuid(), os.getegid()
    account = RUNTIME_STATE_ACCOUNTS.get(component)
    if account is None:
        return 0, 0
    try:
        user = pwd.getpwnam(account[0])
        group = grp.getgrnam(account[1])
    except KeyError as error:
        raise ValueError(f"IMMUTABLE_INSTALL_RUNTIME_STATE_ACCOUNT_MISSING:{component.upper()}") from error
    return user.pw_uid, group.gr_gid


def _ensure_directory_beneath(root: Path, relative: Path, *, owner_uid: int) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("IMMUTABLE_INSTALL_RUNTIME_STATE_PATH_INVALID")
    try:
        metadata = root.lstat()
    except FileNotFoundError as error:
        raise ValueError("IMMUTABLE_INSTALL_RUNTIME_STATE_ROOT_MISSING") from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid not in {0, owner_uid}
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise ValueError("IMMUTABLE_INSTALL_RUNTIME_STATE_ROOT_UNSAFE")
    current = root
    for part in relative.parts:
        current /= part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            current.mkdir(mode=0o755)
            metadata = current.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid not in {0, owner_uid}
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise ValueError("IMMUTABLE_INSTALL_RUNTIME_STATE_PARENT_UNSAFE")
    return current


def _provision_runtime_state(
    component: str,
    release_id: str,
    *,
    state_root: Path,
    owner_uid: int,
    owner_gid: int,
) -> list[str]:
    paths: list[Path] = []
    for layout in RUNTIME_STATE_LAYOUTS[component]:
        parent = _ensure_directory_beneath(state_root, layout, owner_uid=owner_uid)
        path = parent / release_id
        paths.append(path)
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            path.mkdir(mode=0o700)
            os.chown(path, owner_uid, owner_gid)
            metadata = path.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise ValueError("IMMUTABLE_INSTALL_RUNTIME_STATE_NOT_DIRECTORY")
        if metadata.st_uid != owner_uid or metadata.st_gid != owner_gid or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError("IMMUTABLE_INSTALL_RUNTIME_STATE_IDENTITY_UNSAFE")
    return [str(path) for path in paths]


def _subprocess_environment(release_source: Path, sqlite_library: Path | None = None) -> dict[str, str]:
    environment = {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "PIP_CONFIG_FILE": "/dev/null",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONPATH": str(release_source),
    }
    if sqlite_library is not None:
        environment["LD_LIBRARY_PATH"] = str(sqlite_library)
    return environment


def _installed_distributions(python: Path, environment: dict[str, str]) -> dict[str, str]:
    script = (
        "import json,re; from importlib.metadata import distributions; "
        "print(json.dumps({re.sub(r'[-_.]+','-',d.metadata['Name'].lower()):d.version for d in distributions()},sort_keys=True))"
    )
    result = subprocess.run(
        [str(python), "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    value = json.loads(result.stdout)
    if not isinstance(value, dict) or not all(isinstance(key, str) and isinstance(item, str) for key, item in value.items()):
        raise ValueError("IMMUTABLE_INSTALL_DISTRIBUTION_INVENTORY_INVALID")
    return dict(value)


def _tree_manifest(
    root: Path,
    *,
    excluded: frozenset[str] = frozenset(),
) -> tuple[str, dict[str, dict[str, Any]]]:
    files: dict[str, dict[str, Any]] = {}
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        if relative in excluded:
            continue
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if stat.S_ISREG(metadata.st_mode):
            data = path.read_bytes()
            files[relative] = {"type": "file", "mode": mode, "size": len(data), "sha256": _sha256(data)}
        elif stat.S_ISLNK(metadata.st_mode):
            target = os.readlink(path)
            if target.startswith("/") or ".." in PurePosixPath(target).parts:
                raise ValueError("IMMUTABLE_INSTALL_RUNTIME_SYMLINK_UNSAFE")
            files[relative] = {"type": "symlink", "mode": mode, "target": target}
        elif not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("IMMUTABLE_INSTALL_RUNTIME_NODE_UNSAFE")
    digest_input = b"".join(
        f"{relative}\0{json.dumps(identity, separators=(',', ':'), sort_keys=True)}\0".encode()
        for relative, identity in sorted(files.items())
    )
    return _sha256(digest_input), files


def _make_read_only(root: Path, *, include_root: bool = True) -> None:
    paths = sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True)
    for path in paths:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            continue
        if stat.S_ISDIR(metadata.st_mode):
            os.chmod(path, 0o555)
        elif stat.S_ISREG(metadata.st_mode):
            os.chmod(path, 0o555 if stat.S_IMODE(metadata.st_mode) & 0o111 else 0o444)
        else:
            raise ValueError("IMMUTABLE_INSTALL_NODE_TYPE_UNSAFE")
    if include_root:
        os.chmod(root, 0o555)


def _verify_existing_directory(path: Path, *, owner_uid: int) -> None:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_TARGET_NOT_DIRECTORY")
    if metadata.st_uid not in {0, owner_uid} or stat.S_IMODE(metadata.st_mode) != 0o555:
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_TARGET_PERMISSIONS_UNSAFE")


def _verify_existing_release(
    target: Path,
    release_manifest: dict[str, Any],
    release_files: dict[str, bytes],
    *,
    owner_uid: int,
) -> None:
    _verify_existing_directory(target, owner_uid=owner_uid)
    expected = set(release_files) | {"release_manifest.json"}
    actual: set[str] = set()
    for path in sorted(target.rglob("*")):
        relative = str(path.relative_to(target))
        metadata = path.lstat()
        if metadata.st_uid not in {0, owner_uid}:
            raise ValueError("IMMUTABLE_INSTALL_EXISTING_RELEASE_OWNER_MISMATCH")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o555:
                raise ValueError("IMMUTABLE_INSTALL_EXISTING_RELEASE_MODE_MISMATCH")
            continue
        if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise ValueError("IMMUTABLE_INSTALL_EXISTING_RELEASE_NODE_UNSAFE")
        actual.add(relative)
        if relative not in expected:
            raise ValueError("IMMUTABLE_INSTALL_EXISTING_RELEASE_FILE_SET_MISMATCH")
        expected_mode = 0o444
        if relative != "release_manifest.json" and release_manifest["files"][relative].get("executable") is True:
            expected_mode = 0o555
        if stat.S_IMODE(metadata.st_mode) != expected_mode:
            raise ValueError("IMMUTABLE_INSTALL_EXISTING_RELEASE_MODE_MISMATCH")
        expected_data = _json_bytes(release_manifest) if relative == "release_manifest.json" else release_files[relative]
        if _sha256(path.read_bytes()) != _sha256(expected_data):
            raise ValueError(f"IMMUTABLE_INSTALL_EXISTING_RELEASE_HASH_MISMATCH:{relative}")
    if actual != expected:
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_RELEASE_FILE_SET_MISMATCH")


def _verify_existing_runtime(
    target: Path,
    manifest: dict[str, Any],
    *,
    owner_uid: int,
) -> dict[str, Any]:
    _verify_existing_directory(target, owner_uid=owner_uid)
    runtime_manifest_path = target / "runtime_manifest.json"
    try:
        runtime_manifest = json.loads(_secure_read(runtime_manifest_path, maximum_bytes=32 * 1024 * 1024))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_MANIFEST_INVALID") from error
    if not isinstance(runtime_manifest, dict):
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_MANIFEST_INVALID")
    if (
        runtime_manifest.get("production_action_enabled") is not False
        or type(runtime_manifest.get("physical_effect_count")) is not int
        or runtime_manifest["physical_effect_count"] != 0
        or runtime_manifest.get("service_started") is not False
    ):
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_CAPABILITY_UNSAFE")
    expected_identity = {
        "schema": "cra.installed_runtime_manifest.v1",
        "component": manifest["component"],
        "release_id": manifest["release_id"],
        "host_id": manifest["expected_host_id"],
        "bundle_sha256": manifest["bundle_sha256"],
        "source_commit": manifest["source_commit"],
        "source_tree_sha256": manifest["source_tree_sha256"],
        "runtime_platform": manifest["runtime_platform"],
        "sqlite": manifest["sqlite"],
        "production_action_enabled": False,
        "physical_effect_count": 0,
        "service_started": False,
    }
    for name, expected in expected_identity.items():
        if runtime_manifest.get(name) != expected:
            raise ValueError(f"IMMUTABLE_INSTALL_EXISTING_RUNTIME_IDENTITY_MISMATCH:{name}")
    expected_distributions = {str(wheel["distribution"]): str(wheel["version"]) for wheel in manifest["wheels"]}
    if runtime_manifest.get("installed_distributions") != expected_distributions:
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_DISTRIBUTIONS_MISMATCH")
    tree_sha256, runtime_files = _tree_manifest(
        target,
        excluded=frozenset({"runtime_manifest.json"}),
    )
    if (
        runtime_manifest.get("runtime_tree_sha256") != tree_sha256
        or runtime_manifest.get("runtime_file_count") != len(runtime_files)
        or runtime_manifest.get("runtime_files") != runtime_files
    ):
        raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_TREE_MISMATCH")
    for path in target.rglob("*"):
        metadata = path.lstat()
        if metadata.st_uid not in {0, owner_uid}:
            raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_OWNER_MISMATCH")
        if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o555:
                raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_PERMISSIONS_UNSAFE")
            continue
        if not stat.S_ISLNK(metadata.st_mode) and stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("IMMUTABLE_INSTALL_EXISTING_RUNTIME_PERMISSIONS_UNSAFE")
    return dict(runtime_manifest)


def _cleanup_staging(path: Path | None) -> None:
    if path is None or (not path.exists() and not path.is_symlink()):
        return
    if ".staging." not in path.name or not path.name.startswith("."):
        raise RuntimeError("IMMUTABLE_INSTALL_REFUSED_UNSAFE_STAGING_CLEANUP")
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RuntimeError("IMMUTABLE_INSTALL_STAGING_NODE_UNSAFE")
    os.chmod(path, 0o700)
    for child in sorted(path.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        child_metadata = child.lstat()
        if stat.S_ISDIR(child_metadata.st_mode) and not stat.S_ISLNK(child_metadata.st_mode):
            os.chmod(child, 0o700)
    shutil.rmtree(path)


def install_bundle(
    verified: dict[str, Any],
    *,
    _test_roots: tuple[Path, Path] | None = None,
    _test_state_root: Path | None = None,
) -> dict[str, Any]:
    if _test_roots is None and os.geteuid() != 0:
        raise PermissionError("IMMUTABLE_INSTALL_REQUIRES_ROOT")
    manifest = verified["manifest"]
    component = str(manifest["component"])
    release_id = str(manifest["release_id"])
    release_parent, runtime_parent = _test_roots or (INSTALL_ROOTS[component], RUNTIME_ROOTS[component])
    owner_uid = os.geteuid() if _test_roots is not None else 0
    testing = _test_roots is not None or _test_state_root is not None
    state_root = _test_state_root or (release_parent.parent / "runtime-state" if testing else Path("/var/lib"))
    if testing:
        state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(state_root, 0o700)
    state_uid, state_gid = _runtime_state_identity(component, testing=testing)
    _ensure_directory(release_parent, owner_uid=owner_uid)
    _ensure_directory(runtime_parent, owner_uid=owner_uid)
    release_target = release_parent / release_id
    runtime_target = runtime_parent / release_id
    release_stage = release_parent / f".{release_id}.staging.{os.getpid()}"
    runtime_stage = runtime_parent / f".{release_id}.staging.{os.getpid()}"
    release_manifest = verified["release_manifest"]
    release_files = verified["release_files"]
    release_exists = release_target.exists() or release_target.is_symlink()
    runtime_exists = runtime_target.exists() or runtime_target.is_symlink()
    if release_exists:
        _verify_existing_release(
            release_target,
            release_manifest,
            release_files,
            owner_uid=owner_uid,
        )
    existing_runtime_manifest: dict[str, Any] | None = None
    if runtime_exists:
        existing_runtime_manifest = _verify_existing_runtime(
            runtime_target,
            {**manifest, "bundle_sha256": verified["bundle_sha256"]},
            owner_uid=owner_uid,
        )
    if release_exists and runtime_exists:
        assert existing_runtime_manifest is not None
        state_directories = _provision_runtime_state(
            component,
            release_id,
            state_root=state_root,
            owner_uid=state_uid,
            owner_gid=state_gid,
        )
        return {
            "schema": "cra.immutable_runtime_install_result.v2",
            "status": "ALREADY_INSTALLED_NOT_STARTED",
            "component": component,
            "release_id": release_id,
            "host_id": manifest["expected_host_id"],
            "release_root": str(release_target),
            "runtime_root": str(runtime_target),
            "bundle_sha256": verified["bundle_sha256"],
            "runtime_tree_sha256": existing_runtime_manifest["runtime_tree_sha256"],
            "runtime_state_directories": state_directories,
            "runtime_state_provisioned": True,
            "production_action_enabled": False,
            "physical_effect_count": 0,
            "service_started": False,
        }
    for path in (release_stage, runtime_stage):
        if path.exists() or path.is_symlink():
            raise ValueError(f"IMMUTABLE_INSTALL_STAGING_PATH_EXISTS:{path}")

    active_release_stage: Path | None = None
    active_runtime_stage: Path | None = None
    tree_sha256 = str(existing_runtime_manifest["runtime_tree_sha256"]) if existing_runtime_manifest else ""
    try:
        if not release_exists:
            release_stage.mkdir(mode=0o700)
            active_release_stage = release_stage
            for relative, data in sorted(release_files.items()):
                identity = release_manifest["files"][relative]
                mode = 0o755 if identity.get("executable") is True else 0o644
                _write_file(release_stage / relative, data, mode)
            _write_file(release_stage / "release_manifest.json", _json_bytes(release_manifest), 0o644)

        if not runtime_exists:
            runtime_stage.mkdir(mode=0o700)
            active_runtime_stage = runtime_stage
            wheelhouse = runtime_stage / "wheelhouse"
            wheelhouse.mkdir(mode=0o700)
            wheel_paths: list[Path] = []
            for wheel in manifest["wheels"]:
                filename = str(wheel["filename"])
                path = wheelhouse / filename
                _write_file(path, verified["payloads"][f"wheelhouse/{filename}"], 0o400)
                wheel_paths.append(path)

            sqlite_directory: Path | None = None
            if component == "cra":
                sqlite_identity = manifest["sqlite"]
                sqlite_directory = runtime_stage / "sqlite/lib"
                sqlite_directory.mkdir(parents=True, mode=0o700)
                sqlite_name = str(sqlite_identity["filename"])
                _write_file(
                    sqlite_directory / sqlite_name,
                    verified["payloads"][f"sqlite/{sqlite_name}"],
                    0o555,
                )
                os.symlink(sqlite_name, sqlite_directory / "libsqlite3.so.0")
                os.symlink("libsqlite3.so.0", sqlite_directory / "libsqlite3.so")

            release_source = (release_target if release_exists else release_stage) / "src"
            venv = runtime_stage / ".venv"
            base_environment = _subprocess_environment(release_source, sqlite_directory)
            subprocess.run(
                [sys.executable, "-m", "venv", "--without-pip", "--copies", str(venv)],
                check=True,
                env=base_environment,
            )
            pip_environment = dict(base_environment)
            pip_environment["PYTHONPATH"] = ""
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "--python",
                    str(venv),
                    "install",
                    "--disable-pip-version-check",
                    "--no-index",
                    "--no-deps",
                    "--no-compile",
                    *map(str, wheel_paths),
                ],
                check=True,
                env=pip_environment,
            )
            for name in ("Activate.ps1", "activate", "activate.csh", "activate.fish"):
                path = venv / "bin" / name
                if path.exists():
                    path.unlink()
            gitignore = venv / ".gitignore"
            if gitignore.exists():
                gitignore.unlink()
            configuration = venv / "pyvenv.cfg"
            lines = [line for line in configuration.read_text(encoding="utf-8").splitlines() if not line.startswith("command = ")]
            lines.append(f"command = {sys.executable} -m venv --copies --without-pip {runtime_target}/.venv")
            configuration.write_text("\n".join(lines) + "\n", encoding="utf-8")

            runtime_python = venv / "bin/python"
            environment = _subprocess_environment(release_source, sqlite_directory)
            installed = _installed_distributions(runtime_python, environment)
            expected_installed = {str(wheel["distribution"]): str(wheel["version"]) for wheel in manifest["wheels"]}
            if installed != expected_installed:
                raise ValueError("IMMUTABLE_INSTALL_DISTRIBUTIONS_MISMATCH")
            import_script = "; ".join(f"import {name}" for name in IMPORTS[component])
            subprocess.run([str(runtime_python), "-c", import_script], check=True, env=environment)
            for module in CLI_MODULES[component]:
                subprocess.run(
                    [str(runtime_python), "-W", "error::RuntimeWarning", "-m", module, "--help"],
                    check=True,
                    capture_output=True,
                    env=environment,
                )
            if component == "cra":
                result = subprocess.run(
                    [str(runtime_python), "-c", "import sqlite3; print(sqlite3.sqlite_version)"],
                    check=True,
                    capture_output=True,
                    text=True,
                    env=environment,
                )
                if result.stdout.strip() != manifest["sqlite"]["version"]:
                    raise ValueError("IMMUTABLE_INSTALL_SQLITE_RUNTIME_MISMATCH")

            _make_read_only(runtime_stage, include_root=False)
            tree_sha256, runtime_files = _tree_manifest(runtime_stage)
            runtime_manifest = {
                "schema": "cra.installed_runtime_manifest.v1",
                "component": component,
                "release_id": release_id,
                "host_id": manifest["expected_host_id"],
                "bundle_sha256": verified["bundle_sha256"],
                "source_commit": manifest["source_commit"],
                "source_tree_sha256": manifest["source_tree_sha256"],
                "runtime_platform": manifest["runtime_platform"],
                "installed_distributions": installed,
                "sqlite": manifest["sqlite"],
                "runtime_tree_sha256": tree_sha256,
                "runtime_file_count": len(runtime_files),
                "runtime_files": runtime_files,
                "production_action_enabled": False,
                "physical_effect_count": 0,
                "service_started": False,
            }
            _write_file(runtime_stage / "runtime_manifest.json", _json_bytes(runtime_manifest), 0o444)
            os.chmod(runtime_stage, 0o555)

        if active_release_stage is not None:
            _make_read_only(active_release_stage)
        if active_runtime_stage is not None:
            os.replace(active_runtime_stage, runtime_target)
            active_runtime_stage = None
        if active_release_stage is not None:
            os.replace(active_release_stage, release_target)
            active_release_stage = None
        for parent in (runtime_parent, release_parent):
            descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except BaseException:
        _cleanup_staging(active_runtime_stage)
        _cleanup_staging(active_release_stage)
        raise
    state_directories = _provision_runtime_state(
        component,
        release_id,
        state_root=state_root,
        owner_uid=state_uid,
        owner_gid=state_gid,
    )
    return {
        "schema": "cra.immutable_runtime_install_result.v2",
        "status": "INSTALLED_NOT_STARTED",
        "component": component,
        "release_id": release_id,
        "host_id": manifest["expected_host_id"],
        "release_root": str(release_target),
        "runtime_root": str(runtime_target),
        "bundle_sha256": verified["bundle_sha256"],
        "runtime_tree_sha256": tree_sha256,
        "runtime_state_directories": state_directories,
        "runtime_state_provisioned": True,
        "production_action_enabled": False,
        "physical_effect_count": 0,
        "service_started": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify or install a host-bound immutable runtime bundle")
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--expected-bundle-sha256", required=True)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    verified = verify_bundle(args.bundle, args.expected_bundle_sha256)
    if args.verify_only:
        manifest = verified["manifest"]
        result = {
            "schema": "cra.immutable_runtime_bundle_verification.v1",
            "status": "VERIFIED_NOT_INSTALLED",
            "component": manifest["component"],
            "release_id": manifest["release_id"],
            "host_id": manifest["expected_host_id"],
            "bundle_sha256": verified["bundle_sha256"],
            "production_action_enabled": False,
            "physical_effect_count": 0,
        }
    else:
        result = install_bundle(verified)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
