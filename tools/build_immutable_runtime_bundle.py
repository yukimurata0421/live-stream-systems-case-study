from __future__ import annotations

import argparse
import ctypes
import gzip
import hashlib
import io
import json
import os
import platform
import re
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import Any

COMPONENTS = ("cra", "dell", "arena")
EXPECTED_HOST_IDS = {
    "cra": "cra-01-central-authority",
    "dell": "dell-stream-runtime",
    "arena": "arena-monitoring-facts",
}
INSTALL_ROOTS = {
    "cra": "/opt/stream-recovery-control/releases",
    "dell": "/opt/stream-recovery-observation/releases",
    "arena": "/opt/stream-monitoring-cra-projection/releases",
}
RUNTIME_ROOTS = {
    "cra": "/opt/stream-recovery-control/runtimes",
    "dell": "/opt/stream-recovery-observation/runtimes",
    "arena": "/opt/stream-monitoring-cra-projection/runtimes",
}
RELEASE_COMPONENTS = {
    "cra": ("cra.no_action_release_manifest.v1", "cra-authority"),
    "dell": ("cra.observation_plane_release_manifest.v1", "dell"),
    "arena": ("cra.observation_plane_release_manifest.v1", "arena"),
}
COMMON_DISTRIBUTIONS = frozenset(
    {
        "attrs",
        "cffi",
        "cryptography",
        "jsonschema",
        "jsonschema-specifications",
        "pycparser",
        "referencing",
        "rfc8785",
        "rpds-py",
    }
)
COMPONENT_DISTRIBUTIONS = {
    "cra": COMMON_DISTRIBUTIONS,
    "dell": COMMON_DISTRIBUTIONS,
    "arena": COMMON_DISTRIBUTIONS | {"psycopg", "psycopg-binary"},
}
SAFE_RELEASE_ID = re.compile(r"^[a-z0-9][a-z0-9.-]{7,127}$")
MAX_RELEASE_BYTES = 64 * 1024 * 1024
MAX_WHEEL_BYTES = 64 * 1024 * 1024
MAX_BUNDLE_BYTES = 256 * 1024 * 1024
WHEEL_LOCK = "constraints/runtime-wheels-cp314-linux-x86_64.json"


def _normalise_distribution(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _constraints(source: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_line in (source / "constraints/runtime.txt").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.count("==") != 1:
            raise ValueError("RUNTIME_CONSTRAINT_NOT_EXACT")
        name, version = line.split("==", 1)
        normalised = _normalise_distribution(name)
        if not normalised or not version or normalised in result:
            raise ValueError("RUNTIME_CONSTRAINT_INVALID")
        result[normalised] = version
    return result


def expected_distributions(source: Path, component: str) -> dict[str, str]:
    if component not in COMPONENT_DISTRIBUTIONS:
        raise ValueError("RUNTIME_COMPONENT_INVALID")
    constraints = _constraints(source)
    expected = COMPONENT_DISTRIBUTIONS[component]
    missing = sorted(expected - constraints.keys())
    if missing:
        raise ValueError(f"RUNTIME_CONSTRAINT_MISSING:{','.join(missing)}")
    return {name: constraints[name] for name in sorted(expected)}


def _locked_wheels(source: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    value = json.loads((source / WHEEL_LOCK).read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != {"schema", "runtime_platform", "wheel_count", "wheels"}:
        raise ValueError("RUNTIME_WHEEL_LOCK_FIELDS_NOT_EXACT")
    if value["schema"] != "cra.runtime_wheel_lock.v1" or value["runtime_platform"] != runtime_platform():
        raise ValueError("RUNTIME_WHEEL_LOCK_PLATFORM_MISMATCH")
    wheels = value["wheels"]
    if not isinstance(wheels, list) or value["wheel_count"] != len(wheels):
        raise ValueError("RUNTIME_WHEEL_LOCK_COUNT_MISMATCH")
    locked: dict[str, dict[str, Any]] = {}
    for item in wheels:
        if not isinstance(item, dict):
            raise ValueError("RUNTIME_WHEEL_LOCK_IDENTITY_INVALID")
        name = item.get("distribution")
        if not isinstance(name, str) or name in locked:
            raise ValueError("RUNTIME_WHEEL_LOCK_DISTRIBUTION_INVALID")
        if set(item) != {"distribution", "filename", "sha256", "size", "tags", "version"}:
            raise ValueError("RUNTIME_WHEEL_LOCK_IDENTITY_FIELDS_NOT_EXACT")
        locked[name] = dict(item)
    if set(locked) != COMPONENT_DISTRIBUTIONS["arena"]:
        raise ValueError("RUNTIME_WHEEL_LOCK_DISTRIBUTIONS_MISMATCH")
    constraints = _constraints(source)
    for name, item in locked.items():
        if constraints.get(name) != item["version"]:
            raise ValueError(f"RUNTIME_WHEEL_LOCK_CONSTRAINT_MISMATCH:{name}")
    return value, locked


def _safe_archive_members(archive: tarfile.TarFile, *, maximum_total: int) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    total = 0
    for member in archive.getmembers():
        path = PurePosixPath(member.name)
        if member.name in contents or path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
            raise ValueError("RUNTIME_ARCHIVE_MEMBER_PATH_INVALID")
        if not member.isfile() or member.size < 0:
            raise ValueError("RUNTIME_ARCHIVE_MEMBER_NOT_REGULAR")
        total += member.size
        if total > maximum_total:
            raise ValueError("RUNTIME_ARCHIVE_TOO_LARGE")
        handle = archive.extractfile(member)
        if handle is None:
            raise ValueError("RUNTIME_ARCHIVE_MEMBER_UNREADABLE")
        contents[member.name] = handle.read()
    return contents


def read_and_verify_release(archive_path: Path, component: str) -> tuple[dict[str, Any], bytes]:
    if archive_path.is_symlink() or not archive_path.is_file():
        raise ValueError("RUNTIME_RELEASE_ARCHIVE_NOT_REGULAR")
    archive_data = archive_path.read_bytes()
    if len(archive_data) > MAX_RELEASE_BYTES:
        raise ValueError("RUNTIME_RELEASE_ARCHIVE_TOO_LARGE")
    with tarfile.open(fileobj=io.BytesIO(archive_data), mode="r:gz") as archive:
        contents = _safe_archive_members(archive, maximum_total=MAX_RELEASE_BYTES)
    roots = {PurePosixPath(name).parts[0] for name in contents}
    if len(roots) != 1:
        raise ValueError("RUNTIME_RELEASE_ROOT_NOT_EXACT")
    root = roots.pop()
    manifest_name = f"{root}/release_manifest.json"
    if manifest_name not in contents:
        raise ValueError("RUNTIME_RELEASE_MANIFEST_MISSING")
    manifest = json.loads(contents[manifest_name])
    if not isinstance(manifest, dict):
        raise ValueError("RUNTIME_RELEASE_MANIFEST_INVALID")
    expected_schema, expected_component = RELEASE_COMPONENTS[component]
    if manifest.get("schema") != expected_schema or manifest.get("component") != expected_component:
        raise ValueError("RUNTIME_RELEASE_COMPONENT_MISMATCH")
    release_id = manifest.get("release_id")
    if not isinstance(release_id, str) or not SAFE_RELEASE_ID.fullmatch(release_id):
        raise ValueError("RUNTIME_RELEASE_ID_INVALID")
    expected_root = f"{INSTALL_ROOTS[component]}/{release_id}"
    if manifest.get("expected_install_root") != expected_root:
        raise ValueError("RUNTIME_RELEASE_INSTALL_ROOT_MISMATCH")
    if manifest.get("production_action_enabled") is not False:
        raise ValueError("RUNTIME_RELEASE_ACTION_ENABLED")
    if type(manifest.get("control_capability_count")) is not int or manifest["control_capability_count"] != 0:
        raise ValueError("RUNTIME_RELEASE_CONTROL_CAPABILITY_PRESENT")
    if type(manifest.get("physical_effect_count")) is not int or manifest["physical_effect_count"] != 0:
        raise ValueError("RUNTIME_RELEASE_PHYSICAL_EFFECT_PRESENT")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError("RUNTIME_RELEASE_FILE_MANIFEST_INVALID")
    actual_relatives = {str(PurePosixPath(name).relative_to(root)) for name in contents if name != manifest_name}
    if actual_relatives != set(files):
        raise ValueError("RUNTIME_RELEASE_FILE_SET_MISMATCH")
    for relative, identity in files.items():
        if not isinstance(identity, dict):
            raise ValueError("RUNTIME_RELEASE_FILE_IDENTITY_INVALID")
        data = contents[f"{root}/{relative}"]
        if identity.get("sha256") != _sha256(data) or identity.get("size") != len(data):
            raise ValueError(f"RUNTIME_RELEASE_FILE_HASH_MISMATCH:{relative}")
    installer = files.get("tools/install_immutable_runtime_release.py")
    if not isinstance(installer, dict) or not isinstance(installer.get("sha256"), str):
        raise ValueError("RUNTIME_RELEASE_INSTALLER_NOT_BOUND")
    return manifest, archive_data


def wheel_identity(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.suffix != ".whl":
        raise ValueError("RUNTIME_WHEEL_NOT_REGULAR")
    data = path.read_bytes()
    if not data or len(data) > MAX_WHEEL_BYTES:
        raise ValueError("RUNTIME_WHEEL_SIZE_INVALID")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = archive.infolist()
            metadata_members = [item for item in members if item.filename.endswith(".dist-info/METADATA")]
            wheel_members = [item for item in members if item.filename.endswith(".dist-info/WHEEL")]
            if len(metadata_members) != 1 or len(wheel_members) != 1:
                raise ValueError("RUNTIME_WHEEL_METADATA_NOT_EXACT")
            for member in members:
                item = PurePosixPath(member.filename)
                if item.is_absolute() or ".." in item.parts or member.is_dir():
                    continue
                mode = (member.external_attr >> 16) & 0o170000
                if mode not in (0, 0o100000):
                    raise ValueError("RUNTIME_WHEEL_MEMBER_NOT_REGULAR")
            metadata = BytesParser().parsebytes(archive.read(metadata_members[0]))
            wheel_metadata = BytesParser().parsebytes(archive.read(wheel_members[0]))
    except zipfile.BadZipFile as error:
        raise ValueError("RUNTIME_WHEEL_ZIP_INVALID") from error
    name = metadata.get("Name")
    version = metadata.get("Version")
    tags = wheel_metadata.get_all("Tag", [])
    if not name or not version or not tags:
        raise ValueError("RUNTIME_WHEEL_IDENTITY_INCOMPLETE")
    return {
        "filename": path.name,
        "distribution": _normalise_distribution(name),
        "version": version,
        "sha256": _sha256(data),
        "size": len(data),
        "tags": sorted(tags),
    }


def validate_wheelhouse(source: Path, wheelhouse: Path, component: str) -> list[dict[str, Any]]:
    if wheelhouse.is_symlink() or not wheelhouse.is_dir():
        raise ValueError("RUNTIME_WHEELHOUSE_NOT_DIRECTORY")
    paths = sorted(wheelhouse.glob("*.whl"))
    identities = [wheel_identity(path) for path in paths]
    actual: dict[str, dict[str, Any]] = {}
    for identity in identities:
        name = str(identity["distribution"])
        if name in actual:
            raise ValueError(f"RUNTIME_WHEEL_DUPLICATE:{name}")
        actual[name] = identity
    expected = expected_distributions(source, component)
    _, locked = _locked_wheels(source)
    if set(actual) != set(expected):
        raise ValueError("RUNTIME_WHEELHOUSE_DISTRIBUTIONS_MISMATCH")
    for name, version in expected.items():
        if actual[name]["version"] != version:
            raise ValueError(f"RUNTIME_WHEEL_VERSION_MISMATCH:{name}")
        if actual[name] != locked[name]:
            raise ValueError(f"RUNTIME_WHEEL_LOCK_MISMATCH:{name}")
    return [actual[name] for name in sorted(actual)]


def runtime_platform() -> dict[str, Any]:
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


def download_wheelhouse(source: Path, output: Path, component: str, *, python: Path = Path(sys.executable)) -> dict[str, Any]:
    if output.exists():
        raise ValueError("RUNTIME_WHEELHOUSE_OUTPUT_EXISTS")
    expected = expected_distributions(source, component)
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.TemporaryDirectory(prefix=f".{output.name}.", dir=output.parent) as temporary_name:
        temporary = Path(temporary_name)
        command = [
            str(python),
            "-m",
            "pip",
            "download",
            "--disable-pip-version-check",
            "--no-deps",
            "--only-binary=:all:",
            "--dest",
            str(temporary),
            *(f"{name}=={version}" for name, version in expected.items()),
        ]
        subprocess.run(command, check=True)
        wheels = validate_wheelhouse(source, temporary, component)
        manifest = {
            "schema": "cra.runtime_wheelhouse_manifest.v1",
            "component": component,
            "runtime_platform": runtime_platform(),
            "wheel_count": len(wheels),
            "wheels": wheels,
        }
        (temporary / "wheelhouse_manifest.json").write_bytes(_json_bytes(manifest))
        os.chmod(temporary / "wheelhouse_manifest.json", 0o600)
        os.replace(temporary, output)
    return manifest


def _sqlite_identity(source: Path, library: Path) -> tuple[dict[str, Any], bytes]:
    if library.is_symlink() or not library.is_file():
        raise ValueError("RUNTIME_SQLITE_LIBRARY_NOT_REGULAR")
    data = library.read_bytes()
    if not data or len(data) > 32 * 1024 * 1024:
        raise ValueError("RUNTIME_SQLITE_LIBRARY_SIZE_INVALID")
    constraint = json.loads((source / "constraints/sqlite-runtime.json").read_text(encoding="utf-8"))
    if not isinstance(constraint, dict) or constraint.get("schema") != "cra.sqlite_runtime_constraint.v1":
        raise ValueError("RUNTIME_SQLITE_CONSTRAINT_INVALID")
    handle = ctypes.CDLL(str(library.resolve()))
    handle.sqlite3_libversion.restype = ctypes.c_char_p
    raw_version = handle.sqlite3_libversion()
    version = raw_version.decode("ascii") if raw_version is not None else ""
    if version != constraint.get("version"):
        raise ValueError("RUNTIME_SQLITE_VERSION_MISMATCH")
    return (
        {
            "version": version,
            "filename": f"libsqlite3.so.{version}",
            "sha256": _sha256(data),
            "size": len(data),
            "source_archive": constraint["source_archive"],
            "source_archive_sha256": constraint["source_archive_sha256"],
            "sqlite3_c_sha3_256": constraint["sqlite3_c_sha3_256"],
        },
        data,
    )


def _tar_info(name: str, data: bytes) -> tuple[tarfile.TarInfo, io.BytesIO]:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = 0o600
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    return info, io.BytesIO(data)


def build_bundle(
    source: Path,
    release_archive: Path,
    wheelhouse: Path,
    output: Path,
    component: str,
    *,
    sqlite_library: Path | None = None,
) -> dict[str, Any]:
    source = source.resolve()
    if component not in COMPONENTS:
        raise ValueError("RUNTIME_COMPONENT_INVALID")
    if output.exists() or output.with_suffix(output.suffix + ".manifest.json").exists():
        raise ValueError("RUNTIME_BUNDLE_OUTPUT_EXISTS")
    release, release_data = read_and_verify_release(release_archive, component)
    constraint_files = ["constraints/runtime.txt", WHEEL_LOCK]
    if component == "cra":
        constraint_files.append("constraints/sqlite-runtime.json")
    for relative in constraint_files:
        source_data = (source / relative).read_bytes()
        identity = release["files"].get(relative)
        if not isinstance(identity, dict) or identity.get("sha256") != _sha256(source_data):
            raise ValueError(f"RUNTIME_RELEASE_CONSTRAINT_MISMATCH:{relative}")
    wheels = validate_wheelhouse(source, wheelhouse, component)
    sqlite_identity: dict[str, Any] | None = None
    sqlite_data: bytes | None = None
    if component == "cra":
        if sqlite_library is None:
            raise ValueError("RUNTIME_SQLITE_LIBRARY_REQUIRED")
        sqlite_identity, sqlite_data = _sqlite_identity(source, sqlite_library)
    elif sqlite_library is not None:
        raise ValueError("RUNTIME_SQLITE_LIBRARY_UNEXPECTED")

    release_id = str(release["release_id"])
    payloads: dict[str, bytes] = {"source-release.tar.gz": release_data}
    for identity in wheels:
        filename = str(identity["filename"])
        payloads[f"wheelhouse/{filename}"] = (wheelhouse / filename).read_bytes()
    if sqlite_identity is not None and sqlite_data is not None:
        payloads[f"sqlite/{sqlite_identity['filename']}"] = sqlite_data
    files = {name: {"sha256": _sha256(data), "size": len(data)} for name, data in sorted(payloads.items())}
    manifest = {
        "schema": "cra.immutable_runtime_bundle.v1",
        "component": component,
        "release_id": release_id,
        "source_commit": release["source_commit"],
        "source_tree_sha256": release["source_tree_sha256"],
        "source_release_archive_sha256": _sha256(release_data),
        "source_release_manifest_sha256": _sha256(_json_bytes(release)),
        "installer_sha256": release["files"]["tools/install_immutable_runtime_release.py"]["sha256"],
        "expected_host_id": EXPECTED_HOST_IDS[component],
        "expected_install_root": f"{INSTALL_ROOTS[component]}/{release_id}",
        "expected_runtime_root": f"{RUNTIME_ROOTS[component]}/{release_id}",
        "runtime_platform": runtime_platform(),
        "wheel_lock_sha256": _sha256((source / WHEEL_LOCK).read_bytes()),
        "wheels": wheels,
        "sqlite": sqlite_identity,
        "production_action_enabled": False,
        "control_capability_count": 0,
        "physical_effect_count": 0,
        "starts_service": False,
        "installs_config": False,
        "installs_credential": False,
        "file_count": len(files),
        "files": files,
    }
    manifest_data = _json_bytes(manifest)
    prefix = f"stream-recovery-runtime-{release_id}"
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for relative, data in sorted(payloads.items()):
            info, payload = _tar_info(f"{prefix}/{relative}", data)
            archive.addfile(info, payload)
        info, payload = _tar_info(f"{prefix}/runtime_bundle_manifest.json", manifest_data)
        archive.addfile(info, payload)
    if len(tar_buffer.getvalue()) > MAX_BUNDLE_BYTES:
        raise ValueError("RUNTIME_BUNDLE_TOO_LARGE")
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
    sidecar = output.with_suffix(output.suffix + ".manifest.json")
    sidecar.write_bytes(manifest_data)
    os.chmod(sidecar, 0o600)
    return {**manifest, "bundle_sha256": _sha256(output.read_bytes()), "bundle_path": str(output)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an offline immutable Python runtime bundle")
    subparsers = parser.add_subparsers(dest="command", required=True)
    wheelhouse_parser = subparsers.add_parser("wheelhouse")
    wheelhouse_parser.add_argument("--source", type=Path, required=True)
    wheelhouse_parser.add_argument("--component", choices=COMPONENTS, required=True)
    wheelhouse_parser.add_argument("--output", type=Path, required=True)
    bundle_parser = subparsers.add_parser("bundle")
    bundle_parser.add_argument("--source", type=Path, required=True)
    bundle_parser.add_argument("--component", choices=COMPONENTS, required=True)
    bundle_parser.add_argument("--release-archive", type=Path, required=True)
    bundle_parser.add_argument("--wheelhouse", type=Path, required=True)
    bundle_parser.add_argument("--sqlite-library", type=Path)
    bundle_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "wheelhouse":
        result = download_wheelhouse(args.source, args.output, args.component)
    else:
        result = build_bundle(
            args.source,
            args.release_archive,
            args.wheelhouse,
            args.output,
            args.component,
            sqlite_library=args.sqlite_library,
        )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
