from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

from tools.build_immutable_runtime_bundle import (
    EXPECTED_HOST_IDS,
    build_bundle,
    expected_distributions,
    runtime_platform,
    validate_wheelhouse,
    wheel_identity,
)
from tools.build_observation_plane_release import REQUIRED_FILES, build_release
from tools.install_immutable_runtime_release import _provision_runtime_state, verify_bundle

ROOT = Path(__file__).resolve().parents[2]


def _git(path: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _source(tmp_path: Path, component: str) -> tuple[Path, Path]:
    source = tmp_path / f"source-{component}"
    source.mkdir(parents=True)
    for relative in REQUIRED_FILES[component]:
        destination = source / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    lock_wheelhouse = tmp_path / f"lock-wheelhouse-{component}"
    lock_wheelhouse.mkdir()
    for distribution, version in expected_distributions(source, "arena").items():
        filename = f"{distribution.replace('-', '_')}-{version}-py3-none-any.whl"
        _wheel(lock_wheelhouse / filename, distribution, version)
    wheels = [wheel_identity(path) for path in sorted(lock_wheelhouse.glob("*.whl"))]
    lock = {
        "schema": "cra.runtime_wheel_lock.v1",
        "runtime_platform": runtime_platform(),
        "wheel_count": len(wheels),
        "wheels": wheels,
    }
    lock_path = source / "constraints/runtime-wheels-cp314-linux-x86_64.json"
    lock_path.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    wheelhouse = tmp_path / f"wheelhouse-{component}"
    wheelhouse.mkdir()
    for distribution in expected_distributions(source, component):
        identity = next(item for item in wheels if item["distribution"] == distribution)
        shutil.copy2(lock_wheelhouse / str(identity["filename"]), wheelhouse / str(identity["filename"]))
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Runtime Bundle Test")
    _git(source, "config", "user.email", "runtime-bundle-test@example.invalid")
    _git(source, "add", ".")
    _git(source, "commit", "-q", "-m", "immutable runtime bundle fixture")
    return source, wheelhouse


def _wheel(path: Path, distribution: str, version: str) -> None:
    stem = distribution.replace("-", "_")
    metadata_root = f"{stem}-{version}.dist-info"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"{metadata_root}/METADATA",
            f"Metadata-Version: 2.4\nName: {distribution}\nVersion: {version}\n\n",
        )
        archive.writestr(
            f"{metadata_root}/WHEEL",
            "Wheel-Version: 1.0\nGenerator: runtime-bundle-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n",
        )
        archive.writestr(f"{stem}/__init__.py", "")


def _host_contract(path: Path, host_id: str) -> Path:
    path.write_text(json.dumps({"schema_version": 1, "host_id": host_id}) + "\n", encoding="utf-8")
    os.chmod(path, 0o644)
    return path


def _installer(path: Path) -> Path:
    shutil.copy2(ROOT / "tools/install_immutable_runtime_release.py", path)
    os.chmod(path, 0o644)
    return path


def test_runtime_bundle_is_reproducible_host_bound_and_verify_only_has_no_install(tmp_path: Path) -> None:
    source, wheelhouse = _source(tmp_path, "dell")
    release = tmp_path / "dell-release.tar.gz"
    built_release = build_release(source, release, "dell-observation-runtime-test-a", "dell")
    first = build_bundle(source, release, wheelhouse, tmp_path / "first.tar.gz", "dell")
    second = build_bundle(source, release, wheelhouse, tmp_path / "second.tar.gz", "dell")

    assert first["bundle_sha256"] == second["bundle_sha256"]
    assert first["release_id"] == built_release["release_id"]
    assert first["expected_host_id"] == EXPECTED_HOST_IDS["dell"]
    assert first["production_action_enabled"] is False
    assert first["physical_effect_count"] == 0
    assert first["starts_service"] is False
    assert first["installs_config"] is False
    assert first["installs_credential"] is False
    contract = _host_contract(tmp_path / "host.json", EXPECTED_HOST_IDS["dell"])
    installer = _installer(tmp_path / "installer.py")
    verified = verify_bundle(
        tmp_path / "first.tar.gz",
        first["bundle_sha256"],
        host_contract_path=contract,
        _installer_path=installer,
    )
    assert verified["manifest"]["release_id"] == built_release["release_id"]
    assert not (tmp_path / "opt").exists()


def test_runtime_bundle_rejects_wrong_hash_host_extra_wheel_and_existing_output(tmp_path: Path) -> None:
    source, wheelhouse = _source(tmp_path, "arena")
    release = tmp_path / "arena-release.tar.gz"
    build_release(source, release, "arena-cra-projection-runtime-test-a", "arena")
    result = build_bundle(source, release, wheelhouse, tmp_path / "bundle.tar.gz", "arena")
    wrong_contract = _host_contract(tmp_path / "wrong-host.json", "not-the-arena-host")
    installer = _installer(tmp_path / "installer.py")
    with pytest.raises(ValueError, match="IMMUTABLE_INSTALL_EXPECTED_SHA256_INVALID"):
        verify_bundle(tmp_path / "bundle.tar.gz", "invalid", host_contract_path=wrong_contract, _installer_path=installer)
    with pytest.raises(ValueError, match="IMMUTABLE_INSTALL_BUNDLE_SHA256_MISMATCH"):
        verify_bundle(tmp_path / "bundle.tar.gz", "0" * 64, host_contract_path=wrong_contract, _installer_path=installer)
    with pytest.raises(ValueError, match="IMMUTABLE_INSTALL_HOST_CONTRACT_MISMATCH"):
        verify_bundle(
            tmp_path / "bundle.tar.gz",
            result["bundle_sha256"],
            host_contract_path=wrong_contract,
            _installer_path=installer,
        )
    with pytest.raises(ValueError, match="RUNTIME_BUNDLE_OUTPUT_EXISTS"):
        build_bundle(source, release, wheelhouse, tmp_path / "bundle.tar.gz", "arena")

    _wheel(wheelhouse / "unexpected-1.0-py3-none-any.whl", "unexpected", "1.0")
    with pytest.raises(ValueError, match="RUNTIME_WHEELHOUSE_DISTRIBUTIONS_MISMATCH"):
        validate_wheelhouse(source, wheelhouse, "arena")


def test_observation_release_binds_the_exact_installer_hash(tmp_path: Path) -> None:
    source, _ = _source(tmp_path, "dell")
    release = tmp_path / "release.tar.gz"
    result = build_release(source, release, "dell-observation-installer-bound-a", "dell")
    installer = result["files"]["tools/install_immutable_runtime_release.py"]
    expected = hashlib.sha256((ROOT / "tools/install_immutable_runtime_release.py").read_bytes()).hexdigest()
    assert installer["sha256"] == expected


@pytest.mark.parametrize(("component", "expected_count"), [("arena", 4), ("cra", 2), ("dell", 1)])
def test_installer_provisions_all_release_scoped_runtime_state_idempotently(
    tmp_path: Path,
    component: str,
    expected_count: int,
) -> None:
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    release_id = f"{component}-release-a"

    first = _provision_runtime_state(
        component,
        release_id,
        state_root=state_root,
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
    )

    assert len(first) == expected_count
    for value in first:
        path = Path(value)
        assert path.name == release_id
        assert path.stat().st_mode & 0o777 == 0o700
    if first:
        sentinel = Path(first[0]) / "existing-state"
        sentinel.write_text("preserved", encoding="utf-8")
        second = _provision_runtime_state(
            component,
            release_id,
            state_root=state_root,
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
        )
        assert second == first
        assert sentinel.read_text(encoding="utf-8") == "preserved"


def test_installer_rejects_unsafe_existing_runtime_state_without_repairing_it(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    paths = _provision_runtime_state(
        "cra",
        "cra-release-a",
        state_root=state_root,
        owner_uid=os.geteuid(),
        owner_gid=os.getegid(),
    )
    unsafe = Path(paths[0])
    unsafe.chmod(0o770)

    with pytest.raises(ValueError, match="IMMUTABLE_INSTALL_RUNTIME_STATE_IDENTITY_UNSAFE"):
        _provision_runtime_state(
            "cra",
            "cra-release-a",
            state_root=state_root,
            owner_uid=os.geteuid(),
            owner_gid=os.getegid(),
        )

    assert unsafe.stat().st_mode & 0o777 == 0o770
