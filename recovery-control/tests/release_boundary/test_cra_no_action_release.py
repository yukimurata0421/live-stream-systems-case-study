from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from tools.build_cra_no_action_release import REQUIRED_FILES, build_release
from tools.install_immutable_runtime_release import CLI_MODULES, IMPORTS

ROOT = Path(__file__).resolve().parents[2]


def _git(path: Path, *arguments: str) -> None:
    subprocess.run(["git", *arguments], cwd=path, check=True, capture_output=True)


def _repository(tmp_path: Path) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Release Test")
    _git(source, "config", "user.email", "release-test@example.invalid")
    for relative in REQUIRED_FILES:
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"safe fixture for {relative}\n", encoding="utf-8")
    for relative in ("src/dell_recovery_agent/execution.py", "src/runtime_boundary/server.py"):
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"excluded effect-capable fixture for {relative}\n", encoding="utf-8")
    _git(source, "add", ".")
    _git(source, "commit", "-q", "-m", "fixture")
    return source


def _actual_source_repository(tmp_path: Path) -> Path:
    source = tmp_path / "actual-source"
    source.mkdir()
    for name in (".gitignore", "README.md", "pyproject.toml"):
        shutil.copy2(ROOT / name, source / name)
    for name in ("constraints", "contracts", "docs", "harness", "migrations", "ops", "policy", "src", "tests", "tools"):
        for item in (ROOT / name).rglob("*"):
            relative = item.relative_to(ROOT)
            if "__pycache__" in relative.parts or not item.is_file():
                continue
            destination = source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, destination)
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Release Test")
    _git(source, "config", "user.email", "release-test@example.invalid")
    _git(source, "add", ".")
    _git(source, "commit", "-q", "-m", "actual source fixture")
    return source


def test_release_builder_is_reproducible_and_excludes_untracked_runtime_state(tmp_path: Path) -> None:
    source = _repository(tmp_path)
    first = build_release(source, tmp_path / "first.tar.gz", "cra-no-action-test-release-a")
    second = build_release(source, tmp_path / "second.tar.gz", "cra-no-action-test-release-a")

    assert first["archive_sha256"] == second["archive_sha256"]
    assert first["source_tree_sha256"] == second["source_tree_sha256"]
    assert first["systemd_instance"] == "cra-no-action-test-release-a"
    assert first["expected_install_root"] == "/opt/stream-recovery-control/releases/cra-no-action-test-release-a"
    assert first["production_action_enabled"] is False
    assert first["control_capability_count"] == 0
    assert first["physical_effect_count"] == 0
    assert first["central_command_store_packaged"] is False
    assert first["effect_adapter_packaged"] is False
    assert first["dell_agent_packaged"] is False
    assert first["secret_values_recorded"] is False
    with tarfile.open(tmp_path / "first.tar.gz", "r:gz") as archive:
        names = archive.getnames()
    assert not any("dell_recovery_agent" in name for name in names)
    assert not any("runtime_boundary" in name for name in names)
    assert not any(name.endswith("src/cra_authority/storage.py") for name in names)


def test_release_builder_rejects_dirty_tree_and_private_material(tmp_path: Path) -> None:
    source = _repository(tmp_path)
    (source / "untracked.txt").write_text("dirty", encoding="utf-8")
    with pytest.raises(ValueError, match="SOURCE_WORKTREE_NOT_CLEAN"):
        build_release(source, tmp_path / "dirty.tar.gz", "cra-no-action-test-release-b")
    (source / "untracked.txt").unlink()
    private = source / "src/private.pem"
    private_marker = "-----BEGIN " + "PRIVATE KEY-----"
    private.write_text(f"{private_marker}\nnot-a-real-key\n", encoding="utf-8")
    _git(source, "add", ".")
    _git(source, "commit", "-q", "-m", "bad fixture")
    with pytest.raises(ValueError, match="RELEASE_FORBIDDEN_FILE"):
        build_release(source, tmp_path / "bad.tar.gz", "cra-no-action-test-release-c")


def test_actual_no_action_release_is_import_closed_without_central_command_store(tmp_path: Path) -> None:
    source = _actual_source_repository(tmp_path)
    archive_path = tmp_path / "actual.tar.gz"
    build_release(source, archive_path, "cra-no-action-actual-closure")
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    with tarfile.open(archive_path, "r:gz") as archive:
        archive.extractall(extracted, filter="data")
    roots = [item for item in extracted.iterdir() if item.is_dir()]
    assert len(roots) == 1
    release_root = roots[0]
    assert not (release_root / "src/cra_authority/storage.py").exists()

    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(release_root / "src")
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """import importlib.util
import pathlib
import cra_authority.no_action_store
import cra_authority.projection_pull
import cra_authority.provision
import cra_authority.runtime
import cra_authority.recovery_safety
import cra_no_action_soak.gate
import cra_no_action_soak.host_status
import cra_no_action_soak.host_status_pull
import cra_no_action_soak.operator_status
import cra_no_action_soak.sample
root = pathlib.Path.cwd().resolve()
assert root in pathlib.Path(cra_authority.runtime.__file__).resolve().parents
assert importlib.util.find_spec('cra_authority.storage') is None
""",
        ],
        cwd=release_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert probe.stderr == ""
    for module in (
        "cra_authority.projection_pull",
        "cra_authority.provision",
        "cra_authority.runtime",
        "cra_no_action_soak.gate",
        "cra_no_action_soak.host_status_pull",
        "cra_no_action_soak.operator_status",
        "cra_no_action_soak.sample",
        "cra_no_action_soak.recovery_soak",
        "cra_no_action_soak.recovery_observer",
        "tools.freeze_recovery_soak_checkpoint",
    ):
        subprocess.run(
            [sys.executable, "-m", module, "--help"],
            cwd=release_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )


def test_installer_verifies_production_soak_modules() -> None:
    expected = {
        "cra_no_action_soak.gate",
        "cra_no_action_soak.host_status_pull",
        "cra_no_action_soak.sample",
    }

    assert expected <= set(IMPORTS["cra"])
    assert expected <= set(CLI_MODULES["cra"])
