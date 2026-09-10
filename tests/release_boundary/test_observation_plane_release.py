from __future__ import annotations

import json
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from tools.build_observation_plane_release import REQUIRED_FILES, build_release

ROOT = Path(__file__).resolve().parents[2]


def _git(path: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _source(tmp_path: Path, component: str) -> Path:
    source = tmp_path / f"source-{component}"
    source.mkdir(parents=True)
    for relative in REQUIRED_FILES[component]:
        destination = source / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Release Test")
    _git(source, "config", "user.email", "release-test@example.invalid")
    _git(source, "add", ".")
    _git(source, "commit", "-q", "-m", "immutable observation release source")
    return source


@pytest.mark.parametrize(
    ("component", "release_id"),
    (
        ("dell", "dell-observation-test-release-a"),
        ("arena", "arena-cra-projection-test-release-a"),
    ),
)
def test_observation_release_is_reproducible_minimal_and_no_action(
    tmp_path: Path,
    component: str,
    release_id: str,
) -> None:
    source = _source(tmp_path, component)
    first = build_release(source, tmp_path / f"{component}-first.tar.gz", release_id, component)
    second = build_release(source, tmp_path / f"{component}-second.tar.gz", release_id, component)

    assert first["archive_sha256"] == second["archive_sha256"]
    assert first["source_tree_sha256"] == second["source_tree_sha256"]
    assert first["production_action_enabled"] is False
    assert first["control_capability_count"] == 0
    assert first["physical_effect_count"] == 0
    assert first["command_route_packaged"] is False
    assert first["private_key_packaged"] is False

    extracted = tmp_path / f"extracted-{component}"
    with tarfile.open(first["archive_path"], "r:gz") as archive:
        archive.extractall(extracted, filter="data")
    roots = list(extracted.iterdir())
    assert len(roots) == 1
    release_root = roots[0]
    manifest = json.loads((release_root / "release_manifest.json").read_text(encoding="utf-8"))
    assert manifest["source_commit"] == _git(source, "rev-parse", "HEAD")
    assert not list(release_root.rglob("*.pem"))
    assert not list(release_root.rglob("*.sqlite3"))
    assert not (release_root / "src/cra_authority/storage.py").exists()
    assert not (release_root / "src/dell_recovery_agent/execution.py").exists()
    assert not (release_root / "src/cra_authority/recovery_safety.py").exists()
    assert not (release_root / "src/runtime_boundary").exists()
    assert (release_root / "src/cra_dell_recovery/bounded_http.py").is_file()
    assert (release_root / "src/cra_dell_recovery/reloading_tls_server.py").is_file()
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(release_root / "src")
    # pytest-cov enables subprocess collection through inherited COV_CORE_*
    # variables, and coverage itself imports sqlite3. This child verifies the
    # release import closure, so remove harness instrumentation from the child
    # instead of misclassifying coverage's SQLite use as a packaged dependency.
    for name in tuple(environment):
        if name.startswith("COV_CORE_") or name == "COVERAGE_PROCESS_START":
            environment.pop(name)
    subprocess.run(
        [
            str(ROOT / ".venv/bin/python"),
            "-c",
            "import sys; import cra_no_action_soak.recovery_observer; "
            "assert 'sqlite3' not in sys.modules; assert 'runtime_boundary' not in sys.modules",
        ],
        cwd=release_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            str(ROOT / ".venv/bin/python"),
            "-c",
            (
                "import cra_dell_recovery.observation; "
                + (
                    "import dell_recovery_agent.observation_server"
                    if component == "dell"
                    else "import monitoring_projection.live_adapter, monitoring_projection.producer"
                )
            ),
        ],
        cwd=release_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    cli_modules = (
        ("dell_recovery_agent.observation_server", "cra_no_action_soak.host_status_publisher")
        if component == "dell"
        else (
            "monitoring_projection.dell_observation_pull",
            "monitoring_projection.live_adapter",
            "monitoring_projection.producer",
            "monitoring_projection.server",
            "cra_no_action_soak.host_status_publisher",
            "cra_no_action_soak.host_status_pull",
        )
    )
    for module in (*cli_modules, "cra_no_action_soak.recovery_observer"):
        probe = subprocess.run(
            [
                str(ROOT / ".venv/bin/python"),
                "-W",
                "error::RuntimeWarning",
                "-m",
                module,
                "--help",
            ],
            cwd=release_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        assert probe.stderr == ""


def test_observation_release_rejects_dirty_source_forbidden_tracked_state_and_existing_output(tmp_path: Path) -> None:
    source = _source(tmp_path, "arena")
    dirty = source / "dirty.txt"
    dirty.write_text("dirty", encoding="utf-8")
    with pytest.raises(ValueError, match="OBSERVATION_RELEASE_SOURCE_WORKTREE_NOT_CLEAN"):
        build_release(
            source,
            tmp_path / "dirty.tar.gz",
            "arena-cra-projection-test-release-b",
            "arena",
        )
    dirty.unlink()

    forbidden = source / "runtime.sqlite3"
    forbidden.write_bytes(b"not-a-real-database")
    _git(source, "add", "runtime.sqlite3")
    _git(source, "commit", "-q", "-m", "tracked runtime state negative control")
    with pytest.raises(ValueError, match="OBSERVATION_RELEASE_FORBIDDEN_FILE:runtime.sqlite3"):
        build_release(
            source,
            tmp_path / "forbidden.tar.gz",
            "arena-cra-projection-test-release-c",
            "arena",
        )

    clean = _source(tmp_path / "clean", "dell")
    output = tmp_path / "existing.tar.gz"
    output.write_bytes(b"existing")
    with pytest.raises(ValueError, match="OBSERVATION_RELEASE_OUTPUT_EXISTS"):
        build_release(clean, output, "dell-observation-test-release-b", "dell")
