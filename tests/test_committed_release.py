import importlib.util
import json
import os
from pathlib import Path
import subprocess

import pytest

SPEC = importlib.util.spec_from_file_location("release_tool", Path(__file__).parents[1] / "ops/scripts/stream_v3_release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


def command(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args]).decode().strip()


@pytest.fixture
def source(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    command(repo, "init", "-q")
    command(repo, "config", "user.name", "Release Test")
    command(repo, "config", "user.email", "test@example.invalid")
    (repo / "entry.py").write_text("print('committed')\n")
    (repo / "entry.py").chmod(0o755)
    (repo / "profile.json").write_text(json.dumps({
        "schema": "stream.release_profile.v1", "name": "test",
        "files": [{"source": "entry.py", "destination": "bin/entry.py"}],
        "runtime_bindings": [{"destination": "config", "key": "config"}]}))
    command(repo, "add", ".")
    command(repo, "commit", "-qm", "Test fixture")
    return repo, command(repo, "rev-parse", "HEAD")


def build(source, tmp_path):
    repo, commit = source
    output = tmp_path / "artifact"
    return output, release.build(repo, commit, "profile.json", output)


def test_only_committed_blobs_and_reproducible(source, tmp_path):
    repo, commit = source
    first, m = build(source, tmp_path)
    (repo / "entry.py").write_text("dirty, must not ship")
    (repo / "profile.json").write_text("dirty profile")
    second = tmp_path / "second"
    n = release.build(repo, commit, "profile.json", second)
    assert n == m
    assert (first / "bin/entry.py").read_bytes() == (second / "bin/entry.py").read_bytes()
    assert release.verify(second, m["artifact_sha256"])["commit"] == commit


@pytest.mark.parametrize("mutation", ["content", "mode", "extra", "symlink", "missing", "manifest"])
def test_tampering_rejected(source, tmp_path, mutation):
    root, m = build(source, tmp_path)
    p = root / "bin/entry.py"
    if mutation == "content":
        p.chmod(0o755)
        p.write_text("modified")
    elif mutation == "mode":
        p.chmod(0o644)
    elif mutation == "extra":
        (root / "injected.py").write_text("extra")
    elif mutation == "symlink":
        p.unlink()
        p.symlink_to(tmp_path / "outside")
    elif mutation == "missing":
        p.unlink()
    else:
        p = root / "release.json"
        p.chmod(0o644)
        p.write_text(p.read_text().replace('"name":"test"', '"name":"forged"'))
    with pytest.raises(ValueError):
        release.verify(root, m["artifact_sha256"])


@pytest.mark.parametrize("destination", ["../escape", "/absolute", "a/../b", "a//b", "release.json"])
def test_unsafe_profile_never_materializes(source, tmp_path, destination):
    repo, _ = source
    p = repo / "profile.json"
    d = json.loads(p.read_text())
    d["files"][0]["destination"] = destination
    p.write_text(json.dumps(d))
    command(repo, "add", ".")
    command(repo, "commit", "-qm", "Bad fixture")
    with pytest.raises(ValueError):
        release.build(repo, command(repo, "rev-parse", "HEAD"), "profile.json", tmp_path / "artifact")
    assert not (tmp_path / "artifact").exists()


def test_committed_symlink_is_rejected(source, tmp_path):
    repo, _ = source
    (repo / "entry.py").unlink()
    (repo / "entry.py").symlink_to("elsewhere")
    command(repo, "add", ".")
    command(repo, "commit", "-qm", "Symlink fixture")
    with pytest.raises(ValueError):
        release.build(repo, command(repo, "rev-parse", "HEAD"), "profile.json", tmp_path / "artifact")
    assert not list(tmp_path.glob(".release-build-*"))


def test_activation_pins_current_and_artifact_and_requires_binding(source, tmp_path):
    root, m = build(source, tmp_path)
    old = tmp_path / "previous"
    old.mkdir()
    pointer = tmp_path / "current"
    pointer.symlink_to(old)
    with pytest.raises(ValueError):
        release.activate(root, pointer, old, m["artifact_sha256"])
    config = tmp_path / "private-config"
    config.mkdir()
    (config / "secret").write_text("never copied")
    release.bind(root, [("config", str(config))], m["artifact_sha256"])
    with pytest.raises(ValueError):
        release.activate(root, pointer, old, "0" * 64)
    with pytest.raises(ValueError):
        release.activate(root, pointer, tmp_path / "unexpected", m["artifact_sha256"])
    assert pointer.resolve() == old
    receipt = release.activate(root, pointer, old, m["artifact_sha256"])
    assert pointer.resolve() == root
    assert receipt["service_restarted"] is False
    assert receipt["natural_execution_verified"] is False
    assert not any("secret" in name for name in m["files"])
    with pytest.raises(ValueError):
        release.activate(root, pointer, old, m["artifact_sha256"])


def test_partial_runtime_binding_cannot_be_activated(source, tmp_path):
    root, m = build(source, tmp_path)
    with pytest.raises(ValueError):
        release.bind(root, [], m["artifact_sha256"])
    with pytest.raises(ValueError):
        release.bind(root, [("config", "relative")], m["artifact_sha256"])


def test_deployment_entry_builds_only_without_touching_services_pointer_or_state(source, tmp_path):
    repo, commit = source
    root = Path(__file__).parents[1]
    commands = tmp_path / "commands"
    commands.mkdir()
    effect_log = tmp_path / "unexpected-effects"
    for name in ("sudo", "systemctl", "ssh", "kubectl", "docker"):
        stub = commands / name
        stub.write_text('#!/bin/sh\nprintf "%s\\n" "$0" >> "$EFFECT_LOG"\nexit 97\n')
        stub.chmod(0o755)
    previous = tmp_path / "previous"
    previous.mkdir()
    pointer = tmp_path / "current"
    pointer.symlink_to(previous)
    state_root = tmp_path / "state"
    state_root.mkdir()
    state = state_root / "history"
    state.write_text("incident/outbox/receipt history")
    output = tmp_path / "built"
    env = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"],
               EFFECT_LOG=str(effect_log), STREAM_V3_REPO_ROOT=str(repo), STREAM_V3_CURRENT_LINK=str(pointer),
               STREAM_V3_RELEASE_ROOT=str(tmp_path / "releases"), STREAM_RUNTIME_STATE_DIR=str(state_root))
    result = subprocess.run(["bash", str(root / "ops/scripts/deploy_stream_v3_monitoring_release.sh"),
        str(repo), commit, "profile.json", str(output)], env=env, cwd=tmp_path,
        capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    manifest = json.loads(result.stdout)
    assert release.verify(output, manifest["artifact_sha256"])["commit"] == commit
    assert pointer.resolve() == previous
    assert state.read_text() == "incident/outbox/receipt history"
    assert sorted(x.name for x in state_root.iterdir()) == ["history"]
    assert not effect_log.exists()
