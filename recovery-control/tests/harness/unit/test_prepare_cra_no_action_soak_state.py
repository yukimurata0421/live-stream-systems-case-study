from __future__ import annotations

import os
from pathlib import Path

import pytest

from tools.prepare_cra_no_action_soak_state import prepare


def installed_release(parent: Path, epoch: str) -> None:
    parent.mkdir(mode=0o700)
    root = parent / epoch
    root.mkdir(mode=0o700)
    (root / "collector-profile.json").write_text("{}\n", encoding="utf-8")
    (root / "collector-profile.json").chmod(0o400)
    root.chmod(0o500)


def test_prepare_creates_exact_owner_only_state_root(tmp_path: Path) -> None:
    epoch = "candidate-20260901T0832JST-24h-v1"
    installed = tmp_path / "installed"
    state = tmp_path / "state"
    installed_release(installed, epoch)
    state.mkdir(mode=0o700)

    result = prepare(
        epoch=epoch,
        installed_parent=installed,
        state_parent=state,
        owner_uid=os.getuid(),
        owner_gid=os.getgid(),
        check_only=False,
    )

    assert result["status"] == "PASS"
    assert result["created"] is True
    assert (state / epoch).stat().st_mode & 0o777 == 0o700


def test_prepare_check_only_rejects_missing_state_root(tmp_path: Path) -> None:
    epoch = "candidate-20260901T0832JST-24h-v1"
    installed = tmp_path / "installed"
    state = tmp_path / "state"
    installed_release(installed, epoch)
    state.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="STATE_ROOT_MISSING"):
        prepare(
            epoch=epoch,
            installed_parent=installed,
            state_parent=state,
            owner_uid=os.getuid(),
            owner_gid=os.getgid(),
            check_only=True,
        )


@pytest.mark.parametrize(
    ("epoch", "expected"),
    [
        ("../escape", "SOAK_EPOCH_INVALID"),
        ("bad/child", "SOAK_EPOCH_INVALID"),
        ("", "SOAK_EPOCH_INVALID"),
    ],
)
def test_prepare_rejects_epoch_path_escape(tmp_path: Path, epoch: str, expected: str) -> None:
    installed = tmp_path / "installed"
    state = tmp_path / "state"
    installed.mkdir(mode=0o700)
    state.mkdir(mode=0o700)

    with pytest.raises(ValueError, match=expected):
        prepare(
            epoch=epoch,
            installed_parent=installed,
            state_parent=state,
            owner_uid=os.getuid(),
            owner_gid=os.getgid(),
            check_only=False,
        )


def test_prepare_rejects_symlink_state_root(tmp_path: Path) -> None:
    epoch = "candidate-20260901T0832JST-24h-v1"
    installed = tmp_path / "installed"
    state = tmp_path / "state"
    installed_release(installed, epoch)
    state.mkdir(mode=0o700)
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    (state / epoch).symlink_to(target, target_is_directory=True)

    with pytest.raises(ValueError, match="STATE_ROOT_NOT_PLAIN_DIRECTORY"):
        prepare(
            epoch=epoch,
            installed_parent=installed,
            state_parent=state,
            owner_uid=os.getuid(),
            owner_gid=os.getgid(),
            check_only=False,
        )


def test_prepare_rejects_preexisting_wrong_mode(tmp_path: Path) -> None:
    epoch = "candidate-20260901T0832JST-24h-v1"
    installed = tmp_path / "installed"
    state = tmp_path / "state"
    installed_release(installed, epoch)
    state.mkdir(mode=0o700)
    (state / epoch).mkdir(mode=0o755)

    with pytest.raises(ValueError, match="STATE_ROOT_MODE_MISMATCH"):
        prepare(
            epoch=epoch,
            installed_parent=installed,
            state_parent=state,
            owner_uid=os.getuid(),
            owner_gid=os.getgid(),
            check_only=False,
        )
