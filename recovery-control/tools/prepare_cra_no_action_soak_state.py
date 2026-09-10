from __future__ import annotations

import argparse
import grp
import json
import os
import pwd
import re
import stat
from pathlib import Path

EPOCH = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


def _directory(path: Path, *, label: str) -> os.stat_result:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink():
        raise ValueError(f"{label}_NOT_PLAIN_DIRECTORY")
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise ValueError(f"{label}_WRITABLE_BY_GROUP_OR_OTHER")
    return metadata


def _validate_installed_root(path: Path) -> None:
    _directory(path, label="INSTALLED_ROOT")
    profile = path / "collector-profile.json"
    metadata = profile.lstat()
    if not stat.S_ISREG(metadata.st_mode) or profile.is_symlink():
        raise ValueError("COLLECTOR_PROFILE_NOT_REGULAR")
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise ValueError("COLLECTOR_PROFILE_WRITABLE_BY_GROUP_OR_OTHER")


def prepare(
    *,
    epoch: str,
    installed_parent: Path,
    state_parent: Path,
    owner_uid: int,
    owner_gid: int,
    check_only: bool,
) -> dict[str, object]:
    if EPOCH.fullmatch(epoch) is None:
        raise ValueError("SOAK_EPOCH_INVALID")
    if not installed_parent.is_absolute() or not state_parent.is_absolute():
        raise ValueError("SOAK_PARENT_NOT_ABSOLUTE")
    _directory(installed_parent, label="INSTALLED_PARENT")
    _directory(state_parent, label="STATE_PARENT")
    _validate_installed_root(installed_parent / epoch)

    state_root = state_parent / epoch
    created = False
    try:
        metadata = state_root.lstat()
    except FileNotFoundError:
        if check_only:
            raise ValueError("STATE_ROOT_MISSING") from None
        descriptor = os.open(state_parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.mkdir(epoch, mode=0o700, dir_fd=descriptor)
            os.chown(epoch, owner_uid, owner_gid, dir_fd=descriptor, follow_symlinks=False)
            os.fsync(descriptor)
            created = True
        finally:
            os.close(descriptor)
        metadata = state_root.lstat()

    if not stat.S_ISDIR(metadata.st_mode) or state_root.is_symlink():
        raise ValueError("STATE_ROOT_NOT_PLAIN_DIRECTORY")
    if metadata.st_uid != owner_uid or metadata.st_gid != owner_gid:
        raise ValueError("STATE_ROOT_OWNER_MISMATCH")
    if stat.S_IMODE(metadata.st_mode) != 0o700:
        raise ValueError("STATE_ROOT_MODE_MISMATCH")

    return {
        "schema": "cra.no_action_soak_state_preparation.v1",
        "status": "PASS",
        "epoch": epoch,
        "created": created,
        "state_root": str(state_root),
        "owner_uid": owner_uid,
        "owner_gid": owner_gid,
        "mode": "0700",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely prepare one immutable CRA soak state root")
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--installed-parent", type=Path, default=Path("/opt/cra-no-action-soak"))
    parser.add_argument("--state-parent", type=Path, default=Path("/var/lib/cra-no-action-soak"))
    parser.add_argument("--owner", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    owner_uid = pwd.getpwnam(args.owner).pw_uid
    owner_gid = grp.getgrnam(args.group).gr_gid
    result = prepare(
        epoch=args.epoch,
        installed_parent=args.installed_parent,
        state_parent=args.state_parent,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        check_only=args.check_only,
    )
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
