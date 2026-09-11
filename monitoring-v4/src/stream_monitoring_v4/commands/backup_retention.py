from __future__ import annotations

import argparse
import json
import os
import re
import stat
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.runtime.backup_integrity import (
    backup_directory_device,
    matching_backup_copies,
    verified_backup_status,
    verified_restore_status,
)


DAY = 86_400
_BACKUP_NAME = re.compile(r"stream-v4-([0-9]{8}T[0-9]{6}Z)\.dump")
_BACKUP_MEMBER_NAME = re.compile(
    r"stream-v4-[0-9]{8}T[0-9]{6}Z\.dump(?:\.sha256)?"
)
_MAX_BACKUP_MEMBERS = 20_000


@dataclass(frozen=True)
class BackupPair:
    name: str
    created_ts: int
    dump: os.stat_result
    checksum: os.stat_result


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Delete old Monitoring v4 backup pairs only behind a verified restore anchor"
    )
    result.add_argument("--backup-dir", type=Path, required=True)
    result.add_argument("--independent-backup-dir", type=Path, required=True)
    result.add_argument("--restore-verification-dir", type=Path, required=True)
    result.add_argument("--backup-max-age-sec", type=int, default=27 * 3600)
    result.add_argument("--primary-retention-days", type=int, default=35)
    result.add_argument("--independent-retention-days", type=int, default=90)
    result.add_argument("--minimum-preserved-sets", type=int, default=2)
    result.add_argument("--now-ts", type=int, default=None)
    return result


def _validate_arguments(args: argparse.Namespace) -> None:
    if int(args.backup_max_age_sec) < 3600:
        raise ValueError("--backup-max-age-sec must be at least 3600")
    for name, value in (
        ("--primary-retention-days", args.primary_retention_days),
        ("--independent-retention-days", args.independent_retention_days),
    ):
        if int(value) < 1:
            raise ValueError(f"{name} must be at least 1")
    if int(args.minimum_preserved_sets) < 2:
        raise ValueError("--minimum-preserved-sets must be at least 2")


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        stat.S_IFMT(value.st_mode),
    )


def _directory_device(root: Path) -> int:
    return backup_directory_device(root)


def _created_ts(name: str) -> int | None:
    match = _BACKUP_NAME.fullmatch(name)
    if match is None:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return int(parsed.timestamp())


def _pairs(root: Path) -> tuple[BackupPair, ...]:
    _directory_device(root)
    entries: dict[str, os.stat_result] = {}
    for path in Path(root).iterdir():
        if _BACKUP_MEMBER_NAME.fullmatch(path.name) is None:
            continue
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISREG(metadata.st_mode):
            entries[path.name] = metadata
            if len(entries) > _MAX_BACKUP_MEMBERS:
                raise RuntimeError("backup retention candidate limit exceeded")
    pairs: list[BackupPair] = []
    for name, dump in entries.items():
        created_ts = _created_ts(name)
        if created_ts is None:
            continue
        checksum = entries.get(f"{name}.sha256")
        if checksum is None:
            continue
        pairs.append(BackupPair(name, created_ts, dump, checksum))
    return tuple(sorted(pairs, key=lambda item: (item.created_ts, item.name)))


def _delete_pair(root: Path, pair: BackupPair) -> None:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(root, flags)
    try:
        root_before = Path(root).lstat()
        root_open = os.fstat(descriptor)
        if _identity(root_before) != _identity(root_open):
            raise RuntimeError("backup retention root changed before deletion")
        dump_now = os.stat(pair.name, dir_fd=descriptor, follow_symlinks=False)
        checksum_name = f"{pair.name}.sha256"
        checksum_now = os.stat(
            checksum_name,
            dir_fd=descriptor,
            follow_symlinks=False,
        )
        if _identity(dump_now) != _identity(pair.dump):
            raise RuntimeError(f"backup changed before deletion: {pair.name}")
        if _identity(checksum_now) != _identity(pair.checksum):
            raise RuntimeError(f"backup checksum changed before deletion: {pair.name}")
        os.unlink(checksum_name, dir_fd=descriptor)
        # Keep the data-bearing dump until the final destructive operation.
        # If the second unlink fails, an operator can regenerate the checksum;
        # deleting the dump first would make that failure unrecoverable.
        os.unlink(pair.name, dir_fd=descriptor)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _delete_expired(
    root: Path,
    *,
    now_ts: int,
    retention_days: int,
    minimum_preserved_sets: int,
    protected_name: str,
) -> list[str]:
    pairs = _pairs(root)
    minimum = max(2, int(minimum_preserved_sets))
    protected = {item.name for item in pairs[-minimum:]}
    protected.add(protected_name)
    cutoff = int(now_ts) - max(1, int(retention_days)) * DAY
    deleted: list[str] = []
    for pair in pairs:
        if pair.name in protected or pair.created_ts >= cutoff:
            continue
        _delete_pair(root, pair)
        deleted.append(pair.name)
    return deleted


def run_retention(args: argparse.Namespace, *, now_ts: int) -> dict[str, Any]:
    _validate_arguments(args)
    primary_device = _directory_device(args.backup_dir)
    independent_device = _directory_device(args.independent_backup_dir)
    if primary_device == independent_device:
        raise RuntimeError("backup retention requires physically distinct filesystems")
    max_age_sec = max(3600, int(args.backup_max_age_sec))
    primary = verified_backup_status(
        args.backup_dir,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
    )
    independent = verified_backup_status(
        args.independent_backup_dir,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
    )
    if not matching_backup_copies(primary, independent):
        raise RuntimeError("backup retention requires matching fresh verified copies")
    restore = verified_restore_status(
        args.restore_verification_dir,
        backup=primary,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
        pending_grace_sec=0,
    )
    if restore.get("verified") is not True:
        raise RuntimeError("backup retention requires an exact full restore verification")
    protected_name = Path(str(primary["path"])).name
    primary_deleted = _delete_expired(
        args.backup_dir,
        now_ts=now_ts,
        retention_days=args.primary_retention_days,
        minimum_preserved_sets=args.minimum_preserved_sets,
        protected_name=protected_name,
    )
    independent_deleted = _delete_expired(
        args.independent_backup_dir,
        now_ts=now_ts,
        retention_days=args.independent_retention_days,
        minimum_preserved_sets=args.minimum_preserved_sets,
        protected_name=protected_name,
    )
    return {
        "schema": "monitoring_v4.backup_file_retention.v1",
        "completed_at": utc_text(now_ts),
        "restore_anchor_backup": protected_name,
        "restore_anchor_sha256": primary["sha256"],
        "minimum_preserved_sets": max(2, int(args.minimum_preserved_sets)),
        "primary_retention_days": max(1, int(args.primary_retention_days)),
        "independent_retention_days": max(1, int(args.independent_retention_days)),
        "primary_deleted": primary_deleted,
        "independent_deleted": independent_deleted,
        "notification_delivery_enabled": False,
        "runtime_mutation_enabled": False,
    }


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    _validate_arguments(args)
    now_ts = int(time.time()) if args.now_ts is None else int(args.now_ts)
    payload = run_retention(args, now_ts=now_ts)
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
