from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from stream_monitoring_v4.runtime.backup_integrity import (
    backup_directory_device,
    copy_verified_backup,
    matching_backup_copies,
    verified_backup_status,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Select a stable dual-copy restore input")
    result.add_argument("--backup-dir", type=Path, required=True)
    result.add_argument("--independent-backup-dir", type=Path, required=True)
    result.add_argument("--work-dir", type=Path, required=True)
    result.add_argument("--backup-max-age-sec", type=int, default=27 * 3600)
    result.add_argument("--now-ts", type=int, default=None)
    return result


def run(args: argparse.Namespace, *, now_ts: int) -> dict[str, object]:
    max_age_sec = int(args.backup_max_age_sec)
    if max_age_sec < 3600:
        raise ValueError("--backup-max-age-sec must be at least 3600")
    if backup_directory_device(args.backup_dir) == backup_directory_device(
        args.independent_backup_dir
    ):
        raise RuntimeError("restore selection requires physically distinct filesystems")
    primary = verified_backup_status(
        args.backup_dir, now_ts=now_ts, max_age_sec=max_age_sec
    )
    independent = verified_backup_status(
        args.independent_backup_dir, now_ts=now_ts, max_age_sec=max_age_sec
    )
    if not matching_backup_copies(primary, independent):
        raise RuntimeError("restore selection requires matching fresh verified copies")
    work = Path(args.work_dir)
    copy_verified_backup(primary, work / "selected.dump", mode=0o640)
    name = Path(str(primary["path"])).name
    digest = str(primary["sha256"])
    atomic_write_text(work / "backup-name", f"{name}\n", mode=0o640)
    atomic_write_text(work / "backup-sha256", f"{digest}\n", mode=0o640)
    return {
        "schema": "monitoring_v4.restore_selection.v1",
        "backup_name": name,
        "backup_sha256": digest,
        "notification_delivery_enabled": False,
        "runtime_mutation_enabled": False,
    }


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    now_ts = int(time.time()) if args.now_ts is None else int(args.now_ts)
    print(json.dumps(run(args, now_ts=now_ts), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
