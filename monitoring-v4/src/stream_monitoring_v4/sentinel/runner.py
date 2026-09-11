from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from stream_monitoring_v4.runtime.atomic_file import atomic_write_text

from . import kube
from .assessment import SentinelAssessment, SentinelEvidence, assess
from .continuity import previous_status
from .evidence import (
    backup_status,
    report_status,
    restore_verification_status,
    youtube_api_status,
)
from .storage import filesystem_status


def collect(args: Any, *, now_ts: int | None = None) -> SentinelAssessment:
    checked_ts = int(time.time()) if now_ts is None else int(now_ts)
    previous = previous_status(args.output)
    k3s_active, k3s_detail = kube.command(["/usr/bin/systemctl", "is-active", "k3s"])
    pods_ok, pods, pods_error = (
        kube.pods(args.kubectl) if k3s_active else (False, {}, "k3s_inactive")
    )
    release_ok, release_identity, release_error = (
        kube.release_identity(args.kubectl)
        if k3s_active
        else (False, {}, "k3s_inactive")
    )
    youtube_api_evidence = youtube_api_status(
        args.youtube_api_evidence,
        now_ts=checked_ts,
        max_age_sec=max(120, args.youtube_api_max_age_sec),
    )
    report = report_status(
        args.report,
        now_ts=checked_ts,
        max_age_sec=max(60, args.report_max_age_sec),
    )
    backup = backup_status(
        args.backup_dir,
        now_ts=checked_ts,
        max_age_sec=args.backup_max_age_sec,
    )
    independent_backup = backup_status(
        args.independent_backup_dir,
        now_ts=checked_ts,
        max_age_sec=args.backup_max_age_sec,
    )
    restore_verification = restore_verification_status(
        args.restore_verification_dir,
        backup=backup,
        now_ts=checked_ts,
        max_age_sec=max(3600, args.restore_verification_max_age_sec),
        pending_grace_sec=max(0, args.restore_verification_pending_grace_sec),
    )
    filesystem_args = {
        "minimum_free_bytes": max(0, args.minimum_free_bytes),
        "minimum_free_percent": max(0.0, args.minimum_free_percent),
        "minimum_free_inode_percent": max(0.0, args.minimum_free_inode_percent),
    }
    database_filesystem = filesystem_status(
        args.database_storage_dir,
        **filesystem_args,
    )
    backup_filesystem = filesystem_status(args.backup_dir, **filesystem_args)
    independent_backup_filesystem = filesystem_status(
        args.independent_backup_dir,
        **filesystem_args,
    )
    return assess(
        SentinelEvidence(
            now_ts=checked_ts,
            previous=previous,
            k3s_active=k3s_active,
            k3s_detail=k3s_detail,
            pods_ok=pods_ok,
            pods=pods,
            pods_error=pods_error,
            release_ok=release_ok,
            release_identity=release_identity,
            release_error=release_error,
            youtube_api_evidence=youtube_api_evidence,
            report=report,
            backup=backup,
            independent_backup=independent_backup,
            restore_verification=restore_verification,
            database_filesystem=database_filesystem,
            backup_filesystem=backup_filesystem,
            independent_backup_filesystem=independent_backup_filesystem,
            **filesystem_args,
        )
    )


def write_atomic(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n",
        mode=0o640,
    )
