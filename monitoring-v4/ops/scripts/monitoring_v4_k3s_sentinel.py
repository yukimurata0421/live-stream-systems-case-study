#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from stream_monitoring_v4.sentinel import kube
from stream_monitoring_v4.sentinel.continuity import previous_continuity
from stream_monitoring_v4.sentinel.contracts import (
    REPORT_SCHEMA,
    REQUIRED_COMPONENT_COUNTS,
    SENTINEL_SCHEMA,
)
from stream_monitoring_v4.sentinel.evidence import (
    backup_status,
    report_status,
    restore_verification_status,
    youtube_api_status,
)
from stream_monitoring_v4.sentinel.runner import collect, write_atomic
from stream_monitoring_v4.sentinel.storage import (
    filesystem_status,
    filesystems_independent,
)


# Compatibility aliases for existing read-only diagnostics and fixtures.
_command = kube.command
_pods = kube.pods
_release_identity = kube.release_identity
_report = report_status
_backup_status = backup_status
_restore_verification_status = restore_verification_status
_youtube_api_status = youtube_api_status
_filesystem_status = filesystem_status
_filesystems_independent = filesystems_independent
_previous_continuity = previous_continuity


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Read-only host sentinel for the Monitoring v4 k3s subsystem"
    )
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--report", type=Path, required=True)
    result.add_argument("--report-max-age-sec", type=int, default=420)
    result.add_argument(
        "--youtube-api-evidence",
        type=Path,
        default=Path(
            "/var/lib/stream-monitoring-v4/.state/k3s/"
            "youtube-api-source/youtube_api_evidence.json"
        ),
    )
    result.add_argument("--youtube-api-max-age-sec", type=int, default=300)
    result.add_argument(
        "--backup-dir",
        type=Path,
        default=Path("/var/lib/stream-monitoring-v4/.state/postgres-backups"),
    )
    result.add_argument(
        "--independent-backup-dir",
        type=Path,
        default=Path("/var/backups/stream-monitoring-v4/stream-v4-postgres"),
    )
    result.add_argument("--backup-max-age-sec", type=int, default=27 * 3600)
    result.add_argument(
        "--restore-verification-dir",
        type=Path,
        default=Path("/var/lib/stream-monitoring-v4/.state/restore-verifications"),
    )
    result.add_argument(
        "--restore-verification-max-age-sec",
        type=int,
        default=27 * 3600,
    )
    result.add_argument(
        "--restore-verification-pending-grace-sec",
        type=int,
        default=3600,
    )
    result.add_argument(
        "--database-storage-dir",
        type=Path,
        default=Path("/var/lib/rancher/k3s/storage"),
    )
    result.add_argument("--minimum-free-bytes", type=int, default=2 * 1024**3)
    result.add_argument("--minimum-free-percent", type=float, default=10.0)
    result.add_argument("--minimum-free-inode-percent", type=float, default=5.0)
    result.add_argument("--kubectl", default="/usr/local/bin/k3s")
    return result


def _validate_thresholds(args: argparse.Namespace) -> None:
    if args.minimum_free_bytes < 0:
        raise ValueError("--minimum-free-bytes must not be negative")
    for name, value in (
        ("--minimum-free-percent", args.minimum_free_percent),
        ("--minimum-free-inode-percent", args.minimum_free_inode_percent),
    ):
        if not math.isfinite(value) or not 0.0 <= value <= 100.0:
            raise ValueError(f"{name} must be finite and between 0 and 100")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    _validate_thresholds(args)
    assessment = collect(args)
    write_atomic(args.output, assessment.payload)
    print(json.dumps(assessment.payload, sort_keys=True, separators=(",", ":")))
    return 0 if assessment.healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
