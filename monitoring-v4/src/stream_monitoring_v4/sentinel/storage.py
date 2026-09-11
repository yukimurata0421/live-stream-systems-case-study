from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def filesystem_status(
    path: Path,
    *,
    minimum_free_bytes: int,
    minimum_free_percent: float,
    minimum_free_inode_percent: float,
) -> dict[str, Any]:
    try:
        if not path.is_dir():
            raise NotADirectoryError(path)
        values = os.statvfs(path)
        device = int(path.stat().st_dev)
        total_bytes = int(values.f_blocks) * int(values.f_frsize)
        free_bytes = int(values.f_bavail) * int(values.f_frsize)
        free_percent = 100.0 * free_bytes / total_bytes if total_bytes > 0 else 0.0
        total_inodes = int(values.f_files)
        free_inodes = int(values.f_favail)
        free_inode_percent = (
            100.0 * free_inodes / total_inodes if total_inodes > 0 else 100.0
        )
        sufficient = (
            free_bytes >= max(0, int(minimum_free_bytes))
            and free_percent >= max(0.0, float(minimum_free_percent))
            and free_inode_percent >= max(0.0, float(minimum_free_inode_percent))
        )
        return {
            "readable": True,
            "path": str(path),
            "device": device,
            "total_bytes": total_bytes,
            "free_bytes": free_bytes,
            "free_percent": round(free_percent, 3),
            "free_inodes": free_inodes,
            "free_inode_percent": round(free_inode_percent, 3),
            "sufficient": sufficient,
        }
    except (OSError, ValueError, ZeroDivisionError) as exc:
        return {
            "readable": False,
            "path": str(path),
            "sufficient": False,
            "error": type(exc).__name__,
        }


def filesystems_independent(
    first: Mapping[str, Any], second: Mapping[str, Any]
) -> bool:
    return (
        first.get("readable") is True
        and second.get("readable") is True
        and type(first.get("device")) is int
        and type(second.get("device")) is int
        and first["device"] != second["device"]
    )
