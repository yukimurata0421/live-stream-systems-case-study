from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SentinelEvidence:
    now_ts: int
    previous: Mapping[str, Any]
    k3s_active: bool
    k3s_detail: str
    pods_ok: bool
    pods: Mapping[str, Mapping[str, Any]]
    pods_error: str
    release_ok: bool
    release_identity: Mapping[str, str]
    release_error: str
    youtube_api_evidence: Mapping[str, Any]
    report: Mapping[str, Any]
    backup: Mapping[str, Any]
    independent_backup: Mapping[str, Any]
    restore_verification: Mapping[str, Any]
    database_filesystem: Mapping[str, Any]
    backup_filesystem: Mapping[str, Any]
    independent_backup_filesystem: Mapping[str, Any]
    minimum_free_bytes: int
    minimum_free_percent: float
    minimum_free_inode_percent: float


@dataclass(frozen=True)
class SentinelAssessment:
    payload: dict[str, Any]
    failed_checks: tuple[str, ...]

    @property
    def healthy(self) -> bool:
        return not self.failed_checks
