from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.storage.publications import (
    ArtifactPublication,
    PUBLIC_SAFE_ARTIFACT_KEY,
)

from .public_safe import public_safe_bytes, write_public_safe_atomic


class PublicArtifactPublicationRepository(Protocol):
    def reconcilable_artifact_publication(
        self,
        artifact_key: str,
    ) -> ArtifactPublication | None: ...

    def mark_artifact_publication_published(
        self,
        publication_id: str,
        *,
        expected_payload_sha256: str,
        published_at: str,
    ) -> bool: ...

    def record_artifact_publication_failure(
        self,
        publication_id: str,
        *,
        attempted_at: str,
        error: str,
    ) -> bool: ...


@dataclass(frozen=True)
class PublicationResult:
    publication_id: str
    cycle_id: str
    changed: bool


def _exact_regular_file(path: Path, expected: bytes) -> bool:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != len(expected):
            return False
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = -1
            actual = stream.read(len(expected) + 1)
        if actual != expected:
            return False
        return True
    except OSError:
        return False
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def reconcile_public_safe_publication(
    repository: PublicArtifactPublicationRepository,
    output_path: Path,
    *,
    now_ts: int,
) -> PublicationResult | None:
    publication = repository.reconcilable_artifact_publication(
        PUBLIC_SAFE_ARTIFACT_KEY
    )
    if publication is None:
        return None
    attempted_at = utc_text(int(now_ts))
    expected = public_safe_bytes(publication.payload)
    exact_before = _exact_regular_file(output_path, expected)
    if exact_before and publication.state == "published":
        return PublicationResult(
            publication_id=publication.publication_id,
            cycle_id=publication.cycle_id,
            changed=False,
        )
    try:
        if not exact_before:
            write_public_safe_atomic(output_path, publication.payload)
        if not _exact_regular_file(output_path, expected):
            raise OSError("published public-safe artifact bytes do not match intent")
        repository.mark_artifact_publication_published(
            publication.publication_id,
            expected_payload_sha256=publication.payload_sha256,
            published_at=attempted_at,
        )
    except BaseException as exc:
        try:
            repository.record_artifact_publication_failure(
                publication.publication_id,
                attempted_at=attempted_at,
                error=type(exc).__name__,
            )
        except Exception:
            pass
        raise
    return PublicationResult(
        publication_id=publication.publication_id,
        cycle_id=publication.cycle_id,
        changed=True,
    )
