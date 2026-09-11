from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream_monitoring_v4.adapters.json_file import (
    SnapshotReadError,
    read_json_snapshot,
)
from stream_monitoring_v4.runtime.atomic_file import atomic_write_bytes
from stream_monitoring_v4.runtime.projection_lock import projection_lock
from stream_monitoring_v4.runtime.source_revision import (
    UNKNOWN_SOURCE_REVISION,
    normalize_source_revision,
    read_source_revision,
)

from .constants import (
    RAW_ROLLOUT_FILE,
    RAW_JSON_SOURCES,
    RAW_OUTBOX_FILE,
    SAFE_JSON_FILES,
    SAFE_OUTBOX_FILE,
    SAFE_LIFECYCLE_FILE,
    SAFE_REVISION_FILE,
    SAFE_ROLLOUT_FILE,
)
from .lifecycle import runtime_lifecycle_projection
from .outbox import notification_outbox_projection
from .rollout import runtime_rollout_projection
from .sanitize import TRANSFORMS
from .generation import publish_safe_input_generation, projected_file_names


@dataclass(frozen=True)
class ProjectionResult:
    projected: tuple[str, ...]
    unchanged: tuple[str, ...]
    rejected: Mapping[str, str]
    source_revision: str
    generation_id: str

    @property
    def partition_valid(self) -> bool:
        expected = set(projected_file_names())
        projected = set(self.projected)
        unchanged = set(self.unchanged)
        return (
            len(projected) == len(self.projected)
            and len(unchanged) == len(self.unchanged)
            and projected.isdisjoint(unchanged)
            and projected | unchanged == expected
        )

    @property
    def ready(self) -> bool:
        return (
            not self.rejected
            and self.source_revision != UNKNOWN_SOURCE_REVISION
            and normalize_source_revision(self.source_revision) == self.source_revision
            and bool(self.generation_id)
            and self.partition_valid
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "monitoring_v4.safe_input_projection.v4",
            "projected": list(self.projected),
            "unchanged": list(self.unchanged),
            "rejected": dict(sorted(self.rejected.items())),
            "source_revision": self.source_revision,
            "generation_id": self.generation_id,
            "ready": self.ready,
            "credential_fields_projected": False,
            "raw_state_available_to_core": False,
        }


def atomic_projected_write(path: Path, content: bytes) -> bool:
    return atomic_write_bytes(path, content, mode=0o600, skip_if_unchanged=True)


def revision_content(source: Path) -> tuple[str, bytes]:
    if source.is_symlink():
        return UNKNOWN_SOURCE_REVISION, b""
    revision = read_source_revision(source)
    if revision == UNKNOWN_SOURCE_REVISION:
        return revision, b""
    if "@" in revision:
        effective, commit = revision.rsplit("@", 1)
        content = (
            f"STREAM_V3_DEPLOYED_REVISION={effective}\n"
            f"STREAM_V3_DEPLOYED_COMMIT={commit}\n"
        )
    elif re.fullmatch(r"[0-9A-Fa-f]{7,64}", revision):
        content = f"STREAM_V3_DEPLOYED_COMMIT={revision}\n"
    else:
        content = f"STREAM_V3_DEPLOYED_REVISION={revision}\n"
    return revision, content.encode("ascii")


def project_safe_inputs(source_root: Path, target_root: Path) -> ProjectionResult:
    target_root = Path(target_root)
    if target_root.is_symlink():
        raise OSError("safe input target root must not be a symlink")
    target_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target_root.chmod(0o700)
    with projection_lock(
        target_root / ".projection.lock",
        exclusive=True,
        timeout_sec=30.0,
    ):
        return project_safe_inputs_unlocked(source_root, target_root)


def project_safe_inputs_unlocked(
    source_root: Path,
    target_root: Path,
) -> ProjectionResult:
    """Project only fixed, sanitized v3 facts into a v4-owned directory."""

    source_root = Path(source_root)
    target_root = Path(target_root)
    if target_root.is_symlink():
        raise OSError("safe input target root must not be a symlink")
    target_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target_root.chmod(0o700)
    projected: list[str] = []
    unchanged: list[str] = []
    rejected: dict[str, str] = {}
    contents: dict[str, bytes] = {}
    for name in SAFE_JSON_FILES:
        source = source_root / RAW_JSON_SOURCES[name]
        try:
            if source.is_symlink():
                raise SnapshotReadError(
                    "source_symlink_rejected",
                    "allowlisted source is a symlink",
                )
            snapshot = read_json_snapshot(source)
            safe = TRANSFORMS[name](snapshot.payload)
            contents[name] = (
                json.dumps(
                    safe,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        except (OSError, SnapshotReadError, ValueError, TypeError) as exc:
            rejected[name] = (
                exc.reason_code
                if isinstance(exc, SnapshotReadError)
                else type(exc).__name__
            )
            # Retain only a previously sanitized snapshot. Its embedded source
            # timestamp ages out naturally across a transient source gap.

    try:
        outbox_source = source_root / RAW_OUTBOX_FILE
        outbox = notification_outbox_projection(
            outbox_source,
            projected_at_ts=int(outbox_source.stat().st_mtime) if outbox_source.exists() else 0,
        )
        contents[SAFE_OUTBOX_FILE] = (
            json.dumps(
                outbox,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
    except (OSError, SnapshotReadError, ValueError, TypeError) as exc:
        rejected[SAFE_OUTBOX_FILE] = (
            exc.reason_code if isinstance(exc, SnapshotReadError) else type(exc).__name__
        )

    try:
        lifecycle = runtime_lifecycle_projection(source_root)
        contents[SAFE_LIFECYCLE_FILE] = (
            json.dumps(
                lifecycle.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
    except (OSError, SnapshotReadError, ValueError, TypeError) as exc:
        rejected[SAFE_LIFECYCLE_FILE] = (
            exc.reason_code
            if isinstance(exc, SnapshotReadError)
            else type(exc).__name__
        )

    try:
        rollout_source = source_root / RAW_ROLLOUT_FILE
        if rollout_source.is_symlink():
            raise SnapshotReadError(
                "source_symlink_rejected",
                "allowlisted source is a symlink",
            )
        rollout_snapshot = read_json_snapshot(rollout_source)
        rollout = runtime_rollout_projection(rollout_snapshot.payload)
        contents[SAFE_ROLLOUT_FILE] = (
            json.dumps(
                rollout.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
    except (OSError, SnapshotReadError, ValueError, TypeError) as exc:
        rejected[SAFE_ROLLOUT_FILE] = (
            exc.reason_code
            if isinstance(exc, SnapshotReadError)
            else type(exc).__name__
        )

    revision, content = revision_content(source_root / SAFE_REVISION_FILE)
    if content:
        contents[SAFE_REVISION_FILE] = content
    else:
        rejected[SAFE_REVISION_FILE] = "source_revision_unknown"
    generation_id = ""
    if not rejected:
        for name in projected_file_names():
            try:
                target_list = (
                    projected
                    if atomic_projected_write(target_root / name, contents[name])
                    else unchanged
                )
                target_list.append(name)
            except OSError as exc:
                rejected[name] = type(exc).__name__
                break
    if not rejected:
        try:
            generation_id = publish_safe_input_generation(
                target_root,
                contents=contents,
                source_revision=revision,
            )
        except (OSError, ValueError, TypeError) as exc:
            rejected[".projection-generation.json"] = type(exc).__name__
    return ProjectionResult(
        projected=tuple(sorted(projected)),
        unchanged=tuple(sorted(unchanged)),
        rejected=rejected,
        source_revision=revision,
        generation_id=generation_id,
    )
