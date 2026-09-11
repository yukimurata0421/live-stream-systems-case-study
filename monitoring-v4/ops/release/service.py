from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Callable
from pathlib import Path

from ops.scripts.monitoring_v4_source_identity import content_identity

from .manifests import render_manifests, validate_rendered_manifests
from .model import (
    ReleaseResult,
    existing_real_directory,
    validate_release_name,
    validate_render_values,
    validate_snapshot_sha256,
)
from .tree import (
    copy_source,
    freeze_tree,
    fsync_directory,
    records_digest,
    release_lock,
    remove_staging,
    rename_noreplace,
    tree_records,
    validate_frozen_tree,
)


def prepare_release(
    *,
    source_root: Path,
    release_base: Path,
    release_name: str,
    expected_identity: str,
    expected_snapshot_sha256: str,
    app_image: str,
    app_image_id: str,
) -> ReleaseResult:
    validate_render_values(expected_identity, app_image, app_image_id)
    validate_snapshot_sha256(expected_snapshot_sha256)
    validate_release_name(release_name, expected_identity)
    source = existing_real_directory(source_root, label="release source")
    base = existing_real_directory(release_base, label="release base")
    if source == base or base in source.parents or source in base.parents:
        raise RuntimeError("release source and release base must be separate trees")
    source_records = tree_records(source, require_source_layout=True)
    source_digest = records_digest(source_records)
    if source_digest != expected_snapshot_sha256:
        raise RuntimeError("release source snapshot does not match the expected SHA-256")
    if content_identity(source) != expected_identity:
        raise RuntimeError("release source identity does not match the expected identity")
    target = base / release_name
    if target.exists() or target.is_symlink():
        raise RuntimeError("release target already exists")

    with release_lock(base):
        if target.exists() or target.is_symlink():
            raise RuntimeError("release target already exists")
        staging = base / f".{release_name}.staging-{os.getpid()}-{secrets.token_hex(6)}"
        if staging.exists() or staging.is_symlink():
            raise RuntimeError("generated release staging path already exists")
        try:
            copy_source(source, staging)
            copied_records = tree_records(staging, require_source_layout=True)
            if records_digest(copied_records) != source_digest:
                raise RuntimeError("release source changed while it was copied")
            if content_identity(staging) != expected_identity:
                raise RuntimeError("copied release identity does not match the expected identity")
            render_manifests(
                staging,
                expected_identity=expected_identity,
                app_image=app_image,
                app_image_id=app_image_id,
            )
            validate_rendered_manifests(
                staging,
                expected_identity=expected_identity,
                app_image=app_image,
                app_image_id=app_image_id,
            )
            freeze_tree(staging)
            file_count, total_bytes = validate_frozen_tree(staging)
            rename_noreplace(staging, target)
            fsync_directory(base)
        except BaseException:
            remove_staging(staging)
            raise
    return ReleaseResult(
        release=target,
        source_identity=expected_identity,
        snapshot_sha256=source_digest,
        file_count=file_count,
        total_bytes=total_bytes,
    )


def validate_release(
    *,
    release: Path,
    expected_identity: str,
    expected_snapshot_sha256: str,
    app_image: str,
    app_image_id: str,
) -> tuple[int, int]:
    validate_render_values(expected_identity, app_image, app_image_id)
    validate_snapshot_sha256(expected_snapshot_sha256)
    root = existing_real_directory(release, label="prepared release")
    validate_release_name(root.name, expected_identity)
    if content_identity(root) != expected_identity:
        raise RuntimeError("prepared release identity does not match the expected identity")
    source_records = tree_records(
        root,
        require_source_layout=True,
        exclude_rendered=True,
    )
    if records_digest(source_records) != expected_snapshot_sha256:
        raise RuntimeError("prepared release snapshot does not match the expected SHA-256")
    validate_rendered_manifests(
        root,
        expected_identity=expected_identity,
        app_image=app_image,
        app_image_id=app_image_id,
    )
    return validate_frozen_tree(root)


def _current_target(current: Path, base: Path) -> Path:
    try:
        metadata = os.lstat(current)
    except OSError as exc:
        raise RuntimeError("current release symlink is unavailable") from exc
    if not stat.S_ISLNK(metadata.st_mode):
        raise RuntimeError("current release path must be a symlink")
    target = current.resolve(strict=True)
    if target.parent != base or not target.is_dir() or target.is_symlink():
        raise RuntimeError("current release symlink escapes the release base")
    return target


def promote_release(
    *,
    release_base: Path,
    release_name: str,
    expected_current_release: str,
    expected_identity: str,
    expected_snapshot_sha256: str,
    app_image: str,
    app_image_id: str,
    cluster_validator: Callable[[Path], None],
) -> Path:
    validate_release_name(release_name, expected_identity)
    base = existing_real_directory(release_base, label="release base")
    release = base / release_name
    current = base / "current"
    with release_lock(base):
        actual_current = _current_target(current, base)
        if actual_current.name != expected_current_release:
            raise RuntimeError("current release changed before promotion")
        validate_release(
            release=release,
            expected_identity=expected_identity,
            expected_snapshot_sha256=expected_snapshot_sha256,
            app_image=app_image,
            app_image_id=app_image_id,
        )
        cluster_validator(release / "deploy/k3s-rendered")
        if _current_target(current, base) != actual_current:
            raise RuntimeError("current release changed during validation")
        temporary = base / f".current.promote-{os.getpid()}-{secrets.token_hex(6)}"
        if temporary.exists() or temporary.is_symlink():
            raise RuntimeError("generated current symlink path already exists")
        try:
            os.symlink(release, temporary)
            fsync_directory(base)
            os.replace(temporary, current)
            fsync_directory(base)
        finally:
            if temporary.is_symlink():
                temporary.unlink()
        if _current_target(current, base) != release:
            raise RuntimeError("current release promotion did not persist")
    return release
