from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.time import unix_ts, utc_text
from stream_monitoring_v4.adapters.json_file import strict_json_loads


_BARE_SHA256 = re.compile(r"[0-9a-f]{64}")
_BACKUP_NAME = re.compile(r"stream-v4-[0-9]{8}T[0-9]{6}Z\.dump")
_MAX_BACKUP_CANDIDATES = 10_000
_MAX_CHECKSUM_BYTES = 4096
_RESTORE_VERIFICATION_SCHEMA = "monitoring_v4.postgresql_restore_verification.v1"
_MAX_RESTORE_VERIFICATION_BYTES = 16 * 1024


def _identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        stat.S_IFMT(metadata.st_mode),
    )


def backup_directory_device(path: Path) -> int:
    """Return a stable directory device without following a symlink alias."""

    root = Path(path)
    before = root.lstat()
    if not stat.S_ISDIR(before.st_mode):
        raise RuntimeError(f"backup root is not a directory: {root}")
    descriptor = os.open(
        root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        after = root.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or _identity(before) != _identity(opened)
            or _identity(before) != _identity(after)
        ):
            raise RuntimeError(f"backup root changed while checking identity: {root}")
        return int(opened.st_dev)
    finally:
        os.close(descriptor)


def _open_regular(path: Path) -> tuple[int, os.stat_result]:
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError("backup input is not a regular file")
        return descriptor, metadata
    except BaseException:
        os.close(descriptor)
        raise


def _read_small_regular(
    path: Path,
    *,
    max_bytes: int,
    rejected_permission_bits: int = 0,
) -> bytes:
    descriptor, before = _open_regular(path)
    try:
        if stat.S_IMODE(before.st_mode) & int(rejected_permission_bits):
            raise ValueError("bounded file permissions are too broad")
        if before.st_size > max_bytes:
            raise ValueError("bounded file exceeds size limit")
        stream = os.fdopen(descriptor, "rb", closefd=True)
        descriptor = -1
        with stream:
            raw = stream.read(max_bytes + 1)
            after = os.fstat(stream.fileno())
        if len(raw) > max_bytes or _identity(before) != _identity(after):
            raise ValueError("bounded file changed or exceeded size limit")
        final = path.lstat()
        if _identity(before) != _identity(final):
            raise ValueError("bounded file path changed")
        return raw
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def verified_backup_status(
    path: Path,
    *,
    now_ts: int,
    max_age_sec: int,
) -> dict[str, Any]:
    """Validate the newest atomic pg_dump and its filename-bound checksum."""

    try:
        candidates: list[tuple[Path, os.stat_result]] = []
        for item in Path(path).iterdir():
            if _BACKUP_NAME.fullmatch(item.name) is None:
                continue
            metadata = item.lstat()
            if stat.S_ISREG(metadata.st_mode) and metadata.st_size > 0:
                candidates.append((item, metadata))
                if len(candidates) > _MAX_BACKUP_CANDIDATES:
                    return {
                        "readable": True,
                        "fresh": False,
                        "error": "backup_candidate_limit_exceeded",
                    }
        if not candidates:
            return {"readable": True, "fresh": False, "error": "backup_missing"}
        nonfuture = [
            item for item in candidates if int(item[1].st_mtime) <= int(now_ts)
        ]
        latest, selected = max(
            nonfuture or candidates,
            key=lambda item: (item[1].st_mtime_ns, item[0].name),
        )
        # The capability-free host sentinel runs as root with the operator
        # group only.  Allow that group to read, while rejecting group writes,
        # group execution and every permission for other users.
        if stat.S_IMODE(selected.st_mode) & 0o027:
            return {
                "readable": True,
                "fresh": False,
                "path": str(latest),
                "error": "backup_permissions_too_broad",
            }
        checksum_path = latest.with_name(f"{latest.name}.sha256")
        try:
            checksum_raw = _read_small_regular(
                checksum_path,
                max_bytes=_MAX_CHECKSUM_BYTES,
                rejected_permission_bits=0o027,
            )
        except FileNotFoundError:
            return {
                "readable": True,
                "fresh": False,
                "path": str(latest),
                "error": "backup_checksum_missing",
            }
        try:
            checksum_fields = checksum_raw.decode("ascii").strip().split()
        except UnicodeDecodeError:
            checksum_fields = []
        if (
            len(checksum_fields) != 2
            or _BARE_SHA256.fullmatch(checksum_fields[0]) is None
            or checksum_fields[1] != latest.name
        ):
            return {
                "readable": True,
                "fresh": False,
                "path": str(latest),
                "error": "backup_checksum_invalid",
            }
        descriptor, before = _open_regular(latest)
        if _identity(selected) != _identity(before):
            os.close(descriptor)
            return {
                "readable": True,
                "fresh": False,
                "path": str(latest),
                "error": "backup_changed_during_hash",
            }
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
            after = os.fstat(stream.fileno())
        final_path = latest.lstat()
        if _identity(before) != _identity(after) or _identity(before) != _identity(final_path):
            return {
                "readable": True,
                "fresh": False,
                "path": str(latest),
                "error": "backup_changed_during_hash",
            }
        actual_sha256 = digest.hexdigest()
        if actual_sha256 != checksum_fields[0]:
            return {
                "readable": True,
                "fresh": False,
                "path": str(latest),
                "error": "backup_checksum_mismatch",
            }
        modified_ts = int(after.st_mtime)
        age = int(now_ts) - modified_ts
        return {
            "readable": True,
            "path": str(latest),
            "modified_at": utc_text(modified_ts),
            "age_sec": age,
            "future": age < 0,
            "fresh": 0 <= age <= max(3600, int(max_age_sec)),
            "size_bytes": after.st_size,
            "sha256": actual_sha256,
            "candidate_count": len(candidates),
            "future_candidate_count": len(candidates) - len(nonfuture),
        }
    except (OSError, UnicodeError, ValueError) as exc:
        return {"readable": False, "fresh": False, "error": type(exc).__name__}


def matching_backup_copies(primary: dict[str, Any], independent: dict[str, Any]) -> bool:
    return (
        primary.get("fresh") is True
        and independent.get("fresh") is True
        and primary.get("sha256") == independent.get("sha256")
        and Path(str(primary.get("path", ""))).name
        == Path(str(independent.get("path", ""))).name
    )


def copy_verified_backup(
    backup: dict[str, Any],
    target: Path,
    *,
    mode: int = 0o640,
) -> None:
    """Copy an already verified dump while rechecking identity and digest."""

    source = Path(str(backup.get("path", "")))
    expected = str(backup.get("sha256", ""))
    if backup.get("fresh") is not True or _BARE_SHA256.fullmatch(expected) is None:
        raise ValueError("backup is not eligible for a verified copy")
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    source_descriptor, before = _open_regular(source)
    try:
        temporary_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
        )
    except BaseException:
        os.close(source_descriptor)
        raise
    temporary = Path(temporary_name)
    digest = hashlib.sha256()
    try:
        if stat.S_IMODE(before.st_mode) & 0o027:
            raise ValueError("backup permissions are too broad")
        with os.fdopen(source_descriptor, "rb", closefd=True) as input_stream, os.fdopen(
            temporary_descriptor, "wb", closefd=True
        ) as output_stream:
            source_descriptor = -1
            temporary_descriptor = -1
            while chunk := input_stream.read(1024 * 1024):
                digest.update(chunk)
                output_stream.write(chunk)
            output_stream.flush()
            os.fsync(output_stream.fileno())
            after = os.fstat(input_stream.fileno())
        final = source.lstat()
        if _identity(before) != _identity(after) or _identity(before) != _identity(final):
            raise ValueError("backup changed during verified copy")
        if digest.hexdigest() != expected:
            raise ValueError("backup digest changed during verified copy")
        os.chmod(temporary, int(mode) & 0o777)
        os.replace(temporary, target)
        directory = os.open(
            target.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if source_descriptor >= 0:
            os.close(source_descriptor)
        if temporary_descriptor >= 0:
            os.close(temporary_descriptor)
        temporary.unlink(missing_ok=True)


def verified_restore_status(
    path: Path,
    *,
    backup: dict[str, Any],
    now_ts: int,
    max_age_sec: int,
    pending_grace_sec: int = 0,
) -> dict[str, Any]:
    """Validate a full isolated-restore attestation for the selected backup.

    A short explicit grace period lets the host sentinel avoid a predictable
    false alarm between atomic backup publication and the scheduled restore
    verifier. Destructive retention always calls this with zero grace.
    """

    backup_name = Path(str(backup.get("path", ""))).name
    backup_digest = str(backup.get("sha256", ""))
    backup_age = backup.get("age_sec")
    if (
        backup.get("fresh") is not True
        or _BACKUP_NAME.fullmatch(backup_name) is None
        or _BARE_SHA256.fullmatch(backup_digest) is None
        or type(backup_age) is not int
        or backup_age < 0
    ):
        return {
            "readable": False,
            "verified": False,
            "acceptable": False,
            "pending": False,
            "error": "backup_not_eligible_for_restore_verification",
        }
    attestation = Path(path) / f"{backup_name}.restore-verified.json"
    try:
        raw = _read_small_regular(
            attestation,
            max_bytes=_MAX_RESTORE_VERIFICATION_BYTES,
            rejected_permission_bits=0o027,
        )
    except FileNotFoundError:
        grace = max(0, int(pending_grace_sec))
        pending = backup_age <= grace and grace > 0
        return {
            "readable": False,
            "verified": False,
            "acceptable": pending,
            "pending": pending,
            "path": str(attestation),
            "error": "restore_verification_pending" if pending else "restore_verification_missing",
        }
    except (OSError, ValueError) as exc:
        return {
            "readable": False,
            "verified": False,
            "acceptable": False,
            "pending": False,
            "path": str(attestation),
            "error": type(exc).__name__,
        }
    try:
        payload = strict_json_loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("restore verification must be an object")
        if set(payload) != {
            "schema",
            "backup_name",
            "backup_sha256",
            "verified_at",
            "schema_versions",
            "observations",
            "shadow_cycles",
            "public_artifact_publications",
        }:
            raise ValueError("restore verification fields do not match the contract")
        if payload["schema"] != _RESTORE_VERIFICATION_SCHEMA:
            raise ValueError("restore verification schema is unsupported")
        if payload["backup_name"] != backup_name:
            raise ValueError("restore verification names another backup")
        if payload["backup_sha256"] != backup_digest:
            raise ValueError("restore verification digest does not match")
        verified_at = payload["verified_at"]
        if not isinstance(verified_at, str):
            raise ValueError("restore verification timestamp is invalid")
        verified_ts = unix_ts(verified_at)
        age = int(now_ts) - verified_ts
        from stream_monitoring_v4.storage.postgres_schema import SCHEMA_VERSION

        expected_versions = list(range(1, SCHEMA_VERSION + 1))
        versions = payload["schema_versions"]
        if versions != expected_versions:
            raise ValueError("restore verification schema versions are incomplete")
        for field in (
            "observations",
            "shadow_cycles",
            "public_artifact_publications",
        ):
            if type(payload[field]) is not int or payload[field] < 0:
                raise ValueError(f"restore verification {field} count is invalid")
        fresh = 0 <= age <= max(3600, int(max_age_sec))
        return {
            "readable": True,
            "verified": fresh,
            "acceptable": fresh,
            "pending": False,
            "path": str(attestation),
            "verified_at": verified_at,
            "age_sec": age,
            "future": age < 0,
            "fresh": fresh,
            "backup_name": backup_name,
            "backup_sha256": backup_digest,
            "schema_versions": versions,
            "observations": payload["observations"],
            "shadow_cycles": payload["shadow_cycles"],
            "public_artifact_publications": payload[
                "public_artifact_publications"
            ],
        }
    except (
        KeyError,
        TypeError,
        ValueError,
        OverflowError,
        UnicodeError,
        json.JSONDecodeError,
    ) as exc:
        return {
            "readable": False,
            "verified": False,
            "acceptable": False,
            "pending": False,
            "path": str(attestation),
            "error": type(exc).__name__,
        }
