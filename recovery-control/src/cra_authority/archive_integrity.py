from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from cra_authority.json_input import load_object
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.sqlite import production_sqlite_gate
from cra_dell_recovery.time import isoformat_utc, utc_now


def _required_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _required_path(value: Any, *, field: str) -> Path:
    path = Path(_required_string(value, field=field))
    if not path.is_absolute():
        raise ValueError(f"{field} must be an absolute path")
    return path


def _finite_number(value: Any, *, field: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a JSON number")
    parsed = float(value)
    if not math.isfinite(parsed) or not minimum <= parsed <= maximum:
        raise ValueError(f"{field} is outside the safe range")
    return parsed


def _secure_read(path: Path, *, maximum_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("CRA_ARCHIVE_INTEGRITY_INPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("CRA_ARCHIVE_INTEGRITY_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("CRA_ARCHIVE_INTEGRITY_INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("CRA_ARCHIVE_INTEGRITY_INPUT_TOO_LARGE")
    return raw


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists():
            temporary.unlink()


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def _reason_code(error: Exception) -> str:
    raw = str(error).strip()
    if re.fullmatch(r"[A-Z][A-Z0-9_:.-]{0,159}", raw):
        detail = raw
    else:
        detail = re.sub(r"[^A-Z0-9]+", "_", raw.upper()).strip("_")[:120] or "NO_DETAIL"
    error_class = re.sub(r"[^A-Z0-9]+", "_", type(error).__name__.upper()).strip("_")
    return f"{error_class}:{detail}"[:200]


@dataclass(frozen=True)
class ArchiveIntegrityConfig:
    value: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: Path) -> ArchiveIntegrityConfig:
        raw = load_object(_secure_read(path, maximum_bytes=64 * 1024), maximum_bytes=64 * 1024)
        required = {
            "schema",
            "runtime_release_id",
            "archive_database",
            "status_file",
            "busy_timeout_seconds",
        }
        if not isinstance(raw, dict) or set(raw) != required:
            raise ValueError("CRA_ARCHIVE_INTEGRITY_CONFIG_FIELDS_NOT_EXACT")
        if raw["schema"] != "cra.archive_integrity.v1":
            raise ValueError("CRA_ARCHIVE_INTEGRITY_CONFIG_SCHEMA_UNSUPPORTED")
        _required_string(raw["runtime_release_id"], field="runtime_release_id")
        require_runtime_release(raw["runtime_release_id"], "CRA_ARCHIVE_INTEGRITY_RELEASE_MISMATCH")
        archive = _required_path(raw["archive_database"], field="archive_database")
        status = _required_path(raw["status_file"], field="status_file")
        if archive == status:
            raise ValueError("CRA_ARCHIVE_INTEGRITY_STATUS_PATH_CONFLICT")
        _finite_number(raw["busy_timeout_seconds"], field="busy_timeout_seconds", minimum=0.1, maximum=60)
        return cls(raw, path)


def _pragma_result(connection: sqlite3.Connection, pragma: str) -> str:
    rows = [str(row[0]) for row in connection.execute(f"PRAGMA {pragma}")]
    return "ok" if rows == ["ok"] else "failed"


def _inspect(config: ArchiveIntegrityConfig) -> dict[str, Any]:
    if not production_sqlite_gate():
        raise ValueError("CRA_ARCHIVE_INTEGRITY_SQLITE_VERSION_UNSUPPORTED")
    archive = Path(config.value["archive_database"])
    descriptor = os.open(archive, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("CRA_ARCHIVE_DATABASE_NOT_REGULAR_FILE")
        uri = f"file:{quote(str(archive), safe='/')}?mode=ro"
        connection = sqlite3.connect(
            uri,
            uri=True,
            isolation_level=None,
            timeout=float(config.value["busy_timeout_seconds"]),
        )
        try:
            connection.execute("PRAGMA query_only=ON")
            connection.execute(f"PRAGMA busy_timeout={int(float(config.value['busy_timeout_seconds']) * 1000)}")
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            record = connection.execute("SELECT count(*),coalesce(max(rowid),0) FROM archive_records").fetchone()
            checkpoint = connection.execute("SELECT count(*) FROM daily_checkpoints").fetchone()
            quick_check = _pragma_result(connection, "quick_check")
            integrity_check = _pragma_result(connection, "integrity_check")
        finally:
            connection.close()
        after = os.stat(archive, follow_symlinks=False)
        if not stat.S_ISREG(after.st_mode) or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise ValueError("CRA_ARCHIVE_DATABASE_IDENTITY_CHANGED")
    finally:
        os.close(descriptor)
    if quick_check != "ok" or integrity_check != "ok":
        raise ValueError("CRA_ARCHIVE_DATABASE_INTEGRITY_FAILED")
    database_bytes = _size(archive)
    wal_bytes = _size(archive.with_name(f"{archive.name}-wal"))
    shm_bytes = _size(archive.with_name(f"{archive.name}-shm"))
    return {
        "archive_device": int(before.st_dev),
        "archive_inode": int(before.st_ino),
        "archive_record_count": int(record[0]),
        "archive_record_high_water": int(record[1]),
        "archive_checkpoint_count": int(checkpoint[0]),
        "archive_database_bytes": database_bytes,
        "archive_wal_bytes": wal_bytes,
        "archive_shm_bytes": shm_bytes,
        "archive_sqlite_total_bytes": database_bytes + wal_bytes + shm_bytes,
        "archive_quick_check": quick_check,
        "archive_integrity_check": integrity_check,
        "archive_journal_mode": journal_mode,
        "sqlite_version": sqlite3.sqlite_version,
    }


def run_check(config: ArchiveIntegrityConfig) -> dict[str, Any]:
    started = utc_now()
    started_monotonic = time.monotonic()
    base = {
        "schema": "cra.archive_integrity_status.v1",
        "runtime_release_id": config.value["runtime_release_id"],
        "archive_database": config.value["archive_database"],
        "started_at": isoformat_utc(started),
    }
    try:
        detail = _inspect(config)
        result = {
            **base,
            **detail,
            "status": "PASS",
            "reason_code": "ARCHIVE_INTEGRITY_VALID",
            "checked_at": isoformat_utc(utc_now()),
            "duration_seconds": round(time.monotonic() - started_monotonic, 6),
            "check_mode": "EXTERNAL_FULL",
        }
    except Exception as exc:
        result = {
            **base,
            "status": "FAIL",
            "reason_code": _reason_code(exc),
            "checked_at": isoformat_utc(utc_now()),
            "duration_seconds": round(time.monotonic() - started_monotonic, 6),
            "check_mode": "EXTERNAL_FULL",
        }
    _atomic_json(Path(config.value["status_file"]), result)
    return result


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="Run an out-of-process read-only CRA archive integrity check")
    value.add_argument("--config", type=Path, required=True)
    value.add_argument("--check-config", action="store_true")
    return value


def main() -> None:
    args = parser().parse_args()
    config = ArchiveIntegrityConfig.load(args.config)
    if args.check_config:
        print(json.dumps({"config": "VALID", "runtime_release_id": config.value["runtime_release_id"]}, sort_keys=True))
        return
    result = run_check(config)
    print(json.dumps(result, separators=(",", ":"), sort_keys=True), flush=True)
    if result["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
