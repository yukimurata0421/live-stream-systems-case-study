from __future__ import annotations

import base64
import hashlib
import os
import sqlite3
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import canonical_json
from cra_dell_recovery.time import isoformat_utc, utc_now


def load_archive_private_key(path: Path) -> Ed25519PrivateKey:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("CRA_ARCHIVE_KEY_NOT_REGULAR_FILE")
        if metadata.st_mode & 0o077:
            raise ValueError("CRA_ARCHIVE_KEY_PERMISSIONS_UNSAFE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(64 * 1024 + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > 64 * 1024:
        raise ValueError("CRA_ARCHIVE_KEY_TOO_LARGE")
    key = serialization.load_pem_private_key(raw, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("CRA_ARCHIVE_KEY_NOT_ED25519")
    return key


class SignedNoActionArchive:
    """Separate, signed hash-chain archive for removable NO_ACTION evidence."""

    def __init__(self, path: Path, *, key_id: str, private_key: Ed25519PrivateKey) -> None:
        self.path = path
        self.key_id = key_id
        self.private_key = private_key
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.connection = sqlite3.connect(path, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS archive_records (
                record_id TEXT PRIMARY KEY,
                record_type TEXT NOT NULL,
                source_created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
                previous_digest TEXT NOT NULL,
                record_digest TEXT NOT NULL UNIQUE,
                key_id TEXT NOT NULL,
                signature TEXT NOT NULL,
                archived_at TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS daily_checkpoints (
                utc_day TEXT PRIMARY KEY,
                record_count INTEGER NOT NULL,
                last_record_digest TEXT NOT NULL,
                checkpoint_digest TEXT NOT NULL UNIQUE,
                key_id TEXT NOT NULL,
                signature TEXT NOT NULL,
                created_at TEXT NOT NULL
            ) STRICT;
            """
        )
        os.chmod(path, 0o600)
        self._last_integrity_check = "unknown"
        self._last_integrity_checked_at: str | None = None

    def close(self) -> None:
        self.connection.close()

    def _sign(self, digest: str) -> str:
        return base64.b64encode(self.private_key.sign(bytes.fromhex(digest))).decode("ascii")

    def append(
        self,
        *,
        record_id: str,
        record_type: str,
        source_created_at: str,
        payload: dict[str, Any],
    ) -> str:
        encoded = canonical_json(payload).decode("utf-8")
        existing = self.connection.execute(
            "SELECT record_digest,payload_json FROM archive_records WHERE record_id=?", (record_id,)
        ).fetchone()
        if existing is not None:
            if str(existing["payload_json"]) != encoded:
                raise RuntimeError("CRA_ARCHIVE_RECORD_CONFLICT")
            return str(existing["record_digest"])
        previous = self.connection.execute("SELECT record_digest FROM archive_records ORDER BY rowid DESC LIMIT 1").fetchone()
        previous_digest = str(previous[0]) if previous is not None else "0" * 64
        digest = hashlib.sha256(
            canonical_json(
                {
                    "record_id": record_id,
                    "record_type": record_type,
                    "source_created_at": source_created_at,
                    "payload": payload,
                    "previous_digest": previous_digest,
                }
            )
        ).hexdigest()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                "INSERT INTO archive_records VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    record_id,
                    record_type,
                    source_created_at,
                    encoded,
                    previous_digest,
                    digest,
                    self.key_id,
                    self._sign(digest),
                    isoformat_utc(utc_now()),
                ),
            )
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise
        return digest

    def contains(self, record_id: str, digest: str) -> bool:
        row = self.connection.execute("SELECT record_digest FROM archive_records WHERE record_id=?", (record_id,)).fetchone()
        return row is not None and str(row[0]) == digest

    def checkpoint_completed_days(self, *, now: datetime | None = None) -> int:
        current_day = (now or datetime.now(UTC)).astimezone(UTC).date().isoformat()
        days = self.connection.execute(
            """SELECT substr(source_created_at,1,10) AS utc_day,
                      count(*) AS record_count
                 FROM archive_records
                WHERE substr(source_created_at,1,10) < ?
                GROUP BY substr(source_created_at,1,10) ORDER BY utc_day""",
            (current_day,),
        ).fetchall()
        created = 0
        for row in days:
            day = str(row["utc_day"])
            if self.connection.execute("SELECT 1 FROM daily_checkpoints WHERE utc_day=?", (day,)).fetchone() is not None:
                continue
            last = self.connection.execute(
                """SELECT record_digest FROM archive_records
                   WHERE substr(source_created_at,1,10)=? ORDER BY rowid DESC LIMIT 1""",
                (day,),
            ).fetchone()
            if last is None:
                continue
            value = {
                "utc_day": day,
                "record_count": int(row["record_count"]),
                "last_record_digest": str(last[0]),
                "key_id": self.key_id,
            }
            digest = hashlib.sha256(canonical_json(value)).hexdigest()
            self.connection.execute(
                "INSERT INTO daily_checkpoints VALUES(?,?,?,?,?,?,?)",
                (
                    day,
                    value["record_count"],
                    value["last_record_digest"],
                    digest,
                    self.key_id,
                    self._sign(digest),
                    isoformat_utc(utc_now()),
                ),
            )
            created += 1
        return created

    @staticmethod
    def _size(path: Path) -> int:
        try:
            return path.stat().st_size
        except FileNotFoundError:
            return 0

    def status(self, *, full_integrity: bool = False) -> dict[str, Any]:
        record = self.connection.execute("SELECT count(*),coalesce(max(rowid),0) FROM archive_records").fetchone()
        checkpoint = self.connection.execute("SELECT count(*) FROM daily_checkpoints").fetchone()
        quick_check = str(self.connection.execute("PRAGMA quick_check").fetchone()[0])
        integrity_mode = "CACHED"
        if full_integrity or self._last_integrity_checked_at is None:
            self._last_integrity_check = str(self.connection.execute("PRAGMA integrity_check").fetchone()[0])
            self._last_integrity_checked_at = isoformat_utc(utc_now())
            integrity_mode = "FULL"
        database_bytes = self._size(self.path)
        wal_bytes = self._size(self.path.with_name(f"{self.path.name}-wal"))
        shm_bytes = self._size(self.path.with_name(f"{self.path.name}-shm"))
        return {
            "archive_record_count": int(record[0]),
            "archive_checkpoint_count": int(checkpoint[0]),
            "archive_database_bytes": database_bytes,
            "archive_wal_bytes": wal_bytes,
            "archive_shm_bytes": shm_bytes,
            "archive_sqlite_total_bytes": database_bytes + wal_bytes + shm_bytes,
            "archive_quick_check": quick_check,
            "archive_integrity_check": self._last_integrity_check,
            "archive_integrity_check_mode": integrity_mode,
            "archive_integrity_checked_at": self._last_integrity_checked_at,
        }
