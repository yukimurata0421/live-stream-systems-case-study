"""CRA central store: published policies and the audit records pulled in from agents."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS policies (
    version INTEGER PRIMARY KEY,
    published_at TEXT NOT NULL,
    body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    agent_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, instance_id, seq)
);
CREATE TABLE IF NOT EXISTS conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    received_at TEXT NOT NULL,
    payload TEXT NOT NULL
);
"""


class CentralStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5.0, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(SCHEMA)
        self.lock = threading.Lock()

    def close(self) -> None:
        self.db.close()

    def record_policy(self, body: dict[str, Any]) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO policies (version, published_at, body) VALUES (?, ?, ?)",
                (body["version"], datetime.now(UTC).isoformat(), json.dumps(body, sort_keys=True)),
            )

    def policies(self) -> dict[int, dict[str, Any]]:
        rows = self.db.execute("SELECT version, body FROM policies").fetchall()
        return {int(r["version"]): json.loads(r["body"]) for r in rows}

    def latest_policy_version(self) -> int:
        return int(self.db.execute("SELECT COALESCE(MAX(version), -1) FROM policies").fetchone()[0])

    def ingest(self, agent_id: str, instance_id: str, records: list[dict[str, Any]]) -> int:
        """Store records in order without duplicates (I9). Returns the highest stored seq.

        A record whose seq is already stored with different content is kept in `conflicts`
        and never overwrites the original.
        """
        now = datetime.now(UTC).isoformat()
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                high = int(
                    self.db.execute(
                        "SELECT COALESCE(MAX(seq), 0) FROM audit WHERE agent_id = ? AND instance_id = ?",
                        (agent_id, instance_id),
                    ).fetchone()[0]
                )
                for record in sorted(records, key=lambda r: r["seq"]):
                    seq, payload = int(record["seq"]), json.dumps(record["payload"], sort_keys=True)
                    if seq <= high:
                        stored = self.db.execute(
                            "SELECT ts, kind, payload FROM audit WHERE agent_id = ? AND instance_id = ? AND seq = ?",
                            (agent_id, instance_id, seq),
                        ).fetchone()
                        if stored is not None and tuple(stored) != (record["ts"], record["kind"], payload):
                            self.db.execute(
                                "INSERT INTO conflicts (agent_id, instance_id, seq, received_at, payload) VALUES (?, ?, ?, ?, ?)",
                                (agent_id, instance_id, seq, now, json.dumps(record, sort_keys=True)),
                            )
                        continue
                    self.db.execute(
                        "INSERT INTO audit (agent_id, instance_id, seq, ts, kind, payload, received_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (agent_id, instance_id, seq, str(record["ts"]), str(record["kind"]), payload, now),
                    )
                    high = seq
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            self.db.execute("COMMIT")
            return high

    def audit_records(self, agent_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT agent_id, instance_id, seq, ts, kind, payload FROM audit"
        args: tuple[Any, ...] = ()
        if agent_id:
            sql, args = sql + " WHERE agent_id = ?", (agent_id,)
        rows = self.db.execute(sql + " ORDER BY agent_id, instance_id, seq", args).fetchall()
        return [{**dict(r), "payload": json.loads(r["payload"])} for r in rows]

    def conflict_count(self) -> int:
        return int(self.db.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0])
