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
CREATE TABLE IF NOT EXISTS audit_gaps (
    agent_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    expected_seq INTEGER NOT NULL,
    received_seq INTEGER NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    sightings INTEGER NOT NULL DEFAULT 1,
    resolved_at TEXT,
    PRIMARY KEY (agent_id, instance_id, expected_seq, received_seq)
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
        """ACK only the contiguous prefix (I9); stop at the first gap and retain its history.

        A record whose seq is already stored with different content is kept in `conflicts`
        and never overwrites the original.
        """
        now = datetime.now(UTC).isoformat()
        if any(type(r.get("seq")) is not int or r["seq"] < 1 for r in records):
            raise ValueError("seq must be a positive integer")
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                high = self.contiguous_high(agent_id, instance_id)
                for record in sorted(records, key=lambda r: r["seq"]):
                    seq, payload = int(record["seq"]), json.dumps(record["payload"], sort_keys=True)
                    stored = self.db.execute(
                        "SELECT ts, kind, payload FROM audit WHERE agent_id = ? AND instance_id = ? AND seq = ?",
                        (agent_id, instance_id, seq),
                    ).fetchone()
                    if stored is not None and tuple(stored) != (record["ts"], record["kind"], payload):
                        self.db.execute(
                            "INSERT INTO conflicts (agent_id, instance_id, seq, received_at, payload) VALUES (?, ?, ?, ?, ?)",
                            (agent_id, instance_id, seq, now, json.dumps(record, sort_keys=True)),
                        )
                    if seq > high + 1:
                        self.db.execute(
                            """INSERT INTO audit_gaps (agent_id, instance_id, expected_seq, received_seq, first_seen, last_seen)
                               VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(agent_id, instance_id, expected_seq, received_seq)
                               DO UPDATE SET last_seen=excluded.last_seen, sightings=sightings+1, resolved_at=NULL""",
                            (agent_id, instance_id, high + 1, seq, now, now),
                        )
                        break
                    if stored is not None:
                        continue
                    self.db.execute(
                        "INSERT INTO audit (agent_id, instance_id, seq, ts, kind, payload, received_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (agent_id, instance_id, seq, str(record["ts"]), str(record["kind"]), payload, now),
                    )
                    high = seq
                    # Include any legacy rows previously stored beyond a gap, without overwriting them.
                    while self.db.execute(
                        "SELECT 1 FROM audit WHERE agent_id=? AND instance_id=? AND seq=?", (agent_id, instance_id, high + 1)
                    ).fetchone():
                        high += 1
                self.db.execute(
                    """UPDATE audit_gaps SET resolved_at=? WHERE agent_id=? AND instance_id=?
                       AND received_seq <= ? AND resolved_at IS NULL""",
                    (now, agent_id, instance_id, high),
                )
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            self.db.execute("COMMIT")
            return high

    def contiguous_high(self, agent_id: str, instance_id: str) -> int:
        high = 0
        for row in self.db.execute("SELECT seq FROM audit WHERE agent_id=? AND instance_id=? ORDER BY seq", (agent_id, instance_id)):
            if row[0] != high + 1:
                break
            high = int(row[0])
        return high

    def integrity(self, agent_id: str | None = None) -> dict[str, Any]:
        """Current missing sequences and historical gap observations, without altering records."""
        with self.lock:
            where, args = (" WHERE agent_id=?", (agent_id,)) if agent_id else ("", ())
            rows = self.db.execute("SELECT agent_id, instance_id, seq FROM audit" + where + " ORDER BY seq", args).fetchall()
            gaps = self.db.execute("SELECT * FROM audit_gaps" + where, args).fetchall()
            streams: dict[tuple[str, str], set[int]] = {}
            for row in rows:
                streams.setdefault((row[0], row[1]), set()).add(row[2])
            # Count ranges without materializing possibly very large missing sequence sets.
            upper: dict[tuple[str, str], int] = {key: max(seqs) for key, seqs in streams.items()}
            for gap in gaps:
                if gap["resolved_at"] is None:
                    key = (gap["agent_id"], gap["instance_id"])
                    upper[key] = max(upper.get(key, 0), gap["received_seq"] - 1)
            return {
                "missing_seq_count": sum(end - sum(seq <= end for seq in streams.get(key, ())) for key, end in upper.items()),
                "gap_observations": len(gaps),
                "gap_sightings": sum(gap["sightings"] for gap in gaps),
                "unresolved_gaps": sum(gap["resolved_at"] is None for gap in gaps),
                "streams": {
                    f"{a}/{i}": {"contiguous_high": self.contiguous_high(a, i), "max_stored_seq": max(streams.get((a, i), {0}))}
                    for a, i in upper
                },
            }

    def audit_records(self, agent_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT agent_id, instance_id, seq, ts, kind, payload FROM audit"
        args: tuple[Any, ...] = ()
        if agent_id:
            sql, args = sql + " WHERE agent_id = ?", (agent_id,)
        rows = self.db.execute(sql + " ORDER BY agent_id, instance_id, seq", args).fetchall()
        return [{**dict(r), "payload": json.loads(r["payload"])} for r in rows]

    def conflict_count(self) -> int:
        return int(self.db.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0])
