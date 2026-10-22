"""Dell agent durable state: append-only audit log, action reservations (I1/I2) and policy cache."""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from cra.decide import History

SCHEMA = """
CREATE TABLE IF NOT EXISTS audit (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_key TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('dry_run', 'enforce')),
    request_id TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    bytes_sent_at_action INTEGER NOT NULL,
    state TEXT NOT NULL,
    response TEXT,
    verify_result TEXT,
    verified_at TEXT,
    UNIQUE (generation_key, mode)
);
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS audit_no_update BEFORE UPDATE ON audit
BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END;
CREATE TRIGGER IF NOT EXISTS audit_no_delete BEFORE DELETE ON audit
BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END;
"""


def _ts(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds")


class AgentStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5.0)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(SCHEMA)
        self.db.execute("INSERT OR IGNORE INTO kv (key, value) VALUES ('instance_id', ?)", (str(uuid.uuid4()),))

    @property
    def instance_id(self) -> str:
        """Identifies this database; a re-provisioned agent starts a new audit sequence on CRA."""
        return str(self.get("instance_id"))

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    @staticmethod
    def _append(db: sqlite3.Connection, now: datetime, kind: str, payload: dict[str, Any]) -> int:
        cur = db.execute(
            "INSERT INTO audit (ts, kind, payload) VALUES (?, ?, ?)",
            (_ts(now), kind, json.dumps(payload, sort_keys=True, separators=(",", ":"))),
        )
        return int(cur.lastrowid or 0)

    # --- audit -------------------------------------------------------------------------

    def audit(self, now: datetime, kind: str, payload: dict[str, Any]) -> int:
        with self._tx() as db:
            return self._append(db, now, kind, payload)

    def read_audit(self, after_seq: int, limit: int) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT seq, ts, kind, payload FROM audit WHERE seq > ? ORDER BY seq LIMIT ?", (after_seq, limit)).fetchall()
        return [{"seq": r["seq"], "ts": r["ts"], "kind": r["kind"], "payload": json.loads(r["payload"])} for r in rows]

    def last_seq(self) -> int:
        return int(self.db.execute("SELECT COALESCE(MAX(seq), 0) FROM audit").fetchone()[0])

    # --- actions -----------------------------------------------------------------------

    def reserve_action(
        self,
        *,
        now: datetime,
        generation_key: str,
        mode: str,
        request_id: str,
        policy_version: int,
        bytes_sent: int,
        evidence: dict[str, Any],
    ) -> int | None:
        """Durably claim the generation before any request is sent (I1, I2). None if already claimed."""
        with self._tx() as db:
            try:
                cur = db.execute(
                    """INSERT INTO actions (generation_key, mode, request_id, policy_version, created_at,
                                            bytes_sent_at_action, state)
                       VALUES (?, ?, ?, ?, ?, ?, 'RESERVED')""",
                    (generation_key, mode, request_id, policy_version, _ts(now), bytes_sent),
                )
            except sqlite3.IntegrityError:
                return None
            action_id = int(cur.lastrowid or 0)
            self._append(
                db,
                now,
                "action_reserved",
                {
                    "action_id": action_id,
                    "generation_key": generation_key,
                    "mode": mode,
                    "request_id": request_id,
                    "policy_version": policy_version,
                    "evidence": evidence,
                },
            )
            return action_id

    def complete_action(self, now: datetime, action_id: int, state: str, response: dict[str, Any]) -> None:
        with self._tx() as db:
            db.execute(
                "UPDATE actions SET state = ?, response = ? WHERE id = ? AND state = 'RESERVED'",
                (state, json.dumps(response, sort_keys=True), action_id),
            )
            self._append(db, now, "action_result", {"action_id": action_id, "state": state, "response": response})

    def record_verification(self, now: datetime, action_id: int, result: str, detail: dict[str, Any]) -> None:
        with self._tx() as db:
            db.execute(
                "UPDATE actions SET verify_result = ?, verified_at = ? WHERE id = ? AND verify_result IS NULL",
                (result, _ts(now), action_id),
            )
            self._append(db, now, "action_verified", {"action_id": action_id, "result": result, **detail})

    def reconcile_observed_action(self, now: datetime, action_id: int, response: dict[str, Any]) -> None:
        """Preserve the original UNKNOWN audit and append the bound Executor's terminal evidence."""
        with self._tx() as db:
            changed = db.execute(
                "UPDATE actions SET state='EFFECT_OBSERVED', response=? WHERE id=? AND state='OUTCOME_UNKNOWN'",
                (json.dumps(response, sort_keys=True), action_id),
            ).rowcount
            if changed:
                self._append(
                    db,
                    now,
                    "action_reconciled",
                    {
                        "action_id": action_id,
                        "previous_state": "OUTCOME_UNKNOWN",
                        "state": "EFFECT_OBSERVED",
                        "response": response,
                    },
                )

    def resumed_enforce_actions(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT id, request_id FROM actions WHERE mode = 'enforce' AND verify_result = 'DELIVERY_RESUMED' ORDER BY id"
        ).fetchall()

    def unverified_actions(self) -> list[sqlite3.Row]:
        return self.db.execute("SELECT * FROM actions WHERE verify_result IS NULL ORDER BY id").fetchall()

    def history(self, *, now: datetime, generation_key: str, mode: str) -> History:
        db = self.db
        acted = db.execute("SELECT 1 FROM actions WHERE generation_key = ? AND mode = ?", (generation_key, mode)).fetchone()
        last = db.execute("SELECT MAX(created_at) FROM actions WHERE mode = ?", (mode,)).fetchone()[0]

        def count_since(delta: timedelta) -> int:
            return int(
                db.execute("SELECT COUNT(*) FROM actions WHERE mode = ? AND created_at >= ?", (mode, _ts(now - delta))).fetchone()[0]
            )

        return History(
            generation_already_acted=acted is not None,
            last_action_at=datetime.fromisoformat(last) if last else None,
            actions_last_hour=count_since(timedelta(hours=1)),
            actions_last_day=count_since(timedelta(days=1)),
        )

    # --- key/value ---------------------------------------------------------------------

    def get(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])

    def put(self, key: str, value: str, *, now: datetime | None = None, audit_kind: str = "", audit: dict[str, Any] | None = None) -> None:
        with self._tx() as db:
            db.execute("INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
            if audit_kind:
                assert now is not None
                self._append(db, now, audit_kind, audit or {})
