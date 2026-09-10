from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, cast


def utc_now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ShadowState(StrEnum):
    STARTUP_RECONCILING = "STARTUP_RECONCILING"
    INACTIVE = "INACTIVE"
    REQUESTED = "REQUESTED"
    QUIESCING = "QUIESCING"
    ESTABLISHED = "ESTABLISHED"
    MUTATING = "MUTATING"
    VERIFYING_TARGET = "VERIFYING_TARGET"
    RECONCILING = "RECONCILING"
    EXIT_PENDING = "EXIT_PENDING"
    COMPLETED = "COMPLETED"
    QUIESCE_FAILED = "QUIESCE_FAILED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"
    SAFE_BLOCKED = "SAFE_BLOCKED"
    UNKNOWN = "UNKNOWN"


_ALLOWED: dict[ShadowState, frozenset[ShadowState]] = {
    ShadowState.INACTIVE: frozenset({ShadowState.REQUESTED}),
    ShadowState.REQUESTED: frozenset({ShadowState.QUIESCING, ShadowState.ABORTING}),
    ShadowState.QUIESCING: frozenset({ShadowState.ESTABLISHED, ShadowState.QUIESCE_FAILED, ShadowState.ABORTING}),
    ShadowState.ESTABLISHED: frozenset({ShadowState.ABORTING}),
    ShadowState.QUIESCE_FAILED: frozenset({ShadowState.ABORTING}),
    ShadowState.ABORTING: frozenset({ShadowState.ABORTED}),
    ShadowState.ABORTED: frozenset({ShadowState.INACTIVE}),
}


@dataclass(frozen=True)
class StartupProof:
    integrity_ok: bool
    startup_reconciled: bool
    unresolved_transaction_count: int
    active_fence_count: int
    incomplete_abort_count: int
    incomplete_exit_count: int
    transaction_uncertain: bool
    positive_inactive_proof: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "integrity_ok": self.integrity_ok,
            "startup_reconciled": self.startup_reconciled,
            "unresolved_transaction_count": self.unresolved_transaction_count,
            "active_fence_count": self.active_fence_count,
            "incomplete_abort_count": self.incomplete_abort_count,
            "incomplete_exit_count": self.incomplete_exit_count,
            "transaction_uncertain": self.transaction_uncertain,
            "positive_inactive_proof": self.positive_inactive_proof,
        }


class MaintenanceShadowStore:
    """WAL-backed shadow truth with explicit startup reconciliation.

    The API can only manipulate protocol evidence. It cannot call a physical
    adapter, Kubernetes, systemd, signals, or restart commands.
    """

    def __init__(self, database: Path, migration: Path, *, producer_id: str, startup: bool = True) -> None:
        database.parent.mkdir(parents=True, exist_ok=True)
        self.database = database
        self.connection = sqlite3.connect(database, isolation_level=None, timeout=5.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        if not self._has_schema():
            self.connection.executescript(migration.read_text(encoding="utf-8"))
        if startup:
            self._enter_startup_reconciliation(producer_id)

    def _has_schema(self) -> bool:
        row = self.connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_state'").fetchone()
        return row is not None

    @contextmanager
    def _transaction(self) -> Any:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def _enter_startup_reconciliation(self, producer_id: str) -> None:
        now = utc_now_text()
        with self._transaction():
            identity = self.connection.execute("SELECT * FROM coordinator_identity WHERE singleton_id=1").fetchone()
            instance_id = f"maintenance-shadow-{uuid.uuid4()}"
            if identity is None:
                self.connection.execute(
                    "INSERT INTO coordinator_identity VALUES (1,?,?,?,?,?)",
                    (f"maintenance-lineage-{uuid.uuid4()}", instance_id, producer_id, now, now),
                )
                self.connection.execute(
                    "INSERT INTO coordinator_state VALUES (1,?,?,?,?,?,?,?,0)",
                    (ShadowState.STARTUP_RECONCILING.value, None, 0, 0, 1, "STARTUP_NOT_YET_PROVEN", now),
                )
            else:
                self.connection.execute(
                    "UPDATE coordinator_identity SET coordinator_instance_id=?,producer_id=?,updated_at=? WHERE singleton_id=1",
                    (instance_id, producer_id, now),
                )
                self.connection.execute(
                    """UPDATE coordinator_state SET maintenance_state=?,startup_reconciled=0,
                       transaction_uncertain=1,reconciliation_state=?,updated_at=?,version=version+1 WHERE singleton_id=1""",
                    (ShadowState.STARTUP_RECONCILING.value, "STARTUP_NOT_YET_PROVEN", now),
                )

    def reconcile_startup(self) -> StartupProof:
        integrity = str(self.connection.execute("PRAGMA integrity_check").fetchone()[0]) == "ok"
        unresolved = int(self.connection.execute("SELECT count(*) FROM maintenance_transactions WHERE resolved=0").fetchone()[0])
        active_fences = int(
            self.connection.execute(
                "SELECT count(*) FROM maintenance_fences WHERE state IN ('PREPARED','ACTIVE','RELEASE_PREPARED')"
            ).fetchone()[0]
        )
        incomplete_abort = int(
            self.connection.execute(
                "SELECT count(*) FROM maintenance_transactions WHERE resolved=0 AND state IN ('ABORTING','QUIESCE_FAILED')"
            ).fetchone()[0]
        )
        incomplete_exit = int(
            self.connection.execute(
                """SELECT count(*) FROM maintenance_transactions WHERE resolved=0
                   AND state IN ('MUTATING','VERIFYING_TARGET','RECONCILING','EXIT_PENDING','COMPLETED')"""
            ).fetchone()[0]
        )
        positive = integrity and unresolved == 0 and active_fences == 0 and incomplete_abort == 0 and incomplete_exit == 0
        now = utc_now_text()
        state = ShadowState.INACTIVE if positive else ShadowState.SAFE_BLOCKED
        reason = "POSITIVE_INACTIVE_RECONCILIATION" if positive else "UNRESOLVED_DURABLE_MAINTENANCE_STATE"
        with self._transaction():
            self.connection.execute(
                """UPDATE coordinator_state SET maintenance_state=?,maintenance_id=NULL,
                   startup_reconciled=1,transaction_uncertain=?,reconciliation_state=?,updated_at=?,version=version+1
                   WHERE singleton_id=1""",
                (state.value, 0 if positive else 1, reason, now),
            )
            self._record_transition(None, self.generation(), ShadowState.STARTUP_RECONCILING, state, reason, now)
        return StartupProof(integrity, True, unresolved, active_fences, incomplete_abort, incomplete_exit, not positive, positive)

    def state(self) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM coordinator_state WHERE singleton_id=1").fetchone()
        if row is None:
            raise RuntimeError("coordinator state missing")
        return cast(sqlite3.Row, row)

    def identity(self) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM coordinator_identity WHERE singleton_id=1").fetchone()
        if row is None:
            raise RuntimeError("coordinator identity missing")
        return cast(sqlite3.Row, row)

    def generation(self) -> int:
        return int(self.state()["maintenance_generation"])

    def startup_proof(self) -> StartupProof:
        state = self.state()
        integrity = str(self.connection.execute("PRAGMA quick_check").fetchone()[0]) == "ok"
        unresolved = int(self.connection.execute("SELECT count(*) FROM maintenance_transactions WHERE resolved=0").fetchone()[0])
        active_fences = int(
            self.connection.execute(
                "SELECT count(*) FROM maintenance_fences WHERE state IN ('PREPARED','ACTIVE','RELEASE_PREPARED')"
            ).fetchone()[0]
        )
        incomplete_abort = int(
            self.connection.execute(
                "SELECT count(*) FROM maintenance_transactions WHERE resolved=0 AND state IN ('ABORTING','QUIESCE_FAILED')"
            ).fetchone()[0]
        )
        incomplete_exit = int(
            self.connection.execute(
                """SELECT count(*) FROM maintenance_transactions WHERE resolved=0
                   AND state IN ('MUTATING','VERIFYING_TARGET','RECONCILING','EXIT_PENDING','COMPLETED')"""
            ).fetchone()[0]
        )
        reconciled = bool(state["startup_reconciled"])
        uncertain = bool(state["transaction_uncertain"])
        positive = (
            integrity
            and reconciled
            and not uncertain
            and str(state["maintenance_state"]) == ShadowState.INACTIVE.value
            and unresolved == 0
            and active_fences == 0
            and incomplete_abort == 0
            and incomplete_exit == 0
        )
        return StartupProof(integrity, reconciled, unresolved, active_fences, incomplete_abort, incomplete_exit, uncertain, positive)

    def start_rehearsal(self, target_identity: dict[str, Any] | None, *, maintenance_id: str | None = None) -> tuple[str, int]:
        current = self.state()
        if str(current["maintenance_state"]) != ShadowState.INACTIVE.value or not self.startup_proof().positive_inactive_proof:
            raise ValueError("positive INACTIVE proof required")
        identifier = maintenance_id or f"maintenance-shadow-{uuid.uuid4()}"
        generation = int(current["maintenance_generation"]) + 1
        now = utc_now_text()
        encoded_target = json.dumps(target_identity, separators=(",", ":"), sort_keys=True) if target_identity else None
        with self._transaction():
            self.connection.execute(
                "INSERT INTO maintenance_transactions VALUES (?,?,?,?,0,?,?)",
                (identifier, generation, ShadowState.REQUESTED.value, encoded_target, now, now),
            )
            self.connection.execute(
                """UPDATE coordinator_state SET maintenance_state=?,maintenance_id=?,maintenance_generation=?,
                   transaction_uncertain=0,reconciliation_state=?,updated_at=?,version=version+1 WHERE singleton_id=1""",
                (ShadowState.REQUESTED.value, identifier, generation, "SHADOW_REHEARSAL_ACTIVE", now),
            )
            self._record_transition(identifier, generation, ShadowState.INACTIVE, ShadowState.REQUESTED, "NO_EFFECT_REHEARSAL_STARTED", now)
        return identifier, generation

    def transition(self, maintenance_id: str, generation: int, to_state: ShadowState, *, reason: str) -> None:
        current = self.state()
        from_state = ShadowState(str(current["maintenance_state"]))
        if current["maintenance_id"] != maintenance_id or int(current["maintenance_generation"]) != generation:
            raise ValueError("maintenance identity or generation mismatch")
        if to_state not in _ALLOWED.get(from_state, frozenset()):
            raise ValueError(f"invalid shadow transition {from_state.value}->{to_state.value}")
        now = utc_now_text()
        resolved = to_state == ShadowState.INACTIVE
        with self._transaction():
            self.connection.execute(
                "UPDATE maintenance_transactions SET state=?,resolved=?,updated_at=? WHERE maintenance_id=?",
                (to_state.value, int(resolved), now, maintenance_id),
            )
            self.connection.execute(
                """UPDATE coordinator_state SET maintenance_state=?,maintenance_id=?,reconciliation_state=?,
                   transaction_uncertain=0,updated_at=?,version=version+1 WHERE singleton_id=1""",
                (to_state.value, None if resolved else maintenance_id, "POSITIVE_INACTIVE_RECONCILIATION" if resolved else reason, now),
            )
            self._record_transition(maintenance_id, generation, from_state, to_state, reason, now)

    def current_target(self) -> dict[str, Any] | None:
        state = self.state()
        identifier = state["maintenance_id"]
        if identifier is None:
            row = self.connection.execute(
                "SELECT target_identity_json FROM maintenance_transactions ORDER BY maintenance_generation DESC LIMIT 1"
            ).fetchone()
        else:
            row = self.connection.execute(
                "SELECT target_identity_json FROM maintenance_transactions WHERE maintenance_id=?", (identifier,)
            ).fetchone()
        if row is None or row["target_identity_json"] is None:
            return None
        value = json.loads(str(row["target_identity_json"]))
        return dict(value) if isinstance(value, dict) else None

    def authorization_projections(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT projection_json FROM maintenance_authorizations ORDER BY created_at, authorization_id"
        ).fetchall()
        return [dict(json.loads(str(row["projection_json"]))) for row in rows]

    def _record_transition(
        self,
        maintenance_id: str | None,
        generation: int,
        from_state: ShadowState,
        to_state: ShadowState,
        reason: str,
        occurred_at: str,
    ) -> None:
        self.connection.execute(
            "INSERT INTO coordinator_transitions VALUES (?,?,?,?,?,?,?,0)",
            (f"transition-{uuid.uuid4()}", maintenance_id, generation, from_state.value, to_state.value, reason, occurred_at),
        )

    def physical_effect_count(self) -> int:
        return int(self.connection.execute("SELECT coalesce(sum(physical_effect_count),0) FROM coordinator_transitions").fetchone()[0])

    def close(self) -> None:
        self.connection.close()
