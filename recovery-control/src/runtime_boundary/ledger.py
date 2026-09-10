from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .model import EffectRequest


def now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class HandoffDecision:
    accepted: bool
    reason: str
    producer_id: str
    producer_generation: int
    authority_version: int


class EffectLedger:
    def __init__(
        self,
        path: Path,
        *,
        initial_producer_id: str,
        initial_producer_generation: int,
        allow_initialize: bool = True,
    ) -> None:
        self.path = path
        self._lock = threading.RLock()
        if self.path.is_symlink():
            raise RuntimeError("LEDGER_SYMLINK_NOT_ALLOWED")
        if not self.path.exists() and not allow_initialize:
            raise RuntimeError("LEDGER_MISSING_INITIALIZATION_REQUIRED")
        if allow_initialize:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        elif not self.path.parent.is_dir():
            raise RuntimeError("LEDGER_PARENT_MISSING")
        self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=5.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA synchronous=FULL")
        mode = str(self.connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        if mode != "wal":
            raise RuntimeError(f"LEDGER_JOURNAL_MODE_{mode.upper()}")
        established = self.connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_authority'").fetchone()
        if established is None and not allow_initialize:
            self.connection.close()
            raise RuntimeError("LEDGER_SCHEMA_MISSING_INITIALIZATION_REQUIRED")
        try:
            self._migrate(initial_producer_id, initial_producer_generation)
            self.reconcile_started()
        except BaseException:
            self.connection.close()
            raise

    def _migrate(self, producer_id: str, generation: int) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS runtime_authority (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                producer_id TEXT NOT NULL,
                producer_generation INTEGER NOT NULL CHECK(producer_generation > 0),
                authority_version INTEGER NOT NULL CHECK(authority_version > 0),
                projection_high_water INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS effect_requests (
                request_id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                request_digest TEXT NOT NULL,
                producer_id TEXT NOT NULL,
                producer_generation INTEGER NOT NULL,
                operation TEXT NOT NULL CHECK(operation = 'restart_ffmpeg'),
                correlation_id TEXT NOT NULL,
                target_snapshot_id TEXT NOT NULL,
                runtime_observation_id TEXT NOT NULL,
                expected_executor_instance_id TEXT NOT NULL,
                maintenance_evidence_status TEXT NOT NULL,
                projection_id TEXT NOT NULL,
                projection_sequence INTEGER NOT NULL,
                target_identity_json TEXT NOT NULL,
                expected_ffmpeg_generation TEXT NOT NULL,
                state TEXT NOT NULL,
                result_json TEXT,
                accepted_at TEXT NOT NULL,
                execution_started_at TEXT,
                finished_at TEXT
            );
            CREATE INDEX IF NOT EXISTS effect_requests_state_idx ON effect_requests(state);
            CREATE INDEX IF NOT EXISTS effect_requests_correlation_id_idx
                ON effect_requests(correlation_id);
            CREATE TABLE IF NOT EXISTS typed_effect_requests (
                request_id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                request_digest TEXT NOT NULL,
                producer_id TEXT NOT NULL,
                producer_generation INTEGER NOT NULL,
                intent_type TEXT NOT NULL CHECK(intent_type IN (
                    'RESTART_FFMPEG','RECONCILE_FFMPEG','ESCALATE_RUNTIME_RECOVERY'
                )),
                operation TEXT NOT NULL,
                failure_domain TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                target_snapshot_id TEXT NOT NULL,
                runtime_snapshot_id TEXT NOT NULL,
                runtime_observation_id TEXT NOT NULL,
                expected_executor_instance_id TEXT NOT NULL,
                maintenance_evidence_status TEXT NOT NULL,
                projection_id TEXT NOT NULL,
                projection_sequence INTEGER NOT NULL,
                identity_type TEXT NOT NULL CHECK(identity_type IN ('FFMPEG_TARGET','RUNTIME')),
                identity_json TEXT NOT NULL,
                ffmpeg_target_identity_json TEXT,
                runtime_identity_json TEXT,
                expected_ffmpeg_generation TEXT NOT NULL,
                request_json TEXT NOT NULL,
                state TEXT NOT NULL,
                result_json TEXT,
                accepted_at TEXT NOT NULL,
                execution_started_at TEXT,
                finished_at TEXT
            );
            CREATE INDEX IF NOT EXISTS typed_effect_requests_state_idx
                ON typed_effect_requests(state);
            CREATE INDEX IF NOT EXISTS typed_effect_requests_correlation_id_idx
                ON typed_effect_requests(correlation_id);
            CREATE TABLE IF NOT EXISTS effect_scope_fences (
                effect_scope_id TEXT PRIMARY KEY,
                logical_generation_scope_id TEXT,
                action TEXT NOT NULL,
                identity_json TEXT NOT NULL,
                owner_request_id TEXT NOT NULL UNIQUE,
                owner_request_digest TEXT NOT NULL,
                state TEXT NOT NULL,
                effect_boundary_reached INTEGER NOT NULL DEFAULT 0 CHECK(effect_boundary_reached IN (0,1)),
                physical_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(physical_attempt_count BETWEEN 0 AND 1),
                result_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS effect_scope_fences_state_idx
                ON effect_scope_fences(state);
            CREATE TABLE IF NOT EXISTS effect_reconciliations (
                reconciliation_id TEXT PRIMARY KEY,
                effect_scope_id TEXT NOT NULL REFERENCES effect_scope_fences(effect_scope_id),
                resolution TEXT NOT NULL CHECK(resolution IN (
                    'EFFECT_OBSERVED','NO_EFFECT_PROVEN','REMAINS_UNKNOWN'
                )),
                evidence_json TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS effect_reconciliations_scope_idx
                ON effect_reconciliations(effect_scope_id,recorded_at);
            CREATE TABLE IF NOT EXISTS effect_scope_retirements (
                reconciliation_id TEXT PRIMARY KEY,
                effect_scope_id TEXT NOT NULL REFERENCES effect_scope_fences(effect_scope_id),
                evidence_json TEXT NOT NULL,
                evidence_digest TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS effect_scope_retirements_scope_idx
                ON effect_scope_retirements(effect_scope_id);
            """
        )
        columns = {str(row[1]) for row in self.connection.execute("PRAGMA table_info(effect_requests)")}
        for name, definition in (
            ("target_snapshot_id", "TEXT NOT NULL DEFAULT ''"),
            ("runtime_observation_id", "TEXT NOT NULL DEFAULT ''"),
            ("expected_executor_instance_id", "TEXT NOT NULL DEFAULT ''"),
            ("maintenance_evidence_status", "TEXT NOT NULL DEFAULT 'UNKNOWN'"),
        ):
            if name not in columns:
                self.connection.execute(f"ALTER TABLE effect_requests ADD COLUMN {name} {definition}")
        fence_columns = {str(row[1]) for row in self.connection.execute("PRAGMA table_info(effect_scope_fences)")}
        if "logical_generation_scope_id" not in fence_columns:
            self.connection.execute("ALTER TABLE effect_scope_fences ADD COLUMN logical_generation_scope_id TEXT")
        self.connection.execute(
            """
            INSERT OR IGNORE INTO runtime_authority(
                singleton,producer_id,producer_generation,authority_version,updated_at
            ) VALUES(1,?,?,1,?)
            """,
            (producer_id, generation, now_text()),
        )
        self._backfill_effect_scope_fences()
        self._backfill_logical_generation_scopes()
        self.connection.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS one_runtime_effect_per_logical_generation
               ON effect_scope_fences(logical_generation_scope_id)
               WHERE logical_generation_scope_id IS NOT NULL"""
        )

    def _backfill_logical_generation_scopes(self) -> None:
        """Fail closed if legacy rows split one generation across multiple PIDs."""

        from cra_dell_recovery.effect_scope import logical_generation_scope_id

        planned: list[tuple[str, str]] = []
        owners: dict[str, tuple[str, str]] = {}
        for row in self.connection.execute(
            """SELECT effect_scope_id,action,identity_json
               FROM effect_scope_fences ORDER BY created_at,effect_scope_id"""
        ):
            if str(row["action"]) != "restart_ffmpeg":
                continue
            try:
                identity = dict(json.loads(str(row["identity_json"])))
                logical_id = logical_generation_scope_id("restart_ffmpeg", identity)
                exact = json.dumps(identity, sort_keys=True, separators=(",", ":"))
            except (KeyError, TypeError, ValueError):
                continue
            owner = owners.get(logical_id)
            if owner is not None and owner != (str(row["effect_scope_id"]), exact):
                raise RuntimeError("RUNTIME_LOGICAL_GENERATION_PID_INVARIANT_BROKEN")
            owners[logical_id] = (str(row["effect_scope_id"]), exact)
            planned.append((logical_id, str(row["effect_scope_id"])))
        for logical_id, scope_id in planned:
            self.connection.execute(
                """UPDATE effect_scope_fences SET logical_generation_scope_id=?
                   WHERE effect_scope_id=? AND logical_generation_scope_id IS NULL""",
                (logical_id, scope_id),
            )

    def _backfill_effect_scope_fences(self) -> None:
        """Conservatively consume scopes recorded before target-wide fencing existed."""

        candidates: list[tuple[str, str, str, str, str, int, str | None, str]] = []
        for row in self.connection.execute(
            """SELECT request_id,request_digest,request_json,state,result_json,identity_json,operation
               FROM typed_effect_requests ORDER BY accepted_at"""
        ):
            try:
                request = EffectRequest.from_mapping(dict(json.loads(str(row["request_json"]))))
            except (KeyError, TypeError, ValueError):
                continue
            boundary = 0 if str(row["state"]) == "ACCEPTED" else 1
            candidates.append(
                (
                    request.effect_scope_id,
                    str(row["operation"]),
                    str(row["identity_json"]),
                    str(row["request_id"]),
                    str(row["request_digest"]),
                    boundary,
                    None if row["result_json"] is None else str(row["result_json"]),
                    str(row["state"]),
                )
            )
        from cra_dell_recovery.effect_scope import effect_scope_id as restart_scope_id

        for row in self.connection.execute(
            """SELECT request_id,request_digest,state,result_json,target_identity_json,operation
               FROM effect_requests ORDER BY accepted_at"""
        ):
            try:
                identity = dict(json.loads(str(row["target_identity_json"])))
                scope_id = restart_scope_id("restart_ffmpeg", identity)
            except (TypeError, ValueError):
                continue
            boundary = 0 if str(row["state"]) == "ACCEPTED" else 1
            candidates.append(
                (
                    scope_id,
                    str(row["operation"]),
                    json.dumps(identity, sort_keys=True, separators=(",", ":")),
                    str(row["request_id"]),
                    str(row["request_digest"]),
                    boundary,
                    None if row["result_json"] is None else str(row["result_json"]),
                    str(row["state"]),
                )
            )
        for scope_id, action, identity_json, request_id, request_digest, boundary, result_json, state in candidates:
            self.connection.execute(
                """INSERT OR IGNORE INTO effect_scope_fences(
                       effect_scope_id,action,identity_json,owner_request_id,owner_request_digest,
                       state,effect_boundary_reached,physical_attempt_count,result_json,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    scope_id,
                    action,
                    identity_json,
                    request_id,
                    request_digest,
                    state,
                    boundary,
                    boundary,
                    result_json,
                    now_text(),
                    now_text(),
                ),
            )
            if boundary:
                self.connection.execute(
                    """UPDATE effect_scope_fences SET effect_boundary_reached=1,physical_attempt_count=1,
                           state=CASE WHEN state='ACCEPTED' THEN 'OUTCOME_UNKNOWN' ELSE state END,
                           updated_at=? WHERE effect_scope_id=?""",
                    (now_text(), scope_id),
                )

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def authority(self) -> dict[str, Any]:
        with self._lock:
            row = self.connection.execute("SELECT * FROM runtime_authority WHERE singleton=1").fetchone()
            if row is None:
                raise RuntimeError("AUTHORITY_MISSING")
            return dict(row)

    def unresolved_count(self) -> int:
        with self._lock:
            legacy = self.connection.execute(
                """SELECT count(*) FROM effect_requests r
                   LEFT JOIN effect_scope_fences f ON f.owner_request_id=r.request_id
                   WHERE (
                         r.state IN ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')
                         OR (r.state='EFFECT_OBSERVED' AND f.state='EFFECT_OBSERVED_AWAITING_VERIFICATION')
                     )
                     AND coalesce(f.state,'') NOT IN (
                         'RECONCILED_EFFECT_OBSERVED','RELEASED_NO_EFFECT','RETIRED_TARGET_OUTCOME_UNKNOWN'
                     )"""
            ).fetchone()
            typed = self.connection.execute(
                """SELECT count(*) FROM typed_effect_requests r
                   LEFT JOIN effect_scope_fences f ON f.owner_request_id=r.request_id
                   WHERE (
                         r.state IN ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')
                         OR (r.state='EFFECT_OBSERVED' AND f.state='EFFECT_OBSERVED_AWAITING_VERIFICATION')
                     )
                     AND coalesce(f.state,'') NOT IN (
                         'RECONCILED_EFFECT_OBSERVED','RELEASED_NO_EFFECT','RETIRED_TARGET_OUTCOME_UNKNOWN'
                     )"""
            ).fetchone()
            return int(legacy[0]) + int(typed[0])

    def raw_unresolved_count(self) -> int:
        with self._lock:
            legacy = self.connection.execute(
                "SELECT count(*) FROM effect_requests WHERE state IN ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')"
            ).fetchone()
            typed = self.connection.execute(
                "SELECT count(*) FROM typed_effect_requests WHERE state IN ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')"
            ).fetchone()
            return int(legacy[0]) + int(typed[0])

    def unresolved_scopes(self) -> list[dict[str, Any]]:
        """Return the durable unresolved fences needed by a read-only controller oracle."""

        with self._lock:
            rows = self.connection.execute(
                """SELECT effect_scope_id,logical_generation_scope_id,action,identity_json,
                          owner_request_id,owner_request_digest,state,effect_boundary_reached,
                          physical_attempt_count,created_at,updated_at
                   FROM effect_scope_fences
                   WHERE state IN (
                       'ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED','OUTCOME_UNKNOWN',
                       'EFFECT_OBSERVED_AWAITING_VERIFICATION'
                   )
                   ORDER BY created_at,effect_scope_id"""
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["identity"] = json.loads(str(item.pop("identity_json")))
                result.append(item)
            return result

    def reconcile_started(self) -> int:
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = self.connection.execute(
                    "UPDATE effect_requests SET state='OUTCOME_UNKNOWN',finished_at=? WHERE state='EXECUTION_STARTED'",
                    (now_text(),),
                )
                typed_cursor = self.connection.execute(
                    "UPDATE typed_effect_requests SET state='OUTCOME_UNKNOWN',finished_at=? WHERE state='EXECUTION_STARTED'",
                    (now_text(),),
                )
                self.connection.execute(
                    """UPDATE effect_scope_fences SET state='OUTCOME_UNKNOWN',updated_at=?
                       WHERE owner_request_id IN (
                           SELECT request_id FROM typed_effect_requests WHERE state='OUTCOME_UNKNOWN'
                           UNION ALL
                           SELECT request_id FROM effect_requests WHERE state='OUTCOME_UNKNOWN'
                       ) AND state IN ('ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED')""",
                    (now_text(),),
                )
                self.connection.execute("COMMIT")
                return int(cursor.rowcount) + int(typed_cursor.rowcount)
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def switch_authority(
        self,
        *,
        expected_producer_id: str,
        expected_generation: int,
        new_producer_id: str,
        new_generation: int,
    ) -> HandoffDecision:
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute("SELECT * FROM runtime_authority WHERE singleton=1").fetchone()
                if row is None:
                    raise RuntimeError("AUTHORITY_MISSING")
                if str(row["producer_id"]) != expected_producer_id or int(row["producer_generation"]) != expected_generation:
                    self.connection.execute("ROLLBACK")
                    return HandoffDecision(
                        False,
                        "EXPECTED_AUTHORITY_MISMATCH",
                        str(row["producer_id"]),
                        int(row["producer_generation"]),
                        int(row["authority_version"]),
                    )
                if self.unresolved_count() != 0:
                    self.connection.execute("ROLLBACK")
                    return HandoffDecision(
                        False,
                        "UNRESOLVED_EXECUTION_EXISTS",
                        str(row["producer_id"]),
                        int(row["producer_generation"]),
                        int(row["authority_version"]),
                    )
                version = int(row["authority_version"]) + 1
                self.connection.execute(
                    "UPDATE runtime_authority SET producer_id=?,producer_generation=?,authority_version=?,updated_at=? WHERE singleton=1",
                    (new_producer_id, new_generation, version, now_text()),
                )
                self.connection.execute("COMMIT")
                return HandoffDecision(True, "AUTHORITY_SWITCHED", new_producer_id, new_generation, version)
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def accept(self, request: EffectRequest) -> dict[str, Any]:
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                authority = self.connection.execute("SELECT * FROM runtime_authority WHERE singleton=1").fetchone()
                if authority is None:
                    raise RuntimeError("AUTHORITY_MISSING")
                existing = self.connection.execute(
                    "SELECT * FROM effect_requests WHERE request_id=? OR idempotency_key=?",
                    (request.request_id, request.idempotency_key),
                ).fetchone()
                if existing is None:
                    existing = self.connection.execute(
                        "SELECT * FROM typed_effect_requests WHERE request_id=? OR idempotency_key=?",
                        (request.request_id, request.idempotency_key),
                    ).fetchone()
                if existing is not None:
                    if str(existing["request_digest"]) != request.digest():
                        self.connection.execute("ROLLBACK")
                        return {
                            "accepted": False,
                            "reason": "IDEMPOTENCY_CONFLICT",
                            "state": str(existing["state"]),
                            "replay": True,
                        }
                    self.connection.execute("COMMIT")
                    return {
                        "accepted": str(existing["state"]) == "ACCEPTED",
                        "reason": "IDEMPOTENT_REPLAY",
                        "state": str(existing["state"]),
                        "replay": True,
                        "result": json.loads(existing["result_json"]) if existing["result_json"] else None,
                    }
                if str(authority["producer_id"]) != request.producer_id:
                    self.connection.execute("ROLLBACK")
                    return {"accepted": False, "reason": "PRODUCER_NOT_ACTIVE", "state": "REJECTED", "replay": False}
                if int(authority["producer_generation"]) != request.producer_generation:
                    self.connection.execute("ROLLBACK")
                    return {
                        "accepted": False,
                        "reason": "PRODUCER_GENERATION_MISMATCH",
                        "state": "REJECTED",
                        "replay": False,
                    }
                scope = None
                if request.logical_generation_scope_id is not None:
                    scope = self.connection.execute(
                        "SELECT * FROM effect_scope_fences WHERE logical_generation_scope_id=?",
                        (request.logical_generation_scope_id,),
                    ).fetchone()
                if scope is None:
                    scope = self.connection.execute(
                        "SELECT * FROM effect_scope_fences WHERE effect_scope_id=?",
                        (request.effect_scope_id,),
                    ).fetchone()
                if scope is not None:
                    self.connection.execute("COMMIT")
                    same_exact_scope = str(scope["effect_scope_id"]) == request.effect_scope_id
                    return {
                        "accepted": False,
                        "reason": ("EFFECT_SCOPE_ALREADY_CLAIMED" if same_exact_scope else "LOGICAL_GENERATION_PID_INVARIANT_BROKEN"),
                        "state": str(scope["state"]),
                        "replay": True,
                        "effect_scope_id": str(scope["effect_scope_id"]),
                        "owner_request_id": str(scope["owner_request_id"]),
                        "result": json.loads(scope["result_json"]) if scope["result_json"] else None,
                    }
                unresolved = self.connection.execute(
                    """SELECT effect_scope_id,owner_request_id,state,result_json
                       FROM effect_scope_fences
                       WHERE state IN (
                           'ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED','OUTCOME_UNKNOWN',
                           'EFFECT_OBSERVED_AWAITING_VERIFICATION'
                       )
                       ORDER BY created_at,effect_scope_id LIMIT 1"""
                ).fetchone()
                if unresolved is not None:
                    self.connection.execute("COMMIT")
                    return {
                        "accepted": False,
                        "reason": "TARGET_EFFECT_UNRESOLVED",
                        "state": str(unresolved["state"]),
                        "replay": True,
                        "effect_scope_id": str(unresolved["effect_scope_id"]),
                        "owner_request_id": str(unresolved["owner_request_id"]),
                        "result": json.loads(unresolved["result_json"]) if unresolved["result_json"] else None,
                    }
                canonical = request.canonical()
                self.connection.execute(
                    """
                    INSERT INTO typed_effect_requests(
                        request_id,idempotency_key,request_digest,producer_id,
                        producer_generation,intent_type,operation,failure_domain,
                        correlation_id,target_snapshot_id,runtime_snapshot_id,
                        runtime_observation_id,expected_executor_instance_id,
                        maintenance_evidence_status,projection_id,projection_sequence,
                        identity_type,identity_json,ffmpeg_target_identity_json,
                        runtime_identity_json,expected_ffmpeg_generation,request_json,
                        state,accepted_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        request.request_id,
                        request.idempotency_key,
                        request.digest(),
                        request.producer_id,
                        request.producer_generation,
                        request.intent_type,
                        request.operation,
                        request.failure_domain,
                        request.correlation_id,
                        request.target_snapshot_id,
                        request.runtime_snapshot_id,
                        request.runtime_observation_id,
                        request.expected_executor_instance_id,
                        request.maintenance_evidence_status,
                        request.projection_id,
                        request.projection_sequence,
                        request.identity_type,
                        json.dumps(request.fence_identity, sort_keys=True, separators=(",", ":")),
                        json.dumps(request.ffmpeg_target_identity, sort_keys=True, separators=(",", ":"))
                        if request.ffmpeg_target_identity is not None
                        else None,
                        json.dumps(request.runtime_identity, sort_keys=True, separators=(",", ":"))
                        if request.runtime_identity is not None
                        else None,
                        request.expected_ffmpeg_generation,
                        json.dumps(canonical, sort_keys=True, separators=(",", ":")),
                        "ACCEPTED",
                        now_text(),
                    ),
                )
                self.connection.execute(
                    """
                    INSERT INTO effect_scope_fences(
                        effect_scope_id,logical_generation_scope_id,action,identity_json,owner_request_id,
                        owner_request_digest,state,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,'ACCEPTED',?,?)
                    """,
                    (
                        request.effect_scope_id,
                        request.logical_generation_scope_id,
                        request.operation,
                        json.dumps(request.fence_identity, sort_keys=True, separators=(",", ":")),
                        request.request_id,
                        request.digest(),
                        now_text(),
                        now_text(),
                    ),
                )
                self.connection.execute(
                    "UPDATE runtime_authority SET projection_high_water=max(projection_high_water,?),updated_at=? WHERE singleton=1",
                    (request.projection_sequence, now_text()),
                )
                self.connection.execute("COMMIT")
                return {
                    "accepted": True,
                    "reason": "ACCEPTED_DURABLE",
                    "state": "ACCEPTED",
                    "replay": False,
                    "effect_scope_id": request.effect_scope_id,
                }
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def transition(
        self,
        request_id: str,
        *,
        expected: str,
        state: str,
        result: dict[str, Any] | None = None,
        scope_state: str | None = None,
    ) -> bool:
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                timestamp_column = "execution_started_at" if state == "EXECUTION_STARTED" else "finished_at"
                encoded_result = json.dumps(result, sort_keys=True, separators=(",", ":")) if result is not None else None
                cursor = self.connection.execute(
                    f"UPDATE typed_effect_requests SET state=?,result_json=?,{timestamp_column}=? WHERE request_id=? AND state=?",
                    (state, encoded_result, now_text(), request_id, expected),
                )
                updated = int(cursor.rowcount) == 1
                if not updated:
                    legacy_cursor = self.connection.execute(
                        f"UPDATE effect_requests SET state=?,result_json=?,{timestamp_column}=? WHERE request_id=? AND state=?",
                        (state, encoded_result, now_text(), request_id, expected),
                    )
                    updated = int(legacy_cursor.rowcount) == 1
                if updated:
                    self.connection.execute(
                        "UPDATE effect_scope_fences SET state=?,result_json=coalesce(?,result_json),updated_at=? WHERE owner_request_id=?",
                        (scope_state or state, encoded_result, now_text(), request_id),
                    )
                self.connection.execute("COMMIT")
                return updated
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def mark_effect_boundary(self, request_id: str) -> bool:
        """Durably reserve the sole physical attempt before invoking the adapter."""

        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = self.connection.execute(
                    """UPDATE effect_scope_fences
                       SET state='EFFECT_BOUNDARY_REACHED',effect_boundary_reached=1,
                           physical_attempt_count=1,updated_at=?
                       WHERE owner_request_id=? AND state='EXECUTION_STARTED'
                         AND effect_boundary_reached=0 AND physical_attempt_count=0""",
                    (now_text(), request_id),
                )
                self.connection.execute("COMMIT")
                return int(cursor.rowcount) == 1
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def record_reconciliation(
        self,
        *,
        reconciliation_id: str,
        effect_scope_id: str,
        resolution: str,
        evidence: dict[str, Any],
        owner_request_id: str | None = None,
        owner_request_digest: str | None = None,
    ) -> dict[str, Any]:
        """Append an operator/oracle conclusion without erasing the original outcome."""

        if resolution not in {"EFFECT_OBSERVED", "NO_EFFECT_PROVEN", "REMAINS_UNKNOWN"}:
            raise ValueError("unsupported effect reconciliation resolution")
        encoded = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
        import hashlib

        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self.connection.execute(
                    """SELECT effect_scope_id,resolution,evidence_digest
                       FROM effect_reconciliations WHERE reconciliation_id=?""",
                    (reconciliation_id,),
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["effect_scope_id"]) != effect_scope_id
                        or str(existing["resolution"]) != resolution
                        or str(existing["evidence_digest"]) != digest
                    ):
                        raise ValueError("RECONCILIATION_ID_CONFLICT")
                    self.connection.execute("COMMIT")
                    return {
                        "recorded": True,
                        "replay": True,
                        "reconciliation_id": reconciliation_id,
                        "effect_scope_id": effect_scope_id,
                        "resolution": resolution,
                        "evidence_digest": digest,
                    }
                scope = self.connection.execute(
                    """SELECT state,physical_attempt_count,owner_request_id,owner_request_digest
                       FROM effect_scope_fences WHERE effect_scope_id=?""",
                    (effect_scope_id,),
                ).fetchone()
                if scope is None:
                    raise ValueError("effect scope is not present")
                if owner_request_id is not None and str(scope["owner_request_id"]) != owner_request_id:
                    raise ValueError("RECONCILIATION_OWNER_REQUEST_MISMATCH")
                if owner_request_digest is not None and str(scope["owner_request_digest"]) != owner_request_digest:
                    raise ValueError("RECONCILIATION_OWNER_DIGEST_MISMATCH")
                if resolution in {"EFFECT_OBSERVED", "NO_EFFECT_PROVEN"} and str(scope["state"]) not in {
                    "ACCEPTED",
                    "EXECUTION_STARTED",
                    "EFFECT_BOUNDARY_REACHED",
                    "OUTCOME_UNKNOWN",
                    "EFFECT_OBSERVED_AWAITING_VERIFICATION",
                }:
                    raise ValueError("RECONCILIATION_SCOPE_ALREADY_RESOLVED")
                if resolution == "NO_EFFECT_PROVEN" and int(evidence.get("physical_effect_count", -1)) != 0:
                    raise ValueError("NO_EFFECT_PROVEN requires physical_effect_count=0 evidence")
                if resolution == "EFFECT_OBSERVED" and int(evidence.get("physical_effect_count", -1)) != 1:
                    raise ValueError("EFFECT_OBSERVED requires physical_effect_count=1 evidence")
                self.connection.execute(
                    """INSERT INTO effect_reconciliations(
                           reconciliation_id,effect_scope_id,resolution,evidence_json,evidence_digest,recorded_at
                       ) VALUES(?,?,?,?,?,?)""",
                    (reconciliation_id, effect_scope_id, resolution, encoded, digest, now_text()),
                )
                if resolution == "EFFECT_OBSERVED":
                    self.connection.execute(
                        """UPDATE effect_scope_fences SET state='RECONCILED_EFFECT_OBSERVED',
                               effect_boundary_reached=1,physical_attempt_count=1,result_json=?,updated_at=?
                           WHERE effect_scope_id=?""",
                        (encoded, now_text(), effect_scope_id),
                    )
                elif resolution == "NO_EFFECT_PROVEN":
                    self.connection.execute(
                        """UPDATE effect_scope_fences SET state='RELEASED_NO_EFFECT',
                               effect_boundary_reached=0,physical_attempt_count=0,result_json=?,updated_at=?
                           WHERE effect_scope_id=?""",
                        (encoded, now_text(), effect_scope_id),
                    )
                self.connection.execute("COMMIT")
                return {
                    "recorded": True,
                    "replay": False,
                    "reconciliation_id": reconciliation_id,
                    "effect_scope_id": effect_scope_id,
                    "resolution": resolution,
                    "evidence_digest": digest,
                }
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def record_target_retirement(
        self,
        *,
        reconciliation_id: str,
        effect_scope_id: str,
        evidence: dict[str, Any],
        owner_request_id: str,
        owner_request_digest: str,
    ) -> dict[str, Any]:
        """Append proof that an exact old target retired while its effect outcome stays unknown."""

        import hashlib

        encoded = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self.connection.execute(
                    """SELECT effect_scope_id,evidence_digest FROM effect_scope_retirements
                       WHERE reconciliation_id=?""",
                    (reconciliation_id,),
                ).fetchone()
                if existing is not None:
                    if str(existing["effect_scope_id"]) != effect_scope_id or str(existing["evidence_digest"]) != digest:
                        raise ValueError("RECONCILIATION_ID_CONFLICT")
                    self.connection.execute("COMMIT")
                    return {
                        "recorded": True,
                        "replay": True,
                        "reconciliation_id": reconciliation_id,
                        "effect_scope_id": effect_scope_id,
                        "resolution": "TARGET_RETIRED",
                        "evidence_digest": digest,
                    }
                scope = self.connection.execute(
                    """SELECT state,physical_attempt_count,owner_request_id,owner_request_digest
                       FROM effect_scope_fences WHERE effect_scope_id=?""",
                    (effect_scope_id,),
                ).fetchone()
                if scope is None:
                    raise ValueError("effect scope is not present")
                if str(scope["owner_request_id"]) != owner_request_id:
                    raise ValueError("RECONCILIATION_OWNER_REQUEST_MISMATCH")
                if str(scope["owner_request_digest"]) != owner_request_digest:
                    raise ValueError("RECONCILIATION_OWNER_DIGEST_MISMATCH")
                if str(scope["state"]) not in {
                    "EFFECT_BOUNDARY_REACHED",
                    "EXECUTION_STARTED",
                    "OUTCOME_UNKNOWN",
                }:
                    raise ValueError("RECONCILIATION_SCOPE_ALREADY_RESOLVED")
                if int(scope["physical_attempt_count"]) != 1:
                    raise ValueError("TARGET_RETIREMENT_REQUIRES_ONE_PHYSICAL_ATTEMPT")
                self.connection.execute(
                    """INSERT INTO effect_scope_retirements(
                           reconciliation_id,effect_scope_id,evidence_json,evidence_digest,recorded_at
                       ) VALUES(?,?,?,?,?)""",
                    (reconciliation_id, effect_scope_id, encoded, digest, now_text()),
                )
                self.connection.execute(
                    """UPDATE effect_scope_fences SET state='RETIRED_TARGET_OUTCOME_UNKNOWN',
                           effect_boundary_reached=1,physical_attempt_count=1,result_json=?,updated_at=?
                       WHERE effect_scope_id=?""",
                    (encoded, now_text(), effect_scope_id),
                )
                self.connection.execute("COMMIT")
                return {
                    "recorded": True,
                    "replay": False,
                    "reconciliation_id": reconciliation_id,
                    "effect_scope_id": effect_scope_id,
                    "resolution": "TARGET_RETIRED",
                    "evidence_digest": digest,
                }
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def scope(self, effect_scope_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM effect_scope_fences WHERE effect_scope_id=?",
                (effect_scope_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def request(self, request_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.connection.execute("SELECT * FROM typed_effect_requests WHERE request_id=?", (request_id,)).fetchone()
            if row is None:
                row = self.connection.execute("SELECT * FROM effect_requests WHERE request_id=?", (request_id,)).fetchone()
            return dict(row) if row is not None else None

    def request_status(self, request_id: str) -> dict[str, Any] | None:
        """Return a read-only terminal/pending view without exposing the request payload."""

        with self._lock:
            row = self.connection.execute(
                "SELECT state,result_json FROM typed_effect_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if row is None:
                row = self.connection.execute(
                    "SELECT state,result_json FROM effect_requests WHERE request_id=?",
                    (request_id,),
                ).fetchone()
            if row is None:
                return None
            scope = self.connection.execute(
                """SELECT effect_scope_id,state,effect_boundary_reached,physical_attempt_count
                   FROM effect_scope_fences WHERE owner_request_id=?""",
                (request_id,),
            ).fetchone()
            return {
                "request_id": request_id,
                "state": str(row["state"]),
                "result": json.loads(str(row["result_json"])) if row["result_json"] else None,
                "effect_scope_id": str(scope["effect_scope_id"]) if scope is not None else "",
                "effect_scope_state": str(scope["state"]) if scope is not None else "UNKNOWN",
                "effect_boundary_reached": bool(scope["effect_boundary_reached"]) if scope is not None else False,
                "physical_attempt_count": int(scope["physical_attempt_count"]) if scope is not None else 0,
            }

    def request_status_by_correlation(self, correlation_id: str) -> dict[str, Any]:
        """Resolve one controller correlation to its durable executor request.

        Correlation IDs are intentionally separate from stable request IDs.  A
        missing or ambiguous mapping is returned explicitly so callers can
        retain their pending state and fail closed.
        """

        with self._lock:
            rows = self.connection.execute(
                """SELECT request_id FROM typed_effect_requests WHERE correlation_id=?
                   UNION
                   SELECT request_id FROM effect_requests WHERE correlation_id=?
                   ORDER BY request_id""",
                (correlation_id, correlation_id),
            ).fetchall()
            request_ids = [str(row["request_id"]) for row in rows]
            if len(request_ids) != 1:
                return {
                    "correlation_id": correlation_id,
                    "matching_count": len(request_ids),
                    "request_id": "",
                }
            status = self.request_status(request_ids[0])
            if status is None:
                raise RuntimeError("CORRELATION_REQUEST_STATUS_MISSING")
            return {
                "correlation_id": correlation_id,
                "matching_count": 1,
                **status,
            }
