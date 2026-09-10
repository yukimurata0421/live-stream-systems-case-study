from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from cra_dell_recovery.canonical import payload_digest
from cra_dell_recovery.errors import CommandBlocked
from cra_dell_recovery.models import RecoveryAuthorizationInput
from cra_dell_recovery.sqlite import SQLiteLedger, SQLiteRuntimeStatus
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now

from .retention import SignedNoActionArchive
from .schema_contract import UNRESOLVED_COMMAND_STATES, verify_central_schema_contract


class NoActionCentralStore:
    """CRA NO_ACTION ledger with no authorization, command, delivery, or effect API."""

    def __init__(self, path: Path, migration: Path) -> None:
        self.__ledger = SQLiteLedger(path, migration)
        verify_central_schema_contract(self.__ledger)

    @property
    def status(self) -> SQLiteRuntimeStatus:
        return self.__ledger.status

    def close(self) -> None:
        self.__ledger.close()

    def read_one(self, sql: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        return self.__ledger.read_one(sql, parameters)

    def integrity_check(self) -> str:
        return self.__ledger.integrity_check()

    def checkpoint(self, mode: str = "PASSIVE") -> tuple[int, int, int]:
        return self.__ledger.checkpoint(mode)

    def backup_to(self, destination: Path) -> str:
        return self.__ledger.backup_to(destination)

    def compact_no_action_evidence(
        self,
        archive: SignedNoActionArchive,
        *,
        retain_decision_count: int,
        batch_size: int = 512,
    ) -> int:
        if retain_decision_count < 2 or not 1 <= batch_size <= 4096:
            raise ValueError("CRA_RETENTION_LIMIT_INVALID")
        decisions = self.__ledger.read_all(
            """SELECT * FROM cra_policy_decisions
               WHERE decision_id NOT IN (
                   SELECT decision_id FROM cra_policy_decisions
                   ORDER BY decided_at DESC,decision_id DESC LIMIT ?
               )
               AND projection_id NOT IN (
                   SELECT p.projection_id FROM monitoring_evidence_projections AS p
                   WHERE p.observation_sequence=(
                       SELECT max(newest.observation_sequence) FROM monitoring_evidence_projections AS newest
                       WHERE newest.source_instance_id=p.source_instance_id
                   )
               )
               ORDER BY decided_at ASC,decision_id ASC LIMIT ?""",
            (retain_decision_count, batch_size),
        )
        archived: list[tuple[str, str, str]] = []
        for decision in decisions:
            projection = self.__ledger.read_one(
                "SELECT * FROM monitoring_evidence_projections WHERE projection_id=?",
                (str(decision["projection_id"]),),
            )
            if projection is None:
                raise RuntimeError("CRA_RETENTION_PROJECTION_MISSING")
            record_id = f"no-action-decision:{decision['decision_id']}"
            payload = {"decision": dict(decision), "projection": dict(projection)}
            digest = archive.append(
                record_id=record_id,
                record_type="NO_ACTION_DECISION_WITH_PROJECTION",
                source_created_at=str(decision["decided_at"]),
                payload=payload,
            )
            if not archive.contains(record_id, digest):
                raise RuntimeError("CRA_RETENTION_ARCHIVE_NOT_DURABLE")
            archived.append((str(decision["decision_id"]), str(decision["projection_id"]), record_id))
        if not archived:
            archive.checkpoint_completed_days()
            return 0
        deleted = 0
        with self.__ledger.write() as database:
            for decision_id, projection_id, _ in archived:
                referenced = database.execute(
                    """SELECT count(*) FROM cra_verifier_decisions
                       WHERE pre_projection_id=? OR post_projection_id=?""",
                    (projection_id, projection_id),
                ).fetchone()
                if referenced is not None and int(referenced[0]) != 0:
                    continue
                cursor = database.execute("DELETE FROM cra_policy_decisions WHERE decision_id=?", (decision_id,))
                deleted += int(cursor.rowcount)
                database.execute(
                    """DELETE FROM monitoring_evidence_projections WHERE projection_id=?
                       AND NOT EXISTS(
                           SELECT 1 FROM cra_policy_decisions WHERE projection_id=?
                       )
                       AND observation_sequence < (
                           SELECT max(p.observation_sequence) FROM monitoring_evidence_projections AS p
                           WHERE p.source_instance_id=monitoring_evidence_projections.source_instance_id
                       )""",
                    (projection_id, projection_id),
                )
        archive.checkpoint_completed_days()
        return deleted

    def bootstrap(
        self,
        *,
        target_id: str,
        host_id: str,
        agent_id: str,
        controller_instance_id: str,
        agent_installation_id: str,
        authority_epoch: int = 1,
        authority_session_id: str = "session-1",
        now: str | None = None,
    ) -> None:
        stamp = now or isoformat_utc(utc_now())
        with self.__ledger.write() as db:
            db.execute(
                "INSERT INTO control_plane_identity VALUES (1,?,?,?,?,NULL,?,?,0)",
                (str(uuid.uuid4()), str(uuid.uuid4()), controller_instance_id, "CLEAN", stamp, stamp),
            )
            db.execute(
                "INSERT INTO targets VALUES (?,?,?,?,?,1,?,0,0,?,?,0)",
                (
                    target_id,
                    host_id,
                    agent_id,
                    "SAFE_BLOCKED",
                    authority_epoch,
                    agent_installation_id,
                    "NO_ACTION_REQUIRES_FORMAL_RECONCILIATION",
                    stamp,
                ),
            )
            db.execute(
                "INSERT INTO authority_sessions VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)",
                (
                    authority_session_id,
                    target_id,
                    authority_epoch,
                    controller_instance_id,
                    agent_installation_id,
                    f"recon-{authority_session_id}",
                    f"challenge-{authority_session_id}",
                    "BLOCKED",
                    stamp,
                ),
            )

    def ingest_monitoring_projection(self, projection: dict[str, Any]) -> str:
        stamp = isoformat_utc(utc_now())
        with self.__ledger.write() as db:
            existing = db.execute(
                "SELECT payload_sha256 FROM monitoring_evidence_projections WHERE projection_id=?",
                (projection["projection_id"],),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_sha256"]) == str(projection["payload_sha256"]):
                    return "DUPLICATE"
                raise CommandBlocked("MONITORING_PROJECTION_ID_CONFLICT")
            high = db.execute(
                """SELECT observation_sequence FROM monitoring_evidence_projections
                   WHERE source_instance_id=? ORDER BY observation_sequence DESC LIMIT 1""",
                (projection["source_instance_id"],),
            ).fetchone()
            if high is not None and int(projection["observation_sequence"]) <= int(high[0]):
                raise CommandBlocked("MONITORING_PROJECTION_SEQUENCE_REGRESSION")
            incident = dict(projection["incident"])
            db.execute(
                """INSERT INTO monitoring_evidence_projections VALUES (
                       ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                   )""",
                (
                    projection["projection_id"],
                    projection["target_id"],
                    projection["source_instance_id"],
                    projection["source_release_id"],
                    projection["monitoring_cycle_id"],
                    projection["observation_revision"],
                    projection["observation_sequence"],
                    incident["incident_id"],
                    incident["state"],
                    projection["observed_at"],
                    projection["issued_at"],
                    projection["expires_at"],
                    json.dumps(projection, separators=(",", ":"), sort_keys=True),
                    projection["payload_sha256"],
                    projection["key_id"],
                    projection["signature"],
                    stamp,
                ),
            )
        return "ACCEPTED"

    def central_policy_blockers(
        self,
        target_id: str,
        *,
        now: Any,
        minimum_action_interval_sec: int,
        hourly_action_limit: int,
        daily_action_limit: int,
    ) -> tuple[str, ...]:
        blockers: list[str] = []
        unresolved_states = tuple(sorted(UNRESOLVED_COMMAND_STATES))
        unresolved = self.__ledger.read_one(
            f"SELECT count(*) FROM commands WHERE target_id=? AND status IN ({','.join('?' for _ in unresolved_states)})",
            (target_id, *unresolved_states),
        )
        if unresolved is None or int(unresolved[0]):
            blockers.append("CENTRAL_UNRESOLVED_COMMAND")
        completed = [
            parse_utc(str(row[0]))
            for row in self.__ledger.read_all(
                """SELECT effect_boundary_reached_at FROM effect_scope_ledger
                   WHERE target_id=? AND physical_attempt_count=1
                     AND effect_boundary_reached_at IS NOT NULL""",
                (target_id,),
            )
        ]
        if completed and (now - max(completed)).total_seconds() < minimum_action_interval_sec:
            blockers.append("CENTRAL_HARD_COOLDOWN_ACTIVE")
        hourly = sum(1 for item in completed if (now - item).total_seconds() <= 3600)
        daily = sum(1 for item in completed if (now - item).total_seconds() <= 86400)
        if hourly >= hourly_action_limit:
            blockers.append("CENTRAL_HOURLY_ACTION_BUDGET_EXCEEDED")
        if daily >= daily_action_limit:
            blockers.append("CENTRAL_DAILY_ACTION_BUDGET_EXCEEDED")
        return tuple(blockers)

    def existing_policy_decision(
        self,
        projection_id: str,
        policy_revision: str,
    ) -> tuple[str, str, str | None, str, str, tuple[str, ...], str] | None:
        row = self.__ledger.read_one(
            """SELECT decision_id,decision,action,candidate_reason_code,decision_reason_code,
                      blockers_json,decision_digest FROM cra_policy_decisions
               WHERE projection_id=? AND policy_revision=?""",
            (projection_id, policy_revision),
        )
        if row is None:
            return None
        return (
            str(row["decision_id"]),
            str(row["decision"]),
            None if row["action"] is None else str(row["action"]),
            str(row["candidate_reason_code"]),
            str(row["decision_reason_code"]),
            tuple(json.loads(str(row["blockers_json"]))),
            str(row["decision_digest"]),
        )

    def persist_policy_decision(
        self,
        *,
        projection: dict[str, Any],
        authorization: RecoveryAuthorizationInput,
        decision_id: str,
        decision: str,
        blockers: tuple[str, ...],
        action: str | None = None,
        candidate_reason_code: str | None = None,
        decision_reason_code: str | None = None,
        reason_code: str | None = None,
    ) -> str:
        if decision == "AUTHORIZED" or not blockers or "CRA_OPERATING_MODE_NO_ACTION" not in blockers:
            raise ValueError("NO_ACTION_STORE_REFUSES_EXECUTABLE_POLICY_DECISION")
        resolved_candidate_reason_code = candidate_reason_code or authorization.reason_code
        resolved_decision_reason_code = (
            decision_reason_code
            or reason_code
            or (
                "INCIDENT_NOT_CONFIRMED"
                if decision == "NO_ACTION"
                else next((item for item in blockers if item != "CRA_OPERATING_MODE_NO_ACTION"), "CRA_OPERATING_MODE_NO_ACTION")
            )
        )
        if action is not None or not resolved_candidate_reason_code.strip() or not resolved_decision_reason_code.strip():
            raise ValueError("NO_ACTION_DECISION_SEMANTICS_INVALID")
        incident = projection.get("incident")
        observed_target = projection.get("observed_target")
        context_matches = (
            isinstance(incident, dict)
            and authorization.target_id == projection.get("target_id")
            and authorization.incident_id == incident.get("incident_id")
            and authorization.source_episode_id == incident.get("source_episode_id")
            and authorization.observation_revision == projection.get("observation_revision")
            and tuple(blockers) == authorization.blockers
            and isinstance(observed_target, dict)
            and authorization.expected_target.to_dict() == observed_target
        )
        if not context_matches:
            raise ValueError("NO_ACTION_DECISION_CONTEXT_MISMATCH")
        parse_utc(authorization.authorized_at)
        authorization_expiry = parse_utc(authorization.expires_at)
        projection_expiry = parse_utc(str(projection["expires_at"]))
        if authorization_expiry > projection_expiry:
            raise ValueError("NO_ACTION_DECISION_TIME_BOUNDARY_MISMATCH")
        expected_json = json.dumps(authorization.expected_target.to_dict(), separators=(",", ":"), sort_keys=True)
        decision_digest = payload_digest(
            {
                "decision_id": decision_id,
                "projection_id": projection["projection_id"],
                "target_id": authorization.target_id,
                "incident_id": authorization.incident_id,
                "source_episode_id": authorization.source_episode_id,
                "observation_revision": authorization.observation_revision,
                "policy_revision": authorization.policy_revision,
                "decision": decision,
                "action": action,
                "candidate_reason_code": resolved_candidate_reason_code,
                "decision_reason_code": resolved_decision_reason_code,
                "reason_code": resolved_decision_reason_code,
                "blockers": list(blockers),
                "expected_target": authorization.expected_target.to_dict(),
                "decided_at": authorization.authorized_at,
                "expires_at": authorization.expires_at,
            }
        )
        with self.__ledger.write() as db:
            existing = db.execute(
                "SELECT decision_digest,decision FROM cra_policy_decisions WHERE decision_id=?",
                (decision_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["decision_digest"]) != decision_digest:
                    raise CommandBlocked("CRA_POLICY_DECISION_CONFLICT")
                return str(existing["decision"])
            db.execute(
                """INSERT INTO cra_policy_decisions(
                       decision_id,projection_id,target_id,policy_revision,decision,action,
                       reason_code,blockers_json,expected_target_json,decided_at,expires_at,
                       decision_digest,candidate_reason_code,decision_reason_code
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    decision_id,
                    projection["projection_id"],
                    authorization.target_id,
                    authorization.policy_revision,
                    decision,
                    action,
                    resolved_decision_reason_code,
                    json.dumps(list(blockers), separators=(",", ":")),
                    expected_json,
                    authorization.authorized_at,
                    authorization.expires_at,
                    decision_digest,
                    resolved_candidate_reason_code,
                    resolved_decision_reason_code,
                ),
            )
        return decision
