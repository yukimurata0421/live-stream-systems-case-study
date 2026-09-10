from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

from cra_dell_recovery.canonical import SignedMessageCodec, canonical_json, payload_digest
from cra_dell_recovery.crash import CrashInjector, CrashPoint, no_crash
from cra_dell_recovery.effect_scope import effect_scope_id, logical_generation_scope_id
from cra_dell_recovery.errors import CommandBlocked
from cra_dell_recovery.models import RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.recovery_verification import payload_hash
from cra_dell_recovery.sqlite import SQLiteLedger, map_sqlite_failure
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now
from maintenance_audit import audit_maintenance_decision

from .schema_contract import UNRESOLVED_COMMAND_STATES, verify_central_schema_contract


class CentralStore(SQLiteLedger):
    def __init__(self, path: Path, migration: Path, *, allow_legacy_test_verification: bool = False) -> None:
        super().__init__(path, migration)
        self._allow_legacy_test_verification = allow_legacy_test_verification
        try:
            verify_central_schema_contract(self)
            self._backfill_effect_scope_ledger()
            self._backfill_logical_generation_scopes()
        except BaseException:
            self.close()
            raise

    def _backfill_logical_generation_scopes(self) -> None:
        columns = {str(row[1]) for row in self.connection.execute("PRAGMA table_info(effect_scope_ledger)")}
        if "logical_generation_scope_id" not in columns:
            return
        planned: list[tuple[str, str]] = []
        owners: dict[str, tuple[str, str]] = {}
        for row in self.read_all(
            """SELECT effect_scope_id,exact_target_json FROM effect_scope_ledger
               ORDER BY created_at,effect_scope_id"""
        ):
            target = TargetIdentity.from_dict(dict(json.loads(str(row["exact_target_json"]))))
            logical_id = logical_generation_scope_id("restart_ffmpeg", target)
            exact = json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True)
            owner = owners.get(logical_id)
            if owner is not None and owner != (str(row["effect_scope_id"]), exact):
                raise CommandBlocked("CENTRAL_LOGICAL_GENERATION_PID_INVARIANT_BROKEN")
            owners[logical_id] = (str(row["effect_scope_id"]), exact)
            planned.append((logical_id, str(row["effect_scope_id"])))
        with self.write() as db:
            for logical_id, scope_id in planned:
                db.execute(
                    """UPDATE effect_scope_ledger SET logical_generation_scope_id=?
                       WHERE effect_scope_id=? AND logical_generation_scope_id IS NULL""",
                    (logical_id, scope_id),
                )

    def _backfill_effect_scope_ledger(self) -> None:
        if self.read_one("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='effect_scope_ledger'") is None:
            return
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            commands = list(
                db.execute(
                    """SELECT command.command_id,command.target_id,command.status,command.updated_at,
                              authorization.expected_target_json
                       FROM commands AS command
                       JOIN recovery_authorizations AS authorization
                         ON authorization.authorization_id=command.authorization_id
                       ORDER BY command.created_at,command.command_id"""
                )
            )
            for command in commands:
                target = TargetIdentity.from_dict(dict(json.loads(str(command["expected_target_json"]))))
                scope_id = effect_scope_id("restart_ffmpeg", target)
                status = str(command["status"])
                message = db.execute(
                    """SELECT payload_json,payload_sha256 FROM agent_messages
                       WHERE command_id=? AND message_type='COMMAND_STATUS' AND signature_valid=1
                       ORDER BY received_at DESC,message_id DESC LIMIT 1""",
                    (command["command_id"],),
                ).fetchone()
                payload = {} if message is None else dict(json.loads(str(message["payload_json"])))
                attempts = int(payload.get("attempt_count") or 0)
                if attempts == 0 and status in {"EFFECT_OBSERVED", "OUTCOME_UNKNOWN"}:
                    attempts = 1
                if message is None and status == "EFFECT_FAILED":
                    # Legacy rows do not prove whether EFFECT_FAILED happened
                    # before or after the physical boundary. Under-counting can
                    # open a duplicate-effect budget, so migrate conservatively.
                    attempts = 1
                boundary_at = None
                if attempts:
                    boundary_at = str(payload.get("effect_observed_at") or payload.get("agent_observed_at") or command["updated_at"])
                if attempts:
                    state = (
                        "OUTCOME_UNKNOWN"
                        if message is None and status == "EFFECT_FAILED"
                        else status
                        if status in {"EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}
                        else "EFFECT_BOUNDARY_REACHED"
                    )
                elif status == "OUTCOME_UNKNOWN":
                    state = "OUTCOME_UNKNOWN"
                elif status in {"REJECTED", "EFFECT_FAILED", "EXPIRED", "SUPERSEDED"}:
                    state = "PRE_EFFECT_ABORTED"
                elif status in {"ACCEPTED", "EXECUTION_STARTED"}:
                    state = "PRECONDITION_CHECKED"
                else:
                    state = "RESERVED"
                existing = db.execute(
                    "SELECT origin_kind,origin_id FROM effect_scope_ledger WHERE effect_scope_id=?",
                    (scope_id,),
                ).fetchone()
                if existing is not None and (
                    str(existing["origin_kind"]) != "CENTRAL_COMMAND" or str(existing["origin_id"]) != str(command["command_id"])
                ):
                    raise CommandBlocked("CENTRAL_EFFECT_SCOPE_LEDGER_CONFLICT")
                db.execute(
                    """INSERT OR IGNORE INTO effect_scope_ledger(
                           effect_scope_id,target_id,action,exact_target_json,origin_kind,origin_id,state,
                           effect_boundary_reached_at,physical_attempt_count,source_evidence_digest,created_at,updated_at
                       ) VALUES(?,?,'restart_ffmpeg',?,'CENTRAL_COMMAND',?,?,?,?,?,?,?)""",
                    (
                        scope_id,
                        command["target_id"],
                        json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True),
                        command["command_id"],
                        state,
                        boundary_at,
                        attempts,
                        None if message is None else message["payload_sha256"],
                        command["updated_at"],
                        stamp,
                    ),
                )
            for journal in db.execute(
                """SELECT local_action_id,target_id,effect_scope_id,record_state,record_digest,
                          payload_json,recorded_at
                   FROM dell_journal_records ORDER BY agent_installation_id,journal_sequence"""
            ):
                payload = dict(json.loads(str(journal["payload_json"])))
                target = TargetIdentity.from_dict(dict(payload["before_target"]))
                scope_id = effect_scope_id("restart_ffmpeg", target)
                if scope_id != str(journal["effect_scope_id"]):
                    raise CommandBlocked("CENTRAL_IMPORTED_EFFECT_SCOPE_BINDING_MISMATCH")
                attempts = int(payload.get("physical_attempt_count") or 0)
                record_state = str(journal["record_state"])
                if attempts:
                    state = (
                        record_state
                        if record_state in {"EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}
                        else "EFFECT_BOUNDARY_REACHED"
                    )
                    boundary_at = str(journal["recorded_at"])
                elif record_state == "EFFECT_FAILED":
                    state = "PRE_EFFECT_ABORTED"
                    boundary_at = None
                else:
                    state = "PRECONDITION_CHECKED"
                    boundary_at = None
                existing = db.execute(
                    "SELECT origin_kind,origin_id,physical_attempt_count FROM effect_scope_ledger WHERE effect_scope_id=?",
                    (scope_id,),
                ).fetchone()
                if existing is not None and (
                    str(existing["origin_kind"]) != "IMPORTED_LOCAL_ACTION" or str(existing["origin_id"]) != str(journal["local_action_id"])
                ):
                    raise CommandBlocked("CENTRAL_EFFECT_SCOPE_LEDGER_CONFLICT")
                if existing is None:
                    db.execute(
                        """INSERT INTO effect_scope_ledger(
                               effect_scope_id,target_id,action,exact_target_json,origin_kind,origin_id,state,
                               effect_boundary_reached_at,physical_attempt_count,source_evidence_digest,created_at,updated_at
                           ) VALUES(?,?,'restart_ffmpeg',?,'IMPORTED_LOCAL_ACTION',?,?,?,?,?,?,?)""",
                        (
                            scope_id,
                            journal["target_id"],
                            json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True),
                            journal["local_action_id"],
                            state,
                            boundary_at,
                            attempts,
                            journal["record_digest"],
                            journal["recorded_at"],
                            stamp,
                        ),
                    )
                else:
                    if attempts < int(existing["physical_attempt_count"]):
                        raise CommandBlocked("CENTRAL_IMPORTED_EFFECT_ATTEMPT_COUNT_REGRESSION")
                    db.execute(
                        """UPDATE effect_scope_ledger SET state=?,
                               effect_boundary_reached_at=coalesce(effect_boundary_reached_at,?),
                               physical_attempt_count=max(physical_attempt_count,?),source_evidence_digest=?,updated_at=?
                           WHERE effect_scope_id=?""",
                        (state, boundary_at, attempts, journal["record_digest"], stamp, scope_id),
                    )

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
        with self.write() as db:
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
                    "CENTRAL_ACTIVE",
                    authority_epoch,
                    agent_installation_id,
                    "BOOTSTRAPPED_TEST_SHADOW",
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
                    "ACTIVE",
                    stamp,
                ),
            )

    def add_authorization(self, authorization: RecoveryAuthorizationInput) -> None:
        digest = payload_digest(
            {
                "authorization_id": authorization.authorization_id,
                "incident_id": authorization.incident_id,
                "expected_target": authorization.expected_target.to_dict(),
                "blockers": list(authorization.blockers),
            }
        )
        with self.write() as db:
            db.execute(
                """INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,0)""",
                (
                    authorization.incident_id,
                    authorization.target_id,
                    authorization.source_episode_id,
                    "stream_input_transport",
                    "AUTHORIZED",
                    authorization.observation_revision,
                    authorization.authorized_at,
                    authorization.authorized_at,
                    None,
                    authorization.authorized_at,
                ),
            )
            db.execute(
                """INSERT INTO incident_transitions VALUES (?,?,?,?,?,?,?)""",
                (
                    f"transition-{authorization.incident_id}",
                    authorization.incident_id,
                    None,
                    "AUTHORIZED",
                    "FIXTURE_ADAPTER",
                    authorization.source_episode_id,
                    authorization.authorized_at,
                ),
            )
            db.execute(
                """INSERT INTO recovery_authorizations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    authorization.authorization_id,
                    authorization.incident_id,
                    authorization.target_id,
                    authorization.action,
                    authorization.policy_revision,
                    authorization.observation_revision,
                    json.dumps(authorization.expected_target.to_dict(), separators=(",", ":")),
                    json.dumps(list(authorization.blockers), separators=(",", ":")),
                    authorization.authorized_at,
                    authorization.expires_at,
                    digest,
                    "AUTHORIZED",
                ),
            )

    def ingest_monitoring_projection(self, projection: dict[str, Any]) -> str:
        """Persist a verified observation projection without importing control semantics."""

        stamp = isoformat_utc(utc_now())
        with self.write() as db:
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
        unresolved = self.read_one(
            f"SELECT count(*) FROM commands WHERE target_id=? AND status IN ({','.join('?' for _ in unresolved_states)})",
            (target_id, *unresolved_states),
        )
        if unresolved is None or int(unresolved[0]):
            blockers.append("CENTRAL_UNRESOLVED_COMMAND")
        completed = [
            parse_utc(str(row[0]))
            for row in self.read_all(
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
        row = self.read_one(
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
        resolved_candidate_reason_code = candidate_reason_code or authorization.reason_code
        resolved_decision_reason_code = (
            decision_reason_code
            or reason_code
            or (
                authorization.reason_code
                if decision == "AUTHORIZED"
                else (
                    "INCIDENT_NOT_CONFIRMED"
                    if decision == "NO_ACTION"
                    else next((item for item in blockers if item != "CRA_OPERATING_MODE_NO_ACTION"), "CRA_OPERATING_MODE_NO_ACTION")
                )
            )
        )
        if (decision == "AUTHORIZED") != (action is not None):
            raise ValueError("CRA_POLICY_DECISION_ACTION_MISMATCH")
        if not resolved_candidate_reason_code.strip() or not resolved_decision_reason_code.strip():
            raise ValueError("CRA_POLICY_DECISION_REASON_INVALID")
        expected_json = json.dumps(authorization.expected_target.to_dict(), separators=(",", ":"), sort_keys=True)
        decision_value = {
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
        decision_digest = payload_digest(decision_value)
        with self.write() as db:
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
            if decision != "AUTHORIZED":
                return decision
            authorization_digest = payload_digest(
                {
                    "authorization_id": authorization.authorization_id,
                    "incident_id": authorization.incident_id,
                    "target_id": authorization.target_id,
                    "action": authorization.action,
                    "candidate_reason_code": resolved_candidate_reason_code,
                    "decision_reason_code": resolved_decision_reason_code,
                    "policy_revision": authorization.policy_revision,
                    "observation_revision": authorization.observation_revision,
                    "expected_target": authorization.expected_target.to_dict(),
                    "blockers": list(blockers),
                    "authorized_at": authorization.authorized_at,
                    "expires_at": authorization.expires_at,
                }
            )
            incident = db.execute("SELECT * FROM incidents WHERE incident_id=?", (authorization.incident_id,)).fetchone()
            if incident is None:
                db.execute(
                    "INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,0)",
                    (
                        authorization.incident_id,
                        authorization.target_id,
                        authorization.source_episode_id,
                        projection["incident"]["domain"],
                        "AUTHORIZED",
                        authorization.observation_revision,
                        authorization.authorized_at,
                        authorization.authorized_at,
                        None,
                        authorization.authorized_at,
                    ),
                )
                from_state = None
            else:
                if str(incident["target_id"]) != authorization.target_id:
                    raise CommandBlocked("CRA_INCIDENT_TARGET_CONFLICT")
                from_state = str(incident["state"])
                db.execute(
                    """UPDATE incidents SET state='AUTHORIZED',observation_revision=?,confirmed_at=coalesce(confirmed_at,?),
                           last_transition_at=?,version=version+1 WHERE incident_id=?""",
                    (
                        authorization.observation_revision,
                        authorization.authorized_at,
                        authorization.authorized_at,
                        authorization.incident_id,
                    ),
                )
            db.execute(
                "INSERT INTO incident_transitions VALUES (?,?,?,?,?,?,?)",
                (
                    f"transition-{uuid.uuid4()}",
                    authorization.incident_id,
                    from_state,
                    "AUTHORIZED",
                    "CRA_POLICY_AUTHORIZED",
                    projection["projection_id"],
                    authorization.authorized_at,
                ),
            )
            db.execute(
                "INSERT INTO recovery_authorizations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    authorization.authorization_id,
                    authorization.incident_id,
                    authorization.target_id,
                    authorization.action,
                    authorization.policy_revision,
                    authorization.observation_revision,
                    expected_json,
                    json.dumps(list(blockers), separators=(",", ":")),
                    authorization.authorized_at,
                    authorization.expires_at,
                    authorization_digest,
                    "AUTHORIZED",
                ),
            )
        return decision

    def record_verifier_decision(self, value: dict[str, Any]) -> str:
        """Persist CRA's final verdict; this role has no command-creation input."""

        digest = payload_digest(value)
        stamp = str(value["decided_at"])
        command_id = value.get("command_id")
        local_action_id = value.get("local_action_id")
        execution = dict(value["execution_evidence"])
        if execution.get("effect_scope_id") != value.get("effect_scope_id"):
            raise CommandBlocked("CRA_VERIFIER_EXECUTION_EFFECT_SCOPE_BINDING_MISMATCH")
        with self.write() as db:
            existing = db.execute(
                "SELECT decision_digest,verdict FROM cra_verifier_decisions WHERE verifier_decision_id=?",
                (value["verifier_decision_id"],),
            ).fetchone()
            if existing is not None:
                if str(existing["decision_digest"]) != digest:
                    raise CommandBlocked("CRA_VERIFIER_DECISION_CONFLICT")
                return str(existing["verdict"])
            finalized = db.execute(
                "SELECT verifier_decision_id FROM cra_verifier_decisions WHERE effect_scope_id=?",
                (value["effect_scope_id"],),
            ).fetchone()
            if finalized is not None:
                raise CommandBlocked("CRA_VERIFIER_EFFECT_SCOPE_ALREADY_FINALIZED")
            if command_id is not None:
                command = db.execute(
                    """SELECT command.*,authorization.expected_target_json AS authorization_expected_target_json
                       FROM commands AS command
                       JOIN recovery_authorizations AS authorization
                         ON authorization.authorization_id=command.authorization_id
                       WHERE command.command_id=?""",
                    (command_id,),
                ).fetchone()
                if command is None:
                    raise CommandBlocked("CRA_VERIFIER_COMMAND_NOT_FOUND")
                if execution.get("command_id") != command_id or execution.get("local_action_id") is not None:
                    raise CommandBlocked("CRA_VERIFIER_EXECUTION_COMMAND_BINDING_MISMATCH")
                expected_target = TargetIdentity.from_dict(dict(json.loads(str(command["authorization_expected_target_json"]))))
                if str(value["effect_scope_id"]) != effect_scope_id("restart_ffmpeg", expected_target):
                    raise CommandBlocked("CRA_VERIFIER_COMMAND_EFFECT_SCOPE_MISMATCH")
                execution_state = str(execution.get("state") or "OUTCOME_UNKNOWN")
                observed_states = {str(command["status"])} | {
                    str(row[0])
                    for row in db.execute(
                        "SELECT to_state FROM command_transitions WHERE command_id=?",
                        (command_id,),
                    )
                }
                if execution_state not in observed_states:
                    raise CommandBlocked("CRA_VERIFIER_EXECUTION_STATE_NOT_CENTRAL_TRUTH")
                agent_status_row = db.execute(
                    """SELECT payload_json FROM agent_messages
                       WHERE command_id=? AND message_type='COMMAND_STATUS' AND signature_valid=1
                       ORDER BY received_at DESC,message_id DESC LIMIT 1""",
                    (command_id,),
                ).fetchone()
                if agent_status_row is None:
                    raise CommandBlocked("CRA_VERIFIER_SIGNED_AGENT_STATUS_MISSING")
                agent_status = dict(json.loads(str(agent_status_row["payload_json"])))
                if str(agent_status.get("command_state")) != execution_state:
                    raise CommandBlocked("CRA_VERIFIER_AGENT_STATUS_STATE_MISMATCH")
                if int(agent_status.get("attempt_count") or 0) != int(execution.get("physical_attempt_count") or 0):
                    raise CommandBlocked("CRA_VERIFIER_AGENT_STATUS_ATTEMPT_MISMATCH")
                if agent_status.get("before_target") != execution.get("before_target"):
                    raise CommandBlocked("CRA_VERIFIER_AGENT_STATUS_BEFORE_TARGET_MISMATCH")
                if agent_status.get("after_target") != execution.get("after_target"):
                    raise CommandBlocked("CRA_VERIFIER_AGENT_STATUS_AFTER_TARGET_MISMATCH")
                verification_target_id = str(command["target_id"])
                verification_incident_id: str | None = str(command["incident_id"])
            else:
                if execution.get("local_action_id") != local_action_id or execution.get("command_id") is not None:
                    raise CommandBlocked("CRA_VERIFIER_EXECUTION_LOCAL_BINDING_MISMATCH")
                imported = list(
                    db.execute(
                        """SELECT target_id,effect_scope_id,record_state,payload_json FROM dell_journal_records
                           WHERE local_action_id=? ORDER BY journal_sequence""",
                        (local_action_id,),
                    )
                )
                if not imported:
                    raise CommandBlocked("CRA_VERIFIER_LOCAL_ACTION_NOT_IMPORTED")
                if any(str(row["effect_scope_id"]) != str(value["effect_scope_id"]) for row in imported):
                    raise CommandBlocked("CRA_VERIFIER_LOCAL_EFFECT_SCOPE_MISMATCH")
                if str(imported[-1]["record_state"]) != str(execution.get("state") or "OUTCOME_UNKNOWN"):
                    raise CommandBlocked("CRA_VERIFIER_LOCAL_EXECUTION_STATE_MISMATCH")
                local_terminal = dict(json.loads(str(imported[-1]["payload_json"])))
                if int(local_terminal.get("physical_attempt_count") or 0) != int(execution.get("physical_attempt_count") or 0):
                    raise CommandBlocked("CRA_VERIFIER_LOCAL_ATTEMPT_COUNT_MISMATCH")
                if local_terminal.get("before_target") != execution.get("before_target"):
                    raise CommandBlocked("CRA_VERIFIER_LOCAL_BEFORE_TARGET_MISMATCH")
                if local_terminal.get("after_target") != execution.get("after_target"):
                    raise CommandBlocked("CRA_VERIFIER_LOCAL_AFTER_TARGET_MISMATCH")
                command = None
                verification_target_id = str(imported[-1]["target_id"])
                verification_incident_id = None
            projection_rows = {
                str(row["projection_id"]): row
                for row in db.execute(
                    """SELECT projection_id,target_id,source_instance_id,source_release_id,
                              observation_sequence,incident_id,payload_json
                       FROM monitoring_evidence_projections WHERE projection_id IN (?,?)""",
                    (value["pre_projection_id"], value["post_projection_id"]),
                )
            }
            pre_projection = projection_rows.get(str(value["pre_projection_id"]))
            post_projection = projection_rows.get(str(value["post_projection_id"]))
            if pre_projection is None or post_projection is None or pre_projection is post_projection:
                raise CommandBlocked("CRA_VERIFIER_PROJECTION_BINDING_MISSING")
            if str(pre_projection["target_id"]) != verification_target_id or str(post_projection["target_id"]) != verification_target_id:
                raise CommandBlocked("CRA_VERIFIER_PROJECTION_TARGET_MISMATCH")
            if (
                str(pre_projection["source_instance_id"]) != str(post_projection["source_instance_id"])
                or str(pre_projection["source_release_id"]) != str(post_projection["source_release_id"])
                or int(post_projection["observation_sequence"]) <= int(pre_projection["observation_sequence"])
            ):
                raise CommandBlocked("CRA_VERIFIER_PROJECTION_LINEAGE_MISMATCH")
            if str(pre_projection["incident_id"]) != str(post_projection["incident_id"]) or (
                verification_incident_id is not None and str(pre_projection["incident_id"]) != verification_incident_id
            ):
                raise CommandBlocked("CRA_VERIFIER_PROJECTION_INCIDENT_MISMATCH")
            pre_payload = dict(json.loads(str(pre_projection["payload_json"])))
            post_payload = dict(json.loads(str(post_projection["payload_json"])))
            if pre_payload.get("observed_target") != execution.get("before_target"):
                raise CommandBlocked("CRA_VERIFIER_PRE_PROJECTION_EXECUTION_TARGET_MISMATCH")
            if post_payload.get("observed_target") != execution.get("after_target"):
                raise CommandBlocked("CRA_VERIFIER_POST_PROJECTION_EXECUTION_TARGET_MISMATCH")
            db.execute(
                "INSERT INTO cra_verifier_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    value["verifier_decision_id"],
                    command_id,
                    local_action_id,
                    value["effect_scope_id"],
                    value["pre_projection_id"],
                    value["post_projection_id"],
                    value["verdict"],
                    json.dumps(value["reason_codes"], separators=(",", ":"), sort_keys=True),
                    json.dumps(value["execution_evidence"], separators=(",", ":"), sort_keys=True),
                    stamp,
                    digest,
                ),
            )
            if command is not None:
                verdict_state = {
                    "RECOVERED": "VERIFIED",
                    "FAILED": "VERIFICATION_FAILED",
                    "UNKNOWN": "VERIFICATION_UNKNOWN",
                }[str(value["verdict"])]
                previous = str(command["status"])
                db.execute(
                    "UPDATE commands SET status=?,updated_at=? WHERE command_id=?",
                    (verdict_state, stamp, command_id),
                )
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{uuid.uuid4()}",
                        command_id,
                        "CRA",
                        previous,
                        verdict_state,
                        "CRA_FINAL_VERIFICATION",
                        value["verifier_decision_id"],
                        stamp,
                    ),
                )
                incident_state = {
                    "RECOVERED": "RECOVERED",
                    "FAILED": "ESCALATED",
                    "UNKNOWN": "VERIFYING",
                }[str(value["verdict"])]
                incident = db.execute(
                    "SELECT state FROM incidents WHERE incident_id=?",
                    (command["incident_id"],),
                ).fetchone()
                if incident is not None:
                    db.execute(
                        """UPDATE incidents SET state=?,observation_revision=?,closed_at=?,
                           last_transition_at=?,version=version+1 WHERE incident_id=?""",
                        (
                            incident_state,
                            post_payload["observation_revision"],
                            stamp if incident_state == "RECOVERED" else None,
                            stamp,
                            command["incident_id"],
                        ),
                    )
                    db.execute(
                        "INSERT INTO incident_transitions VALUES (?,?,?,?,?,?,?)",
                        (
                            f"transition-{uuid.uuid4()}",
                            command["incident_id"],
                            incident["state"],
                            incident_state,
                            "CRA_FINAL_VERIFICATION",
                            value["verifier_decision_id"],
                            stamp,
                        ),
                    )
            else:
                incident_value = dict(pre_payload["incident"])
                incident_id = str(incident_value["incident_id"])
                incident_state = {
                    "RECOVERED": "RECOVERED",
                    "FAILED": "ESCALATED",
                    "UNKNOWN": "VERIFYING",
                }[str(value["verdict"])]
                incident = db.execute(
                    "SELECT target_id,state,source_episode_id FROM incidents WHERE incident_id=?",
                    (incident_id,),
                ).fetchone()
                if incident is None:
                    episode_conflict = db.execute(
                        "SELECT incident_id FROM incidents WHERE source_episode_id=?",
                        (incident_value["source_episode_id"],),
                    ).fetchone()
                    if episode_conflict is not None:
                        raise CommandBlocked("CRA_VERIFIER_LOCAL_INCIDENT_EPISODE_CONFLICT")
                    db.execute(
                        "INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,0)",
                        (
                            incident_id,
                            verification_target_id,
                            incident_value["source_episode_id"],
                            incident_value["domain"],
                            incident_state,
                            post_payload["observation_revision"],
                            pre_payload["observed_at"],
                            pre_payload["observed_at"],
                            stamp if incident_state == "RECOVERED" else None,
                            stamp,
                        ),
                    )
                    previous_incident_state = None
                else:
                    if str(incident["target_id"]) != verification_target_id or str(incident["source_episode_id"]) != str(
                        incident_value["source_episode_id"]
                    ):
                        raise CommandBlocked("CRA_VERIFIER_LOCAL_INCIDENT_BINDING_MISMATCH")
                    previous_incident_state = str(incident["state"])
                    db.execute(
                        """UPDATE incidents SET state=?,observation_revision=?,closed_at=?,
                           last_transition_at=?,version=version+1 WHERE incident_id=?""",
                        (
                            incident_state,
                            post_payload["observation_revision"],
                            stamp if incident_state == "RECOVERED" else None,
                            stamp,
                            incident_id,
                        ),
                    )
                db.execute(
                    "INSERT INTO incident_transitions VALUES (?,?,?,?,?,?,?)",
                    (
                        f"transition-{uuid.uuid4()}",
                        incident_id,
                        previous_incident_state,
                        incident_state,
                        "CRA_LOCAL_ACTION_FINAL_VERIFICATION",
                        value["verifier_decision_id"],
                        stamp,
                    ),
                )
        return str(value["verdict"])

    def record_effect_reconciliation(
        self,
        *,
        effect_scope_id_value: str,
        reconciled_state: str,
        evidence: dict[str, Any],
        reconciliation_id: str | None = None,
        recorded_at: str | None = None,
    ) -> str:
        """Append a later interpretation without rewriting the raw effect state."""

        if reconciled_state not in {"EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}:
            raise ValueError("CENTRAL_EFFECT_RECONCILIATION_STATE_INVALID")
        stamp = recorded_at or isoformat_utc(utc_now())
        identifier = reconciliation_id or f"effect-reconciliation-{uuid.uuid4()}"
        digest = payload_digest(
            {
                "reconciliation_id": identifier,
                "effect_scope_id": effect_scope_id_value,
                "reconciled_state": reconciled_state,
                "evidence": evidence,
                "recorded_at": stamp,
            }
        )
        with self.write() as db:
            scope = db.execute(
                "SELECT state FROM effect_scope_ledger WHERE effect_scope_id=?",
                (effect_scope_id_value,),
            ).fetchone()
            if scope is None:
                raise CommandBlocked("CENTRAL_EFFECT_RECONCILIATION_SCOPE_MISSING")
            existing = db.execute(
                "SELECT evidence_digest FROM effect_reconciliations WHERE reconciliation_id=?",
                (identifier,),
            ).fetchone()
            if existing is not None:
                if str(existing["evidence_digest"]) != digest:
                    raise CommandBlocked("CENTRAL_EFFECT_RECONCILIATION_CONFLICT")
                return "DUPLICATE"
            db.execute(
                "INSERT INTO effect_reconciliations VALUES (?,?,?,?,?,?,?)",
                (
                    identifier,
                    effect_scope_id_value,
                    scope["state"],
                    reconciled_state,
                    json.dumps(evidence, separators=(",", ":"), sort_keys=True),
                    digest,
                    stamp,
                ),
            )
        return "RECORDED"

    def dell_journal_watermark(self, agent_installation_id: str, target_id: str) -> tuple[int, str]:
        row = self.read_one(
            """SELECT highest_sequence,highest_record_digest FROM dell_journal_watermarks
               WHERE agent_installation_id=? AND target_id=?""",
            (agent_installation_id, target_id),
        )
        return (0, "0" * 64) if row is None else (int(row[0]), str(row[1]))

    def import_dell_journal_page(self, page: dict[str, Any]) -> tuple[int, str]:
        agent_installation_id = str(page["agent_installation_id"])
        target_id = str(page["target_id"])
        current_sequence, current_digest = self.dell_journal_watermark(agent_installation_id, target_id)
        if int(page["after_sequence"]) != current_sequence:
            raise CommandBlocked("DELL_JOURNAL_PAGE_CURSOR_MISMATCH")
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            for raw in page["records"]:
                record = dict(raw)
                sequence = int(record.pop("journal_sequence"))
                record_digest = str(record.pop("record_digest"))
                if sequence != current_sequence + 1:
                    raise CommandBlocked("DELL_JOURNAL_SEQUENCE_GAP")
                if str(record["previous_digest"]) != current_digest:
                    raise CommandBlocked("DELL_JOURNAL_HASH_CHAIN_GAP")
                if payload_digest(record) != record_digest:
                    raise CommandBlocked("DELL_JOURNAL_RECORD_DIGEST_MISMATCH")
                if str(record["target_id"]) != target_id:
                    raise CommandBlocked("DELL_JOURNAL_TARGET_MISMATCH")
                before = TargetIdentity.from_dict(dict(record["before_target"]))
                if str(record["effect_scope_id"]) != effect_scope_id("restart_ffmpeg", before):
                    raise CommandBlocked("DELL_JOURNAL_EFFECT_SCOPE_MISMATCH")
                logical_scope_id = logical_generation_scope_id("restart_ffmpeg", before)
                db.execute(
                    """INSERT INTO dell_journal_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        agent_installation_id,
                        sequence,
                        record["local_action_id"],
                        record["local_session_id"],
                        target_id,
                        record["effect_scope_id"],
                        record["record_state"],
                        record["previous_digest"],
                        record_digest,
                        json.dumps(record, separators=(",", ":"), sort_keys=True),
                        record["recorded_at"],
                        stamp,
                    ),
                )
                attempts = int(record.get("physical_attempt_count") or 0)
                record_state = str(record["record_state"])
                if attempts:
                    ledger_state = (
                        record_state
                        if record_state in {"EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}
                        else "EFFECT_BOUNDARY_REACHED"
                    )
                    boundary_at = str(record["recorded_at"])
                elif record_state == "EFFECT_FAILED":
                    ledger_state = "PRE_EFFECT_ABORTED"
                    boundary_at = None
                else:
                    ledger_state = "PRECONDITION_CHECKED"
                    boundary_at = None
                ledger = db.execute(
                    """SELECT effect_scope_id,origin_kind,origin_id,physical_attempt_count
                       FROM effect_scope_ledger
                       WHERE effect_scope_id=? OR logical_generation_scope_id=?""",
                    (record["effect_scope_id"], logical_scope_id),
                ).fetchone()
                if ledger is not None and str(ledger["effect_scope_id"]) != str(record["effect_scope_id"]):
                    raise CommandBlocked("CENTRAL_LOGICAL_GENERATION_PID_INVARIANT_BROKEN")
                if ledger is not None and (
                    str(ledger["origin_kind"]) != "IMPORTED_LOCAL_ACTION" or str(ledger["origin_id"]) != str(record["local_action_id"])
                ):
                    raise CommandBlocked("CENTRAL_EFFECT_SCOPE_LEDGER_CONFLICT")
                if ledger is None:
                    db.execute(
                        """INSERT INTO effect_scope_ledger(
                               effect_scope_id,logical_generation_scope_id,target_id,action,exact_target_json,
                               origin_kind,origin_id,state,
                               effect_boundary_reached_at,physical_attempt_count,source_evidence_digest,created_at,updated_at
                           ) VALUES(?,?,?,'restart_ffmpeg',?,'IMPORTED_LOCAL_ACTION',?,?,?,?,?,?,?)""",
                        (
                            record["effect_scope_id"],
                            logical_scope_id,
                            target_id,
                            json.dumps(before.to_dict(), separators=(",", ":"), sort_keys=True),
                            record["local_action_id"],
                            ledger_state,
                            boundary_at,
                            attempts,
                            record_digest,
                            record["recorded_at"],
                            stamp,
                        ),
                    )
                else:
                    if attempts < int(ledger["physical_attempt_count"]):
                        raise CommandBlocked("CENTRAL_IMPORTED_EFFECT_ATTEMPT_COUNT_REGRESSION")
                    db.execute(
                        """UPDATE effect_scope_ledger SET state=?,
                               effect_boundary_reached_at=coalesce(effect_boundary_reached_at,?),
                               physical_attempt_count=max(physical_attempt_count,?),source_evidence_digest=?,updated_at=?
                           WHERE effect_scope_id=?""",
                        (
                            ledger_state,
                            boundary_at,
                            attempts,
                            record_digest,
                            stamp,
                            record["effect_scope_id"],
                        ),
                    )
                current_sequence = sequence
                current_digest = record_digest
            db.execute(
                """INSERT INTO dell_journal_watermarks VALUES (?,?,?,?,?,?)
                   ON CONFLICT(agent_installation_id,target_id) DO UPDATE SET
                       highest_sequence=excluded.highest_sequence,
                       highest_record_digest=excluded.highest_record_digest,
                       updated_at=excluded.updated_at""",
                (agent_installation_id, target_id, current_sequence, current_digest, None, stamp),
            )
        return current_sequence, current_digest

    def command_count(self) -> int:
        return int(self.connection.execute("SELECT count(*) FROM commands").fetchone()[0])

    def outbox_count(self) -> int:
        return int(self.connection.execute("SELECT count(*) FROM outbox_messages").fetchone()[0])

    def pending_envelopes(self) -> list[dict[str, Any]]:
        if not self.delivery_allowed():
            return []
        rows = self.connection.execute(
            """SELECT canonical_payload,payload_sha256,signature FROM outbox_messages
               WHERE state IN ('PENDING','RETRY','IN_FLIGHT') ORDER BY created_at"""
        ).fetchall()
        envelopes: list[dict[str, Any]] = []
        for row in rows:
            envelope = dict(json.loads(str(row["canonical_payload"])))
            envelope["payload_sha256"] = row["payload_sha256"]
            envelope["signature"] = row["signature"]
            envelopes.append(envelope)
        return envelopes

    def delivery_allowed(self) -> bool:
        identity = self.connection.execute("SELECT restore_state FROM control_plane_identity WHERE singleton_id=1").fetchone()
        if identity is None or identity[0] != "CLEAN":
            return False
        bad = self.connection.execute(
            """SELECT count(*) FROM outbox_messages o LEFT JOIN commands c ON c.command_id=o.command_id
               WHERE c.command_id IS NULL OR o.payload_sha256 != c.payload_sha256"""
        ).fetchone()[0]
        return int(bad) == 0 and self.writable_probe()

    def create_command(
        self,
        authorization_id: str,
        codec: SignedMessageCodec,
        *,
        command_id: str | None = None,
        crash: CrashInjector = no_crash,
    ) -> dict[str, Any]:
        stamp = utc_now()
        command_id = command_id or f"cmd-{uuid.uuid4()}"
        try:
            with self.write() as db:
                identity = db.execute(
                    "SELECT controller_instance_id,restore_state FROM control_plane_identity WHERE singleton_id=1"
                ).fetchone()
                auth = db.execute(
                    "SELECT * FROM recovery_authorizations WHERE authorization_id=?",
                    (authorization_id,),
                ).fetchone()
                if identity is None or auth is None:
                    raise CommandBlocked("MISSING_AUTHORIZATION_CONTEXT")
                if identity["restore_state"] != "CLEAN":
                    raise CommandBlocked("RESTORED_NEEDS_RECONCILIATION")
                target = db.execute("SELECT * FROM targets WHERE target_id=?", (auth["target_id"],)).fetchone()
                incident = db.execute("SELECT * FROM incidents WHERE incident_id=?", (auth["incident_id"],)).fetchone()
                session = db.execute(
                    "SELECT * FROM authority_sessions WHERE target_id=? AND state='ACTIVE'",
                    (auth["target_id"],),
                ).fetchone()
                if target is None or incident is None or session is None:
                    raise CommandBlocked("INCONSISTENT_CENTRAL_PROJECTION")
                blockers = json.loads(str(auth["blockers_json"]))
                if (
                    auth["status"] != "AUTHORIZED"
                    or incident["state"] != "AUTHORIZED"
                    or target["authority_state"] != "CENTRAL_ACTIVE"
                    or session["authority_epoch"] != target["current_authority_epoch"]
                    or blockers
                ):
                    raise CommandBlocked("AUTHORIZATION_NOT_EXECUTABLE")
                if parse_utc(str(auth["expires_at"])) <= stamp:
                    raise CommandBlocked("AUTHORIZATION_EXPIRED")
                expected = TargetIdentity.from_dict(json.loads(str(auth["expected_target_json"])))
                scope_id = effect_scope_id("restart_ffmpeg", expected)
                logical_scope_id = logical_generation_scope_id("restart_ffmpeg", expected)
                if (
                    db.execute(
                        """SELECT effect_scope_id FROM effect_scope_ledger
                           WHERE effect_scope_id=? OR logical_generation_scope_id=?""",
                        (scope_id, logical_scope_id),
                    ).fetchone()
                    is not None
                ):
                    raise CommandBlocked("CENTRAL_LOGICAL_GENERATION_ALREADY_FENCED")
                seq = int(target["next_command_seq"])
                expires_at = min(parse_utc(str(auth["expires_at"])), stamp + timedelta(seconds=30))
                unsigned: dict[str, Any] = {
                    "protocol": "cra_dell_recovery.command.v1",
                    "message_type": "execute_command",
                    "message_id": f"message-{command_id}",
                    "sender_id": "cra-authority",
                    "receiver_id": str(target["agent_id"]),
                    "controller_instance_id": str(identity["controller_instance_id"]),
                    "authority_session_id": str(session["authority_session_id"]),
                    "authority_epoch": int(session["authority_epoch"]),
                    "command_seq": seq,
                    "command_id": command_id,
                    "idempotency_key": f"{session['authority_epoch']}:{seq}:{authorization_id}",
                    "incident_id": str(auth["incident_id"]),
                    "authorization_id": authorization_id,
                    "policy_revision": str(auth["policy_revision"]),
                    "action": "restart_ffmpeg",
                    "reason_code": "confirmed_tcp_stall",
                    "target": {
                        "target_id": str(target["target_id"]),
                        "host_id": str(target["host_id"]),
                        "namespace": expected.namespace,
                        "workload": "deployment/stream-v3",
                        "container_name": expected.container_name,
                    },
                    "expected_target": expected.to_dict(),
                    "decision": {
                        "decision_cycle_id": str(auth["observation_revision"]),
                        "observed_at": str(auth["authorized_at"]),
                        "confirmed_at": str(auth["authorized_at"]),
                        "evidence_fresh_until": str(auth["expires_at"]),
                    },
                    "verification_contract": {
                        "required_new_ffmpeg_generation": True,
                        "require_same_host_boot_id": True,
                        "require_same_pod_uid": True,
                        "health_owner": "monitoring-v4",
                    },
                    "issued_at": isoformat_utc(stamp),
                    "expires_at": isoformat_utc(expires_at),
                    "key_id": codec.signer.key_id,
                }
                envelope = codec.encode(unsigned)
                digest = str(envelope["payload_sha256"])
                audit_maintenance_decision(
                    path_id="MP-07",
                    phase="ADMISSION",
                    operation="issue_recovery_command",
                    path_role="AUTHORITY_PRODUCER",
                    process_service="cra-authority-shadow.service",
                    resource_identity=f"target/{target['target_id']}",
                    correlation_id=command_id,
                    target_identity=expected.to_dict(),
                    authorization_id=authorization_id,
                    in_flight_evidence={"status": "CONFIRMED", "count": 0, "source": "central_command_transaction"},
                    generation_evidence={
                        "status": "CONFIRMED",
                        "authority_epoch": int(session["authority_epoch"]),
                        "command_seq": seq,
                        "source": "Central SQLite targets.next_command_seq",
                    },
                )
                db.execute(
                    """INSERT INTO commands VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        command_id,
                        target["target_id"],
                        auth["incident_id"],
                        authorization_id,
                        session["authority_session_id"],
                        session["authority_epoch"],
                        seq,
                        envelope["idempotency_key"],
                        "restart_ffmpeg",
                        "confirmed_tcp_stall",
                        expected.host_boot_id,
                        expected.pod_uid,
                        expected.container_name,
                        expected.container_id,
                        expected.ffmpeg_generation,
                        expected.ffmpeg_pid,
                        "OUTBOX_PENDING",
                        envelope["issued_at"],
                        envelope["expires_at"],
                        digest,
                        envelope["issued_at"],
                        envelope["issued_at"],
                    ),
                )
                db.execute(
                    """INSERT INTO outbox_messages VALUES (?,?,?,?,?,?,?,?,?,0,NULL,NULL,?,?)""",
                    (
                        f"outbox-{command_id}",
                        command_id,
                        "execute_command",
                        canonical_json(envelope).decode("utf-8"),
                        digest,
                        envelope["key_id"],
                        envelope["signature"],
                        "PENDING",
                        envelope["issued_at"],
                        envelope["issued_at"],
                        envelope["issued_at"],
                    ),
                )
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{command_id}-committed",
                        command_id,
                        "CRA",
                        "DRAFT",
                        "COMMITTED",
                        "COMMAND_INTENT_COMMITTED",
                        None,
                        envelope["issued_at"],
                    ),
                )
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{command_id}-outbox",
                        command_id,
                        "CRA",
                        "COMMITTED",
                        "OUTBOX_PENDING",
                        "TRANSACTIONAL_OUTBOX_COMMIT",
                        None,
                        envelope["issued_at"],
                    ),
                )
                db.execute(
                    "UPDATE targets SET next_command_seq=next_command_seq+1, version=version+1 WHERE target_id=?",
                    (target["target_id"],),
                )
                db.execute(
                    "UPDATE recovery_authorizations SET status='CONSUMED' WHERE authorization_id=?",
                    (authorization_id,),
                )
                db.execute(
                    """INSERT INTO effect_scope_ledger(
                           effect_scope_id,logical_generation_scope_id,target_id,action,exact_target_json,
                           origin_kind,origin_id,state,
                           effect_boundary_reached_at,physical_attempt_count,source_evidence_digest,created_at,updated_at
                       ) VALUES(?,?,?,'restart_ffmpeg',?,'CENTRAL_COMMAND',?,'RESERVED',NULL,0,?,?,?)""",
                    (
                        scope_id,
                        logical_scope_id,
                        target["target_id"],
                        json.dumps(expected.to_dict(), separators=(",", ":"), sort_keys=True),
                        command_id,
                        digest,
                        envelope["issued_at"],
                        envelope["issued_at"],
                    ),
                )
                crash(CrashPoint.CENTRAL_BEFORE_COMMIT)
            crash(CrashPoint.CENTRAL_AFTER_COMMIT_BEFORE_SEND)
            return envelope
        except sqlite3.Error as exc:
            raise map_sqlite_failure(exc) from exc

    def begin_delivery(self, command_id: str) -> None:
        if not self.delivery_allowed():
            raise CommandBlocked("DELIVERY_FORBIDDEN")
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            outbox = db.execute(
                """SELECT o.outbox_id,o.attempt_count,c.status FROM outbox_messages o
                   JOIN commands c ON c.command_id=o.command_id WHERE o.command_id=?""",
                (command_id,),
            ).fetchone()
            if outbox is None:
                raise CommandBlocked("OUTBOX_NOT_FOUND")
            attempt_no = int(outbox["attempt_count"]) + 1
            db.execute(
                "UPDATE outbox_messages SET state='IN_FLIGHT',attempt_count=?,last_attempt_at=?,updated_at=? WHERE command_id=?",
                (attempt_no, stamp, stamp, command_id),
            )
            db.execute("UPDATE commands SET status='SENT',updated_at=? WHERE command_id=?", (stamp, command_id))
            if outbox["status"] != "SENT":
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{uuid.uuid4()}",
                        command_id,
                        "CRA",
                        outbox["status"],
                        "SENT",
                        "DELIVERY_STARTED",
                        None,
                        stamp,
                    ),
                )
            db.execute(
                "INSERT INTO delivery_attempts VALUES (?,?,?,?,NULL,'IN_FLIGHT',NULL,NULL,NULL,'SEND_STARTED')",
                (f"delivery-{command_id}-{attempt_no}", outbox["outbox_id"], attempt_no, stamp),
            )

    def record_receipt(
        self,
        command_id: str,
        receipt: dict[str, Any],
        *,
        verifier: SignedMessageCodec,
    ) -> None:
        receipt = verifier.decode(receipt)
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            outbox = db.execute(
                """SELECT outbox.outbox_id,outbox.attempt_count,
                          command.authority_epoch,command.command_seq,target.last_agent_installation_id
                   FROM outbox_messages AS outbox
                   JOIN commands AS command ON command.command_id=outbox.command_id
                   JOIN targets AS target ON target.target_id=command.target_id
                   WHERE outbox.command_id=?""",
                (command_id,),
            ).fetchone()
            if outbox is None:
                raise CommandBlocked("OUTBOX_NOT_FOUND")
            if (
                str(receipt["command_id"]) != command_id
                or int(receipt["authority_epoch"]) != int(outbox["authority_epoch"])
                or int(receipt["command_seq"]) != int(outbox["command_seq"])
                or str(receipt["agent_installation_id"]) != str(outbox["last_agent_installation_id"])
            ):
                raise CommandBlocked("COMMAND_RECEIPT_BINDING_MISMATCH")
            db.execute(
                "UPDATE outbox_messages SET state='ACKED',updated_at=? WHERE command_id=?",
                (stamp, command_id),
            )
            state = "ACCEPTED" if receipt["disposition"] in ("ACCEPTED", "DUPLICATE") else "REJECTED"
            current = db.execute("SELECT status FROM commands WHERE command_id=?", (command_id,)).fetchone()
            if current is None:
                raise CommandBlocked("COMMAND_NOT_FOUND")
            db.execute("UPDATE commands SET status=?,updated_at=? WHERE command_id=?", (state, stamp, command_id))
            if current["status"] != state:
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{uuid.uuid4()}",
                        command_id,
                        "DELL_AGENT",
                        current["status"],
                        state,
                        receipt["reason_code"],
                        receipt["receipt_id"],
                        stamp,
                    ),
                )
            db.execute(
                """UPDATE delivery_attempts SET finished_at=?,transport_outcome='RESPONSE',http_status=200,
                   receipt_message_id=?,receipt_digest=?,detail_code=?
                   WHERE outbox_id=? AND attempt_no=?""",
                (
                    stamp,
                    receipt["receipt_id"],
                    receipt["payload_sha256"],
                    receipt["reason_code"],
                    outbox["outbox_id"],
                    outbox["attempt_count"],
                ),
            )
            db.execute(
                "INSERT OR IGNORE INTO agent_messages VALUES (?,?,?,?,?,1,?)",
                (
                    receipt["receipt_id"],
                    command_id,
                    "COMMAND_RECEIPT",
                    json.dumps(receipt, separators=(",", ":"), sort_keys=True),
                    receipt["payload_sha256"],
                    stamp,
                ),
            )

    def deliver_command(
        self,
        command_id: str,
        send: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        verifier: SignedMessageCodec,
        crash: CrashInjector = no_crash,
    ) -> dict[str, Any]:
        envelope = next(
            (item for item in self.pending_envelopes() if item["command_id"] == command_id),
            None,
        )
        if envelope is None:
            raise CommandBlocked("DELIVERABLE_OUTBOX_NOT_FOUND")
        self.begin_delivery(command_id)
        receipt = send(envelope)
        crash(CrashPoint.CENTRAL_AFTER_SEND_BEFORE_RECEIPT)
        self.record_receipt(command_id, receipt, verifier=verifier)
        return receipt

    def record_status(self, status: dict[str, Any], *, verifier: SignedMessageCodec) -> None:
        status = verifier.decode(status)
        command_id = str(status["command_id"])
        new_state = str(status["command_state"])
        allowed = {
            "ACCEPTED",
            "EXECUTION_STARTED",
            "EFFECT_OBSERVED",
            "EFFECT_FAILED",
            "OUTCOME_UNKNOWN",
            "REJECTED",
        }
        if new_state not in allowed:
            raise CommandBlocked("UNSUPPORTED_AGENT_COMMAND_STATE")
        received_now = utc_now()
        stamp = isoformat_utc(received_now)
        agent_observed_at = parse_utc(str(status["agent_observed_at"]))
        if agent_observed_at > received_now + timedelta(seconds=2):
            raise CommandBlocked("COMMAND_STATUS_TIMESTAMP_IN_FUTURE")
        effect_observed_value = status.get("effect_observed_at")
        if effect_observed_value is not None and parse_utc(str(effect_observed_value)) > agent_observed_at:
            raise CommandBlocked("COMMAND_STATUS_EFFECT_AFTER_AGENT_OBSERVATION")
        with self.write() as db:
            duplicate = db.execute(
                "SELECT command_id,payload_sha256 FROM agent_messages WHERE message_id=?",
                (status["status_id"],),
            ).fetchone()
            if duplicate is not None:
                if str(duplicate["command_id"]) != command_id or str(duplicate["payload_sha256"]) != str(status["payload_sha256"]):
                    raise CommandBlocked("COMMAND_STATUS_ID_CONFLICT")
                return
            current = db.execute(
                """SELECT command.status,command.authority_epoch,command.command_seq,
                          authorization.expected_target_json,target.last_agent_installation_id
                   FROM commands AS command
                   JOIN recovery_authorizations AS authorization
                     ON authorization.authorization_id=command.authorization_id
                   JOIN targets AS target ON target.target_id=command.target_id
                   WHERE command.command_id=?""",
                (command_id,),
            ).fetchone()
            if current is None:
                raise CommandBlocked("COMMAND_NOT_FOUND")
            expected_target = TargetIdentity.from_dict(dict(json.loads(str(current["expected_target_json"]))))
            if (
                int(status["authority_epoch"]) != int(current["authority_epoch"])
                or int(status["command_seq"]) != int(current["command_seq"])
                or str(status["agent_installation_id"]) != str(current["last_agent_installation_id"])
                or str(status["effect_scope_id"]) != effect_scope_id("restart_ffmpeg", expected_target)
            ):
                raise CommandBlocked("COMMAND_STATUS_BINDING_MISMATCH")
            before_target = status.get("before_target")
            if before_target is None or TargetIdentity.from_dict(dict(before_target)) != expected_target:
                raise CommandBlocked("COMMAND_STATUS_BEFORE_TARGET_MISMATCH")
            if new_state == "EFFECT_OBSERVED" and (
                int(status["attempt_count"]) != 1 or status.get("after_target") is None or status.get("effect_observed_at") is None
            ):
                raise CommandBlocked("COMMAND_STATUS_EFFECT_EVIDENCE_INCOMPLETE")
            if new_state in {"ACCEPTED", "REJECTED"} and int(status["attempt_count"]) != 0:
                raise CommandBlocked("COMMAND_STATUS_PRE_EFFECT_STATE_HAS_ATTEMPT")
            if int(status["attempt_count"]) == 0 and (
                status.get("after_target") is not None or status.get("effect_observed_at") is not None
            ):
                raise CommandBlocked("COMMAND_STATUS_ZERO_ATTEMPT_HAS_EFFECT_EVIDENCE")
            current_state = str(current["status"])
            paths: dict[tuple[str, str], list[str]] = {
                ("SENT", "ACCEPTED"): ["ACCEPTED"],
                ("SENT", "REJECTED"): ["REJECTED"],
                ("SENT", "EXECUTION_STARTED"): ["ACCEPTED", "EXECUTION_STARTED"],
                ("SENT", "EFFECT_OBSERVED"): ["ACCEPTED", "EXECUTION_STARTED", "EFFECT_OBSERVED"],
                ("SENT", "EFFECT_FAILED"): ["ACCEPTED", "EXECUTION_STARTED", "EFFECT_FAILED"],
                ("SENT", "OUTCOME_UNKNOWN"): ["ACCEPTED", "EXECUTION_STARTED", "OUTCOME_UNKNOWN"],
                ("ACCEPTED", "EXECUTION_STARTED"): ["EXECUTION_STARTED"],
                ("ACCEPTED", "EFFECT_OBSERVED"): ["EXECUTION_STARTED", "EFFECT_OBSERVED"],
                ("ACCEPTED", "EFFECT_FAILED"): ["EXECUTION_STARTED", "EFFECT_FAILED"],
                ("ACCEPTED", "OUTCOME_UNKNOWN"): ["EXECUTION_STARTED", "OUTCOME_UNKNOWN"],
                ("EXECUTION_STARTED", "EFFECT_OBSERVED"): ["EFFECT_OBSERVED"],
                ("EXECUTION_STARTED", "EFFECT_FAILED"): ["EFFECT_FAILED"],
                ("EXECUTION_STARTED", "OUTCOME_UNKNOWN"): ["OUTCOME_UNKNOWN"],
            }
            path = [] if current_state == new_state else paths.get((current_state, new_state))
            if path is None:
                raise CommandBlocked("INVALID_AGENT_STATUS_TRANSITION")
            previous = current_state
            for index, state in enumerate(path):
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{uuid.uuid4()}",
                        command_id,
                        "DELL_AGENT",
                        previous,
                        state,
                        status["reason_code"],
                        status["status_id"],
                        stamp,
                    ),
                )
                previous = state
                if index == len(path) - 1:
                    db.execute(
                        "UPDATE commands SET status=?,updated_at=? WHERE command_id=?",
                        (state, stamp, command_id),
                    )
            if new_state == "EFFECT_FAILED" and int(status["attempt_count"]) == 0:
                db.execute(
                    "UPDATE commands SET status='REJECTED',updated_at=? WHERE command_id=?",
                    (stamp, command_id),
                )
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{uuid.uuid4()}",
                        command_id,
                        "CRA",
                        "EFFECT_FAILED",
                        "REJECTED",
                        "PRE_EFFECT_ABORTED_CLASSIFIED",
                        status["status_id"],
                        stamp,
                    ),
                )
            db.execute(
                "INSERT OR IGNORE INTO agent_messages VALUES (?,?,?,?,?,1,?)",
                (
                    status["status_id"],
                    command_id,
                    "COMMAND_STATUS",
                    json.dumps(status, separators=(",", ":"), sort_keys=True),
                    status["payload_sha256"],
                    stamp,
                ),
            )
            ledger = db.execute(
                """SELECT state,physical_attempt_count,effect_boundary_reached_at
                   FROM effect_scope_ledger WHERE origin_kind='CENTRAL_COMMAND' AND origin_id=?""",
                (command_id,),
            ).fetchone()
            if ledger is None:
                raise CommandBlocked("CENTRAL_EFFECT_SCOPE_LEDGER_MISSING")
            attempts = int(status["attempt_count"])
            if attempts < int(ledger["physical_attempt_count"]):
                raise CommandBlocked("CENTRAL_EFFECT_ATTEMPT_COUNT_REGRESSION")
            if attempts:
                ledger_state = (
                    new_state if new_state in {"EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"} else "EFFECT_BOUNDARY_REACHED"
                )
                boundary_at = str(status.get("effect_observed_at") or status["agent_observed_at"])
                if ledger["effect_boundary_reached_at"] is not None and str(ledger["effect_boundary_reached_at"]) != boundary_at:
                    raise CommandBlocked("CENTRAL_EFFECT_BOUNDARY_TIMESTAMP_CONFLICT")
            elif new_state in {"REJECTED", "EFFECT_FAILED"}:
                ledger_state = "PRE_EFFECT_ABORTED"
                boundary_at = None
            elif new_state == "OUTCOME_UNKNOWN":
                ledger_state = "OUTCOME_UNKNOWN"
                boundary_at = None
            elif new_state in {"ACCEPTED", "EXECUTION_STARTED"}:
                ledger_state = "PRECONDITION_CHECKED"
                boundary_at = None
            else:
                ledger_state = "RESERVED"
                boundary_at = None
            db.execute(
                """UPDATE effect_scope_ledger SET state=?,
                       effect_boundary_reached_at=coalesce(effect_boundary_reached_at,?),
                       physical_attempt_count=max(physical_attempt_count,?),source_evidence_digest=?,updated_at=?
                   WHERE origin_kind='CENTRAL_COMMAND' AND origin_id=?""",
                (
                    ledger_state,
                    boundary_at,
                    attempts,
                    status["payload_sha256"],
                    stamp,
                    command_id,
                ),
            )

    def record_recovery_verification(self, verification: dict[str, Any]) -> str:
        if not self._allow_legacy_test_verification:
            raise CommandBlocked("LEGACY_MONITORING_VERIFICATION_PATH_DISABLED")
        if verification.get("payload_sha256") != payload_hash(verification):
            raise CommandBlocked("VERIFICATION_PAYLOAD_HASH_MISMATCH")
        stamp = isoformat_utc(utc_now())
        identity = str(verification["idempotency_key"])
        with self.write() as db:
            duplicate = db.execute(
                "SELECT payload_sha256 FROM recovery_verification_messages WHERE idempotency_key=?",
                (identity,),
            ).fetchone()
            if duplicate is not None:
                if duplicate["payload_sha256"] == verification["payload_sha256"]:
                    return "DUPLICATE"
                raise CommandBlocked("VERIFICATION_PAYLOAD_CONFLICT")
            command = db.execute("SELECT * FROM commands WHERE command_id=?", (verification["command_id"],)).fetchone()
            if command is None:
                raise CommandBlocked("VERIFICATION_COMMAND_NOT_FOUND")
            if command["incident_id"] != verification["incident_id"] or command["target_id"] != verification["target_id"]:
                raise CommandBlocked("VERIFICATION_BINDING_MISMATCH")
            current_state = str(command["status"])
            if current_state in {"VERIFIED", "VERIFICATION_FAILED"}:
                raise CommandBlocked("VERIFICATION_COMMAND_TERMINAL")
            if current_state not in {"EFFECT_OBSERVED", "VERIFYING", "VERIFICATION_UNKNOWN"}:
                raise CommandBlocked("PHYSICAL_EFFECT_NOT_CONFIRMED")
            previous = db.execute(
                "SELECT observed_at FROM recovery_verifications WHERE command_id=?",
                (verification["command_id"],),
            ).fetchone()
            if previous is not None and parse_utc(str(previous["observed_at"])) >= parse_utc(str(verification["observed_at"])):
                raise CommandBlocked("STALE_VERIFICATION")
            db.execute(
                """INSERT INTO recovery_verification_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    verification["verification_id"],
                    verification["command_id"],
                    verification["incident_id"],
                    verification["target_id"],
                    verification["monitoring_cycle_id"],
                    verification["observation_revision"],
                    identity,
                    verification["verdict"],
                    json.dumps(verification, separators=(",", ":"), sort_keys=True),
                    verification["payload_sha256"],
                    verification["observed_at"],
                    verification["evidence_fresh_until"],
                    stamp,
                ),
            )
            db.execute(
                """INSERT INTO recovery_verifications VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(command_id) DO UPDATE SET
                     verification_id=excluded.verification_id,
                     monitoring_cycle_id=excluded.monitoring_cycle_id,
                     verdict=excluded.verdict,
                     observed_target_json=excluded.observed_target_json,
                     checks_json=excluded.checks_json,
                     evidence_refs_json=excluded.evidence_refs_json,
                     observed_at=excluded.observed_at,
                     payload_sha256=excluded.payload_sha256""",
                (
                    verification["verification_id"],
                    verification["command_id"],
                    verification["monitoring_cycle_id"],
                    verification["verdict"],
                    json.dumps(verification["observed_target"], separators=(",", ":"), sort_keys=True),
                    json.dumps(verification["checks"], separators=(",", ":"), sort_keys=True),
                    json.dumps(verification["evidence_refs"], separators=(",", ":"), sort_keys=True),
                    verification["observed_at"],
                    verification["payload_sha256"],
                ),
            )
            if current_state != "VERIFYING":
                db.execute(
                    "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                    (
                        f"transition-{uuid.uuid4()}",
                        verification["command_id"],
                        "MONITORING_V4",
                        current_state,
                        "VERIFYING",
                        "RECOVERY_VERIFICATION_STARTED",
                        verification["verification_id"],
                        stamp,
                    ),
                )
            terminal = {
                "RECOVERED": "VERIFIED",
                "FAILED": "VERIFICATION_FAILED",
                "UNKNOWN": "VERIFICATION_UNKNOWN",
            }[str(verification["verdict"])]
            db.execute(
                "INSERT INTO command_transitions VALUES (?,?,?,?,?,?,?,?)",
                (
                    f"transition-{uuid.uuid4()}",
                    verification["command_id"],
                    "MONITORING_V4",
                    "VERIFYING",
                    terminal,
                    verification["reason_codes"][0],
                    verification["verification_id"],
                    stamp,
                ),
            )
            db.execute(
                "UPDATE commands SET status=?,updated_at=? WHERE command_id=?",
                (terminal, stamp, verification["command_id"]),
            )
            if terminal == "VERIFIED":
                db.execute(
                    "UPDATE incidents SET state='RECOVERED',closed_at=?,last_transition_at=?,version=version+1 WHERE incident_id=?",
                    (stamp, stamp, verification["incident_id"]),
                )
        return "ACCEPTED"

    def mark_restored(
        self,
        backup_id: str = "unknown-backup",
        *,
        crash: CrashInjector = no_crash,
    ) -> None:
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            db.execute(
                """UPDATE control_plane_identity SET restore_state='RESTORED_NEEDS_RECONCILIATION',
                   restored_from_backup_id=?,database_instance_id=?,updated_at=?,version=version+1 WHERE singleton_id=1""",
                (backup_id, str(uuid.uuid4()), stamp),
            )
            db.execute(
                """UPDATE targets SET authority_state='RESTORED_NEEDS_RECONCILIATION',
                   state_reason='RESTORE_REQUIRES_RECONCILIATION',version=version+1"""
            )
            db.execute("UPDATE authority_sessions SET state='BLOCKED' WHERE state IN ('ACTIVE','PROPOSED')")
            db.execute(
                "UPDATE commands SET status='SUPERSEDED',updated_at=? WHERE status IN ('COMMITTED','OUTBOX_PENDING','SENT')",
                (stamp,),
            )
            db.execute(
                "UPDATE outbox_messages SET state='TERMINAL',updated_at=? WHERE state IN ('PENDING','IN_FLIGHT','RETRY')",
                (stamp,),
            )
        crash(CrashPoint.CRA_AFTER_RESTORE)

    def install_reconciliation(
        self,
        *,
        target_id: str,
        new_epoch: int,
        session_id: str,
        controller_instance_id: str,
        agent_installation_id: str,
        reconciliation_id: str,
        challenge_id: str,
        dell_high_epoch: int,
        dell_high_seq: int,
    ) -> None:
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            db.execute("UPDATE authority_sessions SET state='REPLACED',ended_at=? WHERE state='ACTIVE'", (stamp,))
            db.execute(
                "INSERT INTO authority_sessions VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)",
                (
                    session_id,
                    target_id,
                    new_epoch,
                    controller_instance_id,
                    agent_installation_id,
                    reconciliation_id,
                    challenge_id,
                    "ACTIVE",
                    stamp,
                ),
            )
            db.execute(
                """UPDATE targets SET authority_state='CENTRAL_ACTIVE',current_authority_epoch=?,
                   next_command_seq=?,last_agent_installation_id=?,last_dell_high_epoch=?,last_dell_high_seq=?,
                   state_reason='RECONCILED',last_reconciled_at=?,version=version+1 WHERE target_id=?""",
                (new_epoch, 1, agent_installation_id, dell_high_epoch, dell_high_seq, stamp, target_id),
            )
            db.execute(
                """UPDATE control_plane_identity SET restore_state='CLEAN',updated_at=?,version=version+1
                   WHERE singleton_id=1""",
                (stamp,),
            )
            db.execute(
                """UPDATE dell_journal_watermarks SET reconciliation_id=?,updated_at=?
                   WHERE agent_installation_id=? AND target_id=?""",
                (reconciliation_id, stamp, agent_installation_id, target_id),
            )

    def authority_snapshot(self, target_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM targets WHERE target_id=?", (target_id,)).fetchone()
        if row is None:
            raise KeyError(target_id)
        return cast(sqlite3.Row, row)
