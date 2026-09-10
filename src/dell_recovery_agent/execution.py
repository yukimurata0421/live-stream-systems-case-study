from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from cra_dell_recovery.canonical import SignedMessageCodec, canonical_json
from cra_dell_recovery.crash import CrashInjector, CrashPoint, no_crash
from cra_dell_recovery.effect_scope import effect_scope_id
from cra_dell_recovery.errors import AuthorityDeadlineExceeded
from cra_dell_recovery.models import LocalRecoveryCandidate, TargetIdentity, is_expected_ffmpeg_successor
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now
from dell_recovery_agent.deadline import AuthorityOperationDeadline
from dell_recovery_agent.storage import DellStore
from maintenance_audit import audit_maintenance_decision

LOCAL_CANDIDATE_MAX_AGE_SECONDS = 30.0
LOCAL_CANDIDATE_MAX_FUTURE_SKEW_SECONDS = 2.0
LOCAL_CANDIDATE_MIN_DISTINCT_SAMPLES = 3
LOCAL_CANDIDATE_MAX_DISTINCT_SAMPLES = 1_000_000
LOCAL_CANDIDATE_MAX_CONFIRM_THRESHOLD = 20
LOCAL_CANDIDATE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,239}$")
LOCAL_CANDIDATE_ALLOWED_EVIDENCE_KEYS = frozenset(
    {
        "tcp_stall_confirmed",
        "distinct_sample_count",
        "stall_confirm_threshold",
        "network_down",
        "remote_warning_only",
        "youtube_state_only",
        "low_upload_pressure",
        "ffmpeg_missing",
        "maintenance",
        "outcome_unknown",
    }
)
LOCAL_CANDIDATE_REJECTED_EVIDENCE_KEYS = frozenset(
    {
        "network_down",
        "remote_warning_only",
        "youtube_state_only",
        "low_upload_pressure",
        "ffmpeg_missing",
        "maintenance",
        "outcome_unknown",
    }
)


def local_candidate_evidence_rejection(evidence: object) -> str | None:
    """Validate the complete v1 local evidence vocabulary without semantic extras."""

    if not isinstance(evidence, dict):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if len(evidence) > len(LOCAL_CANDIDATE_ALLOWED_EVIDENCE_KEYS):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if any(not isinstance(key, str) for key in evidence):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if set(evidence) - LOCAL_CANDIDATE_ALLOWED_EVIDENCE_KEYS:
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    for key in LOCAL_CANDIDATE_REJECTED_EVIDENCE_KEYS:
        if key in evidence and not isinstance(evidence[key], bool):
            return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if evidence.get("tcp_stall_confirmed") is not True:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    distinct_samples = evidence.get("distinct_sample_count")
    if isinstance(distinct_samples, bool) or not isinstance(distinct_samples, int):
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    if not LOCAL_CANDIDATE_MIN_DISTINCT_SAMPLES <= distinct_samples <= LOCAL_CANDIDATE_MAX_DISTINCT_SAMPLES:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    confirm_threshold = evidence.get("stall_confirm_threshold")
    if isinstance(confirm_threshold, bool) or not isinstance(confirm_threshold, int):
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    if not LOCAL_CANDIDATE_MIN_DISTINCT_SAMPLES <= confirm_threshold <= LOCAL_CANDIDATE_MAX_CONFIRM_THRESHOLD:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    if distinct_samples < confirm_threshold:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    if any(evidence.get(item) is True for item in LOCAL_CANDIDATE_REJECTED_EVIDENCE_KEYS):
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    return None


def local_candidate_matches_existing(existing: Any, candidate: LocalRecoveryCandidate) -> bool:
    """Bind a local idempotency key to the complete persisted semantic payload."""

    try:
        stored_target = json.loads(str(existing["target_identity_json"]))
        stored_evidence = json.loads(str(existing["evidence_json"]))
        stored_target_json = json.dumps(stored_target, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
        candidate_target_json = json.dumps(
            candidate.target_identity.to_dict(), ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
        )
        stored_evidence_json = json.dumps(stored_evidence, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
        candidate_evidence_json = json.dumps(candidate.evidence, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    return (
        str(existing["target_id"]) == candidate.target_id
        and str(existing["action"]) == candidate.action
        and str(existing["reason_code"]) == candidate.reason_code
        and stored_target_json == candidate_target_json
        and stored_evidence_json == candidate_evidence_json
    )


class TargetObserver(Protocol):
    def observe(self) -> TargetIdentity | None: ...


class PhysicalAdapter(Protocol):
    def restart_ffmpeg(self, expected_target: TargetIdentity) -> FakeExecutionResult: ...


@dataclass(frozen=True)
class FakeExecutionResult:
    effect_observed: bool
    after_target: TargetIdentity
    reason_code: str = "FAKE_EFFECT_OBSERVED"


class FakeTargetObserver:
    def __init__(self, *observations: TargetIdentity | None) -> None:
        self._observations = list(observations)
        self._last = observations[-1] if observations else None

    def observe(self) -> TargetIdentity | None:
        if self._observations:
            self._last = self._observations.pop(0)
        return self._last


class FakePhysicalAdapter:
    """No signal, process, Pod, or Deployment operation exists in this adapter."""

    def __init__(self, *, after_target: TargetIdentity | None = None) -> None:
        self.attempt_count = 0
        self.after_target = after_target

    def restart_ffmpeg(self, expected_target: TargetIdentity) -> FakeExecutionResult:
        self.attempt_count += 1
        successor = self.after_target or TargetIdentity.from_dict(
            {
                **expected_target.to_dict(),
                "ffmpeg_generation": f"{expected_target.ffmpeg_generation}:fake-successor",
                "ffmpeg_pid": expected_target.ffmpeg_pid + 1,
            }
        )
        return FakeExecutionResult(True, successor)


class AgentService:
    def __init__(
        self,
        store: DellStore,
        codec: SignedMessageCodec,
        observer: TargetObserver,
        adapter: PhysicalAdapter,
        *,
        critical_db_deadline_seconds: float = 3.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.codec = codec
        self.observer = observer
        self.adapter = adapter
        if critical_db_deadline_seconds <= 0:
            raise ValueError("critical DB deadline must be positive")
        self.critical_db_deadline_seconds = critical_db_deadline_seconds
        self.monotonic = monotonic

    def _observation_failure_reason(self) -> str:
        reason = getattr(self.observer, "last_reason_code", "TARGET_OBSERVATION_UNAVAILABLE")
        allowed = {"STALE_TARGET", "SNAPSHOT_UNSTABLE", "TARGET_OBSERVATION_UNAVAILABLE"}
        return reason if reason in allowed else "TARGET_OBSERVATION_UNAVAILABLE"

    def _receipt(self, command: dict[str, Any], disposition: str, reason: str, state: str) -> dict[str, Any]:
        fence = self.store.fence(str(command["target"]["target_id"]))
        identity = self.store.agent_identity()
        return self.codec.encode(
            {
                "protocol": "cra_dell_recovery.command_receipt.v1",
                "message_type": "command_receipt",
                "receipt_id": f"receipt-{uuid.uuid4()}",
                "command_id": str(command["command_id"]),
                "authority_epoch": int(command["authority_epoch"]),
                "command_seq": int(command["command_seq"]),
                "disposition": disposition,
                "command_state": state,
                "reason_code": reason,
                "highest_authority_epoch_seen": int(fence["highest_authority_epoch_seen"]),
                "highest_command_seq_consumed": int(fence["highest_command_seq_consumed"]),
                "agent_installation_id": str(identity["agent_installation_id"]),
                "agent_observed_at": isoformat_utc(utc_now()),
                "key_id": self.codec.signer.key_id,
            }
        )

    def _reject(self, command: dict[str, Any], reason: str) -> dict[str, Any]:
        return self._receipt(command, "REJECTED", reason, "REJECTED")

    def handle_command(self, payload: dict[str, Any], *, crash: CrashInjector = no_crash) -> dict[str, Any]:
        deadline = AuthorityOperationDeadline.start(
            deadline_seconds=self.critical_db_deadline_seconds,
            monotonic=self.monotonic,
        )
        command = self.codec.decode(payload)
        command_id = str(command["command_id"])
        target_id = str(command["target"]["target_id"])
        expected = TargetIdentity.from_dict(command["expected_target"])
        existing = self.store.read_one("SELECT * FROM agent_commands WHERE command_id=?", (command_id,))
        if existing is not None:
            exact = (
                int(existing["authority_epoch"]) == int(command["authority_epoch"])
                and int(existing["command_seq"]) == int(command["command_seq"])
                and str(existing["payload_sha256"]) == str(command["payload_sha256"])
            )
            if exact:
                return self._receipt(command, "DUPLICATE", "EXACT_DUPLICATE", str(existing["state"]))
            return self._reject(command, "COMMAND_ID_CONFLICT")
        slot = self.store.read_one(
            "SELECT command_id,payload_sha256 FROM agent_commands WHERE target_id=? AND authority_epoch=? AND command_seq=?",
            (target_id, command["authority_epoch"], command["command_seq"]),
        )
        if slot is not None:
            return self._reject(command, "COMMAND_ID_CONFLICT")
        fence = self.store.fence(target_id)
        identity = self.store.agent_identity()
        if identity["ledger_state"] != "HEALTHY":
            return self._reject(command, "LEDGER_UNAVAILABLE")
        policy = self.store.read_one("SELECT * FROM agent_safety_policies WHERE target_id=?", (target_id,))
        if policy is None or parse_utc(str(policy["expires_at"])) <= utc_now():
            return self._reject(command, "SAFETY_POLICY_UNAVAILABLE")
        if command["action"] != policy["action"] or command["policy_revision"] != policy["policy_revision"]:
            return self._reject(command, "SAFETY_POLICY_MISMATCH")
        if fence["authority_state"] == "LOCAL_FALLBACK":
            return self._reject(command, "LOCAL_AUTHORITY_ACTIVE")
        if fence["authority_state"] != "CENTRAL_ACTIVE":
            return self._reject(command, "CENTRAL_AUTHORITY_NOT_ACTIVE")
        if not int(fence["action_ready"]):
            return self._reject(command, "CENTRAL_ACTION_NOT_READY")
        if not self.store.process_lease_valid(target_id):
            return self._reject(command, "AUTHORITY_LEASE_NOT_VALID")
        if int(command["authority_epoch"]) != int(fence["highest_authority_epoch_seen"]):
            return self._reject(command, "STALE_AUTHORITY_EPOCH")
        if command["authority_session_id"] != fence["active_authority_session_id"]:
            return self._reject(command, "AUTHORITY_SESSION_MISMATCH")
        expected_seq = int(fence["highest_command_seq_consumed"]) + 1
        if int(command["command_seq"]) < expected_seq:
            return self._reject(command, "STALE_SEQUENCE")
        if int(command["command_seq"]) > expected_seq:
            return self._reject(command, "SEQUENCE_GAP")
        if parse_utc(str(command["expires_at"])) <= utc_now():
            return self._reject(command, "COMMAND_EXPIRED")
        unresolved_row = self.store.read_one(
            """SELECT (SELECT count(*) FROM agent_commands WHERE target_id=? AND state IN
               ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')) +
               (SELECT count(*) FROM local_actions WHERE target_id=? AND state IN
               ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN'))""",
            (target_id, target_id),
        )
        if unresolved_row is None:
            return self._reject(command, "LEDGER_UNAVAILABLE")
        unresolved = unresolved_row[0]
        if int(unresolved):
            return self._reject(command, "ACTIVE_UNRESOLVED_ACTION")
        completed_times = [
            parse_utc(str(row[0]))
            for row in self.store.read_all(
                "SELECT finished_at FROM execution_attempts WHERE state='EFFECT_OBSERVED' AND finished_at IS NOT NULL"
            )
        ]
        now = utc_now()
        if completed_times and (now - max(completed_times)).total_seconds() < int(policy["minimum_action_interval_sec"]):
            return self._reject(command, "HARD_COOLDOWN_ACTIVE")
        action_cost = int(policy["action_cost_sec"])
        hourly_cost = sum(action_cost for stamp in completed_times if (now - stamp).total_seconds() <= 3600)
        daily_cost = sum(action_cost for stamp in completed_times if (now - stamp).total_seconds() <= 86400)
        if hourly_cost + action_cost > int(policy["hourly_action_cost_limit_sec"]):
            return self._reject(command, "HOURLY_ACTION_BUDGET_EXCEEDED")
        if daily_cost + action_cost > int(policy["daily_action_cost_limit_sec"]):
            return self._reject(command, "DAILY_ACTION_BUDGET_EXCEEDED")
        generation_conflict = self.store.generation_scope_conflict(expected)
        if generation_conflict is not None:
            self.store.set_authority_state(target_id, "SAFE_BLOCKED", "GENERATION_PID_INVARIANT_BROKEN")
            return self._reject(command, "GENERATION_PID_INVARIANT_BROKEN")
        claimed_scope = self.store.read_one(
            "SELECT owner_id,state FROM effect_scope_fences WHERE effect_scope_id=?",
            (effect_scope_id("restart_ffmpeg", expected),),
        )
        if (
            claimed_scope is not None
            and str(claimed_scope["owner_id"]) != command_id
            and str(claimed_scope["state"]) != "RELEASED_NO_EFFECT"
        ):
            return self._reject(command, "EFFECT_SCOPE_ALREADY_CLAIMED")
        before = self.observer.observe()
        if before is None:
            return self._reject(command, self._observation_failure_reason())
        if before != expected:
            return self._reject(command, "STALE_TARGET")
        audit_maintenance_decision(
            path_id="MP-08",
            phase="ADMISSION",
            operation="restart_ffmpeg",
            path_role="NORMAL_MUTATOR",
            process_service="dell-recovery-agent-shadow.service",
            resource_identity=f"target/{target_id}/stream-engine/ffmpeg",
            correlation_id=command_id,
            target_identity=before.to_dict(),
            authorization_id=str(command["authorization_id"]),
            in_flight_evidence={"status": "CONFIRMED", "count": 0, "source": "Dell execution ledger"},
            generation_evidence={
                "status": "CONFIRMED",
                "authority_epoch": int(command["authority_epoch"]),
                "command_seq": int(command["command_seq"]),
                "source": "Dell authority_fences high-water",
            },
        )
        stamp = isoformat_utc(utc_now())
        try:
            with self.store.write() as db:
                deadline.ensure_valid()
                current = db.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
                if (
                    current is None
                    or current["authority_state"] != "CENTRAL_ACTIVE"
                    or not int(current["action_ready"])
                    or int(current["highest_authority_epoch_seen"]) != int(command["authority_epoch"])
                    or current["active_authority_session_id"] != command["authority_session_id"]
                    or int(current["highest_command_seq_consumed"]) + 1 != int(command["command_seq"])
                    or not self.store.process_lease_valid(target_id)
                    or parse_utc(str(command["expires_at"])) <= utc_now()
                ):
                    raise sqlite3.IntegrityError("authority changed before accept commit")
                claimed, _, _ = self.store.claim_effect_scope(
                    db,
                    target_id=target_id,
                    target=expected,
                    owner_kind="CENTRAL_COMMAND",
                    owner_id=command_id,
                    stamp=stamp,
                )
                if not claimed:
                    raise sqlite3.IntegrityError("effect scope already claimed")
                db.execute(
                    """INSERT INTO agent_commands VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        command_id,
                        target_id,
                        command["authority_session_id"],
                        command["authority_epoch"],
                        command["command_seq"],
                        command["incident_id"],
                        command["authorization_id"],
                        command["idempotency_key"],
                        command["action"],
                        command["reason_code"],
                        canonical_json(command).decode("utf-8"),
                        command["payload_sha256"],
                        command["key_id"],
                        command["signature"],
                        json.dumps(expected.to_dict(), separators=(",", ":"), sort_keys=True),
                        "ACCEPTED",
                        None,
                        command["issued_at"],
                        command["expires_at"],
                        stamp,
                        stamp,
                    ),
                )
                db.execute(
                    """INSERT INTO execution_attempts VALUES (?, ?, 1, 'RESERVED', ?, NULL, NULL,
                       'SIGTERM', NULL, ?, NULL, NULL, NULL, NULL)""",
                    (
                        f"execution-{command_id}",
                        command_id,
                        json.dumps(before.to_dict(), separators=(",", ":"), sort_keys=True),
                        stamp,
                    ),
                )
                db.execute(
                    "UPDATE authority_fences SET highest_command_seq_consumed=?,version=version+1 WHERE target_id=?",
                    (command["command_seq"], target_id),
                )
                deadline.ensure_valid()
            crash(CrashPoint.DELL_AFTER_ACCEPT_COMMIT)
        except AuthorityDeadlineExceeded:
            self.store.set_process_lease_valid(target_id, False)
            return self._reject(command, "DB_DEADLINE_AUTHORITY_INVALIDATED")
        except sqlite3.Error:
            return self._reject(command, "LEDGER_UNAVAILABLE")
        try:
            deadline.ensure_valid()
        except AuthorityDeadlineExceeded:
            self.store.set_process_lease_valid(target_id, False)
            with self.store.write() as db:
                db.execute(
                    "UPDATE agent_commands SET state='EFFECT_FAILED',terminal_reason_code=?,updated_at=? WHERE command_id=?",
                    ("DB_DEADLINE_AUTHORITY_INVALIDATED", stamp, command_id),
                )
                db.execute(
                    "UPDATE execution_attempts SET state='FAILED',finished_at=?,outcome_reason_code=? WHERE command_id=?",
                    (stamp, "DB_DEADLINE_AUTHORITY_INVALIDATED", command_id),
                )
                self.store.transition_effect_scope(
                    db,
                    owner_id=command_id,
                    state="RELEASED_NO_EFFECT",
                    reason="DB_DEADLINE_BEFORE_EFFECT",
                    evidence={"physical_attempt_count": 0},
                    stamp=stamp,
                )
            return self._reject(command, "DB_DEADLINE_AUTHORITY_INVALIDATED")
        with self.store.write() as db:
            db.execute(
                "UPDATE agent_commands SET state='EXECUTION_STARTED',updated_at=? WHERE command_id=?",
                (stamp, command_id),
            )
            self.store.transition_effect_scope(
                db,
                owner_id=command_id,
                state="EXECUTION_STARTED",
                reason="EXECUTION_STARTED_DURABLE",
                evidence={"physical_attempt_count": 0},
                stamp=stamp,
            )
            db.execute(
                "UPDATE execution_attempts SET state='STARTED',started_at=? WHERE command_id=?",
                (stamp, command_id),
            )
        crash(CrashPoint.DELL_AFTER_EXECUTION_STARTED_COMMIT)
        try:
            deadline.ensure_valid()
        except AuthorityDeadlineExceeded:
            self.store.set_process_lease_valid(target_id, False)
            with self.store.write() as db:
                db.execute(
                    "UPDATE agent_commands SET state='EFFECT_FAILED',terminal_reason_code=?,updated_at=? WHERE command_id=?",
                    ("DB_DEADLINE_AUTHORITY_INVALIDATED", stamp, command_id),
                )
                db.execute(
                    "UPDATE execution_attempts SET state='FAILED',finished_at=?,outcome_reason_code=? WHERE command_id=?",
                    (stamp, "DB_DEADLINE_AUTHORITY_INVALIDATED", command_id),
                )
                self.store.transition_effect_scope(
                    db,
                    owner_id=command_id,
                    state="RELEASED_NO_EFFECT",
                    reason="DB_DEADLINE_BEFORE_EFFECT",
                    evidence={"physical_attempt_count": 0},
                    stamp=stamp,
                )
            return self._reject(command, "DB_DEADLINE_AUTHORITY_INVALIDATED")
        pre_effect = self.observer.observe()
        if pre_effect is None or pre_effect != expected:
            reason = self._observation_failure_reason() if pre_effect is None else "STALE_TARGET"
            with self.store.write() as db:
                db.execute(
                    "UPDATE agent_commands SET state='EFFECT_FAILED',terminal_reason_code=?,updated_at=? WHERE command_id=?",
                    (reason, stamp, command_id),
                )
                db.execute(
                    """UPDATE execution_attempts SET state='FAILED',pre_effect_target_json=?,finished_at=?,
                       outcome_reason_code=? WHERE command_id=?""",
                    (
                        None if pre_effect is None else json.dumps(pre_effect.to_dict(), separators=(",", ":"), sort_keys=True),
                        stamp,
                        reason,
                        command_id,
                    ),
                )
                self.store.transition_effect_scope(
                    db,
                    owner_id=command_id,
                    state="RELEASED_NO_EFFECT",
                    reason=reason,
                    evidence={"physical_attempt_count": 0},
                    stamp=stamp,
                )
            return self._reject(command, reason)
        audit_maintenance_decision(
            path_id="MP-08",
            phase="EFFECT_BOUNDARY",
            operation="restart_ffmpeg",
            path_role="NORMAL_MUTATOR",
            process_service="dell-recovery-agent-shadow.service",
            resource_identity=f"target/{target_id}/stream-engine/ffmpeg",
            correlation_id=command_id,
            target_identity=pre_effect.to_dict(),
            authorization_id=str(command["authorization_id"]),
            in_flight_evidence={"status": "CONFIRMED", "count": 1, "source": "execution_attempts.state=STARTED"},
            generation_evidence={
                "status": "CONFIRMED",
                "authority_epoch": int(command["authority_epoch"]),
                "command_seq": int(command["command_seq"]),
                "source": "Dell authority_fences high-water",
            },
        )
        with self.store.write() as db:
            self.store.transition_effect_scope(
                db,
                owner_id=command_id,
                state="EFFECT_BOUNDARY_REACHED",
                reason="PHYSICAL_ATTEMPT_RESERVED",
                evidence={"physical_attempt_count": 1},
                stamp=stamp,
                effect_boundary=True,
            )
        try:
            result = self.adapter.restart_ffmpeg(expected)
            if result.effect_observed and not is_expected_ffmpeg_successor(expected, result.after_target):
                raise RuntimeError("POST_EFFECT_TARGET_RELATION_UNSAFE")
        except BaseException:
            with self.store.write() as db:
                db.execute(
                    "UPDATE agent_commands SET state='OUTCOME_UNKNOWN',terminal_reason_code=?,updated_at=? WHERE command_id=?",
                    ("PHYSICAL_ADAPTER_OUTCOME_UNKNOWN", stamp, command_id),
                )
                db.execute(
                    "UPDATE execution_attempts SET state='OUTCOME_UNKNOWN',finished_at=?,outcome_reason_code=? WHERE command_id=?",
                    (stamp, "PHYSICAL_ADAPTER_OUTCOME_UNKNOWN", command_id),
                )
                self.store.transition_effect_scope(
                    db,
                    owner_id=command_id,
                    state="OUTCOME_UNKNOWN",
                    reason="PHYSICAL_ADAPTER_OUTCOME_UNKNOWN",
                    evidence={"physical_attempt_count": 1},
                    stamp=stamp,
                )
            return self._receipt(command, "ACCEPTED", "PHYSICAL_ADAPTER_OUTCOME_UNKNOWN", "OUTCOME_UNKNOWN")
        crash(CrashPoint.DELL_AFTER_FAKE_EFFECT_BEFORE_STATUS)
        state = "EFFECT_OBSERVED" if result.effect_observed else "EFFECT_FAILED"
        with self.store.write() as db:
            db.execute(
                "UPDATE agent_commands SET state=?,terminal_reason_code=?,updated_at=? WHERE command_id=?",
                (state, result.reason_code, stamp, command_id),
            )
            db.execute(
                """UPDATE execution_attempts SET state=?,pre_effect_target_json=?,after_target_json=?,
                   signal_return_code=0,effect_observed_at=?,finished_at=?,outcome_reason_code=? WHERE command_id=?""",
                (
                    state,
                    json.dumps(pre_effect.to_dict(), separators=(",", ":"), sort_keys=True),
                    json.dumps(result.after_target.to_dict(), separators=(",", ":"), sort_keys=True),
                    stamp,
                    stamp,
                    result.reason_code,
                    command_id,
                ),
            )
            self.store.transition_effect_scope(
                db,
                owner_id=command_id,
                state=state,
                reason=result.reason_code,
                evidence={
                    "physical_attempt_count": 1,
                    "after_target": result.after_target.to_dict(),
                },
                stamp=stamp,
            )
        return self._receipt(command, "ACCEPTED", result.reason_code, state)

    def evaluate_shadow(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Evaluate all fences without consuming sequence or invoking any adapter."""
        command = self.codec.decode(payload)
        target_id = str(command["target"]["target_id"])
        reason = "SHADOW_POLICY_ACCEPTED"
        decision = "WOULD_ACCEPT"
        with self.store.read() as db:
            fence = db.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
            identity = db.execute("SELECT * FROM agent_identity WHERE singleton_id=1").fetchone()
            policy = db.execute("SELECT * FROM agent_safety_policies WHERE target_id=?", (target_id,)).fetchone()
            existing = db.execute("SELECT payload_sha256 FROM agent_commands WHERE command_id=?", (command["command_id"],)).fetchone()
            slot = db.execute(
                "SELECT command_id FROM agent_commands WHERE target_id=? AND authority_epoch=? AND command_seq=?",
                (target_id, command["authority_epoch"], command["command_seq"]),
            ).fetchone()
            unresolved_row = db.execute(
                """SELECT (SELECT count(*) FROM agent_commands WHERE target_id=? AND state IN
                   ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')) +
                   (SELECT count(*) FROM local_actions WHERE target_id=? AND state IN
                   ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN'))""",
                (target_id, target_id),
            ).fetchone()
        if fence is None or identity is None or unresolved_row is None:
            reason = "LEDGER_UNAVAILABLE"
            decision = "WOULD_REJECT"
            return {
                "command_id": str(command["command_id"]),
                "shadow_decision": decision,
                "reason_code": reason,
                "authority_epoch": int(command["authority_epoch"]),
                "command_seq": int(command["command_seq"]),
                "physical_attempt_count": 0,
                "agent_observed_at": isoformat_utc(utc_now()),
                "physical_adapter": "FAKE_COUNTER_ONLY",
            }
        if existing is not None:
            reason = "EXACT_DUPLICATE" if str(existing[0]) == str(command["payload_sha256"]) else "COMMAND_ID_CONFLICT"
        elif slot is not None:
            reason = "COMMAND_ID_CONFLICT"
        elif identity["ledger_state"] != "HEALTHY":
            reason = "LEDGER_UNAVAILABLE"
        elif policy is None or parse_utc(str(policy["expires_at"])) <= utc_now():
            reason = "SAFETY_POLICY_UNAVAILABLE"
        elif command["action"] != policy["action"] or command["policy_revision"] != policy["policy_revision"]:
            reason = "SAFETY_POLICY_MISMATCH"
        elif fence["authority_state"] == "LOCAL_FALLBACK":
            reason = "LOCAL_AUTHORITY_ACTIVE"
        elif fence["authority_state"] != "CENTRAL_ACTIVE":
            reason = "CENTRAL_AUTHORITY_NOT_ACTIVE"
        elif not self.store.process_lease_valid(target_id):
            reason = "AUTHORITY_LEASE_NOT_VALID"
        elif int(command["authority_epoch"]) != int(fence["highest_authority_epoch_seen"]):
            reason = "STALE_AUTHORITY_EPOCH"
        elif command["authority_session_id"] != fence["active_authority_session_id"]:
            reason = "AUTHORITY_SESSION_MISMATCH"
        elif int(command["command_seq"]) < int(fence["highest_command_seq_consumed"]) + 1:
            reason = "STALE_SEQUENCE"
        elif int(command["command_seq"]) > int(fence["highest_command_seq_consumed"]) + 1:
            reason = "SEQUENCE_GAP"
        elif parse_utc(str(command["expires_at"])) <= utc_now():
            reason = "COMMAND_EXPIRED"
        else:
            unresolved = unresolved_row[0]
            if int(unresolved):
                reason = "ACTIVE_UNRESOLVED_ACTION"
            else:
                observed = self.observer.observe()
                if observed is None:
                    reason = self._observation_failure_reason()
                elif observed != TargetIdentity.from_dict(command["expected_target"]):
                    reason = "STALE_TARGET"
                else:
                    verify = self.observer.observe()
                    if verify is None:
                        reason = self._observation_failure_reason()
                    elif verify != observed:
                        reason = "SNAPSHOT_UNSTABLE"
        if reason not in {"SHADOW_POLICY_ACCEPTED", "EXACT_DUPLICATE"}:
            decision = "WOULD_REJECT"
        audit_maintenance_decision(
            path_id="MP-08",
            phase="ADMISSION",
            operation="shadow_restart_ffmpeg_evaluation",
            path_role="NORMAL_MUTATOR",
            process_service="dell-recovery-agent-shadow.service",
            resource_identity=f"target/{target_id}/stream-engine/ffmpeg",
            correlation_id=str(command["command_id"]),
            target_identity=TargetIdentity.from_dict(command["expected_target"]).to_dict(),
            authorization_id=str(command["authorization_id"]),
            in_flight_evidence={"status": "CONFIRMED", "count": 0, "source": "shadow evaluator has no physical adapter call"},
            generation_evidence={
                "status": "CONFIRMED",
                "authority_epoch": int(command["authority_epoch"]),
                "command_seq": int(command["command_seq"]),
                "source": "signed shadow command",
            },
        )
        stamp = isoformat_utc(utc_now())
        event = {
            "command_id": str(command["command_id"]),
            "shadow_decision": decision,
            "reason_code": reason,
            "authority_epoch": int(command["authority_epoch"]),
            "command_seq": int(command["command_seq"]),
            "physical_attempt_count": 0,
        }
        with self.store.write() as db:
            db.execute(
                "INSERT INTO agent_events VALUES (?,?,?,?,?,?)",
                (
                    f"event-{uuid.uuid4()}",
                    "SHADOW_COMMAND_EVALUATED",
                    "INFO" if decision == "WOULD_ACCEPT" else "WARN",
                    reason,
                    json.dumps(event, separators=(",", ":"), sort_keys=True),
                    stamp,
                ),
            )
        return {
            **event,
            "agent_observed_at": stamp,
            "physical_adapter": "FAKE_COUNTER_ONLY",
        }

    def command_status(self, command_id: str) -> dict[str, Any]:
        with self.store.read() as db:
            command = db.execute("SELECT * FROM agent_commands WHERE command_id=?", (command_id,)).fetchone()
            attempt = db.execute("SELECT * FROM execution_attempts WHERE command_id=?", (command_id,)).fetchone()
            scope = db.execute(
                "SELECT effect_scope_id,physical_attempt_count FROM effect_scope_fences WHERE owner_id=?",
                (command_id,),
            ).fetchone()
            identity = db.execute("SELECT agent_installation_id FROM agent_identity WHERE singleton_id=1").fetchone()
        if command is None:
            raise KeyError(command_id)
        if scope is None or identity is None:
            raise RuntimeError("COMMAND_STATUS_LEDGER_BINDING_MISSING")
        return self.codec.encode(
            {
                "protocol": "cra_dell_recovery.command_status.v1",
                "message_type": "command_status",
                "status_id": f"status-{uuid.uuid4()}",
                "command_id": command_id,
                "authority_epoch": int(command["authority_epoch"]),
                "command_seq": int(command["command_seq"]),
                "agent_installation_id": str(identity["agent_installation_id"]),
                "effect_scope_id": str(scope["effect_scope_id"]),
                "command_state": str(command["state"]),
                "reason_code": str(command["terminal_reason_code"] or "STATUS_QUERY"),
                "attempt_count": int(scope["physical_attempt_count"]),
                "before_target": None if attempt is None else json.loads(str(attempt["before_target_json"])),
                "after_target": None
                if attempt is None or attempt["after_target_json"] is None
                else json.loads(str(attempt["after_target_json"])),
                "effect_observed_at": None if attempt is None else attempt["effect_observed_at"],
                "agent_observed_at": isoformat_utc(utc_now()),
                "key_id": self.codec.signer.key_id,
            }
        )

    def handle_local_candidate(self, candidate: LocalRecoveryCandidate) -> str:
        if not isinstance(candidate.local_action_id, str) or LOCAL_CANDIDATE_ID_PATTERN.fullmatch(candidate.local_action_id) is None:
            return "LOCAL_ACTION_ID_INVALID"
        try:
            self.store.assert_managed_target(candidate.target_id)
        except ValueError:
            return "LOCAL_TARGET_NOT_MANAGED"
        existing_action = self.store.read_one(
            """SELECT state,target_id,action,reason_code,target_identity_json,evidence_json
               FROM local_actions WHERE local_action_id=?""",
            (candidate.local_action_id,),
        )
        if existing_action is not None:
            if local_candidate_matches_existing(existing_action, candidate):
                return str(existing_action["state"])
            return "LOCAL_ACTION_ID_CONFLICT"
        fence = self.store.fence(candidate.target_id)
        if fence["authority_state"] != "LOCAL_FALLBACK":
            return "LOCAL_AUTHORITY_NOT_ACTIVE"
        identity = self.store.agent_identity()
        if identity["ledger_state"] != "HEALTHY":
            return "LEDGER_UNAVAILABLE"
        policy = self.store.read_one("SELECT * FROM agent_safety_policies WHERE target_id=?", (candidate.target_id,))
        if policy is None or not int(policy["local_fallback_enabled"]) or parse_utc(str(policy["expires_at"])) <= utc_now():
            return "SAFETY_POLICY_UNAVAILABLE"
        if candidate.action != "restart_ffmpeg" or candidate.reason_code != "confirmed_tcp_stall":
            return "LOCAL_ACTION_NOT_ALLOWLISTED"
        evidence_rejection = local_candidate_evidence_rejection(candidate.evidence)
        if evidence_rejection is not None:
            return evidence_rejection
        try:
            evidence_age_seconds = (utc_now() - parse_utc(candidate.observed_at)).total_seconds()
        except (TypeError, ValueError):
            return "LOCAL_EVIDENCE_TIME_INVALID"
        if evidence_age_seconds < -LOCAL_CANDIDATE_MAX_FUTURE_SKEW_SECONDS:
            return "LOCAL_EVIDENCE_FROM_FUTURE"
        if evidence_age_seconds > LOCAL_CANDIDATE_MAX_AGE_SECONDS:
            return "LOCAL_EVIDENCE_STALE"
        unresolved_row = self.store.read_one(
            """SELECT (SELECT count(*) FROM agent_commands WHERE target_id=? AND state IN
               ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')) +
               (SELECT count(*) FROM local_actions WHERE target_id=? AND state IN
               ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN'))""",
            (candidate.target_id, candidate.target_id),
        )
        if unresolved_row is None:
            return "LEDGER_UNAVAILABLE"
        unresolved = unresolved_row[0]
        if int(unresolved):
            return "ACTIVE_UNRESOLVED_ACTION"
        completed_times = [
            parse_utc(str(row[0]))
            for row in self.store.read_all(
                """SELECT finished_at FROM execution_attempts
                   WHERE state='EFFECT_OBSERVED' AND finished_at IS NOT NULL
                   UNION ALL SELECT updated_at FROM local_actions WHERE state='EFFECT_OBSERVED'"""
            )
        ]
        now = utc_now()
        if completed_times and (now - max(completed_times)).total_seconds() < int(policy["minimum_action_interval_sec"]):
            return "HARD_COOLDOWN_ACTIVE"
        action_cost = int(policy["action_cost_sec"])
        hourly_cost = sum(action_cost for stamp in completed_times if (now - stamp).total_seconds() <= 3600)
        daily_cost = sum(action_cost for stamp in completed_times if (now - stamp).total_seconds() <= 86400)
        if hourly_cost + action_cost > int(policy["hourly_action_cost_limit_sec"]):
            return "HOURLY_ACTION_BUDGET_EXCEEDED"
        if daily_cost + action_cost > int(policy["daily_action_cost_limit_sec"]):
            return "DAILY_ACTION_BUDGET_EXCEEDED"
        generation_conflict = self.store.generation_scope_conflict(candidate.target_identity)
        if generation_conflict is not None:
            self.store.set_authority_state(
                candidate.target_id,
                "SAFE_BLOCKED",
                "GENERATION_PID_INVARIANT_BROKEN",
            )
            return "GENERATION_PID_INVARIANT_BROKEN"
        claimed_scope = self.store.read_one(
            "SELECT owner_id,state FROM effect_scope_fences WHERE effect_scope_id=?",
            (effect_scope_id("restart_ffmpeg", candidate.target_identity),),
        )
        if (
            claimed_scope is not None
            and str(claimed_scope["owner_id"]) != candidate.local_action_id
            and str(claimed_scope["state"]) != "RELEASED_NO_EFFECT"
        ):
            return "EFFECT_SCOPE_ALREADY_CLAIMED"
        before = self.observer.observe()
        if before is None:
            return "TARGET_OBSERVATION_UNAVAILABLE"
        if before != candidate.target_identity:
            return "STALE_TARGET"
        pre_effect = self.observer.observe()
        if pre_effect is None or pre_effect != before:
            return "STALE_TARGET" if pre_effect is not None else "TARGET_OBSERVATION_UNAVAILABLE"
        audit_maintenance_decision(
            path_id="MP-09",
            phase="ADMISSION",
            operation="restart_ffmpeg",
            path_role="LOCAL_FALLBACK",
            process_service="dell-recovery-agent-shadow.service",
            resource_identity=f"target/{candidate.target_id}/stream-engine/ffmpeg",
            correlation_id=candidate.local_action_id,
            target_identity=pre_effect.to_dict(),
            in_flight_evidence={"status": "CONFIRMED", "count": 0, "source": "Dell command and local action ledger"},
            generation_evidence={
                "status": "CONFIRMED",
                "local_action_id": candidate.local_action_id,
                "source": "LocalRecoveryCandidate identity before local sequence allocation",
            },
        )
        session_id = self.store.begin_local_fallback(candidate.target_id)
        next_seq_row = self.store.read_one(
            "SELECT coalesce(max(local_action_seq),0)+1 FROM local_actions WHERE local_session_id=?",
            (session_id,),
        )
        if next_seq_row is None:
            return "LEDGER_UNAVAILABLE"
        next_seq = int(next_seq_row[0])
        stamp = isoformat_utc(utc_now())
        with self.store.write() as db:
            current = db.execute("SELECT authority_state FROM authority_fences WHERE target_id=?", (candidate.target_id,)).fetchone()
            unresolved = db.execute(
                """SELECT (SELECT count(*) FROM agent_commands WHERE target_id=? AND state IN
                   ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')) +
                   (SELECT count(*) FROM local_actions WHERE target_id=? AND state IN
                   ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN'))""",
                (candidate.target_id, candidate.target_id),
            ).fetchone()[0]
            if current is None or current["authority_state"] != "LOCAL_FALLBACK" or int(unresolved):
                return "AUTHORITY_OR_UNRESOLVED_ACTION_CHANGED"
            claimed, _, _ = self.store.claim_effect_scope(
                db,
                target_id=candidate.target_id,
                target=before,
                owner_kind="LOCAL_ACTION",
                owner_id=candidate.local_action_id,
                stamp=stamp,
            )
            if not claimed:
                return "EFFECT_SCOPE_ALREADY_CLAIMED"
            db.execute(
                "INSERT INTO local_actions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    candidate.local_action_id,
                    session_id,
                    candidate.target_id,
                    next_seq,
                    candidate.action,
                    candidate.reason_code,
                    json.dumps(before.to_dict(), separators=(",", ":"), sort_keys=True),
                    json.dumps(candidate.evidence, separators=(",", ":"), sort_keys=True),
                    "EXECUTION_STARTED",
                    stamp,
                    stamp,
                ),
            )
            self.store.transition_effect_scope(
                db,
                owner_id=candidate.local_action_id,
                state="EXECUTION_STARTED",
                reason="LOCAL_EXECUTION_STARTED_DURABLE",
                evidence={"physical_attempt_count": 0},
                stamp=stamp,
            )
            self.store.append_local_journal(
                db,
                local_action_id=candidate.local_action_id,
                record_state="EXECUTION_STARTED",
                before_target=before,
                after_target=None,
                evidence=candidate.evidence,
                reason_code=candidate.reason_code,
                stamp=stamp,
            )
        audit_maintenance_decision(
            path_id="MP-09",
            phase="EFFECT_BOUNDARY",
            operation="restart_ffmpeg",
            path_role="LOCAL_FALLBACK",
            process_service="dell-recovery-agent-shadow.service",
            resource_identity=f"target/{candidate.target_id}/stream-engine/ffmpeg",
            correlation_id=candidate.local_action_id,
            target_identity=before.to_dict(),
            in_flight_evidence={"status": "CONFIRMED", "count": 1, "source": "local_actions.state=EXECUTION_STARTED"},
            generation_evidence={
                "status": "CONFIRMED",
                "local_session_id": session_id,
                "local_action_seq": next_seq,
                "source": "Dell local_actions unique sequence",
            },
        )
        with self.store.write() as db:
            self.store.transition_effect_scope(
                db,
                owner_id=candidate.local_action_id,
                state="EFFECT_BOUNDARY_REACHED",
                reason="LOCAL_PHYSICAL_ATTEMPT_RESERVED",
                evidence={"physical_attempt_count": 1},
                stamp=stamp,
                effect_boundary=True,
            )
            self.store.append_local_journal(
                db,
                local_action_id=candidate.local_action_id,
                record_state="EFFECT_BOUNDARY_REACHED",
                before_target=before,
                after_target=None,
                evidence=candidate.evidence,
                reason_code="LOCAL_PHYSICAL_ATTEMPT_RESERVED",
                stamp=stamp,
            )
        try:
            result = self.adapter.restart_ffmpeg(before)
            if result.effect_observed and not is_expected_ffmpeg_successor(before, result.after_target):
                raise RuntimeError("POST_EFFECT_TARGET_RELATION_UNSAFE")
        except BaseException:
            with self.store.write() as db:
                db.execute(
                    "UPDATE local_actions SET state='OUTCOME_UNKNOWN',updated_at=? WHERE local_action_id=?",
                    (stamp, candidate.local_action_id),
                )
                self.store.transition_effect_scope(
                    db,
                    owner_id=candidate.local_action_id,
                    state="OUTCOME_UNKNOWN",
                    reason="LOCAL_PHYSICAL_ADAPTER_OUTCOME_UNKNOWN",
                    evidence={"physical_attempt_count": 1},
                    stamp=stamp,
                )
                self.store.append_local_journal(
                    db,
                    local_action_id=candidate.local_action_id,
                    record_state="OUTCOME_UNKNOWN",
                    before_target=before,
                    after_target=None,
                    evidence=candidate.evidence,
                    reason_code="LOCAL_PHYSICAL_ADAPTER_OUTCOME_UNKNOWN",
                    stamp=stamp,
                )
            return "OUTCOME_UNKNOWN"
        final = "EFFECT_OBSERVED" if result.effect_observed else "EFFECT_FAILED"
        with self.store.write() as db:
            db.execute(
                "UPDATE local_actions SET state=?,updated_at=? WHERE local_action_id=?",
                (final, stamp, candidate.local_action_id),
            )
            self.store.transition_effect_scope(
                db,
                owner_id=candidate.local_action_id,
                state=final,
                reason=result.reason_code,
                evidence={"physical_attempt_count": 1, "after_target": result.after_target.to_dict()},
                stamp=stamp,
            )
            self.store.append_local_journal(
                db,
                local_action_id=candidate.local_action_id,
                record_state=final,
                before_target=before,
                after_target=result.after_target,
                evidence=candidate.evidence,
                reason_code=result.reason_code,
                stamp=stamp,
            )
        return final
