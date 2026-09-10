from __future__ import annotations

import hashlib
import json
import secrets
import time
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from cra_dell_recovery.canonical import SignedMessageCodec
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now
from dell_recovery_agent.deadline import AuthorityOperationDeadline
from dell_recovery_agent.execution import TargetObserver
from dell_recovery_agent.storage import DellStore


class DellReconciliationService:
    def __init__(
        self,
        store: DellStore,
        codec: SignedMessageCodec,
        observer: TargetObserver,
        *,
        critical_db_deadline_seconds: float = 3.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.codec = codec
        self.observer = observer
        if critical_db_deadline_seconds <= 0:
            raise ValueError("critical DB deadline must be positive")
        self.critical_db_deadline_seconds = critical_db_deadline_seconds
        self.monotonic = monotonic

    def issue_challenge(self, target_id: str) -> dict[str, Any]:
        now = utc_now()
        reconciliation_id = f"reconciliation-{uuid.uuid4()}"
        challenge_id = f"challenge-{uuid.uuid4()}"
        nonce = secrets.token_urlsafe(32)
        observed = self.observer.observe()
        with self.store.write() as db:
            fence = db.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
            identity = db.execute("SELECT * FROM agent_identity WHERE singleton_id=1").fetchone()
            if fence is None or identity is None:
                raise ValueError("reconciliation snapshot incomplete")
            unresolved = [
                str(row[0])
                for row in db.execute(
                    """SELECT command_id FROM agent_commands WHERE target_id=?
                       AND state IN ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')""",
                    (target_id,),
                )
            ]
            unresolved_local_actions = [
                str(row[0])
                for row in db.execute(
                    """SELECT local_action_id FROM local_actions WHERE target_id=?
                       AND state IN ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')""",
                    (target_id,),
                )
            ]
            journal = db.execute(
                """SELECT journal_sequence,record_digest FROM local_action_journal
                   WHERE target_id=? ORDER BY journal_sequence DESC LIMIT 1""",
                (target_id,),
            ).fetchone()
            ack = db.execute(
                "SELECT ack_sequence FROM local_action_journal_acks WHERE target_id=?",
                (target_id,),
            ).fetchone()
            db.execute(
                "INSERT INTO reconciliation_sessions VALUES (?,?,?,?,NULL,NULL,NULL,'CHALLENGE_ISSUED',?,?,NULL)",
                (
                    reconciliation_id,
                    target_id,
                    challenge_id,
                    hashlib.sha256(nonce.encode()).hexdigest(),
                    isoformat_utc(now),
                    isoformat_utc(now + timedelta(seconds=30)),
                ),
            )
            db.execute(
                """UPDATE authority_fences SET authority_state='RECONCILING',action_ready=0,
                   state_reason='CHALLENGE_ISSUED' WHERE target_id=?""",
                (target_id,),
            )
        return self.codec.encode(
            {
                "protocol": "cra_dell_recovery.reconciliation_challenge.v1",
                "message_type": "reconciliation_challenge",
                "reconciliation_id": reconciliation_id,
                "challenge_id": challenge_id,
                "challenge_nonce": nonce,
                "target_id": target_id,
                "agent_installation_id": str(identity["agent_installation_id"]),
                "highest_authority_epoch_seen": int(fence["highest_authority_epoch_seen"]),
                "highest_command_seq_consumed": int(fence["highest_command_seq_consumed"]),
                "unresolved_command_ids": unresolved,
                "unresolved_local_action_ids": unresolved_local_actions,
                "local_journal_high_water": 0 if journal is None else int(journal["journal_sequence"]),
                "local_journal_high_digest": "0" * 64 if journal is None else str(journal["record_digest"]),
                "local_journal_ack_watermark": 0 if ack is None else int(ack["ack_sequence"]),
                "target_identity": None if observed is None else observed.to_dict(),
                "issued_at": isoformat_utc(now),
                "expires_at": isoformat_utc(now + timedelta(seconds=30)),
                "key_id": self.codec.signer.key_id,
            }
        )

    def journal_page(self, payload: dict[str, Any]) -> dict[str, Any]:
        value = self.codec.decode(payload)
        if value.get("message_type") != "local_action_journal_page_request":
            raise ValueError("journal page request type invalid")
        if parse_utc(str(value["expires_at"])) <= utc_now():
            raise ValueError("journal page request expired")
        session = self.store.read_one(
            """SELECT target_id,state FROM reconciliation_sessions
               WHERE reconciliation_id=?""",
            (str(value["reconciliation_id"]),),
        )
        if session is None or str(session["target_id"]) != str(value["target_id"]) or session["state"] != "CHALLENGE_ISSUED":
            raise ValueError("journal page reconciliation is not active")
        rows = self.store.local_journal_rows(
            str(value["target_id"]),
            after_sequence=int(value["after_sequence"]),
            limit=int(value["page_limit"]),
        )
        records: list[dict[str, Any]] = []
        for row in rows:
            record = dict(json.loads(str(row["payload_json"])))
            record["journal_sequence"] = int(row["journal_sequence"])
            record["record_digest"] = str(row["record_digest"])
            records.append(record)
        high_sequence, high_digest = self.store.local_journal_high_water(str(value["target_id"]))
        identity = self.store.agent_identity()
        now = utc_now()
        last_sequence = int(value["after_sequence"]) if not records else int(records[-1]["journal_sequence"])
        return self.codec.encode(
            {
                "protocol": "cra_dell_recovery.local_action_journal_page.v1",
                "message_type": "local_action_journal_page",
                "page_id": f"journal-page-{uuid.uuid4()}",
                "request_id": value["request_id"],
                "reconciliation_id": value["reconciliation_id"],
                "target_id": value["target_id"],
                "agent_installation_id": str(identity["agent_installation_id"]),
                "after_sequence": value["after_sequence"],
                "records": records,
                "journal_high_water": high_sequence,
                "journal_high_digest": high_digest,
                "has_more": last_sequence < high_sequence,
                "issued_at": isoformat_utc(now),
                "expires_at": isoformat_utc(now + timedelta(seconds=30)),
                "key_id": self.codec.signer.key_id,
            }
        )

    def commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        deadline = AuthorityOperationDeadline.start(
            deadline_seconds=self.critical_db_deadline_seconds,
            monotonic=self.monotonic,
        )
        value = self.codec.decode(payload)
        if parse_utc(str(value["expires_at"])) <= utc_now():
            raise ValueError("reconciliation commit expired")
        self.store.commit_reconciliation(
            target_id=str(value["target_id"]),
            reconciliation_id=str(value["reconciliation_id"]),
            challenge_id=str(value["challenge_id"]),
            nonce=str(value["challenge_nonce"]),
            agent_installation_id=str(value["agent_installation_id"]),
            new_epoch=int(value["proposed_authority_epoch"]),
            session_id=str(value["proposed_authority_session_id"]),
            controller_instance_id=str(value["controller_instance_id"]),
            journal_ack_sequence=int(value["journal_ack_sequence"]),
            journal_ack_record_digest=str(value["journal_ack_record_digest"]),
            deadline=deadline,
        )
        return {
            "result": "COMMITTED",
            "target_id": value["target_id"],
            "authority_epoch": value["proposed_authority_epoch"],
            "authority_session_id": value["proposed_authority_session_id"],
        }
