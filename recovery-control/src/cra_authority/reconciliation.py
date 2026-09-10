from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any, Protocol

from cra_authority.storage import CentralStore
from cra_dell_recovery.canonical import SignedMessageCodec
from cra_dell_recovery.time import isoformat_utc, utc_now


class ReconciliationPeer(Protocol):
    def issue_challenge(self, target_id: str) -> dict[str, Any]: ...

    def commit(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def journal_page(self, payload: dict[str, Any]) -> dict[str, Any]: ...


def commit_response_matches(
    result: dict[str, Any],
    *,
    target_id: str,
    authority_epoch: int,
    authority_session_id: str,
) -> bool:
    return (
        result.get("result") == "COMMITTED"
        and str(result.get("target_id") or "") == target_id
        and not isinstance(result.get("authority_epoch"), bool)
        and isinstance(result.get("authority_epoch"), int)
        and int(result["authority_epoch"]) == authority_epoch
        and str(result.get("authority_session_id") or "") == authority_session_id
    )


class CentralReconciler:
    def __init__(
        self,
        store: CentralStore,
        codec: SignedMessageCodec,
        peer: ReconciliationPeer,
    ) -> None:
        self.store = store
        self.codec = codec
        self.peer = peer

    def reconcile(self, target_id: str) -> int:
        challenge = self.codec.decode(self.peer.issue_challenge(target_id))
        if str(challenge["target_id"]) != target_id:
            raise ValueError("Dell challenge target binding mismatch")
        central = self.store.authority_snapshot(target_id)
        dell_high_epoch = int(challenge["highest_authority_epoch_seen"])
        dell_high_seq = int(challenge["highest_command_seq_consumed"])
        if challenge["unresolved_command_ids"] or challenge["unresolved_local_action_ids"]:
            raise ValueError("unresolved Dell action blocks reconciliation")
        agent_installation_id = str(challenge["agent_installation_id"])
        journal_sequence, journal_digest = self.store.dell_journal_watermark(agent_installation_id, target_id)
        journal_high_water = int(challenge["local_journal_high_water"])
        journal_high_digest = str(challenge["local_journal_high_digest"])
        if journal_sequence > journal_high_water:
            raise ValueError("CRA journal watermark is ahead of Dell")
        while journal_sequence < journal_high_water:
            request_now = utc_now()
            request = self.codec.encode(
                {
                    "protocol": "cra_dell_recovery.local_action_journal_page.v1",
                    "message_type": "local_action_journal_page_request",
                    "request_id": f"journal-request-{uuid.uuid4()}",
                    "reconciliation_id": challenge["reconciliation_id"],
                    "target_id": target_id,
                    "after_sequence": journal_sequence,
                    "page_limit": 100,
                    "issued_at": isoformat_utc(request_now),
                    "expires_at": isoformat_utc(request_now + timedelta(seconds=30)),
                    "key_id": self.codec.signer.key_id,
                }
            )
            page = self.codec.decode(self.peer.journal_page(request))
            if (
                str(page["reconciliation_id"]) != str(challenge["reconciliation_id"])
                or str(page["target_id"]) != target_id
                or str(page["agent_installation_id"]) != agent_installation_id
                or int(page["journal_high_water"]) != journal_high_water
                or str(page["journal_high_digest"]) != journal_high_digest
            ):
                raise ValueError("Dell journal page binding mismatch")
            previous_sequence = journal_sequence
            journal_sequence, journal_digest = self.store.import_dell_journal_page(page)
            if journal_sequence == previous_sequence:
                raise ValueError("Dell journal pagination made no progress")
        if journal_sequence != journal_high_water or journal_digest != journal_high_digest:
            raise ValueError("Dell journal high-water mismatch")
        new_epoch = max(int(central["current_authority_epoch"]), dell_high_epoch) + 1
        session_id = f"session-{uuid.uuid4()}"
        identity = self.store.connection.execute(
            "SELECT controller_instance_id FROM control_plane_identity WHERE singleton_id=1"
        ).fetchone()
        if identity is None:
            raise ValueError("central identity unavailable")
        now = utc_now()
        commit = self.codec.encode(
            {
                "protocol": "cra_dell_recovery.reconciliation_commit.v1",
                "message_type": "reconciliation_commit",
                "commit_id": f"commit-{uuid.uuid4()}",
                "reconciliation_id": challenge["reconciliation_id"],
                "challenge_id": challenge["challenge_id"],
                "challenge_nonce": challenge["challenge_nonce"],
                "target_id": target_id,
                "controller_instance_id": str(identity[0]),
                "agent_installation_id": challenge["agent_installation_id"],
                "proposed_authority_epoch": new_epoch,
                "proposed_authority_session_id": session_id,
                "journal_ack_sequence": journal_sequence,
                "journal_ack_record_digest": journal_digest,
                "issued_at": isoformat_utc(now),
                "expires_at": isoformat_utc(now + timedelta(seconds=30)),
                "key_id": self.codec.signer.key_id,
            }
        )
        result = self.peer.commit(commit)
        if result.get("result") != "COMMITTED":
            raise ValueError("Dell did not commit reconciliation")
        if not commit_response_matches(
            result,
            target_id=target_id,
            authority_epoch=new_epoch,
            authority_session_id=session_id,
        ):
            raise ValueError("Dell commit response binding mismatch")
        self.store.install_reconciliation(
            target_id=target_id,
            new_epoch=new_epoch,
            session_id=session_id,
            controller_instance_id=str(identity[0]),
            agent_installation_id=str(challenge["agent_installation_id"]),
            reconciliation_id=str(challenge["reconciliation_id"]),
            challenge_id=str(challenge["challenge_id"]),
            dell_high_epoch=dell_high_epoch,
            dell_high_seq=dell_high_seq,
        )
        return new_epoch
