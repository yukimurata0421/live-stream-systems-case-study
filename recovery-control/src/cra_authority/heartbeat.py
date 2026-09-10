from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from cra_authority.storage import CentralStore
from cra_dell_recovery.canonical import SignedMessageCodec
from cra_dell_recovery.models import MonitoringReadiness
from cra_dell_recovery.time import isoformat_utc, utc_now


class AuthorityHeartbeatPublisher:
    """Authority readiness lease, deliberately distinct from systemd process watchdog."""

    def __init__(
        self,
        store: CentralStore,
        codec: SignedMessageCodec,
        *,
        interval_seconds: float = 2.0,
        lease_ttl_seconds: float = 15.0,
    ) -> None:
        self.store = store
        self.codec = codec
        self.interval_seconds = interval_seconds
        self.lease_ttl_seconds = lease_ttl_seconds
        self._sequence: dict[str, int] = {}

    def build(self, target_id: str, monitoring: MonitoringReadiness) -> dict[str, Any] | None:
        # Authority liveness and action readiness are separate safety signals.
        # A healthy CRA keeps its lease while publishing decision_ready=false
        # when Monitoring cannot currently support a recovery authorization.
        if not self.store.delivery_allowed():
            return None
        target = self.store.authority_snapshot(target_id)
        if target["authority_state"] != "CENTRAL_ACTIVE":
            return None
        session = self.store.connection.execute(
            "SELECT * FROM authority_sessions WHERE target_id=? AND state='ACTIVE'",
            (target_id,),
        ).fetchone()
        if session is None or int(session["authority_epoch"]) != int(target["current_authority_epoch"]):
            return None
        seq = self._sequence.get(target_id, 0) + 1
        now = utc_now()
        envelope = self.codec.encode(
            {
                "protocol": "cra_dell_recovery.heartbeat.v1",
                "message_type": "authority_heartbeat",
                "heartbeat_id": f"heartbeat-{uuid.uuid4()}",
                "sender_id": "cra-authority",
                "receiver_id": str(target["agent_id"]),
                "controller_instance_id": str(session["controller_instance_id"]),
                "authority_session_id": str(session["authority_session_id"]),
                "authority_epoch": int(session["authority_epoch"]),
                "heartbeat_seq": seq,
                "target_id": target_id,
                "decision_ready": monitoring.authority_ready,
                "monitoring_observed_at": monitoring.observed_at,
                "issued_at": isoformat_utc(now),
                "expires_at": isoformat_utc(now + timedelta(seconds=self.lease_ttl_seconds)),
                "key_id": self.codec.signer.key_id,
            }
        )
        stamp = isoformat_utc(now)
        with self.store.write() as db:
            db.execute(
                "UPDATE authority_sessions SET last_heartbeat_sent_at=? WHERE authority_session_id=? AND state='ACTIVE'",
                (stamp, session["authority_session_id"]),
            )
        self._sequence[target_id] = seq
        return envelope
