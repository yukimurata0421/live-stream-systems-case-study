"""Dell -> CRA synchronisation. Always initiated by Dell; nothing here can grant or block an action
except by adopting a newer signed policy. Failure only delays policy updates and audit delivery."""

from __future__ import annotations

import json
import secrets
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra import audit_ack, signing
from cra import policy as policy_mod
from cra.store import AgentStore

POLICY_KEY = "policy_envelope"
ACKED_KEY = "acked_seq"


def load_cached_policy(store: AgentStore, public_key: str) -> policy_mod.Policy | None:
    raw = store.get(POLICY_KEY)
    if raw is None:
        return None
    try:
        return policy_mod.open_envelope(json.loads(raw), public_key)
    except (ValueError, policy_mod.PolicyError):
        return None


class CentralSync:
    def __init__(
        self,
        *,
        store: AgentStore,
        agent_id: str,
        url: str,
        policy_public_key: str,
        agent_key: Ed25519PrivateKey,
        batch: int = 500,
        timeout_sec: float = 5.0,
    ) -> None:
        self.store = store
        self.agent_id = agent_id
        self.url = url.rstrip("/")
        self.policy_public_key = policy_public_key
        self.agent_key = agent_key
        self.batch = batch
        self.timeout_sec = timeout_sec
        self._last_rejection = ""

    def _http(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(self.url + path, data=data, method=method, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=self.timeout_sec) as response:
            return json.loads(response.read(1 << 20))

    def fetch_policy(self, now: datetime) -> str:
        """Adopt the central policy when it is validly signed and newer (I8). Returns a status word."""
        envelope = self._http("GET", "/v1/policy")
        current = load_cached_policy(self.store, self.policy_public_key)
        try:
            candidate = policy_mod.open_envelope(envelope, self.policy_public_key)
            error = policy_mod.adoption_error(current, candidate)
        except policy_mod.PolicyError as exc:
            candidate, error = None, str(exc)
        if error:
            key = f"{error}:{getattr(candidate, 'version', '?')}"
            if key != self._last_rejection:
                self._last_rejection = key
                self.store.audit(now, "policy_rejected", {"reason": error, "version": getattr(candidate, "version", None)})
            return "REJECTED"
        assert candidate is not None
        if current == candidate:
            return "UNCHANGED"
        self.store.put(
            POLICY_KEY,
            json.dumps(envelope, sort_keys=True),
            now=now,
            audit_kind="policy_adopted",
            audit={"version": candidate.version, "mode": candidate.mode, "previous_version": getattr(current, "version", None)},
        )
        return "ADOPTED"

    def push_audit(self) -> int:
        """Send unacknowledged audit records; returns the new acknowledged sequence."""
        acked = int(self.store.get(ACKED_KEY) or 0)
        records = self.store.read_audit(acked, self.batch)
        if not records:
            return acked
        body = {
            "agent_id": self.agent_id,
            "instance_id": self.store.instance_id,
            "records": records,
            "nonce": secrets.token_hex(16),
        }
        response = self._http("POST", "/v1/audit", {"body": body, "signature": signing.sign(self.agent_key, body)})
        new_acked = audit_ack.verify_ack(self.policy_public_key, response, body, previous=acked)
        if new_acked != acked:
            self.store.put(ACKED_KEY, str(new_acked))
        return new_acked

    def cycle(self, now: datetime) -> str:
        try:
            policy_status = self.fetch_policy(now)
            acked = self.push_audit()
            for _ in range(20):  # drain a backlog left by a LAN outage
                if acked >= self.store.last_seq():
                    break
                previous, acked = acked, self.push_audit()
                if acked == previous:
                    break
        except (OSError, urllib.error.URLError, ValueError) as exc:
            return f"ERROR:{type(exc).__name__}"
        return f"{policy_status}:acked={acked}"
