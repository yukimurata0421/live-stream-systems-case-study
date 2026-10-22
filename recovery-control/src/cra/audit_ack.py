"""Request-bound audit acknowledgements, signed by the already trusted central key."""

from __future__ import annotations

import hashlib
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra import signing

SCHEMA = "cra.audit_ack.v1"


def request_digest(body: dict[str, Any]) -> str:
    return hashlib.sha256(signing.canonical(body)).hexdigest()


def sign_ack(key: Ed25519PrivateKey, request: dict[str, Any], high: int) -> dict[str, Any]:
    ack = {
        "schema": SCHEMA,
        "agent_id": request["agent_id"],
        "instance_id": request["instance_id"],
        "nonce": request.get("nonce", ""),
        "request_sha256": request_digest(request),
        "acked_seq": high,
    }
    # The top-level value allows a central-first rollout with the previous agent.
    return {"acked_seq": high, "ack": ack, "signature": signing.sign(key, ack)}


def verify_ack(public_key: str, response: Any, request: dict[str, Any], *, previous: int) -> int:
    if not isinstance(response, dict) or not isinstance(response.get("ack"), dict):
        raise ValueError("ACK_UNSIGNED")  # noqa: TRY004 - invalid wire data, handled by sync's ValueError boundary
    ack = response["ack"]
    signature = response.get("signature")
    if not isinstance(signature, str) or not signing.verify(public_key, ack, signature):
        raise ValueError("ACK_SIGNATURE_INVALID")
    expected = {
        "schema": SCHEMA,
        "agent_id": request["agent_id"],
        "instance_id": request["instance_id"],
        "nonce": request["nonce"],
        "request_sha256": request_digest(request),
        "acked_seq": ack.get("acked_seq"),
    }
    if ack != expected:
        raise ValueError("ACK_REQUEST_MISMATCH")
    high = ack["acked_seq"]
    if type(high) is not int or not previous <= high <= request["records"][-1]["seq"] or response.get("acked_seq") != high:
        raise ValueError("ACK_RANGE_INVALID")
    return high
