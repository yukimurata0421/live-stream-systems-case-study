"""Signed, finite operator grants without CRA policy or automatic failover."""
from __future__ import annotations

import hashlib
import hmac
import json
import math
from dataclasses import dataclass

FIELDS = frozenset({"schema", "relay_instance", "target", "encoder_instance", "sender_instance",
                    "route_generation", "authority_epoch", "sequence", "input_sequence",
                    "issued_at", "expires_at", "grant_expires_at", "route"})


class LeaseRejected(ValueError):
    pass


def canonical(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sign(value: dict, key: bytes) -> str:
    return hmac.new(key, canonical(value), hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class Lease:
    body: dict
    elapsed_deadline: float

    def valid(self, wall: float, elapsed: float) -> bool:
        return (elapsed < self.elapsed_deadline and wall < self.body["expires_at"]
                and wall < self.body["grant_expires_at"]
                and wall >= self.body["issued_at"] - 2)


def admit(body: dict, signature: str, *, key: bytes, relay_instance: str, highwater: int,
          wall: float, elapsed: float, previous: Lease | None, continuous: bool = False) -> Lease:
    if set(body) != FIELDS or body.get("schema") != "stream.relay_lease.v1":
        raise LeaseRejected("LEASE_SCHEMA_INVALID")
    if not isinstance(signature, str) or not hmac.compare_digest(sign(body, key), signature):
        raise LeaseRejected("LEASE_SIGNATURE_INVALID")
    for name in ("relay_instance", "target", "encoder_instance", "sender_instance", "authority_epoch"):
        if not isinstance(body[name], str) or not 1 <= len(body[name]) <= 256:
            raise LeaseRejected("LEASE_IDENTITY_INVALID")
    if body["relay_instance"] != relay_instance or body["route"] != "PRIMARY":
        raise LeaseRejected("LEASE_ROUTE_OR_INSTANCE_MISMATCH")
    for name in ("sequence", "input_sequence", "route_generation"):
        if type(body[name]) is not int or body[name] <= 0:
            raise LeaseRejected("LEASE_SEQUENCE_INVALID")
    if body["sequence"] <= highwater:
        raise LeaseRejected("LEASE_REPLAY")
    for name in ("issued_at", "expires_at", "grant_expires_at"):
        if type(body[name]) not in (int, float) or not math.isfinite(body[name]):
            raise LeaseRejected("LEASE_TIME_INVALID")
    issued, expires, grant = (body[n] for n in ("issued_at", "expires_at", "grant_expires_at"))
    if not 0 < expires - issued <= 15 or not issued - 2 <= wall < expires or expires > grant:
        raise LeaseRejected("LEASE_EXPIRED_OR_CLOCK_UNBOUND")
    if not 0 < grant - issued <= 86400:
        raise LeaseRejected("LEASE_GRANT_TOO_LONG")
    # Continuous authorization still limits each publishing permission to its short lease.
    if continuous and grant != expires:
        raise LeaseRejected("CONTINUOUS_GRANT_NOT_LEASE_BOUND")
    if previous is not None:
        old = previous.body
        fixed = ("target", "encoder_instance", "sender_instance", "route_generation", "authority_epoch")
        if not continuous:
            fixed += ("grant_expires_at",)
        if any(body[n] != old[n] for n in fixed):
            raise LeaseRejected("LEASE_IDENTITY_CHANGED_WITHOUT_STOP")
        if body["input_sequence"] <= old["input_sequence"]:
            raise LeaseRejected("LEASE_SOURCE_NOT_PROGRESSING")
    # A future issue time must not extend the elapsed deadline.
    return Lease(dict(body), elapsed + min(15, expires - wall))
