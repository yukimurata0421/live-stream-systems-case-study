from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import rfc8785
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from .errors import SignatureValidationError, UnknownKeyError
from .schema import SchemaRegistry

SIGNATURE_FIELDS = frozenset({"payload_sha256", "signature"})


def unsigned_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in SIGNATURE_FIELDS}


def canonical_json(payload: Mapping[str, Any]) -> bytes:
    return rfc8785.dumps(unsigned_payload(payload))


def payload_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload)).hexdigest()


@dataclass(frozen=True)
class Signer:
    key_id: str
    private_key: Ed25519PrivateKey  # gitleaks:allow -- typed key object, not key material

    def sign(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if payload.get("key_id") != self.key_id:
            raise ValueError("payload key_id does not match signer")
        result = dict(payload)
        canonical = canonical_json(result)
        result["payload_sha256"] = hashlib.sha256(canonical).hexdigest()
        result["signature"] = base64.b64encode(self.private_key.sign(canonical)).decode("ascii")
        return result


class KeyRing:
    def __init__(self, keys: Mapping[str, Ed25519PublicKey]):
        self._keys = dict(keys)

    def verify(self, payload: Mapping[str, Any]) -> None:
        key_id = str(payload.get("key_id") or "")
        key = self._keys.get(key_id)
        if key is None:
            raise UnknownKeyError(f"unknown key_id: {key_id or '<missing>'}")
        expected_digest = payload_digest(payload)
        if str(payload.get("payload_sha256") or "") != expected_digest:
            raise SignatureValidationError("payload digest mismatch")
        try:
            signature = base64.b64decode(str(payload.get("signature") or ""), validate=True)
            key.verify(signature, canonical_json(payload))
        except (InvalidSignature, ValueError) as exc:
            raise SignatureValidationError("signature verification failed") from exc


class SignedMessageCodec:
    def __init__(self, registry: SchemaRegistry, signer: Signer, verifier: KeyRing):
        self.registry = registry
        self.signer = signer
        self.verifier = verifier

    def encode(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        signed = self.signer.sign(payload)
        self.registry.validate(signed)
        return signed

    def decode(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(payload)
        self.registry.validate(value)
        self.verifier.verify(value)
        return value
