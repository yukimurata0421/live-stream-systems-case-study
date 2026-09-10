from __future__ import annotations

import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator

from cra_dell_recovery.canonical import KeyRing, Signer, canonical_json
from cra_dell_recovery.errors import (
    ProtocolValidationError,
    ProtocolVersionError,
    SignatureValidationError,
    UnknownKeyError,
)
from cra_dell_recovery.schema import PROTOCOL_TO_SCHEMA, SchemaRegistry

CANONICAL_VECTOR = (
    '{"authority_epoch":7,"authority_session_id":"session-1","controller_instance_id":"controller-1",'
    '"decision_ready":true,"expires_at":"2026-08-23T00:00:16.000Z",'
    '"heartbeat_id":"heartbeat-vector-1","heartbeat_seq":11,'
    '"issued_at":"2026-08-23T00:00:01.000Z","key_id":"cra-test-key",'
    '"message_type":"authority_heartbeat","monitoring_observed_at":"2026-08-23T00:00:00.000Z",'
    '"protocol":"cra_dell_recovery.heartbeat.v1","receiver_id":"dell-agent",'
    '"sender_id":"cra-authority","target_id":"stream-target"}'
)
EXPECTED_DIGEST = "008eca93d14133fc2dd8a1cca63877a113d9bb46562af07cecd6dccd3a39c224"
EXPECTED_SIGNATURE = "p9ClEHtJolhpWTDXuoPPop3N3O+wUW4JQQsuln5nJw2A2maS37rkQWIg95kXKAw9tkyHUj5DOm/z9GTlHA8xBw=="


def vector_payload() -> dict[str, object]:
    return {
        "protocol": "cra_dell_recovery.heartbeat.v1",
        "message_type": "authority_heartbeat",
        "heartbeat_id": "heartbeat-vector-1",
        "sender_id": "cra-authority",
        "receiver_id": "dell-agent",
        "controller_instance_id": "controller-1",
        "authority_session_id": "session-1",
        "authority_epoch": 7,
        "heartbeat_seq": 11,
        "target_id": "stream-target",
        "decision_ready": True,
        "monitoring_observed_at": "2026-08-23T00:00:00.000Z",
        "issued_at": "2026-08-23T00:00:01.000Z",
        "expires_at": "2026-08-23T00:00:16.000Z",
        "key_id": "cra-test-key",
    }


def test_all_versioned_contract_schemas_are_valid() -> None:
    root_dir = Path(__file__).resolve().parents[2]
    assert len(PROTOCOL_TO_SCHEMA) == 8
    for filename in PROTOCOL_TO_SCHEMA.values():
        schema = json.loads((root_dir / "contracts/cra_dell_recovery/v1" / filename).read_text())
        Draft202012Validator.check_schema(schema)


def test_rfc8785_canonical_vector() -> None:
    assert canonical_json(vector_payload()).decode() == CANONICAL_VECTOR


def test_sha256_and_signature_vector(registry: SchemaRegistry) -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signed = Signer("cra-test-key", private).sign(vector_payload())
    assert signed["payload_sha256"] == EXPECTED_DIGEST
    assert signed["signature"] == EXPECTED_SIGNATURE
    registry.validate(signed)
    KeyRing({"cra-test-key": private.public_key()}).verify(signed)


def test_tampered_payload_fails_signature() -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signed = Signer("cra-test-key", private).sign(vector_payload())
    signed["heartbeat_seq"] = 12
    with pytest.raises(SignatureValidationError):
        KeyRing({"cra-test-key": private.public_key()}).verify(signed)


def test_wrong_key_fails_signature() -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    wrong = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    signed = Signer("cra-test-key", private).sign(vector_payload())
    with pytest.raises(SignatureValidationError):
        KeyRing({"cra-test-key": wrong.public_key()}).verify(signed)


def test_unknown_key_fails_closed() -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signed = Signer("cra-test-key", private).sign(vector_payload())
    with pytest.raises(UnknownKeyError):
        KeyRing({}).verify(signed)


def test_unknown_message_field_is_rejected(registry: SchemaRegistry) -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signed = Signer("cra-test-key", private).sign({**vector_payload(), "unexpected": True})
    with pytest.raises(ProtocolValidationError):
        registry.validate(signed)


def test_unknown_nested_target_field_is_rejected(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    command["expected_target"]["pid_only_fallback"] = True  # type: ignore[index]
    with pytest.raises(ProtocolValidationError):
        environment.central_codec.registry.validate(command)  # type: ignore[attr-defined]


def test_protocol_version_mismatch_is_rejected(registry: SchemaRegistry) -> None:
    with pytest.raises(ProtocolVersionError):
        registry.validate({"protocol": "cra_dell_recovery.heartbeat.v2"})
