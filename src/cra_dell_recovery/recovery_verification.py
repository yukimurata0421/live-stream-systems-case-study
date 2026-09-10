from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from cra_dell_recovery.canonical import canonical_json
from cra_dell_recovery.time import parse_utc

SCHEMA_ID = "monitoring_v4.recovery_verification.v1"
CHECK_NAMES = (
    "stream_engine_ready",
    "ffmpeg_present",
    "ffmpeg_generation_changed",
    "tcp_flow_healthy",
    "upload_progress_healthy",
    "external_public_signal",
    "youtube_state",
    "startup_gate",
)
CORE_RECOVERY_CHECKS = (
    "stream_engine_ready",
    "ffmpeg_present",
    "ffmpeg_generation_changed",
    "tcp_flow_healthy",
    "upload_progress_healthy",
    "startup_gate",
)


def payload_hash(value: dict[str, Any]) -> str:
    unsigned = {key: item for key, item in value.items() if key != "payload_sha256"}
    return hashlib.sha256(canonical_json(unsigned)).hexdigest()


class RecoveryVerificationContract:
    def __init__(self, schema_path: Path) -> None:
        self.schema_path = schema_path
        import json

        self.schema: dict[str, Any] = json.loads(schema_path.read_text(encoding="utf-8"))
        self.validator = Draft202012Validator(self.schema, format_checker=FormatChecker())

    def validate(self, value: dict[str, Any]) -> None:
        self.validator.validate(value)
        if value["payload_sha256"] != payload_hash(value):
            raise ValueError("RecoveryVerification payload_sha256 mismatch")
        if value["idempotency_key"] != ":".join((value["command_id"], value["monitoring_cycle_id"], value["observation_revision"])):
            raise ValueError("RecoveryVerification idempotency_key mismatch")
        if parse_utc(str(value["evidence_fresh_until"])) < parse_utc(str(value["observed_at"])):
            raise ValueError("RecoveryVerification freshness window is reversed")
        for name in CHECK_NAMES:
            if parse_utc(str(value["checks"][name]["observed_at"])) > parse_utc(str(value["observed_at"])):
                raise ValueError(f"check {name} is newer than verification observation")


def build_recovery_verification(
    *,
    command_id: str,
    incident_id: str,
    target_id: str,
    monitoring_cycle_id: str,
    observed_at: str,
    evidence_fresh_until: str,
    expected_effect: dict[str, Any],
    observed_target: dict[str, Any],
    checks: dict[str, dict[str, str]],
    verdict: str,
    reason_codes: tuple[str, ...],
    observation_revision: str,
    policy_revision: str,
) -> dict[str, Any]:
    identity = ":".join((command_id, monitoring_cycle_id, observation_revision))
    verification_id = f"verification-{hashlib.sha256(identity.encode()).hexdigest()[:32]}"
    evidence_refs = sorted({str(checks[name]["evidence_ref"]) for name in CHECK_NAMES})
    value: dict[str, Any] = {
        "schema": SCHEMA_ID,
        "verification_id": verification_id,
        "command_id": command_id,
        "incident_id": incident_id,
        "target_id": target_id,
        "monitoring_cycle_id": monitoring_cycle_id,
        "observed_at": observed_at,
        "evidence_fresh_until": evidence_fresh_until,
        "expected_effect": expected_effect,
        "observed_target": observed_target,
        "checks": checks,
        "verdict": verdict,
        "reason_codes": sorted(set(reason_codes)),
        "evidence_refs": evidence_refs,
        "observation_revision": observation_revision,
        "policy_revision": policy_revision,
        "idempotency_key": identity,
    }
    value["payload_sha256"] = payload_hash(value)
    return value
