from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from .ids import stable_id, validate_id
from .time import parse_utc, unix_ts

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
CORE_CHECKS = (
    "stream_engine_ready",
    "ffmpeg_present",
    "ffmpeg_generation_changed",
    "tcp_flow_healthy",
    "upload_progress_healthy",
    "startup_gate",
)
SOURCE_CONTRACT = {
    "stream_engine_ready": "cra_dell_recovery.target_snapshot.v1",
    "ffmpeg_present": "cra_dell_recovery.target_snapshot.v1",
    "ffmpeg_generation_changed": "cra_dell_recovery.target_snapshot.v1",
    "tcp_flow_healthy": "monitoring_v4.observation.v1:rtmps_tcp",
    "upload_progress_healthy": "monitoring_v4.observation.v1:runtime_delivery_watchdog",
    "external_public_signal": "monitoring_v4.current.v1:viewer_external",
    "youtube_state": "monitoring_v4.current.v1:youtube_lifecycle",
    "startup_gate": "cra_dell_recovery.target_snapshot.v1",
}


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()


@dataclass(frozen=True)
class VerificationEvidence:
    result: str
    evidence_ref: str
    observed_at: str
    valid_until: str
    evidence_role: str
    source_contract: str

    def __post_init__(self) -> None:
        if self.result not in {"PASS", "FAIL", "UNKNOWN", "NOT_APPLICABLE"}:
            raise ValueError("unsupported verification check result")
        validate_id(self.evidence_ref, field="evidence_ref")
        parse_utc(self.observed_at, field="observed_at")
        parse_utc(self.valid_until, field="valid_until")
        if unix_ts(self.valid_until) < unix_ts(self.observed_at):
            raise ValueError("verification evidence freshness window is reversed")
        if self.evidence_role not in {
            "current_authoritative",
            "current_correlated",
            "supporting",
            "diagnostic",
        }:
            raise ValueError("unsupported evidence_role")
        if not self.source_contract:
            raise ValueError("source_contract is required")

    def to_check(self) -> dict[str, str]:
        return {
            "result": self.result,
            "evidence_ref": self.evidence_ref,
            "observed_at": self.observed_at,
            "evidence_role": self.evidence_role,
        }


@dataclass(frozen=True)
class RecoveryVerification:
    SCHEMA: ClassVar[str] = "monitoring_v4.recovery_verification.v1"

    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        value = dict(self.payload)
        if value.get("schema") != self.SCHEMA:
            raise ValueError("unsupported RecoveryVerification schema")
        validate_id(str(value.get("verification_id", "")), field="verification_id")
        if value.get("verdict") not in {"RECOVERED", "FAILED", "UNKNOWN"}:
            raise ValueError("unsupported RecoveryVerification verdict")
        unsigned = {key: item for key, item in value.items() if key != "payload_sha256"}
        if hashlib.sha256(_canonical(unsigned)).hexdigest() != value.get(
            "payload_sha256"
        ):
            raise ValueError("RecoveryVerification payload hash mismatch")

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


class RecoveryVerificationProducer:
    """Monitoring-owned reducer. Supporting/public evidence can never establish recovery."""

    def produce(
        self,
        *,
        command_id: str,
        incident_id: str,
        target_id: str,
        monitoring_cycle_id: str,
        expected_effect: Mapping[str, Any],
        observed_target: Mapping[str, Any],
        checks: Mapping[str, VerificationEvidence],
        policy_revision: str,
        now_at: str,
    ) -> RecoveryVerification:
        if set(checks) != set(CHECK_NAMES):
            raise ValueError("RecoveryVerification requires the exact check set")
        now_ts = unix_ts(now_at)
        stale = [
            name for name in CORE_CHECKS if unix_ts(checks[name].valid_until) < now_ts
        ]
        wrong_role = [
            name
            for name in CORE_CHECKS
            if checks[name].evidence_role != "current_authoritative"
        ]
        wrong_source = [
            name
            for name in CHECK_NAMES
            if checks[name].source_contract != SOURCE_CONTRACT[name]
        ]
        expected_generation = str(expected_effect["before_ffmpeg_generation"])
        observed_generation = observed_target.get("ffmpeg_generation")
        target_identity_mismatch = (
            str(observed_target.get("host_boot_id"))
            != str(expected_effect.get("before_host_boot_id"))
            or str(observed_target.get("pod_uid"))
            != str(expected_effect.get("before_pod_uid"))
            or observed_generation in (None, "")
        )
        generation_claim_conflict = (
            str(observed_generation) == expected_generation
            and checks["ffmpeg_generation_changed"].result == "PASS"
        )
        failures = [name for name in CORE_CHECKS if checks[name].result == "FAIL"]
        unknown = [name for name in CORE_CHECKS if checks[name].result != "PASS"]
        if (
            wrong_source
            or wrong_role
            or stale
            or target_identity_mismatch
            or generation_claim_conflict
        ):
            verdict = "UNKNOWN"
            reason = "EVIDENCE_IDENTITY_OR_FRESHNESS_UNKNOWN"
        elif failures:
            verdict = "FAILED"
            reason = "CORE_CHECK_FAILED"
        elif unknown:
            verdict = "UNKNOWN"
            reason = "CORE_EVIDENCE_UNKNOWN"
        else:
            verdict = "RECOVERED"
            reason = "FRESH_CORE_CHECKS_RECOVERED"
        revision_material = [
            f"{name}:{checks[name].evidence_ref}:{checks[name].observed_at}:{checks[name].valid_until}:{checks[name].result}"
            for name in CHECK_NAMES
        ]
        observation_revision = f"obsrev-{hashlib.sha256('|'.join(revision_material).encode()).hexdigest()[:32]}"
        identity = f"{command_id}:{monitoring_cycle_id}:{observation_revision}"
        value: dict[str, Any] = {
            "schema": RecoveryVerification.SCHEMA,
            "verification_id": stable_id("verification", identity),
            "command_id": command_id,
            "incident_id": incident_id,
            "target_id": target_id,
            "monitoring_cycle_id": monitoring_cycle_id,
            "observed_at": now_at,
            "evidence_fresh_until": min(
                checks[name].valid_until for name in CORE_CHECKS
            ),
            "expected_effect": {
                "action": "restart_ffmpeg",
                "before_ffmpeg_generation": expected_generation,
                "require_new_ffmpeg_generation": True,
                "require_same_host_boot_id": True,
                "require_same_pod_uid": True,
            },
            "observed_target": dict(observed_target),
            "checks": {name: checks[name].to_check() for name in CHECK_NAMES},
            "verdict": verdict,
            "reason_codes": [reason],
            "evidence_refs": sorted({item.evidence_ref for item in checks.values()}),
            "observation_revision": observation_revision,
            "policy_revision": policy_revision,
            "idempotency_key": identity,
        }
        value["payload_sha256"] = hashlib.sha256(_canonical(value)).hexdigest()
        return RecoveryVerification(value)
