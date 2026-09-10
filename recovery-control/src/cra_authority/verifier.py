from __future__ import annotations

import hashlib
from typing import Any, Protocol

from cra_authority.monitoring_evidence import MonitoringEvidenceProjection
from cra_dell_recovery.effect_scope import effect_scope_id
from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now


class VerifierStore(Protocol):
    def record_verifier_decision(self, value: dict[str, Any]) -> str: ...


class CraVerifier:
    """Combines post-action facts; deliberately exposes no effect/command API."""

    HEALTHY_CHECKS = {
        "stream_engine_ready": "TRUE",
        "ffmpeg_present": "TRUE",
        "tcp_flow_healthy": "TRUE",
        "upload_progress_healthy": "TRUE",
        "startup_gate": "TRUE",
        "target_stable": "TRUE",
        "tcp_stall": "FALSE",
        "network_down": "FALSE",
        "maintenance": "FALSE",
        "delivery_bad": "FALSE",
    }

    def __init__(self, store: VerifierStore) -> None:
        self._store = store

    def verify(
        self,
        *,
        pre: MonitoringEvidenceProjection,
        post: MonitoringEvidenceProjection,
        execution_evidence: dict[str, Any],
    ) -> str:
        integrity_reasons: list[str] = []
        failure_reasons: list[str] = []
        unknown_reasons: list[str] = []
        if pre.target_id != post.target_id:
            integrity_reasons.append("PROJECTION_TARGET_MISMATCH")
        if post.sequence <= pre.sequence:
            integrity_reasons.append("POST_PROJECTION_NOT_NEWER")
        if pre.value["source_instance_id"] != post.value["source_instance_id"]:
            integrity_reasons.append("MONITORING_SOURCE_CHANGED")
        if pre.value["source_release_id"] != post.value["source_release_id"]:
            integrity_reasons.append("MONITORING_RELEASE_CHANGED")
        if pre.incident["incident_id"] != post.incident["incident_id"]:
            integrity_reasons.append("MONITORING_INCIDENT_CHANGED")
        if pre.incident["source_episode_id"] != post.incident["source_episode_id"]:
            integrity_reasons.append("MONITORING_SOURCE_EPISODE_CHANGED")
        if parse_utc(str(post.value["expires_at"])) <= utc_now():
            integrity_reasons.append("POST_PROJECTION_EXPIRED")
        for name, ready in post.readiness.items():
            if not ready:
                unknown_reasons.append(f"POST_READINESS_{name.upper()}_FALSE")
        post_incident_state = str(post.incident["state"])
        if post_incident_state == "CONFIRMED":
            failure_reasons.append("POST_INCIDENT_STILL_CONFIRMED")
        elif post_incident_state != "CLEAR":
            unknown_reasons.append("POST_INCIDENT_NOT_CLEAR")
        before = self._target(
            execution_evidence.get("before_target"),
            "EXECUTION_BEFORE_TARGET_INVALID",
            integrity_reasons,
        )
        after = self._target(
            execution_evidence.get("after_target"),
            "EXECUTION_AFTER_TARGET_INVALID",
            integrity_reasons,
        )
        if before is not None:
            if before != pre.observed_target:
                integrity_reasons.append("PRE_PROJECTION_EXECUTION_TARGET_MISMATCH")
            if execution_evidence.get("effect_scope_id") != effect_scope_id("restart_ffmpeg", before):
                integrity_reasons.append("EFFECT_SCOPE_BINDING_MISMATCH")
        if after is not None and after != post.observed_target:
            integrity_reasons.append("POST_PROJECTION_EXECUTION_TARGET_MISMATCH")
        if before is not None and after is not None:
            if before.host_boot_id != after.host_boot_id or before.pod_uid != after.pod_uid:
                integrity_reasons.append("RECOVERY_FAILURE_DOMAIN_CHANGED")
            if before.ffmpeg_generation == after.ffmpeg_generation:
                failure_reasons.append("FFMPEG_GENERATION_NOT_CHANGED")
        if int(execution_evidence.get("physical_attempt_count") or 0) != 1:
            integrity_reasons.append("PHYSICAL_ATTEMPT_COUNT_NOT_ONE")
        for name, required in self.HEALTHY_CHECKS.items():
            check = post.checks.get(name)
            if check is None:
                unknown_reasons.append(f"POST_CHECK_{name.upper()}_MISSING")
            elif check["status"] != required:
                reason = f"POST_CHECK_{name.upper()}_NOT_{required}"
                if check["status"] == "UNKNOWN":
                    unknown_reasons.append(reason)
                else:
                    failure_reasons.append(reason)
        execution_state = str(execution_evidence.get("state") or "OUTCOME_UNKNOWN")
        if execution_state == "EFFECT_FAILED":
            failure_reasons.append("EXECUTION_EFFECT_FAILED")
        elif execution_state != "EFFECT_OBSERVED":
            unknown_reasons.append("EXECUTION_NOT_OBSERVED")
        if integrity_reasons:
            verdict = "UNKNOWN"
        elif execution_state == "EFFECT_FAILED":
            verdict = "FAILED"
        elif execution_state != "EFFECT_OBSERVED" or unknown_reasons:
            verdict = "UNKNOWN"
        elif failure_reasons:
            verdict = "FAILED"
        else:
            verdict = "RECOVERED"
        reasons = integrity_reasons + failure_reasons + unknown_reasons
        command_id = execution_evidence.get("command_id")
        local_action_id = execution_evidence.get("local_action_id")
        if (command_id is None) == (local_action_id is None):
            raise ValueError("execution evidence must bind exactly one command or local action")
        stable = hashlib.sha256(
            f"{command_id or local_action_id}:{post.projection_id}:{post.value['observation_revision']}".encode()
        ).hexdigest()[:32]
        value: dict[str, Any] = {
            "verifier_decision_id": f"cra-verification-{stable}",
            "command_id": command_id,
            "local_action_id": local_action_id,
            "effect_scope_id": str(execution_evidence.get("effect_scope_id") or ""),
            "pre_projection_id": pre.projection_id,
            "post_projection_id": post.projection_id,
            "verdict": verdict,
            "reason_codes": sorted(set(reasons)),
            "execution_evidence": execution_evidence,
            "decided_at": isoformat_utc(parse_utc(str(post.value["issued_at"]))),
        }
        return self._store.record_verifier_decision(value)

    @staticmethod
    def _target(value: object, reason: str, reasons: list[str]) -> TargetIdentity | None:
        if not isinstance(value, dict):
            reasons.append(reason)
            return None
        try:
            return TargetIdentity.from_dict(value)
        except (TypeError, ValueError):
            reasons.append(reason)
            return None
