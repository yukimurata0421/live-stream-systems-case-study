from __future__ import annotations

from collections.abc import Mapping
from typing import Any

EXACT_TARGET_FIELDS = {
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "container_name",
    "container_id",
    "ffmpeg_generation",
    "ffmpeg_pid",
}


def expected_effect_admission(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Independent expected-value generator; imports no runtime SUT code."""

    if fixture.get("operation") != "restart_ffmpeg":
        return {"accepted": False, "reason": "OPERATION_NOT_ALLOWED", "effect_count": 0}
    if bool(fixture.get("expired")):
        return {"accepted": False, "reason": "REQUEST_EXPIRED", "effect_count": 0}
    request_target = fixture.get("request_target")
    current_target = fixture.get("current_target")
    if not isinstance(request_target, Mapping) or set(request_target) != EXACT_TARGET_FIELDS:
        return {"accepted": False, "reason": "TARGET_IDENTITY_NOT_EXACT", "effect_count": 0}
    if request_target != current_target or fixture.get("target_snapshot_status") != "VALID":
        return {"accepted": False, "reason": "TARGET_IDENTITY_DRIFT", "effect_count": 0}
    if fixture.get("request_pid_namespace") != "host" or fixture.get("current_pid_namespace") != "host":
        return {"accepted": False, "reason": "PID_NAMESPACE_MISMATCH", "effect_count": 0}
    if fixture.get("expected_executor_instance") != fixture.get("current_executor_instance"):
        return {"accepted": False, "reason": "EXECUTOR_INSTANCE_DRIFT", "effect_count": 0}
    if fixture.get("expected_ffmpeg_generation") != fixture.get("current_ffmpeg_generation"):
        return {"accepted": False, "reason": "FFMPEG_GENERATION_DRIFT", "effect_count": 0}
    if fixture.get("producer_id") != fixture.get("active_producer_id"):
        return {"accepted": False, "reason": "PRODUCER_NOT_ACTIVE", "effect_count": 0}
    if fixture.get("producer_generation") != fixture.get("active_producer_generation"):
        return {"accepted": False, "reason": "PRODUCER_GENERATION_MISMATCH", "effect_count": 0}
    duplicate_state = str(fixture.get("duplicate_state") or "")
    if duplicate_state:
        return {
            "accepted": duplicate_state == "ACCEPTED",
            "reason": "IDEMPOTENT_REPLAY",
            "effect_count": 1 if duplicate_state == "ACCEPTED" else 0,
        }
    if bool(fixture.get("crash_after_effect_started")):
        return {"accepted": True, "reason": "OUTCOME_UNKNOWN", "effect_count": 0, "automatic_retry": False}
    return {"accepted": True, "reason": "EFFECT_OBSERVED", "effect_count": 1}


def expected_handoff(fixture: Mapping[str, Any]) -> dict[str, Any]:
    if int(fixture.get("active_producer_count", 0)) != 1:
        return {"accepted": False, "reason": "SINGLE_ACTIVE_PRODUCER_VIOLATION"}
    if int(fixture.get("unresolved_count", 0)) != 0:
        return {"accepted": False, "reason": "UNRESOLVED_EXECUTION_EXISTS"}
    if fixture.get("expected_producer_id") != fixture.get("active_producer_id"):
        return {"accepted": False, "reason": "EXPECTED_AUTHORITY_MISMATCH"}
    if fixture.get("expected_generation") != fixture.get("active_producer_generation"):
        return {"accepted": False, "reason": "EXPECTED_AUTHORITY_MISMATCH"}
    return {"accepted": True, "reason": "AUTHORITY_SWITCHED"}
