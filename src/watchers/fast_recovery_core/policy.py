from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

RESTART_FFMPEG = "RESTART_FFMPEG"
RECONCILE_FFMPEG = "RECONCILE_FFMPEG"
ESCALATE_RUNTIME_RECOVERY = "ESCALATE_RUNTIME_RECOVERY"
NO_ACTION = "NO_ACTION"


@dataclass(frozen=True)
class RecoveryIntentDecision:
    reason_kind: str
    failure_domain: str
    intent_type: str
    target_type: str
    effect_owner: str
    effect_scope: str
    automatic_retry: bool
    verification: str
    decision_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _decision(
    reason_kind: str,
    *,
    failure_domain: str,
    intent_type: str,
    target_type: str,
    effect_owner: str,
    effect_scope: str,
    verification: str,
    decision_reason: str,
) -> RecoveryIntentDecision:
    return RecoveryIntentDecision(
        reason_kind=reason_kind,
        failure_domain=failure_domain,
        intent_type=intent_type,
        target_type=target_type,
        effect_owner=effect_owner,
        effect_scope=effect_scope,
        automatic_retry=False,
        verification=verification,
        decision_reason=decision_reason,
    )


def select_recovery_intent(
    reason_kind: str,
    *,
    runtime_observation: Mapping[str, Any] | None = None,
) -> RecoveryIntentDecision:
    """Map an already-confirmed legacy policy reason to a typed physical intent.

    Cooldown, budget, confirmation and authority gates remain owned by the
    existing Fast Recovery loop. This function cannot grant execution.
    """

    observation = runtime_observation or {}
    if reason_kind in {"tcp_stall", "remote_warning"}:
        return _decision(
            reason_kind,
            failure_domain="DELIVERY_PATH",
            intent_type=RESTART_FFMPEG,
            target_type="FfmpegTargetIdentity",
            effect_owner="stream-engine-effect-executor",
            effect_scope="ffmpeg_child",
            verification="new exact FFmpeg generation and positive transport progress",
            decision_reason="legacy k8s policy terminates the current FFmpeg child",
        )
    if reason_kind == "ffmpeg_missing":
        runtime_valid = (
            str(observation.get("runtime_snapshot_status") or "") == "VALID"
            and isinstance(observation.get("runtime_identity"), Mapping)
        )
        lifecycle = str(observation.get("runtime_lifecycle_state") or "UNKNOWN")
        cardinality = str(observation.get("managed_child_cardinality") or "UNKNOWN")
        if runtime_valid and lifecycle == "RESTART_DELAY" and cardinality == "0":
            return _decision(
                reason_kind,
                failure_domain="FFMPEG_LOCAL",
                intent_type=RECONCILE_FFMPEG,
                target_type="RuntimeIdentity",
                effect_owner="stream-engine-effect-executor",
                effect_scope="ffmpeg_child_convergence",
                verification="exactly one new managed FFmpeg child observed",
                decision_reason="owner runtime is stable and waiting to restart a missing child",
            )
        if runtime_valid and cardinality == "1":
            return _decision(
                reason_kind,
                failure_domain="OBSERVATION_SKEW",
                intent_type=NO_ACTION,
                target_type="RuntimeIdentity",
                effect_owner="none",
                effect_scope="none",
                verification="refresh observation",
                decision_reason="runtime already owns one managed FFmpeg child",
            )
        if runtime_valid and lifecycle in {"INITIALIZING", "STARTING_FFMPEG", "CONNECTIVITY_WAIT", "STOPPING"}:
            return _decision(
                reason_kind,
                failure_domain="STARTUP_OR_CONNECTIVITY_TRANSIENT",
                intent_type=NO_ACTION,
                target_type="RuntimeIdentity",
                effect_owner="none",
                effect_scope="none",
                verification="wait for lifecycle transition and fresh observation",
                decision_reason=f"runtime lifecycle {lifecycle} is not a reconcile boundary",
            )
        if runtime_valid and cardinality not in {"2+", "UNKNOWN"}:
            return _decision(
                reason_kind,
                failure_domain="RUNTIME_LOCAL",
                intent_type=ESCALATE_RUNTIME_RECOVERY,
                target_type="RuntimeIdentity",
                effect_owner="legacy-runtime-recovery-adapter",
                effect_scope="runtime",
                verification="new runtime and FFmpeg identity with transport progress",
                decision_reason="missing child cannot be safely reconciled by the narrow executor",
            )
        return _decision(
            reason_kind,
            failure_domain="UNKNOWN",
            intent_type=NO_ACTION,
            target_type="RuntimeIdentity",
            effect_owner="none",
            effect_scope="none",
            verification="obtain fresh runtime identity and exact child cardinality",
            decision_reason="runtime identity or child cardinality is unavailable/unsafe",
        )
    if reason_kind == "network_down":
        return _decision(
            reason_kind,
            failure_domain="UNKNOWN_NETWORK",
            intent_type=NO_ACTION,
            target_type="none",
            effect_owner="none",
            effect_scope="none",
            verification="connectivity recovery observation",
            decision_reason="production branch persists connectivity wait and returns without mutation",
        )
    if reason_kind in {"low_upload_pressure", "healthy", "startup_transient", "target_unavailable", ""}:
        return _decision(
            reason_kind,
            failure_domain="NONE_OR_INSUFFICIENT_EVIDENCE",
            intent_type=NO_ACTION,
            target_type="none",
            effect_owner="none",
            effect_scope="none",
            verification="continued observation",
            decision_reason="legacy policy does not select a physical recovery effect",
        )
    return _decision(
        reason_kind,
        failure_domain="UNKNOWN",
        intent_type=NO_ACTION,
        target_type="none",
        effect_owner="none",
        effect_scope="none",
        verification="policy inventory update required",
        decision_reason="UNMODELED_POLICY_PATH",
    )


def implementation_matrix() -> list[dict[str, Any]]:
    """Stable rows consumed by the policy/spec consistency gate."""

    fixtures: list[tuple[str, dict[str, Any]]] = [
        ("tcp_stall", {}),
        ("remote_warning", {}),
        (
            "ffmpeg_missing",
            {
                "runtime_snapshot_status": "VALID",
                "runtime_identity": {"present": True},
                "runtime_lifecycle_state": "RESTART_DELAY",
                "managed_child_cardinality": "0",
            },
        ),
        ("network_down", {}),
        ("low_upload_pressure", {}),
        ("healthy", {}),
        ("startup_transient", {}),
        ("target_unavailable", {}),
    ]
    return [select_recovery_intent(reason, runtime_observation=observation).to_dict() for reason, observation in fixtures]
