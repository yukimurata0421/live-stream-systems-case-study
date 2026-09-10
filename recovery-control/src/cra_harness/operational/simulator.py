from __future__ import annotations

from typing import Any

from cra_harness.operational.model import OperationalScenario

CREDENTIAL_FAILURES = {"EXPIRED", "OLD_REVOKED", "REVOKED"}
TARGET_FAILURES = {"STALE", "UNSTABLE", "MISSING", "UNOBSERVABLE", "IDENTITY_CONFLICT"}


def simulate_candidate_policy(scenario: OperationalScenario) -> dict[str, Any]:
    """Run the v3 candidate policy against fake state only; no adapter is available here."""

    flags = scenario.flags
    target_valid = scenario.target == "VALID"
    credential_valid = scenario.credential not in CREDENTIAL_FAILURES
    monitoring_ready = scenario.monitoring == "FRESH"
    transport_can_deliver = scenario.network not in {"ASYMMETRIC_REQUEST_LOSS", "COMPLETE_PARTITION"}
    maintenance_blocks = scenario.maintenance != "NONE"
    external_mutation = flags["external_mutation"] or scenario.legacy_recovery == "EXTERNAL_MUTATION_OBSERVED"

    credential_reason = "NONE"
    if scenario.credential == "EXPIRED":
        credential_reason = "OBSERVATION_CREDENTIAL_EXPIRED"
    elif scenario.credential in {"OLD_REVOKED", "REVOKED"}:
        credential_reason = "VERIFY_KEY_UNKNOWN"
    elif scenario.credential == "ROTATING" and not flags["rotation_success"]:
        credential_reason = "SIGNING_KEY_UNAVAILABLE"

    heartbeat_renewed = monitoring_ready and credential_valid and target_valid and scenario.network != "COMPLETE_PARTITION"
    authority_after = scenario.authority
    permanent_fallback = False
    if scenario.maintenance == "ACTIVE":
        authority_after = "MAINTENANCE"
    elif scenario.maintenance == "EXITING" and not (target_valid and flags["reconciliation_complete"]):
        authority_after = "RECONCILING"
    elif not target_valid and scenario.authority == "CENTRAL_ACTIVE":
        authority_after = "CENTRAL_SUSPECT"
    elif not heartbeat_renewed and scenario.authority == "CENTRAL_ACTIVE":
        authority_after = "LOCAL_FALLBACK" if scenario.elapsed_ms >= 15_000 else "CENTRAL_SUSPECT"
        permanent_fallback = authority_after == "LOCAL_FALLBACK" and scenario.network != "JITTER"
    elif scenario.network == "JITTER" and scenario.elapsed_ms < 4_000:
        authority_after = scenario.authority

    maintenance_established = scenario.maintenance == "ACTIVE"
    if scenario.maintenance == "ENTERING" and flags["command_in_flight"]:
        maintenance_established = False
    if scenario.maintenance in {"ENTERING", "ACTIVE"} and scenario.legacy_recovery == "WOULD_MUTATE" and not flags["legacy_quiesced"]:
        maintenance_established = False

    command_state = "NONE"
    if flags["command_in_flight"]:
        command_state = "PENDING"
    if external_mutation or flags["target_mutation"]:
        command_state = "SUPERSEDED_RECONCILIATION_REQUIRED" if flags["command_in_flight"] else "NONE"
    elif not target_valid:
        command_state = "BLOCKED_TARGET_UNAVAILABLE"
    elif not credential_valid:
        command_state = "BLOCKED_CREDENTIAL_UNAVAILABLE"
    elif maintenance_blocks:
        command_state = "BLOCKED_MAINTENANCE"
    elif scenario.authority == "LOCAL_FALLBACK" and flags["late_central_command"]:
        command_state = "REJECTED_LOCAL_AUTHORITY_ACTIVE"
    elif scenario.authority in {"CENTRAL_SUSPECT", "RECONCILING", "SAFE_BLOCKED", "MAINTENANCE"}:
        command_state = "BLOCKED_AUTHORITY_NOT_ACTIVE"

    central_would_effect = 0
    local_would_effect = 0
    central_eligible = all(
        (
            scenario.authority == "CENTRAL_ACTIVE",
            authority_after == "CENTRAL_ACTIVE",
            target_valid,
            credential_valid,
            monitoring_ready,
            transport_can_deliver,
            not maintenance_blocks,
            not external_mutation,
            not flags["authorization_pending"],
            flags["confirmed_tcp_stall"],
            not flags["stream_healthy"],
        )
    )
    if central_eligible:
        central_would_effect = 1
        command_state = "WOULD_ACCEPT"
    local_fallback_eligible = all(
        (
            scenario.authority == "LOCAL_FALLBACK",
            target_valid,
            flags["local_evidence_valid"],
            flags["confirmed_tcp_stall"],
            not maintenance_blocks,
            not external_mutation,
            not flags["stream_healthy"],
        )
    )
    if local_fallback_eligible:
        local_would_effect = 1

    if scenario.authority == "LOCAL_FALLBACK":
        central_would_effect = 0
    if scenario.authority == "CENTRAL_ACTIVE":
        local_would_effect = 0
    if not target_valid or not credential_valid or maintenance_blocks or external_mutation:
        central_would_effect = 0
        local_would_effect = 0

    simulated_attempts = central_would_effect + local_would_effect
    physical_retry_count = 0
    outcome = "NOT_ATTEMPTED"
    if simulated_attempts:
        outcome = "OUTCOME_UNKNOWN" if flags["physical_outcome_unknown"] else "WOULD_EFFECT"
    if scenario.network == "ASYMMETRIC_RESPONSE_LOSS" and flags["command_in_flight"]:
        outcome = "OUTCOME_UNKNOWN"
        physical_retry_count = 0

    effect_source = "UNKNOWN"
    if external_mutation or scenario.legacy_recovery == "WOULD_MUTATE":
        effect_source = "EXTERNAL"
    elif local_would_effect:
        effect_source = "LOCAL"
    elif central_would_effect:
        effect_source = "CRA"

    fault_observed_ms = 0 if flags["confirmed_tcp_stall"] else None
    confirmed_ms = 2_000 if flags["confirmed_tcp_stall"] else None
    authority_transition_ms: int | None = None
    would_accept_ms: int | None = None
    if central_would_effect:
        authority_transition_ms = 0
        would_accept_ms = 2_025
    elif local_would_effect:
        authority_transition_ms = 15_000
        would_accept_ms = 15_525
    time_in_no_authority_action_window_ms = 0
    if authority_after in {"CENTRAL_SUSPECT", "RECONCILING", "SAFE_BLOCKED"}:
        time_in_no_authority_action_window_ms = min(max(scenario.elapsed_ms, 0), 45_000)

    return {
        "policy_revision": "operational-v3-candidate-b",
        "authority_before": scenario.authority,
        "authority_after": authority_after,
        "target_state": scenario.target,
        "monitoring_state": scenario.monitoring,
        "credential_state": scenario.credential,
        "credential_failure_classification": credential_reason,
        "network_state": scenario.network,
        "maintenance_state": scenario.maintenance,
        "legacy_recovery_state": scenario.legacy_recovery,
        "heartbeat_renewed": heartbeat_renewed,
        "maintenance_established": maintenance_established,
        "legacy_quiesce_required": scenario.legacy_recovery == "WOULD_MUTATE" and not flags["legacy_quiesced"],
        "command_state": command_state,
        "central_automatic_would_effect_count": central_would_effect,
        "local_fallback_would_effect_count": local_would_effect,
        "simulated_effect_attempt_count": simulated_attempts,
        "physical_retry_count": physical_retry_count,
        "simulated_outcome": outcome,
        "old_target_command_effect_count": 0,
        "local_fallback_eligible_eventually": local_fallback_eligible,
        "permanent_fallback": permanent_fallback,
        "authority_continuity_unnecessarily_lost": (
            scenario.credential == "ROTATING"
            and flags["rotation_success"]
            and target_valid
            and monitoring_ready
            and scenario.network != "COMPLETE_PARTITION"
            and scenario.maintenance == "NONE"
            and authority_after != scenario.authority
        ),
        "unnecessary_restart_count": 0,
        "effect_source": effect_source,
        "cra_verified_success": effect_source == "CRA" and outcome == "WOULD_EFFECT",
        "verification_shadow_verdict": "UNKNOWN" if effect_source in {"EXTERNAL", "UNKNOWN"} else "WOULD_VERIFY",
        "verification_unknown_reason": "EFFECT_SOURCE_EXTERNAL" if effect_source == "EXTERNAL" else None,
        "fault_observed_ms": fault_observed_ms,
        "confirmed_ms": confirmed_ms,
        "authority_transition_ms": authority_transition_ms,
        "would_accept_ms": would_accept_ms,
        "simulated_time_to_recovery_ms": would_accept_ms,
        "time_in_no_authority_action_window_ms": time_in_no_authority_action_window_ms,
        "production_mutation_count": 0,
        "physical_attempt_count": 0,
        "ffmpeg_signal_count": 0,
        "pod_mutation_count": 0,
        "deployment_mutation_count": 0,
        "host_restart_count": 0,
        "durable_accept_recorded": flags["durable_accept_recorded"],
    }
