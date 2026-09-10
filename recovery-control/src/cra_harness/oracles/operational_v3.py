from __future__ import annotations

from typing import Any

CREDENTIAL_FAILURES = {"EXPIRED", "OLD_REVOKED", "REVOKED"}


def evaluate_operational_invariants(scenario: dict[str, Any], observation: dict[str, Any]) -> list[dict[str, Any]]:
    """Independent invariant oracle over serialized input and raw observation."""

    violations: list[dict[str, Any]] = []

    def require(invariant_id: str, condition: bool, evidence: str) -> None:
        if not condition:
            violations.append({"invariant_id": invariant_id, "evidence": evidence})

    mutation_fields = (
        "production_mutation_count",
        "physical_attempt_count",
        "ffmpeg_signal_count",
        "pod_mutation_count",
        "deployment_mutation_count",
        "host_restart_count",
    )
    require("OSV3-SAFETY-NO-PHYSICAL", all(observation.get(name) == 0 for name in mutation_fields), "mutation counters")
    if scenario["maintenance"] == "ACTIVE":
        require(
            "OSV3-MAINTENANCE-NO-AUTOMATIC",
            observation.get("central_automatic_would_effect_count") == 0 and observation.get("local_fallback_would_effect_count") == 0,
            "maintenance automatic action counts",
        )
    if scenario["maintenance"] == "ENTERING" and scenario["flags"]["command_in_flight"]:
        require(
            "OSV3-MAINTENANCE-ENTERING-DRAIN",
            observation.get("maintenance_established") is False,
            "maintenance established with in-flight command",
        )
    if scenario["maintenance"] == "EXITING" and not (scenario["target"] == "VALID" and scenario["flags"]["reconciliation_complete"]):
        require(
            "OSV3-MAINTENANCE-EXIT-RECONCILE",
            observation.get("authority_after") == "RECONCILING",
            "authority after maintenance exit",
        )
    if scenario["target"] != "VALID":
        require(
            "OSV3-TARGET-NOT-VALID-NO-ACTION",
            observation.get("central_automatic_would_effect_count") == 0 and observation.get("local_fallback_would_effect_count") == 0,
            "target unavailable action counts",
        )
    if scenario["credential"] in CREDENTIAL_FAILURES:
        require(
            "OSV3-CREDENTIAL-FAILURE-NO-ACTION",
            observation.get("simulated_effect_attempt_count") == 0 and observation.get("physical_attempt_count") == 0,
            "credential failure action counts",
        )
    external = scenario["flags"]["external_mutation"] or scenario["legacy_recovery"] == "EXTERNAL_MUTATION_OBSERVED"
    if external:
        require(
            "OSV3-EXTERNAL-MUTATION-SUPERSEDE",
            observation.get("old_target_command_effect_count") == 0,
            "old target command effect count",
        )
    if scenario["authority"] == "LOCAL_FALLBACK":
        require(
            "OSV3-LOCAL-FALLBACK-FENCES-CENTRAL",
            observation.get("central_automatic_would_effect_count") == 0,
            "central effect while local fallback",
        )
    if scenario["authority"] == "CENTRAL_ACTIVE":
        require(
            "OSV3-CENTRAL-ACTIVE-FENCES-LOCAL",
            observation.get("local_fallback_would_effect_count") == 0,
            "local effect while central active",
        )
    if scenario["flags"]["physical_outcome_unknown"] or scenario["network"] == "ASYMMETRIC_RESPONSE_LOSS":
        require(
            "OSV3-UNCERTAIN-OUTCOME-NO-RETRY",
            observation.get("physical_retry_count") == 0,
            "physical retry count",
        )
    if observation.get("effect_source") == "EXTERNAL":
        require(
            "OSV3-EXTERNAL-NOT-CRA-SUCCESS",
            observation.get("cra_verified_success") is False,
            "external effect attribution",
        )
    if scenario["flags"]["stream_healthy"]:
        require(
            "OSV3-HEALTHY-STREAM-NO-RESTART",
            observation.get("unnecessary_restart_count") == 0,
            "healthy stream restart count",
        )
    local_should_eventually_be_eligible = all(
        (
            scenario["authority"] == "LOCAL_FALLBACK",
            scenario["target"] == "VALID",
            scenario["flags"]["local_evidence_valid"],
            scenario["flags"]["confirmed_tcp_stall"],
            scenario["maintenance"] == "NONE",
            not external,
            not scenario["flags"]["stream_healthy"],
        )
    )
    if local_should_eventually_be_eligible:
        require(
            "OSV3-LOCAL-FALLBACK-EVENTUAL-ELIGIBILITY",
            observation.get("local_fallback_eligible_eventually") is True,
            "local fallback eligibility",
        )
    if scenario["network"] == "JITTER" and scenario["elapsed_ms"] < 4_000:
        require(
            "OSV3-BOUNDED-JITTER-NO-PERMANENT-FALLBACK",
            observation.get("permanent_fallback") is False,
            "permanent fallback under bounded jitter",
        )
    if scenario["credential"] == "ROTATING" and scenario["flags"]["rotation_success"]:
        require(
            "OSV3-ROTATION-CONTINUITY",
            observation.get("authority_continuity_unnecessarily_lost") is False,
            "authority continuity during successful rotation",
        )
    return violations


def detect_negative_control(control: dict[str, Any]) -> list[dict[str, Any]]:
    observed = dict(control["injected_observation"])
    violations: list[dict[str, Any]] = []

    def violation(invariant_id: str, broken: bool, evidence: str) -> None:
        if broken:
            violations.append({"invariant_id": invariant_id, "evidence": evidence})

    violation("NC-01-EXACTLY-ONCE", int(observed.get("physical_effect_count", 0)) > 1, "physical_effect_count")
    violation("NC-02-STALE-TARGET", bool(observed.get("stale_target_effect")), "stale_target_effect")
    violation("NC-03-WRITE-AHEAD", bool(observed.get("effect_before_durable_accept")), "effect_before_durable_accept")
    violation("NC-04-SINGLE-AUTHORITY", bool(observed.get("dual_authority_active")), "dual_authority_active")
    violation("NC-05-UNKNOWN-NO-RETRY", int(observed.get("outcome_unknown_retry_count", 0)) > 0, "outcome_unknown_retry_count")
    violation("NC-06-RESTORE-NO-REPLAY", int(observed.get("restored_pending_replay_count", 0)) > 0, "restored_pending_replay_count")
    violation("NC-07-FALLBACK-FENCE", int(observed.get("central_attempt_after_fallback", 0)) > 0, "central_attempt_after_fallback")
    violation(
        "NC-08-RECONCILIATION-REQUIRED",
        bool(observed.get("central_active_without_reconciliation")),
        "central_active_without_reconciliation",
    )
    violation(
        "NC-09-MONITORING-READINESS",
        bool(observed.get("heartbeat_renewed_while_monitoring_stale")),
        "heartbeat_renewed_while_monitoring_stale",
    )
    violation("NC-10-RESTORE-PENDING", int(observed.get("restored_pending_command_count", 0)) > 0, "restored_pending_command_count")
    violation(
        "NC-11-MAINTENANCE-ESCAPE",
        bool(observed.get("maintenance_central_command_accepted")),
        "maintenance_central_command_accepted",
    )
    violation(
        "NC-12-TARGET-UNAVAILABLE",
        int(observed.get("target_unobservable_physical_attempt", 0)) > 0,
        "target_unobservable_physical_attempt",
    )
    violation("NC-13-EXPIRED-CREDENTIAL", bool(observed.get("expired_observer_heartbeat_renewed")), "expired_observer_heartbeat_renewed")
    violation(
        "NC-14-ASYMMETRIC-RETRY",
        int(observed.get("receipt_lost_physical_effect_count", 0)) > 1,
        "receipt_lost_physical_effect_count",
    )
    violation(
        "NC-15-EFFECT-PROVENANCE",
        bool(observed.get("external_effect_recorded_as_cra_success")),
        "external_effect_recorded_as_cra_success",
    )
    return violations
