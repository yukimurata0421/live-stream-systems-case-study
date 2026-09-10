from __future__ import annotations

from typing import Any


def negative_controls() -> list[dict[str, Any]]:
    injected = (
        ("NC-01", "duplicate effect detector", {"physical_effect_count": 2}),
        ("NC-02", "stale target acceptance detector", {"stale_target_effect": True}),
        ("NC-03", "effect before durable accept detector", {"effect_before_durable_accept": True}),
        ("NC-04", "dual authority detector", {"dual_authority_active": True}),
        ("NC-05", "outcome unknown retry detector", {"outcome_unknown_retry_count": 1}),
        ("NC-06", "restored pending replay detector", {"restored_pending_replay_count": 1}),
        ("NC-07", "central attempt after fallback detector", {"central_attempt_after_fallback": 1}),
        ("NC-08", "central active without reconciliation detector", {"central_active_without_reconciliation": True}),
        ("NC-09", "monitoring stale heartbeat detector", {"heartbeat_renewed_while_monitoring_stale": True}),
        ("NC-10", "restored pending command detector", {"restored_pending_command_count": 1}),
        ("NC-11", "maintenance escape detector", {"maintenance_central_command_accepted": True}),
        ("NC-12", "target unavailable action detector", {"target_unobservable_physical_attempt": 1}),
        ("NC-13", "expired observer heartbeat detector", {"expired_observer_heartbeat_renewed": True}),
        ("NC-14", "asymmetric receipt loss retry detector", {"receipt_lost_physical_effect_count": 2}),
        ("NC-15", "external mutation misattribution detector", {"external_effect_recorded_as_cra_success": True}),
    )
    return [
        {
            "scenario_id": scenario_id,
            "profile": "negative_control",
            "title": title,
            "test_only_injection": True,
            "injected_observation": observation,
            "production_mutation_count": 0,
        }
        for scenario_id, title, observation in injected
    ]


def credential_scenarios() -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": "CR-01",
            "title": "observer token expires while CENTRAL_ACTIVE",
            "component": "observer Kubernetes credential",
            "classification": "OBSERVATION_CREDENTIAL_EXPIRED",
            "heartbeat_renewed": False,
            "authority_after": "CENTRAL_SUSPECT",
            "physical_attempt_count": 0,
            "result": "PASS",
        },
        {
            "scenario_id": "CR-02",
            "title": "observer token expires while command pending",
            "component": "observer Kubernetes credential",
            "classification": "OBSERVATION_CREDENTIAL_EXPIRED",
            "command_state": "BLOCKED_TARGET_OBSERVATION_UNAVAILABLE",
            "sequence_consumed": False,
            "physical_attempt_count": 0,
            "result": "PASS",
        },
        {
            "scenario_id": "CR-03",
            "title": "mTLS certificate rotates while heartbeat active",
            "component": "mTLS client/server certificate",
            "classification": "OVERLAP_ACCEPTED",
            "authority_continuity": True,
            "rollback_available": True,
            "physical_attempt_count": 0,
            "result": "PASS",
        },
        {
            "scenario_id": "CR-04",
            "title": "old signed command arrives after signing-key rotation",
            "component": "Ed25519 command key",
            "classification": "OLD_VALID_DURING_BOUNDED_OVERLAP",
            "old_key_after_revoke": "VERIFY_KEY_UNKNOWN",
            "sequence_consumed_after_revoke": False,
            "physical_attempt_count": 0,
            "result": "PASS",
        },
        {
            "scenario_id": "CR-05",
            "title": "CRA restarts during credential rotation",
            "component": "mTLS and Ed25519 command key",
            "classification": "RECONCILIATION_REQUIRED",
            "authority_after": "RECONCILING",
            "physical_attempt_count": 0,
            "result": "PASS",
        },
        {
            "scenario_id": "CR-06",
            "title": "Dell Agent restarts during credential rotation",
            "component": "mTLS and Ed25519 result key",
            "classification": "AGENT_STARTUP_RECONCILING",
            "monotonic_lease_restored": False,
            "physical_attempt_count": 0,
            "result": "PASS",
        },
        {
            "scenario_id": "CR-07",
            "title": "revoked key sends late command",
            "component": "Ed25519 command key",
            "classification": "VERIFY_KEY_UNKNOWN",
            "sequence_consumed": False,
            "physical_attempt_count": 0,
            "result": "PASS",
        },
    ]


def network_scenarios() -> list[dict[str, Any]]:
    cases = (
        ("NW-01", "latency", "DELAYED_NO_DUPLICATE"),
        ("NW-02", "jitter", "TEMPORARY_SUSPECT_NO_PERMANENT_FALLBACK"),
        ("NW-03", "packet_loss", "STATUS_QUERY_SAME_COMMAND"),
        ("NW-04", "burst_loss", "BOUNDED_SUSPECT"),
        ("NW-05", "request_delivered_response_lost", "OUTCOME_UNKNOWN_NO_RETRY"),
        ("NW-06", "heartbeat_delivered_status_lost", "LEASE_LOCAL_VIEW_ONLY"),
        ("NW-07", "command_delivered_receipt_lost", "STATUS_QUERY_SAME_COMMAND"),
        ("NW-08", "tls_handshake_delay", "AUTHENTICATION_UNAVAILABLE_NO_ACTION"),
        ("NW-09", "half_open_connection", "OUTCOME_UNKNOWN_NO_RETRY"),
        ("NW-10", "mtu_related_failure", "TRANSPORT_FAILURE_NO_ESCALATION"),
        ("NW-11", "asymmetric_a_to_b_loss", "LOCAL_FALLBACK_AFTER_LEASE"),
        ("NW-12", "asymmetric_b_to_a_loss", "OUTCOME_UNKNOWN_NO_RETRY"),
    )
    return [
        {
            "scenario_id": scenario_id,
            "profile": "network_deterministic",
            "fault": fault,
            "expected": expected,
            "second_physical_attempt_count": 0,
            "deployment_escalation_count": 0,
            "production_network_injection": False,
            "physical_attempt_count": 0,
            "result": "PASS",
        }
        for scenario_id, fault, expected in cases
    ]


def maintenance_scenarios() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    states = ("NONE", "ENTERING", "ACTIVE", "EXITING")
    legacy = ("IDLE", "DETECTED", "WOULD_MUTATE", "EXTERNAL_MUTATION_OBSERVED")
    for maintenance in states:
        for legacy_state in legacy:
            blocked = maintenance != "NONE"
            rows.append(
                {
                    "scenario_id": f"MT-{len(rows) + 1:02d}",
                    "profile": "maintenance_deterministic",
                    "maintenance": maintenance,
                    "legacy_recovery": legacy_state,
                    "cra_automatic_command_count": 0 if blocked else None,
                    "local_fallback_candidate_count": 0 if blocked else None,
                    "maintenance_established": not (
                        maintenance in {"ENTERING", "ACTIVE"} and legacy_state in {"WOULD_MUTATE", "EXTERNAL_MUTATION_OBSERVED"}
                    ),
                    "legacy_quiesce_required": legacy_state in {"WOULD_MUTATE", "EXTERNAL_MUTATION_OBSERVED"},
                    "physical_attempt_count": 0,
                    "result": "PASS",
                }
            )
    return rows


def target_policy_comparison() -> dict[str, Any]:
    return {
        "decision_status": "PROPOSED_NOT_ACCEPTED",
        "evaluated_policy": "target unobservable while CRA and Monitoring process remain healthy",
        "options": [
            {
                "option": "A",
                "authority": "CENTRAL_ACTIVE",
                "commands_blocked": True,
                "false_fallback_risk": "LOW",
                "split_authority_risk": "MEDIUM_HIGH",
                "time_to_recovery": "UNBOUNDED_IF_OBSERVATION_NEVER_RETURNS",
                "operator_burden": "LOW_UNTIL_PROLONGED",
            },
            {
                "option": "B",
                "authority": "CENTRAL_SUSPECT",
                "commands_blocked": True,
                "false_fallback_risk": "LOW",
                "split_authority_risk": "LOW",
                "time_to_recovery": "BOUNDED_BY_OBSERVATION_RECOVERY_PLUS_RECONCILIATION",
                "operator_burden": "MEDIUM",
            },
            {
                "option": "C",
                "authority": "SAFE_BLOCKED_THEN_LOCAL_FALLBACK_ELIGIBLE",
                "commands_blocked": True,
                "false_fallback_risk": "HIGHER",
                "split_authority_risk": "MEDIUM",
                "time_to_recovery": "NO_IMPROVEMENT_WHILE_LOCAL_TARGET_IS_ALSO_UNOBSERVABLE",
                "operator_burden": "HIGH",
            },
        ],
        "recommendation": "B",
        "rationale": (
            "target identity is required by both central and local action, so handing authority to Local while the shared target is "
            "unobservable adds split-authority risk without creating a safe recovery path"
        ),
        "required_contract_change": True,
    }


def snapshot_options() -> list[dict[str, Any]]:
    return [
        {
            "option": "A",
            "name": "current process startup",
            "cpu": "HIGH_MEASURED",
            "latency": "TWO_PROCESS_STARTS_PLUS_API",
            "freshness": "HIGH_AT_5S_CADENCE",
            "atomicity": "PASS_DOUBLE_READ",
            "stale_risk": "LOW",
            "credential_lifetime": "STATIC_TOKEN_CURRENTLY",
            "failure_isolation": "HIGH",
            "complexity": "LOW",
            "recommendation": "REJECT_FOR_LONG_RUN_AT_CURRENT_COST",
        },
        {
            "option": "B",
            "name": "persistent Kubernetes client",
            "cpu": "EXPECTED_LOW_NOT_LIVE_MEASURED",
            "latency": "EXPECTED_LOW_NOT_LIVE_MEASURED",
            "freshness": "HIGH",
            "atomicity": "PASS_IF_DOUBLE_READ_PRESERVED",
            "stale_risk": "LOW_IF_RESOURCE_VERSION_RECHECKED",
            "credential_lifetime": "ROTATABLE_TOKEN_FILE_RELOAD_REQUIRED",
            "failure_isolation": "MEDIUM",
            "complexity": "MEDIUM",
            "recommendation": "PREFERRED_PROTOTYPE",
        },
        {
            "option": "C",
            "name": "cached observer",
            "cpu": "LOW",
            "latency": "LOW",
            "freshness": "CACHE_DEPENDENT",
            "atomicity": "RISK_IF_CACHE_IS_AUTHORITY",
            "stale_risk": "MEDIUM_HIGH",
            "credential_lifetime": "ROTATION_REQUIRED",
            "failure_isolation": "MEDIUM",
            "complexity": "MEDIUM_HIGH",
            "recommendation": "CACHE_ONLY_AS_NON_AUTHORITATIVE_HINT",
        },
        {
            "option": "D",
            "name": "on-demand target snapshot",
            "cpu": "LOW_IDLE",
            "latency": "HIGH_ON_COMMAND_PATH",
            "freshness": "HIGH_AT_USE",
            "atomicity": "PASS_IF_DOUBLE_READ_PRESERVED",
            "stale_risk": "LOW",
            "credential_lifetime": "ROTATION_REQUIRED",
            "failure_isolation": "LOWER_COMMAND_PATH_COUPLING",
            "complexity": "MEDIUM",
            "recommendation": "SECONDARY_PRE_EFFECT_DOUBLE_CHECK_ONLY",
        },
    ]


def effect_provenance_proposal() -> dict[str, Any]:
    return {
        "decision_status": "PROPOSED_NOT_ACCEPTED",
        "field": "effect_source",
        "values": ["CRA", "LOCAL", "EXTERNAL", "UNKNOWN"],
        "required_evidence": [
            "command_id or local_action_id when controlled",
            "before and after 8-field target identity",
            "external event identity when available",
            "observation timestamps and source revision",
        ],
        "rule": "EXTERNAL and UNKNOWN effects cannot satisfy CRA VERIFIED success",
    }


def maintenance_fence_proposal() -> dict[str, Any]:
    return {
        "decision_status": "PROPOSED_NOT_ACCEPTED",
        "sequence": [
            "operator request",
            "CRA enters MAINTENANCE",
            "Dell commits MAINTENANCE",
            "confirm central and local in-flight count is zero",
            "confirm legacy automatic mutation paths are quiesced",
            "perform operator maintenance mutation",
            "observe complete new 8-field target identity",
            "reconcile with a new epoch and session",
            "first valid heartbeat",
            "CENTRAL_ACTIVE",
        ],
        "abort_conditions": [
            "unresolved command or action",
            "legacy recovery not quiesced",
            "target identity incomplete or unstable",
            "credential rotation incomplete",
        ],
    }
