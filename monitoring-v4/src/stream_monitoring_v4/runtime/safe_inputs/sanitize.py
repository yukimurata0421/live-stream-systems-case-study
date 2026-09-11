from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

from stream_contracts.monitoring_v4.time import parse_utc, utc_text
from stream_monitoring_v4.adapters.audio import AUDIO_EVIDENCE_ALLOWLIST
from stream_monitoring_v4.adapters.external import REASON_ALLOWLIST, TARGET_ALLOWLIST
from stream_monitoring_v4.adapters.monitoring import CHECK_ALLOWLIST
from stream_monitoring_v4.adapters.rendering import MAP_CONDITIONS
from stream_monitoring_v4.adapters.youtube import (
    RESOLVER_KEYS,
    WATCHDOG_DELIVERY_KEYS,
    WATCHDOG_INPUT_QUALITY_KEYS,
    WATCHDOG_LIFECYCLE_KEYS,
)
from stream_monitoring_v4.reliability.projector import FORMAL_OBJECTIVES

from .constants import REASON_PATTERN, TOKEN_PATTERN


def mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def token(value: Any, *, default: str = "source_value_unparseable") -> str:
    if value is None or value == "":
        return ""
    if not isinstance(value, str):
        return default
    raw = value.strip()
    return raw if TOKEN_PATTERN.fullmatch(raw) else (default if raw else "")


def timestamp(value: Any) -> str:
    if not isinstance(value, str):
        return "invalid-source-timestamp" if value is not None else ""
    try:
        return utc_text(parse_utc(value, field="source timestamp"))
    except (TypeError, ValueError):
        return "invalid-source-timestamp" if value.strip() else ""


def reason_list(
    value: Any,
    *,
    allowed: frozenset[str] | None = None,
) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value[:32]:
        raw = item.strip().lower() if isinstance(item, str) else ""
        if REASON_PATTERN.fullmatch(raw) and (allowed is None or raw in allowed):
            result.append(raw)
        elif raw or item is not None:
            result.append("source_reason_unparseable")
    return sorted(set(result))


def primitives(payload: Mapping[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool) or value is None:
            result[key] = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            result[key] = value
        elif isinstance(value, str):
            result[key] = token(value)
        elif isinstance(value, list):
            result[key] = reason_list(value)
    return result


def youtube_watchdog(payload: Mapping[str, Any]) -> dict[str, Any]:
    keys = tuple(
        dict.fromkeys(
            WATCHDOG_LIFECYCLE_KEYS
            + WATCHDOG_DELIVERY_KEYS
            + WATCHDOG_INPUT_QUALITY_KEYS
        )
    )
    result = primitives(payload, keys)
    for key in (
        "remote_probe_ts_utc",
        "data_api_checked_ts_utc",
        "oauth_checked_ts_utc",
        "stats_file_updated_at_utc",
        "ts_utc",
    ):
        if key in payload:
            result[key] = timestamp(payload.get(key))
    details: list[dict[str, str]] = []
    raw_details = payload.get("oauth_stream_health_issue_details")
    if isinstance(raw_details, list):
        for value in raw_details[:32]:
            item = mapping(value)
            issue_type = token(item.get("type"))
            severity = str(item.get("severity", "")).strip().lower()
            if severity not in {"info", "warning", "error"}:
                severity = "source_severity_unparseable" if severity else ""
            if issue_type or severity:
                details.append({"type": issue_type, "severity": severity})
    if details:
        result["oauth_stream_health_issue_details"] = details
    return result


def youtube_resolver(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = primitives(payload, RESOLVER_KEYS)
    result["ts_utc"] = timestamp(payload.get("ts_utc"))
    return result


def map_runtime(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = primitives(
        payload,
        ("schema", "status", "delivery_critical_ok", "weather_ok"),
    )
    result["checked_at_utc"] = timestamp(payload.get("checked_at_utc"))
    conditions = mapping(payload.get("conditions"))
    result["conditions"] = {
        name: conditions[name]
        for name in MAP_CONDITIONS
        if isinstance(conditions.get(name), bool)
    }
    allowed_reasons = frozenset(MAP_CONDITIONS)
    result["critical_reasons"] = reason_list(
        payload.get("critical_reasons"),
        allowed=allowed_reasons,
    )
    result["weather_reasons"] = reason_list(
        payload.get("weather_reasons"),
        allowed=allowed_reasons,
    )
    raw_errors = payload.get("probe_errors")
    result["probe_errors"] = ["redacted"] * min(
        100,
        len(raw_errors) if isinstance(raw_errors, list) else 0,
    )
    return result


def subsystems(payload: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"ts_utc": timestamp(payload.get("ts_utc"))}
    for name in ("youtube_lifecycle", "local_delivery"):
        source = mapping(payload.get(name))
        result[name] = {"state": token(source.get("state"))}
    source_music = mapping(payload.get("music"))
    music = primitives(
        source_music,
        (
            "state",
            "confidence",
            "audio_fail_count",
            "pulse_source_missing_count",
            "pulse_route_ok",
            "play_history_recent",
            "track_transition_within_grace",
            "bucket_boundary_within_grace",
            "now_playing_status",
        ),
    )
    music["evidence"] = reason_list(
        source_music.get("evidence"),
        allowed=AUDIO_EVIDENCE_ALLOWLIST,
    )
    result["music"] = music
    return result


def viewer(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = primitives(
        payload,
        (
            "status",
            "frame_ok",
            "black_detected",
            "freeze_detected",
            "consecutive_probe_failures",
            "consecutive_visual_failures",
            "duration_sec",
        ),
    )
    result["checked_at_utc"] = timestamp(payload.get("checked_at_utc"))
    result["reason"] = "present" if str(payload.get("reason", "")).strip() else ""
    return result


def external(payload: Mapping[str, Any]) -> dict[str, Any]:
    reason = str(payload.get("reason", "")).strip()
    result = primitives(
        payload,
        (
            "status",
            "formal_sli",
            "evidence_age_seconds",
            "status_duration_seconds",
            "consecutive_status_samples",
        ),
    )
    result["reason"] = (
        reason if reason in REASON_ALLOWLIST else "source_reason_unrecognized"
    )
    for key in ("checked_at_utc", "evidence_at_utc", "status_since_utc"):
        if key in payload:
            result[key] = timestamp(payload.get(key))
    source_targets = mapping(payload.get("targets"))
    targets: dict[str, dict[str, Any]] = {}
    for name in TARGET_ALLOWLIST:
        source = mapping(source_targets.get(name))
        if source:
            targets[name] = primitives(
                source,
                (
                    "status",
                    "fresh_locations",
                    "observed_locations",
                    "passed_locations",
                    "minimum_locations",
                    "minimum_pass_ratio",
                    "pass_ratio",
                    "sample_age_seconds",
                ),
            )
    result["targets"] = targets
    return result


def monitoring(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks = mapping(payload.get("checks"))
    return {
        "updated_at_utc": timestamp(payload.get("updated_at_utc")),
        "repair_enabled": payload.get("repair_enabled") is True,
        "checks": {
            name: {"ok": mapping(checks.get(name)).get("ok")}
            for name in CHECK_ALLOWLIST
            if isinstance(mapping(checks.get(name)).get("ok"), bool)
        },
    }


def feedback(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = primitives(
        payload,
        (
            "measurement_status",
            "target_met_on_observed_samples",
            "source_disagreement",
            "source_disagreement_current",
            "sli_pct",
            "coverage_pct",
            "source_freshness_pct",
        ),
    )
    result["measurement_unknown_reasons"] = reason_list(
        payload.get("measurement_unknown_reasons")
    )
    prometheus = mapping(payload.get("prometheus_current"))
    if prometheus:
        result["prometheus_current"] = {
            **primitives(prometheus, ("available", "eligible", "good")),
            "ts_utc": timestamp(prometheus.get("ts_utc")),
        }
    raw = mapping(payload.get("raw_current"))
    if raw:
        result["raw_current"] = {
            **primitives(raw, ("available", "eligible", "classification")),
            "ts_utc": timestamp(raw.get("ts_utc")),
        }
    return result


def burn(payload: Mapping[str, Any]) -> dict[str, Any]:
    fast = mapping(payload.get("fast_feedback"))
    return {
        "checked_at_utc": timestamp(payload.get("checked_at_utc")),
        "no_automatic_recovery": payload.get("no_automatic_recovery") is True,
        "fast_feedback": {
            "youtube_input_quality": feedback(
                mapping(fast.get("youtube_input_quality"))
            )
        },
    }


def formal(payload: Mapping[str, Any]) -> dict[str, Any]:
    source_gates = mapping(payload.get("formal_gates"))
    gates: dict[str, dict[str, Any]] = {}
    for objective_id in FORMAL_OBJECTIVES:
        source = mapping(source_gates.get(objective_id))
        if not source:
            continue
        gate = primitives(
            source,
            (
                "compliance_status",
                "measurement_status",
                "sli_pct",
                "coverage_pct",
                "source_freshness_pct",
                "source_disagreement",
                "source_disagreement_current",
            ),
        )
        gate["measurement_unknown_reasons"] = reason_list(
            source.get("measurement_unknown_reasons")
        )
        gates[objective_id] = gate
    return {
        "checked_at_utc": timestamp(payload.get("checked_at_utc")),
        "no_automatic_recovery": payload.get("no_automatic_recovery") is True,
        "formal_gates": gates,
    }


def network_observer(payload: Mapping[str, Any]) -> dict[str, Any]:
    classification = mapping(payload.get("classification"))
    signals = mapping(classification.get("signals"))
    allowed_signals = (
        "v4_route_changed",
        "v6_route_changed",
        "ipv4_addr_changed",
        "ipv6_addr_changed",
        "dns_order_changed",
        "tcp_connect_ipv4_ok",
        "tcp_connect_ipv6_ok",
    )
    return {
        "schema": token(payload.get("schema")),
        "ts_utc": timestamp(payload.get("ts_utc")),
        "classification": {
            **primitives(
                classification,
                ("status", "cause_layer", "cause", "affected_path", "impact", "action_hint"),
            ),
            "signals": {
                name: signals[name]
                for name in allowed_signals
                if isinstance(signals.get(name), bool)
            },
        },
    }


def resource_memory(payload: Mapping[str, Any]) -> dict[str, Any]:
    assessment = mapping(payload.get("assessment"))
    host = mapping(payload.get("host_memory"))
    pressure = mapping(payload.get("memory_pressure"))
    return {
        "schema_version": token(payload.get("schema_version")),
        "ts_utc": timestamp(payload.get("ts_utc")),
        "assessment": primitives(
            assessment,
            (
                "status",
                "memory_is_sli",
                "restart_allowed_by_memory_alone",
                "baseline_ready",
                "baseline_coverage_sec",
            ),
        ),
        "current_pressure": primitives(
            mapping(assessment.get("current_pressure")),
            (
                "mem_available_warn",
                "mem_available_critical",
                "mem_available_emergency",
                "swap_growth",
                "sustained_swap_growth",
                "psi_some",
                "psi_full",
                "oom_event_delta",
            ),
        ),
        "swap_capacity": primitives(
            mapping(assessment.get("swap_capacity")),
            ("used_ratio", "observe", "warn", "critical"),
        ),
        "host_memory": primitives(
            host,
            ("mem_available_mb", "mem_available_ratio", "swap_used_mb", "swap_used_ratio"),
        ),
        "memory_pressure": primitives(pressure, ("some_avg10", "full_avg10")),
    }


def memory_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    overall = mapping(payload.get("overall"))
    host = mapping(payload.get("host"))
    return {
        **primitives(payload, ("schema_version", "classification_policy_version")),
        "generated_at_utc": timestamp(payload.get("generated_at_utc")),
        "metric_classification": token(payload.get("metric_classification")),
        "overall": primitives(
            overall,
            (
                "severity",
                "current_incident",
                "warn",
                "operational_adequacy_severity",
                "operational_adequacy_warn",
                "operational_adequacy_critical",
                "peak_guardrail_severity",
            ),
        ),
        "host": primitives(
            host,
            (
                "mem_available_bytes",
                "mem_available_reference_pct",
                "swap_total_bytes",
                "swap_used_bytes",
                "swap_used_reference_pct",
            ),
        ),
        "swap_capacity": primitives(
            mapping(host.get("swap_capacity")),
            (
                "severity",
                "observe_ratio",
                "warn_ratio",
                "critical_ratio",
                "observe_bytes",
                "warn_bytes",
                "critical_bytes",
                "contributes_to_current_severity",
            ),
        ),
    }


def recovery_plan(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ts_utc": timestamp(payload.get("ts_utc")),
        **primitives(
            payload,
            ("action", "scope", "mode", "executable", "execute", "reason"),
        ),
        "blocked_by": reason_list(payload.get("blocked_by")),
    }


def notification_state(payload: Mapping[str, Any]) -> dict[str, Any]:
    active = mapping(payload.get("active"))
    contract_valid = isinstance(payload.get("active"), Mapping) and isinstance(
        payload.get("maintenance_active"), bool
    )
    return {
        "updated_ts_utc": timestamp(payload.get("updated_ts_utc")),
        "maintenance_active": payload.get("maintenance_active") is True,
        "active_incident_count": len(active),
        "state_contract_valid": contract_valid,
    }


def adsb_freshness(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ts_utc": timestamp(payload.get("ts_utc")),
        "last_change_ts": timestamp(payload.get("last_change_ts")),
        **primitives(
            payload,
            ("status", "aircraft_count", "source_messages", "sample_ts", "source_now"),
        ),
        "reason_present": bool(str(payload.get("reason", "")).strip()),
    }


def youtube_api_quota(payload: Mapping[str, Any]) -> dict[str, Any]:
    window = mapping(payload.get("window"))
    totals = mapping(payload.get("totals"))
    ingest = mapping(payload.get("ingest"))
    return {
        "status": token(payload.get("status")),
        "target_day": token(payload.get("target_day")),
        "window": {
            **primitives(window, ("open_day", "lag_sec")),
            "start_utc": timestamp(window.get("start_utc")),
            "end_utc": timestamp(window.get("end_utc")),
            "effective_end_utc": timestamp(window.get("effective_end_utc")),
        },
        "totals": primitives(totals, ("calls", "units", "quota_exceeded_events")),
        "ingest": primitives(
            ingest,
            (
                "log_exists",
                "coverage_ok",
                "parse_errors",
                "missing_ts",
                "coverage_observed_ratio",
                "coverage_gap_start_sec",
                "coverage_gap_end_sec",
            ),
        ),
    }


def control_loop(payload: Mapping[str, Any]) -> dict[str, Any]:
    tasks = mapping(payload.get("tasks"))
    safe_tasks: dict[str, dict[str, Any]] = {}
    for raw_name, raw_item in list(tasks.items())[:64]:
        name = token(raw_name)
        item = mapping(raw_item)
        if not name:
            continue
        safe_tasks[name] = {
            **primitives(
                item,
                (
                    "status",
                    "interval_sec",
                    "timeout_sec",
                    "run_count",
                    "consecutive_failures",
                    "last_returncode",
                    "last_duration_sec",
                ),
            ),
            "last_completed_at_utc": timestamp(item.get("last_completed_at_utc")),
            "last_success_at_utc": timestamp(item.get("last_success_at_utc")),
            "last_failure_at_utc": timestamp(item.get("last_failure_at_utc")),
            "next_due_at_utc": timestamp(item.get("next_due_at_utc")),
            "fresh_until_utc": timestamp(item.get("fresh_until_utc")),
        }
    return {
        "schema": token(payload.get("schema")),
        "updated_at_utc": timestamp(payload.get("updated_at_utc") or payload.get("ts_utc")),
        "mode": token(payload.get("mode")),
        "configured_task_count": payload.get("configured_task_count")
        if type(payload.get("configured_task_count")) is int
        and payload.get("configured_task_count") >= 0
        else None,
        "all_tasks_observed": payload.get("all_tasks_observed") is True,
        "fresh": payload.get("fresh") is True,
        "ok": payload.get("ok") is True,
        "failed_tasks": reason_list(payload.get("failed_tasks")),
        "stale_or_unobserved_tasks": reason_list(payload.get("stale_or_unobserved_tasks")),
        "tasks": safe_tasks,
    }


TRANSFORMS: dict[str, Callable[[Mapping[str, Any]], dict[str, Any]]] = {
    "youtube_watchdog_stats.json": youtube_watchdog,
    "youtube_video_id_resolver_state.json": youtube_resolver,
    "map_runtime_status.json": map_runtime,
    "subsystems_status.json": subsystems,
    "viewer_synthetic_status.json": viewer,
    "external_blackbox_status.json": external,
    "monitoring_watchdog_state.json": monitoring,
    "operational_reliability_burn_status.json": burn,
    "operational_reliability_status.json": formal,
    "network_observer.json": network_observer,
    "resource_memory.json": resource_memory,
    "memory_status.json": memory_status,
    "recovery_action_plan.json": recovery_plan,
    "notification_state.json": notification_state,
    "adsb_freshness_state.json": adsb_freshness,
    "youtube_api_quota_state.json": youtube_api_quota,
    "control_loop_state.json": control_loop,
}
