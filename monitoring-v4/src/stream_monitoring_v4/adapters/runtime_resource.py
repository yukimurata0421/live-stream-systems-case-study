from __future__ import annotations

from typing import Any, Mapping

from ._operational_base import OperationalSnapshotAdapter, mapping
from .snapshot_support import primitive_fields


def resource_status(payload: Mapping[str, Any]) -> str:
    status = str(mapping(payload.get("assessment")).get("status", "")).lower()
    if status in {"ok", "observe"}:
        return "good"
    if status in {"warn", "degraded", "critical"}:
        return "bad"
    return "unknown"


def memory_status(payload: Mapping[str, Any]) -> str:
    severity = str(mapping(payload.get("overall")).get("severity", "")).strip().lower()
    if severity == "ok":
        return "good"
    if severity in {"warn", "degraded", "critical", "error"}:
        return "bad"
    return "unknown"


class RuntimeResourceAdapter(OperationalSnapshotAdapter):
    domain = "runtime_resource"
    source = "resource_memory"
    timestamp_keys = ("ts_utc",)
    # arena's feed-protection override runs this diagnostic every 15 minutes
    # with up to two minutes of timer accuracy, so 20 minutes is the bounded
    # freshness contract rather than the source file's nominal one minute.
    freshness_limit_sec = 1200
    evidence_role = "current_correlated"

    def status(self, payload: Mapping[str, Any]) -> str:
        return resource_status(payload)

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        assessment = mapping(payload.get("assessment"))
        return {
            "assessment": primitive_fields(
                assessment,
                (
                    "status",
                    "memory_is_sli",
                    "restart_allowed_by_memory_alone",
                    "baseline_ready",
                    "baseline_coverage_sec",
                ),
            ),
            "current_pressure": primitive_fields(
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
            "swap_capacity": primitive_fields(
                mapping(assessment.get("swap_capacity")),
                ("used_ratio", "observe", "warn", "critical"),
            ),
            "host_memory": primitive_fields(
                mapping(payload.get("host_memory")),
                ("mem_available_mb", "mem_available_ratio", "swap_used_mb", "swap_used_ratio"),
            ),
            "memory_pressure": primitive_fields(
                mapping(payload.get("memory_pressure")),
                ("some_avg10", "full_avg10"),
            ),
        }


class MemoryStatusAdapter(OperationalSnapshotAdapter):
    domain = "runtime_resource"
    source = "memory_status"
    timestamp_keys = ("generated_at_utc",)
    freshness_limit_sec = 180

    def status(self, payload: Mapping[str, Any]) -> str:
        return memory_status(payload)

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        host = mapping(payload.get("host"))
        return {
            "schema_version": payload.get("schema_version"),
            "classification_policy_version": str(
                payload.get("classification_policy_version", "")
            )[:96],
            "metric_classification": str(payload.get("metric_classification", ""))[:64],
            "overall": primitive_fields(
                mapping(payload.get("overall")),
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
            "host": primitive_fields(
                host,
                (
                    "mem_available_bytes",
                    "mem_available_reference_pct",
                    "swap_total_bytes",
                    "swap_used_bytes",
                    "swap_used_reference_pct",
                ),
            ),
            "swap_capacity": primitive_fields(
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
