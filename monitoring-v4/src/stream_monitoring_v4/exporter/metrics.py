from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import DOMAINS
from stream_contracts.monitoring_v4.time import unix_ts

from stream_monitoring_v4.storage.ports import ExporterRepository


LEGACY_COMPATIBILITY_METRICS = frozenset(
    {
        "stream_v3_external_blackbox_age_seconds",
        "stream_v3_external_blackbox_collector_age_seconds",
        "stream_v3_external_blackbox_ok",
        "stream_v3_external_blackbox_sample_available",
        "stream_v3_external_blackbox_status",
        "stream_v3_map_monitor_delivery_critical_ok",
        "stream_v3_map_monitor_sample_age_seconds",
        "stream_v3_map_monitor_sample_available",
        "stream_v3_map_monitor_status",
        "stream_v3_map_monitor_weather_ok",
        "stream_v3_map_asset_identity_ok",
        "stream_v3_map_precipitation_data_ok",
        "stream_v3_map_precipitation_generation_integrity",
        "stream_v3_map_precipitation_render_applied",
        "stream_v3_map_precipitation_validtime_match",
        "stream_v3_map_semantic_visual_contract_ok",
        "stream_v3_monitoring_watchdog_age_seconds",
        "stream_v3_monitoring_watchdog_all_ok",
        "stream_v3_monitoring_watchdog_repair_enabled",
        "stream_v3_viewer_synthetic_black_detected",
        "stream_v3_viewer_synthetic_consecutive_probe_failures",
        "stream_v3_viewer_synthetic_consecutive_visual_failures",
        "stream_v3_viewer_synthetic_frame_ok",
        "stream_v3_viewer_synthetic_freeze_detected",
        "stream_v3_viewer_synthetic_sample_age_seconds",
        "stream_v3_viewer_synthetic_sample_available",
        "stream_v3_viewer_synthetic_status",
        "stream_v3_youtube_input_quality_eligible",
        "stream_v3_youtube_input_quality_good",
        "stream_v3_youtube_input_quality_issue_count",
        "stream_v3_youtube_input_quality_probe_fresh",
        "stream_v3_youtube_input_quality_state",
        "stream_v3_youtube_input_quality_warning_or_error_issue_count",
    }
)


def _label(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")[:160]


def _metric_value(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "0"
    if not math.isfinite(number):
        return "0"
    return f"{number:.12g}"


class MetricWriter:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.seen: set[str] = set()

    def metric(
        self,
        name: str,
        value: Any,
        *,
        labels: Mapping[str, Any] | None = None,
        help_text: str = "",
        metric_type: str = "gauge",
    ) -> None:
        if not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", name):
            raise ValueError(f"invalid metric name: {name}")
        rendered_labels = ""
        if labels:
            rendered_labels = "{" + ",".join(
                f'{key}="{_label(value)}"' for key, value in sorted(labels.items())
            ) + "}"
        if name not in self.seen:
            self.lines.append(f"# HELP {name} {help_text or name}")
            self.lines.append(f"# TYPE {name} {metric_type}")
            self.seen.add(name)
        self.lines.append(f"{name}{rendered_labels} {_metric_value(value)}")

    def render(self) -> str:
        return "\n".join(self.lines) + "\n"


def _latest_by_source(repository: ExporterRepository, domain: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in repository.latest_observations(domain):
        result.setdefault(item.source, item)
    return result


def _age(now_ts: int, observed_at: str) -> int | None:
    age = int(now_ts) - unix_ts(observed_at)
    return age if age >= 0 else None


def _legacy_map(writer: MetricWriter, repository: ExporterRepository, now_ts: int) -> None:
    item = _latest_by_source(repository, "rendering").get("map_runtime")
    age = _age(now_ts, item.observed_at) if item else None
    available = item is not None and age is not None
    payload = item.payload if available else {}
    writer.metric("stream_v3_map_monitor_sample_available", available)
    writer.metric(
        "stream_v3_map_monitor_sample_age_seconds",
        age if age is not None else 0,
    )
    writer.metric(
        "stream_v3_map_monitor_status",
        1,
        labels={"status": payload.get("status", "unknown")},
    )
    writer.metric(
        "stream_v3_map_monitor_delivery_critical_ok",
        payload.get("delivery_critical_ok") is True,
    )
    writer.metric("stream_v3_map_monitor_weather_ok", payload.get("weather_ok") is True)
    conditions = payload.get("conditions") if isinstance(payload.get("conditions"), Mapping) else {}
    writer.metric(
        "stream_v3_map_asset_identity_ok",
        conditions.get("asset_identity") is True,
    )
    writer.metric(
        "stream_v3_map_semantic_visual_contract_ok",
        conditions.get("semantic_visual_contract") is True,
    )
    writer.metric(
        "stream_v3_map_precipitation_data_ok",
        conditions.get("precipitation_data_ok") is True,
    )
    writer.metric(
        "stream_v3_map_precipitation_generation_integrity",
        conditions.get("precipitation_generation_integrity") is True,
    )
    writer.metric(
        "stream_v3_map_precipitation_render_applied",
        conditions.get("precipitation_render_applied") is True,
    )
    writer.metric(
        "stream_v3_map_precipitation_validtime_match",
        conditions.get("precipitation_validtime_match") is True,
    )


def _legacy_viewer(writer: MetricWriter, repository: ExporterRepository, now_ts: int) -> None:
    item = _latest_by_source(repository, "viewer_external").get("viewer_synthetic")
    age = _age(now_ts, item.observed_at) if item else None
    available = item is not None and age is not None
    payload = item.payload if available else {}
    writer.metric("stream_v3_viewer_synthetic_sample_available", available)
    writer.metric(
        "stream_v3_viewer_synthetic_sample_age_seconds",
        age if age is not None else 0,
    )
    writer.metric(
        "stream_v3_viewer_synthetic_status",
        1,
        labels={"status": payload.get("status", "unknown")},
    )
    for metric, key in (
        ("stream_v3_viewer_synthetic_frame_ok", "frame_ok"),
        ("stream_v3_viewer_synthetic_black_detected", "black_detected"),
        ("stream_v3_viewer_synthetic_freeze_detected", "freeze_detected"),
        ("stream_v3_viewer_synthetic_consecutive_probe_failures", "consecutive_probe_failures"),
        ("stream_v3_viewer_synthetic_consecutive_visual_failures", "consecutive_visual_failures"),
    ):
        writer.metric(metric, payload.get(key))


def _legacy_monitoring(writer: MetricWriter, repository: ExporterRepository, now_ts: int) -> None:
    item = _latest_by_source(repository, "monitoring_platform").get("monitoring_self")
    age = _age(now_ts, item.observed_at) if item else None
    payload = item.payload if item is not None and age is not None else {}
    writer.metric(
        "stream_v3_monitoring_watchdog_all_ok",
        item is not None and age is not None and item.status == "good",
    )
    writer.metric(
        "stream_v3_monitoring_watchdog_age_seconds",
        age if age is not None else 0,
    )
    writer.metric("stream_v3_monitoring_watchdog_repair_enabled", payload.get("repair_enabled") is True)
    writer.metric(
        "stream_v3_monitoring_v4_monitoring_check_failures",
        payload.get("bad_count", 0),
    )


def _legacy_external(writer: MetricWriter, repository: ExporterRepository, now_ts: int) -> None:
    item = _latest_by_source(repository, "viewer_external").get("external_blackbox")
    age = _age(now_ts, item.observed_at) if item else None
    available = item is not None and age is not None
    payload = item.payload if available else {}
    status = str(payload.get("status", "unknown"))
    writer.metric("stream_v3_external_blackbox_ok", available and status == "ok")
    writer.metric(
        "stream_v3_external_blackbox_status",
        {"ok": 1, "failed": 0}.get(status, -1) if available else -1,
    )
    writer.metric("stream_v3_external_blackbox_sample_available", available)
    writer.metric(
        "stream_v3_external_blackbox_age_seconds",
        age if age is not None else 0,
    )
    checked_at = payload.get("checked_at_utc")
    writer.metric(
        "stream_v3_external_blackbox_collector_age_seconds",
        (
            _age(now_ts, str(checked_at))
            if isinstance(checked_at, str) and checked_at
            else 0
        )
        or 0,
    )


def _legacy_input_quality(writer: MetricWriter, repository: ExporterRepository, now_ts: int) -> None:
    item = _latest_by_source(repository, "youtube_input_quality").get("youtube_input_quality_oauth")
    age = _age(now_ts, item.observed_at) if item else None
    payload = item.payload if item is not None and age is not None else {}
    fresh = item is not None and age is not None and age <= 600
    eligible = bool(
        fresh
        and payload.get("oauth_probe_ok") is True
        and str(payload.get("oauth_stream_status", "")).lower() == "active"
        and payload.get("ingest_connected") is True
    )
    details = payload.get("oauth_stream_health_issue_details")
    issues = details if isinstance(details, list) else []
    warning_count = sum(
        isinstance(value, Mapping) and str(value.get("severity", "")).lower() in {"warning", "error"}
        for value in issues
    )
    issue_count_value = payload.get("oauth_stream_health_issues", 0)
    try:
        issue_count = max(0, int(issue_count_value or 0), len(issues))
    except (TypeError, ValueError):
        issue_count = len(issues)
    probe_ok = payload.get("oauth_probe_ok") is True
    stream_status = str(payload.get("oauth_stream_status", "")).strip().lower()
    health_status = str(payload.get("oauth_stream_health_status", "")).strip().lower()
    ingest_connected = payload.get("ingest_connected") is True
    if not fresh:
        classification = "ineligible_oauth_probe_stale_or_missing"
    elif not probe_ok:
        classification = "ineligible_oauth_probe_failed"
    elif stream_status != "active":
        classification = "ineligible_stream_not_active"
    elif not ingest_connected:
        classification = "ineligible_local_ingest_disconnected"
    elif health_status == "good" and warning_count == 0:
        classification = "good"
    elif health_status == "nodata":
        classification = "bad_health_nodata"
    elif health_status == "ok":
        classification = "bad_health_warning"
    elif health_status == "bad":
        classification = "bad_health_error"
    elif warning_count:
        classification = "bad_configuration_issue"
    else:
        classification = "bad_health_unknown"
    writer.metric("stream_v3_youtube_input_quality_probe_fresh", fresh)
    writer.metric("stream_v3_youtube_input_quality_eligible", eligible)
    writer.metric("stream_v3_youtube_input_quality_good", eligible and item is not None and item.status == "good")
    writer.metric("stream_v3_youtube_input_quality_issue_count", issue_count)
    writer.metric("stream_v3_youtube_input_quality_warning_or_error_issue_count", warning_count)
    writer.metric(
        "stream_v3_youtube_input_quality_state",
        1,
        labels={
            "classification": classification,
            "health_status": health_status,
        },
    )


def render_metrics(
    repository: ExporterRepository,
    *,
    now_ts: int,
    build_revision: str,
) -> str:
    writer = MetricWriter()
    revision = re.sub(r"[^a-zA-Z0-9_.-]", "_", build_revision)[:80] or "unknown"
    writer.metric(
        "stream_v3_monitoring_v4_build_info",
        1,
        labels={"revision": revision, "mode": "credential_isolated_shadow"},
    )
    writer.metric("stream_v3_monitoring_v4_real_delivery_enabled", 0)
    writer.metric("stream_v3_monitoring_v4_runtime_mutation_enabled", 0)
    for domain in sorted(DOMAINS):
        current = repository.current(domain)
        current_age = _age(now_ts, current.observed_at) if current else None
        state = current.state if current and current_age is not None else "unknown"
        writer.metric(
            "stream_v3_monitoring_v4_current_snapshot_available",
            current is not None and current_age is not None,
            labels={"domain": domain},
        )
        writer.metric(
            "stream_v3_monitoring_v4_domain_state",
            1,
            labels={"domain": domain, "state": state},
        )
        writer.metric(
            "stream_v3_monitoring_v4_current_snapshot_age_seconds",
            current_age if current_age is not None else 0,
            labels={"domain": domain},
        )
    metrics_snapshot = repository.monitoring_metrics_snapshot()
    rejection_count = int(metrics_snapshot["rejection_count"])
    cycle_count = int(metrics_snapshot["cycle_count"])
    transition_rows = metrics_snapshot["transition_counts"]
    writer.metric(
        "stream_v3_monitoring_v4_schema_rejections_retained",
        rejection_count,
        help_text="Monitoring v4 schema rejection rows currently retained",
    )
    outbox_counts = repository.delivery_outbox_counts(max_attempts=8)
    pending = repository.undelivered_intent_count(
        now_ts=now_ts,
        max_attempts=8,
        attempt_timeout_sec=120,
    )
    writer.metric("stream_v3_monitoring_v4_outbox_pending", pending)
    writer.metric(
        "stream_v3_monitoring_v4_outbox_shadow_quarantined",
        outbox_counts.get("shadow_quarantined", 0),
    )
    for state in ("in_flight", "retryable_failed", "permanent_failed", "uncertain", "exhausted"):
        writer.metric(
            "stream_v3_monitoring_v4_outbox_state",
            outbox_counts.get(state, 0),
            labels={"state": state},
        )
    writer.metric(
        "stream_v3_monitoring_v4_shadow_cycles_retained",
        cycle_count,
        help_text="Monitoring v4 shadow cycle rows currently retained",
    )
    for row in transition_rows:
        writer.metric(
            "stream_v3_monitoring_v4_incident_transitions_total",
            row["count"],
            labels={"domain": row["domain"], "phase": row["phase"]},
            metric_type="counter",
        )
    parity = metrics_snapshot.get("latest_parity")
    if parity:
        writer.metric(
            "stream_v3_monitoring_v4_unclassified_parity_differences",
            parity.get("unclassified_contract_difference_count", 0),
        )
    publication_counts = metrics_snapshot.get("publication_counts", {})
    for state in ("pending", "published", "superseded"):
        writer.metric(
            "stream_v3_monitoring_v4_public_artifact_publications",
            publication_counts.get(state, 0),
            labels={"state": state},
        )
    scope_counts: dict[tuple[str, str], int] = defaultdict(int)
    for projection in repository.current_sli_projections():
        scope_counts[(projection.assessment_scope, projection.compliance_status)] += 1
        writer.metric(
            "stream_v3_monitoring_v4_sli_projection_info",
            1,
            labels={
                "objective": projection.objective_id,
                "scope": projection.assessment_scope,
                "window": projection.window,
                "compliance": projection.compliance_status,
            },
        )
    _legacy_map(writer, repository, now_ts)
    _legacy_viewer(writer, repository, now_ts)
    _legacy_monitoring(writer, repository, now_ts)
    _legacy_external(writer, repository, now_ts)
    _legacy_input_quality(writer, repository, now_ts)
    for name in sorted(LEGACY_COMPATIBILITY_METRICS):
        writer.metric(
            "stream_v3_monitoring_v4_compatibility_metric_covered",
            1,
            labels={"name": name},
        )
    return writer.render()
