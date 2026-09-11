#!/usr/bin/env python3
"""
Collect public-safe Stream v3 Prometheus and Loki snapshots.

The public pages should not expose Grafana, Prometheus, Loki, raw nginx logs, or
host-local details directly. This script queries the local Grafana proxy, keeps
only the fields needed for operations status, and writes static JSON files that
can be pushed to GCP/Cloudflare.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
PROM_OUT = BASE_DIR / "stream-v3-prometheus.json"
LOKI_OUT = BASE_DIR / "stream-v3-loki.json"

GRAFANA_BASE = os.environ.get("STREAM_V3_GRAFANA_BASE", "http://127.0.0.1:8088/grafana").rstrip("/")
PROM_UID = os.environ.get("STREAM_V3_PROMETHEUS_UID", "Prometheus")
LOKI_UID = os.environ.get("STREAM_V3_LOKI_UID", "Loki")
HTTP_TIMEOUT_SEC = float(os.environ.get("STREAM_V3_EXPORT_TIMEOUT_SEC", "8"))
LOKI_LIMIT = int(os.environ.get("STREAM_V3_LOKI_LIMIT", "80"))
PROM_TREND_WINDOW_SEC = int(os.environ.get("STREAM_V3_PROM_TREND_WINDOW_SEC", str(24 * 3600)))
PROM_TREND_STEP_SEC = int(os.environ.get("STREAM_V3_PROM_TREND_STEP_SEC", "900"))
FRESHNESS_WARN_SEC = int(os.environ.get("STREAM_V3_PUBLIC_FRESHNESS_WARN_SEC", "180"))
FRESHNESS_BAD_SEC = int(os.environ.get("STREAM_V3_PUBLIC_FRESHNESS_BAD_SEC", "300"))
RECOVERY_BOUNDARY_WARN_SEC = int(os.environ.get("STREAM_V3_RECOVERY_BOUNDARY_WARN_SEC", "7200"))
RECOVERY_ACTIVITY_DAYS = 7
RECOVERY_ACTIVITY_QUERY_LIMIT = int(os.environ.get("STREAM_V3_RECOVERY_ACTIVITY_QUERY_LIMIT", "5000"))
RECOVERY_ACTIVITY_TZ = timezone(timedelta(hours=9), name="JST")
REMOTE_RECOVERY_UNIT = "stream-v3-remote-recovery.service"
SHADOW_SLI_UNIT = "stream-v3-shadow-sli.service"
RECOVERY_SSH_TARGET = os.environ.get(
    "STREAM_V3_RECOVERY_SSH_TARGET",
    os.environ.get("STREAM_V3_RELIABILITY_SSH_TARGET", ""),
).strip()


PROM_QUERIES: list[dict[str, Any]] = [
    {
        "id": "current_fail_1h",
        "group": "Decision",
        "label": "Current fail 1h",
        "unit": "count",
        "kind": "zero_ok",
        "query": 'max(stream_v3_current_fail{job="stream_v3_arena_monitor",window_hours="1"})',
    },
    {
        "id": "youtube_issue",
        "group": "Decision",
        "label": "YouTube issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_youtube_watchdog_healthy{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "ingest_issue",
        "group": "Decision",
        "label": "Ingest issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_youtube_ingest_connected{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "same_url_issue",
        "group": "Decision",
        "label": "Same URL issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_same_url_live{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "network_issue",
        "group": "Decision",
        "label": "Network issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_network_ok{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "watchdog_issue",
        "group": "Decision",
        "label": "Watchdog issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_stream_watchdog_ok{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "upload_p95_1h",
        "group": "Guardrails",
        "label": "Upload p95 1h",
        "unit": "Mbps",
        "kind": "threshold",
        "warn": 5,
        "bad": 8,
        "query": 'max(stream_v3_upload_p95_mbps{job="stream_v3_arena_monitor",window_hours="1"})',
    },
    {
        "id": "adsb_age",
        "group": "Guardrails",
        "label": "ADS-B source age",
        "unit": "sec",
        "kind": "threshold",
        "warn": 180,
        "bad": 300,
        "query": 'max(stream_v3_adsb_source_age_seconds{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "api_open_day_units",
        "group": "Guardrails",
        "label": "YouTube API units today (PT)",
        "unit": "units",
        "kind": "threshold",
        "warn": 8000,
        "bad": 9000,
        "query": 'max(stream_v3_youtube_api_open_day_units{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "memory_guard_issue",
        "group": "Guardrails",
        "label": "Memory guard issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_runtime_memory_current_ok{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_sample_age",
        "group": "Map Runtime",
        "label": "Map monitor sample age",
        "unit": "sec",
        "kind": "threshold",
        "warn": 180,
        "bad": 300,
        "query": 'max(stream_v3_map_monitor_sample_age_seconds{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_delivery_issue",
        "group": "Map Runtime",
        "label": "Map delivery issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_map_monitor_delivery_critical_ok{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_render_issue",
        "group": "Map Runtime",
        "label": "Render heartbeat issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_map_render_ready{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_browser_issue",
        "group": "Map Runtime",
        "label": "Browser rendering issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_map_browser_contract_ok{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_nvenc_issue",
        "group": "Map Runtime",
        "label": "NVENC issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_map_nvenc_active{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_rtmp_issue",
        "group": "Map Runtime",
        "label": "RTMP issue",
        "unit": "flag",
        "kind": "zero_ok",
        "query": '1 - max(stream_v3_map_rtmp_socket_established{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_weather_issue",
        "group": "Map Runtime",
        "label": "Precipitation layer issue",
        "unit": "flag",
        "kind": "zero_warn",
        "query": '1 - max(stream_v3_map_monitor_weather_ok{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "map_runtime_restarts",
        "group": "Map Runtime",
        "label": "Kubernetes container restarts (current Pod)",
        "unit": "count",
        "kind": "zero_warn",
        "query": 'max(stream_v3_map_container_restart_count{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "notify_issues",
        "group": "Recovery",
        "label": "Notify issues",
        "unit": "count",
        "kind": "zero_ok",
        "query": 'max(stream_v3_notify_pending{job="stream_v3_arena_monitor"}) + max(stream_v3_notify_active_incidents{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "restarts_1h",
        "group": "Recovery",
        "label": "Fast Recovery dispatches 1h",
        "unit": "count",
        "kind": "zero_ok",
        "query": 'max(stream_v3_fast_recovery_restart_count{job="stream_v3_arena_monitor",window_hours="1"})',
    },
    {
        "id": "ffmpeg_clusters_1h",
        "group": "Recovery",
        "label": "FFmpeg incident clusters 1h",
        "unit": "count",
        "kind": "zero_ok",
        "query": 'max(stream_v3_ffmpeg_restart_incident_clusters{job="stream_v3_arena_monitor",window_hours="1"})',
    },
    {
        "id": "subsystems_degraded",
        "group": "Coverage",
        "label": "Subsystems degraded",
        "unit": "count",
        "kind": "zero_ok",
        "query": 'max(stream_v3_subsystems_degraded_count{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "maintenance_active",
        "group": "Coverage",
        "label": "Maintenance active",
        "unit": "flag",
        "kind": "zero_ok",
        "query": 'max(stream_v3_maintenance_active{job="stream_v3_arena_monitor"})',
    },
]


PROM_TRENDS: list[dict[str, Any]] = [
    {
        "id": "upload_p95_24h",
        "label": "Upload p95 24h",
        "unit": "Mbps",
        "query": 'max(stream_v3_upload_p95_mbps{job="stream_v3_arena_monitor",window_hours="1"})',
    },
    {
        "id": "adsb_age_24h",
        "label": "ADS-B source age 24h",
        "unit": "sec",
        "query": 'max(stream_v3_adsb_source_age_seconds{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "api_open_day_units_24h",
        "label": "YouTube API units today (PT)",
        "unit": "units",
        "query": 'max(stream_v3_youtube_api_open_day_units{job="stream_v3_arena_monitor"})',
    },
    {
        "id": "ffmpeg_uptime_24h",
        "label": "FFmpeg uptime 24h",
        "unit": "sec",
        "query": 'max(stream_v3_youtube_ffmpeg_uptime_seconds{job="stream_v3_arena_monitor"})',
    },
]


OBJECTIVE_SLI_QUERY = '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/objective_sli.jsonl"}'

RECOVERY_ACTIVITY_QUERIES = (
    {
        "source": "fast_recovery",
        "query": r'{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/fast_recovery_events.jsonl"} | json | kind="restart"',
    },
    {
        "source": "stream_engine",
        "query": r'{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/stream_engine_events.jsonl"} | json | event_type=~"ffmpeg_restart_scheduled|ffmpeg_restarted|self_recovery|render_browser_self_recovery_completed"',
    },
    {
        "source": "stream_watchdog",
        "query": r'{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/stream_watchdog_events.jsonl"} | json | event_type=~"dj_restart|restart_dj|stream_restart|restart_stream|youtube_watchdog_restart"',
    },
    {
        "source": "stream_notify_runtime",
        "query": r'{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/stream_notify_events.jsonl"} | json | phase="auto_recovered" |~ "runtime:lifecycle:"',
    },
)


LOKI_QUERIES: list[dict[str, Any]] = [
    {
        "id": "priority",
        "label": "Priority incidents",
        "query": r'{app="stream_v3",source="state_jsonl",filename=~"/stream_v3/.state/arena-monitor/logs/(youtube_watchdog|fast_recovery_events|stream_watchdog_events|network_observer|recovery_action_plan|recovery_orchestrator|stream_notify_events|subsystems_status|v3_control_loop)\\.jsonl"} |~ "gateway|timeout|error|failed|failure|degraded|restart|recovery|broken pipe|tls|ssl|stall"',
    },
    {
        "id": "youtube_watchdog",
        "label": "YouTube watchdog",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/youtube_watchdog.jsonl"}',
    },
    {
        "id": "fast_recovery",
        "label": "Fast recovery",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/fast_recovery_events.jsonl"}',
    },
    {
        "id": "stream_watchdog",
        "label": "Stream watchdog",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/stream_watchdog_events.jsonl"}',
    },
    {
        "id": "network_observer",
        "label": "Network observer",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/network_observer.jsonl"}',
    },
    {
        "id": "recovery_action_plan",
        "label": "Recovery action plan",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/recovery_action_plan.jsonl"}',
    },
    {
        "id": "recovery_orchestrator",
        "label": "Recovery orchestrator",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/recovery_orchestrator.jsonl"}',
    },
    {
        "id": "stream_notify_events",
        "label": "Notify events",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/stream_notify_events.jsonl"}',
    },
    {
        "id": "subsystems_status",
        "label": "Subsystems status",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/subsystems_status.jsonl"}',
    },
    {
        "id": "v3_control_loop",
        "label": "Control loop",
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/v3_control_loop.jsonl"}',
    },
    {
        "id": "priority_7d",
        "label": "Priority incidents 7d",
        "window_sec": str(7 * 24 * 3600),
        "query": r'{app="stream_v3",source="state_jsonl",filename=~"/stream_v3/.state/arena-monitor/logs/(youtube_watchdog|fast_recovery_events|stream_watchdog_events|network_observer|recovery_action_plan|recovery_orchestrator|stream_notify_events|subsystems_status|v3_control_loop)\\.jsonl"} |~ "gateway|timeout|error|failed|failure|degraded|restart|recovery|broken pipe|tls|ssl|stall"',
    },
    {
        "id": "recovery_orchestrator_30d",
        "label": "Recovery orchestrator 30d",
        "window_sec": str(30 * 24 * 3600),
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/recovery_orchestrator.jsonl"} |~ "executed|not_executed|shadow_mode|completed|restart"',
    },
    {
        "id": "recovery_action_plan_30d",
        "label": "Recovery action plan 30d",
        "window_sec": str(30 * 24 * 3600),
        "query": '{app="stream_v3",filename="/stream_v3/.state/arena-monitor/logs/recovery_action_plan.jsonl"} |~ "execute|restart|gates_block|blocked|allow|deny"',
    },
]


SAFE_EVENT_KEYS = {
    "action",
    "api_cost_burn_rate_active",
    "api_cost_projected_units_per_day",
    "api_cost_threshold_units_per_day",
    "api_live_state",
    "api_ok",
    "availability_ok",
    "candidate_new_url_found",
    "degraded_public_count",
    "blocked_by",
    "evidence_action",
    "evidence_reason",
    "evidence_state",
    "executable",
    "execute",
    "fail_count",
    "failure_kind",
    "failure_subkind",
    "ffmpeg_uptime_sec",
    "force_live_triggered",
    "health_source",
    "healthy",
    "incident_reason",
    "incident_stage",
    "ingest_connected",
    "judgment",
    "judgment_reason",
    "legacy_healthy",
    "level",
    "local_ok",
    "maintenance_active",
    "mode",
    "oauth_healthy",
    "oauth_ok",
    "public_ok",
    "reason",
    "result",
    "scope",
    "stage",
    "status",
    "stream_active",
    "ts",
    "ts_utc",
    "url_recovery_phase",
}


SECRET_RE = re.compile(
    r"(?i)(authorization|bearer|token|secret|password|passwd|api[_-]?key|x-api-key|access[_-]?token)"
)
SECRET_VALUE_RE = re.compile(
    r"(?i)\b(authorization|token|secret|password|passwd|api[_-]?key|x-api-key|access[_-]?token)"
    r"\b\s*[:=]\s*(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
BEARER_VALUE_RE = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
URL_RE = re.compile(r"https?://[^\s\"']+")
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
SAFE_METRIC_LABEL_KEYS = frozenset({"window_hours"})


def fetch_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full_url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as res:
        return json.loads(res.read().decode("utf-8", errors="replace"))


def atomic_write_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def prom_query(query: str) -> list[dict[str, Any]]:
    url = f"{GRAFANA_BASE}/api/datasources/proxy/uid/{urllib.parse.quote(PROM_UID)}/api/v1/query"
    payload = fetch_json(url, {"query": query})
    if payload.get("status") != "success":
        raise RuntimeError(payload.get("error") or "prometheus query failed")
    return payload.get("data", {}).get("result", [])


def prom_query_range(query: str, start: int, end: int, step: int) -> list[dict[str, Any]]:
    url = f"{GRAFANA_BASE}/api/datasources/proxy/uid/{urllib.parse.quote(PROM_UID)}/api/v1/query_range"
    payload = fetch_json(url, {"query": query, "start": start, "end": end, "step": step})
    if payload.get("status") != "success":
        raise RuntimeError(payload.get("error") or "prometheus range query failed")
    return payload.get("data", {}).get("result", [])


def loki_query(query: str, limit: int, window_sec: int | None = None) -> list[dict[str, Any]]:
    url = f"{GRAFANA_BASE}/api/datasources/proxy/uid/{urllib.parse.quote(LOKI_UID)}/loki/api/v1/query_range"
    params: dict[str, Any] = {"query": query, "limit": limit, "direction": "BACKWARD"}
    if window_sec:
        now_ns = int(time.time() * 1_000_000_000)
        params["start"] = now_ns - window_sec * 1_000_000_000
        params["end"] = now_ns
    payload = fetch_json(url, params)
    if payload.get("status") != "success":
        raise RuntimeError(payload.get("error") or "loki query failed")
    return payload.get("data", {}).get("result", [])


def numeric_value(result: list[dict[str, Any]]) -> float | None:
    values: list[float] = []
    for item in result:
        raw = item.get("value", [None, None])[1]
        try:
            values.append(float(raw))
        except Exception:
            continue
    if not values:
        return None
    return max(values)


def classify_prom(query_def: dict[str, Any], value: float | None) -> str:
    if value is None:
        return "unknown"
    if query_def.get("kind") == "zero_ok":
        return "ok" if value <= 0 else "bad"
    if query_def.get("kind") == "zero_warn":
        return "ok" if value <= 0 else "warn"
    if query_def.get("kind") == "threshold":
        if value >= float(query_def.get("bad", 0)):
            return "bad"
        if value >= float(query_def.get("warn", 0)):
            return "warn"
        return "ok"
    return "ok"


def public_number(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    if unit in {"sec", "units", "count", "flag"}:
        return float(round(value))
    if unit == "Mbps":
        return round(value, 2)
    return round(value, 3)


def collect_prometheus_trends(generated_at: float) -> list[dict[str, Any]]:
    end = int(generated_at)
    start = end - PROM_TREND_WINDOW_SEC
    trends: list[dict[str, Any]] = []

    for trend_def in PROM_TRENDS:
        trend = {k: v for k, v in trend_def.items() if k != "query"}
        try:
            result = prom_query_range(trend_def["query"], start, end, PROM_TREND_STEP_SEC)
            points: list[dict[str, Any]] = []
            values: list[float] = []
            for row in result[:1]:
                for ts, raw in row.get("values", []):
                    try:
                        value = float(raw)
                    except Exception:
                        continue
                    public_value = public_number(value, trend_def.get("unit"))
                    if public_value is None:
                        continue
                    points.append({"ts": int(float(ts)), "value": public_value})
                    values.append(public_value)
            latest = values[-1] if values else None
            trend.update({
                "status": "ok",
                "window_sec": PROM_TREND_WINDOW_SEC,
                "step_sec": PROM_TREND_STEP_SEC,
                "points": points,
                "latest": latest,
                "min": min(values) if values else None,
                "max": max(values) if values else None,
            })
        except Exception as exc:
            trend.update({
                "status": "error",
                "window_sec": PROM_TREND_WINDOW_SEC,
                "step_sec": PROM_TREND_STEP_SEC,
                "points": [],
                "error": safe_text(str(exc)),
            })
        trends.append(trend)

    return trends


def collect_prometheus() -> dict[str, Any]:
    generated_at = time.time()
    items: list[dict[str, Any]] = []
    summary = {"total": 0, "ok": 0, "warn": 0, "bad": 0, "unknown": 0}

    for query_def in PROM_QUERIES:
        item = {k: v for k, v in query_def.items() if k not in {"warn", "bad", "kind"}}
        try:
            result = prom_query(query_def["query"])
            value = numeric_value(result)
            state = classify_prom(query_def, value)
            published_value = public_number(value, query_def.get("unit"))
            series = []
            for row in result[:12]:
                raw = row.get("value", [None, None])[1]
                try:
                    parsed = public_number(float(raw), query_def.get("unit"))
                except Exception:
                    parsed = None
                series.append(
                    {
                        "metric": safe_metric_labels(row.get("metric", {})),
                        "value": parsed,
                    }
                )
            item.update({"status": "ok", "state": state, "value": published_value, "series": series})
        except Exception as exc:
            item.update(
                {
                    "status": "error",
                    "state": "unknown",
                    "value": None,
                    "error": safe_text(str(exc)),
                }
            )

        summary["total"] += 1
        summary[item["state"]] = summary.get(item["state"], 0) + 1
        items.append(item)

    if summary["bad"]:
        severity = "bad"
    elif summary["warn"] or summary["unknown"]:
        severity = "warn"
    else:
        severity = "ok"
    summary["severity"] = severity

    return {
        "schema": "stream-v3-prometheus-public.v1",
        "generated_at": generated_at,
        "generated_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(generated_at)),
        "source": {"kind": "grafana-proxy", "prometheus_uid": safe_text(PROM_UID)},
        "freshness_policy": {
            "warn_after_sec": FRESHNESS_WARN_SEC,
            "bad_after_sec": FRESHNESS_BAD_SEC,
        },
        "summary": summary,
        "items": items,
        "trends": collect_prometheus_trends(generated_at),
    }


def safe_text(value: str) -> str:
    value = BEARER_VALUE_RE.sub("bearer [redacted]", value)
    value = SECRET_VALUE_RE.sub(lambda match: f"{match.group(1)}=[redacted]", value)
    value = SECRET_RE.sub("***", value)
    value = URL_RE.sub("[url-redacted]", value)
    value = IP_RE.sub("[ip-redacted]", value)
    return value[:320]


def safe_metric_labels(labels: Any) -> dict[str, str]:
    if not isinstance(labels, dict):
        return {}
    return {
        key: safe_text(str(labels[key]))
        for key in sorted(SAFE_METRIC_LABEL_KEYS)
        if key in labels
    }


def basename_from_labels(labels: dict[str, Any]) -> str:
    filename = str(labels.get("filename") or "")
    if not filename:
        return str(labels.get("source") or "stream_v3")
    return filename.rsplit("/", 1)[-1]


def safe_event(raw: str, labels: dict[str, Any], ts_ns: str) -> dict[str, Any]:
    source = basename_from_labels(labels)
    event: dict[str, Any] = {"ts_ns": ts_ns, "source": source}
    try:
        parsed = json.loads(raw)
    except Exception:
        event["message"] = safe_text(raw)
        return event

    if isinstance(parsed, dict):
        for key in sorted(SAFE_EVENT_KEYS):
            if key in parsed:
                value = parsed[key]
                if isinstance(value, str):
                    value = safe_text(value)
                elif isinstance(value, (dict, list)):
                    value = safe_text(json.dumps(value, ensure_ascii=False))
                event[key] = value
        if "ts_utc" not in event and "ts" not in event:
            event["ts_utc"] = ns_to_iso(ts_ns)
        return event

    event["message"] = safe_text(str(parsed))
    return event


def ns_to_iso(ts_ns: str) -> str:
    try:
        ts = int(ts_ns) / 1_000_000_000
    except Exception:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def int_value(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def float_value(value: Any) -> float | None:
    try:
        return round(float(value), 6)
    except Exception:
        return None


def parse_utc_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def journal_json_records(
    unit: str,
    *,
    since_epoch: int,
    grep_pattern: str = "",
    lines: int | None = None,
) -> list[dict[str, Any]]:
    command = [
        "journalctl",
        "--unit",
        unit,
        "--since",
        f"@{max(0, int(since_epoch))}",
        "--no-pager",
        "--output=json",
    ]
    if grep_pattern:
        command.extend(["--grep", grep_pattern])
    if lines is not None:
        command.extend(["--lines", str(max(1, int(lines)))])
    completed = run_systemd_unit_command(unit, command)
    if completed.returncode not in (0, 1):
        raise RuntimeError("journal query failed")
    records: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def command_for_system_host(command: list[str], target: str) -> list[str]:
    if not target:
        return command
    return [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=8",
        "-o", "ServerAliveInterval=10",
        "-o", "ServerAliveCountMax=2",
        target,
        shlex.join(command),
    ]


def run_systemd_unit_command(unit: str, command: list[str]) -> subprocess.CompletedProcess[str]:
    targets = [""]
    if RECOVERY_SSH_TARGET:
        targets.append(RECOVERY_SSH_TARGET)
    errors: list[str] = []
    for target in targets:
        try:
            loaded = subprocess.run(
                command_for_system_host(
                    ["systemctl", "show", unit, "--property=LoadState", "--value"],
                    target,
                ),
                check=False,
                capture_output=True,
                text=True,
                timeout=max(10.0, HTTP_TIMEOUT_SEC),
            )
            if loaded.returncode != 0 or loaded.stdout.strip() != "loaded":
                errors.append("unit unavailable")
                continue
            return subprocess.run(
                command_for_system_host(command, target),
                check=False,
                capture_output=True,
                text=True,
                timeout=max(15.0, HTTP_TIMEOUT_SEC),
            )
        except (OSError, subprocess.SubprocessError):
            errors.append("command unavailable")
    raise RuntimeError(f"systemd unit source unavailable: {unit} ({', '.join(errors)})")


def journal_record_datetime(record: dict[str, Any]) -> datetime | None:
    raw = record.get("__REALTIME_TIMESTAMP") or record.get("_SOURCE_REALTIME_TIMESTAMP")
    try:
        return datetime.fromtimestamp(int(str(raw)) / 1_000_000, tz=timezone.utc)
    except Exception:
        return None


def latest_shadow_sli_from_journal(now: datetime) -> tuple[dict[str, Any], str] | None:
    try:
        completed = run_systemd_unit_command(
            SHADOW_SLI_UNIT,
            [
                "journalctl",
                "--unit",
                SHADOW_SLI_UNIT,
                "--since",
                f"@{int((now - timedelta(hours=3)).timestamp())}",
                "--no-pager",
                "--output=cat",
            ],
        )
    except Exception:
        return None
    if completed.returncode != 0:
        return None
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    for message in completed.stdout.splitlines():
        message = message.strip()
        if not message.startswith("{"):
            continue
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict) or not isinstance(payload.get("windows"), dict):
            continue
        ts = parse_utc_datetime(payload.get("ts_utc"))
        if ts is not None:
            candidates.append((ts, payload))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    ts, payload = candidates[-1]
    return payload, str(int(ts.timestamp() * 1_000_000_000))


def recovery_action_for_event(source: str, event: dict[str, Any]) -> str:
    event_type = str(event.get("event_type") or event.get("kind") or "")
    if source == "fast_recovery":
        if event_type != "restart":
            return ""
        if event.get("recovery_scope") == "ffmpeg_child":
            # Fast Recovery can emit more than one dispatch attempt for the same
            # child. Count the resulting stream-engine restart evidence instead
            # of treating each request as a runtime/stream restart.
            return ""
        return "restart_stream"
    if source == "stream_engine":
        if event_type == "render_browser_self_recovery_completed":
            return "restart_browser"
        if event_type in {"ffmpeg_restart_scheduled", "ffmpeg_restarted", "self_recovery"}:
            return "restart_ffmpeg"
        return ""
    if source == "stream_watchdog":
        if event_type in {"dj_restart", "restart_dj"}:
            return "restart_dj"
        if event_type in {"stream_restart", "restart_stream", "youtube_watchdog_restart"}:
            return "restart_stream"
    if source == "stream_notify_runtime":
        incident_ids = event.get("incident_ids")
        if event.get("phase") == "auto_recovered" and isinstance(incident_ids, list):
            if any(str(incident_id).startswith("runtime:lifecycle:") for incident_id in incident_ids):
                return "restart_stream"
    return ""


def recovery_event_datetime(source: str, event: dict[str, Any], ts_ns: str) -> datetime | None:
    if source == "stream_notify_runtime":
        incident_ids = event.get("incident_ids")
        if isinstance(incident_ids, list):
            for incident_id in incident_ids:
                match = re.match(r"^runtime:lifecycle:[^:]+:(\d{9,})$", str(incident_id))
                if match:
                    try:
                        return datetime.fromtimestamp(int(match.group(1)), tz=timezone.utc)
                    except (OverflowError, OSError, ValueError):
                        pass

    parsed = parse_utc_datetime(event.get("ts_utc"))
    if parsed is not None:
        return parsed
    try:
        return datetime.fromtimestamp(int(ts_ns) / 1_000_000_000, tz=timezone.utc)
    except Exception:
        return None


def recovery_activity_loki_events(now: datetime) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    for query_def in RECOVERY_ACTIVITY_QUERIES:
        source = str(query_def["source"])
        try:
            streams = loki_query(
                str(query_def["query"]),
                RECOVERY_ACTIVITY_QUERY_LIMIT,
                window_sec=(RECOVERY_ACTIVITY_DAYS + 1) * 24 * 3600,
            )
        except Exception:
            errors.append(source)
            continue
        for stream in streams:
            for ts_ns, raw in stream.get("values", []):
                try:
                    event = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(event, dict):
                    continue
                action = recovery_action_for_event(source, event)
                ts = recovery_event_datetime(source, event, ts_ns)
                if action and ts is not None and ts <= now:
                    events.append({"ts": ts, "action": action, "source": source})
    return events, errors


def recovery_activity_journal_events(now: datetime) -> tuple[list[dict[str, Any]], bool]:
    pattern = r"ok (action-plan restart_dj|restart deployment/stream-v3-runtime)"
    try:
        records = journal_json_records(
            REMOTE_RECOVERY_UNIT,
            since_epoch=int((now - timedelta(days=RECOVERY_ACTIVITY_DAYS + 1)).timestamp()),
            grep_pattern=pattern,
        )
    except Exception:
        return [], False
    events: list[dict[str, Any]] = []
    for record in records:
        message = str(record.get("MESSAGE") or "")
        ts = journal_record_datetime(record)
        if ts is None or ts > now:
            continue
        if "ok action-plan restart_dj:" in message:
            action = "restart_dj"
        elif "ok restart deployment/stream-v3-runtime:" in message:
            action = "restart_stream"
        else:
            continue
        events.append({"ts": ts, "action": action, "source": "remote_recovery"})
    return events, True


def dedupe_recovery_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exact_seen: set[tuple[int, str, str]] = set()
    exact: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda item: item["ts"]):
        key = (int(event["ts"].timestamp()), str(event["action"]), str(event["source"]))
        if key in exact_seen:
            continue
        exact_seen.add(key)
        exact.append(event)

    deduped: list[dict[str, Any]] = []
    for event in exact:
        duplicate = next(
            (
                prior
                for prior in reversed(deduped)
                if event["action"] == prior["action"]
                and event["source"] != prior["source"]
                and (event["ts"] - prior["ts"]).total_seconds() <= 30
            ),
            None,
        )
        if duplicate is None:
            deduped.append(event)
    return deduped


def collect_recovery_activity(now: datetime | None = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    local_today = now.astimezone(RECOVERY_ACTIVITY_TZ).date()
    start_date = local_today - timedelta(days=RECOVERY_ACTIVITY_DAYS - 1)
    loki_events, source_errors = recovery_activity_loki_events(now)
    journal_events, journal_ok = recovery_activity_journal_events(now)
    if not journal_ok:
        source_errors.append("remote_recovery")

    events = []
    for event in dedupe_recovery_events([*loki_events, *journal_events]):
        local_date = event["ts"].astimezone(RECOVERY_ACTIVITY_TZ).date()
        if start_date <= local_date <= local_today:
            events.append({**event, "local_date": local_date.isoformat()})

    action_order = ("restart_stream", "restart_ffmpeg", "restart_dj", "restart_browser")
    counts = {action: 0 for action in action_order}
    daily_counts = {
        (start_date + timedelta(days=offset)).isoformat(): 0
        for offset in range(RECOVERY_ACTIVITY_DAYS)
    }
    for event in events:
        action = str(event["action"])
        counts[action] = counts.get(action, 0) + 1
        daily_counts[event["local_date"]] += 1

    total = sum(counts.values())
    active_days = sum(1 for count in daily_counts.values() if count > 0)
    latest = max((event["ts"] for event in events), default=None)
    status = "ok" if not source_errors else "partial" if events else "error"
    return {
        "schema": "stream-v3-recovery-activity-public.v1",
        "status": status,
        "generated_at_utc": iso_utc(now),
        "period": {
            "days": RECOVERY_ACTIVITY_DAYS,
            "start_date": start_date.isoformat(),
            "end_date": local_today.isoformat(),
            "timezone": "Asia/Tokyo",
            "includes_current_day": True,
        },
        "total_executed": total,
        "active_day_count": active_days,
        "average_per_day": round(total / RECOVERY_ACTIVITY_DAYS, 1),
        "action_counts": counts,
        "daily_counts": daily_counts,
        "latest_action_at_utc": iso_utc(latest) if latest else "",
        "source_complete": not source_errors,
        "missing_sources": sorted(set(source_errors)),
        "count_basis": "executed actions plus observed unplanned runtime lifecycle changes; FFmpeg child dispatch attempts are excluded and cross-source duplicates within 30 seconds are counted once",
        "scope_note": "runtime/stream, Kubernetes container, FFmpeg child, and browser helper are separate failure domains",
    }


def safe_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    safe: dict[str, int] = {}
    for key, raw in value.items():
        key_text = re.sub(r"[^a-zA-Z0-9_-]", "", str(key))[:64]
        if key_text:
            safe[key_text] = int_value(raw)
    return safe


def safe_classifier_replay(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        "classifier": safe_text(str(value.get("classifier") or "")),
        "target_action": safe_text(str(value.get("target_action") or "")),
        "basis": safe_text(str(value.get("basis") or "")),
        "eligible_count": int_value(value.get("eligible_count")),
        "covered_count": int_value(value.get("covered_count")),
        "uncovered_count": int_value(value.get("uncovered_count")),
        "coverage_ratio": float_value(value.get("coverage_ratio")),
        "covered_by_trigger": safe_count_map(value.get("covered_by_trigger")),
        "uncovered_by_trigger": safe_count_map(value.get("uncovered_by_trigger")),
    }


def collect_recovery_boundary() -> dict[str, Any]:
    boundary: dict[str, Any] = {
        "schema": "stream-v3-recovery-boundary-public.v1",
        "status": "unknown",
        "windows": {},
    }
    try:
        now = datetime.now(timezone.utc)
        journal_snapshot = latest_shadow_sli_from_journal(now)
        if journal_snapshot is not None:
            parsed, ts_ns = journal_snapshot
            source = SHADOW_SLI_UNIT
        else:
            streams = loki_query(OBJECTIVE_SLI_QUERY, 1, window_sec=30 * 24 * 3600)
            values: list[tuple[str, str]] = []
            for stream in streams:
                values.extend(stream.get("values", []))
            values.sort(key=lambda row: row[0], reverse=True)
            if not values:
                boundary.update({"status": "empty", "error": "objective_sli snapshot not found"})
                return boundary
            ts_ns, raw = values[0]
            parsed = json.loads(raw)
            source = "objective_sli.jsonl"

        snapshot_ts = parse_utc_datetime(parsed.get("ts_utc"))
        age_sec = None if snapshot_ts is None else max(0, int((now - snapshot_ts).total_seconds()))
        fresh = age_sec is not None and age_sec <= RECOVERY_BOUNDARY_WARN_SEC
        boundary.update({
            "status": "ok" if fresh else "stale",
            "ts_ns": ts_ns,
            "ts_utc": safe_text(str(parsed.get("ts_utc") or ns_to_iso(ts_ns))),
            "source": source,
            "age_sec": age_sec,
            "fresh": fresh,
            "warn_after_sec": RECOVERY_BOUNDARY_WARN_SEC,
        })
        for window in ("last_24h", "last_7d", "last_30d"):
            window_data = parsed.get("windows", {}).get(window, {})
            shadow = window_data.get("subsystems_shadow", {})
            boundary["windows"][window] = {
                "window_complete": bool(window_data.get("window_complete")),
                "data_coverage_sec": int_value(window_data.get("data_coverage_sec")),
                "production_action_counts": safe_count_map(shadow.get("production_action_counts")),
                "production_action_sample_count": int_value(shadow.get("production_action_sample_count")),
                "shadow_selected_action_counts": safe_count_map(shadow.get("selected_action_counts")),
                "shadow_executable_plan_counts": safe_count_map(shadow.get("executable_plan_counts")),
                "shadow_recovery_intent_action_counts": safe_count_map(shadow.get("recovery_intent_action_counts")),
                "shadow_non_none_action_count": int_value(shadow.get("shadow_non_none_action_count")),
                "shadow_executable_plan_action_count": int_value(shadow.get("shadow_executable_plan_action_count")),
                "shadow_recovery_intent_action_count": int_value(shadow.get("shadow_recovery_intent_action_count")),
                "shadow_destructive_action_count": int_value(shadow.get("shadow_destructive_action_count")),
                "shadow_vs_production_disagreement_count": int_value(shadow.get("shadow_vs_production_disagreement_count")),
                "shadow_vs_production_disagreement_by_reason": safe_count_map(shadow.get("shadow_vs_production_disagreement_by_reason")),
                "current_classifier_replay": safe_classifier_replay(shadow.get("current_classifier_replay")),
                "diff_basis": safe_text(str(shadow.get("diff_basis") or "")),
                "interpretation": safe_text(str(shadow.get("interpretation") or "")),
            }
    except Exception as exc:
        boundary.update({"status": "error", "error": safe_text(str(exc))})
    return boundary


def collect_loki() -> dict[str, Any]:
    generated_at = time.time()
    sections: list[dict[str, Any]] = []
    total_events = 0
    error_sections = 0

    for query_def in LOKI_QUERIES:
        section = {k: query_def[k] for k in ("id", "label")}
        window_sec = int(query_def.get("window_sec", 0) or 0)
        if window_sec:
            section["window_sec"] = window_sec
        try:
            streams = loki_query(query_def["query"], LOKI_LIMIT, window_sec=window_sec or None)
            events: list[dict[str, Any]] = []
            for stream in streams:
                labels = stream.get("stream", {})
                for ts_ns, raw in stream.get("values", []):
                    events.append(safe_event(raw, labels, ts_ns))
            events.sort(key=lambda row: row.get("ts_ns", ""), reverse=True)
            section.update({"status": "ok", "events": events[:LOKI_LIMIT], "event_count": len(events)})
            total_events += len(events)
        except Exception as exc:
            section.update(
                {
                    "status": "error",
                    "events": [],
                    "event_count": 0,
                    "error": safe_text(str(exc)),
                }
            )
            error_sections += 1
        sections.append(section)

    return {
        "schema": "stream-v3-loki-public.v1",
        "generated_at": generated_at,
        "generated_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(generated_at)),
        "freshness_policy": {
            "warn_after_sec": FRESHNESS_WARN_SEC,
            "bad_after_sec": FRESHNESS_BAD_SEC,
        },
        "source": {
            "kind": "grafana-proxy",
            "loki_uid": safe_text(LOKI_UID),
            "raw_nginx_logs": "omitted",
            "raw_host_logs": "omitted",
        },
        "summary": {
            "total_sections": len(sections),
            "error_sections": error_sections,
            "total_events": total_events,
            "severity": "warn" if error_sections else "ok",
        },
        "recovery_activity": collect_recovery_activity(
            datetime.fromtimestamp(generated_at, tz=timezone.utc)
        ),
        "recovery_boundary": collect_recovery_boundary(),
        "sections": sections,
    }


def main() -> None:
    atomic_write_json(PROM_OUT, collect_prometheus())
    atomic_write_json(LOKI_OUT, collect_loki())


if __name__ == "__main__":
    main()
