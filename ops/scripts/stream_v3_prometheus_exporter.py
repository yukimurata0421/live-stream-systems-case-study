#!/usr/bin/env python3
"""Prometheus exporter for the ADS-B stream_v3 arena-monitor operation."""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_REPO_ROOT = Path("/home/yuki/projects/stream_v3")
DEFAULT_STATE_ROOT = DEFAULT_REPO_ROOT / ".state" / "arena-monitor"


def stream_cli(repo_root: Path) -> Path:
    for name in ("stream-prod", "stream-new"):
        candidate = repo_root / "bin" / name
        if candidate.exists():
            return candidate
    return repo_root / "bin" / "stream-new"


def command_env(repo_root: Path, state_root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": str(repo_root / "src"),
            "STREAM_BASE_DIR": str(repo_root),
            "STREAM_RUNTIME_STATE_DIR": str(state_root),
            "STREAM_RUNTIME_LOG_DIR": str(state_root / "logs"),
            "STREAM_V3_STATE_ROOT": str(state_root),
            "STREAM_V2_STATE_ROOT": str(state_root),
            "STREAM_V2_SOURCE_STATE_ROOT": str(state_root),
        }
    )
    return env


class MetricsCache:
    def __init__(self, *, repo_root: Path, state_root: Path, ttl_sec: float, timeout_sec: float) -> None:
        self.repo_root = repo_root
        self.state_root = state_root
        self.ttl_sec = ttl_sec
        self.timeout_sec = timeout_sec
        self._payload = ""
        self._error = ""
        self._updated = 0.0

    def get(self) -> tuple[str, str]:
        now = time.monotonic()
        if self._payload and now - self._updated < self.ttl_sec:
            return self._payload, self._error
        try:
            self._payload = build_metrics(
                repo_root=self.repo_root,
                state_root=self.state_root,
                timeout_sec=self.timeout_sec,
            )
            self._error = ""
        except Exception as exc:  # pragma: no cover - defensive service boundary
            self._error = f"{type(exc).__name__}: {exc}"
            self._payload = build_error_metrics(self._error)
        self._updated = now
        return self._payload, self._error


def run_json(repo_root: Path, state_root: Path, args: list[str], *, timeout_sec: float) -> dict[str, Any]:
    proc = subprocess.run(
        args,
        cwd=repo_root,
        env=command_env(repo_root, state_root),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_sec,
        check=False,
    )
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        if proc.returncode != 0:
            raise RuntimeError(f"{args[0]} exited {proc.returncode}: {proc.stderr.strip()}")
        raise
    if proc.returncode != 0 and not isinstance(payload, dict):
        raise RuntimeError(f"{args[0]} exited {proc.returncode}: {proc.stderr.strip()}")
    return payload


def read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            value = json.load(fh)
    except FileNotFoundError:
        return {}
    return value if isinstance(value, dict) else {}


def count_pending_outbox(path: Path) -> int:
    count = 0
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict) and str(item.get("status", "pending")) == "pending":
            count += 1
    return count


def iter_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def bool_metric(value: Any) -> float:
    return 1.0 if bool(value) else 0.0


def parse_ts(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return round(float(ordered[idx]), 3)


def host_memory_snapshot() -> dict[str, float]:
    values: dict[str, float] = {}
    try:
        lines = Path("/proc/meminfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        if ":" not in line:
            continue
        key, rest = line.split(":", 1)
        parts = rest.strip().split()
        if not parts:
            continue
        try:
            values[key] = float(parts[0]) / 1024.0
        except ValueError:
            continue
    total = values.get("MemTotal", 0.0)
    available = values.get("MemAvailable", 0.0)
    swap_total = values.get("SwapTotal", 0.0)
    swap_free = values.get("SwapFree", 0.0)
    return {
        "mem_available_mb": round(available, 3),
        "mem_available_ratio": round(available / total, 6) if total > 0 else 0.0,
        "swap_used_mb": round(max(0.0, swap_total - swap_free), 3),
        "swap_used_ratio": round(max(0.0, swap_total - swap_free) / swap_total, 6) if swap_total > 0 else 0.0,
    }


def memory_quantity_mib(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    units = (
        ("Ki", 1.0 / 1024.0),
        ("Mi", 1.0),
        ("Gi", 1024.0),
        ("Ti", 1024.0 * 1024.0),
        ("K", 1000.0 / (1024.0 * 1024.0)),
        ("M", 1000.0 * 1000.0 / (1024.0 * 1024.0)),
        ("G", 1000.0 * 1000.0 * 1000.0 / (1024.0 * 1024.0)),
    )
    for suffix, factor in units:
        if text.endswith(suffix):
            try:
                return round(float(text[: -len(suffix)]) * factor, 3)
            except ValueError:
                return None
    try:
        return round(float(text) / (1024.0 * 1024.0), 3)
    except ValueError:
        return None


def kubectl_json(args: list[str], *, timeout_sec: float) -> dict[str, Any]:
    kubectl = os.environ.get("STREAM_KUBECTL_BIN", "kubectl")
    proc = subprocess.run(
        [kubectl, *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_sec,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"kubectl exited {proc.returncode}")
    payload = json.loads(proc.stdout)
    return payload if isinstance(payload, dict) else {}


def runtime_memory_snapshot(*, timeout_sec: float, now: float) -> dict[str, Any]:
    namespace = os.environ.get("STREAM_V3_RUNTIME_NAMESPACE", "stream-v3")
    deployment = os.environ.get("STREAM_V3_RUNTIME_DEPLOYMENT", "stream-v3-runtime")
    warning_ratio = as_float(os.environ.get("STREAM_V3_RUNTIME_MEMORY_WARN_RATIO"), 0.85)
    try:
        deployment_json = kubectl_json(
            ["-n", namespace, "get", "deployment", deployment, "-o", "json"],
            timeout_sec=timeout_sec,
        )
        metrics_json = kubectl_json(
            ["get", "--raw", f"/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods"],
            timeout_sec=timeout_sec,
        )
    except Exception as exc:
        return {
            "available": False,
            "current_ok": False,
            "warning_ratio": warning_ratio,
            "error": f"{type(exc).__name__}: {exc}",
            "containers": [],
        }

    pod_spec = (
        (((deployment_json.get("spec") or {}).get("template") or {}).get("spec") or {})
        if isinstance(deployment_json, dict)
        else {}
    )
    limits_by_container: dict[str, float | None] = {}
    requests_by_container: dict[str, float | None] = {}
    for container in pod_spec.get("containers") or []:
        if not isinstance(container, dict):
            continue
        name = str(container.get("name") or "")
        resources = container.get("resources") if isinstance(container.get("resources"), dict) else {}
        limits = resources.get("limits") if isinstance(resources.get("limits"), dict) else {}
        requests = resources.get("requests") if isinstance(resources.get("requests"), dict) else {}
        limits_by_container[name] = memory_quantity_mib(limits.get("memory"))
        requests_by_container[name] = memory_quantity_mib(requests.get("memory"))

    containers: list[dict[str, Any]] = []
    latest_ts = 0.0
    items = metrics_json.get("items") if isinstance(metrics_json.get("items"), list) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        pod_name = str(metadata.get("name") or "")
        if not pod_name.startswith(f"{deployment}-"):
            continue
        ts = parse_ts(item.get("timestamp"))
        if ts is not None:
            latest_ts = max(latest_ts, ts)
        for container in item.get("containers") or []:
            if not isinstance(container, dict):
                continue
            name = str(container.get("name") or "")
            usage = container.get("usage") if isinstance(container.get("usage"), dict) else {}
            current_mib = memory_quantity_mib(usage.get("memory"))
            limit_mib = limits_by_container.get(name)
            request_mib = requests_by_container.get(name)
            ratio = current_mib / limit_mib if current_mib is not None and limit_mib and limit_mib > 0 else None
            containers.append(
                {
                    "namespace": namespace,
                    "pod": pod_name,
                    "container": name,
                    "current_mib": current_mib,
                    "limit_mib": limit_mib,
                    "request_mib": request_mib,
                    "usage_ratio": round(ratio, 6) if ratio is not None else None,
                    "over_warning": bool(ratio is not None and ratio >= warning_ratio),
                }
            )

    available = bool(containers)
    current_ok = available and all(not item["over_warning"] for item in containers)
    return {
        "available": available,
        "current_ok": current_ok,
        "warning_ratio": warning_ratio,
        "sample_age_seconds": max(0.0, now - latest_ts) if latest_ts else 0.0,
        "containers": containers,
    }


def tcp_send_rows(state_root: Path, *, now: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in iter_jsonl(state_root / "logs" / "fast_recovery_events.jsonl"):
        if str(item.get("kind")) != "tcp_send_sample":
            continue
        ts = parse_ts(item.get("ts_utc") or item.get("generated_at_utc"))
        if ts is None:
            continue
        mbps = as_float(item.get("mbps", item.get("send_mbps")), default=-1.0)
        if mbps < 0:
            continue
        payload = dict(item)
        payload["_ts"] = ts
        payload["_mbps"] = mbps
        if ts <= now:
            rows.append(payload)
    rows.sort(key=lambda item: as_float(item.get("_ts")))
    return rows


def upload_fallback_by_window(state_root: Path, *, now: float) -> dict[str, dict[str, float]]:
    rows = tcp_send_rows(state_root, now=now)
    result: dict[str, dict[str, float]] = {}
    for hours in (1, 8, 24):
        cutoff = now - hours * 3600
        window_rows = [item for item in rows if as_float(item.get("_ts")) >= cutoff]
        values = [as_float(item.get("_mbps")) for item in window_rows]
        result[str(hours)] = {
            "p95": percentile(values, 95) or 0.0,
            "max": round(max(values), 3) if values else 0.0,
            "over_budget_sec": float(
                sum(
                    max(1, min(int(as_float(item.get("sample_interval_sec"), 60.0)), 600))
                    for item in window_rows
                    if as_float(item.get("_mbps")) > 5.0
                )
            ),
            "sample_count": float(len(values)),
        }
    return result


def latest_tcp_send_sample(state_root: Path, *, now: float) -> dict[str, Any]:
    rows = tcp_send_rows(state_root, now=now)
    if not rows:
        return {}
    latest = rows[-1]
    return latest if now - as_float(latest.get("_ts")) <= 5 * 60 else {}


def age_seconds(value: Any, *, now: float) -> float:
    ts = parse_ts(value)
    if ts is None:
        return 0.0
    return max(0.0, now - ts)


def label_value(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class MetricWriter:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.seen: set[str] = set()

    def metric(
        self,
        name: str,
        value: Any,
        *,
        labels: dict[str, Any] | None = None,
        help_text: str = "",
        metric_type: str = "gauge",
    ) -> None:
        label_text = ""
        if labels:
            pairs = [f'{key}="{label_value(val)}"' for key, val in sorted(labels.items())]
            label_text = "{" + ",".join(pairs) + "}"
        if name not in self.seen:
            self.lines.append(f"# HELP {name} {help_text or name}")
            self.lines.append(f"# TYPE {name} {metric_type}")
            self.seen.add(name)
        self.lines.append(f"{name}{label_text} {as_float(value)}")

    def render(self) -> str:
        return "\n".join(self.lines) + "\n"


def build_metrics(*, repo_root: Path, state_root: Path, timeout_sec: float) -> str:
    cli = stream_cli(repo_root)
    health = run_json(
        repo_root,
        state_root,
        [str(cli), "health-summary", "--windows", "1,8,24", "--json"],
        timeout_sec=timeout_sec,
    )
    objective = run_json(
        repo_root,
        state_root,
        [str(cli), "objective-sli", "--json", "--no-record"],
        timeout_sec=timeout_sec,
    )
    subsystems = read_json(state_root / "subsystems_status.json")
    memory = read_json(state_root / "memory_status.json")
    youtube_watchdog = read_json(state_root / "youtube_watchdog_stats.json")
    stream_watchdog = read_json(state_root / "stream_watchdog_stats.json")
    network = read_json(state_root / "network_observer_latest.json")
    resource_memory = read_json(state_root / "resource_memory.json")
    recovery_plan = read_json(state_root / "recovery_action_plan.json")
    notify_state = read_json(state_root / "stream_notify_state.json")
    adsb_freshness = read_json(state_root / "watchdog" / "adsb_freshness_state.json")
    pulse_health = read_json(state_root / "watchdog" / "pulse_health_state.json")
    recovery_stage = read_json(state_root / "watchdog" / "recovery_stage_state.json")
    slo_snapshot = read_json(state_root / "slo_snapshot.json")
    now = time.time()
    host_memory = host_memory_snapshot()
    runtime_memory = runtime_memory_snapshot(timeout_sec=timeout_sec, now=now)
    upload_fallback = upload_fallback_by_window(state_root, now=now)
    latest_tcp_sample = latest_tcp_send_sample(state_root, now=now)

    writer = MetricWriter()
    writer.metric("stream_v3_exporter_up", 1, help_text="Exporter scrape success.")

    for window in health.get("windows", []):
        if not isinstance(window, dict):
            continue
        observe = window.get("observe") if isinstance(window.get("observe"), dict) else {}
        checks = observe.get("checks") if isinstance(observe.get("checks"), dict) else {}
        labels = {"window_hours": str(window.get("hours", ""))}
        writer.metric("stream_v3_health_pass", window.get("pass", observe.get("pass")), labels=labels, help_text="Health summary pass by window.")
        writer.metric("stream_v3_current_fail", checks.get("current_fail"), labels=labels, help_text="Current failure flag by window.")
        writer.metric("stream_v3_historical_degraded", checks.get("historical_degraded"), labels=labels, help_text="Historical degraded flag by window.")
        writer.metric("stream_v3_youtube_warn_count", checks.get("youtube_warn_count"), labels=labels, help_text="YouTube warning count by window.")
        writer.metric("stream_v3_fast_recovery_restart_count", observe.get("fast_recovery_restart_count"), labels=labels, help_text="Fast recovery restart count by window.")
        writer.metric("stream_v3_ffmpeg_restart_incident_clusters", observe.get("ffmpeg_restart_incident_clusters_24h"), labels=labels, help_text="FFmpeg restart incident cluster count.")
        writer.metric("stream_v3_rtmps_ssl_tls_count", observe.get("rtmps_ssl_tls_count_24h"), labels=labels, help_text="RTMPS SSL/TLS event count.")
        fallback = upload_fallback.get(str(window.get("hours", "")), {})
        p95 = observe.get("ffmpeg_tcp_send_mbps_24h_p95")
        max_mbps = observe.get("ffmpeg_tcp_send_mbps_24h_max")
        over_budget = observe.get("ffmpeg_tcp_send_mbps_24h_over_budget_duration_sec")
        fallback_has_samples = as_float(fallback.get("sample_count")) > 0
        writer.metric("stream_v3_upload_p95_mbps", fallback.get("p95") if fallback_has_samples else p95, labels=labels, help_text="FFmpeg TCP send p95 Mbps.")
        writer.metric("stream_v3_upload_max_mbps", fallback.get("max") if fallback_has_samples else max_mbps, labels=labels, help_text="FFmpeg TCP send max Mbps.")
        writer.metric("stream_v3_upload_over_budget_seconds", fallback.get("over_budget_sec") if fallback_has_samples else over_budget, labels=labels, help_text="Seconds above upload budget.")
        writer.metric("stream_v3_fast_mode_active", observe.get("fast_mode_current_active"), labels=labels, help_text="Fast mode active flag.")
        writer.metric("stream_v3_api_open_day_units", (((observe.get("api_cost_reports") or {}).get("open_day_latest") or {}).get("units")), labels=labels, help_text="YouTube API units for current PT day.")

    metrics = objective.get("metrics") if isinstance(objective.get("metrics"), dict) else {}
    upload = metrics.get("upload_budget") if isinstance(metrics.get("upload_budget"), dict) else {}
    since = upload.get("since_samples_started") if isinstance(upload.get("since_samples_started"), dict) else {}
    writer.metric("stream_v3_upload_within_5mbps_ratio_pct", since.get("within_5mbps_ratio_pct"), help_text="Upload samples within 5 Mbps ratio.")

    api_usage = metrics.get("youtube_api_usage") if isinstance(metrics.get("youtube_api_usage"), dict) else {}
    for day in api_usage.get("by_pt_day", []):
        if not isinstance(day, dict):
            continue
        labels = {"pt_day": str(day.get("pt_day", ""))}
        writer.metric("stream_v3_youtube_api_units", day.get("units"), labels=labels, help_text="YouTube API units by PT day.")
        writer.metric("stream_v3_youtube_api_quota_exceeded_events", day.get("quota_exceeded_events"), labels=labels, help_text="YouTube API quota exceeded events by PT day.")

    memory_pressure = metrics.get("memory_pressure") if isinstance(metrics.get("memory_pressure"), dict) else {}
    for window_name in ("rolling_1h", "rolling_8h", "rolling_24h"):
        payload = memory_pressure.get(window_name) if isinstance(memory_pressure.get(window_name), dict) else {}
        labels = {"window": window_name}
        writer.metric("stream_v3_memory_warn_count", payload.get("warn_count"), labels=labels, help_text="Memory guardrail warn count.")
        writer.metric("stream_v3_memory_critical_count", payload.get("critical_count"), labels=labels, help_text="Memory guardrail critical count.")
        writer.metric("stream_v3_memory_non_reclaimable_p95_mib", payload.get("host_non_reclaimable_estimate_mib_p95"), labels=labels, help_text="Host non-reclaimable memory p95 MiB.")
        writer.metric("stream_v3_memory_available_min_mib", payload.get("host_mem_available_mib_min"), labels=labels, help_text="Host MemAvailable minimum MiB.")

    overall = subsystems.get("overall") if isinstance(subsystems.get("overall"), dict) else {}
    writer.metric("stream_v3_subsystems_healthy", 1 if overall.get("state") == "healthy" else 0, help_text="Subsystem overall healthy flag.")
    writer.metric("stream_v3_same_url_live", 1 if overall.get("stream_public_state") == "same_url_live" else 0, help_text="Same URL live flag.")
    writer.metric("stream_v3_subsystems_degraded_count", len(overall.get("degraded_subsystems") or []), help_text="Degraded subsystem count.")

    latest_memory_overall = memory.get("overall") if isinstance(memory.get("overall"), dict) else {}
    memory_current_ok = latest_memory_overall.get("severity") == "ok"
    if not latest_memory_overall and host_memory:
        memory_current_ok = as_float(host_memory.get("mem_available_ratio")) >= 0.10
    writer.metric("stream_v3_notify_pending", count_pending_outbox(state_root / "stream_notify_outbox.jsonl"), help_text="Pending notification messages.")
    writer.metric("stream_v3_memory_current_ok", 1 if memory_current_ok else 0, help_text="Latest memory severity ok flag.")
    writer.metric("stream_v3_maintenance_active", notify_state.get("maintenance_active"), help_text="Maintenance mode active flag.")
    writer.metric("stream_v3_notify_active_incidents", len(notify_state.get("active") or {}), help_text="Active notification incidents.")
    writer.metric("stream_v3_runtime_memory_current_ok", 1 if runtime_memory.get("current_ok") else 0, help_text="stream-v3-runtime Pod memory guardrail ok flag.")
    writer.metric("stream_v3_runtime_memory_sample_available", 1 if runtime_memory.get("available") else 0, help_text="stream-v3-runtime Pod memory sample availability.")
    writer.metric("stream_v3_runtime_memory_sample_age_seconds", runtime_memory.get("sample_age_seconds"), help_text="Age of stream-v3-runtime Pod memory sample.")
    writer.metric("stream_v3_runtime_memory_warning_ratio", runtime_memory.get("warning_ratio"), help_text="stream-v3-runtime container memory warning ratio.")
    writer.metric("stream_v3_runtime_memory_container_count", len(runtime_memory.get("containers") or []), help_text="stream-v3-runtime memory container sample count.")
    for item in runtime_memory.get("containers") or []:
        if not isinstance(item, dict):
            continue
        labels = {
            "namespace": item.get("namespace", "stream-v3"),
            "pod": item.get("pod", ""),
            "container": item.get("container", ""),
        }
        writer.metric("stream_v3_runtime_memory_current_mib", item.get("current_mib"), labels=labels, help_text="stream-v3-runtime container current memory MiB.")
        writer.metric("stream_v3_runtime_memory_limit_mib", item.get("limit_mib"), labels=labels, help_text="stream-v3-runtime container memory limit MiB.")
        writer.metric("stream_v3_runtime_memory_request_mib", item.get("request_mib"), labels=labels, help_text="stream-v3-runtime container memory request MiB.")
        writer.metric("stream_v3_runtime_memory_usage_ratio", item.get("usage_ratio"), labels=labels, help_text="stream-v3-runtime container memory usage divided by limit.")
        writer.metric("stream_v3_runtime_memory_over_warning", 1 if item.get("over_warning") else 0, labels=labels, help_text="stream-v3-runtime container memory over warning ratio.")

    writer.metric("stream_v3_youtube_watchdog_healthy", youtube_watchdog.get("healthy"), help_text="YouTube watchdog healthy flag.")
    writer.metric("stream_v3_youtube_public_ok", youtube_watchdog.get("public_ok"), help_text="YouTube public probe ok flag.")
    writer.metric("stream_v3_youtube_api_ok", youtube_watchdog.get("api_ok"), help_text="YouTube API probe ok flag.")
    writer.metric("stream_v3_youtube_oauth_ok", youtube_watchdog.get("oauth_ok"), help_text="YouTube OAuth probe ok flag.")
    writer.metric("stream_v3_youtube_local_ok", youtube_watchdog.get("local_ok"), help_text="Local stream evidence ok flag.")
    writer.metric("stream_v3_youtube_ingest_connected", youtube_watchdog.get("ingest_connected"), help_text="FFmpeg ingest socket connected flag.")
    writer.metric("stream_v3_youtube_stream_active", youtube_watchdog.get("stream_active"), help_text="Stream process active flag.")
    writer.metric("stream_v3_youtube_fail_count", youtube_watchdog.get("fail_count"), help_text="YouTube watchdog consecutive failure count.")
    writer.metric("stream_v3_youtube_degraded_public_count", youtube_watchdog.get("degraded_public_count"), help_text="YouTube degraded public count.")
    writer.metric("stream_v3_youtube_ffmpeg_uptime_seconds", youtube_watchdog.get("ffmpeg_uptime_sec"), help_text="FFmpeg process uptime seconds.")
    writer.metric("stream_v3_youtube_api_projected_units_per_day", youtube_watchdog.get("api_cost_projected_units_per_day"), help_text="Projected YouTube API units per PT day.")
    writer.metric("stream_v3_youtube_api_threshold_units_per_day", youtube_watchdog.get("api_cost_threshold_units_per_day"), help_text="YouTube API projected units threshold.")
    writer.metric("stream_v3_youtube_api_burn_rate_active", youtube_watchdog.get("api_cost_burn_rate_active"), help_text="YouTube API burn-rate guard active flag.")
    writer.metric("stream_v3_youtube_url_recovery_elapsed_seconds", youtube_watchdog.get("url_recovery_elapsed_sec"), help_text="YouTube URL recovery elapsed seconds.")
    writer.metric("stream_v3_youtube_candidate_new_url_found", youtube_watchdog.get("candidate_new_url_found"), help_text="Candidate replacement URL found flag.")
    writer.metric("stream_v3_youtube_stats_age_seconds", age_seconds(youtube_watchdog.get("stats_file_updated_at_utc") or youtube_watchdog.get("ts_utc"), now=now), help_text="Age of YouTube watchdog stats.")

    writer.metric("stream_v3_stream_watchdog_ok", 1 if stream_watchdog.get("status") == "ok" else 0, help_text="Local stream watchdog ok flag.")
    writer.metric("stream_v3_stream_watchdog_ffmpeg_count", stream_watchdog.get("ffmpeg_count"), help_text="Local stream watchdog ffmpeg process count.")
    writer.metric("stream_v3_stream_watchdog_runtime_snapshot_age_seconds", stream_watchdog.get("runtime_snapshot_age_sec"), help_text="Runtime snapshot age seconds.")
    writer.metric("stream_v3_stream_watchdog_stats_age_seconds", age_seconds(stream_watchdog.get("ts_utc"), now=now), help_text="Age of local stream watchdog stats.")

    route = network.get("route") if isinstance(network.get("route"), dict) else {}
    addresses = network.get("addresses") if isinstance(network.get("addresses"), dict) else {}
    dns = network.get("dns") if isinstance(network.get("dns"), dict) else {}
    tcp4 = network.get("tcp_connect_ipv4") if isinstance(network.get("tcp_connect_ipv4"), dict) else {}
    tcp6 = network.get("tcp_connect_ipv6") if isinstance(network.get("tcp_connect_ipv6"), dict) else {}
    ffmpeg_socket = network.get("ffmpeg_socket") if isinstance(network.get("ffmpeg_socket"), dict) else {}
    classification = network.get("classification") if isinstance(network.get("classification"), dict) else {}
    network_fallback_ok = (
        not network
        and bool(youtube_watchdog.get("ingest_connected"))
        and stream_watchdog.get("status") == "ok"
        and bool(latest_tcp_sample)
    )
    writer.metric("stream_v3_network_ok", 1 if classification.get("status") == "ok" or network_fallback_ok else 0, help_text="Network observer overall ok flag.")
    writer.metric("stream_v3_network_ipv4_route_ok", ((route.get("ipv4_default") or {}).get("ok")) if network else network_fallback_ok, help_text="IPv4 default route ok flag.")
    writer.metric("stream_v3_network_ipv6_route_ok", ((route.get("ipv6_default") or {}).get("ok")), help_text="IPv6 default route ok flag.")
    writer.metric("stream_v3_network_addresses_ok", addresses.get("ok") if network else network_fallback_ok, help_text="Network interface addresses ok flag.")
    writer.metric("stream_v3_network_dns_ok", dns.get("ok") if network else network_fallback_ok, help_text="RTMPS DNS resolution ok flag.")
    writer.metric("stream_v3_network_tcp_connect_ok", tcp4.get("ok") if network else network_fallback_ok, labels={"family": "ipv4"}, help_text="RTMPS TCP connect ok flag.")
    writer.metric("stream_v3_network_tcp_connect_elapsed_ms", tcp4.get("elapsed_ms"), labels={"family": "ipv4"}, help_text="RTMPS TCP connect elapsed milliseconds.")
    writer.metric("stream_v3_network_tcp_connect_ok", tcp6.get("ok"), labels={"family": "ipv6"}, help_text="RTMPS TCP connect ok flag.")
    writer.metric("stream_v3_network_tcp_connect_elapsed_ms", tcp6.get("elapsed_ms"), labels={"family": "ipv6"}, help_text="RTMPS TCP connect elapsed milliseconds.")
    remote_socket_connected = bool(latest_tcp_sample)
    socket_connected = bool(ffmpeg_socket.get("connected")) or remote_socket_connected
    socket_source = ffmpeg_socket if bool(ffmpeg_socket.get("connected")) else latest_tcp_sample
    writer.metric("stream_v3_network_ffmpeg_socket_connected", socket_connected, help_text="FFmpeg RTMPS socket connected flag.")
    writer.metric("stream_v3_network_ffmpeg_socket_notsent_bytes", socket_source.get("notsent"), help_text="FFmpeg socket unsent bytes.")
    writer.metric("stream_v3_network_ffmpeg_socket_unacked", socket_source.get("unacked"), help_text="FFmpeg socket unacked packet count.")
    writer.metric("stream_v3_network_ffmpeg_socket_lastsnd_ms", socket_source.get("lastsnd_ms"), help_text="FFmpeg socket last send age milliseconds.")
    writer.metric("stream_v3_upload_latest_mbps", latest_tcp_sample.get("mbps", latest_tcp_sample.get("send_mbps")), help_text="Latest FFmpeg TCP send Mbps.")
    writer.metric("stream_v3_upload_latest_age_seconds", age_seconds(latest_tcp_sample.get("ts_utc") or latest_tcp_sample.get("generated_at_utc"), now=now) if latest_tcp_sample else 0, help_text="Age of latest FFmpeg TCP send sample.")
    writer.metric("stream_v3_network_observer_age_seconds", age_seconds(network.get("ts_utc"), now=now), help_text="Age of network observer sample.")

    host_mem = resource_memory.get("host_memory") if isinstance(resource_memory.get("host_memory"), dict) else {}
    if not host_mem:
        host_mem = host_memory
    mem_pressure = resource_memory.get("memory_pressure") if isinstance(resource_memory.get("memory_pressure"), dict) else {}
    vm_activity = resource_memory.get("vm_activity") if isinstance(resource_memory.get("vm_activity"), dict) else {}
    cgroups = resource_memory.get("cgroups") if isinstance(resource_memory.get("cgroups"), dict) else {}
    writer.metric("stream_v3_host_mem_available_mib", host_mem.get("mem_available_mb"), help_text="Host MemAvailable MiB.")
    writer.metric("stream_v3_host_mem_available_ratio", host_mem.get("mem_available_ratio"), help_text="Host MemAvailable ratio.")
    writer.metric("stream_v3_host_swap_used_mib", host_mem.get("swap_used_mb"), help_text="Host swap used MiB.")
    writer.metric("stream_v3_host_swap_used_ratio", host_mem.get("swap_used_ratio"), help_text="Host swap used ratio.")
    writer.metric("stream_v3_host_memory_pressure_some_avg10", mem_pressure.get("some_avg10"), help_text="Host memory PSI some avg10.")
    writer.metric("stream_v3_host_memory_pressure_full_avg10", mem_pressure.get("full_avg10"), help_text="Host memory PSI full avg10.")
    writer.metric("stream_v3_host_pgmajfault_delta_per_min", vm_activity.get("pgmajfault_delta_per_min"), help_text="Major page faults per minute.")
    writer.metric("stream_v3_host_pswpin_delta_per_min", vm_activity.get("pswpin_delta_per_min"), help_text="Swap-in pages per minute.")
    writer.metric("stream_v3_resource_memory_age_seconds", age_seconds(resource_memory.get("ts_utc"), now=now), help_text="Age of resource memory sample.")
    for unit, payload in cgroups.items():
        if not isinstance(payload, dict):
            continue
        labels = {"unit": unit}
        writer.metric("stream_v3_cgroup_memory_current_mib", payload.get("memory_current_mb"), labels=labels, help_text="Cgroup current memory MiB.")
        writer.metric("stream_v3_cgroup_memory_peak_mib", payload.get("memory_peak_mb"), labels=labels, help_text="Cgroup peak memory MiB.")
        writer.metric("stream_v3_cgroup_swap_current_mib", payload.get("memory_swap_current_mb"), labels=labels, help_text="Cgroup current swap MiB.")

    writer.metric("stream_v3_adsb_messages_last_change_age_seconds", age_seconds(adsb_freshness.get("last_change_ts"), now=now), help_text="Age since ADS-B message count last changed.")
    writer.metric("stream_v3_audio_dj_missing_count", pulse_health.get("dj_missing_count"), help_text="Auto DJ pulse missing count.")
    writer.metric("stream_v3_audio_capture_missing_count", pulse_health.get("capture_missing_count"), help_text="Capture pulse missing count.")
    writer.metric("stream_v3_audio_dj_latency_high_count", pulse_health.get("dj_latency_high_count"), help_text="Auto DJ pulse high-latency count.")
    writer.metric("stream_v3_audio_capture_latency_high_count", pulse_health.get("capture_latency_high_count"), help_text="Capture pulse high-latency count.")
    writer.metric("stream_v3_audio_stage", recovery_stage.get("audio_stage"), help_text="Audio recovery stage.")
    writer.metric("stream_v3_pulse_stage", recovery_stage.get("pulse_stage"), help_text="Pulse recovery stage.")
    writer.metric("stream_v3_slo_pulse_unavailable_count", slo_snapshot.get("pulse_unavailable_count"), help_text="Pulse unavailable count in SLO window.")
    writer.metric("stream_v3_slo_restart_trigger_count", slo_snapshot.get("restart_trigger_count"), help_text="Restart trigger count in SLO window.")

    writer.metric("stream_v3_recovery_action_pending", 1 if recovery_plan.get("action") not in ("", "none", None) else 0, help_text="Recovery orchestrator has a non-noop action.")
    writer.metric("stream_v3_recovery_action_executable", recovery_plan.get("executable"), help_text="Recovery action executable flag.")
    writer.metric("stream_v3_recovery_action_blocked_count", len(recovery_plan.get("blocked_by") or []), help_text="Recovery action blocked-by count.")
    writer.metric("stream_v3_recovery_plan_age_seconds", age_seconds(recovery_plan.get("ts_utc"), now=now), help_text="Age of recovery action plan.")
    return writer.render()


def build_error_metrics(error: str) -> str:
    writer = MetricWriter()
    writer.metric("stream_v3_exporter_up", 0, help_text="Exporter scrape success.")
    writer.metric("stream_v3_exporter_error", 1, labels={"error": error[:120]}, help_text="Exporter error flag.")
    return writer.render()


def make_handler(cache: MetricsCache) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                _, error = cache.get()
                body = ("ok\n" if not error else f"{error}\n").encode("utf-8")
                self.send_response(200 if not error else 503)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path not in ("/", "/metrics"):
                self.send_error(404)
                return
            body, _ = cache.get()
            raw = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

    return Handler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9108)
    parser.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT)
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--cache-sec", type=float, default=240.0)
    parser.add_argument("--timeout-sec", type=float, default=45.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cache = MetricsCache(
        repo_root=args.repo_root,
        state_root=args.state_root,
        ttl_sec=args.cache_sec,
        timeout_sec=args.timeout_sec,
    )
    server = ThreadingHTTPServer((args.host, args.port), make_handler(cache))
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
