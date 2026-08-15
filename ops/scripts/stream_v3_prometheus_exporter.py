#!/usr/bin/env python3
"""Prometheus exporter for the ADS-B stream_v3 observability monitor."""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime
from pathlib import Path
from typing import Any

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from stream_core.k8s_gpu_guard import summarize_runtime_gpu
from stream_core.common.youtube_input_quality import (
    DEFAULT_INPUT_QUALITY_MAX_AGE_SEC,
    classify_input_quality_sample,
)


def default_repo_root() -> Path:
    return Path(
        os.environ.get("STREAM_V3_REPO_DIR", Path(__file__).resolve().parents[2])
    ).expanduser()


def default_state_root(repo_root: Path) -> Path:
    configured = os.environ.get("STREAM_V3_OBSERVABILITY_STATE_ROOT") or os.environ.get(
        "STREAM_RUNTIME_STATE_DIR"
    )
    if configured:
        return Path(configured).expanduser()
    return repo_root / ".state" / "observability-monitor"


DEFAULT_REPO_ROOT = default_repo_root()
DEFAULT_STATE_ROOT = default_state_root(DEFAULT_REPO_ROOT)
HEALTH_SUMMARY_SNAPSHOT = "health_summary_snapshot.json"
OBJECTIVE_SLI_SNAPSHOT = "objective_sli_snapshot.json"
MAP_EXPECTED_CONTAINERS = (
    "stream-engine",
    "precipitation-fetcher",
    "auto-dj",
    "fast-recovery-loop",
)


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
    def __init__(
        self,
        *,
        repo_root: Path,
        state_root: Path,
        ttl_sec: float,
        timeout_sec: float,
        health_snapshot_file: Path | None,
        max_health_snapshot_age_sec: float,
        objective_snapshot_file: Path | None,
        max_objective_snapshot_age_sec: float,
    ) -> None:
        self.repo_root = repo_root
        self.state_root = state_root
        self.ttl_sec = ttl_sec
        self.timeout_sec = timeout_sec
        self.health_snapshot_file = health_snapshot_file
        self.max_health_snapshot_age_sec = max_health_snapshot_age_sec
        self.objective_snapshot_file = objective_snapshot_file
        self.max_objective_snapshot_age_sec = max_objective_snapshot_age_sec
        self._payload = ""
        self._error = ""
        self._updated = 0.0
        self._last_success_updated = 0.0
        self._last_refresh_duration = 0.0
        self._lock = threading.Lock()

    def get(self) -> tuple[str, str]:
        now = time.monotonic()
        if self._payload and now - self._updated < self.ttl_sec:
            return self._render(now), self._error
        with self._lock:
            now = time.monotonic()
            if self._payload and now - self._updated < self.ttl_sec:
                return self._render(now), self._error
            self._refresh()
            return self._render(time.monotonic()), self._error

    def _refresh(self) -> None:
        started = time.monotonic()
        try:
            payload = build_metrics(
                repo_root=self.repo_root,
                state_root=self.state_root,
                timeout_sec=self.timeout_sec,
                health_snapshot_file=self.health_snapshot_file,
                max_health_snapshot_age_sec=self.max_health_snapshot_age_sec,
                objective_snapshot_file=self.objective_snapshot_file,
                max_objective_snapshot_age_sec=self.max_objective_snapshot_age_sec,
            )
        except Exception as exc:  # pragma: no cover - defensive service boundary
            finished = time.monotonic()
            self._last_refresh_duration = finished - started
            self._error = f"{type(exc).__name__}: {exc}"
            if not self._payload:
                self._payload = build_error_metrics(self._error)
            self._updated = finished
            return

        finished = time.monotonic()
        self._payload = payload
        self._error = ""
        self._updated = finished
        self._last_success_updated = finished
        self._last_refresh_duration = finished - started

    def _render(self, now: float) -> str:
        payload = self._payload or build_error_metrics(self._error or "no metrics generated")
        if self._error:
            payload = replace_metric_value(payload, "stream_v3_exporter_up", 0)
        return append_exporter_cache_metrics(
            payload,
            error=self._error,
            now=now,
            last_success_updated=self._last_success_updated,
            last_refresh_duration=self._last_refresh_duration,
        )


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


def runtime_gpu_snapshot(*, timeout_sec: float, now: float) -> dict[str, Any]:
    namespace = os.environ.get("STREAM_V3_RUNTIME_NAMESPACE", "stream-v3")
    deployment = os.environ.get("STREAM_V3_RUNTIME_DEPLOYMENT", "stream-v3-runtime")
    selector = os.environ.get(
        "STREAM_V3_RUNTIME_SELECTOR",
        "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
    )
    container_name = os.environ.get("STREAM_V3_RUNTIME_GPU_CONTAINER", "stream-engine")
    try:
        deployment_json = kubectl_json(
            ["-n", namespace, "get", "deployment", deployment, "-o", "json"],
            timeout_sec=timeout_sec,
        )
        pods_json = kubectl_json(
            ["-n", namespace, "get", "pods", "-l", selector, "-o", "json"],
            timeout_sec=timeout_sec,
        )
    except Exception as exc:
        return {
            "available": False,
            "status": "unavailable",
            "status_ok": False,
            "restart_blocked": False,
            "driver_mismatch": False,
            "gpu_runtime_error": False,
            "gpu_requested": False,
            "stream_engine_ready": False,
            "stream_engine_running": False,
            "container_waiting": False,
            "pod_count": 0,
            "pods": [],
            "error": f"{type(exc).__name__}: {exc}",
        }
    return summarize_runtime_gpu(
        deployment_json,
        pods_json,
        deployment=deployment,
        container_name=container_name,
    )


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


def optional_age_seconds(value: Any, *, now: float) -> float | None:
    ts = parse_ts(value)
    if ts is None:
        return None
    return max(0.0, now - ts)


def child_dict(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    return value if isinstance(value, dict) else {}


def first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def window_metric(observe: dict[str, Any], base: str, hours: Any) -> Any:
    hour_text = str(hours)
    return first_present(
        observe.get(f"{base}_{hour_text}h"),
        observe.get(f"stream_engine_{base}_{hour_text}h"),
    )


def subsystem_last_ok_age(subsystem: dict[str, Any], *, now: float) -> float | None:
    return optional_age_seconds(subsystem.get("last_ok_ts_utc"), now=now)


def boolish(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "ok", "healthy", "running"}
    return bool(value)


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


def write_external_blackbox_metrics(writer: MetricWriter, status: dict[str, Any], *, now: float) -> None:
    current_status = str(status.get("status") or "unknown")
    status_value = {"ok": 1, "failed": 0, "unknown": -1}.get(current_status, -1)
    evidence_at = status.get("evidence_at_utc")
    evidence_age = optional_age_seconds(evidence_at, now=now)
    collector_age = optional_age_seconds(status.get("checked_at_utc"), now=now)
    writer.metric(
        "stream_v3_external_blackbox_ok",
        1 if current_status == "ok" else 0,
        help_text="Compatibility flag for independent external black-box success.",
    )
    writer.metric(
        "stream_v3_external_blackbox_status",
        status_value,
        help_text="Independent external black-box status: ok=1 failed=0 unknown=-1.",
    )
    writer.metric(
        "stream_v3_external_blackbox_sample_available",
        1 if evidence_age is not None else 0,
        help_text="Independent external black-box source sample availability.",
    )
    writer.metric(
        "stream_v3_external_blackbox_age_seconds",
        evidence_age if evidence_age is not None else 0,
        help_text="Age of the oldest latest external source sample across required targets.",
    )
    writer.metric(
        "stream_v3_external_blackbox_collector_age_seconds",
        collector_age if collector_age is not None else 0,
        help_text="Age of the arena-side external black-box import attempt.",
    )
    targets = status.get("targets") if isinstance(status.get("targets"), dict) else {}
    for target_name, target in targets.items():
        if not isinstance(target, dict):
            continue
        labels = {"target": str(target_name)}
        target_status = str(target.get("status") or "unknown")
        writer.metric(
            "stream_v3_external_blackbox_target_status",
            {"ok": 1, "failed": 0, "unknown": -1}.get(target_status, -1),
            labels=labels,
            help_text="Per-target external black-box status: ok=1 failed=0 unknown=-1.",
        )
        writer.metric(
            "stream_v3_external_blackbox_target_fresh_locations",
            target.get("fresh_locations"),
            labels=labels,
        )
        writer.metric(
            "stream_v3_external_blackbox_target_pass_ratio",
            target.get("pass_ratio"),
            labels=labels,
        )
        writer.metric(
            "stream_v3_external_blackbox_target_sample_age_seconds",
            target.get("sample_age_seconds"),
            labels=labels,
        )


def write_map_runtime_metrics(writer: MetricWriter, status: dict[str, Any], *, now: float) -> None:
    checked_at = status.get("checked_at_utc")
    sample_age = optional_age_seconds(checked_at, now=now)
    sample_available = bool(
        status.get("schema")
        in {"stream_v3.map_runtime_monitor.v1", "stream_v3.map_runtime_monitor.v2"}
        and sample_age is not None
    )
    current_status = str(status.get("status") or "unknown")
    writer.metric(
        "stream_v3_map_monitor_sample_available",
        1 if sample_available else 0,
        help_text="Production map runtime monitor sample availability flag.",
    )
    writer.metric(
        "stream_v3_map_monitor_sample_age_seconds",
        sample_age if sample_age is not None else 0,
        help_text="Age of the production map runtime monitor sample.",
    )
    writer.metric(
        "stream_v3_map_monitor_status",
        1,
        labels={"status": current_status},
        help_text="Production map runtime monitor status label.",
    )
    writer.metric(
        "stream_v3_map_monitor_delivery_critical_ok",
        1 if boolish(status.get("delivery_critical_ok")) else 0,
        help_text="Map delivery critical contract aggregate flag.",
    )
    writer.metric(
        "stream_v3_map_monitor_weather_ok",
        1 if boolish(status.get("weather_ok")) else 0,
        help_text="JMA precipitation acquisition and browser-render aggregate flag.",
    )

    readiness = child_dict(status, "readiness")
    conditions = child_dict(status, "conditions")
    render = child_dict(status, "render")
    browser = child_dict(status, "browser")
    precipitation = child_dict(status, "precipitation")
    weather_status = child_dict(precipitation, "status")
    weather_health = child_dict(precipitation, "health")
    precipitation_render = child_dict(precipitation, "render")
    writer.metric(
        "stream_v3_map_runtime_ready",
        1 if boolish(readiness.get("ready")) else 0,
        help_text="Runtime readiness result observed by the map monitor.",
    )
    writer.metric(
        "stream_v3_map_nvenc_active",
        1 if boolish(readiness.get("nvenc_active")) else 0,
        help_text="NVENC active flag observed by runtime readiness.",
    )
    writer.metric(
        "stream_v3_map_rtmp_socket_established",
        1 if boolish(readiness.get("rtmp_socket_established")) else 0,
        help_text="RTMP socket established flag observed by runtime readiness.",
    )
    writer.metric(
        "stream_v3_map_render_ready",
        1 if boolish(conditions.get("render_heartbeat")) else 0,
        help_text="Map render heartbeat contract flag including freshness.",
    )
    writer.metric(
        "stream_v3_map_render_age_seconds",
        render.get("age_sec"),
        help_text="Age of the latest browser render-ready heartbeat.",
    )
    writer.metric(
        "stream_v3_map_tiles_ready",
        1 if boolish(render.get("map_tiles_ready")) else 0,
        help_text="MapLibre tile readiness flag.",
    )
    writer.metric(
        "stream_v3_map_aircraft_sample_ready",
        1 if boolish(render.get("aircraft_sample_ready")) else 0,
        help_text="Recent ADS-B aircraft sample readiness flag.",
    )
    writer.metric(
        "stream_v3_map_browser_contract_ok",
        1 if boolish(browser.get("contract_ok")) else 0,
        help_text="Chromium ADS-B map and SwiftShader launch contract flag.",
    )
    writer.metric(
        "stream_v3_map_webgl2_blocklisted",
        1 if boolish(browser.get("webgl2_blocklisted")) else 0,
        help_text="Chromium browser log contains a WebGL2 blocklist failure.",
    )
    writer.metric(
        "stream_v3_map_webgl_context_fatal",
        1 if boolish(browser.get("context_fatal_failure")) else 0,
        help_text="Chromium browser log contains a fatal WebGL context failure.",
    )
    writer.metric(
        "stream_v3_map_semantic_visual_contract_ok",
        1 if boolish(conditions.get("semantic_visual_contract")) else 0,
        help_text="Browser semantic map sources, layers, UI, aircraft, coverage, and range-ring contract flag.",
    )
    writer.metric(
        "stream_v3_map_asset_identity_ok",
        1 if boolish(conditions.get("asset_identity")) else 0,
        help_text="Browser and server map asset revisions match the source-controlled asset manifest.",
    )
    writer.metric(
        "stream_v3_map_precipitation_available",
        1 if boolish(weather_status.get("available")) else 0,
        help_text="Processed JMA precipitation layer availability flag.",
    )
    writer.metric(
        "stream_v3_map_precipitation_current",
        1 if weather_health.get("state") == "current" else 0,
        help_text="JMA precipitation fetcher current-state flag.",
    )
    writer.metric(
        "stream_v3_map_precipitation_observed_age_seconds",
        precipitation.get("observed_age_sec"),
        help_text="Age of the JMA precipitation analysis observation.",
    )
    writer.metric(
        "stream_v3_map_precipitation_consecutive_failures",
        weather_health.get("consecutive_failures"),
        help_text="Consecutive JMA precipitation fetch failures.",
    )
    writer.metric(
        "stream_v3_map_precipitation_data_ok",
        1 if boolish(conditions.get("precipitation_data_ok")) else 0,
        help_text="JMA precipitation freshness, fetcher-health, and served-generation integrity flag.",
    )
    writer.metric(
        "stream_v3_map_precipitation_generation_integrity",
        1 if boolish(conditions.get("precipitation_generation_integrity")) else 0,
        help_text="All manifest-declared JMA tiles were served with matching digest, size, and PNG dimensions.",
    )
    writer.metric(
        "stream_v3_map_precipitation_render_applied",
        1 if boolish(conditions.get("precipitation_render_applied")) else 0,
        help_text="Browser precipitation render matches the current JMA generation or no-rain state.",
    )
    writer.metric(
        "stream_v3_map_precipitation_validtime_match",
        1 if boolish(conditions.get("precipitation_validtime_match")) else 0,
        help_text="Browser precipitation generation matches the current processed JMA validtime.",
    )
    writer.metric(
        "stream_v3_map_precipitation_expected",
        1 if weather_status.get("has_precipitation") is True else 0,
        help_text="Current processed JMA generation contains visible precipitation pixels.",
    )
    writer.metric(
        "stream_v3_map_precipitation_layer_loaded",
        1 if precipitation_render.get("layer_loaded") is True else 0,
        help_text="Browser reports a precipitation raster source loaded in MapLibre.",
    )

    pod = child_dict(status, "pod")
    containers = child_dict(pod, "containers")
    expected = status.get("expected_containers")
    expected_names = expected if isinstance(expected, list) and expected else list(MAP_EXPECTED_CONTAINERS)
    for name in expected_names:
        container = containers.get(name) if isinstance(containers.get(name), dict) else {}
        labels = {"container": name}
        writer.metric(
            "stream_v3_map_expected_container_present",
            1 if name in containers else 0,
            labels=labels,
            help_text="Expected production map runtime container presence flag.",
        )
        writer.metric(
            "stream_v3_map_container_ready",
            1 if boolish(container.get("ready")) else 0,
            labels=labels,
            help_text="Production map runtime container readiness flag.",
        )
        writer.metric(
            "stream_v3_map_container_restart_count",
            container.get("restart_count"),
            labels=labels,
            help_text="Production map runtime container restart count.",
        )


def write_viewer_synthetic_metrics(writer: MetricWriter, status: dict[str, Any], *, now: float) -> None:
    sample_age = optional_age_seconds(status.get("checked_at_utc"), now=now)
    sample_available = status.get("schema") == "stream_v3.viewer_synthetic.v1" and sample_age is not None
    writer.metric(
        "stream_v3_viewer_synthetic_sample_available",
        1 if sample_available else 0,
        help_text="Public viewer synthetic sample availability flag.",
    )
    writer.metric(
        "stream_v3_viewer_synthetic_sample_age_seconds",
        sample_age if sample_age is not None else 0,
        help_text="Age of the public viewer synthetic sample.",
    )
    writer.metric(
        "stream_v3_viewer_synthetic_status",
        1,
        labels={"status": str(status.get("status") or "unknown")},
        help_text="Public viewer synthetic status label.",
    )
    writer.metric(
        "stream_v3_viewer_synthetic_frame_ok",
        1 if status.get("frame_ok") is True else 0,
        help_text="Public viewer frame capture success flag.",
    )
    writer.metric(
        "stream_v3_viewer_synthetic_black_detected",
        1 if status.get("black_detected") is True else 0,
        help_text="Black video interval detected in the public viewer sample.",
    )
    writer.metric(
        "stream_v3_viewer_synthetic_freeze_detected",
        1 if status.get("freeze_detected") is True else 0,
        help_text="Frozen video interval detected in the public viewer sample.",
    )
    writer.metric(
        "stream_v3_viewer_synthetic_consecutive_probe_failures",
        status.get("consecutive_probe_failures"),
        help_text="Consecutive public viewer capture failures.",
    )
    writer.metric(
        "stream_v3_viewer_synthetic_consecutive_visual_failures",
        status.get("consecutive_visual_failures"),
        help_text="Consecutive public viewer black or frozen frame detections.",
    )


def replace_metric_value(payload: str, name: str, value: Any) -> str:
    prefix = f"{name} "
    rendered = str(as_float(value))
    lines = []
    replaced = False
    for line in payload.splitlines():
        if line.startswith(prefix):
            lines.append(f"{name} {rendered}")
            replaced = True
        else:
            lines.append(line)
    if not replaced:
        writer = MetricWriter()
        writer.metric(name, value)
        lines.extend(writer.render().splitlines())
    return "\n".join(lines) + "\n"


def append_exporter_cache_metrics(
    payload: str,
    *,
    error: str,
    now: float,
    last_success_updated: float,
    last_refresh_duration: float,
) -> str:
    writer = MetricWriter()
    cache_age = max(0.0, now - last_success_updated) if last_success_updated else 0.0
    writer.metric(
        "stream_v3_exporter_cache_age_seconds",
        round(cache_age, 3),
        help_text="Age of last successful exporter payload.",
    )
    writer.metric(
        "stream_v3_exporter_last_refresh_success",
        0 if error else 1,
        help_text="Last exporter refresh success flag.",
    )
    writer.metric(
        "stream_v3_exporter_last_refresh_duration_seconds",
        round(max(0.0, last_refresh_duration), 3),
        help_text="Last exporter refresh duration seconds.",
    )
    writer.metric(
        "stream_v3_exporter_error",
        1 if error else 0,
        labels={"error": error[:120] if error else ""},
        help_text="Exporter error flag.",
    )
    return payload.rstrip("\n") + "\n" + writer.render()


def load_snapshot_payload(
    path: Path,
    *,
    now: float,
    max_age_sec: float,
    required_key: str,
) -> tuple[dict[str, Any] | None, float, str]:
    snapshot = read_json(path)
    if not snapshot:
        return None, 0.0, "missing"
    updated_ts = parse_ts(snapshot.get("updated_at_utc"))
    if updated_ts is None:
        return None, 0.0, "missing updated_at_utc"
    age = max(0.0, now - updated_ts)
    if age > max_age_sec:
        return None, age, f"stale age={round(age, 3)} max={max_age_sec}"
    payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}
    if required_key and required_key not in payload:
        return None, age, f"payload {required_key} missing"
    required_value = payload.get(required_key)
    if required_key == "windows" and not required_value:
        return None, age, "payload windows missing"
    return payload, age, ""


def build_metrics(
    *,
    repo_root: Path,
    state_root: Path,
    timeout_sec: float,
    health_snapshot_file: Path | None = None,
    max_health_snapshot_age_sec: float = 600.0,
    objective_snapshot_file: Path | None = None,
    max_objective_snapshot_age_sec: float = 600.0,
) -> str:
    cli = stream_cli(repo_root)
    now = time.time()
    health_snapshot_used = False
    health_snapshot_age = 0.0
    health_snapshot_error = ""
    health: dict[str, Any] | None = None
    if health_snapshot_file is not None:
        health, health_snapshot_age, health_snapshot_error = load_snapshot_payload(
            health_snapshot_file,
            now=now,
            max_age_sec=max_health_snapshot_age_sec,
            required_key="windows",
        )
        health_snapshot_used = health is not None
    if health is None:
        health = run_json(
            repo_root,
            state_root,
            [str(cli), "health-summary", "--windows", "1,8,24", "--json"],
            timeout_sec=timeout_sec,
        )
    objective_snapshot_used = False
    objective_snapshot_age = 0.0
    objective_snapshot_error = ""
    objective: dict[str, Any] | None = None
    if objective_snapshot_file is not None:
        objective, objective_snapshot_age, objective_snapshot_error = load_snapshot_payload(
            objective_snapshot_file,
            now=now,
            max_age_sec=max_objective_snapshot_age_sec,
            required_key="metrics",
        )
        objective_snapshot_used = objective is not None
    if objective is None:
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
    monitoring_watchdog = read_json(state_root / "monitoring_watchdog_state.json")
    adsb_freshness = read_json(state_root / "watchdog" / "adsb_freshness_state.json")
    pulse_health = read_json(state_root / "watchdog" / "pulse_health_state.json")
    recovery_stage = read_json(state_root / "watchdog" / "recovery_stage_state.json")
    slo_snapshot = read_json(state_root / "slo_snapshot.json")
    runtime_state = read_json(state_root / "stream_runtime_state_remote.json")
    map_runtime = read_json(state_root / "map_runtime_status.json")
    viewer_synthetic = read_json(state_root / "viewer_synthetic_status.json")
    operational_reliability = read_json(state_root / "operational_reliability_status.json")
    reliability_burn = read_json(state_root / "operational_reliability_burn_status.json")
    external_blackbox = read_json(state_root / "external_blackbox_status.json")
    rendering = child_dict(subsystems, "rendering")
    music = child_dict(subsystems, "music")
    local_delivery = child_dict(subsystems, "local_delivery")
    host_memory = host_memory_snapshot()
    runtime_memory = runtime_memory_snapshot(timeout_sec=timeout_sec, now=now)
    runtime_gpu = runtime_gpu_snapshot(timeout_sec=timeout_sec, now=now)
    upload_fallback = upload_fallback_by_window(state_root, now=now)
    latest_tcp_sample = latest_tcp_send_sample(state_root, now=now)

    writer = MetricWriter()
    writer.metric("stream_v3_exporter_up", 1, help_text="Exporter scrape success.")
    reliability_age = age_seconds(operational_reliability.get("checked_at_utc"), now=now)
    writer.metric(
        "stream_v3_operational_reliability_rollup_age_seconds",
        reliability_age,
        help_text="Age of the durable SLI and Same URL rollup.",
    )
    writer.metric(
        "stream_v3_operational_reliability_rollup_ok",
        1 if operational_reliability.get("status") == "ok" else 0,
        help_text="Durable operational reliability rollup health.",
    )
    gates = operational_reliability.get("formal_gates") if isinstance(operational_reliability.get("formal_gates"), dict) else {}
    for family, gate in gates.items():
        if not isinstance(gate, dict):
            continue
        labels = {"family": str(family)}
        status_value = {"met": 1, "breached": 0, "unknown": -1}.get(
            str(gate.get("compliance_status") or "unknown"), -1
        )
        writer.metric(
            "stream_v3_formal_sli_compliance_status",
            status_value,
            labels=labels,
            help_text="Formal SLI status: met=1 breached=0 unknown=-1.",
        )
        writer.metric("stream_v3_formal_sli_coverage_pct", gate.get("coverage_pct"), labels=labels)
        writer.metric("stream_v3_formal_sli_source_freshness_pct", gate.get("source_freshness_pct"), labels=labels)
        writer.metric(
            "stream_v3_formal_sli_source_disagreement",
            1 if gate.get("source_disagreement") is True else 0,
            labels=labels,
        )
    revision = operational_reliability.get("revision") if isinstance(operational_reliability.get("revision"), dict) else {}
    writer.metric("stream_v3_monitor_worktree_clean", 1 if revision.get("worktree_clean") is True else 0)
    writer.metric(
        "stream_v3_monitor_revision_matches_deployed",
        1 if revision.get("matches_expected_revision") is True else 0,
    )
    burn_alerts = reliability_burn.get("multi_window_burn_alerts") if isinstance(reliability_burn.get("multi_window_burn_alerts"), list) else []
    writer.metric("stream_v3_multi_window_burn_alerts", len(burn_alerts))
    writer.metric(
        "stream_v3_multi_window_burn_evaluation_age_seconds",
        age_seconds(reliability_burn.get("checked_at_utc"), now=now),
    )
    write_external_blackbox_metrics(writer, external_blackbox, now=now)
    write_map_runtime_metrics(writer, map_runtime, now=now)
    write_viewer_synthetic_metrics(writer, viewer_synthetic, now=now)
    writer.metric("stream_v3_health_snapshot_used", 1 if health_snapshot_used else 0, help_text="Health summary snapshot used flag.")
    writer.metric("stream_v3_health_snapshot_age_seconds", health_snapshot_age, help_text="Health summary snapshot age seconds.")
    writer.metric(
        "stream_v3_health_snapshot_available",
        1 if health_snapshot_used else 0,
        labels={"reason": "" if health_snapshot_used else health_snapshot_error[:120]},
        help_text="Fresh health summary snapshot availability flag.",
    )
    writer.metric("stream_v3_objective_snapshot_used", 1 if objective_snapshot_used else 0, help_text="Objective SLI snapshot used flag.")
    writer.metric("stream_v3_objective_snapshot_age_seconds", objective_snapshot_age, help_text="Objective SLI snapshot age seconds.")
    writer.metric(
        "stream_v3_objective_snapshot_available",
        1 if objective_snapshot_used else 0,
        labels={"reason": "" if objective_snapshot_used else objective_snapshot_error[:120]},
        help_text="Fresh objective SLI snapshot availability flag.",
    )
    watchdog_checks = monitoring_watchdog.get("checks") if isinstance(monitoring_watchdog.get("checks"), dict) else {}
    watchdog_all_ok = bool(watchdog_checks) and all(
        bool(item.get("ok")) for item in watchdog_checks.values() if isinstance(item, dict)
    )
    writer.metric("stream_v3_monitoring_watchdog_all_ok", 1 if watchdog_all_ok else 0, help_text="Monitoring watchdog aggregate ok flag.")
    writer.metric(
        "stream_v3_monitoring_watchdog_age_seconds",
        age_seconds(monitoring_watchdog.get("updated_at_utc"), now=now),
        help_text="Age of monitoring watchdog state.",
    )
    writer.metric(
        "stream_v3_monitoring_watchdog_repair_enabled",
        monitoring_watchdog.get("repair_enabled"),
        help_text="Monitoring watchdog repair enabled flag.",
    )
    writer.metric(
        "stream_v3_monitoring_watchdog_recent_repair_count",
        len(monitoring_watchdog.get("repairs") or []),
        help_text="Monitoring watchdog repair actions in latest run.",
    )
    for name, item in watchdog_checks.items():
        if not isinstance(item, dict):
            continue
        labels = {"check": name, "repair": item.get("repair", "")}
        writer.metric(
            "stream_v3_monitoring_watchdog_check_ok",
            item.get("ok"),
            labels=labels,
            help_text="Monitoring watchdog per-check ok flag.",
        )
        writer.metric(
            "stream_v3_monitoring_watchdog_check_fail_count",
            item.get("fail_count"),
            labels=labels,
            help_text="Monitoring watchdog consecutive failure count by check.",
        )

    for window in health.get("windows", []):
        if not isinstance(window, dict):
            continue
        observe = window.get("observe") if isinstance(window.get("observe"), dict) else {}
        checks = observe.get("checks") if isinstance(observe.get("checks"), dict) else {}
        labels = {"window_hours": str(window.get("hours", ""))}
        writer.metric("stream_v3_health_pass", window.get("pass", observe.get("pass")), labels=labels, help_text="Health summary pass by window.")
        writer.metric("stream_v3_current_fail", checks.get("current_fail"), labels=labels, help_text="Current failure flag by window.")
        writer.metric("stream_v3_historical_degraded", checks.get("historical_degraded"), labels=labels, help_text="Historical degraded flag by window.")
        writer.metric(
            "stream_v3_youtube_warn_count",
            checks.get("youtube_warn_count"),
            labels=labels,
            help_text="Watchdog warning count by window; supporting history, not the YouTube input-quality SLI.",
        )
        writer.metric("stream_v3_fast_recovery_restart_count", observe.get("fast_recovery_restart_count"), labels=labels, help_text="Fast recovery restart count by window.")
        ffmpeg_clusters = window_metric(observe, "ffmpeg_restart_incident_clusters", window.get("hours"))
        if ffmpeg_clusters is not None:
            writer.metric("stream_v3_ffmpeg_restart_incident_clusters", ffmpeg_clusters, labels=labels, help_text="FFmpeg restart incident cluster count.")
        rtmps_ssl_tls_count = window_metric(observe, "rtmps_ssl_tls_count", window.get("hours"))
        if rtmps_ssl_tls_count is not None:
            writer.metric("stream_v3_rtmps_ssl_tls_count", rtmps_ssl_tls_count, labels=labels, help_text="RTMPS SSL/TLS event count.")
        fallback = upload_fallback.get(str(window.get("hours", "")), {})
        p95 = observe.get("ffmpeg_tcp_send_mbps_24h_p95")
        max_mbps = observe.get("ffmpeg_tcp_send_mbps_24h_max")
        over_budget = observe.get("ffmpeg_tcp_send_mbps_24h_over_budget_duration_sec")
        fallback_has_samples = as_float(fallback.get("sample_count")) > 0
        writer.metric("stream_v3_upload_p95_mbps", fallback.get("p95") if fallback_has_samples else p95, labels=labels, help_text="FFmpeg TCP send p95 Mbps.")
        writer.metric("stream_v3_upload_max_mbps", fallback.get("max") if fallback_has_samples else max_mbps, labels=labels, help_text="FFmpeg TCP send max Mbps.")
        writer.metric("stream_v3_upload_over_budget_seconds", fallback.get("over_budget_sec") if fallback_has_samples else over_budget, labels=labels, help_text="Seconds above upload budget.")
        writer.metric("stream_v3_fast_mode_active", observe.get("fast_mode_current_active"), labels=labels, help_text="Fast mode active flag.")
        open_day_latest = child_dict(child_dict(observe, "api_cost_reports"), "open_day_latest")
        if str(window.get("hours", "")) == "24":
            writer.metric("stream_v3_api_open_day_units", open_day_latest.get("units"), labels=labels, help_text="YouTube API units for current PT day.")
            writer.metric("stream_v3_youtube_api_open_day_units", open_day_latest.get("units"), help_text="YouTube API units for current PT day.")
            writer.metric("stream_v3_youtube_api_open_day_report_fresh", open_day_latest.get("fresh"), help_text="YouTube API open PT day report fresh flag.")
            writer.metric("stream_v3_youtube_api_open_day_report_age_seconds", open_day_latest.get("effective_end_age_sec"), help_text="Age of YouTube API open PT day report.")

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
    latest_memory_host = memory.get("host") if isinstance(memory.get("host"), dict) else {}
    swap_capacity = (
        latest_memory_host.get("swap_capacity")
        if isinstance(latest_memory_host.get("swap_capacity"), dict)
        else {}
    )
    swap_capacity_severity = str(swap_capacity.get("severity") or "unknown")
    swap_capacity_level = {"ok": 0, "observe": 1, "warn": 2, "critical": 3}.get(
        swap_capacity_severity,
        -1,
    )
    memory_current_ok = latest_memory_overall.get("severity") == "ok"
    if not latest_memory_overall and host_memory:
        memory_current_ok = as_float(host_memory.get("mem_available_ratio")) >= 0.10
    writer.metric("stream_v3_notify_pending", count_pending_outbox(state_root / "stream_notify_outbox.jsonl"), help_text="Pending notification messages.")
    writer.metric("stream_v3_memory_current_ok", 1 if memory_current_ok else 0, help_text="Monitoring host memory guardrail ok flag.")
    writer.metric("stream_v3_monitor_host_memory_current_ok", 1 if memory_current_ok else 0, help_text="Monitoring host memory guardrail ok flag.")
    writer.metric(
        "stream_v3_host_swap_capacity_level",
        swap_capacity_level,
        help_text="Resident swap capacity level: -1 unknown, 0 ok, 1 observe, 2 warn, 3 critical; not current pressure by itself.",
    )
    for capacity_status in ("ok", "observe", "warn", "critical"):
        writer.metric(
            "stream_v3_host_swap_capacity_status",
            1 if swap_capacity_severity == capacity_status else 0,
            labels={"status": capacity_status},
            help_text="Resident swap capacity classification; not current pressure by itself.",
        )
    writer.metric("stream_v3_maintenance_active", notify_state.get("maintenance_active"), help_text="Maintenance mode active flag.")
    writer.metric("stream_v3_notify_active_incidents", len(notify_state.get("active") or {}), help_text="Active notification incidents.")
    writer.metric("stream_v3_runtime_memory_current_ok", 1 if runtime_memory.get("current_ok") else 0, help_text="stream-v3-runtime Pod memory guardrail ok flag.")
    writer.metric("stream_v3_runtime_memory_sample_available", 1 if runtime_memory.get("available") else 0, help_text="stream-v3-runtime Pod memory sample availability.")
    writer.metric("stream_v3_runtime_memory_sample_age_seconds", runtime_memory.get("sample_age_seconds"), help_text="Age of stream-v3-runtime Pod memory sample.")
    writer.metric("stream_v3_runtime_memory_warning_ratio", runtime_memory.get("warning_ratio"), help_text="stream-v3-runtime container memory warning ratio.")
    writer.metric("stream_v3_runtime_memory_container_count", len(runtime_memory.get("containers") or []), help_text="stream-v3-runtime memory container sample count.")
    writer.metric("stream_v3_runtime_gpu_status_ok", 1 if runtime_gpu.get("status_ok") else 0, help_text="stream-v3-runtime stream-engine GPU status ok flag.")
    writer.metric("stream_v3_runtime_gpu_sample_available", 1 if runtime_gpu.get("available") else 0, help_text="stream-v3-runtime GPU pod-status sample availability.")
    writer.metric("stream_v3_runtime_gpu_restart_blocked", 1 if runtime_gpu.get("restart_blocked") else 0, help_text="Runtime restart blocked because stream-engine is waiting on GPU runtime failure.")
    writer.metric("stream_v3_runtime_gpu_driver_mismatch", 1 if runtime_gpu.get("driver_mismatch") else 0, help_text="stream-engine current pod state reports NVIDIA driver/library mismatch.")
    writer.metric("stream_v3_runtime_gpu_runtime_error", 1 if runtime_gpu.get("gpu_runtime_error") else 0, help_text="stream-engine current pod state reports a GPU runtime error.")
    writer.metric("stream_v3_runtime_gpu_requested", 1 if runtime_gpu.get("gpu_requested") else 0, help_text="stream-engine requests an NVIDIA GPU.")
    writer.metric("stream_v3_runtime_gpu_stream_engine_ready", 1 if runtime_gpu.get("stream_engine_ready") else 0, help_text="stream-engine container ready flag from pod status.")
    writer.metric("stream_v3_runtime_gpu_stream_engine_running", 1 if runtime_gpu.get("stream_engine_running") else 0, help_text="stream-engine container running flag from pod status.")
    writer.metric("stream_v3_runtime_gpu_container_waiting", 1 if runtime_gpu.get("container_waiting") else 0, help_text="stream-engine container waiting flag from pod status.")
    writer.metric("stream_v3_runtime_gpu_pod_count", runtime_gpu.get("pod_count"), help_text="stream-v3-runtime pod count included in GPU status.")
    writer.metric("stream_v3_runtime_gpu_status", 1, labels={"status": runtime_gpu.get("status", "unknown")}, help_text="stream-v3-runtime GPU status label.")
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
    for item in runtime_gpu.get("pods") or []:
        if not isinstance(item, dict):
            continue
        labels = {
            "pod": item.get("pod", ""),
            "container": item.get("container", ""),
            "state": item.get("state", "unknown"),
            "reason": item.get("reason", ""),
        }
        writer.metric("stream_v3_runtime_gpu_pod_state", 1, labels=labels, help_text="stream-engine GPU-relevant current pod state.")

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
    try:
        input_quality_max_age_sec = max(
            1,
            int(
                os.environ.get(
                    "STREAM_V3_YOUTUBE_INPUT_QUALITY_MAX_AGE_SEC",
                    str(DEFAULT_INPUT_QUALITY_MAX_AGE_SEC),
                )
            ),
        )
    except ValueError:
        input_quality_max_age_sec = DEFAULT_INPUT_QUALITY_MAX_AGE_SEC
    input_quality = classify_input_quality_sample(
        youtube_watchdog,
        now_ts=int(now),
        max_age_sec=input_quality_max_age_sec,
    )
    writer.metric(
        "stream_v3_youtube_input_quality_probe_fresh",
        input_quality.get("fresh"),
        help_text="Fresh YouTube OAuth input-quality probe flag.",
    )
    writer.metric(
        "stream_v3_youtube_input_quality_eligible",
        input_quality.get("eligible"),
        help_text="Input-quality SLI eligibility: fresh OAuth probe, active stream, and connected local ingest.",
    )
    writer.metric(
        "stream_v3_youtube_input_quality_good",
        input_quality.get("good"),
        help_text="Eligible YouTube input-quality sample is good with healthStatus=good and no warning/error issue.",
    )
    writer.metric(
        "stream_v3_youtube_input_quality_issue_count",
        input_quality.get("issue_count"),
        help_text="YouTube live stream configuration issue count in the latest OAuth probe.",
    )
    writer.metric(
        "stream_v3_youtube_input_quality_warning_or_error_issue_count",
        input_quality.get("warning_or_worse_count"),
        help_text="Persisted YouTube configuration issues with warning or error severity.",
    )
    writer.metric(
        "stream_v3_youtube_input_quality_state",
        1,
        labels={
            "classification": input_quality.get("classification", "unknown"),
            "health_status": input_quality.get("health_status", ""),
        },
        help_text="Current YouTube input-quality classification.",
    )

    writer.metric("stream_v3_stream_watchdog_ok", 1 if stream_watchdog.get("status") == "ok" else 0, help_text="Local stream watchdog ok flag.")
    runtime_heartbeat_age = first_present(
        local_delivery.get("runtime_age_sec"),
        stream_watchdog.get("runtime_snapshot_age_sec"),
        optional_age_seconds(runtime_state.get("updated_at_utc"), now=now),
    )
    ffmpeg_present = (
        "ffmpeg_alive" in (local_delivery.get("evidence") or [])
        or boolish(runtime_state.get("status") == "running")
        or boolish(youtube_watchdog.get("stream_active"))
    )
    writer.metric("stream_v3_stream_watchdog_ffmpeg_count", 1 if ffmpeg_present else 0, help_text="Remote runtime FFmpeg presence count.")
    writer.metric("stream_v3_runtime_ffmpeg_present", 1 if ffmpeg_present else 0, help_text="Remote runtime FFmpeg presence flag.")
    if runtime_heartbeat_age is not None:
        writer.metric("stream_v3_stream_watchdog_runtime_snapshot_age_seconds", runtime_heartbeat_age, help_text="Runtime heartbeat age seconds.")
        writer.metric("stream_v3_runtime_heartbeat_age_seconds", runtime_heartbeat_age, help_text="Runtime heartbeat age seconds.")
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
    resource_assessment = (
        resource_memory.get("assessment")
        if isinstance(resource_memory.get("assessment"), dict)
        else {}
    )
    writer.metric("stream_v3_host_mem_available_mib", host_mem.get("mem_available_mb"), help_text="Host MemAvailable MiB.")
    writer.metric("stream_v3_host_mem_available_ratio", host_mem.get("mem_available_ratio"), help_text="Host MemAvailable ratio.")
    writer.metric("stream_v3_host_swap_used_mib", host_mem.get("swap_used_mb"), help_text="Host swap used MiB.")
    writer.metric("stream_v3_host_swap_used_ratio", host_mem.get("swap_used_ratio"), help_text="Host swap used ratio.")
    writer.metric("stream_v3_host_memory_pressure_some_avg10", mem_pressure.get("some_avg10"), help_text="Host memory PSI some avg10.")
    writer.metric("stream_v3_host_memory_pressure_full_avg10", mem_pressure.get("full_avg10"), help_text="Host memory PSI full avg10.")
    writer.metric("stream_v3_host_pgmajfault_delta_per_min", vm_activity.get("pgmajfault_delta_per_min"), help_text="Major page faults per minute.")
    writer.metric("stream_v3_host_pswpin_delta_per_min", vm_activity.get("pswpin_delta_per_min"), help_text="Swap-in pages per minute.")
    writer.metric("stream_v3_resource_memory_age_seconds", age_seconds(resource_memory.get("ts_utc"), now=now), help_text="Age of resource memory sample.")
    writer.metric(
        "stream_v3_resource_memory_baseline_ready",
        1 if resource_assessment.get("baseline_ready") else 0,
        help_text="Seven-day resource-memory baseline readiness flag.",
    )
    writer.metric(
        "stream_v3_resource_memory_baseline_coverage_seconds",
        resource_assessment.get("baseline_coverage_sec"),
        help_text="Resource-memory baseline coverage seconds.",
    )
    resource_status = str(resource_assessment.get("status") or "unknown")
    for assessment_status in ("ok", "observe", "warn", "degraded", "critical"):
        writer.metric(
            "stream_v3_resource_memory_assessment_status",
            1 if resource_status == assessment_status else 0,
            labels={"status": assessment_status},
            help_text="Diagnostic resource-memory assessment status; memory alone cannot authorize runtime recovery.",
        )
    for unit, payload in cgroups.items():
        if not isinstance(payload, dict):
            continue
        writer.metric("stream_v3_cgroup_memory_sample_available", 1 if payload.get("available") else 0, labels={"unit": unit}, help_text="Systemd cgroup memory sample availability.")
        if not payload.get("available"):
            continue
        labels = {"unit": unit}
        writer.metric("stream_v3_cgroup_memory_current_mib", payload.get("memory_current_mb"), labels=labels, help_text="Cgroup current memory MiB.")
        writer.metric("stream_v3_cgroup_memory_peak_mib", payload.get("memory_peak_mb"), labels=labels, help_text="Cgroup peak memory MiB.")
        writer.metric("stream_v3_cgroup_swap_current_mib", payload.get("memory_swap_current_mb"), labels=labels, help_text="Cgroup current swap MiB.")

    adsb_last_change_age = optional_age_seconds(adsb_freshness.get("last_change_ts"), now=now)
    adsb_sample_age = optional_age_seconds(
        first_present(adsb_freshness.get("sample_ts"), adsb_freshness.get("ts_utc")),
        now=now,
    )
    adsb_rendering_evidence_age = subsystem_last_ok_age(rendering, now=now)
    adsb_available = adsb_last_change_age is not None
    adsb_source_status = str(adsb_freshness.get("status") or "").strip().lower()
    adsb_source_ok = adsb_available and adsb_source_status in {"", "ok", "healthy"}
    adsb_motion_ok = (
        boolish(rendering.get("aircraft_messages_moving", True))
        or boolish(rendering.get("aircraft_positions_moving", True))
    )
    adsb_ok = (
        adsb_source_ok
        and rendering.get("state") == "healthy"
        and boolish(rendering.get("aircraft_json_ok", True))
        and adsb_motion_ok
        and boolish(rendering.get("stream1090_report_ok", True))
        and boolish(rendering.get("upstream_stream1090_report_ok", True))
    )
    writer.metric(
        "stream_v3_adsb_evidence_available",
        1 if adsb_available else 0,
        help_text="Displayed ADS-B source evidence availability flag.",
    )
    writer.metric(
        "stream_v3_adsb_rendering_ok",
        1 if adsb_ok else 0,
        help_text="Rendering subsystem and displayed ADS-B source are healthy.",
    )
    writer.metric("stream_v3_adsb_messages_moving", rendering.get("aircraft_messages_moving"), help_text="ADS-B aircraft message movement flag.")
    writer.metric("stream_v3_adsb_positions_moving", rendering.get("aircraft_positions_moving"), help_text="ADS-B aircraft position movement flag.")
    if adsb_last_change_age is not None:
        writer.metric(
            "stream_v3_adsb_evidence_age_seconds",
            adsb_last_change_age,
            help_text="Age since the displayed ADS-B source message counter last changed.",
        )
        writer.metric(
            "stream_v3_adsb_source_age_seconds",
            adsb_last_change_age,
            help_text="Age since the displayed ADS-B source message counter last changed.",
        )
        writer.metric("stream_v3_adsb_messages_last_change_age_seconds", adsb_last_change_age, help_text="Age since ADS-B message count last changed.")
    if adsb_sample_age is not None:
        writer.metric(
            "stream_v3_adsb_source_sample_age_seconds",
            adsb_sample_age,
            help_text="Age of the latest displayed ADS-B source probe.",
        )
    if adsb_rendering_evidence_age is not None:
        writer.metric(
            "stream_v3_adsb_rendering_evidence_age_seconds",
            adsb_rendering_evidence_age,
            help_text="Age of the latest healthy rendering subsystem evidence.",
        )

    audio_fail_count = as_float(first_present(pulse_health.get("dj_missing_count"), music.get("audio_fail_count")), 0.0)
    audio_fail_count += as_float(pulse_health.get("capture_missing_count"), 0.0)
    audio_fail_count += as_float(pulse_health.get("dj_latency_high_count"), 0.0)
    audio_fail_count += as_float(pulse_health.get("capture_latency_high_count"), 0.0)
    audio_fail_count += as_float(music.get("pulse_source_missing_count"), 0.0)
    audio_stage = first_present(recovery_stage.get("audio_stage"), 0)
    pulse_stage = first_present(recovery_stage.get("pulse_stage"), 0)
    audio_fault_count = audio_fail_count + as_float(audio_stage) + as_float(pulse_stage)
    if music and music.get("state") != "healthy":
        audio_fault_count += 1
    audio_available = bool(music) or bool(pulse_health) or bool(recovery_stage)
    audio_age = first_present(subsystem_last_ok_age(music, now=now), optional_age_seconds(recovery_stage.get("audio_last_ts"), now=now))
    writer.metric("stream_v3_audio_evidence_available", 1 if audio_available else 0, help_text="Audio evidence availability flag.")
    writer.metric("stream_v3_audio_ok", 1 if audio_available and audio_fault_count <= 0 else 0, help_text="Audio evidence ok flag.")
    writer.metric("stream_v3_audio_fault_count", audio_fault_count, help_text="Audio fault count from current evidence.")
    if audio_age is not None:
        writer.metric("stream_v3_audio_evidence_age_seconds", audio_age, help_text="Age of latest healthy audio evidence.")

    writer.metric("stream_v3_slo_snapshot_available", 1 if slo_snapshot else 0, help_text="SLO snapshot availability flag.")
    if slo_snapshot:
        writer.metric("stream_v3_slo_snapshot_age_seconds", age_seconds(slo_snapshot.get("ts_utc"), now=now), help_text="SLO snapshot age seconds.")
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
    parser.add_argument("--cache-sec", type=float, default=60.0)
    parser.add_argument("--timeout-sec", type=float, default=45.0)
    parser.add_argument("--health-snapshot-file", type=Path, default=None)
    parser.add_argument("--max-health-snapshot-age-sec", type=float, default=600.0)
    parser.add_argument("--objective-snapshot-file", type=Path, default=None)
    parser.add_argument("--max-objective-snapshot-age-sec", type=float, default=600.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cache = MetricsCache(
        repo_root=args.repo_root,
        state_root=args.state_root,
        ttl_sec=args.cache_sec,
        timeout_sec=args.timeout_sec,
        health_snapshot_file=args.health_snapshot_file,
        max_health_snapshot_age_sec=args.max_health_snapshot_age_sec,
        objective_snapshot_file=args.objective_snapshot_file,
        max_objective_snapshot_age_sec=args.max_objective_snapshot_age_sec,
    )
    server = ThreadingHTTPServer((args.host, args.port), make_handler(cache))
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
