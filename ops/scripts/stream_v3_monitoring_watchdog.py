#!/usr/bin/env python3
"""Self-check the public stream_v3 observability path."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from stream_v3_prometheus_exporter import (  # noqa: E402
    HEALTH_SUMMARY_SNAPSHOT,
    OBJECTIVE_SLI_SNAPSHOT,
    default_repo_root,
    default_state_root,
    snapshot_candidates,
)


DEFAULT_REQUIRED_METRICS = (
    "stream_v3_exporter_up",
    "stream_v3_health_pass",
    "stream_v3_youtube_watchdog_healthy",
    "stream_v3_recovery_action_pending",
)


def iso_now(now: float | None = None) -> str:
    return datetime.fromtimestamp(time.time() if now is None else now, timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


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


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError:
        return {}
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def http_get_text(url: str, *, timeout_sec: float) -> tuple[int, str]:
    request = Request(url, headers={"User-Agent": "stream-v3-monitoring-watchdog/1.0"})
    with urlopen(request, timeout=timeout_sec) as response:  # noqa: S310 - operator-configured local URLs
        body = response.read().decode("utf-8", errors="replace")
        return int(getattr(response, "status", 200)), body


def with_default_path(url: str, path: str) -> str:
    parsed = urlparse(url)
    if parsed.path and parsed.path != "/":
        return url
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def check_http(url: str, *, timeout_sec: float, name: str) -> dict[str, Any]:
    if not url:
        return {"ok": True, "skipped": True, "detail": "not configured"}
    started = time.monotonic()
    try:
        status, body = http_get_text(url, timeout_sec=timeout_sec)
    except (OSError, URLError, TimeoutError) as exc:
        return {"ok": False, "status": 0, "elapsed_ms": elapsed_ms(started), "detail": str(exc)}
    return {
        "ok": 200 <= status < 300,
        "status": status,
        "elapsed_ms": elapsed_ms(started),
        "detail": f"{name} returned HTTP {status}",
        "body_prefix": body[:120],
    }


def elapsed_ms(started: float) -> int:
    return int(round((time.monotonic() - started) * 1000))


def metric_present(metrics: str, name: str) -> bool:
    return re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+", metrics, flags=re.MULTILINE) is not None


def metric_value(metrics: str, name: str) -> float | None:
    match = re.search(rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([-+0-9.eE]+)", metrics, flags=re.MULTILINE)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def check_exporter_metrics(
    exporter_url: str,
    *,
    timeout_sec: float,
    required_metrics: tuple[str, ...],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not exporter_url:
        skipped = {"ok": True, "skipped": True, "detail": "not configured"}
        return skipped, skipped
    started = time.monotonic()
    try:
        status, body = http_get_text(exporter_url, timeout_sec=timeout_sec)
    except (OSError, URLError, TimeoutError) as exc:
        failed = {"ok": False, "status": 0, "elapsed_ms": elapsed_ms(started), "detail": str(exc)}
        return failed, {"ok": False, "missing": list(required_metrics), "detail": "exporter fetch failed"}

    http_check = {
        "ok": 200 <= status < 300,
        "status": status,
        "elapsed_ms": elapsed_ms(started),
        "detail": f"exporter returned HTTP {status}",
    }
    missing = [name for name in required_metrics if not metric_present(body, name)]
    exporter_up = metric_value(body, "stream_v3_exporter_up")
    contract_check = {
        "ok": http_check["ok"] and not missing and exporter_up == 1.0,
        "missing": missing,
        "exporter_up": exporter_up,
        "required_metric_count": len(required_metrics),
        "detail": "required public metrics present" if not missing else "required public metrics missing",
    }
    return http_check, contract_check


def snapshot_age_seconds(path: Path, *, now: float) -> float | None:
    payload = read_json(path)
    snapshot = payload.get("_snapshot") if isinstance(payload.get("_snapshot"), dict) else {}
    ts = parse_ts(snapshot.get("snapshot_ts_utc") or payload.get("ts_utc"))
    if ts is not None:
        return max(0.0, now - ts)
    try:
        return max(0.0, now - path.stat().st_mtime)
    except OSError:
        return None


def check_snapshots(state_root: Path, *, now: float, max_age_sec: float) -> dict[str, Any]:
    details: dict[str, Any] = {}
    ok = True
    for name in (HEALTH_SUMMARY_SNAPSHOT, OBJECTIVE_SLI_SNAPSHOT):
        selected = next((path for path in snapshot_candidates(state_root, name) if path.exists()), None)
        if selected is None:
            ok = False
            details[name] = {"ok": False, "detail": "missing"}
            continue
        age = snapshot_age_seconds(selected, now=now)
        item_ok = age is not None and age <= max_age_sec
        ok = ok and item_ok
        details[name] = {
            "ok": item_ok,
            "path": str(selected),
            "age_seconds": round(float(age or 0.0), 3),
            "max_age_seconds": max_age_sec,
        }
    return {"ok": ok, "snapshots": details}


def run_repair(command: str, *, timeout_sec: float) -> dict[str, Any]:
    if not command.strip():
        return {"ok": False, "detail": "repair command not configured"}
    started = time.monotonic()
    completed = subprocess.run(
        shlex.split(command),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_sec,
        check=False,
    )
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "elapsed_ms": elapsed_ms(started),
        "stdout_tail": completed.stdout.strip()[-500:],
        "stderr_tail": completed.stderr.strip()[-500:],
    }


def load_repair_count(state_file: Path) -> int:
    previous = read_json(state_file)
    try:
        return int(previous.get("repair_count", 0) or 0)
    except (TypeError, ValueError):
        return 0


def split_metrics(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def run_checks(
    *,
    state_root: Path,
    state_file: Path,
    exporter_url: str,
    prometheus_url: str,
    grafana_url: str,
    timeout_sec: float,
    snapshot_max_age_sec: float,
    required_metrics: tuple[str, ...],
    repair_enabled: bool,
    repair_command: str,
    repair_timeout_sec: float,
    now: float | None = None,
) -> dict[str, Any]:
    current_time = time.time() if now is None else now
    exporter_http, metrics_contract = check_exporter_metrics(
        exporter_url,
        timeout_sec=timeout_sec,
        required_metrics=required_metrics,
    )
    checks = {
        "exporter_http": exporter_http,
        "metrics_contract": metrics_contract,
        "prometheus_ready": check_http(with_default_path(prometheus_url, "/-/ready"), timeout_sec=timeout_sec, name="prometheus")
        if prometheus_url
        else {"ok": True, "skipped": True, "detail": "not configured"},
        "grafana_health": check_http(with_default_path(grafana_url, "/api/health"), timeout_sec=timeout_sec, name="grafana")
        if grafana_url
        else {"ok": True, "skipped": True, "detail": "not configured"},
        "snapshot_freshness": check_snapshots(state_root, now=current_time, max_age_sec=snapshot_max_age_sec),
    }
    ok = all(bool(item.get("ok")) for item in checks.values())
    repair_attempted = False
    repair_result: dict[str, Any] = {}
    repair_count = load_repair_count(state_file)
    if not ok and repair_enabled:
        repair_attempted = True
        repair_count += 1
        repair_result = run_repair(repair_command, timeout_sec=repair_timeout_sec)
    payload = {
        "ts_utc": iso_now(current_time),
        "ok": ok,
        "checks": checks,
        "required_metrics": list(required_metrics),
        "repair_enabled": repair_enabled,
        "repair_attempted": repair_attempted,
        "repair_count": repair_count,
        "repair_result": repair_result,
    }
    atomic_write_json(state_file, payload)
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo_root = default_repo_root()
    state_root = default_state_root(repo_root)
    required_metrics = os.environ.get("STREAM_V3_MONITORING_REQUIRED_METRICS", ",".join(DEFAULT_REQUIRED_METRICS))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, default=state_root)
    parser.add_argument("--state-file", type=Path, default=None)
    parser.add_argument("--exporter-url", default=os.environ.get("STREAM_V3_MONITORING_EXPORTER_URL", "http://127.0.0.1:9108/metrics"))
    parser.add_argument("--prometheus-url", default=os.environ.get("STREAM_V3_MONITORING_PROMETHEUS_URL", ""))
    parser.add_argument("--grafana-url", default=os.environ.get("STREAM_V3_MONITORING_GRAFANA_URL", ""))
    parser.add_argument("--timeout-sec", type=float, default=float(os.environ.get("STREAM_V3_MONITORING_TIMEOUT_SEC", "5")))
    parser.add_argument("--snapshot-max-age-sec", type=float, default=float(os.environ.get("STREAM_V3_SNAPSHOT_MAX_AGE_SEC", "600")))
    parser.add_argument("--required-metrics", default=required_metrics)
    parser.add_argument("--repair", action="store_true", default=env_bool("STREAM_V3_MONITORING_REPAIR", False))
    parser.add_argument("--repair-command", default=os.environ.get("STREAM_V3_MONITORING_REPAIR_COMMAND", ""))
    parser.add_argument("--repair-timeout-sec", type=float, default=float(os.environ.get("STREAM_V3_MONITORING_REPAIR_TIMEOUT_SEC", "60")))
    parser.add_argument("--soft-exit", action="store_true", help="return 0 even when checks fail")
    parser.add_argument("--json", action="store_true", help="print compact JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    state_root = args.state_root.expanduser()
    state_file = (args.state_file or state_root / "monitoring_watchdog_state.json").expanduser()
    payload = run_checks(
        state_root=state_root,
        state_file=state_file,
        exporter_url=str(args.exporter_url),
        prometheus_url=str(args.prometheus_url),
        grafana_url=str(args.grafana_url),
        timeout_sec=float(args.timeout_sec),
        snapshot_max_age_sec=float(args.snapshot_max_age_sec),
        required_metrics=split_metrics(str(args.required_metrics)),
        repair_enabled=bool(args.repair),
        repair_command=str(args.repair_command),
        repair_timeout_sec=float(args.repair_timeout_sec),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    else:
        print("stream_v3 monitoring watchdog: " + ("ok" if payload.get("ok") else "failed"))
    return 0 if payload.get("ok") or args.soft_exit else 2


if __name__ == "__main__":
    raise SystemExit(main())
