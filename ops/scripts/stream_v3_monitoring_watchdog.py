#!/usr/bin/env python3
"""Watch the stream_v3 monitoring surface and optionally repair it.

This is intentionally outside Prometheus: when Prometheus/Grafana/exporter is
the broken layer, the repair loop must not depend on Grafana panels.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_STATE_ROOT = Path(
    os.environ.get("STREAM_V3_OBSERVABILITY_STATE_ROOT")
    or os.environ.get("STREAM_RUNTIME_STATE_DIR")
    or "/var/lib/stream-v3/observability-monitor"
).expanduser()
DEFAULT_STATE_FILE = DEFAULT_STATE_ROOT / "monitoring_watchdog_state.json"
DEFAULT_COMPOSE_FILE = Path("/opt/monitoring/docker-compose.yml")
DEFAULT_HEALTH_SNAPSHOT_FILE = DEFAULT_STATE_ROOT / "health_summary_snapshot.json"
DEFAULT_OBJECTIVE_SNAPSHOT_FILE = DEFAULT_STATE_ROOT / "objective_sli_snapshot.json"
DEFAULT_MONITORING_V4_SENTINEL_FILE = Path(
    os.environ.get(
        "STREAM_V4_SENTINEL_FILE",
        "/var/lib/stream-monitoring-v4/.state/sentinel/k3s-status.json",
    )
)
ARENA_GRAFANA_URL = "http://127.0.0.1:30300/grafana"
ARENA_K3S_PROMETHEUS_URL = "http://127.0.0.1:19090"
ARENA_GRAFANA_REPAIR = "k3s:arena-monitoring:arena-monitoring-grafana"
ARENA_PROMETHEUS_REPAIR = "k3s:arena-monitoring:arena-monitoring-prometheus"
DEFAULT_ALERT_RULE_FILES = (
    Path(__file__).resolve().parents[1] / "monitoring" / "prometheus" / "rules" / "stream_v3.yml",
    Path(__file__).resolve().parents[1]
    / "monitoring"
    / "prometheus"
    / "rules"
    / "stream_v3_map.yml",
)
ALERT_NAME_PATTERN = re.compile(r"(?m)^\s*-\s+alert:\s*(StreamV3[A-Za-z0-9_]+)\s*$")


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    reason: str
    repair: str = ""


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def run(command: list[str], *, timeout_sec: float = 30.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout_sec,
    )


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(raw_tmp)
    try:
        os.fchmod(fd, 0o640)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        tmp.unlink(missing_ok=True)
        raise


def fetch_text(url: str, *, timeout_sec: float = 5.0) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as response:
            body = response.read(1024 * 1024).decode("utf-8", errors="replace")
            return 200 <= response.status < 300, body
    except (OSError, urllib.error.URLError) as exc:
        return False, f"{type(exc).__name__}: {exc}"


def fetch_json(url: str, *, timeout_sec: float = 5.0) -> tuple[bool, dict[str, Any], str]:
    ok, text = fetch_text(url, timeout_sec=timeout_sec)
    if not ok:
        return False, {}, text
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        return False, {}, f"json decode failed: {exc}"
    return True, value if isinstance(value, dict) else {}, ""


def prometheus_query(query: str, *, timeout_sec: float = 5.0) -> tuple[bool, list[dict[str, Any]], str]:
    qs = urllib.parse.urlencode({"query": query})
    ok, payload, err = fetch_json(f"http://127.0.0.1:9090/api/v1/query?{qs}", timeout_sec=timeout_sec)
    if not ok:
        return False, [], err
    if payload.get("status") != "success":
        return False, [], str(payload)
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    result = data.get("result") if isinstance(data.get("result"), list) else []
    return True, result, ""


def grafana_prometheus_query(query: str, *, timeout_sec: float = 5.0) -> tuple[bool, list[dict[str, Any]], str]:
    qs = urllib.parse.urlencode({"query": query})
    url = f"{ARENA_GRAFANA_URL}/api/datasources/proxy/uid/Prometheus/api/v1/query?{qs}"
    ok, payload, err = fetch_json(url, timeout_sec=timeout_sec)
    if not ok:
        return False, [], err
    if payload.get("status") != "success":
        return False, [], str(payload)
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    result = data.get("result") if isinstance(data.get("result"), list) else []
    return True, result, ""


def first_value(result: list[dict[str, Any]]) -> float | None:
    if not result:
        return None
    value = result[0].get("value")
    if not isinstance(value, list) or len(value) < 2:
        return None
    try:
        return float(value[1])
    except (TypeError, ValueError):
        return None


def source_alert_rule_names(paths: tuple[Path, ...]) -> tuple[str, ...]:
    names: list[str] = []
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"alert rule source missing or linked: {path.name}")
        names.extend(ALERT_NAME_PATTERN.findall(path.read_text(encoding="utf-8")))
    if not names or len(names) != len(set(names)):
        raise ValueError("alert rule source is empty or contains duplicate names")
    return tuple(sorted(names))


def prometheus_alert_rule_names(base_url: str) -> tuple[bool, tuple[str, ...], str]:
    ok, payload, error = fetch_json(
        f"{base_url.rstrip('/')}/api/v1/rules?type=alert",
        timeout_sec=5,
    )
    if not ok:
        return False, (), error
    if payload.get("status") != "success":
        return False, (), "Prometheus rules API did not return success"
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    groups = data.get("groups") if isinstance(data.get("groups"), list) else []
    names: set[str] = set()
    for group in groups:
        rules = group.get("rules") if isinstance(group, dict) else []
        if not isinstance(rules, list):
            continue
        for rule in rules:
            name = rule.get("name") if isinstance(rule, dict) else None
            if isinstance(name, str) and name.startswith("StreamV3"):
                names.add(name)
    return True, tuple(sorted(names)), ""


def alert_rule_contract_check(
    paths: tuple[Path, ...] = DEFAULT_ALERT_RULE_FILES,
) -> Check:
    try:
        expected = set(source_alert_rule_names(paths))
    except (OSError, UnicodeError, ValueError) as exc:
        return Check("arena_alert_rule_contract_loaded", False, f"source error: {exc}")
    ok, observed_names, error = prometheus_alert_rule_names(ARENA_K3S_PROMETHEUS_URL)
    if not ok:
        return Check("arena_alert_rule_contract_loaded", False, f"rules API error: {error}")
    observed = set(observed_names)
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    matched = not missing and not unexpected
    reason = f"expected={len(expected)} observed={len(observed)}"
    if missing:
        reason += " missing=" + ",".join(missing[:10])
    if unexpected:
        reason += " unexpected=" + ",".join(unexpected[:10])
    return Check("arena_alert_rule_contract_loaded", matched, reason)


def docker_running(container: str) -> bool:
    cp = run(["docker", "inspect", "-f", "{{.State.Running}}", container], timeout_sec=10)
    return cp.returncode == 0 and cp.stdout.strip().lower() == "true"


def parse_utc(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        from datetime import datetime

        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


EXPECTED_STREAM_V3_SERIES = {
    "health_pass_1h": 'stream_v3_health_pass{job="stream_v3_observability_monitor",window_hours="1"}',
    "health_pass_8h": 'stream_v3_health_pass{job="stream_v3_observability_monitor",window_hours="8"}',
    "health_pass_24h": 'stream_v3_health_pass{job="stream_v3_observability_monitor",window_hours="24"}',
    "current_fail_1h": 'stream_v3_current_fail{job="stream_v3_observability_monitor",window_hours="1"}',
    "current_fail_24h": 'stream_v3_current_fail{job="stream_v3_observability_monitor",window_hours="24"}',
    "same_url_live": 'stream_v3_same_url_live{job="stream_v3_observability_monitor"}',
    "youtube_ingest_connected": 'stream_v3_youtube_ingest_connected{job="stream_v3_observability_monitor"}',
    "stream_watchdog_ok": 'stream_v3_stream_watchdog_ok{job="stream_v3_observability_monitor"}',
}


def missing_series(query_fn, series: dict[str, str]) -> tuple[list[str], list[str]]:
    missing: list[str] = []
    errors: list[str] = []
    for name, query in series.items():
        ok, result, err = query_fn(query, timeout_sec=5)
        if not ok:
            errors.append(f"{name}: {err}")
            continue
        if not result:
            missing.append(name)
    return missing, errors


def snapshot_check(path: Path, max_age_sec: float, *, name: str, required_key: str) -> Check:
    payload = read_json(path)
    updated_ts = parse_utc(payload.get("updated_at_utc"))
    if updated_ts is None:
        return Check(name, False, "missing updated_at_utc", "systemd:stream-v3-health-snapshot.service")
    age = max(0.0, time.time() - updated_ts)
    snapshot_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    required_value = snapshot_payload.get(required_key)
    ok = age <= max_age_sec and bool(required_value)
    count = len(required_value) if isinstance(required_value, list) else (1 if required_value else 0)
    reason = f"age={round(age, 3)} max={max_age_sec} {required_key}={count}"
    return Check(name, ok, reason, "systemd:stream-v3-health-snapshot.service")


def monitoring_v4_sentinel_check(path: Path, max_age_sec: float) -> Check:
    """Validate the detection-only observer outside the monitored V4 k3s domain."""

    payload = read_json(path)
    if not payload:
        return Check("monitoring_v4_host_sentinel", False, "missing or invalid sentinel status")
    checked_ts = parse_utc(payload.get("checked_at"))
    if checked_ts is None:
        return Check("monitoring_v4_host_sentinel", False, "missing checked_at")
    raw_age = time.time() - checked_ts
    if raw_age < -60:
        return Check(
            "monitoring_v4_host_sentinel",
            False,
            f"future checked_at offset={round(-raw_age, 3)}",
        )
    age = max(0.0, raw_age)
    contract_ok = (
        payload.get("schema") == "monitoring_v4.k3s_host_sentinel.v6"
        and payload.get("detection_only") is True
        and payload.get("automatic_k3s_restart_enabled") is False
        and payload.get("automatic_runtime_mutation_enabled") is False
    )
    status_good = payload.get("status") == "good"
    fresh = age <= max_age_sec
    reason = (
        f"age={round(age, 3)} max={max_age_sec} "
        f"status={payload.get('status', 'missing')} contract_ok={str(contract_ok).lower()}"
    )
    return Check(
        "monitoring_v4_host_sentinel",
        fresh and status_good and contract_ok,
        reason,
    )


def checks(
    max_cache_age_sec: float,
    health_snapshot_file: Path,
    objective_snapshot_file: Path,
    monitoring_v4_sentinel_file: Path = DEFAULT_MONITORING_V4_SENTINEL_FILE,
    max_monitoring_v4_sentinel_age_sec: float = 360.0,
) -> list[Check]:
    out: list[Check] = []

    if docker_running("monitoring-prometheus"):
        ok, text = fetch_text("http://127.0.0.1:9090/-/ready", timeout_sec=5)
        out.append(Check("prometheus_ready", ok and "Ready" in text, text.strip()[:160], "docker:prometheus"))
    else:
        out.append(Check("prometheus_ready", False, "container monitoring-prometheus is not running", "docker:prometheus"))

    ok, text = fetch_text(f"{ARENA_K3S_PROMETHEUS_URL}/-/ready", timeout_sec=5)
    out.append(
        Check(
            "arena_k3s_prometheus_ready",
            ok and "Ready" in text,
            text.strip()[:160],
            ARENA_PROMETHEUS_REPAIR,
        )
    )
    out.append(alert_rule_contract_check())

    ok, payload, err = fetch_json(f"{ARENA_GRAFANA_URL}/api/health", timeout_sec=5)
    reason = err or f"database={payload.get('database', '')} version={payload.get('version', '')}"
    out.append(Check("public_grafana_health", ok and payload.get("database") == "ok", reason, ARENA_GRAFANA_REPAIR))

    ok, result, err = prometheus_query('up{job="stream_v3_observability_monitor"}', timeout_sec=5)
    value = first_value(result)
    out.append(Check("stream_v3_target_up", ok and value == 1.0, err or f"value={value}", "systemd:adsb-streamnew-prometheus-exporter.service"))

    ok, result, err = prometheus_query('stream_v3_exporter_up{job="stream_v3_observability_monitor"}', timeout_sec=5)
    value = first_value(result)
    out.append(Check("stream_v3_exporter_up", ok and value == 1.0, err or f"value={value}", "systemd:adsb-streamnew-prometheus-exporter.service"))

    ok, result, err = prometheus_query('stream_v3_exporter_last_refresh_success{job="stream_v3_observability_monitor"}', timeout_sec=5)
    value = first_value(result)
    out.append(Check("stream_v3_exporter_refresh_success", ok and value == 1.0, err or f"value={value}", "systemd:adsb-streamnew-prometheus-exporter.service"))

    ok, result, err = prometheus_query('stream_v3_exporter_cache_age_seconds{job="stream_v3_observability_monitor"}', timeout_sec=5)
    value = first_value(result)
    out.append(
        Check(
            "stream_v3_exporter_cache_fresh",
            ok and value is not None and value <= max_cache_age_sec,
            err or f"value={value} max={max_cache_age_sec}",
            "systemd:adsb-streamnew-prometheus-exporter.service",
        )
    )

    out.append(
        snapshot_check(
            health_snapshot_file,
            max_cache_age_sec,
            name="stream_v3_health_snapshot_fresh",
            required_key="windows",
        )
    )
    out.append(
        monitoring_v4_sentinel_check(
            monitoring_v4_sentinel_file,
            max_monitoring_v4_sentinel_age_sec,
        )
    )
    out.append(
        snapshot_check(
            objective_snapshot_file,
            max_cache_age_sec,
            name="stream_v3_objective_snapshot_fresh",
            required_key="metrics",
        )
    )

    missing, errors = missing_series(prometheus_query, EXPECTED_STREAM_V3_SERIES)
    contract_ok = not missing and not errors
    reason_parts = []
    if missing:
        reason_parts.append("missing=" + ",".join(missing))
    if errors:
        reason_parts.append("errors=" + "; ".join(errors[:3]))
    out.append(
        Check(
            "stream_v3_metrics_contract_present",
            contract_ok,
            "ok" if contract_ok else " ".join(reason_parts),
            "systemd:adsb-streamnew-prometheus-exporter.service",
        )
    )

    grafana_missing, grafana_errors = missing_series(
        grafana_prometheus_query,
        {"health_pass_24h": EXPECTED_STREAM_V3_SERIES["health_pass_24h"]},
    )
    grafana_contract_ok = not grafana_missing and not grafana_errors
    grafana_reason_parts = []
    if grafana_missing:
        grafana_reason_parts.append("missing=" + ",".join(grafana_missing))
    if grafana_errors:
        grafana_reason_parts.append("errors=" + "; ".join(grafana_errors[:3]))
    out.append(
        Check(
            "grafana_prometheus_proxy_contract_present",
            grafana_contract_ok,
            "ok" if grafana_contract_ok else " ".join(grafana_reason_parts),
            ARENA_GRAFANA_REPAIR,
        )
    )

    return out


def repair(action: str, compose_file: Path) -> tuple[bool, str]:
    if action.startswith("docker:"):
        service = action.split(":", 1)[1]
        cp = run(["docker", "compose", "-f", str(compose_file), "restart", service], timeout_sec=60)
        return cp.returncode == 0, (cp.stderr or cp.stdout).strip()
    if action.startswith("systemd:"):
        unit = action.split(":", 1)[1]
        cp = run(["systemctl", "restart", unit], timeout_sec=60)
        return cp.returncode == 0, (cp.stderr or cp.stdout).strip()
    if action.startswith("k3s:"):
        parts = action.split(":", 2)
        if len(parts) != 3 or not parts[1] or not parts[2]:
            return False, f"invalid k3s repair action: {action}"
        namespace, deployment = parts[1], parts[2]
        cp = run(["k3s", "kubectl", "-n", namespace, "rollout", "restart", "deployment", deployment], timeout_sec=60)
        if cp.returncode != 0:
            return False, (cp.stderr or cp.stdout).strip()
        status = run(
            ["k3s", "kubectl", "-n", namespace, "rollout", "status", "deployment", deployment, "--timeout=180s"],
            timeout_sec=210,
        )
        return status.returncode == 0, (status.stderr or status.stdout or cp.stderr or cp.stdout).strip()
    return False, f"unknown repair action: {action}"


def update_state(
    state: dict[str, Any],
    current_checks: list[Check],
    *,
    repair_enabled: bool,
    threshold: int,
    cooldown_sec: float,
    compose_file: Path,
) -> dict[str, Any]:
    now = time.time()
    previous_entries = state.get("checks") if isinstance(state.get("checks"), dict) else {}
    entries: dict[str, dict[str, Any]] = {}
    repairs: list[dict[str, Any]] = []
    actions_taken: set[str] = set()

    for item in current_checks:
        entry = previous_entries.get(item.name) if isinstance(previous_entries.get(item.name), dict) else {}
        fail_count = 0 if item.ok else int(entry.get("fail_count", 0) or 0) + 1
        last_repair_ts = float(entry.get("last_repair_ts", 0.0) or 0.0)
        entry = {
            "ok": item.ok,
            "reason": item.reason,
            "repair": item.repair,
            "fail_count": fail_count,
            "last_seen_utc": utc_now(),
            "last_repair_ts": last_repair_ts,
            "last_repair_utc": entry.get("last_repair_utc", ""),
        }
        if (
            repair_enabled
            and not item.ok
            and item.repair
            and fail_count >= threshold
            and now - last_repair_ts >= cooldown_sec
            and item.repair not in actions_taken
        ):
            ok, detail = repair(item.repair, compose_file)
            actions_taken.add(item.repair)
            entry["last_repair_ts"] = now
            entry["last_repair_utc"] = utc_now()
            entry["fail_count"] = 0 if ok else fail_count
            repairs.append({"check": item.name, "action": item.repair, "ok": ok, "detail": detail[:500]})
        entries[item.name] = entry

    return {
        "schema_version": 1,
        "updated_at_utc": utc_now(),
        "repair_enabled": repair_enabled,
        "threshold": threshold,
        "cooldown_sec": cooldown_sec,
        "checks": entries,
        "repairs": repairs,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    parser.add_argument("--max-cache-age-sec", type=float, default=600.0)
    parser.add_argument("--health-snapshot-file", type=Path, default=DEFAULT_HEALTH_SNAPSHOT_FILE)
    parser.add_argument("--objective-snapshot-file", type=Path, default=DEFAULT_OBJECTIVE_SNAPSHOT_FILE)
    parser.add_argument(
        "--monitoring-v4-sentinel-file",
        type=Path,
        default=DEFAULT_MONITORING_V4_SENTINEL_FILE,
    )
    parser.add_argument("--max-monitoring-v4-sentinel-age-sec", type=float, default=360.0)
    parser.add_argument("--threshold", type=int, default=3)
    parser.add_argument("--cooldown-sec", type=float, default=600.0)
    parser.add_argument("--repair", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    current_checks = checks(
        args.max_cache_age_sec,
        args.health_snapshot_file,
        args.objective_snapshot_file,
        args.monitoring_v4_sentinel_file,
        max(1.0, float(args.max_monitoring_v4_sentinel_age_sec)),
    )
    state = update_state(
        read_json(args.state_file),
        current_checks,
        repair_enabled=bool(args.repair),
        threshold=max(1, int(args.threshold)),
        cooldown_sec=max(60.0, float(args.cooldown_sec)),
        compose_file=args.compose_file,
    )
    write_json(args.state_file, state)
    print(json.dumps(state, ensure_ascii=False, separators=(",", ":")))
    return 0 if all(item.ok for item in current_checks) else 2


if __name__ == "__main__":
    raise SystemExit(main())
