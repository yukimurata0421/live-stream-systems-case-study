#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .systemctl_control import run_systemctl
    from .fast_recovery_core import tcp_metrics
except ImportError:
    from systemctl_control import run_systemctl
    from fast_recovery_core import tcp_metrics


BASE_DIR = Path(__file__).resolve().parents[2]
STATE_ROOT = Path(os.environ.get("STREAM_RUNTIME_STATE_DIR", str(BASE_DIR / ".state" / "adsb-streamnew-v2")))
LOG_DIR = Path(os.environ.get("STREAM_RUNTIME_LOG_DIR", str(STATE_ROOT / "logs")))


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def int_env(name: str, default: int) -> int:
    raw = env(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def float_env(name: str, default: float) -> float:
    raw = env(name, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


STREAM_SERVICE = env("NO_STREAM_SERVICE", "adsb-streamnew-youtube-stream.service")
RTMPS_HOST = env("NO_RTMPS_HOST", "a.rtmps.youtube.com")
RTMPS_PORT = int_env("NO_RTMPS_PORT", 443)
INTERFACE = env("NO_INTERFACE", "")
STATE_FILE = Path(env("NO_STATE_FILE", str(STATE_ROOT / "network_observer_state.json")))
LATEST_FILE = Path(env("NO_LATEST_FILE", str(STATE_ROOT / "network_observer_latest.json")))
EVENT_LOG_FILE = Path(env("NO_EVENT_LOG_FILE", str(LOG_DIR / "network_observer.jsonl")))
NORMAL_INTERVAL_SEC = max(1.0, float_env("NO_INTERVAL_SEC", 5.0))
BURST_INTERVAL_SEC = max(0.2, float_env("NO_BURST_INTERVAL_SEC", 1.0))
CONNECT_TIMEOUT_SEC = max(0.2, float_env("NO_CONNECT_TIMEOUT_SEC", 1.0))
EVENT_MIN_INTERVAL_SEC = max(1, int_env("NO_EVENT_MIN_INTERVAL_SEC", 60))
LOG_EVERY_SEC = max(0, int_env("NO_LOG_EVERY_SEC", 60))
BURST_WINDOWS_JST = env("NO_BURST_WINDOWS_JST", "08:03-08:06")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def run(cmd: list[str], *, timeout_sec: float = 2.0) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(cmd, text=True, capture_output=True, check=False, timeout=timeout_sec)
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(cmd, 124, exc.stdout or "", exc.stderr or "timeout")


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        f.write("\n")


def parse_json_stdout(cp: subprocess.CompletedProcess[str]) -> Any:
    if cp.returncode != 0 or not (cp.stdout or "").strip():
        return None
    try:
        return json.loads(cp.stdout)
    except json.JSONDecodeError:
        return None


def route_summary(family: str) -> dict[str, Any]:
    cp = run(["ip", "-j", family, "route", "show", "default"])
    data = parse_json_stdout(cp)
    if not isinstance(data, list) or not data:
        return {"ok": False, "raw": (cp.stderr or cp.stdout or "").strip()}
    route = data[0] if isinstance(data[0], dict) else {}
    return {
        "ok": True,
        "dev": route.get("dev", ""),
        "gateway": route.get("gateway", ""),
        "prefsrc": route.get("prefsrc", ""),
        "protocol": route.get("protocol", ""),
        "metric": route.get("metric", ""),
        "expires": route.get("expires", ""),
        "raw": route,
    }


def choose_interface(configured: str, v4_route: dict[str, Any], v6_route: dict[str, Any]) -> str:
    if configured:
        return configured
    for route in (v4_route, v6_route):
        dev = str(route.get("dev") or "")
        if dev:
            return dev
    return ""


def address_summary(interface: str) -> dict[str, Any]:
    if not interface:
        return {"interface": "", "ipv4_global": [], "ipv6_global": [], "ok": False}
    cp = run(["ip", "-j", "addr", "show", "dev", interface])
    data = parse_json_stdout(cp)
    if not isinstance(data, list) or not data:
        return {
            "interface": interface,
            "ipv4_global": [],
            "ipv6_global": [],
            "ok": False,
            "raw": (cp.stderr or cp.stdout or "").strip(),
        }
    addrs = data[0].get("addr_info", []) if isinstance(data[0], dict) else []
    ipv4: list[dict[str, Any]] = []
    ipv6: list[dict[str, Any]] = []
    for item in addrs if isinstance(addrs, list) else []:
        if not isinstance(item, dict) or item.get("scope") != "global":
            continue
        row = {
            "local": item.get("local", ""),
            "prefixlen": item.get("prefixlen", ""),
            "dynamic": bool(item.get("dynamic", False)),
            "valid_life_time": item.get("valid_life_time", ""),
            "preferred_life_time": item.get("preferred_life_time", ""),
        }
        if item.get("family") == "inet":
            ipv4.append(row)
        elif item.get("family") == "inet6":
            ipv6.append(row)
    return {"interface": interface, "ipv4_global": ipv4, "ipv6_global": ipv6, "ok": True}


def _unique_addrinfo(host: str, port: int, family: socket.AddressFamily) -> list[dict[str, str | int]]:
    try:
        infos = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
    except OSError:
        return []
    out: list[dict[str, str | int]] = []
    seen: set[tuple[str, int, str]] = set()
    for info in infos:
        fam = "ipv6" if info[0] == socket.AF_INET6 else "ipv4"
        sockaddr = info[4]
        addr = str(sockaddr[0])
        key = (fam, int(sockaddr[1]), addr)
        if key in seen:
            continue
        seen.add(key)
        out.append({"family": fam, "address": addr, "port": int(sockaddr[1])})
    return out


def dns_summary(host: str, port: int) -> dict[str, Any]:
    ordered = _unique_addrinfo(host, port, socket.AF_UNSPEC)
    v4 = _unique_addrinfo(host, port, socket.AF_INET)
    v6 = _unique_addrinfo(host, port, socket.AF_INET6)
    return {
        "host": host,
        "port": port,
        "ok": bool(ordered),
        "preferred_family": str(ordered[0].get("family", "")) if ordered else "",
        "ordered": ordered[:12],
        "ipv4_count": len(v4),
        "ipv6_count": len(v6),
        "ipv4_first": v4[0] if v4 else {},
        "ipv6_first": v6[0] if v6 else {},
    }


def tcp_connect_probe(host: str, port: int, family: socket.AddressFamily, timeout_sec: float) -> dict[str, Any]:
    addrs = _unique_addrinfo(host, port, family)
    family_name = "ipv6" if family == socket.AF_INET6 else "ipv4"
    if not addrs:
        return {"family": family_name, "ok": False, "address": "", "error": "no_address"}
    errors: list[str] = []
    for item in addrs:
        started = time.monotonic()
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(timeout_sec)
        try:
            addr = str(item["address"])
            sockaddr: Any = (addr, port, 0, 0) if family == socket.AF_INET6 else (addr, port)
            sock.connect(sockaddr)
            elapsed_ms = round((time.monotonic() - started) * 1000, 1)
            return {
                "family": family_name,
                "ok": True,
                "address": addr,
                "port": port,
                "elapsed_ms": elapsed_ms,
                "error": "",
            }
        except OSError as exc:
            errors.append(f"{item.get('address')}:{type(exc).__name__}:{exc}")
        finally:
            sock.close()
    return {"family": family_name, "ok": False, "address": "", "port": port, "error": "; ".join(errors[:3])}


def get_main_pid(unit: str) -> int:
    cp = run_systemctl(["show", unit, "--property=MainPID", "--value"], require_privilege=False, check=False)
    if cp.returncode != 0:
        return 0
    try:
        pid = int((cp.stdout or "").strip())
    except ValueError:
        return 0
    return pid if pid > 1 else 0


def get_child_ffmpeg_pid(main_pid: int) -> int:
    if main_pid <= 1:
        return 0
    cp = run(["pgrep", "-P", str(main_pid), "ffmpeg"])
    if cp.returncode != 0:
        return 0
    for line in (cp.stdout or "").splitlines():
        try:
            pid = int(line.strip())
        except ValueError:
            continue
        if pid > 1:
            return pid
    return 0


def family_from_conn(conn: str) -> str:
    parts = (conn or "").split()
    if len(parts) < 5:
        return ""
    peer = parts[4]
    if peer.startswith("["):
        return "ipv6"
    if ":" in peer:
        return "ipv4"
    return ""


def ffmpeg_socket_summary(stream_service: str, port: int) -> dict[str, Any]:
    main_pid = get_main_pid(stream_service)
    ffmpeg_pid = get_child_ffmpeg_pid(main_pid)
    metrics = tcp_metrics.parse_ffmpeg_tcp_metrics(ffmpeg_pid=ffmpeg_pid, ports=[port], run_cmd=run)
    conn = str(metrics.get("conn", "") or "")
    return {
        "stream_service": stream_service,
        "main_pid": main_pid,
        "ffmpeg_pid": ffmpeg_pid,
        "connected": bool(conn),
        "remote_family": family_from_conn(conn),
        "conn": conn,
        "bytes_sent": metrics.get("bytes_sent", 0),
        "notsent": metrics.get("notsent", 0),
        "unacked": metrics.get("unacked", 0),
        "lastsnd_ms": metrics.get("lastsnd_ms", 0),
    }


def _compact_network(snapshot: dict[str, Any]) -> dict[str, Any]:
    route = snapshot.get("route", {}) if isinstance(snapshot.get("route"), dict) else {}
    addrs = snapshot.get("addresses", {}) if isinstance(snapshot.get("addresses"), dict) else {}
    dns = snapshot.get("dns", {}) if isinstance(snapshot.get("dns"), dict) else {}

    def stable_route(raw: Any) -> dict[str, Any]:
        item = raw if isinstance(raw, dict) else {}
        return {
            "ok": bool(item.get("ok")),
            "dev": item.get("dev", ""),
            "gateway": item.get("gateway", ""),
            "prefsrc": item.get("prefsrc", ""),
            "protocol": item.get("protocol", ""),
            "metric": item.get("metric", ""),
        }

    def stable_addrs(raw: Any) -> list[dict[str, Any]]:
        items = raw if isinstance(raw, list) else []
        out: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "local": item.get("local", ""),
                    "prefixlen": item.get("prefixlen", ""),
                    "dynamic": bool(item.get("dynamic", False)),
                }
            )
        return out

    return {
        "v4_default": stable_route(route.get("ipv4_default", {})),
        "v6_default": stable_route(route.get("ipv6_default", {})),
        "ipv4_global": stable_addrs(addrs.get("ipv4_global", [])),
        "ipv6_global": stable_addrs(addrs.get("ipv6_global", [])),
        "dns_preferred_family": dns.get("preferred_family", ""),
        "dns_ipv4_count": dns.get("ipv4_count", 0),
        "dns_ipv6_count": dns.get("ipv6_count", 0),
    }


def classify_snapshot(snapshot: dict[str, Any], previous_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    prev = _compact_network(previous_snapshot or {}) if previous_snapshot else {}
    curr = _compact_network(snapshot)
    ffmpeg = snapshot.get("ffmpeg_socket", {}) if isinstance(snapshot.get("ffmpeg_socket"), dict) else {}
    tcp_v4 = snapshot.get("tcp_connect_ipv4", {}) if isinstance(snapshot.get("tcp_connect_ipv4"), dict) else {}
    tcp_v6 = snapshot.get("tcp_connect_ipv6", {}) if isinstance(snapshot.get("tcp_connect_ipv6"), dict) else {}
    dns = snapshot.get("dns", {}) if isinstance(snapshot.get("dns"), dict) else {}

    v4_route_changed = bool(prev) and prev.get("v4_default") != curr.get("v4_default")
    v6_route_changed = bool(prev) and prev.get("v6_default") != curr.get("v6_default")
    ipv4_addr_changed = bool(prev) and prev.get("ipv4_global") != curr.get("ipv4_global")
    ipv6_addr_changed = bool(prev) and prev.get("ipv6_global") != curr.get("ipv6_global")
    dns_order_changed = bool(prev) and prev.get("dns_preferred_family") != curr.get("dns_preferred_family")

    route_or_addr_changed = v4_route_changed or v6_route_changed or ipv4_addr_changed or ipv6_addr_changed
    ffmpeg_family = str(ffmpeg.get("remote_family", "") or "")
    affected_path = "none"
    status = "ok"
    cause_layer = "none"
    cause = "ok"
    impact = "no_current_network_degradation"
    action_hint = "observe"

    if not dns.get("ok"):
        status = "degraded"
        cause_layer = "dns"
        cause = "rtmps_dns_failure"
        affected_path = "rtmps"
        impact = "new_ingest_connections_may_fail"
        action_hint = "correlate_with_fast_recovery_before_restart"
    elif not tcp_v4.get("ok") and not tcp_v6.get("ok"):
        status = "incident_candidate"
        cause_layer = "remote_connectivity"
        cause = "rtmps_connect_failure_all_families"
        affected_path = "rtmps"
        impact = "new_rtmps_connections_unavailable"
        action_hint = "restart_only_if_fast_recovery_confirms_delivery_loss"
    elif ffmpeg_family == "ipv4" and not tcp_v4.get("ok"):
        status = "incident_candidate"
        cause_layer = "remote_connectivity"
        cause = "rtmps_ipv4_connect_failure"
        affected_path = "rtmps_ipv4"
        impact = "current_ipv4_ingest_path_at_risk"
        action_hint = "consider_ipv6_fallback_or router/isp investigation"
    elif ffmpeg_family == "ipv6" and not tcp_v6.get("ok"):
        status = "incident_candidate"
        cause_layer = "remote_connectivity"
        cause = "rtmps_ipv6_connect_failure"
        affected_path = "rtmps_ipv6"
        impact = "current_ipv6_ingest_path_at_risk"
        action_hint = "prefer_ipv4_for_ingest_or restart_after_route_stabilizes"
    elif route_or_addr_changed:
        status = "route_change_observed"
        cause_layer = "local_route"
        if v6_route_changed or ipv6_addr_changed:
            cause = "ipv6_prefix_or_default_route_churn"
            affected_path = "rtmps_ipv6" if ffmpeg_family == "ipv6" else "non_ingest_ipv6"
            impact = (
                "current_rtmps_ipv6_tcp_session_may_break"
                if ffmpeg_family == "ipv6"
                else "current_ingest_uses_ipv4_observe_only"
            )
        else:
            cause = "ipv4_default_route_or_address_churn"
            affected_path = "rtmps_ipv4" if ffmpeg_family == "ipv4" else "non_ingest_ipv4"
            impact = (
                "current_rtmps_ipv4_tcp_session_may_break"
                if ffmpeg_family == "ipv4"
                else "current_ingest_not_on_ipv4_observe_only"
            )
        action_hint = "observe_and_correlate_with_fast_recovery"
    elif dns_order_changed:
        status = "route_change_observed"
        cause_layer = "dns"
        cause = "rtmps_dns_family_order_changed"
        affected_path = str(dns.get("preferred_family", "") or "rtmps")
        impact = "future_ingest_family_selection_changed"
        action_hint = "observe"

    return {
        "status": status,
        "cause_layer": cause_layer,
        "cause": cause,
        "affected_path": affected_path,
        "impact": impact,
        "action_hint": action_hint,
        "signals": {
            "v4_route_changed": v4_route_changed,
            "v6_route_changed": v6_route_changed,
            "ipv4_addr_changed": ipv4_addr_changed,
            "ipv6_addr_changed": ipv6_addr_changed,
            "dns_order_changed": dns_order_changed,
            "dns_preferred_family": dns.get("preferred_family", ""),
            "tcp_connect_ipv4_ok": bool(tcp_v4.get("ok")),
            "tcp_connect_ipv6_ok": bool(tcp_v6.get("ok")),
            "ffmpeg_remote_family": ffmpeg_family,
        },
    }


def snapshot() -> dict[str, Any]:
    ts = iso_now()
    v4_route = route_summary("-4")
    v6_route = route_summary("-6")
    interface = choose_interface(INTERFACE, v4_route, v6_route)
    payload: dict[str, Any] = {
        "schema": "stream_v2_network_observer/v1",
        "ts_utc": ts,
        "rtmps_host": RTMPS_HOST,
        "rtmps_port": RTMPS_PORT,
        "interface": interface,
        "route": {"ipv4_default": v4_route, "ipv6_default": v6_route},
        "addresses": address_summary(interface),
        "dns": dns_summary(RTMPS_HOST, RTMPS_PORT),
        "tcp_connect_ipv4": tcp_connect_probe(RTMPS_HOST, RTMPS_PORT, socket.AF_INET, CONNECT_TIMEOUT_SEC),
        "tcp_connect_ipv6": tcp_connect_probe(RTMPS_HOST, RTMPS_PORT, socket.AF_INET6, CONNECT_TIMEOUT_SEC),
        "ffmpeg_socket": ffmpeg_socket_summary(STREAM_SERVICE, RTMPS_PORT),
    }
    return payload


def event_signature(classification: dict[str, Any], snapshot_payload: dict[str, Any]) -> str:
    signals = classification.get("signals", {}) if isinstance(classification.get("signals"), dict) else {}
    compact = _compact_network(snapshot_payload)
    parts = [
        str(classification.get("status", "")),
        str(classification.get("cause", "")),
        str(signals.get("ffmpeg_remote_family", "")),
        str(signals.get("tcp_connect_ipv4_ok", "")),
        str(signals.get("tcp_connect_ipv6_ok", "")),
        json.dumps(compact.get("v4_default", {}), sort_keys=True, separators=(",", ":")),
        json.dumps(compact.get("v6_default", {}), sort_keys=True, separators=(",", ":")),
        json.dumps(compact.get("ipv4_global", []), sort_keys=True, separators=(",", ":")),
        json.dumps(compact.get("ipv6_global", []), sort_keys=True, separators=(",", ":")),
    ]
    return "|".join(parts)


def should_append_event(state: dict[str, Any], classification: dict[str, Any], signature: str, now_ts: int) -> bool:
    if classification.get("status") == "ok":
        return False
    last_signature = str(state.get("last_event_signature", "") or "")
    last_ts = int(state.get("last_event_ts", 0) or 0)
    return signature != last_signature or now_ts - last_ts >= EVENT_MIN_INTERVAL_SEC


def observe_once(*, json_output: bool = False) -> dict[str, Any]:
    state = read_json(STATE_FILE)
    previous_snapshot = state.get("last_snapshot") if isinstance(state.get("last_snapshot"), dict) else None
    payload = snapshot()
    classification = classify_snapshot(payload, previous_snapshot=previous_snapshot)
    payload["classification"] = classification

    now_ts = int(time.time())
    signature = event_signature(classification, payload)
    write_json(LATEST_FILE, payload)

    if should_append_event(state, classification, signature, now_ts):
        append_jsonl(
            EVENT_LOG_FILE,
            {
                "ts_utc": payload["ts_utc"],
                "kind": "network_observation",
                "classification": classification,
                "rtmps_host": RTMPS_HOST,
                "rtmps_port": RTMPS_PORT,
                "interface": payload.get("interface", ""),
                "route": payload.get("route", {}),
                "addresses": payload.get("addresses", {}),
                "dns": payload.get("dns", {}),
                "tcp_connect_ipv4": payload.get("tcp_connect_ipv4", {}),
                "tcp_connect_ipv6": payload.get("tcp_connect_ipv6", {}),
                "ffmpeg_socket": payload.get("ffmpeg_socket", {}),
            },
        )
        state["last_event_signature"] = signature
        state["last_event_ts"] = now_ts

    state["last_snapshot"] = {
        "route": payload.get("route", {}),
        "addresses": payload.get("addresses", {}),
        "dns": payload.get("dns", {}),
        "ffmpeg_socket": payload.get("ffmpeg_socket", {}),
        "tcp_connect_ipv4": payload.get("tcp_connect_ipv4", {}),
        "tcp_connect_ipv6": payload.get("tcp_connect_ipv6", {}),
    }
    state["updated_at_utc"] = payload["ts_utc"]
    write_json(STATE_FILE, state)

    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return payload


def _minutes_since_midnight_jst(now_utc_ts: float) -> int:
    # JST is UTC+9 and has no daylight saving time.
    return int((now_utc_ts + 9 * 3600) % 86400) // 60


def _parse_hhmm(value: str) -> int:
    hour, minute = value.split(":", 1)
    return int(hour) * 60 + int(minute)


def in_burst_window(now_utc_ts: float, windows: str) -> bool:
    minute = _minutes_since_midnight_jst(now_utc_ts)
    for raw in windows.split(","):
        item = raw.strip()
        if not item or "-" not in item:
            continue
        start_raw, end_raw = item.split("-", 1)
        try:
            start = _parse_hhmm(start_raw.strip())
            end = _parse_hhmm(end_raw.strip())
        except Exception:
            continue
        if start <= end:
            if start <= minute <= end:
                return True
        elif minute >= start or minute <= end:
            return True
    return False


def run_loop() -> int:
    last_log_ts = 0.0
    while True:
        try:
            payload = observe_once(json_output=False)
            classification = payload.get("classification", {})
            now_for_log = time.time()
            should_log = (
                classification.get("status") != "ok"
                or LOG_EVERY_SEC <= 0
                or now_for_log - last_log_ts >= LOG_EVERY_SEC
            )
            if should_log:
                print(
                    f"[{payload.get('ts_utc')}] status={classification.get('status')} "
                    f"cause={classification.get('cause')} affected={classification.get('affected_path')}",
                    flush=True,
                )
                last_log_ts = now_for_log
        except Exception as exc:
            print(f"[{iso_now()}] ERROR network observer failed: {type(exc).__name__}: {exc}", flush=True)
        now = time.time()
        interval = BURST_INTERVAL_SEC if in_burst_window(now, BURST_WINDOWS_JST) else NORMAL_INTERVAL_SEC
        time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(description="Observe stream_v2 ingest network path with IPv4/IPv6 separation.")
    parser.add_argument("--once", action="store_true", help="Run one observation and exit.")
    parser.add_argument("--json", action="store_true", help="Print the observation JSON.")
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    args = parser.parse_args()
    if args.loop:
        return run_loop()
    observe_once(json_output=args.json or args.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
