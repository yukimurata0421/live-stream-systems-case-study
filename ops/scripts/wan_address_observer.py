#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import socket
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_STATE_DIR = BASE_DIR / ".state" / "wan-observer"
DEFAULT_ANCHORS = (
    "cloudflare_v4=1.1.1.1:443,"
    "google_v4=8.8.8.8:443,"
    "cloudflare_v6=[2606:4700:4700::1111]:443,"
    "google_v6=[2001:4860:4860::8888]:443"
)


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_jst(ts_utc: str) -> str:
    dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    return (dt + timedelta(hours=9)).isoformat(timespec="seconds").replace("+00:00", "+09:00")


def run(cmd: list[str], *, timeout_sec: float = 2.0) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(cmd, text=True, capture_output=True, check=False, timeout=timeout_sec)
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(cmd, 124, exc.stdout or "", exc.stderr or "timeout")


def parse_json_stdout(cp: subprocess.CompletedProcess[str]) -> Any:
    if cp.returncode != 0 or not cp.stdout.strip():
        return None
    try:
        return json.loads(cp.stdout)
    except json.JSONDecodeError:
        return None


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


def default_route(family: str) -> dict[str, Any]:
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


def choose_interface(configured: str, ipv4_route: dict[str, Any], ipv6_route: dict[str, Any]) -> str:
    if configured:
        return configured
    for route in (ipv4_route, ipv6_route):
        dev = str(route.get("dev") or "")
        if dev:
            return dev
    return ""


def network_for(local: str, prefixlen: Any) -> str:
    try:
        return str(ipaddress.ip_network(f"{local}/{prefixlen}", strict=False))
    except ValueError:
        return ""


def address_summary(interface: str) -> dict[str, Any]:
    if not interface:
        return {"ok": False, "interface": "", "ipv4_global": [], "ipv6_global": []}
    cp = run(["ip", "-j", "addr", "show", "dev", interface])
    data = parse_json_stdout(cp)
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return {
            "ok": False,
            "interface": interface,
            "ipv4_global": [],
            "ipv6_global": [],
            "raw": (cp.stderr or cp.stdout or "").strip(),
        }

    ipv4: list[dict[str, Any]] = []
    ipv6: list[dict[str, Any]] = []
    for item in data[0].get("addr_info", []):
        if not isinstance(item, dict) or item.get("scope") != "global":
            continue
        row = {
            "local": item.get("local", ""),
            "prefixlen": item.get("prefixlen", ""),
            "network": network_for(str(item.get("local", "")), item.get("prefixlen", "")),
            "dynamic": bool(item.get("dynamic", False)),
            "valid_life_time": item.get("valid_life_time", ""),
            "preferred_life_time": item.get("preferred_life_time", ""),
        }
        if item.get("family") == "inet":
            ipv4.append(row)
        elif item.get("family") == "inet6":
            ipv6.append(row)
    return {"ok": True, "interface": interface, "ipv4_global": ipv4, "ipv6_global": ipv6}


def public_ipv4(url: str, timeout_sec: float) -> dict[str, Any]:
    if not url:
        return {"enabled": False}
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as response:
            body = response.read(128).decode("utf-8", errors="replace").strip()
    except Exception as exc:
        return {"enabled": True, "ok": False, "error": str(exc)}
    return {"enabled": True, "ok": True, "address": body}


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def ip_literal_family(host: str) -> str:
    try:
        version = ipaddress.ip_address(host.split("%", 1)[0]).version
    except ValueError:
        return ""
    return "ipv6" if version == 6 else "ipv4"


def parse_anchor(spec: str) -> dict[str, Any]:
    raw = spec.strip()
    name = ""
    target = raw
    if "=" in raw:
        name, target = (part.strip() for part in raw.split("=", 1))
    if not target:
        return {"name": name, "target": target, "ok": False, "error": "empty_target"}

    if target.startswith("["):
        end = target.find("]")
        if end < 0 or len(target) <= end + 2 or target[end + 1] != ":":
            return {"name": name, "target": target, "ok": False, "error": "invalid_bracketed_ipv6_target"}
        host = target[1:end]
        port_text = target[end + 2 :]
    else:
        if target.count(":") != 1:
            return {"name": name, "target": target, "ok": False, "error": "expected_host_colon_port"}
        host, port_text = (part.strip() for part in target.rsplit(":", 1))

    try:
        port = int(port_text)
    except ValueError:
        return {"name": name, "target": target, "host": host, "ok": False, "error": "invalid_port"}
    if not host or not 0 < port < 65536:
        return {"name": name, "target": target, "host": host, "port": port, "ok": False, "error": "invalid_host_or_port"}

    return {
        "name": name or target,
        "target": target,
        "host": host,
        "port": port,
        "literal_family": ip_literal_family(host),
    }


def tcp_anchor_probe(spec: str, timeout_sec: float) -> dict[str, Any]:
    parsed = parse_anchor(spec)
    if "host" not in parsed or "port" not in parsed:
        return parsed

    started = time.monotonic()
    try:
        with socket.create_connection((str(parsed["host"]), int(parsed["port"])), timeout=timeout_sec):
            pass
    except OSError as exc:
        parsed["ok"] = False
        parsed["elapsed_ms"] = round((time.monotonic() - started) * 1000, 1)
        parsed["error"] = f"{type(exc).__name__}: {exc}"
        return parsed

    parsed["ok"] = True
    parsed["elapsed_ms"] = round((time.monotonic() - started) * 1000, 1)
    parsed["error"] = ""
    return parsed


def tcp_anchor_probes(specs: list[str], timeout_sec: float) -> list[dict[str, Any]]:
    return [tcp_anchor_probe(spec, timeout_sec) for spec in specs]


def signature(payload: dict[str, Any]) -> dict[str, Any]:
    addresses = payload.get("addresses", {})
    ipv4 = addresses.get("ipv4_global", []) if isinstance(addresses, dict) else []
    ipv6 = addresses.get("ipv6_global", []) if isinstance(addresses, dict) else []
    return {
        "interface": payload.get("interface", ""),
        "ipv4_default_dev": payload.get("routes", {}).get("ipv4_default", {}).get("dev", ""),
        "ipv4_default_gateway": payload.get("routes", {}).get("ipv4_default", {}).get("gateway", ""),
        "ipv6_default_dev": payload.get("routes", {}).get("ipv6_default", {}).get("dev", ""),
        "ipv6_default_gateway": payload.get("routes", {}).get("ipv6_default", {}).get("gateway", ""),
        "ipv4_locals": sorted(str(item.get("local", "")) for item in ipv4 if isinstance(item, dict)),
        "ipv6_locals": sorted(str(item.get("local", "")) for item in ipv6 if isinstance(item, dict)),
        "ipv6_networks": sorted(str(item.get("network", "")) for item in ipv6 if isinstance(item, dict)),
        "public_ipv4": payload.get("public_ipv4", {}).get("address", ""),
    }


def changed_fields(previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    if not previous:
        return []
    return [key for key, value in current.items() if previous.get(key) != value]


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    ts_utc = iso_utc_now()
    ipv4_route = default_route("-4")
    ipv6_route = default_route("-6")
    interface = choose_interface(args.interface, ipv4_route, ipv6_route)
    addresses = address_summary(interface)
    payload: dict[str, Any] = {
        "schema": "stream_v3_wan_address_observer/v2",
        "ts_utc": ts_utc,
        "ts_jst": iso_jst(ts_utc),
        "interface": interface,
        "routes": {
            "ipv4_default": ipv4_route,
            "ipv6_default": ipv6_route,
        },
        "addresses": addresses,
        "public_ipv4": public_ipv4(args.public_ipv4_url, args.public_ipv4_timeout_sec),
        "tcp_anchors": tcp_anchor_probes(args.anchor, args.anchor_timeout_sec),
    }
    sig = signature(payload)
    previous = read_json(args.state_file).get("signature", {})
    changes = changed_fields(previous, sig if isinstance(sig, dict) else {})
    payload["signature"] = sig
    payload["changed"] = bool(changes)
    payload["changed_fields"] = changes
    return payload


def parse_args() -> argparse.Namespace:
    state_dir = Path(env("WAO_STATE_DIR", str(DEFAULT_STATE_DIR)))
    parser = argparse.ArgumentParser(description="Log host WAN-facing route/address state for daily TCP stall triage.")
    parser.add_argument("--interface", default=env("WAO_INTERFACE", ""))
    parser.add_argument("--state-file", type=Path, default=Path(env("WAO_STATE_FILE", str(state_dir / "wan_address_observer_state.json"))))
    parser.add_argument("--latest-file", type=Path, default=Path(env("WAO_LATEST_FILE", str(state_dir / "wan_address_observer_latest.json"))))
    parser.add_argument("--output-jsonl", type=Path, default=Path(env("WAO_OUTPUT_JSONL", str(state_dir / "logs" / "wan_address_observer.jsonl"))))
    parser.add_argument("--public-ipv4-url", default=env("WAO_PUBLIC_IPV4_URL", ""))
    parser.add_argument("--public-ipv4-timeout-sec", type=float, default=float(env("WAO_PUBLIC_IPV4_TIMEOUT_SEC", "2.0") or "2.0"))
    parser.add_argument("--anchor", action="append", default=split_csv(env("WAO_ANCHORS", DEFAULT_ANCHORS)))
    parser.add_argument("--anchor-timeout-sec", type=float, default=float(env("WAO_ANCHOR_TIMEOUT_SEC", "1.5") or "1.5"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_payload(args)
    append_jsonl(args.output_jsonl, payload)
    write_json(args.latest_file, payload)
    write_json(args.state_file, {"schema": payload["schema"], "ts_utc": payload["ts_utc"], "signature": payload["signature"]})
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
