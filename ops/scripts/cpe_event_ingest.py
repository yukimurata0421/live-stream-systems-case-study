#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_STATE_DIR = BASE_DIR / ".state" / "cpe-observer"


KEYWORD_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("scheduled_reconnect", ("scheduled reconnect", "schedule reconnect", "periodic reconnect", "auto reconnect", "daily reconnect", "timer reconnect")),
    ("cpe_reboot", ("reboot", "restart", "system started", "boot complete", "watchdog reset")),
    ("wan_disconnect", ("wan down", "link down", "disconnect", "disconnected", "pdn disconnect", "detach", "lte detach", "5g detach", "no service")),
    ("wan_connect", ("wan up", "link up", "connected", "pdn connect", "attach", "registered", "lte connect", "5g connect")),
    ("ipv6_prefix_delegation", ("prefix delegation", "delegated prefix", "dhcpv6", "ia_pd", "ipv6 prefix", "prefix expired", "prefix renewed")),
    ("router_advertisement", ("router advertisement", "ra received", "ra timeout", "ra expired")),
    ("nat_session", ("nat", "session flush", "conntrack", "mapping", "masquerade")),
    ("cellular_radio", ("rsrp", "rsrq", "sinr", "cell id", "earfcn", "nr-arfcn", "band", "handover")),
)


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_jst(ts_utc: str) -> str:
    dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    return (dt + timedelta(hours=9)).isoformat(timespec="seconds").replace("+00:00", "+09:00")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        f.write("\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def classify_cpe_event(message: str) -> dict[str, Any]:
    lower = message.lower()
    matched: list[str] = []
    event_class = "unclassified"
    for group, keywords in KEYWORD_GROUPS:
        group_matches = [keyword for keyword in keywords if keyword in lower]
        if group_matches:
            matched.extend(group_matches)
            if event_class == "unclassified":
                event_class = group
    severity = "info"
    if any(word in lower for word in ("error", "fail", "failed", "timeout", "critical", "panic")):
        severity = "error"
    elif any(word in lower for word in ("warn", "warning", "unstable")):
        severity = "warning"
    return {"event_class": event_class, "matched_keywords": matched, "severity": severity}


def build_payload(message: str, source: str, args: argparse.Namespace) -> dict[str, Any]:
    ts_utc = iso_utc_now()
    return {
        "schema": "stream_v3_cpe_event_ingest/v1",
        "ts_utc": ts_utc,
        "ts_jst": iso_jst(ts_utc),
        "sample_reason": args.sample_reason,
        "source": source,
        "raw": message.strip(),
        **classify_cpe_event(message),
    }


def write_payload(payload: dict[str, Any], args: argparse.Namespace) -> None:
    append_jsonl(args.output_jsonl, payload)
    write_json(args.latest_file, payload)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)


def read_stdin(args: argparse.Namespace) -> int:
    for line in sys.stdin:
        if line.strip():
            write_payload(build_payload(line, "stdin", args), args)
    return 0


def listen_udp(args: argparse.Namespace) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((args.listen_host, args.listen_port))
    started_ts_utc = iso_utc_now()
    write_payload(
        {
            "schema": "stream_v3_cpe_event_ingest/v1",
            "ts_utc": started_ts_utc,
            "ts_jst": iso_jst(started_ts_utc),
            "sample_reason": args.sample_reason,
            "source": f"udp://{args.listen_host}:{args.listen_port}",
            "raw": "udp_listener_started",
            "event_class": "observer_started",
            "matched_keywords": [],
            "severity": "info",
        },
        args,
    )
    received = 0
    while True:
        data, address = sock.recvfrom(args.max_datagram_bytes)
        message = data.decode(args.encoding, errors="replace")
        write_payload(build_payload(message, f"{address[0]}:{address[1]}", args), args)
        received += 1
        if args.cycles > 0 and received >= args.cycles:
            break
    return 0


def parse_api_headers(header_values: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for value in header_values:
        if ":" not in value:
            raise ValueError("API header must be formatted as 'Name: value'")
        name, header_value = value.split(":", 1)
        name = name.strip()
        header_value = header_value.strip()
        if not name:
            raise ValueError("API header name must not be empty")
        headers[name] = header_value
    return headers


def fetch_api_once(args: argparse.Namespace) -> dict[str, Any]:
    request = urllib.request.Request(args.api_url, method=args.api_method, headers=parse_api_headers(args.api_header))
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=args.api_timeout_sec) as response:
            body = response.read(args.max_api_bytes)
            status = response.getcode()
            content_type = response.headers.get("Content-Type", "")
    except (OSError, urllib.error.URLError) as exc:
        payload = build_payload(f"{type(exc).__name__}: {exc}", args.api_url, args)
        payload["api"] = {
            "ok": False,
            "url": args.api_url,
            "method": args.api_method,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            "error": f"{type(exc).__name__}: {exc}",
        }
        return payload

    text = body.decode(args.encoding, errors="replace")
    payload = build_payload(text, args.api_url, args)
    payload["api"] = {
        "ok": 200 <= status < 400,
        "url": args.api_url,
        "method": args.api_method,
        "status": status,
        "content_type": content_type,
        "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        "bytes_read": len(body),
    }
    if "json" in content_type.lower():
        try:
            payload["api_json"] = json.loads(text)
        except json.JSONDecodeError:
            payload["api_json_error"] = "JSONDecodeError"
    return payload


def poll_api(args: argparse.Namespace) -> int:
    completed = 0
    while True:
        loop_started = time.monotonic()
        write_payload(fetch_api_once(args), args)
        completed += 1
        if args.cycles > 0 and completed >= args.cycles:
            break
        sleep_sec = args.api_interval_sec - (time.monotonic() - loop_started)
        time.sleep(max(0.0, sleep_sec))
    return 0


def parse_args() -> argparse.Namespace:
    state_dir = Path(env("CPE_OBSERVER_STATE_DIR", str(DEFAULT_STATE_DIR)))
    parser = argparse.ArgumentParser(description="Ingest CPE syslog/API text events into JSONL for WAN event attribution.")
    parser.add_argument("--listen-host", default=env("CPE_OBSERVER_LISTEN_HOST", "127.0.0.1"))
    parser.add_argument("--listen-port", type=int, default=int(env("CPE_OBSERVER_LISTEN_PORT", "5514") or "5514"))
    parser.add_argument("--read-stdin", action="store_true")
    parser.add_argument("--api-url", default=env("CPE_OBSERVER_API_URL", ""))
    parser.add_argument("--api-method", default=env("CPE_OBSERVER_API_METHOD", "GET"))
    parser.add_argument("--api-header", action="append", default=[value for value in env("CPE_OBSERVER_API_HEADERS", "").splitlines() if value.strip()])
    parser.add_argument("--api-interval-sec", type=float, default=float(env("CPE_OBSERVER_API_INTERVAL_SEC", "60") or "60"))
    parser.add_argument("--api-timeout-sec", type=float, default=float(env("CPE_OBSERVER_API_TIMEOUT_SEC", "5") or "5"))
    parser.add_argument("--max-api-bytes", type=int, default=int(env("CPE_OBSERVER_MAX_API_BYTES", "1048576") or "1048576"))
    parser.add_argument("--cycles", type=int, default=int(env("CPE_OBSERVER_CYCLES", "0") or "0"))
    parser.add_argument("--encoding", default=env("CPE_OBSERVER_ENCODING", "utf-8"))
    parser.add_argument("--max-datagram-bytes", type=int, default=int(env("CPE_OBSERVER_MAX_DATAGRAM_BYTES", "8192") or "8192"))
    parser.add_argument("--sample-reason", default=env("CPE_OBSERVER_SAMPLE_REASON", "cpe_syslog_ingest"))
    parser.add_argument("--latest-file", type=Path, default=Path(env("CPE_OBSERVER_LATEST_FILE", str(state_dir / "cpe_event_ingest_latest.json"))))
    parser.add_argument("--output-jsonl", type=Path, default=Path(env("CPE_OBSERVER_OUTPUT_JSONL", str(state_dir / "logs" / "cpe_event_ingest.jsonl"))))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.read_stdin:
        return read_stdin(args)
    if args.api_url:
        return poll_api(args)
    return listen_udp(args)


if __name__ == "__main__":
    raise SystemExit(main())
