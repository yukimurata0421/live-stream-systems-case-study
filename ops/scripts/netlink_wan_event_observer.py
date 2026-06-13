#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_STATE_DIR = BASE_DIR / ".state" / "wan-observer"


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


def classify_ip_monitor_line(line: str) -> dict[str, Any]:
    raw = line.strip()
    lower = raw.lower()
    action = "unknown"
    if lower.startswith("deleted ") or " deleted " in lower:
        action = "deleted"
    elif lower.startswith("new") or " new" in lower:
        action = "new"
    elif "state down" in lower or " linkdown" in lower:
        action = "down"
    elif "state up" in lower:
        action = "up"

    event_class = "unknown"
    if " default " in f" {lower} " or lower.startswith("default "):
        event_class = "default_route"
    elif "inet6" in lower or re.search(r"\b[0-9a-f]{0,4}:[0-9a-f:]+/[0-9]{1,3}\b", lower):
        event_class = "ipv6_address_or_prefix"
    elif "inet " in lower or re.search(r"\b\d{1,3}(?:\.\d{1,3}){3}/\d{1,2}\b", lower):
        event_class = "ipv4_address_or_prefix"
    elif re.search(r"^\d+:\s+\S+:", raw):
        event_class = "link"
    elif " neigh " in f" {lower} ":
        event_class = "neighbor"

    interface = ""
    dev_match = re.search(r"\bdev\s+([^\s]+)", raw)
    if dev_match:
        interface = dev_match.group(1)
    else:
        link_match = re.search(r"^\d+:\s+([^\s:@]+)", raw)
        if link_match:
            interface = link_match.group(1)

    address = ""
    address_match = re.search(r"\b(?:inet6?|local)\s+([0-9a-fA-F:.]+(?:/\d{1,3})?)", raw)
    if address_match:
        address = address_match.group(1)
    else:
        route_addr_match = re.search(r"\b([0-9a-fA-F:.]+/\d{1,3})\b", raw)
        if route_addr_match:
            address = route_addr_match.group(1)

    return {
        "raw": raw,
        "action": action,
        "event_class": event_class,
        "interface": interface,
        "address": address,
    }


def build_payload(line: str, args: argparse.Namespace) -> dict[str, Any]:
    ts_utc = iso_utc_now()
    return {
        "schema": "stream_v3_netlink_wan_event_observer/v1",
        "ts_utc": ts_utc,
        "ts_jst": iso_jst(ts_utc),
        "sample_reason": args.sample_reason,
        **classify_ip_monitor_line(line),
    }


def write_payload(payload: dict[str, Any], args: argparse.Namespace) -> None:
    append_jsonl(args.output_jsonl, payload)
    write_json(args.latest_file, payload)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)


def monitor(args: argparse.Namespace) -> int:
    if args.read_stdin:
        for line in sys.stdin:
            if line.strip():
                write_payload(build_payload(line, args), args)
        return 0

    command = ["ip", "monitor", args.objects]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    started_ts_utc = iso_utc_now()
    started_payload = {
        "schema": "stream_v3_netlink_wan_event_observer/v1",
        "ts_utc": started_ts_utc,
        "ts_jst": iso_jst(started_ts_utc),
        "sample_reason": args.sample_reason,
        "event_class": "observer_started",
        "action": "started",
        "interface": "",
        "address": "",
        "raw": " ".join(command),
    }
    write_payload(started_payload, args)

    assert process.stdout is not None
    for line in process.stdout:
        if line.strip():
            write_payload(build_payload(line, args), args)

    stderr_text = ""
    if process.stderr is not None:
        stderr_text = process.stderr.read()
    if stderr_text:
        error_ts_utc = iso_utc_now()
        write_payload(
            {
                "schema": "stream_v3_netlink_wan_event_observer/v1",
                "ts_utc": error_ts_utc,
                "ts_jst": iso_jst(error_ts_utc),
                "sample_reason": args.sample_reason,
                "event_class": "observer_error",
                "action": "exited",
                "interface": "",
                "address": "",
                "raw": stderr_text.strip(),
            },
            args,
        )
    return process.wait()


def parse_args() -> argparse.Namespace:
    state_dir = Path(env("NETLINK_WAN_STATE_DIR", str(DEFAULT_STATE_DIR)))
    parser = argparse.ArgumentParser(description="Record ip-monitor netlink WAN route/address/link events.")
    parser.add_argument("--objects", default=env("NETLINK_WAN_OBJECTS", "all"))
    parser.add_argument("--sample-reason", default=env("NETLINK_WAN_SAMPLE_REASON", "continuous_netlink_monitor"))
    parser.add_argument("--read-stdin", action="store_true")
    parser.add_argument("--latest-file", type=Path, default=Path(env("NETLINK_WAN_LATEST_FILE", str(state_dir / "netlink_wan_event_observer_latest.json"))))
    parser.add_argument("--output-jsonl", type=Path, default=Path(env("NETLINK_WAN_OUTPUT_JSONL", str(state_dir / "logs" / "netlink_wan_event_observer.jsonl"))))
    return parser.parse_args()


def main() -> int:
    return monitor(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
