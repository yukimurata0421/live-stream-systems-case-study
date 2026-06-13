#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_STATE_DIR = BASE_DIR / ".state" / "packet-captures"
DEFAULT_FILTER = (
    "(tcp port 443 and (host 1.1.1.1 or host 8.8.8.8 or "
    "host 2606:4700:4700::1111 or host 2001:4860:4860::8888 or "
    "net 142.250.0.0/15 or net 172.217.0.0/16 or net 216.58.192.0/19)) "
    "or port 53 or icmp6 or udp port 546 or udp port 547"
)


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def bool_env(name: str, default: bool = False) -> bool:
    raw = env(name, "1" if default else "0").lower()
    return raw in {"1", "true", "yes", "on"}


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


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return slug.strip("_") or "capture"


def output_path(args: argparse.Namespace, ts_utc: str) -> Path:
    timestamp = ts_utc.replace(":", "").replace("-", "").replace("Z", "Z")
    return args.capture_dir / f"rtmps_tcpdump_{timestamp}_{safe_slug(args.sample_reason)}.pcap"


def build_tcpdump_command(args: argparse.Namespace, destination: Path) -> list[str]:
    command = [
        args.tcpdump_binary,
        "-i",
        args.interface,
        "-s",
        str(args.snaplen),
        "-nn",
    ]
    if args.tcpdump_user:
        command.extend(["-Z", args.tcpdump_user])
    command.extend(
        [
        "-G",
        str(args.duration_sec),
        "-W",
        "1",
        "-w",
        str(destination),
        args.capture_filter,
        ]
    )
    return command


def run_capture(args: argparse.Namespace) -> dict[str, Any]:
    ts_utc = iso_utc_now()
    args.capture_dir.mkdir(parents=True, exist_ok=True)
    destination = output_path(args, ts_utc)
    command = build_tcpdump_command(args, destination)
    if args.dry_run:
        return {
            "schema": "stream_v3_rtmps_tcpdump_ring/v1",
            "ts_utc": ts_utc,
            "ts_jst": iso_jst(ts_utc),
            "sample_reason": args.sample_reason,
            "ok": True,
            "dry_run": True,
            "command": command,
            "pcap_path": str(destination),
        }

    started = time.monotonic()
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=args.duration_sec + args.timeout_grace_sec, check=False)
        returncode = completed.returncode
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
    except (OSError, subprocess.TimeoutExpired) as exc:
        returncode = None
        stderr = f"{type(exc).__name__}: {exc}"
        stdout = ""

    exists = destination.exists()
    size = destination.stat().st_size if exists else 0
    return {
        "schema": "stream_v3_rtmps_tcpdump_ring/v1",
        "ts_utc": ts_utc,
        "ts_jst": iso_jst(ts_utc),
        "sample_reason": args.sample_reason,
        "ok": returncode == 0 and exists,
        "returncode": returncode,
        "elapsed_sec": round(time.monotonic() - started, 1),
        "command": command,
        "pcap_path": str(destination),
        "pcap_exists": exists,
        "pcap_size_bytes": size,
        "snaplen": args.snaplen,
        "payload_policy": "snaplen_128_metadata_only; no full packet payload capture intended",
        "stdout_tail": stdout[-1000:],
        "stderr_tail": stderr[-2000:],
    }


def parse_args() -> argparse.Namespace:
    state_dir = Path(env("TCPDUMP_RING_STATE_DIR", str(DEFAULT_STATE_DIR)))
    parser = argparse.ArgumentParser(description="Run a bounded tcpdump capture for RTMPS/WAN attribution around 08:05 JST.")
    parser.add_argument("--tcpdump-binary", default=env("TCPDUMP_RING_BINARY", "tcpdump"))
    parser.add_argument("--interface", default=env("TCPDUMP_RING_INTERFACE", "any"))
    parser.add_argument("--tcpdump-user", default=env("TCPDUMP_RING_TCPDUMP_USER", "root"))
    parser.add_argument("--duration-sec", type=int, default=int(env("TCPDUMP_RING_DURATION_SEC", "1200") or "1200"))
    parser.add_argument("--timeout-grace-sec", type=int, default=int(env("TCPDUMP_RING_TIMEOUT_GRACE_SEC", "15") or "15"))
    parser.add_argument("--snaplen", type=int, default=int(env("TCPDUMP_RING_SNAPLEN", "128") or "128"))
    parser.add_argument("--capture-filter", default=env("TCPDUMP_RING_FILTER", DEFAULT_FILTER))
    parser.add_argument("--sample-reason", default=env("TCPDUMP_RING_SAMPLE_REASON", "jst_0800_0820_packet_metadata"))
    parser.add_argument("--capture-dir", type=Path, default=Path(env("TCPDUMP_RING_CAPTURE_DIR", str(state_dir / "pcap"))))
    parser.add_argument("--latest-file", type=Path, default=Path(env("TCPDUMP_RING_LATEST_FILE", str(state_dir / "rtmps_tcpdump_ring_latest.json"))))
    parser.add_argument("--output-jsonl", type=Path, default=Path(env("TCPDUMP_RING_OUTPUT_JSONL", str(state_dir / "logs" / "rtmps_tcpdump_ring.jsonl"))))
    parser.add_argument("--dry-run", action="store_true", default=bool_env("TCPDUMP_RING_DRY_RUN", True))
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = run_capture(args)
    append_jsonl(args.output_jsonl, payload)
    write_json(args.latest_file, payload)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
