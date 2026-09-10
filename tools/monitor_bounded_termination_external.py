#!/usr/bin/env python3
"""Read-only arena oracle: aggregate healthyだけでなくfresh OAuth ingestと映像を確認する。"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import time
from pathlib import Path
from typing import Any

REMOTE_PROBE = """
import datetime as dt, json, pathlib, subprocess
base = pathlib.Path('/var/lib/stream-v3/arena-monitor')
keys = {
 'youtube_watchdog_stats.json': ['ts_utc','oauth_checked_ts_utc','status','healthy','api_live_state','action',
 'oauth_stream_status','oauth_stream_health_status','oauth_probe_ok','api_cost_burn_rate_active'],
 'viewer_synthetic_status.json': ['checked_at_utc','status','frame_ok','black_detected','freeze_detected',
 'consecutive_probe_failures','consecutive_visual_failures'],
}
result = {}
for name, selected in keys.items():
 value = json.loads((base / name).read_text())
 result[name] = {key: value.get(key) for key in selected}
result['remote_now'] = dt.datetime.now(dt.timezone.utc).isoformat()
result['boot_id'] = pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
result['release'] = str(pathlib.Path('/opt/stream-v3/releases/current').resolve())
result['service'] = subprocess.check_output(['systemctl','is-active','stream-v3-arena-monitor.service'], text=True).strip()
print(json.dumps(result))
"""


def assess(raw: dict[str, Any], local_now: dt.datetime) -> tuple[list[str], dict[str, float | None]]:
    def parsed(value: str) -> dt.datetime:
        stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("NAIVE_ORACLE_TIME")
        return stamp

    remote_now = parsed(raw["remote_now"])
    youtube = raw["youtube_watchdog_stats.json"]
    viewer = raw["viewer_synthetic_status.json"]
    reasons = []
    ages: dict[str, float | None] = {}
    for key, stamp in (
        ("youtube", youtube.get("ts_utc")),
        ("oauth", youtube.get("oauth_checked_ts_utc")),
        ("viewer", viewer.get("checked_at_utc")),
    ):
        try:
            if not isinstance(stamp, str):
                raise ValueError("SOURCE_TIME_MISSING")
            ages[key] = (remote_now - parsed(stamp)).total_seconds()
        except (ValueError, TypeError):
            ages[key] = None
            reasons.append(f"{key.upper()}_TIMESTAMP_INVALID")
    if abs((local_now - remote_now).total_seconds()) > 10:
        reasons.append("ORACLE_CLOCK_SKEW")
    if raw["boot_id"] != "47493c1e-ec47-4a1e-b4b5-3afb0c75631c" or raw["service"] != "active":
        reasons.append("ARENA_IDENTITY_OR_SERVICE_DRIFT")
    expected_release = "/opt/stream-v3/releases/example-monitoring-release"
    if raw["release"] != expected_release:
        reasons.append("ARENA_RELEASE_DRIFT")
    for key, limit in (("youtube", 120), ("oauth", 240), ("viewer", 360)):
        age = ages[key]
        if age is not None and not -1 <= age <= limit:
            reasons.append(f"{key.upper()}_STALE")
    if not (
        youtube["status"] == "ok"
        and youtube["healthy"] is True
        and youtube["api_live_state"] == "live"
        and youtube["action"] == "none"
        and youtube["oauth_probe_ok"] is True
        and youtube["oauth_stream_status"] == "active"
        and youtube["oauth_stream_health_status"] in {"good", "ok"}
    ):
        reasons.append("YOUTUBE_INGEST_NOT_HEALTHY")
    if not (
        viewer["status"] == "healthy"
        and viewer["frame_ok"] is True
        and viewer["black_detected"] is False
        and viewer["freeze_detected"] is False
        and viewer["consecutive_probe_failures"] == 0
        and viewer["consecutive_visual_failures"] == 0
    ):
        reasons.append("VIEWER_NOT_HEALTHY")
    return reasons, ages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration-sec", type=float, default=900)
    parser.add_argument("--interval-sec", type=float, default=60)
    args = parser.parse_args()
    start = time.monotonic()
    count = 0
    with args.output.open("x", encoding="utf-8") as handle:
        while True:
            tick = time.monotonic()
            raw = {}
            try:
                result = subprocess.run(
                    ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "arena-server", "python3 -"],
                    input=REMOTE_PROBE,
                    capture_output=True,
                    text=True,
                    timeout=25,
                    check=True,
                )
                raw = json.loads(result.stdout)
                now = dt.datetime.now(dt.UTC)
                reasons, ages = assess(raw, now)
                count += 1
                record = {
                    "record_type": "sample",
                    "sequence": count,
                    "observed_at": now.isoformat(),
                    "elapsed_sec": time.monotonic() - start,
                    "status": "STOP" if reasons else "OK",
                    "reasons": reasons,
                    "ages_sec": ages,
                    "evidence": raw,
                }
            except Exception as exc:
                reasons = [f"HARNESS_ERROR:{type(exc).__name__}"]
                record = {"record_type": "error", "status": "STOP", "reasons": reasons, "evidence": raw}
            text = json.dumps(record, sort_keys=True)
            handle.write(text + "\n")
            handle.flush()
            print(text, flush=True)
            if reasons:
                return 2
            if time.monotonic() - start >= args.duration_sec:
                summary = {"record_type": "summary", "status": "PASS", "samples": count, "elapsed_sec": time.monotonic() - start}
                handle.write(json.dumps(summary) + "\n")
                print(json.dumps(summary), flush=True)
                return 0
            time.sleep(max(0, args.interval_sec - (time.monotonic() - tick)))


if __name__ == "__main__":
    raise SystemExit(main())
