#!/usr/bin/env python3
"""
graphs1090 health checker

- checks if graphs1090.service is active
- checks if /run/graphs1090 has fresh PNG outputs
- restarts graphs1090.service when unhealthy
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path


SERVICE_NAME = os.environ.get("SERVICE_NAME", "graphs1090.service")
GRAPH_DIR = Path(os.environ.get("GRAPH_DIR", "/run/graphs1090"))
MAX_STALE_SEC = int(os.environ.get("MAX_STALE_SEC", "600"))
RESTART_WAIT_SEC = int(os.environ.get("RESTART_WAIT_SEC", "20"))


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def get_service_state(name: str) -> str:
    res = run_cmd(["systemctl", "is-active", name])
    return (res.stdout or "").strip()


def newest_png_age_seconds(directory: Path) -> float | None:
    if not directory.exists() or not directory.is_dir():
        return None

    newest_mtime = None
    for p in directory.glob("*.png"):
        try:
            mtime = p.stat().st_mtime
        except OSError:
            continue
        if newest_mtime is None or mtime > newest_mtime:
            newest_mtime = mtime

    if newest_mtime is None:
        return None
    return time.time() - newest_mtime


def health_reasons() -> list[str]:
    reasons: list[str] = []

    state = get_service_state(SERVICE_NAME)
    if state != "active":
        reasons.append(f"service_state={state or 'unknown'}")

    age = newest_png_age_seconds(GRAPH_DIR)
    if age is None:
        reasons.append(f"graph_output_missing:{GRAPH_DIR}")
    elif age > MAX_STALE_SEC:
        reasons.append(f"graph_output_stale:{int(age)}s>{MAX_STALE_SEC}s")

    return reasons


def restart_service() -> None:
    run_cmd(["systemctl", "restart", SERVICE_NAME])


def main() -> int:
    reasons = health_reasons()
    if not reasons:
        print(f"OK: {SERVICE_NAME} healthy")
        return 0

    print(f"WARN: {SERVICE_NAME} unhealthy ({', '.join(reasons)}), restarting")
    restart_service()
    time.sleep(max(RESTART_WAIT_SEC, 1))

    reasons_after = health_reasons()
    if not reasons_after:
        print(f"RECOVERED: {SERVICE_NAME} healthy after restart")
        return 0

    print(f"ERROR: {SERVICE_NAME} still unhealthy ({', '.join(reasons_after)})")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
