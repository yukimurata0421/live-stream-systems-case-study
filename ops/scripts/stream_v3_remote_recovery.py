#!/usr/bin/env python3
"""Remote recovery loop for stream_v3 k3s workloads.

This is intended to run on arena-server with a namespace-scoped kubeconfig.
It requests recovery of the streaming host's runtime workload; long-horizon
stream quality decisions stay in arena monitoring tasks.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


PROMETHEUS_URL = os.environ.get("STREAM_V3_RECOVERY_PROMETHEUS_URL", "http://127.0.0.1:9090").rstrip("/")
JOB = os.environ.get("STREAM_V3_RECOVERY_JOB", "stream_v3_new_server")
NAMESPACE = os.environ.get("STREAM_K8S_NAMESPACE", "stream-v3")
KUBECTL = os.environ.get("STREAM_KUBECTL_BIN", "kubectl")
STATE_FILE = Path(
    os.environ.get(
        "STREAM_V3_REMOTE_RECOVERY_STATE_FILE",
        "/home/yuki/projects/stream_v3/.state/remote-recovery/state.json",
    )
)
COOLDOWN_SEC = int(os.environ.get("STREAM_V3_REMOTE_RECOVERY_COOLDOWN_SEC", "600"))
APPLY = os.environ.get("STREAM_V3_REMOTE_RECOVERY_APPLY", "1").strip().lower() in {"1", "true", "yes", "on"}
WORKLOADS = tuple(
    item.strip()
    for item in os.environ.get(
        "STREAM_V3_RECOVERY_WORKLOADS",
        "deployment/stream-v3-runtime",
    ).split(",")
    if item.strip()
)


def log(message: str) -> None:
    print(f"[stream-v3-remote-recovery] {message}", flush=True)


def load_state() -> dict[str, Any]:
    try:
        with STATE_FILE.open("r", encoding="utf-8") as fh:
            value = json.load(fh)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, sort_keys=True)
        fh.write("\n")
    tmp.replace(STATE_FILE)


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)


def prometheus_value(query: str) -> float | None:
    url = f"{PROMETHEUS_URL}/api/v1/query?{urllib.parse.urlencode({'query': query})}"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            payload = json.load(response)
    except Exception as exc:
        log(f"prometheus query failed query={query!r}: {exc}")
        return None
    results = ((payload.get("data") or {}).get("result") or []) if isinstance(payload, dict) else []
    if not results:
        return None
    try:
        return float(results[0]["value"][1])
    except (KeyError, TypeError, ValueError, IndexError):
        return None


def workload_active(workload: str) -> tuple[bool, str]:
    cp = run([KUBECTL, "-n", NAMESPACE, "get", workload, "-o", "json"])
    if cp.returncode != 0:
        return False, (cp.stderr or cp.stdout).strip()
    try:
        payload = json.loads(cp.stdout)
    except json.JSONDecodeError:
        return False, "invalid kubectl json"
    spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else {}
    status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
    desired = int(spec.get("replicas", 1) or 1)
    ready = int(status.get("readyReplicas", 0) or 0)
    available = int(status.get("availableReplicas", 0) or 0)
    return desired > 0 and ready >= desired and available >= desired, f"desired={desired} ready={ready} available={available}"


def restart_workload(workload: str, reason: str) -> bool:
    command = [KUBECTL, "-n", NAMESPACE, "rollout", "restart", workload]
    if not APPLY:
        log(f"plan restart {workload}: {reason}: {' '.join(command)}")
        return True
    cp = run(command)
    ok = cp.returncode == 0
    detail = (cp.stdout or cp.stderr).strip()
    log(f"{'ok' if ok else 'error'} restart {workload}: {reason}: {detail}")
    return ok


def maybe_restart(workload: str, reason: str, state: dict[str, Any], now: int) -> bool:
    key = f"last_restart_ts:{workload}"
    last = int(state.get(key, 0) or 0)
    if now - last < COOLDOWN_SEC:
        log(f"skip restart {workload}: cooldown active age={now - last}s reason={reason}")
        return True
    ok = restart_workload(workload, reason)
    if ok and APPLY:
        state[key] = now
        save_state(state)
    return ok


def main() -> int:
    now = int(time.time())
    state = load_state()
    rc = 0

    target_up = prometheus_value(f'up{{job="{JOB}"}}')
    exporter_up = prometheus_value(f'stream_v2_exporter_up{{job="{JOB}"}}')
    log(f"metrics target_up={target_up} exporter_up={exporter_up} apply={int(APPLY)}")

    for workload in WORKLOADS:
        active, detail = workload_active(workload)
        if active:
            log(f"ok {workload}: {detail}")
            continue
        rc = 2
        if not maybe_restart(workload, f"workload inactive: {detail}", state, now):
            rc = 1

    return rc


if __name__ == "__main__":
    sys.exit(main())
