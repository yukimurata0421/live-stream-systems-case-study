#!/usr/bin/env python3
"""Remote recovery loop for stream_v3 k3s workloads.

This is intended to run on the observability host with a namespace-scoped kubeconfig.
It requests recovery of the streaming host's runtime workload; long-horizon
stream quality decisions stay in observability monitoring tasks.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_REPO_ROOT = Path(os.environ.get("STREAM_V3_REPO_DIR", Path(__file__).resolve().parents[2])).expanduser()
DEFAULT_OBSERVABILITY_STATE_ROOT = DEFAULT_REPO_ROOT / ".state" / "observability-monitor"
PROMETHEUS_URL = os.environ.get("STREAM_V3_RECOVERY_PROMETHEUS_URL", "http://127.0.0.1:9090").rstrip("/")
JOB = os.environ.get("STREAM_V3_RECOVERY_JOB", "stream_v3_observability_monitor")
NAMESPACE = os.environ.get("STREAM_K8S_NAMESPACE", "stream-v3")
KUBECTL = os.environ.get("STREAM_KUBECTL_BIN", "kubectl")
STATE_FILE = Path(
    os.environ.get(
        "STREAM_V3_REMOTE_RECOVERY_STATE_FILE",
        str(DEFAULT_REPO_ROOT / ".state" / "remote-recovery" / "state.json"),
    )
)
ACTION_PLAN_FILE = Path(
    os.environ.get(
        "STREAM_V3_REMOTE_RECOVERY_ACTION_PLAN_FILE",
        str(DEFAULT_OBSERVABILITY_STATE_ROOT / "recovery_action_plan.json"),
    )
)
SCOPED_RECOVERY_SCRIPT = Path(
    os.environ.get(
        "STREAM_V3_SCOPED_RECOVERY_SCRIPT",
        str(Path(__file__).resolve().with_name("stream_v3_scoped_recovery.py")),
    )
)
COOLDOWN_SEC = int(os.environ.get("STREAM_V3_REMOTE_RECOVERY_COOLDOWN_SEC", "600"))
APPLY = os.environ.get("STREAM_V3_REMOTE_RECOVERY_APPLY", "0").strip().lower() in {"1", "true", "yes", "on"}
APPLY_ACTION_PLAN = os.environ.get("STREAM_V3_REMOTE_RECOVERY_APPLY_ACTION_PLAN", "0").strip().lower() in {"1", "true", "yes", "on"}
ACTION_PLAN_MAX_AGE_SEC = int(os.environ.get("STREAM_V3_REMOTE_RECOVERY_ACTION_PLAN_MAX_AGE_SEC", "180"))
ACTION_TIMEOUT_SEC = float(os.environ.get("STREAM_V3_REMOTE_RECOVERY_ACTION_TIMEOUT_SEC", "45"))
WORKLOADS = tuple(
    item.strip()
    for item in os.environ.get(
        "STREAM_V3_RECOVERY_WORKLOADS",
        "deployment/stream-v3-runtime",
    ).split(",")
    if item.strip()
)
SAME_URL_PRESERVING_WORKLOADS = frozenset({"deployment/stream-v3-runtime"})
ALLOWED_ACTION_PLAN_ACTIONS = frozenset({"restart_dj", "restart_ffmpeg"})
ACTION_TO_SCOPED_HELPER = {
    "restart_dj": "restart-dj",
    "restart_ffmpeg": "restart-ffmpeg",
}
LOW_UPLOAD_REASON_TERMS = (
    "low_upload",
    "low upload",
    "upload_budget",
    "upload budget",
    "upload_pressure",
    "upload pressure",
    "send_mbps",
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


def read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
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


def recovery_policy_blocker(workload: str, reason: str) -> str:
    if workload not in SAME_URL_PRESERVING_WORKLOADS:
        return "workload_not_same_url_preserving"
    text = reason.strip().lower()
    if any(term in text for term in LOW_UPLOAD_REASON_TERMS):
        return "low_upload_not_restart_cause"
    return ""


def reason_has_low_upload(reason: str) -> bool:
    text = reason.strip().lower()
    return any(term in text for term in LOW_UPLOAD_REASON_TERMS)


def parse_ts(ts_utc: object) -> int:
    text = str(ts_utc or "").strip()
    if not text:
        return 0
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def maybe_restart(workload: str, reason: str, state: dict[str, Any], now: int) -> bool:
    blocker = recovery_policy_blocker(workload, reason)
    if blocker:
        log(f"block restart {workload}: {blocker}: {reason}")
        return False
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


def action_plan_blocker(plan: dict[str, Any], state: dict[str, Any], now: int) -> str:
    if not APPLY_ACTION_PLAN:
        return "action_plan_apply_disabled"
    action = str(plan.get("action") or "")
    if action in {"", "none"}:
        return "no_action"
    if action not in ALLOWED_ACTION_PLAN_ACTIONS:
        return f"action_not_allowed:{action}"
    if not bool(plan.get("executable")):
        return "plan_not_executable"
    blocked_by = [str(item) for item in plan.get("blocked_by", []) if str(item)]
    unexpected_blockers = [item for item in blocked_by if item != "shadow_mode"]
    if unexpected_blockers:
        return "blocked_by:" + ",".join(unexpected_blockers)
    event_id = str(plan.get("event_id") or "")
    if event_id and str(state.get("last_action_plan_event_id") or "") == event_id:
        return "already_executed_event"
    ts = parse_ts(plan.get("ts_utc"))
    if ts <= 0 or now - ts > ACTION_PLAN_MAX_AGE_SEC:
        return f"stale_action_plan:age={now - ts if ts else 'unknown'}"
    reason_text = " ".join(
        [
            str(plan.get("reason") or ""),
            str(plan.get("action") or ""),
            " ".join(str(item) for item in plan.get("blocked_by", []) if item),
            " ".join(str(step.get("description") or "") for step in plan.get("steps", []) if isinstance(step, dict)),
        ]
    )
    if reason_has_low_upload(reason_text):
        return "low_upload_not_restart_cause"
    key = f"last_action_ts:{action}"
    last = int(state.get(key, 0) or 0)
    if now - last < COOLDOWN_SEC:
        return f"cooldown_active:{action}:age={now - last}s"
    return ""


def execute_action_plan(plan: dict[str, Any], state: dict[str, Any], now: int) -> bool:
    blocker = action_plan_blocker(plan, state, now)
    action = str(plan.get("action") or "")
    if blocker:
        if blocker != "no_action":
            log(f"skip action-plan action={action or '-'}: {blocker}")
        return True
    helper_action = ACTION_TO_SCOPED_HELPER[action]
    event_id = str(plan.get("event_id") or "")
    reason = f"action_plan:{event_id or 'no_event'}:{action}"
    command = [
        sys.executable,
        str(SCOPED_RECOVERY_SCRIPT),
        helper_action,
        "--reason",
        reason,
        "--timeout-sec",
        str(max(5.0, ACTION_TIMEOUT_SEC)),
    ]
    if not APPLY:
        command.append("--dry-run")
    cp = run(command)
    detail = (cp.stdout or cp.stderr or "").strip()
    ok = cp.returncode == 0
    log(f"{'ok' if ok else 'error'} action-plan {action}: rc={cp.returncode} detail={detail}")
    if APPLY:
        state[f"last_action_ts:{action}"] = now
        state["last_action_plan_event_id"] = event_id
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

    if rc == 0 and not execute_action_plan(read_json(ACTION_PLAN_FILE), state, now):
        rc = 1

    return rc


if __name__ == "__main__":
    sys.exit(main())
