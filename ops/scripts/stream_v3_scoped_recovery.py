#!/usr/bin/env python3
"""Scoped recovery actions for stream_v3 runtime.

These actions intentionally avoid rolling the whole runtime Deployment.  They
preserve the current YouTube URL by touching only the failing local scope:
Auto DJ's container or the stream-engine FFmpeg child process.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Sequence


KUBECTL = os.environ.get("STREAM_KUBECTL_BIN", "kubectl")
NAMESPACE = os.environ.get("STREAM_K8S_NAMESPACE", "stream-v3")
RUNTIME_SELECTOR = os.environ.get(
    "STREAM_V3_RUNTIME_SELECTOR",
    "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
)
STREAM_ENGINE_CONTAINER = os.environ.get("STREAM_V3_STREAM_ENGINE_CONTAINER", "stream-engine")
AUTO_DJ_CONTAINER = os.environ.get("STREAM_V3_AUTO_DJ_CONTAINER", "auto-dj")
LOW_UPLOAD_REASON_TERMS = (
    "low_upload",
    "low upload",
    "upload_budget",
    "upload budget",
    "upload_pressure",
    "upload pressure",
    "send_mbps",
)


@dataclass(frozen=True)
class ContainerStatus:
    restart_count: int
    container_id: str = ""
    ready: bool = False


def log(message: str) -> None:
    print(f"[stream-v3-scoped-recovery] {message}", flush=True)


def run(command: Sequence[str], *, timeout_sec: float = 15.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout_sec,
    )


def kubectl(*args: str, timeout_sec: float = 15.0) -> subprocess.CompletedProcess[str]:
    return run([KUBECTL, "-n", NAMESPACE, *args], timeout_sec=timeout_sec)


def guard_reason(reason: str) -> str:
    text = reason.strip().lower()
    if any(term in text for term in LOW_UPLOAD_REASON_TERMS):
        return "low_upload_not_restart_cause"
    return ""


def runtime_pod_json() -> dict[str, Any]:
    cp = kubectl("get", "pods", "-l", RUNTIME_SELECTOR, "-o", "json")
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout or "kubectl get pods failed").strip())
    try:
        payload = json.loads(cp.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid kubectl pod json") from exc
    return payload if isinstance(payload, dict) else {}


def select_runtime_pod(payload: dict[str, Any]) -> dict[str, Any]:
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    running: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        status = item.get("status") if isinstance(item.get("status"), dict) else {}
        if str(status.get("phase") or "") == "Running":
            running.append(item)
    if len(running) != 1:
        raise RuntimeError(f"expected exactly one Running stream-v3 runtime pod, got {len(running)}")
    return running[0]


def pod_name(pod: dict[str, Any]) -> str:
    metadata = pod.get("metadata") if isinstance(pod.get("metadata"), dict) else {}
    name = str(metadata.get("name") or "")
    if not name:
        raise RuntimeError("runtime pod name missing")
    return name


def container_status(pod: dict[str, Any], container: str) -> ContainerStatus:
    status = pod.get("status") if isinstance(pod.get("status"), dict) else {}
    statuses = status.get("containerStatuses") if isinstance(status.get("containerStatuses"), list) else []
    for item in statuses:
        if isinstance(item, dict) and str(item.get("name") or "") == container:
            return ContainerStatus(
                restart_count=int(item.get("restartCount") or 0),
                container_id=str(item.get("containerID") or ""),
                ready=bool(item.get("ready")),
            )
    raise RuntimeError(f"container status not found: {container}")


def wait_for_container_restart(
    *,
    pod: str,
    container: str,
    before: ContainerStatus,
    timeout_sec: float,
    require_peer_unchanged: tuple[str, ContainerStatus] | None = None,
) -> ContainerStatus:
    deadline = time.monotonic() + timeout_sec
    last: ContainerStatus | None = None
    while time.monotonic() < deadline:
        payload = runtime_pod_json()
        current_pod = select_runtime_pod(payload)
        if pod_name(current_pod) != pod:
            raise RuntimeError("runtime pod changed; refusing to treat this as scoped restart")
        current = container_status(current_pod, container)
        last = current
        if require_peer_unchanged is not None:
            peer_name, peer_before = require_peer_unchanged
            peer = container_status(current_pod, peer_name)
            if peer.restart_count != peer_before.restart_count or peer.container_id != peer_before.container_id:
                raise RuntimeError(f"peer container changed during scoped restart: {peer_name}")
        if current.restart_count > before.restart_count or (
            before.container_id and current.container_id and current.container_id != before.container_id
        ):
            return current
        time.sleep(1.0)
    raise RuntimeError(f"container did not restart within {timeout_sec:.0f}s: {container}; last={last}")


def rtmps_ffmpeg_pid_info(pod: str) -> tuple[str, int | None, str]:
    cp = exec_in_container(
        pod,
        STREAM_ENGINE_CONTAINER,
        r'''
        set -eu
        rows="$(pgrep -a ffmpeg | grep -E 'rtmp://|rtmps://' || true)"
        count="$(printf '%s\n' "$rows" | sed '/^$/d' | wc -l | tr -d ' ')"
        if [ "$count" != "1" ]; then
            printf 'rtmps_ffmpeg_count=%s\n' "$count"
            exit 10
        fi
        printf '%s\n' "$rows" | awk 'NR==1 {print $1}'
        ''',
        timeout_sec=8.0,
    )
    text = (cp.stdout or cp.stderr or "").strip()
    if cp.returncode == 10:
        return "", int(first_int(text) or 0), text
    if cp.returncode != 0:
        return "", None, text
    return (cp.stdout or "").strip().splitlines()[-1].strip(), 1, text


def wait_for_rtmps_ffmpeg_restart(*, pod: str, old_pid: str, timeout_sec: float) -> str:
    deadline = time.monotonic() + timeout_sec
    last = ""
    while time.monotonic() < deadline:
        current, count, _detail = rtmps_ffmpeg_pid_info(pod)
        if count not in {None, 1}:
            last = f"count={count}"
            time.sleep(1.0)
            continue
        if current:
            last = current
        if current and current != old_pid:
            return current
        time.sleep(1.0)
    raise RuntimeError(f"RTMPS FFmpeg child did not restart within {timeout_sec:.0f}s; old_pid={old_pid} last_pid={last}")


def first_int(text: str) -> str:
    match = re.search(r"\b(\d+)\b", text)
    return match.group(1) if match else ""


def exec_in_container(pod: str, container: str, script: str, *, timeout_sec: float = 15.0) -> subprocess.CompletedProcess[str]:
    return kubectl("exec", pod, "-c", container, "--", "sh", "-lc", script, timeout_sec=timeout_sec)


def restart_dj(*, reason: str, dry_run: bool, timeout_sec: float) -> int:
    blocker = guard_reason(reason)
    if blocker:
        log(f"block restart-dj: {blocker}: {reason}")
        return 2
    payload = runtime_pod_json()
    pod = select_runtime_pod(payload)
    name = pod_name(pod)
    before = container_status(pod, AUTO_DJ_CONTAINER)
    peer_before = container_status(pod, STREAM_ENGINE_CONTAINER)
    command = [KUBECTL, "-n", NAMESPACE, "exec", name, "-c", AUTO_DJ_CONTAINER, "--", "sh", "-lc", "kill -TERM 1"]
    if dry_run:
        log("plan restart-dj: " + " ".join(command))
        return 0
    cp = exec_in_container(name, AUTO_DJ_CONTAINER, "kill -TERM 1", timeout_sec=timeout_sec)
    detail = (cp.stdout or cp.stderr or "").strip()
    log(f"requested restart-dj pod={name} rc={cp.returncode} detail={detail}")
    after = wait_for_container_restart(
        pod=name,
        container=AUTO_DJ_CONTAINER,
        before=before,
        timeout_sec=timeout_sec,
        require_peer_unchanged=(STREAM_ENGINE_CONTAINER, peer_before),
    )
    log(f"ok restart-dj restart_count={before.restart_count}->{after.restart_count}")
    return 0


def restart_ffmpeg(*, reason: str, dry_run: bool, timeout_sec: float) -> int:
    blocker = guard_reason(reason)
    if blocker:
        log(f"block restart-ffmpeg: {blocker}: {reason}")
        return 2
    payload = runtime_pod_json()
    pod = select_runtime_pod(payload)
    name = pod_name(pod)
    before = container_status(pod, STREAM_ENGINE_CONTAINER)
    peer_before = container_status(pod, AUTO_DJ_CONTAINER)
    current_pid, current_count, current_detail = rtmps_ffmpeg_pid_info(name)
    if current_count == 0:
        command = [KUBECTL, "-n", NAMESPACE, "exec", name, "-c", STREAM_ENGINE_CONTAINER, "--", "sh", "-lc", "kill -TERM 1"]
        if dry_run:
            log("plan restart-ffmpeg-fallback: " + " ".join(command))
            return 0
        cp = exec_in_container(name, STREAM_ENGINE_CONTAINER, "kill -TERM 1", timeout_sec=timeout_sec)
        detail = (cp.stdout or cp.stderr or "").strip()
        log(f"requested restart-ffmpeg-fallback pod={name} rc={cp.returncode} detail={detail}")
        after = wait_for_container_restart(
            pod=name,
            container=STREAM_ENGINE_CONTAINER,
            before=before,
            timeout_sec=timeout_sec,
            require_peer_unchanged=(AUTO_DJ_CONTAINER, peer_before),
        )
        log(f"ok restart-ffmpeg-fallback restart_count={before.restart_count}->{after.restart_count}")
        return 0
    if current_count not in {None, 1}:
        log(f"error restart-ffmpeg: expected one RTMPS ffmpeg child, got {current_count}: {current_detail}")
        return 1
    if not current_pid:
        log(f"error restart-ffmpeg: could not inspect RTMPS ffmpeg child: {current_detail}")
        return 1
    script = r'''
        set -eu
        rows="$(pgrep -a ffmpeg | grep -E 'rtmp://|rtmps://' || true)"
        count="$(printf '%s\n' "$rows" | sed '/^$/d' | wc -l | tr -d ' ')"
        test "$count" = "1"
        pid="$(printf '%s\n' "$rows" | awk 'NR==1 {print $1}')"
        kill -TERM "$pid"
        printf 'terminated_rtmps_ffmpeg_pid=%s\n' "$pid"
    '''
    if dry_run:
        log(f"plan restart-ffmpeg: kubectl -n {NAMESPACE} exec {name} -c {STREAM_ENGINE_CONTAINER} -- sh -lc <rtmps-ffmpeg-terminate>")
        return 0
    cp = exec_in_container(name, STREAM_ENGINE_CONTAINER, script, timeout_sec=timeout_sec)
    detail = (cp.stdout or cp.stderr or "").strip()
    if cp.returncode != 0:
        log(f"error restart-ffmpeg pod={name} rc={cp.returncode} detail={detail}")
        return 1
    old_pid = first_int(detail) or current_pid
    if not old_pid:
        log(f"error restart-ffmpeg pod={name}: old pid missing detail={detail}")
        return 1
    new_pid = wait_for_rtmps_ffmpeg_restart(pod=name, old_pid=old_pid, timeout_sec=timeout_sec)
    log(f"ok restart-ffmpeg pod={name} old_pid={old_pid} new_pid={new_pid}")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run scoped stream_v3 recovery actions.")
    parser.add_argument("action", choices=("restart-dj", "restart-ffmpeg"))
    parser.add_argument("--reason", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        if args.action == "restart-dj":
            return restart_dj(reason=args.reason, dry_run=args.dry_run, timeout_sec=max(5.0, float(args.timeout_sec)))
        if args.action == "restart-ffmpeg":
            return restart_ffmpeg(reason=args.reason, dry_run=args.dry_run, timeout_sec=max(5.0, float(args.timeout_sec)))
    except Exception as exc:  # pragma: no cover - kept narrow by unit tests around helpers.
        log(f"error {args.action}: {exc}")
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
