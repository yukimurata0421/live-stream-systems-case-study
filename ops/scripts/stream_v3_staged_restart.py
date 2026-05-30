#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Sequence


NAMESPACE = os.environ.get("STREAM_K8S_NAMESPACE", "stream-v3")
KUBECTL = os.environ.get("STREAM_KUBECTL_BIN", "kubectl")
RUNTIME = os.environ.get("STREAM_V3_RUNTIME_WORKLOAD", "deployment/stream-v3-runtime")
RUNTIME_SELECTOR = os.environ.get(
    "STREAM_V3_RUNTIME_SELECTOR",
    "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
)


@dataclass(frozen=True)
class StepResult:
    name: str
    ok: bool
    detail: str


def run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(command), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)


def kubectl(*args: str) -> subprocess.CompletedProcess[str]:
    return run([KUBECTL, "-n", NAMESPACE, *args])


def workload_available(workload: str = RUNTIME) -> tuple[bool, str]:
    cp = kubectl("get", workload, "-o", "json")
    if cp.returncode != 0:
        return False, (cp.stderr or cp.stdout).strip()
    try:
        payload = json.loads(cp.stdout)
    except json.JSONDecodeError:
        return False, "invalid workload json"
    spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else {}
    status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
    desired = int(spec.get("replicas", 1) or 1)
    ready = int(status.get("readyReplicas", 0) or 0)
    available = int(status.get("availableReplicas", 0) or 0)
    return desired > 0 and ready >= desired and available >= desired, f"desired={desired} ready={ready} available={available}"


def wait_available(timeout_sec: int, poll_sec: int = 5) -> StepResult:
    deadline = time.monotonic() + timeout_sec
    last_detail = ""
    while time.monotonic() <= deadline:
        ok, detail = workload_available()
        if ok:
            return StepResult("wait_available", True, detail)
        last_detail = detail
        time.sleep(poll_sec)
    return StepResult("wait_available", False, last_detail or "timeout")


def rollout_restart(reason: str) -> StepResult:
    cp = kubectl("rollout", "restart", RUNTIME)
    detail = (cp.stdout or cp.stderr).strip()
    return StepResult("rollout_restart", cp.returncode == 0, f"{reason}: {detail}")


def delete_runtime_pods(reason: str) -> StepResult:
    cp = kubectl("delete", "pod", "-l", RUNTIME_SELECTOR)
    detail = (cp.stdout or cp.stderr).strip()
    return StepResult("delete_runtime_pods", cp.returncode == 0, f"{reason}: {detail}")


def scale_runtime(replicas: int, reason: str) -> StepResult:
    cp = kubectl("scale", RUNTIME, f"--replicas={replicas}")
    detail = (cp.stdout or cp.stderr).strip()
    return StepResult(f"scale_{replicas}", cp.returncode == 0, f"{reason}: {detail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Request a staged stream_v3 runtime restart from the monitoring host.")
    parser.add_argument("--reason", default="manual arena restart request")
    parser.add_argument("--timeout-sec", type=int, default=90)
    parser.add_argument("--hard", action="store_true", help="allow delete-pod and scale fallback if rollout restart is not enough")
    args = parser.parse_args()

    results: list[StepResult] = [rollout_restart(args.reason)]
    if results[-1].ok:
        results.append(wait_available(args.timeout_sec))
        if results[-1].ok:
            print(json.dumps([result.__dict__ for result in results], ensure_ascii=False))
            return 0

    if args.hard:
        results.append(delete_runtime_pods(args.reason))
        if results[-1].ok:
            results.append(wait_available(args.timeout_sec))
            if results[-1].ok:
                print(json.dumps([result.__dict__ for result in results], ensure_ascii=False))
                return 0
        results.extend([scale_runtime(0, args.reason), scale_runtime(1, args.reason), wait_available(args.timeout_sec)])

    print(json.dumps([result.__dict__ for result in results], ensure_ascii=False))
    return 0 if results and results[-1].ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
