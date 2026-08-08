#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


KUBECTL = ["/usr/local/bin/k3s", "kubectl"]
NAMESPACE = "stream-v3"
SELECTOR = "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime"
BOOT_ANNOTATION = "stream-v3.io/gpu-gate-boot-id"
ESTABLISHED_ANNOTATION = "stream-v3.io/stream-established-boot-id"
GATE_STATUS_FILE = Path("/var/lib/stream-v3/gpu-startup-gate.json")
REPORT_FILE = Path("/var/lib/stream-v3/postboot-verification.json")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def boot_epoch() -> int:
    try:
        for line in Path("/proc/stat").read_text(encoding="utf-8").splitlines():
            if line.startswith("btime "):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return 0


def run(command: list[str], *, timeout: float = 8.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def kubectl_json(args: list[str]) -> dict[str, Any]:
    cp = run([*KUBECTL, *args])
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout or f"kubectl exited {cp.returncode}").strip())
    payload = json.loads(cp.stdout)
    return payload if isinstance(payload, dict) else {}


def gate_status() -> dict[str, Any]:
    try:
        payload = json.loads(GATE_STATUS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def k3s_admission_errors() -> list[str]:
    cp = run(["journalctl", "-b", "-u", "k3s", "--no-pager", "-o", "cat"], timeout=15.0)
    if cp.returncode != 0:
        return [f"journalctl unavailable: {(cp.stderr or '').strip()}"]
    return [
        line.strip()
        for line in cp.stdout.splitlines()
        if "stream-v3-runtime" in line
        and ("UnexpectedAdmissionError" in line or "no healthy devices present" in line)
    ]


def parse_utc_epoch(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def recovery_role_mutations_allowed() -> bool:
    role = kubectl_json(["-n", NAMESPACE, "get", "role", "stream-v3-recovery", "-o", "json"])
    for rule in role.get("rules", []):
        if not isinstance(rule, dict):
            continue
        if "apps" not in (rule.get("apiGroups") or []):
            continue
        if not {"deployments", "deployments/scale"}.intersection(rule.get("resources") or []):
            continue
        if {"patch", "update"}.intersection(rule.get("verbs") or []):
            return True
    return False


def runtime_pod() -> dict[str, Any]:
    payload = kubectl_json(["-n", NAMESPACE, "get", "pods", "-l", SELECTOR, "-o", "json"])
    items = payload.get("items")
    pods = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    if len(pods) != 1:
        raise RuntimeError(f"expected one runtime Pod, found {len(pods)}")
    return pods[0]


def in_pod_readiness(pod_name: str) -> dict[str, Any]:
    cp = run(
        [
            *KUBECTL,
            "-n",
            NAMESPACE,
            "exec",
            pod_name,
            "-c",
            "stream-engine",
            "--",
            "python3",
            "/app/src/stream_core/runtime_readiness.py",
            "--check-only",
        ],
        timeout=10.0,
    )
    if cp.returncode != 0:
        return {"ready": False, "reason": (cp.stderr or cp.stdout or f"readiness exited {cp.returncode}").strip()}
    try:
        payload = json.loads(cp.stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return {"ready": False, "reason": "readiness output is invalid"}
    return payload if isinstance(payload, dict) else {"ready": False, "reason": "readiness output is not an object"}


def fast_recovery_evidence(pod_name: str, *, since_epoch: int) -> dict[str, Any]:
    code = """
import json
import sys
from datetime import datetime
from pathlib import Path

since = int(sys.argv[1])
restarts = []
samples = []
path = Path('/state/logs/fast_recovery_events.jsonl')
if path.exists():
    for line in path.read_text(encoding='utf-8').splitlines():
        try:
            row = json.loads(line)
            ts = int(datetime.fromisoformat(str(row.get('ts_utc', '')).replace('Z', '+00:00')).timestamp())
        except Exception:
            continue
        if ts < since:
            continue
        if row.get('kind') in {'restart', 'restart_failed'}:
            restarts.append({'ts_utc': row.get('ts_utc'), 'kind': row.get('kind'), 'trigger': row.get('trigger')})
        if row.get('kind') == 'tcp_send_sample' and int(row.get('bytes_sent_delta') or 0) > 0:
            samples.append({
                'ts_utc': row.get('ts_utc'),
                'bytes_sent_delta': row.get('bytes_sent_delta'),
                'mbps': row.get('mbps'),
            })
print(json.dumps({'restarts': restarts, 'positive_tcp_samples': samples[-3:]}, separators=(',', ':')))
"""
    cp = run(
        [
            *KUBECTL,
            "-n",
            NAMESPACE,
            "exec",
            pod_name,
            "-c",
            "fast-recovery-loop",
            "--",
            "python3",
            "-c",
            code,
            str(since_epoch),
        ],
        timeout=10.0,
    )
    if cp.returncode != 0:
        return {"error": (cp.stderr or cp.stdout or f"evidence exited {cp.returncode}").strip()}
    try:
        payload = json.loads(cp.stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return {"error": "fast recovery evidence output is invalid"}
    return payload if isinstance(payload, dict) else {"error": "fast recovery evidence is not an object"}


def container_summary(pod: dict[str, Any]) -> list[dict[str, Any]]:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return [
        {
            "name": str(status.get("name") or ""),
            "ready": bool(status.get("ready")),
            "restart_count": int(status.get("restartCount") or 0),
            "state": next(iter(status.get("state", {})), "unknown"),
        }
        for status in statuses
        if isinstance(status, dict)
    ]


def evaluate_once(*, boot_id: str, since_epoch: int) -> dict[str, Any]:
    gate = gate_status()
    pod = runtime_pod()
    deployment = kubectl_json(["-n", NAMESPACE, "get", "deployment", "stream-v3-runtime", "-o", "json"])
    metadata = pod.get("metadata") if isinstance(pod.get("metadata"), dict) else {}
    spec = pod.get("spec") if isinstance(pod.get("spec"), dict) else {}
    annotations = metadata.get("annotations") if isinstance(metadata.get("annotations"), dict) else {}
    containers = container_summary(pod)
    readiness = in_pod_readiness(str(metadata.get("name") or ""))
    recovery = fast_recovery_evidence(str(metadata.get("name") or ""), since_epoch=since_epoch)
    admission_errors = k3s_admission_errors()
    pod_name = str(metadata.get("name") or "")
    pod_uid = str(metadata.get("uid") or "")
    current_pod_admission_errors = [
        line
        for line in admission_errors
        if f'podUID="{pod_uid}"' in line or f'pod="{NAMESPACE}/{pod_name}"' in line
    ]
    stale_pod_admission_errors = [
        line for line in admission_errors if line not in current_pod_admission_errors
    ]
    deployment_metadata = (
        deployment.get("metadata") if isinstance(deployment.get("metadata"), dict) else {}
    )
    deployment_spec = deployment.get("spec") if isinstance(deployment.get("spec"), dict) else {}
    template = (
        deployment_spec.get("template")
        if isinstance(deployment_spec.get("template"), dict)
        else {}
    )
    template_metadata = (
        template.get("metadata") if isinstance(template.get("metadata"), dict) else {}
    )
    template_annotations = (
        template_metadata.get("annotations")
        if isinstance(template_metadata.get("annotations"), dict)
        else {}
    )
    restarted_at = str(template_annotations.get("kubectl.kubernetes.io/restartedAt") or "")
    rollout_after_boot = parse_utc_epoch(restarted_at) >= since_epoch
    recovery_mutations_allowed = recovery_role_mutations_allowed()
    gates = spec.get("schedulingGates") if isinstance(spec.get("schedulingGates"), list) else []

    failures: list[str] = []
    if gate.get("boot_id") != boot_id or not gate.get("gpu", {}).get("ready"):
        failures.append("GPU startup gate has not reached ready for the current boot")
    if annotations.get(BOOT_ANNOTATION) != boot_id:
        failures.append("runtime Pod was not released by the current boot GPU gate")
    if annotations.get(ESTABLISHED_ANNOTATION) != boot_id:
        failures.append("runtime Pod was not marked established for the current boot")
    if gates:
        failures.append("runtime Pod still has a scheduling gate")
    if len(containers) != 3 or not all(item["ready"] for item in containers):
        failures.append("not all three runtime containers are ready")
    if any(item["restart_count"] != 0 for item in containers):
        failures.append("a runtime container restarted after boot")
    if not readiness.get("ready"):
        failures.append(f"runtime readiness failed: {readiness.get('reason')}")
    if recovery.get("restarts"):
        failures.append("Fast Recovery executed a runtime restart after boot")
    if not recovery.get("positive_tcp_samples"):
        failures.append("no positive RTMPS TCP send sample observed after boot")
    if current_pod_admission_errors:
        failures.append("k3s logged a GPU admission error for the current runtime Pod")
    if rollout_after_boot:
        failures.append("Deployment rollout restart occurred after host boot")
    if not recovery_mutations_allowed:
        failures.append("recovery Deployment mutation permissions were not restored after readiness")

    return {
        "schema": "stream_v3_postboot_verification.v1",
        "checked_at_utc": utc_now(),
        "boot_id": boot_id,
        "ok": not failures,
        "failures": failures,
        "gate": gate,
        "pod": {
            "name": metadata.get("name"),
            "uid": metadata.get("uid"),
            "phase": pod.get("status", {}).get("phase"),
            "containers": containers,
            "gate_release_boot_id": annotations.get(BOOT_ANNOTATION),
            "established_boot_id": annotations.get(ESTABLISHED_ANNOTATION),
        },
        "deployment": {
            "generation": deployment_metadata.get("generation"),
            "revision": (deployment_metadata.get("annotations") or {}).get(
                "deployment.kubernetes.io/revision"
            ),
            "restarted_at_utc": restarted_at,
            "rollout_after_boot": rollout_after_boot,
        },
        "readiness": readiness,
        "fast_recovery": recovery,
        "k3s_runtime_gpu_admission_errors": admission_errors,
        "current_pod_gpu_admission_errors": current_pod_admission_errors,
        "stale_preboot_pod_gpu_admission_errors": stale_pod_admission_errors,
        "recovery_deployment_mutations_allowed": recovery_mutations_allowed,
    }


def write_report(payload: dict[str, Any]) -> None:
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = REPORT_FILE.with_name(f".{REPORT_FILE.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(REPORT_FILE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify stream v3 after a real host reboot.")
    parser.add_argument("--timeout-sec", type=int, default=300)
    parser.add_argument("--poll-sec", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    boot_id = read_text(Path("/proc/sys/kernel/random/boot_id"))
    since_epoch = boot_epoch()
    deadline = time.monotonic() + max(1, args.timeout_sec)
    last: dict[str, Any] = {
        "schema": "stream_v3_postboot_verification.v1",
        "checked_at_utc": utc_now(),
        "boot_id": boot_id,
        "ok": False,
        "failures": ["verification has not completed"],
    }
    while time.monotonic() < deadline:
        try:
            last = evaluate_once(boot_id=boot_id, since_epoch=since_epoch)
        except Exception as exc:
            last = {
                "schema": "stream_v3_postboot_verification.v1",
                "checked_at_utc": utc_now(),
                "boot_id": boot_id,
                "ok": False,
                "failures": [f"{type(exc).__name__}: {exc}"],
            }
        write_report(last)
        if last.get("ok"):
            print(json.dumps(last, ensure_ascii=False, sort_keys=True))
            return 0
        time.sleep(max(1, args.poll_sec))
    print(json.dumps(last, ensure_ascii=False, sort_keys=True))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
