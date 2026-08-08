#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GATE_NAME = "stream-v3.io/gpu-ready"
BOOT_ANNOTATION = "stream-v3.io/gpu-gate-boot-id"
ESTABLISHED_ANNOTATION = "stream-v3.io/stream-established-boot-id"
NAMESPACE = os.environ.get("STREAM_V3_GATE_NAMESPACE", "stream-v3")
SELECTOR = os.environ.get(
    "STREAM_V3_GATE_SELECTOR",
    "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
)
NODE_NAME = os.environ.get("STREAM_V3_GATE_NODE", socket.gethostname().split(".")[0])
POLL_SEC = max(1.0, float(os.environ.get("STREAM_V3_GATE_POLL_SEC", "2")))
STATUS_FILE = Path(os.environ.get("STREAM_V3_GATE_STATUS_FILE", "/var/lib/stream-v3/gpu-startup-gate.json"))
ROLE_BACKUP_FILE = Path(
    os.environ.get("STREAM_V3_GATE_ROLE_BACKUP_FILE", "/var/lib/stream-v3/recovery-role-rules.json")
)
RECOVERY_ROLE = os.environ.get("STREAM_V3_GATE_RECOVERY_ROLE", "stream-v3-recovery")
ESTABLISHMENT_FILE = os.environ.get(
    "STREAM_V3_GATE_ESTABLISHMENT_FILE",
    "/state/runtime/stream_boot_established.json",
)
BOOT_ID_FILE = Path("/proc/sys/kernel/random/boot_id")
KUBECTL = ["/usr/local/bin/k3s", "kubectl"]
STOP = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stop_handler(_signum: int, _frame: object) -> None:
    global STOP
    STOP = True


def read_boot_id() -> str:
    try:
        return BOOT_ID_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def run(command: list[str], *, timeout: float = 5.0) -> subprocess.CompletedProcess[str]:
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


def positive_quantity(value: Any) -> bool:
    try:
        return float(str(value or "0")) >= 1
    except ValueError:
        return False


def gpu_gate_status() -> dict[str, Any]:
    nvidia = run(["/usr/bin/nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"], timeout=4.0)
    driver_version = (nvidia.stdout or "").splitlines()[0].strip() if nvidia.stdout else ""
    host_gpu_ok = nvidia.returncode == 0 and bool(driver_version)
    try:
        node = kubectl_json(["get", "node", NODE_NAME, "-o", "json"])
        plugins = kubectl_json(
            ["-n", "kube-system", "get", "pods", "-l", "name=nvidia-device-plugin-ds", "-o", "json"]
        )
    except Exception as exc:
        return {
            "ready": False,
            "host_gpu_ok": host_gpu_ok,
            "driver_version": driver_version,
            "node_ready": False,
            "gpu_allocatable": False,
            "device_plugin_ready": False,
            "reason": f"kubernetes unavailable: {type(exc).__name__}: {exc}",
        }

    conditions = {
        str(item.get("type") or ""): str(item.get("status") or "")
        for item in node.get("status", {}).get("conditions", [])
        if isinstance(item, dict)
    }
    node_ready = conditions.get("Ready") == "True"
    gpu_allocatable = positive_quantity(node.get("status", {}).get("allocatable", {}).get("nvidia.com/gpu"))
    plugin_items = plugins.get("items") if isinstance(plugins.get("items"), list) else []
    device_plugin_ready = any(
        item.get("status", {}).get("phase") == "Running"
        and any(
            status.get("name") == "nvidia-device-plugin-ctr"
            and status.get("ready") is True
            for status in item.get("status", {}).get("containerStatuses", [])
            if isinstance(status, dict)
        )
        for item in plugin_items
        if isinstance(item, dict)
    )
    ready = host_gpu_ok and node_ready and gpu_allocatable and device_plugin_ready
    failed = [
        label
        for label, ok in (
            ("host_gpu", host_gpu_ok),
            ("node_ready", node_ready),
            ("gpu_allocatable", gpu_allocatable),
            ("device_plugin_ready", device_plugin_ready),
        )
        if not ok
    ]
    return {
        "ready": ready,
        "host_gpu_ok": host_gpu_ok,
        "driver_version": driver_version,
        "node_ready": node_ready,
        "gpu_allocatable": gpu_allocatable,
        "device_plugin_ready": device_plugin_ready,
        "reason": "GPU startup gate ready" if ready else f"waiting for {','.join(failed)}",
    }


def pod_gate_action(pod: dict[str, Any], *, boot_id: str, gpu_ready: bool) -> str:
    metadata = pod.get("metadata") if isinstance(pod.get("metadata"), dict) else {}
    spec = pod.get("spec") if isinstance(pod.get("spec"), dict) else {}
    annotations = metadata.get("annotations") if isinstance(metadata.get("annotations"), dict) else {}
    gates = spec.get("schedulingGates") if isinstance(spec.get("schedulingGates"), list) else []
    has_gate = any(isinstance(gate, dict) and gate.get("name") == GATE_NAME for gate in gates)
    if has_gate:
        return "release" if gpu_ready else "wait"
    if annotations.get(BOOT_ANNOTATION) == boot_id:
        return "keep"
    return "delete_stale"


def runtime_pods() -> list[dict[str, Any]]:
    payload = kubectl_json(["-n", NAMESPACE, "get", "pods", "-l", SELECTOR, "-o", "json"])
    items = payload.get("items")
    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []


def current_pod_established(pod: dict[str, Any], *, boot_id: str) -> bool:
    metadata = pod.get("metadata") if isinstance(pod.get("metadata"), dict) else {}
    name = str(metadata.get("name") or "")
    uid = str(metadata.get("uid") or "")
    annotations = metadata.get("annotations") if isinstance(metadata.get("annotations"), dict) else {}
    if not name or not uid or annotations.get(BOOT_ANNOTATION) != boot_id:
        return False
    cp = run(
        [
            *KUBECTL,
            "-n",
            NAMESPACE,
            "exec",
            name,
            "-c",
            "fast-recovery-loop",
            "--",
            "cat",
            ESTABLISHMENT_FILE,
        ],
        timeout=5.0,
    )
    if cp.returncode != 0:
        return False
    try:
        marker = json.loads(cp.stdout)
    except json.JSONDecodeError:
        return False
    return bool(
        isinstance(marker, dict)
        and marker.get("ready") is True
        and marker.get("boot_id") == boot_id
        and marker.get("pod_uid") == uid
        and marker.get("pod_name") == name
    )


def mark_pod_established(name: str, boot_id: str) -> None:
    cp = run(
        [
            *KUBECTL,
            "-n",
            NAMESPACE,
            "annotate",
            "pod",
            name,
            f"{ESTABLISHED_ANNOTATION}={boot_id}",
            "--overwrite",
        ]
    )
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout or f"annotate exited {cp.returncode}").strip())


def has_deployment_mutation_verbs(rules: list[dict[str, Any]]) -> bool:
    for rule in rules:
        api_groups = rule.get("apiGroups") if isinstance(rule.get("apiGroups"), list) else []
        resources = rule.get("resources") if isinstance(rule.get("resources"), list) else []
        verbs = rule.get("verbs") if isinstance(rule.get("verbs"), list) else []
        if "apps" in api_groups and {"deployments", "deployments/scale"}.intersection(resources):
            if {"patch", "update"}.intersection(verbs):
                return True
    return False


def restricted_role_rules(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    restricted: list[dict[str, Any]] = []
    for source in rules:
        rule = json.loads(json.dumps(source))
        api_groups = rule.get("apiGroups") if isinstance(rule.get("apiGroups"), list) else []
        resources = rule.get("resources") if isinstance(rule.get("resources"), list) else []
        if "apps" in api_groups and {"deployments", "deployments/scale"}.intersection(resources):
            rule["verbs"] = [
                verb
                for verb in (rule.get("verbs") if isinstance(rule.get("verbs"), list) else [])
                if verb not in {"patch", "update"}
            ]
        restricted.append(rule)
    return restricted


def write_role_backup(rules: list[dict[str, Any]]) -> None:
    ROLE_BACKUP_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = ROLE_BACKUP_FILE.with_name(f".{ROLE_BACKUP_FILE.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps({"rules": rules}, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(ROLE_BACKUP_FILE)


def read_role_backup() -> list[dict[str, Any]]:
    try:
        payload = json.loads(ROLE_BACKUP_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rules = payload.get("rules") if isinstance(payload, dict) else None
    return [rule for rule in rules if isinstance(rule, dict)] if isinstance(rules, list) else []


def set_recovery_mutations_allowed(allowed: bool) -> dict[str, Any]:
    role = kubectl_json(["-n", NAMESPACE, "get", "role", RECOVERY_ROLE, "-o", "json"])
    current = role.get("rules") if isinstance(role.get("rules"), list) else []
    current_rules = [rule for rule in current if isinstance(rule, dict)]
    currently_allowed = has_deployment_mutation_verbs(current_rules)

    if allowed:
        if currently_allowed:
            return {"allowed": True, "changed": False, "reason": "recovery mutation permissions already restored"}
        restored = read_role_backup()
        if not restored or not has_deployment_mutation_verbs(restored):
            return {"allowed": False, "changed": False, "reason": "recovery Role backup is unavailable"}
        desired = restored
        reason = "restored recovery mutation permissions after current Pod establishment"
    else:
        if not currently_allowed:
            return {"allowed": False, "changed": False, "reason": "recovery mutations already blocked"}
        write_role_backup(current_rules)
        desired = restricted_role_rules(current_rules)
        reason = "blocked recovery mutation permissions until current Pod establishment"

    patch = json.dumps({"rules": desired}, separators=(",", ":"))
    cp = run([*KUBECTL, "-n", NAMESPACE, "patch", "role", RECOVERY_ROLE, "--type=merge", "-p", patch])
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout or f"Role patch exited {cp.returncode}").strip())
    return {"allowed": allowed, "changed": True, "reason": reason}


def delete_stale_pod(name: str) -> None:
    cp = run([*KUBECTL, "-n", NAMESPACE, "delete", "pod", name, "--wait=false"])
    if cp.returncode != 0 and "NotFound" not in (cp.stderr or ""):
        raise RuntimeError((cp.stderr or cp.stdout or f"delete exited {cp.returncode}").strip())


def release_pod(name: str, boot_id: str) -> None:
    cp = run(
        [
            *KUBECTL,
            "-n",
            NAMESPACE,
            "annotate",
            "pod",
            name,
            f"{BOOT_ANNOTATION}={boot_id}",
            "--overwrite",
        ]
    )
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout or f"annotate exited {cp.returncode}").strip())
    patch = json.dumps([{"op": "remove", "path": "/spec/schedulingGates"}])
    cp = run([*KUBECTL, "-n", NAMESPACE, "patch", "pod", name, "--type=json", "-p", patch])
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout or f"patch exited {cp.returncode}").strip())


def write_status(payload: dict[str, Any]) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_name(f".{STATUS_FILE.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(STATUS_FILE)


def main() -> int:
    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    boot_id = read_boot_id()
    if not boot_id:
        raise SystemExit("host boot id is unavailable")
    print(f"[gpu-startup-gate] boot_id={boot_id} node={NODE_NAME}", flush=True)

    while not STOP:
        checked_at = utc_now()
        gpu = gpu_gate_status()
        actions: list[dict[str, str]] = []
        error = ""
        recovery_permissions: dict[str, Any] = {}
        stream_established = False
        try:
            pods = runtime_pods()
            established_pods = [
                pod
                for pod in pods
                if current_pod_established(pod, boot_id=boot_id)
            ]
            stream_established = bool(established_pods)
            for pod in established_pods:
                metadata = pod.get("metadata") if isinstance(pod.get("metadata"), dict) else {}
                annotations = (
                    metadata.get("annotations")
                    if isinstance(metadata.get("annotations"), dict)
                    else {}
                )
                name = str(metadata.get("name") or "")
                if name and annotations.get(ESTABLISHED_ANNOTATION) != boot_id:
                    print(f"[gpu-startup-gate] marking established pod={name}", flush=True)
                    mark_pod_established(name, boot_id)
            recovery_permissions = set_recovery_mutations_allowed(stream_established)
            if recovery_permissions.get("changed"):
                print(f"[gpu-startup-gate] {recovery_permissions.get('reason')}", flush=True)
            for pod in pods:
                name = str(pod.get("metadata", {}).get("name") or "")
                action = pod_gate_action(pod, boot_id=boot_id, gpu_ready=bool(gpu.get("ready")))
                if action == "delete_stale":
                    print(f"[gpu-startup-gate] deleting stale pre-boot pod={name}", flush=True)
                    delete_stale_pod(name)
                elif action == "release":
                    print(f"[gpu-startup-gate] releasing pod={name} driver={gpu.get('driver_version')}", flush=True)
                    release_pod(name, boot_id)
                actions.append({"pod": name, "action": action})
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            print(f"[gpu-startup-gate] {error}", flush=True)

        write_status(
            {
                "schema": "stream_v3_gpu_startup_gate.v1",
                "checked_at_utc": checked_at,
                "boot_id": boot_id,
                "node": NODE_NAME,
                "gpu": gpu,
                "stream_established": stream_established,
                "recovery_permissions": recovery_permissions,
                "actions": actions,
                "error": error,
            }
        )
        time.sleep(POLL_SEC)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
