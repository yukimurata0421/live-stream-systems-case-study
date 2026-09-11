from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from stream_core.k8s_gpu_guard import summarize_runtime_gpu
from stream_core.runtime_readiness import current_pod_establishment


def kubectl_json(kubectl: str, args: list[str], *, timeout_sec: float) -> dict[str, Any]:
    completed = subprocess.run(
        [kubectl, *args],
        text=True,
        capture_output=True,
        timeout=timeout_sec,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            (completed.stderr or completed.stdout or f"kubectl exited {completed.returncode}").strip()
        )
    payload = json.loads(completed.stdout)
    return payload if isinstance(payload, dict) else {}


def runtime_gpu_restart_block(
    *,
    kubectl: str,
    namespace: str,
    deployment: str,
    selector: str,
    container_name: str,
    timeout_sec: float,
) -> dict[str, Any]:
    try:
        deployment_json = kubectl_json(
            kubectl,
            ["-n", namespace, "get", "deployment", deployment, "-o", "json"],
            timeout_sec=timeout_sec,
        )
        pods_json = kubectl_json(
            kubectl,
            ["-n", namespace, "get", "pods", "-l", selector, "-o", "json"],
            timeout_sec=timeout_sec,
        )
    except (OSError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        return {"status": "unavailable", "restart_blocked": False, "error": f"{type(exc).__name__}: {exc}"}
    return summarize_runtime_gpu(
        deployment_json,
        pods_json,
        deployment=deployment,
        container_name=container_name,
    )


def stream_establishment(path: Path, *, pod_uid: str, pod_name: str) -> dict[str, Any]:
    return current_pod_establishment(path, pod_uid=pod_uid, pod_name=pod_name)
