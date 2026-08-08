from __future__ import annotations

from typing import Any

GPU_DRIVER_MISMATCH_PATTERNS = (
    "driver/library version mismatch",
    "failed to initialize nvml",
)

GPU_RUNTIME_ERROR_PATTERNS = (
    *GPU_DRIVER_MISMATCH_PATTERNS,
    "nvidia-container-cli",
    "could not select device driver",
    "nvidia.com/gpu",
)

GPU_STARTUP_GATE = "stream-v3.io/gpu-ready"
GPU_GATE_BOOT_ANNOTATION = "stream-v3.io/gpu-gate-boot-id"
STREAM_ESTABLISHED_BOOT_ANNOTATION = "stream-v3.io/stream-established-boot-id"


def summarize_runtime_startup(
    pods_json: dict[str, Any],
    *,
    deployment: str = "stream-v3-runtime",
    expected_boot_id: str = "",
) -> dict[str, Any]:
    pods: list[dict[str, Any]] = []
    for item in list_dicts(pods_json.get("items")):
        metadata = child(item, "metadata")
        pod_name = str(metadata.get("name") or "")
        if deployment and not pod_name.startswith(f"{deployment}-"):
            continue
        annotations = child(metadata, "annotations")
        spec = child(item, "spec")
        gates = list_dicts(spec.get("schedulingGates"))
        has_gpu_gate = any(str(gate.get("name") or "") == GPU_STARTUP_GATE for gate in gates)
        released_boot_id = str(annotations.get(GPU_GATE_BOOT_ANNOTATION) or "")
        established_boot_id = str(annotations.get(STREAM_ESTABLISHED_BOOT_ANNOTATION) or "")
        released_for_current_boot = not expected_boot_id or released_boot_id == expected_boot_id
        protected = bool(
            has_gpu_gate
            or not released_boot_id
            or not released_for_current_boot
            or established_boot_id != released_boot_id
        )
        if has_gpu_gate:
            reason = "waiting for GPU device-plugin startup gate"
        elif not released_boot_id:
            reason = "GPU startup gate has not released this Pod"
        elif not released_for_current_boot:
            reason = "runtime Pod belongs to a previous host boot"
        elif established_boot_id != released_boot_id:
            reason = "current Pod has not established NVENC RTMPS delivery"
        else:
            reason = "current Pod has established NVENC RTMPS delivery"
        pods.append(
            {
                "pod": pod_name,
                "protected": protected,
                "reason": reason,
                "has_gpu_gate": has_gpu_gate,
                "released_boot_id": released_boot_id,
                "established_boot_id": established_boot_id,
                "expected_boot_id": expected_boot_id,
            }
        )

    protected_pods = [pod for pod in pods if bool(pod.get("protected"))]
    if not pods:
        status = "pod_missing"
        restart_blocked = True
        reason = "runtime Pod is not present during startup"
    elif protected_pods:
        status = "starting"
        restart_blocked = True
        reason = first_nonempty(pod.get("reason") for pod in protected_pods)
    else:
        status = "established"
        restart_blocked = False
        reason = "current Pod has established NVENC RTMPS delivery"
    return {
        "available": bool(pods),
        "status": status,
        "restart_blocked": restart_blocked,
        "restart_block_reason": reason if restart_blocked else "",
        "pod_count": len(pods),
        "pods": pods,
    }


def runtime_node_name(
    pods_json: dict[str, Any],
    *,
    deployment: str = "stream-v3-runtime",
) -> str:
    for item in list_dicts(pods_json.get("items")):
        metadata = child(item, "metadata")
        pod_name = str(metadata.get("name") or "")
        if deployment and not pod_name.startswith(f"{deployment}-"):
            continue
        node_name = str(child(item, "spec").get("nodeName") or "")
        if node_name:
            return node_name
    return ""


def summarize_runtime_gpu(
    deployment_json: dict[str, Any],
    pods_json: dict[str, Any],
    *,
    deployment: str = "stream-v3-runtime",
    container_name: str = "stream-engine",
) -> dict[str, Any]:
    gpu_requested = deployment_requests_gpu(deployment_json, container_name=container_name)
    pods = runtime_pod_statuses(pods_json, deployment=deployment, container_name=container_name)
    stream_engine_ready = any(bool(item.get("ready")) for item in pods)
    stream_engine_running = any(item.get("state") == "running" for item in pods)
    container_waiting = any(item.get("state") == "waiting" for item in pods)
    driver_mismatch = any(bool(item.get("driver_mismatch")) for item in pods)
    gpu_runtime_error = any(bool(item.get("gpu_runtime_error")) for item in pods)
    restart_blocked = bool(gpu_requested and gpu_runtime_error and not stream_engine_ready)

    if restart_blocked and driver_mismatch:
        status = "driver_mismatch"
    elif restart_blocked:
        status = "gpu_runtime_error"
    elif gpu_requested and stream_engine_ready and not gpu_runtime_error:
        status = "ok"
    elif not pods:
        status = "pod_missing"
    else:
        status = "not_ready"

    reason = first_nonempty(item.get("reason_detail", "") for item in pods) or status
    return {
        "available": bool(pods),
        "status": status,
        "status_ok": status == "ok",
        "restart_blocked": restart_blocked,
        "restart_block_reason": reason if restart_blocked else "",
        "gpu_requested": gpu_requested,
        "driver_mismatch": driver_mismatch,
        "gpu_runtime_error": gpu_runtime_error,
        "stream_engine_ready": stream_engine_ready,
        "stream_engine_running": stream_engine_running,
        "container_waiting": container_waiting,
        "pod_count": len(pods),
        "pods": pods,
    }


def deployment_requests_gpu(deployment_json: dict[str, Any], *, container_name: str) -> bool:
    pod_spec = child(child(child(deployment_json, "spec"), "template"), "spec")
    for container in list_dicts(pod_spec.get("containers")):
        if str(container.get("name") or "") != container_name:
            continue
        resources = child(container, "resources")
        for key in ("limits", "requests"):
            quantity = child(resources, key).get("nvidia.com/gpu")
            if quantity_is_positive(quantity):
                return True
        return False
    return False


def runtime_pod_statuses(
    pods_json: dict[str, Any],
    *,
    deployment: str,
    container_name: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in list_dicts(pods_json.get("items")):
        metadata = child(item, "metadata")
        pod_name = str(metadata.get("name") or "")
        if deployment and not pod_name.startswith(f"{deployment}-"):
            continue
        status = child(item, "status")
        phase = str(status.get("phase") or "")
        container_status = find_container_status(status, container_name)
        state, reason, message = current_container_state(container_status)
        reason_detail = compact_detail(reason, message)
        result.append(
            {
                "pod": pod_name,
                "container": container_name,
                "phase": phase,
                "ready": bool(container_status.get("ready")),
                "state": state,
                "reason": reason,
                "message": message,
                "reason_detail": reason_detail,
                "driver_mismatch": has_any_pattern(reason_detail, GPU_DRIVER_MISMATCH_PATTERNS),
                "gpu_runtime_error": has_any_pattern(reason_detail, GPU_RUNTIME_ERROR_PATTERNS),
            }
        )
    return result


def find_container_status(status: dict[str, Any], container_name: str) -> dict[str, Any]:
    for item in list_dicts(status.get("containerStatuses")):
        if str(item.get("name") or "") == container_name:
            return item
    return {}


def current_container_state(container_status: dict[str, Any]) -> tuple[str, str, str]:
    state = child(container_status, "state")
    for state_name in ("waiting", "terminated", "running"):
        detail = child(state, state_name)
        if not detail:
            continue
        return state_name, str(detail.get("reason") or ""), str(detail.get("message") or "")
    return "unknown", "", ""


def compact_detail(reason: str, message: str, *, limit: int = 240) -> str:
    detail = ": ".join(part for part in (reason.strip(), message.strip()) if part)
    if len(detail) <= limit:
        return detail
    return detail[: limit - 3] + "..."


def child(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key) if isinstance(parent, dict) else None
    return value if isinstance(value, dict) else {}


def list_dicts(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def quantity_is_positive(value: Any) -> bool:
    if value in (None, "", "0", 0, 0.0):
        return False
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return bool(str(value).strip())


def has_any_pattern(text: str, patterns: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(pattern in lowered for pattern in patterns)


def first_nonempty(values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""
