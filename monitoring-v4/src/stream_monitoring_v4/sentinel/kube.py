from __future__ import annotations

import json
import subprocess
from typing import Any

from .contracts import (
    SHA256_PATTERN,
    integer,
    json_object,
    mapping,
    mapping_list,
    text,
)


def command(command_line: list[str], *, timeout: int = 8) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            command_line,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, type(exc).__name__
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        return False, (detail[-1] if detail else f"exit_{result.returncode}")[:200]
    return True, result.stdout


def pods(kubectl: str) -> tuple[bool, dict[str, Any], str]:
    ok, output = command(
        [kubectl, "kubectl", "-n", "stream-monitoring-v4", "get", "pods", "-o", "json"]
    )
    if not ok:
        return False, {}, output
    try:
        payload = json_object(output)
        items = mapping_list(payload.get("items", []))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False, {}, "pod_json_invalid"
    result: dict[str, Any] = {}
    try:
        for item in items:
            metadata = mapping(item.get("metadata", {}))
            status = mapping(item.get("status", {}))
            spec = mapping(item.get("spec", {}))
            labels = mapping(metadata.get("labels", {}))
            conditions = mapping_list(status.get("conditions", []))
            containers = mapping_list(spec.get("containers", []))
            container_statuses = mapping_list(status.get("containerStatuses", []))
            name = text(metadata, "name")
            uid = text(metadata, "uid")
            component_value = labels.get("app.kubernetes.io/component", "")
            phase_value = status.get("phase", "")
            if not isinstance(component_value, str) or not isinstance(phase_value, str):
                raise ValueError("pod component and phase must be strings")
            ready = any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in conditions
            )
            images = [text(value, "image") for value in containers]
            image_ids = [text(value, "imageID") for value in container_statuses]
            restart_counts = [integer(value, "restartCount") for value in container_statuses]
            if any(value < 0 for value in restart_counts):
                raise ValueError("pod restart count must not be negative")
            if name in result:
                raise ValueError("pod name must be unique")
            if ready and (not uid or not images or len(images) != len(image_ids)):
                raise ValueError("ready pod container status is incomplete")
            result[name] = {
                "uid": uid,
                "component": component_value,
                "phase": phase_value,
                "ready": ready,
                "images": images,
                "image_ids": image_ids,
                "restart_count": sum(restart_counts),
            }
    except (TypeError, ValueError, OverflowError):
        return False, {}, "pod_json_invalid"
    return True, result, ""


def release_identity(kubectl: str) -> tuple[bool, dict[str, str], str]:
    ok, output = command(
        [
            kubectl,
            "kubectl",
            "-n",
            "stream-monitoring-v4",
            "get",
            "configmap",
            "monitoring-v4-release",
            "-o",
            "json",
        ]
    )
    if not ok:
        return False, {}, output
    try:
        data = mapping(json_object(output).get("data", {}))
        revision = text(data, "build-revision")
        image_id = text(data, "app-image-id")
    except (TypeError, ValueError, json.JSONDecodeError):
        return False, {}, "release_json_invalid"
    if len(revision) != 40 or any(
        character not in "0123456789abcdef" for character in revision
    ):
        return False, {}, "release_revision_invalid"
    digest = SHA256_PATTERN.search(image_id)
    if digest is None:
        return False, {}, "release_image_id_invalid"
    return True, {"revision": revision, "app_image_id": digest.group(0)}, ""
