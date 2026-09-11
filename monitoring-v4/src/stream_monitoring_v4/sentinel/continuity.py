from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from stream_monitoring_v4.adapters.json_file import SnapshotReadError, read_json_snapshot

from .contracts import REQUIRED_COMPONENT_COUNTS, SENTINEL_SCHEMA


def previous_status(path: Path) -> dict[str, Any]:
    try:
        payload = read_json_snapshot(path, max_bytes=2 * 1024 * 1024).payload
    except (OSError, TypeError, ValueError, json.JSONDecodeError, SnapshotReadError):
        return {}
    return dict(payload)


def previous_continuity(
    previous: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], str, bool]:
    if previous.get("schema") != SENTINEL_SCHEMA:
        return {}, "", False
    raw_runtime = previous.get("pod_runtime")
    raw_release = previous.get("release")
    if not isinstance(raw_runtime, Mapping) or not isinstance(raw_release, Mapping):
        return {}, "", False
    revision = raw_release.get("revision")
    if (
        not isinstance(revision, str)
        or len(revision) != 40
        or any(character not in "0123456789abcdef" for character in revision)
    ):
        return {}, "", False
    runtime: dict[str, dict[str, Any]] = {}
    counts = {component: 0 for component in REQUIRED_COMPONENT_COUNTS}
    seen_uids: set[str] = set()
    for name, raw in raw_runtime.items():
        if not isinstance(name, str) or not isinstance(raw, Mapping):
            return {}, "", False
        uid = raw.get("uid")
        component = raw.get("component")
        restart_count = raw.get("restart_count")
        if (
            not isinstance(uid, str)
            or not uid
            or uid in seen_uids
            or not isinstance(component, str)
            or component not in REQUIRED_COMPONENT_COUNTS
            or type(restart_count) is not int
            or restart_count < 0
        ):
            return {}, "", False
        seen_uids.add(uid)
        counts[component] += 1
        runtime[name] = {
            "uid": uid,
            "component": component,
            "restart_count": restart_count,
        }
    complete = counts == REQUIRED_COMPONENT_COUNTS
    return runtime, revision, complete
