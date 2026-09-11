from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.runtime_evidence import (
    RuntimeLifecycleEvent,
    RuntimeLifecycleProjection,
)
from stream_contracts.monitoring_v4.time import unix_ts
from stream_monitoring_v4.adapters.json_file import (
    SnapshotReadError,
    strict_json_loads,
)

from .constants import RAW_LIFECYCLE_FILES
from .sanitize import timestamp, token
from .stable_io import stable_text_lines


def runtime_lifecycle_projection(source_root: Path) -> RuntimeLifecycleProjection:
    raw_events: dict[str, Mapping[str, Any]] = {}
    for index, relative in enumerate(RAW_LIFECYCLE_FILES):
        lines = stable_text_lines(
            source_root / relative,
            required=index == len(RAW_LIFECYCLE_FILES) - 1,
        )
        for line in lines:
            if not line.strip():
                continue
            try:
                value = strict_json_loads(line)
            except (json.JSONDecodeError, ValueError) as exc:
                raise SnapshotReadError(
                    "source_json_invalid",
                    "runtime lifecycle JSONL line is invalid",
                ) from exc
            if not isinstance(value, Mapping):
                raise SnapshotReadError(
                    "source_not_object",
                    "runtime lifecycle JSONL line is not an object",
                )
            event_id = token(value.get("event_id"), default="")
            event_type = token(value.get("event_type"), default="")
            if event_type in {"ffmpeg_restart_scheduled", "ffmpeg_started"} and not event_id:
                raise SnapshotReadError(
                    "source_value_invalid",
                    "runtime lifecycle event_id is invalid",
                )
            if event_id:
                raw_events[event_id] = value

    ordered = sorted(
        raw_events.values(),
        key=lambda item: (
            str(item.get("ts_utc", "")),
            str(item.get("event_id", "")),
        ),
    )
    started: dict[tuple[str, int], list[Mapping[str, Any]]] = {}
    scheduled: list[Mapping[str, Any]] = []
    for item in ordered:
        event_type = token(item.get("event_type"), default="")
        if event_type not in {"ffmpeg_restart_scheduled", "ffmpeg_started"}:
            continue
        restart_count = item.get("restart_count")
        if type(restart_count) is not int:
            raise SnapshotReadError(
                "source_value_invalid",
                "runtime restart_count is invalid",
            )
        run_id = token(item.get("run_id"), default="")
        if not run_id:
            raise SnapshotReadError(
                "source_value_invalid",
                "runtime lifecycle run_id is invalid",
            )
        key = (run_id, restart_count)
        if event_type == "ffmpeg_started":
            started.setdefault(key, []).append(item)
        else:
            scheduled.append(item)

    projected: list[RuntimeLifecycleEvent] = []
    for item in scheduled:
        restart_count = item.get("restart_count")
        exit_code = item.get("exit_code")
        delay_sec = item.get("delay_sec")
        if any(
            type(value) is not int
            for value in (restart_count, exit_code, delay_sec)
        ):
            raise SnapshotReadError(
                "source_value_invalid",
                "runtime recovery value is invalid",
            )
        run_id = token(item.get("run_id"), default="")
        opened_at = timestamp(item.get("ts_utc"))
        try:
            opened_ts = unix_ts(opened_at)
        except ValueError as exc:
            raise SnapshotReadError(
                "source_timestamp_invalid",
                "runtime restart timestamp is invalid",
            ) from exc
        candidates = [
            candidate
            for candidate in started.get((run_id, restart_count), [])
            if unix_ts(timestamp(candidate.get("ts_utc"))) >= opened_ts
        ]
        if not candidates:
            continue
        recovered = min(
            candidates,
            key=lambda candidate: (
                str(candidate.get("ts_utc", "")),
                str(candidate.get("event_id", "")),
            ),
        )
        scheduled_event_id = token(item.get("event_id"), default="")
        recovered_event_id = token(recovered.get("event_id"), default="")
        projected.append(
            RuntimeLifecycleEvent(
                event_id=scheduled_event_id,
                event_type="ffmpeg_auto_recovered",
                opened_at=opened_at,
                recovered_at=timestamp(recovered.get("ts_utc")),
                run_id=run_id,
                restart_count=restart_count,
                exit_code=exit_code,
                delay_sec=delay_sec,
                scheduled_event_id=scheduled_event_id,
                recovered_event_id=recovered_event_id,
            )
        )
    projected.sort(key=lambda item: (item.opened_at, item.event_id))
    return RuntimeLifecycleProjection(tuple(projected[-32:]))
