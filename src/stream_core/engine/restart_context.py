from __future__ import annotations

import time

from recovery_profile import consume_context, parse_context_age_sec, read_context

from .events import utc_now


def restart_reason_payload(path) -> dict | None:
    return read_context(path)


def restart_reason_age_sec(payload: dict) -> float | None:
    return parse_context_age_sec(payload)


def restart_reason_is_recent(path, *, deadline: float) -> bool:
    if not path.exists():
        return False
    payload = restart_reason_payload(path)
    if payload is not None:
        if payload.get("consumed_at_utc"):
            return False
        age_sec = restart_reason_age_sec(payload)
        if age_sec is not None:
            return 0 <= age_sec <= deadline
    try:
        age_sec = time.time() - path.stat().st_mtime
    except OSError:
        return False
    return 0 <= age_sec <= deadline


def has_recent_restart_context(cfg) -> bool:
    if cfg.pre_ffmpeg_restart_context_max_age_sec <= 0:
        return False
    deadline = float(cfg.pre_ffmpeg_restart_context_max_age_sec)
    if restart_reason_is_recent(cfg.restart_reason_file, deadline=deadline):
        return True
    try:
        age_sec = time.time() - cfg.runtime_state_file.stat().st_mtime
    except OSError:
        return False
    return 0 <= age_sec <= deadline


def emit_startup_restart_context(cfg, *, run_id: str, stream_pid: int, append_event) -> None:
    if not cfg.restart_reason_file.exists():
        return
    payload = restart_reason_payload(cfg.restart_reason_file)
    if payload is None:
        append_event("startup_restart_context_error", note="failed to parse restart reason file")
        return

    age_sec = restart_reason_age_sec(payload)
    max_age_sec = cfg.pre_ffmpeg_restart_context_max_age_sec
    consumed_before = bool(payload.get("consumed_at_utc"))
    stale = False
    if consumed_before:
        stale = True
    elif age_sec is not None and max_age_sec > 0:
        stale = not (0 <= age_sec <= float(max_age_sec))

    event_id = append_event(
        "startup_restart_context",
        restart_context=payload,
        restart_context_age_sec=round(age_sec, 3) if age_sec is not None else None,
        restart_context_stale=stale,
        restart_context_consumed_before=consumed_before,
        restart_context_max_age_sec=max_age_sec,
    )
    if consumed_before:
        return

    consumed_payload = {
        **payload,
        "consumed_at_utc": utc_now(),
        "consumed_by": "stream_engine",
        "consumed_run_id": run_id,
        "consumed_event_id": event_id,
        "consumed_stream_pid": stream_pid,
        "stale_at_consume": stale,
        "age_sec_at_consume": round(age_sec, 3) if age_sec is not None else None,
    }
    try:
        consume_context(cfg.restart_reason_file, payload, consumed_payload=consumed_payload)
    except Exception:
        append_event("startup_restart_context_consume_error", note="failed to mark restart reason consumed")
