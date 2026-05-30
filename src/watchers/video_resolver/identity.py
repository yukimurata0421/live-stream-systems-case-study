from __future__ import annotations

import hashlib
from datetime import datetime


def parse_iso_ts(raw: str) -> int:
    text = raw.strip()
    if not text:
        return 0
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp())
    except Exception:
        return 0


def latest_iso_ts(*values: str) -> str:
    best_raw = ""
    best_ts = 0
    for raw in values:
        text = str(raw or "").strip()
        ts = parse_iso_ts(text)
        if ts > best_ts:
            best_ts = ts
            best_raw = text
    return best_raw


def build_remote_sample_id(
    *,
    remote_probe_ts_utc: str,
    remote_source: str,
    recovery_episode_id: str,
    ffmpeg_generation: str,
    selected_video_id: str,
) -> str:
    if not remote_probe_ts_utc or not remote_source:
        return ""
    raw = "|".join(
        [
            remote_probe_ts_utc,
            remote_source,
            recovery_episode_id,
            ffmpeg_generation,
            selected_video_id,
        ]
    )
    return "rps-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def ffmpeg_generation_from_runtime(local_runtime: dict) -> str:
    main_pid = int(local_runtime.get("stream_main_pid", 0) or 0)
    ffmpeg_pid = int(local_runtime.get("ffmpeg_pid", 0) or 0)
    if main_pid <= 0 and ffmpeg_pid <= 0:
        return ""
    return f"stream_pid={main_pid}:ffmpeg_pid={ffmpeg_pid}"


def recovery_episode_id_from_state(state: dict, now_ts: int, fast_mode: bool) -> str:
    if not fast_mode:
        return ""
    start_ts = int(state.get("fast_search_window_start_ts", 0) or 0) or now_ts
    return f"fast-{start_ts}"
