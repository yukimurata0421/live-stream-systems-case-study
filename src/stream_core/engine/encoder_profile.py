from __future__ import annotations

import time
from typing import Any

try:
    from stream_core.recovery_profile import select_encoder_profile
except ModuleNotFoundError:
    from recovery_profile import select_encoder_profile


def emergency_low_upload_profile(cfg: Any, restart_context: dict[str, Any] | None) -> dict[str, object] | None:
    profile = effective_encoder_profile(cfg, restart_context)
    if profile.get("mode") != "emergency_low_upload":
        return None
    return profile


def effective_encoder_profile(cfg: Any, restart_context: dict[str, Any] | None) -> dict[str, object]:
    return select_encoder_profile(
        restart_context=restart_context,
        enabled=cfg.emergency_low_upload_enabled,
        triggers=set(cfg.emergency_low_upload_triggers),
        fallback_duration_sec=cfg.emergency_low_upload_duration_sec,
        fallback_video_bitrate=cfg.emergency_low_upload_video_bitrate,
        fallback_video_maxrate=cfg.emergency_low_upload_video_maxrate,
        fallback_video_bufsize=cfg.emergency_low_upload_video_bufsize,
        fallback_audio_bitrate=cfg.emergency_low_upload_audio_bitrate,
        normal_video_bitrate=cfg.video_bitrate,
        normal_video_maxrate=cfg.video_maxrate,
        normal_video_bufsize=cfg.video_bufsize,
        normal_audio_bitrate=cfg.audio_bitrate,
    )


def encoder_profile_expired(profile: dict[str, object], *, now_ts: float | None = None) -> bool:
    if profile.get("mode") != "emergency_low_upload":
        return False
    try:
        until_ts = float(profile.get("until_ts", 0) or 0)
    except (TypeError, ValueError):
        return False
    current = time.time() if now_ts is None else now_ts
    return until_ts > 0 and current >= until_ts

