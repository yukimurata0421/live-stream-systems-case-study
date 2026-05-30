from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    from stream_core.recovery_profile import build_restart_context, low_upload_profile_from_trigger, write_context
except ImportError:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from stream_core.recovery_profile import build_restart_context, low_upload_profile_from_trigger, write_context


def build_fast_recovery_restart_context(
    *,
    reason_kind: str,
    reason: str,
    now_ts: int,
    stream_service: str,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    metrics: dict[str, Any] | None,
    emergency_low_upload_enabled: bool,
    emergency_low_upload_triggers: set[str],
    emergency_low_upload_duration_sec: int,
    emergency_low_upload_video_bitrate: str,
    emergency_low_upload_video_maxrate: str,
    emergency_low_upload_video_bufsize: str,
    emergency_low_upload_audio_bitrate: str,
) -> dict[str, Any]:
    profile = low_upload_profile_from_trigger(
        reason_kind=reason_kind,
        now_ts=now_ts,
        enabled=emergency_low_upload_enabled,
        triggers=emergency_low_upload_triggers,
        duration_sec=emergency_low_upload_duration_sec,
        video_bitrate=emergency_low_upload_video_bitrate,
        video_maxrate=emergency_low_upload_video_maxrate,
        video_bufsize=emergency_low_upload_video_bufsize,
        audio_bitrate=emergency_low_upload_audio_bitrate,
    )
    return build_restart_context(
        source="fast_recovery",
        component="stream",
        reason_kind=reason_kind,
        reason=reason,
        target_unit=stream_service,
        now_ts=now_ts,
        ffmpeg_pid=ffmpeg_pid,
        ffmpeg_uptime_sec=ffmpeg_uptime_sec,
        metrics=metrics,
        emergency_low_upload_profile=profile,
    )


def write_fast_recovery_restart_context(path: Path, payload: dict[str, Any]) -> None:
    write_context(path, payload)

