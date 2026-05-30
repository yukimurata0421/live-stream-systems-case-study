#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_text(ts: int | float | None = None) -> str:
    value = time.time() if ts is None else ts
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_context_age_sec(payload: dict[str, Any], *, now_ts: float | None = None) -> float | None:
    ts_raw = str(payload.get("ts_utc", "") or "").strip()
    if not ts_raw:
        return None
    try:
        ts_text = f"{ts_raw[:-1]}+00:00" if ts_raw.endswith("Z") else ts_raw
        dt = datetime.fromisoformat(ts_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (time.time() if now_ts is None else now_ts) - dt.timestamp()
    except Exception:
        return None


def read_context(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def write_context(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def consume_context(path: Path, payload: dict[str, Any], *, consumed_payload: dict[str, Any]) -> None:
    write_context(path, {**payload, **consumed_payload})


def low_upload_profile_from_trigger(
    *,
    reason_kind: str,
    now_ts: int,
    enabled: bool,
    triggers: set[str],
    duration_sec: int,
    video_bitrate: str,
    video_maxrate: str,
    video_bufsize: str,
    audio_bitrate: str = "",
) -> dict[str, Any] | None:
    if not enabled or reason_kind not in triggers:
        return None
    duration = max(60, int(duration_sec))
    profile_name = "network_down_low_upload" if reason_kind == "network_down" else f"{reason_kind}_low_upload"
    profile: dict[str, Any] = {
        "name": profile_name,
        "duration_sec": duration,
        "expires_at_utc": utc_text(now_ts + duration),
        "video_bitrate": video_bitrate,
        "video_maxrate": video_maxrate,
        "video_bufsize": video_bufsize,
    }
    if audio_bitrate:
        profile["audio_bitrate"] = audio_bitrate
    return profile


def build_restart_context(
    *,
    source: str,
    component: str,
    reason_kind: str,
    reason: str,
    target_unit: str,
    now_ts: int,
    ffmpeg_pid: int = 0,
    ffmpeg_uptime_sec: int = 0,
    metrics: dict[str, Any] | None = None,
    emergency_low_upload_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ts_utc": utc_text(now_ts),
        "source": source,
        "component": component,
        "reason": reason,
        "trigger": reason_kind,
        "target_unit": target_unit,
        "ffmpeg_pid": ffmpeg_pid,
        "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
    }
    if metrics:
        payload["metrics"] = metrics
    if emergency_low_upload_profile:
        payload["emergency_low_upload_profile"] = emergency_low_upload_profile
    return payload


def select_encoder_profile(
    *,
    restart_context: dict[str, Any] | None,
    enabled: bool,
    triggers: set[str],
    fallback_duration_sec: int,
    fallback_video_bitrate: str,
    fallback_video_maxrate: str,
    fallback_video_bufsize: str,
    fallback_audio_bitrate: str,
    normal_video_bitrate: str,
    normal_video_maxrate: str,
    normal_video_bufsize: str,
    normal_audio_bitrate: str,
    now_ts: float | None = None,
) -> dict[str, Any]:
    normal = {
        "name": "normal",
        "mode": "normal",
        "video_bitrate": normal_video_bitrate,
        "video_maxrate": normal_video_maxrate,
        "video_bufsize": normal_video_bufsize,
        "audio_bitrate": normal_audio_bitrate,
    }
    if not enabled or not restart_context:
        return normal
    trigger = str(restart_context.get("trigger") or restart_context.get("reason_kind") or "").strip()
    if not trigger and "network down" in str(restart_context.get("reason", "")).lower():
        trigger = "network_down"
    if trigger not in triggers:
        return normal

    age_sec = parse_context_age_sec(restart_context, now_ts=now_ts)
    if age_sec is None or age_sec < 0:
        return normal

    profile_payload = restart_context.get("emergency_low_upload_profile")
    profile = profile_payload if isinstance(profile_payload, dict) else {}
    try:
        duration_sec = int(profile.get("duration_sec", fallback_duration_sec) or fallback_duration_sec)
    except (TypeError, ValueError):
        duration_sec = fallback_duration_sec
    duration_sec = max(60, duration_sec)
    if age_sec > duration_sec:
        return normal

    return {
        "name": str(profile.get("name") or "network_down_low_upload"),
        "mode": "emergency_low_upload",
        "trigger": trigger,
        "age_sec": round(age_sec, 3),
        "duration_sec": duration_sec,
        "until_ts": (time.time() if now_ts is None else now_ts) + max(0.0, duration_sec - age_sec),
        "video_bitrate": str(profile.get("video_bitrate") or fallback_video_bitrate),
        "video_maxrate": str(profile.get("video_maxrate") or fallback_video_maxrate),
        "video_bufsize": str(profile.get("video_bufsize") or fallback_video_bufsize),
        "audio_bitrate": str(profile.get("audio_bitrate") or fallback_audio_bitrate or normal_audio_bitrate),
        "restart_context": restart_context,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Read stream recovery profile context JSON.")
    parser.add_argument("path", type=Path)
    parser.add_argument("--json", action="store_true", help="Print parsed context as JSON")
    args = parser.parse_args()
    payload = read_context(args.path)
    if args.json:
        print(json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":")))
    else:
        print(args.path if payload is not None else "")
    return 0 if payload is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
