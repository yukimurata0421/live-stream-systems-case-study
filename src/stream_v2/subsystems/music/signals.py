from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ...timeutil import age_seconds, parse_utc
from ..common import text, truthy
from . import actions


@dataclass(frozen=True)
class MusicSignals:
    now_playing_fresh: bool
    now_playing_status: str
    now_playing_title: str
    now_playing_age_sec: float | None
    audio_energy_low: bool
    pulse_source_missing: bool
    pulse_route_ok: bool
    pulse_route_anomaly: bool
    play_history_recent: bool
    track_transition_within_grace: bool
    bucket_boundary_within_grace: bool
    audio_fail_count: int
    pulse_source_missing_count: int
    reason: str
    observed_ts_utc: str

    @property
    def has_healthy_signal(self) -> bool:
        return self.now_playing_fresh or self.pulse_route_ok or self.play_history_recent

    def healthy_evidence_names(self) -> list[str]:
        names: list[str] = []
        if self.now_playing_fresh:
            names.append("now_playing_fresh")
        if self.pulse_route_ok:
            names.append("pulse_route_ok")
        if self.play_history_recent:
            names.append("play_history_recent")
        return names

    def failure_names(self) -> list[str]:
        failures: list[str] = []
        if self.audio_energy_low:
            if self.track_transition_within_grace or self.bucket_boundary_within_grace:
                failures.append(actions.FAILURE_AUDIO_ENERGY_LOW_TRANSITION_GRACE)
            else:
                failures.append(actions.FAILURE_AUDIO_ENERGY_LOW)
        if self.pulse_source_missing:
            failures.append(actions.FAILURE_PULSE_SOURCE_MISSING)
        if self.pulse_route_anomaly:
            failures.append(actions.FAILURE_PULSE_ROUTE_ANOMALY)
        if not self.now_playing_fresh and self.now_playing_age_sec is not None and self.now_playing_age_sec > 180:
            failures.append(actions.FAILURE_NOW_PLAYING_STALE)
        return failures


def collect_signals(
    *,
    timeline: dict[str, Any],
    restart_reason: dict[str, Any],
    overlay_now_playing: dict[str, Any],
    pulse_health: dict[str, Any],
    play_history: dict[str, Any],
    audio_fail_count: int,
    pulse_source_missing_count: int,
    now: datetime,
) -> MusicSignals:
    now_playing = timeline.get("now_playing_state") if isinstance(timeline.get("now_playing_state"), dict) else {}
    if not now_playing and overlay_now_playing:
        nested = overlay_now_playing.get("now_playing") if isinstance(overlay_now_playing.get("now_playing"), dict) else {}
        now_playing = {
            "updated_at_utc": overlay_now_playing.get("updated_at_utc"),
            "status": overlay_now_playing.get("status"),
            "title": nested.get("title") or overlay_now_playing.get("title"),
            "bucket": nested.get("bucket") or overlay_now_playing.get("bucket"),
            "prefix": nested.get("prefix") or overlay_now_playing.get("prefix"),
        }
    observed_at = parse_utc(now_playing.get("updated_at_utc"))
    age = age_seconds(observed_at, now)
    status = text(now_playing.get("status")).lower()
    fresh_status = status in {"playing", "transition", "starting"}
    now_playing_fresh = bool(now_playing) and age is not None and age <= 90 and fresh_status

    reason = " ".join(
        part for part in [
            text(restart_reason.get("reason")),
            text(timeline.get("reason")),
        ] if part
    ).lower()
    component = text(restart_reason.get("component")).lower()
    inferred_audio_count = _int(restart_reason.get("consecutive_fail_count"), default=audio_fail_count)
    if inferred_audio_count <= 0 and ("audio energy" in reason or "audio_energy_low" in reason or "silent" in reason):
        inferred_audio_count = 1
    audio_energy_low = (
        component in {"dj", "audio", "stream", ""}
        and ("audio energy" in reason or "audio_energy_low" in reason or "silent" in reason or inferred_audio_count > 0)
    )
    pulse_source_missing = (
        "pulse source" in reason
        or "pulse_source_missing" in reason
        or "stream_capture_source_output_missing" in reason
        or pulse_source_missing_count > 0
    )
    pulse_route = _pulse_route(pulse_health)
    transition = _transition_detail(timeline=timeline, restart_reason=restart_reason, overlay_now_playing=overlay_now_playing)
    play_history_recent = _play_history_recent(play_history, now)

    return MusicSignals(
        now_playing_fresh=now_playing_fresh,
        now_playing_status=status,
        now_playing_title=text(now_playing.get("title")),
        now_playing_age_sec=age,
        audio_energy_low=audio_energy_low,
        pulse_source_missing=pulse_source_missing,
        pulse_route_ok=pulse_route["ok"],
        pulse_route_anomaly=pulse_route["anomaly"],
        play_history_recent=play_history_recent,
        track_transition_within_grace=transition["track_transition_within_grace"],
        bucket_boundary_within_grace=transition["bucket_boundary_within_grace"],
        audio_fail_count=inferred_audio_count,
        pulse_source_missing_count=pulse_source_missing_count,
        reason=reason,
        observed_ts_utc=text(now_playing.get("updated_at_utc") or restart_reason.get("ts_utc") or timeline.get("ts_utc") or play_history.get("ts_utc")),
    )


def _pulse_route(payload: dict[str, Any]) -> dict[str, bool]:
    if not payload:
        return {"ok": False, "anomaly": False}
    counts = [
        _int(payload.get("dj_missing_count"), default=0),
        _int(payload.get("capture_missing_count"), default=0),
        _int(payload.get("dj_latency_high_count"), default=0),
        _int(payload.get("capture_latency_high_count"), default=0),
    ]
    explicit_ok = truthy(payload.get("ok")) or text(payload.get("status")).lower() in {"ok", "healthy"}
    anomaly = any(value > 0 for value in counts) or text(payload.get("status")).lower() in {"failed", "fail", "unhealthy"}
    return {"ok": (explicit_ok or not anomaly) and bool(payload), "anomaly": anomaly}


def _transition_detail(*, timeline: dict[str, Any], restart_reason: dict[str, Any], overlay_now_playing: dict[str, Any]) -> dict[str, bool]:
    detail = timeline.get("now_playing_transition_detail") if isinstance(timeline.get("now_playing_transition_detail"), dict) else {}
    if not detail:
        detail = restart_reason
    note = text(overlay_now_playing.get("note")).lower()
    return {
        "track_transition_within_grace": truthy(detail.get("track_transition_within_grace")) and "heartbeat update" not in note,
        "bucket_boundary_within_grace": truthy(detail.get("bucket_boundary_within_grace")),
    }


def _play_history_recent(payload: dict[str, Any], now: datetime) -> bool:
    if not payload:
        return False
    ts = parse_utc(payload.get("ts_utc") or payload.get("ts_jst") or payload.get("selected_at_utc") or payload.get("started_at_utc"))
    age = age_seconds(ts, now) if ts else None
    event = text(payload.get("event") or payload.get("kind")).lower()
    return age is not None and age <= 900 and event in {"", "track_selected", "track_started", "selected", "play"}


def _int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
