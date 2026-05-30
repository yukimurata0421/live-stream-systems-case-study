from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ...timeutil import age_seconds, parse_utc
from ..common import text, truthy
from . import actions


@dataclass(frozen=True)
class RenderingSignals:
    runtime_snapshot_fresh: bool
    watchdog_ok: bool
    overlay_unavailable: bool
    stream1090_unavailable: bool
    adsb_unhealthy: bool
    video_frame_unhealthy: bool
    runtime_snapshot_stale: bool
    stream1090_report_fresh: bool
    stream1090_report_ok: bool
    upstream_report_fresh: bool
    upstream_report_ok: bool
    aircraft_json_ok: bool
    aircraft_messages_moving: bool
    aircraft_positions_moving: bool
    adsb_freshness_ok: bool
    adsb_freshness_stale: bool
    overlay_report_ts_utc: str
    upstream_report_ts_utc: str
    stream1090_target: str
    reason: str
    timeline_ts_utc: str
    runtime_snapshot_updated_at_utc: str
    runtime_snapshot_age_sec: float | None

    @property
    def has_healthy_signal(self) -> bool:
        return self.runtime_snapshot_fresh or self.watchdog_ok or self.stream1090_report_ok or self.adsb_freshness_ok

    def healthy_evidence_names(self) -> list[str]:
        names: list[str] = []
        if self.runtime_snapshot_fresh:
            names.append("runtime_snapshot_fresh")
        if self.watchdog_ok:
            names.append("watchdog_ok")
        if self.stream1090_report_ok:
            names.append("stream1090_report_ok")
        if self.upstream_report_ok:
            names.append("upstream_stream1090_report_ok")
        if self.adsb_freshness_ok:
            names.append("adsb_freshness_ok")
        return names

    def failure_names(self) -> list[str]:
        failures: list[str] = []
        if self.overlay_unavailable:
            failures.append(actions.FAILURE_OVERLAY_UNAVAILABLE)
        if self.stream1090_unavailable:
            failures.append(actions.FAILURE_STREAM1090_UNAVAILABLE)
        if self.adsb_unhealthy:
            failures.append(actions.FAILURE_ADSB_FRESHNESS_STALL)
        if self.video_frame_unhealthy:
            failures.append(actions.FAILURE_VIDEO_FRAME_UNHEALTHY)
        if self.runtime_snapshot_stale:
            failures.append(actions.FAILURE_RUNTIME_SNAPSHOT_STALE)
        if self.upstream_report_fresh and not self.upstream_report_ok:
            failures.append(actions.FAILURE_UPSTREAM_STREAM1090_UNAVAILABLE)
        return failures


def collect_signals(
    *,
    stream_stats: dict[str, Any],
    timeline: dict[str, Any],
    runtime: dict[str, Any],
    stream1090_report: dict[str, Any],
    upstream_report: dict[str, Any],
    adsb_freshness: dict[str, Any],
    now: datetime,
) -> RenderingSignals:
    runtime_snapshot = timeline.get("runtime_snapshot") if isinstance(timeline.get("runtime_snapshot"), dict) else {}
    timeline_age = _float(runtime_snapshot.get("age_sec"))
    runtime_ts = text(runtime_snapshot.get("updated_at_utc") or runtime.get("updated_at_utc"))
    parsed_runtime_ts = parse_utc(runtime_ts)
    computed_runtime_age = age_seconds(parsed_runtime_ts, now) if parsed_runtime_ts else None
    runtime_age = timeline_age if timeline_age is not None else computed_runtime_age
    runtime_snapshot_fresh = bool(runtime_snapshot) and runtime_age is not None and runtime_age <= 90

    status = text(stream_stats.get("status")).lower()
    judgment = text(stream_stats.get("judgment")).lower()
    reason = " ".join(
        part for part in [text(stream_stats.get("reason")), text(timeline.get("reason"))]
        if part
    ).lower()
    watchdog_ok = status == "ok" and judgment in {"ok", ""}

    stream1090 = _report_signals(stream1090_report, now)
    upstream = _report_signals(upstream_report, now)
    adsb = _adsb_signals(adsb_freshness, now)

    overlay_unavailable = "overlay" in reason and any(token in reason for token in ["unavailable", "unhealthy", "not listening", "index"])
    stream1090_unavailable = "stream1090" in reason or "map proxy" in reason
    adsb_unhealthy = "adsb" in reason or "aircraft json" in reason or "messages stalled" in reason
    video_frame_unhealthy = "video frame" in reason or "dark frame" in reason or "luma" in reason
    runtime_snapshot_stale = bool(runtime or timeline or stream_stats) and not runtime_snapshot_fresh and (
        "runtime_snapshot" in reason or _float(stream_stats.get("runtime_snapshot_age_sec"), default=0) > 90
    )
    if stream1090["fresh"]:
        overlay_unavailable = overlay_unavailable or not stream1090["html_reachable"] or stream1090["judgment_bad"]
        stream1090_unavailable = stream1090_unavailable or stream1090["stream1090_unavailable"]
        adsb_unhealthy = adsb_unhealthy or not stream1090["aircraft_json_ok"] or not stream1090["aircraft_moving"]
        video_frame_unhealthy = video_frame_unhealthy or stream1090["visual_unhealthy"]
    if adsb["fresh"]:
        adsb_unhealthy = adsb_unhealthy or adsb["stale"]

    return RenderingSignals(
        runtime_snapshot_fresh=runtime_snapshot_fresh,
        watchdog_ok=watchdog_ok,
        overlay_unavailable=overlay_unavailable,
        stream1090_unavailable=stream1090_unavailable,
        adsb_unhealthy=adsb_unhealthy,
        video_frame_unhealthy=video_frame_unhealthy,
        runtime_snapshot_stale=runtime_snapshot_stale,
        stream1090_report_fresh=stream1090["fresh"],
        stream1090_report_ok=stream1090["ok"],
        upstream_report_fresh=upstream["fresh"],
        upstream_report_ok=upstream["ok"],
        aircraft_json_ok=stream1090["aircraft_json_ok"],
        aircraft_messages_moving=stream1090["messages_moving"],
        aircraft_positions_moving=stream1090["positions_moving"],
        adsb_freshness_ok=adsb["ok"],
        adsb_freshness_stale=adsb["stale"],
        overlay_report_ts_utc=text(stream1090_report.get("ts_utc")),
        upstream_report_ts_utc=text(upstream_report.get("ts_utc")),
        stream1090_target=text(stream1090_report.get("target")),
        reason=reason,
        timeline_ts_utc=text(timeline.get("ts_utc") or stream_stats.get("ts_utc")),
        runtime_snapshot_updated_at_utc=runtime_ts,
        runtime_snapshot_age_sec=runtime_age,
    )


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _report_signals(payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
    ts = parse_utc(payload.get("ts_utc"))
    age = age_seconds(ts, now) if ts else None
    fresh = bool(payload) and age is not None and age <= 1800
    html_reachable = truthy(checks.get("html_reachable"))
    aircraft_json_ok = truthy(checks.get("aircraft_json_ok"))
    messages_delta = _float(checks.get("messages_delta"), default=0) or 0
    position_change_count = _float(checks.get("position_change_count"), default=0) or 0
    messages_moving = messages_delta > 0
    positions_moving = position_change_count > 0
    # At night, aircraft counts and position changes can be very low. Message
    # movement is the primary ADS-B freshness signal; positions are corroborating
    # evidence, not a hard requirement.
    aircraft_moving = messages_moving or positions_moving
    warnings = checks.get("warnings") if isinstance(checks.get("warnings"), list) else []
    judgment = text(payload.get("judgment")).lower()
    judgment_bad = bool(payload) and judgment not in {"", "ok", "report_only_ok"}
    visual = checks.get("visual") if isinstance(checks.get("visual"), dict) else {}
    visual_unhealthy = truthy(payload.get("visual_unhealthy")) or truthy(visual.get("unhealthy")) or text(visual.get("verdict")).lower() in {"failed", "unhealthy"}
    stream1090_unavailable = any("stream1090" in text(item).lower() for item in warnings)
    ok = fresh and html_reachable and aircraft_json_ok and aircraft_moving and not judgment_bad and not visual_unhealthy
    return {
        "fresh": fresh,
        "ok": ok,
        "html_reachable": html_reachable,
        "aircraft_json_ok": aircraft_json_ok,
        "messages_moving": messages_moving,
        "positions_moving": positions_moving,
        "aircraft_moving": aircraft_moving,
        "judgment_bad": judgment_bad,
        "visual_unhealthy": visual_unhealthy,
        "stream1090_unavailable": stream1090_unavailable,
    }


def _adsb_signals(payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    ts = parse_utc(payload.get("ts_utc") or payload.get("updated_at_utc"))
    age = age_seconds(ts, now) if ts else None
    fresh = bool(payload) and age is not None and age <= 180
    status = text(payload.get("status") or payload.get("judgment")).lower()
    stale = fresh and (
        status in {"stale", "failed", "fail", "unhealthy"}
        or truthy(payload.get("stale"))
        or truthy(payload.get("messages_stalled"))
        or truthy(payload.get("aircraft_json_stale"))
    )
    ok = fresh and not stale and status in {"", "ok", "healthy"}
    return {"fresh": fresh, "ok": ok, "stale": stale}
