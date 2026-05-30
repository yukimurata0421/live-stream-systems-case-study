from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ...timeutil import age_seconds, parse_utc
from ..common import text, truthy
from . import actions


@dataclass(frozen=True)
class SourceFreshness:
    source: str
    evidence_name: str
    file_name: str
    payload: dict[str, Any]
    ttl_sec: float
    observed_ts_utc: str
    age_sec: float | None
    fresh: bool


@dataclass(frozen=True)
class MonitoringSignals:
    sources: list[SourceFreshness]
    quota_guard_active: bool
    cost_report_degraded: bool

    @property
    def fresh_names(self) -> list[str]:
        return [item.evidence_name for item in self.sources if item.fresh]

    @property
    def stale_names(self) -> list[str]:
        return [item.evidence_name for item in self.sources if not item.fresh]

    def failure_names(self) -> list[str]:
        failures: list[str] = []
        if "watchdog_stats_fresh" in self.stale_names:
            failures.append(actions.FAILURE_WATCHDOG_STALE)
        if "resolver_cache_fresh" in self.stale_names:
            failures.append(actions.FAILURE_RESOLVER_STALE)
        if "stream_watchdog_fresh" in self.stale_names:
            failures.append(actions.FAILURE_STREAM_WATCHDOG_STALE)
        if self.quota_guard_active:
            failures.append(actions.FAILURE_QUOTA_GUARD_ACTIVE)
        if self.cost_report_degraded:
            failures.append(actions.FAILURE_COST_REPORT_DEGRADED)
        return failures


def collect_signals(*, ytw: dict[str, Any], resolver: dict[str, Any], cost: dict[str, Any], stream_stats: dict[str, Any], timeline: dict[str, Any], now: datetime) -> MonitoringSignals:
    specs = [
        ("youtube_watchdog", ytw, "watchdog_stats_fresh", 180.0, "youtube_watchdog_stats.json"),
        ("youtube_video_id_resolver", resolver, "resolver_cache_fresh", 300.0, "youtube_video_id_resolver_state.json"),
        ("youtube_api_cost_report", cost, "cost_report_fresh", 1800.0, "reports/youtube_api_cost/open_day_latest.json"),
        ("stream_watchdog", stream_stats or timeline, "stream_watchdog_fresh", 180.0, "stream_watchdog_stats.json"),
    ]
    return collect_signals_from_specs(specs=specs, ytw=ytw, cost=cost, now=now)


def collect_rich_signals(
    *,
    ytw: dict[str, Any],
    resolver: dict[str, Any],
    cost: dict[str, Any],
    stream_stats: dict[str, Any],
    timeline: dict[str, Any],
    stream1090_report: dict[str, Any],
    upstream_report: dict[str, Any],
    stream_engine_event: dict[str, Any],
    play_history: dict[str, Any],
    now: datetime,
) -> MonitoringSignals:
    specs = [
        ("youtube_watchdog", ytw, "watchdog_stats_fresh", 180.0, "youtube_watchdog_stats.json"),
        ("youtube_video_id_resolver", resolver, "resolver_cache_fresh", 300.0, "youtube_video_id_resolver_state.json"),
        ("youtube_api_cost_report", cost, "cost_report_fresh", 1800.0, "reports/youtube_api_cost/open_day_latest.json"),
        ("stream_watchdog", stream_stats or timeline, "stream_watchdog_fresh", 180.0, "stream_watchdog_stats.json"),
    ]
    for spec in [
        ("stream1090_report", stream1090_report, "stream1090_report_fresh", 1800.0, "logs/stream1090_report.jsonl"),
        ("upstream_stream1090_report", upstream_report, "upstream_stream1090_report_fresh", 1800.0, "logs/upstream_stream1090_report.jsonl"),
        ("stream_engine", stream_engine_event, "stream_engine_event_fresh", 180.0, "logs/stream_engine_events.jsonl"),
        ("auto_dj", play_history, "play_history_fresh", 900.0, "logs/play_history.jsonl"),
    ]:
        observed_ts = _observed_ts(spec[1]) if spec[1] else ""
        if spec[0] == "stream_engine" and observed_ts:
            ts = parse_utc(observed_ts)
            age = age_seconds(ts, now) if ts else None
            if age is not None and age <= spec[3]:
                specs.append(spec)
            continue
        if spec[1] and observed_ts:
            specs.append(spec)
    return collect_signals_from_specs(specs=specs, ytw=ytw, cost=cost, now=now)


def collect_signals_from_specs(*, specs: list[tuple[str, dict[str, Any], str, float, str]], ytw: dict[str, Any], cost: dict[str, Any], now: datetime) -> MonitoringSignals:
    sources: list[SourceFreshness] = []
    for source, payload, name, ttl, file_name in specs:
        observed_ts = _observed_ts(payload) if payload else ""
        ts = parse_utc(observed_ts) if observed_ts else None
        age = age_seconds(ts, now) if ts else None
        sources.append(SourceFreshness(source, name, file_name, payload, ttl, observed_ts, age, bool(payload) and age is not None and age <= ttl))
    quota_guard_active = truthy(ytw.get("api_cost_burn_rate_active")) or truthy(cost.get("quota_guard_active"))
    cost_report_degraded = text(cost.get("status")).lower() == "degraded"
    return MonitoringSignals(sources=sources, quota_guard_active=quota_guard_active, cost_report_degraded=cost_report_degraded)


def _observed_ts(payload: dict[str, Any]) -> str:
    if not payload:
        return ""
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    ingest = payload.get("ingest") if isinstance(payload.get("ingest"), dict) else {}
    return text(
        payload.get("ts_utc")
        or payload.get("ts_jst")
        or payload.get("updated_at_utc")
        or payload.get("selected_at_utc")
        or payload.get("started_at_utc")
        or window.get("effective_end_utc")
        or ingest.get("last_in_window_utc")
        or ingest.get("last_seen_utc")
    )
