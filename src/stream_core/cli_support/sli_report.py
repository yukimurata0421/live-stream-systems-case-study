from __future__ import annotations

import bisect
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from stream_core.common.json_io import iter_jsonl, iter_jsonl_recent
from stream_core.common.timeutil import jst_text_or_unknown, utc_text_from_ts
from stream_core.common.youtube_input_quality import (
    DEFAULT_INPUT_QUALITY_MAX_AGE_SEC,
    classify_input_quality_sample,
    normalize_health_issue_details,
)


DEFAULT_WINDOWS = "24h,7d,28d,30d"
DEFAULT_PROMETHEUS_URL = "http://127.0.0.1:9090"
DEFAULT_STEP_SEC = 60
MAX_QUERY_CHUNK_SEC = 7 * 24 * 3600
RAW_EVENT_CORRELATION_SEC = 6 * 60
INPUT_QUALITY_MAX_SAMPLE_SPAN_SEC = DEFAULT_INPUT_QUALITY_MAX_AGE_SEC
DEFAULT_MINIMUM_COVERAGE_PCT = 99.0
DEFAULT_SOURCE_FRESHNESS_PCT = 99.0
YOUTUBE_SOURCE_MAX_AGE_SEC = 180
UPLOAD_SOURCE_MAX_AGE_SEC = 180
AUDIO_SOURCE_MAX_AGE_SEC = 180
SOURCE_DISAGREEMENT_TOLERANCE_PCT = 0.25
VIEWER_PROBE_CADENCE_SEC = 300
RENDERING_EVENT_CADENCE_SEC = 60


@dataclass(frozen=True)
class SliReportContext:
    youtube_watchdog_events_file: Path
    prometheus_url: str = DEFAULT_PROMETHEUS_URL
    operational_reliability_db_file: Path | None = None
    viewer_synthetic_events_file: Path | None = None
    subsystems_status_events_file: Path | None = None


@dataclass(frozen=True)
class WindowSpec:
    label: str
    duration_sec: int


POLICIES = {
    "youtube_availability": {
        "indicator": "public live + ingest connected + watchdog healthy",
        "target_pct": 99.0,
        "official_window": "7d",
        "minimum_coverage_pct": DEFAULT_MINIMUM_COVERAGE_PCT,
        "minimum_source_freshness_pct": DEFAULT_SOURCE_FRESHNESS_PCT,
    },
    "same_url_preservation": {
        "indicator": "stream_v3_same_url_live == 1 + actual URL unchanged + zero uncontrolled replacement",
        "target_pct": 99.9,
        "official_window": "30d",
        "minimum_coverage_pct": DEFAULT_MINIMUM_COVERAGE_PCT,
        "minimum_source_freshness_pct": DEFAULT_SOURCE_FRESHNESS_PCT,
    },
    "upload_ceiling": {
        "indicator": 'stream_v3_upload_p95_mbps{window_hours="1"} <= 5.0',
        "target_pct": 99.0,
        "official_window": "24h",
        "minimum_coverage_pct": DEFAULT_MINIMUM_COVERAGE_PCT,
        "minimum_source_freshness_pct": DEFAULT_SOURCE_FRESHNESS_PCT,
    },
    "youtube_input_quality": {
        "indicator": (
            "fresh OAuth probe + streamStatus=active + ingest connected; "
            "good iff healthStatus=good and no warning/error configuration issue"
        ),
        "target_pct": 99.0,
        "official_window": "7d",
        "minimum_coverage_pct": DEFAULT_MINIMUM_COVERAGE_PCT,
        "minimum_source_freshness_pct": DEFAULT_SOURCE_FRESHNESS_PCT,
    },
    "visual_correctness": {
        "indicator": "sampled capture checklist",
        "target_pct": 99.0,
        "official_window": "sampled_checks",
        "minimum_coverage_pct": 100.0,
    },
    "audio_correctness": {
        "indicator": "stream_v3_audio_ok == 1",
        "target_pct": 99.5,
        "official_window": "7d",
        "minimum_coverage_pct": DEFAULT_MINIMUM_COVERAGE_PCT,
        "minimum_source_freshness_pct": DEFAULT_SOURCE_FRESHNESS_PCT,
    },
}


METRIC_QUERIES = {
    "youtube_public_ok": "stream_v3_youtube_public_ok",
    "youtube_ingest_connected": "stream_v3_youtube_ingest_connected",
    "youtube_watchdog_healthy": "stream_v3_youtube_watchdog_healthy",
    "youtube_stats_age_seconds": "stream_v3_youtube_stats_age_seconds",
    "same_url_live": "stream_v3_same_url_live",
    "upload_p95_1h": 'stream_v3_upload_p95_mbps{window_hours="1"}',
    "upload_latest_age_seconds": "stream_v3_upload_latest_age_seconds",
    "watchdog_warn_count_1h": 'stream_v3_youtube_warn_count{window_hours="1"}',
    "youtube_input_quality_eligible": "stream_v3_youtube_input_quality_eligible",
    "youtube_input_quality_good": "stream_v3_youtube_input_quality_good",
    "youtube_input_quality_probe_fresh": "stream_v3_youtube_input_quality_probe_fresh",
    "audio_ok": "stream_v3_audio_ok",
    "audio_evidence_available": "stream_v3_audio_evidence_available",
    "audio_evidence_age_seconds": "stream_v3_audio_evidence_age_seconds",
}


class SliReportError(RuntimeError):
    pass


def parse_windows(raw: str) -> list[WindowSpec]:
    text = (raw or DEFAULT_WINDOWS).strip()
    specs: list[WindowSpec] = []
    seen: set[str] = set()
    for token in text.split(","):
        value = token.strip().lower()
        if not value:
            continue
        if value.endswith("m") and value[:-1].isdigit():
            minutes = int(value[:-1])
            label = f"{minutes}m"
            seconds = minutes * 60
        elif value.endswith("h") and value[:-1].isdigit():
            hours = int(value[:-1])
            label = f"{hours}h"
            seconds = hours * 3600
        elif value.endswith("d") and value[:-1].isdigit():
            days = int(value[:-1])
            label = f"{days}d"
            seconds = days * 86400
        else:
            raise ValueError(f"invalid SLI window: {token!r}; use values such as 15m,24h,7d,28d,30d")
        if seconds <= 0:
            raise ValueError(f"invalid SLI window: {token!r}")
        if label not in seen:
            specs.append(WindowSpec(label=label, duration_sec=seconds))
            seen.add(label)
    if not specs:
        raise ValueError("at least one SLI window is required")
    return specs


def parse_end_time(raw: str, *, now: Callable[[], float] = time.time, step_sec: int = DEFAULT_STEP_SEC) -> int:
    text = (raw or "").strip()
    if not text:
        value = int(now())
    else:
        try:
            value = int(float(text))
        except ValueError:
            try:
                value = int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
            except ValueError as exc:
                raise ValueError(f"invalid --end-time: {raw!r}; use epoch seconds or ISO-8601") from exc
    return value // step_sec * step_sec


def _safe_endpoint(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return urllib.parse.urlunsplit((parsed.scheme, host, parsed.path.rstrip("/"), "", ""))


def prometheus_query_range(
    base_url: str,
    query: str,
    start_ts: int,
    end_ts: int,
    step_sec: int,
    timeout_sec: float,
) -> list[dict]:
    params = urllib.parse.urlencode(
        {"query": query, "start": start_ts, "end": end_ts, "step": step_sec}
    )
    url = f"{base_url.rstrip('/')}/api/v1/query_range?{params}"
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as response:
            payload = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise SliReportError(f"Prometheus query failed for {query}: {exc}") from exc
    if payload.get("status") != "success":
        raise SliReportError(f"Prometheus query failed for {query}: {payload.get('error', 'unknown error')}")
    result = payload.get("data", {}).get("result", [])
    if not isinstance(result, list):
        raise SliReportError(f"Prometheus returned an invalid result for {query}")
    return [item for item in result if isinstance(item, dict)]


def fetch_metric_points(
    base_url: str,
    query: str,
    start_ts: int,
    end_ts: int,
    *,
    step_sec: int = DEFAULT_STEP_SEC,
    timeout_sec: float = 15.0,
    query_range: Callable[[str, str, int, int, int, float], list[dict]] = prometheus_query_range,
) -> dict[int, float]:
    points: dict[int, float] = {}
    cursor = start_ts
    while cursor < end_ts:
        stop = min(cursor + MAX_QUERY_CHUNK_SEC, end_ts)
        series = query_range(base_url, query, cursor, stop, step_sec, timeout_sec)
        if len(series) > 1:
            labels = [item.get("metric", {}) for item in series]
            raise SliReportError(f"expected one series for {query}, got {len(series)}: {labels}")
        if series:
            values = series[0].get("values", [])
            if isinstance(values, list):
                for row in values:
                    if not isinstance(row, list) or len(row) != 2:
                        continue
                    try:
                        ts = int(float(row[0]))
                        value = float(row[1])
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(value):
                        points[ts] = value
        cursor = stop
    return points


def _round(value: float | None, digits: int = 3) -> float | None:
    return round(value, digits) if value is not None and math.isfinite(value) else None


def _bad_run_stats(status: dict[int, bool], step_sec: int) -> tuple[int, float]:
    bad = sorted(ts for ts, healthy in status.items() if not healthy)
    if not bad:
        return 0, 0.0
    runs: list[int] = []
    start = previous = bad[0]
    for ts in bad[1:]:
        if ts - previous > step_sec:
            runs.append((previous - start) // step_sec + 1)
            start = ts
        previous = ts
    runs.append((previous - start) // step_sec + 1)
    return len(runs), max(runs) * step_sec / 60.0


def summarize_status(
    status: dict[int, bool],
    *,
    start_ts: int,
    end_ts: int,
    step_sec: int = DEFAULT_STEP_SEC,
) -> dict:
    selected = {ts: healthy for ts, healthy in status.items() if start_ts <= ts <= end_ts}
    expected_points = (end_ts - start_ts) // step_sec + 1
    observed_points = len(selected)
    good_points = sum(1 for healthy in selected.values() if healthy)
    bad_points = observed_points - good_points
    ratio = good_points / observed_points * 100.0 if observed_points else None
    conservative = good_points / expected_points * 100.0 if expected_points else None
    run_count, max_run_min = _bad_run_stats(selected, step_sec)
    return {
        "start_utc": utc_text_from_ts(start_ts),
        "end_utc": utc_text_from_ts(end_ts),
        "step_sec": step_sec,
        "expected_points": expected_points,
        "observed_points": observed_points,
        "missing_points": max(0, expected_points - observed_points),
        "coverage_pct": _round(observed_points / expected_points * 100.0 if expected_points else None),
        "good_points": good_points,
        "bad_points": bad_points,
        "bad_minutes": _round(bad_points * step_sec / 60.0),
        "sli_pct": _round(ratio),
        "conservative_sli_pct": _round(conservative),
        "bad_run_count": run_count,
        "max_bad_run_minutes": _round(max_run_min),
    }


def summarize_metric(
    points: dict[int, float],
    healthy: Callable[[float], bool],
    *,
    start_ts: int,
    end_ts: int,
    step_sec: int = DEFAULT_STEP_SEC,
) -> dict:
    status = {ts: bool(healthy(value)) for ts, value in points.items()}
    summary = summarize_status(status, start_ts=start_ts, end_ts=end_ts, step_sec=step_sec)
    values = [value for ts, value in sorted(points.items()) if start_ts <= ts <= end_ts]
    summary["latest_value"] = _round(values[-1], 6) if values else None
    summary["max_value"] = _round(max(values), 6) if values else None
    return summary


def composite_status(
    series: Iterable[dict[int, float]],
    healthy: Callable[[list[float]], bool],
) -> dict[int, bool]:
    items = list(series)
    if not items:
        return {}
    common = set(items[0])
    for points in items[1:]:
        common.intersection_update(points)
    return {ts: bool(healthy([points[ts] for points in items])) for ts in sorted(common)}


def _policy_scope(window: WindowSpec, official_window: str) -> str:
    if official_window == "sampled_checks":
        return "supporting_trend"
    if window.label == official_window:
        return "formal_assessment"
    official = parse_windows(official_window)[0].duration_sec
    return "fast_feedback" if window.duration_sec < official else "trend"


def with_policy(
    summary: dict,
    policy: dict,
    window: WindowSpec,
    *,
    source_freshness_pct: float | None = None,
    source_disagreement: bool = False,
    additional_unknown_reasons: Iterable[str] = (),
    hard_breach_reasons: Iterable[str] = (),
) -> dict:
    result = dict(summary)
    target = float(policy["target_pct"])
    budget_minutes = window.duration_sec / 60.0 * (100.0 - target) / 100.0
    bad_minutes = float(summary.get("bad_minutes") or 0.0)
    observed = int(summary.get("observed_points") or 0)
    coverage = summary.get("coverage_pct")
    minimum_coverage = float(policy.get("minimum_coverage_pct", DEFAULT_MINIMUM_COVERAGE_PCT))
    minimum_freshness = float(policy.get("minimum_source_freshness_pct", DEFAULT_SOURCE_FRESHNESS_PCT))
    unknown_reasons = [str(reason) for reason in additional_unknown_reasons if str(reason)]
    if observed <= 0:
        unknown_reasons.append("no_measurement_evidence")
    if coverage is None or float(coverage) < minimum_coverage:
        unknown_reasons.append("minimum_coverage_not_met")
    if source_freshness_pct is not None and float(source_freshness_pct) < minimum_freshness:
        unknown_reasons.append("source_freshness_not_met")
    if source_disagreement:
        unknown_reasons.append("source_disagreement")
    breach_reasons = sorted({str(reason) for reason in hard_breach_reasons if str(reason)})
    official = window.label == policy["official_window"]
    target_met = bool(float(summary["sli_pct"]) >= target) if observed and summary.get("sli_pct") is not None else None
    if not official:
        compliance_status = "unknown"
    elif breach_reasons:
        compliance_status = "breached"
    elif unknown_reasons:
        compliance_status = "unknown"
    else:
        compliance_status = "met" if target_met else "breached"
    result.update(
        {
            "assessment_scope": _policy_scope(window, str(policy["official_window"])),
            "target_pct": target,
            "nominal_budget_minutes": _round(budget_minutes),
            "budget_consumed_pct": _round(bad_minutes / budget_minutes * 100.0 if budget_minutes else None),
            "budget_remaining_minutes": _round(budget_minutes - bad_minutes),
            "budget_exceeded_by_minutes": _round(max(0.0, bad_minutes - budget_minutes)),
            "target_met_on_observed_samples": target_met,
            "is_official_window": official,
            "compliance_status": compliance_status,
            "measurement_status": "unknown" if unknown_reasons else "valid",
            "measurement_unknown_reasons": sorted(set(unknown_reasons)),
            "minimum_coverage_pct": minimum_coverage,
            "source_freshness_pct": _round(source_freshness_pct),
            "minimum_source_freshness_pct": minimum_freshness,
            "source_disagreement": bool(source_disagreement),
            "hard_breach_reasons": breach_reasons,
        }
    )
    return result


def freshness_pct(
    ages: dict[int, float],
    *,
    start_ts: int,
    end_ts: int,
    max_age_sec: float,
) -> float | None:
    selected = [value for ts, value in ages.items() if start_ts <= ts <= end_ts]
    if not selected:
        return None
    return sum(value <= max_age_sec for value in selected) / len(selected) * 100.0


def _parse_event_ts(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def load_timestamped_events(
    path: Path | None,
    *,
    timestamp_field: str,
    start_ts: int,
    end_ts: int,
    project: Callable[[dict], dict] | None = None,
) -> list[tuple[int, dict]]:
    if path is None:
        return []
    deduplicated: dict[int, dict] = {}
    for payload in iter_jsonl_recent(
        path,
        cutoff_ts=start_ts,
        timestamp=lambda row: _parse_event_ts(row.get(timestamp_field)),
        project=project,
    ):
        ts = _parse_event_ts(payload.get(timestamp_field))
        if start_ts <= ts <= end_ts:
            # ``iter_jsonl_recent`` is newest-first, so preserve the first row
            # when a copytruncate boundary contains the same event twice.
            deduplicated.setdefault(ts, payload)
    return sorted(deduplicated.items())


def _viewer_event_projection(payload: dict) -> dict:
    return {
        "checked_at_utc": payload.get("checked_at_utc"),
        "frame_ok": payload.get("frame_ok"),
        "black_detected": payload.get("black_detected"),
        "freeze_detected": payload.get("freeze_detected"),
        "reason": payload.get("reason"),
    }


def _rendering_event_projection(payload: dict) -> dict:
    rendering = payload.get("rendering")
    retained_rendering = {}
    if isinstance(rendering, dict):
        retained_rendering = {
            key: rendering.get(key)
            for key in (
                "state",
                "aircraft_json_ok",
                "aircraft_messages_moving",
                "aircraft_positions_moving",
                "stream1090_report_ok",
                "upstream_stream1090_report_ok",
                "adsb_freshness_ok",
            )
            if key in rendering
        }
    return {
        "ts_utc": payload.get("ts_utc"),
        "rendering": retained_rendering,
    }


def viewer_probe_measurements(
    events: list[tuple[int, dict]],
    *,
    start_ts: int,
    end_ts: int,
    cadence_sec: int = VIEWER_PROBE_CADENCE_SEC,
) -> dict:
    selected = [(ts, payload) for ts, payload in events if start_ts <= ts <= end_ts]
    expected_points = (end_ts - start_ts) // cadence_sec + 1
    eligible = [(ts, payload) for ts, payload in selected if payload.get("frame_ok") is True]
    status = {
        ts: not (
            payload.get("black_detected") is True
            or payload.get("freeze_detected") is True
        )
        for ts, payload in eligible
    }
    good_points = sum(status.values())
    bad_points = len(status) - good_points
    unknown = [(ts, payload) for ts, payload in selected if payload.get("frame_ok") is not True]
    reason_counts: dict[str, int] = {}
    for _ts, payload in unknown:
        reason = str(payload.get("reason") or "probe_result_unavailable")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    run_count, max_run_minutes = _bad_run_stats(status, cadence_sec)
    return {
        "start_utc": utc_text_from_ts(start_ts),
        "end_utc": utc_text_from_ts(end_ts),
        "evidence_unit": "viewer_probe_events",
        "probe_cadence_sec": cadence_sec,
        "expected_points": expected_points,
        "total_probe_events": len(selected),
        "observed_points": len(eligible),
        "good_points": good_points,
        "bad_points": bad_points,
        "unknown_points": len(unknown),
        "probe_failure_points": len(unknown),
        "missing_points": max(0, expected_points - len(selected)),
        "event_coverage_pct": _round(
            len(selected) / expected_points * 100.0 if expected_points else None
        ),
        "coverage_pct": _round(
            len(eligible) / expected_points * 100.0 if expected_points else None
        ),
        "sli_pct": _round(
            good_points / len(eligible) * 100.0 if eligible else None
        ),
        "bad_run_count": run_count,
        "max_bad_run_duration_minutes": _round(max_run_minutes),
        "unknown_reason_counts": reason_counts,
        "first_event_utc": utc_text_from_ts(selected[0][0]) if selected else "",
        "last_event_utc": utc_text_from_ts(selected[-1][0]) if selected else "",
        "probe_failures_are_unknown": True,
    }


def rendering_event_measurements(
    events: list[tuple[int, dict]],
    *,
    start_ts: int,
    end_ts: int,
    cadence_sec: int = RENDERING_EVENT_CADENCE_SEC,
) -> dict:
    selected = [(ts, payload) for ts, payload in events if start_ts <= ts <= end_ts]
    expected_points = (end_ts - start_ts) // cadence_sec + 1
    status: dict[int, bool] = {}
    unknown_points = 0
    required = (
        "aircraft_json_ok",
        "aircraft_messages_moving",
        "aircraft_positions_moving",
        "stream1090_report_ok",
        "upstream_stream1090_report_ok",
        "adsb_freshness_ok",
    )
    for ts, payload in selected:
        rendering = payload.get("rendering")
        if not isinstance(rendering, dict) or any(key not in rendering for key in required):
            unknown_points += 1
            continue
        motion_ok = (
            rendering.get("aircraft_messages_moving") is True
            or rendering.get("aircraft_positions_moving") is True
        )
        status[ts] = bool(
            rendering.get("state") == "healthy"
            and rendering.get("aircraft_json_ok") is True
            and motion_ok
            and rendering.get("stream1090_report_ok") is True
            and rendering.get("upstream_stream1090_report_ok") is True
            and rendering.get("adsb_freshness_ok") is True
        )
    good_points = sum(status.values())
    bad_points = len(status) - good_points
    run_count, max_run_minutes = _bad_run_stats(status, cadence_sec)
    return {
        "start_utc": utc_text_from_ts(start_ts),
        "end_utc": utc_text_from_ts(end_ts),
        "evidence_unit": "rendering_monitor_events",
        "expected_points": expected_points,
        "total_monitor_events": len(selected),
        "observed_points": len(status),
        "good_points": good_points,
        "bad_points": bad_points,
        "unknown_points": unknown_points,
        "missing_points": max(0, expected_points - len(selected)),
        "event_coverage_pct": _round(
            len(selected) / expected_points * 100.0 if expected_points else None
        ),
        "coverage_pct": _round(
            len(status) / expected_points * 100.0 if expected_points else None
        ),
        "sli_pct": _round(good_points / len(status) * 100.0 if status else None),
        "bad_run_count": run_count,
        "max_bad_run_minutes": _round(max_run_minutes),
        "motion_contract": "messages_moving_or_positions_moving",
    }


def load_watchdog_events(path: Path, *, start_ts: int, end_ts: int) -> list[tuple[int, dict]]:
    deduplicated: dict[object, tuple[int, dict]] = {}
    for payload in iter_jsonl(path):
        ts = _parse_event_ts(payload.get("ts_utc"))
        if ts < start_ts or ts > end_ts:
            continue
        key: object = payload.get("event_id") or (
            ts,
            payload.get("status"),
            payload.get("live_url"),
            payload.get("ingest_connected"),
        )
        deduplicated[key] = (ts, payload)
    return sorted(deduplicated.values(), key=lambda item: item[0])


def watchdog_evidence(events: list[tuple[int, dict]], *, start_ts: int, end_ts: int) -> dict:
    selected = [(ts, payload) for ts, payload in events if start_ts <= ts <= end_ts]
    urls = [str(payload.get("live_url") or "") for _, payload in selected if payload.get("live_url")]
    video_ids = [str(payload.get("video_id") or "") for _, payload in selected if payload.get("video_id")]
    expected_ids = [
        str(payload.get("expected_video_id") or "")
        for _, payload in selected
        if payload.get("expected_video_id")
    ]
    transitions = sum(1 for left, right in zip(urls, urls[1:]) if left != right)
    warning_rows = [payload for _, payload in selected if payload.get("status") == "warn"]
    return {
        "row_count": len(selected),
        "first_utc": utc_text_from_ts(selected[0][0]) if selected else "",
        "last_utc": utc_text_from_ts(selected[-1][0]) if selected else "",
        "warning_row_count": len(warning_rows),
        "warning_ingest_false_count": sum(payload.get("ingest_connected") is False for payload in warning_rows),
        "warning_public_false_count": sum(payload.get("public_ok") is False for payload in warning_rows),
        "warning_local_pipeline_count": sum(payload.get("failure_kind") == "local_pipeline" for payload in warning_rows),
        "rows_with_live_url": len(urls),
        "distinct_live_url_count": len(set(urls)),
        "distinct_video_id_count": len(set(video_ids)),
        "distinct_expected_video_id_count": len(set(expected_ids)),
        "live_url_transition_count": transitions,
        "candidate_new_url_count": sum(bool(payload.get("candidate_new_url_found")) for _, payload in selected),
        "force_live_trigger_count": sum(bool(payload.get("force_live_triggered")) for _, payload in selected),
        "current_video_id_matches_expected": (
            video_ids[-1] == expected_ids[-1] if video_ids and expected_ids else None
        ),
        "actual_url_stable": bool(urls) and len(set(urls)) == 1 and transitions == 0,
    }


def _bad_duration_run_stats(segments: list[tuple[int, int]]) -> tuple[int, float]:
    if not segments:
        return 0, 0.0
    merged: list[tuple[int, int]] = []
    for start, end in sorted(segments):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return len(merged), max(end - start for start, end in merged) / 60.0


def youtube_input_quality_measurements(
    events: list[tuple[int, dict]],
    *,
    start_ts: int,
    end_ts: int,
    max_sample_span_sec: int = INPUT_QUALITY_MAX_SAMPLE_SPAN_SEC,
) -> dict:
    """Build the formal input-quality SLI from time-weighted raw OAuth evidence.

    A row may carry state only until the next watchdog row and never beyond the
    OAuth probe freshness deadline. Local ingest disconnections are excluded from
    this SLI and remain availability evidence. Gaps are reported as missing rather
    than silently counted as good.
    """
    max_span = max(1, int(max_sample_span_sec))
    candidates = [
        (ts, payload)
        for ts, payload in events
        if ts <= end_ts
        and ts + max_span >= start_ts
        and isinstance(payload, dict)
        and (
            "oauth_probe_ok" in payload
            or "oauth_stream_health_status" in payload
            or "oauth_stream_status" in payload
        )
    ]
    candidates.sort(key=lambda item: item[0])

    total_seconds = max(0, end_ts - start_ts)
    covered_seconds = 0
    eligible_seconds = 0
    good_seconds = 0
    bad_seconds = 0
    excluded_seconds = 0
    bad_segments: list[tuple[int, int]] = []
    classification_seconds: dict[str, int] = {}
    issue_type_rows: dict[str, int] = {}
    issue_severity_rows: dict[str, int] = {}
    selected_row_count = 0
    eligible_row_count = 0
    good_row_count = 0
    bad_row_count = 0
    issue_detail_missing_rows = 0

    for index, (event_ts, payload) in enumerate(candidates):
        next_ts = candidates[index + 1][0] if index + 1 < len(candidates) else end_ts
        classification = classify_input_quality_sample(
            payload,
            now_ts=event_ts,
            max_age_sec=max_span,
        )
        checked_ts = int(classification.get("checked_ts") or 0)
        segment_start = max(start_ts, event_ts)
        freshness_end = checked_ts + max_span if checked_ts > 0 else event_ts
        segment_end = min(end_ts, next_ts, event_ts + max_span, freshness_end)
        if segment_end <= segment_start:
            continue

        duration = segment_end - segment_start
        covered_seconds += duration
        selected_row_count += 1
        label = str(classification.get("classification") or "unknown")
        classification_seconds[label] = classification_seconds.get(label, 0) + duration
        if classification.get("eligible"):
            eligible_seconds += duration
            eligible_row_count += 1
            if classification.get("good"):
                good_seconds += duration
                good_row_count += 1
            else:
                bad_seconds += duration
                bad_row_count += 1
                bad_segments.append((segment_start, segment_end))
        else:
            excluded_seconds += duration

        details = normalize_health_issue_details(payload.get("oauth_stream_health_issue_details"))
        if int(classification.get("issue_count") or 0) > 0 and not details:
            issue_detail_missing_rows += 1
        for issue in details:
            issue_type = issue.get("type") or "unknown"
            severity = issue.get("severity") or "unknown"
            issue_type_rows[issue_type] = issue_type_rows.get(issue_type, 0) + 1
            issue_severity_rows[severity] = issue_severity_rows.get(severity, 0) + 1

    missing_seconds = max(0, total_seconds - covered_seconds)
    sli_pct = good_seconds / eligible_seconds * 100.0 if eligible_seconds else None
    conservative_pct = good_seconds / total_seconds * 100.0 if total_seconds else None
    bad_run_count, max_bad_run_minutes = _bad_duration_run_stats(bad_segments)

    def minute_points(seconds: int) -> int:
        return int(round(seconds / 60.0))

    return {
        "start_utc": utc_text_from_ts(start_ts),
        "end_utc": utc_text_from_ts(end_ts),
        "step_sec": 60,
        "evidence_unit": "time_weighted_minutes",
        "measurement_source": "youtube_watchdog_raw_oauth_probe",
        "max_sample_span_sec": max_span,
        "expected_points": minute_points(total_seconds),
        "observed_points": minute_points(covered_seconds),
        "missing_points": minute_points(missing_seconds),
        "coverage_pct": _round(covered_seconds / total_seconds * 100.0 if total_seconds else None),
        "measurement_coverage_pct": _round(covered_seconds / total_seconds * 100.0 if total_seconds else None),
        "eligibility_pct": _round(eligible_seconds / total_seconds * 100.0 if total_seconds else None),
        "good_points": minute_points(good_seconds),
        "bad_points": minute_points(bad_seconds),
        "bad_minutes": _round(bad_seconds / 60.0),
        "sli_pct": _round(sli_pct),
        "conservative_sli_pct": _round(conservative_pct),
        "bad_run_count": bad_run_count,
        "max_bad_run_minutes": _round(max_bad_run_minutes),
        "window_seconds": total_seconds,
        "timeline_covered_seconds": covered_seconds,
        "timeline_missing_seconds": missing_seconds,
        "eligible_seconds": eligible_seconds,
        "good_seconds": good_seconds,
        "bad_seconds": bad_seconds,
        "excluded_seconds": excluded_seconds,
        "raw_probe_row_count": selected_row_count,
        "eligible_probe_row_count": eligible_row_count,
        "good_probe_row_count": good_row_count,
        "bad_probe_row_count": bad_row_count,
        "issue_detail_missing_row_count": issue_detail_missing_rows,
        "classification_minutes": {
            key: _round(value / 60.0) for key, value in sorted(classification_seconds.items())
        },
        "issue_type_rows": dict(sorted(issue_type_rows.items())),
        "issue_severity_rows": dict(sorted(issue_severity_rows.items())),
        "local_pipeline_excluded_from_input_quality": True,
        "missing_evidence_is_not_good": True,
    }


def prometheus_input_quality_measurements(
    eligible: dict[int, float],
    good: dict[int, float],
    *,
    start_ts: int,
    end_ts: int,
    step_sec: int,
) -> dict:
    common = sorted(
        ts
        for ts in set(eligible).intersection(good)
        if start_ts <= ts <= end_ts and eligible[ts] >= 1.0
    )
    status = {ts: good[ts] >= 1.0 for ts in common}
    summary = summarize_status(status, start_ts=start_ts, end_ts=end_ts, step_sec=step_sec)
    summary.update(
        {
            "measurement_source": "prometheus_current_oauth_quality_state",
            "local_pipeline_excluded_from_input_quality": True,
            "missing_evidence_is_not_good": True,
        }
    )
    return summary


def availability_measurements(
    public: dict[int, float],
    ingest: dict[int, float],
    watchdog: dict[int, float],
    events: list[tuple[int, dict]],
    *,
    start_ts: int,
    end_ts: int,
    step_sec: int,
) -> dict:
    common = sorted(
        ts
        for ts in set(public).intersection(ingest, watchdog)
        if start_ts <= ts <= end_ts
    )
    raw_status = {
        ts: public[ts] >= 1.0 and ingest[ts] >= 1.0 and watchdog[ts] >= 1.0
        for ts in common
    }
    ingest_watchdog_status = {
        ts: ingest[ts] >= 1.0 and watchdog[ts] >= 1.0
        for ts in common
    }
    public_only = [
        ts
        for ts in common
        if public[ts] < 1.0 and ingest[ts] >= 1.0 and watchdog[ts] >= 1.0
    ]
    event_times = [ts for ts, _ in events]
    corroborated: set[int] = set()
    for candidate in public_only:
        index = bisect.bisect_left(event_times, candidate)
        nearby: list[tuple[int, dict]] = []
        if index < len(events):
            nearby.append(events[index])
        if index > 0:
            nearby.append(events[index - 1])
        if any(
            abs(ts - candidate) <= RAW_EVENT_CORRELATION_SEC
            and payload.get("public_ok") is False
            and payload.get("ingest_connected") is not False
            for ts, payload in nearby
        ):
            corroborated.add(candidate)
    selected_status = dict(ingest_watchdog_status)
    if events:
        for ts in corroborated:
            selected_status[ts] = False
        correlation = "raw_watchdog_correlated"
    else:
        selected_status = raw_status
        correlation = "raw_watchdog_unavailable_raw_composite_retained"
    return {
        "raw_composite": summarize_status(raw_status, start_ts=start_ts, end_ts=end_ts, step_sec=step_sec),
        "ingest_watchdog": summarize_status(
            ingest_watchdog_status, start_ts=start_ts, end_ts=end_ts, step_sec=step_sec
        ),
        "selected": summarize_status(selected_status, start_ts=start_ts, end_ts=end_ts, step_sec=step_sec),
        "public_only_candidate_points": len(public_only),
        "public_only_corroborated_points": len(corroborated),
        "public_only_excluded_points": len(public_only) - len(corroborated) if events else 0,
        "correlation": correlation,
    }


def _window_payload(
    window: WindowSpec,
    *,
    end_ts: int,
    step_sec: int,
    metrics: dict[str, dict[int, float]],
    events: list[tuple[int, dict]],
    viewer_events: list[tuple[int, dict]],
    subsystem_events: list[tuple[int, dict]],
    same_url_ledger: dict | None = None,
) -> dict[str, dict]:
    start_ts = end_ts - window.duration_sec
    raw_evidence = watchdog_evidence(events, start_ts=start_ts, end_ts=end_ts)
    ledger_evidence = dict(same_url_ledger or {})

    availability = availability_measurements(
        metrics.get("youtube_public_ok", {}),
        metrics.get("youtube_ingest_connected", {}),
        metrics.get("youtube_watchdog_healthy", {}),
        events,
        start_ts=start_ts,
        end_ts=end_ts,
        step_sec=step_sec,
    )
    youtube_freshness = freshness_pct(
        metrics.get("youtube_stats_age_seconds", {}),
        start_ts=start_ts,
        end_ts=end_ts,
        max_age_sec=YOUTUBE_SOURCE_MAX_AGE_SEC,
    )
    availability["selected"] = with_policy(
        availability["selected"],
        POLICIES["youtube_availability"],
        window,
        source_freshness_pct=youtube_freshness,
        additional_unknown_reasons=(
            ["youtube_source_freshness_unavailable"] if youtube_freshness is None else []
        ),
        source_disagreement=(
            int(availability.get("public_only_candidate_points") or 0)
            > int(availability.get("public_only_corroborated_points") or 0)
        ),
    )

    same_url_unknown: list[str] = []
    ledger_coverage = ledger_evidence.get("coverage_pct")
    if ledger_evidence.get("available"):
        if float(ledger_coverage or 0.0) < DEFAULT_MINIMUM_COVERAGE_PCT:
            same_url_unknown.append("same_url_ledger_minimum_coverage_not_met")
        if int(ledger_evidence.get("unclassified_transition_count") or 0) > 0:
            same_url_unknown.append("same_url_transition_unclassified")
    elif raw_evidence["rows_with_live_url"] == 0:
        same_url_unknown.append("actual_url_evidence_unavailable")
    elif not raw_evidence["actual_url_stable"]:
        same_url_unknown.append("actual_url_transition_requires_ledger_review")
    same_url_metric_summary = summarize_metric(
        metrics.get("same_url_live", {}),
        lambda value: value >= 1.0,
        start_ts=start_ts,
        end_ts=end_ts,
        step_sec=step_sec,
    )
    same_url_disagreement = bool(
        raw_evidence["actual_url_stable"]
        and int(same_url_metric_summary.get("bad_points") or 0) > 0
    )
    same_url_raw = with_policy(
        same_url_metric_summary,
        POLICIES["same_url_preservation"],
        window,
        source_freshness_pct=youtube_freshness,
        additional_unknown_reasons=(
            (["youtube_source_freshness_unavailable"] if youtube_freshness is None else [])
            + same_url_unknown
        ),
        source_disagreement=same_url_disagreement,
        hard_breach_reasons=(
            ["uncontrolled_url_replacement"]
            if int(ledger_evidence.get("uncontrolled_transition_count") or 0) > 0
            else []
        ),
    )
    if raw_evidence["actual_url_stable"]:
        same_url_status = "within_target_actual_url_stable"
        actual_burn = 0
    elif raw_evidence["rows_with_live_url"] == 0:
        same_url_status = "unknown_no_actual_url_evidence"
        actual_burn = None
    else:
        same_url_status = "requires_review_actual_url_transition"
        actual_burn = None

    upload_freshness = freshness_pct(
        metrics.get("upload_latest_age_seconds", {}),
        start_ts=start_ts,
        end_ts=end_ts,
        max_age_sec=UPLOAD_SOURCE_MAX_AGE_SEC,
    )
    upload = with_policy(
        summarize_metric(
            metrics.get("upload_p95_1h", {}),
            lambda value: value <= 5.0,
            start_ts=start_ts,
            end_ts=end_ts,
            step_sec=step_sec,
        ),
        POLICIES["upload_ceiling"],
        window,
        source_freshness_pct=upload_freshness,
        additional_unknown_reasons=(
            ["upload_source_freshness_unavailable"] if upload_freshness is None else []
        ),
    )
    raw_input_quality = youtube_input_quality_measurements(
        events,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    prometheus_input_quality = prometheus_input_quality_measurements(
        metrics.get("youtube_input_quality_eligible", {}),
        metrics.get("youtube_input_quality_good", {}),
        start_ts=start_ts,
        end_ts=end_ts,
        step_sec=step_sec,
    )
    if int(raw_input_quality.get("eligible_seconds") or 0) > 0:
        selected_input_quality = raw_input_quality
        selected_input_quality_source = "raw_oauth_probe_time_weighted"
    else:
        selected_input_quality = prometheus_input_quality
        selected_input_quality_source = "prometheus_oauth_quality_fallback"
    raw_pct = raw_input_quality.get("sli_pct")
    prom_pct = prometheus_input_quality.get("sli_pct")
    prom_coverage = float(prometheus_input_quality.get("coverage_pct") or 0.0)
    input_source_disagreement = bool(
        raw_pct is not None
        and prom_pct is not None
        and prom_coverage >= DEFAULT_MINIMUM_COVERAGE_PCT
        and abs(float(raw_pct) - float(prom_pct)) > SOURCE_DISAGREEMENT_TOLERANCE_PCT
    )
    input_freshness = float(selected_input_quality.get("coverage_pct") or 0.0)
    input_quality = with_policy(
        selected_input_quality,
        POLICIES["youtube_input_quality"],
        window,
        source_freshness_pct=input_freshness,
        source_disagreement=input_source_disagreement,
    )
    legacy_watchdog_warning = summarize_metric(
        metrics.get("watchdog_warn_count_1h", {}),
        lambda value: value == 0.0,
        start_ts=start_ts,
        end_ts=end_ts,
        step_sec=step_sec,
    )
    audio_freshness = freshness_pct(
        metrics.get("audio_evidence_age_seconds", {}),
        start_ts=start_ts,
        end_ts=end_ts,
        max_age_sec=AUDIO_SOURCE_MAX_AGE_SEC,
    )
    audio_available = metrics.get("audio_evidence_available", {})
    audio_unknown = []
    if audio_freshness is None:
        audio_unknown.append("audio_source_freshness_unavailable")
    if audio_available and not all(
        value >= 1.0 for ts, value in audio_available.items() if start_ts <= ts <= end_ts
    ):
        audio_unknown.append("audio_evidence_unavailable")
    audio = with_policy(
        summarize_metric(
            metrics.get("audio_ok", {}),
            lambda value: value >= 1.0,
            start_ts=start_ts,
            end_ts=end_ts,
            step_sec=step_sec,
        ),
        POLICIES["audio_correctness"],
        window,
        source_freshness_pct=audio_freshness,
        additional_unknown_reasons=audio_unknown,
    )
    viewer_support = viewer_probe_measurements(
        viewer_events, start_ts=start_ts, end_ts=end_ts
    )
    adsb_support = rendering_event_measurements(
        subsystem_events, start_ts=start_ts, end_ts=end_ts
    )
    visual_support = dict(viewer_support)
    visual_support.update(
        {
            "viewer_probe_events": viewer_support,
            "adsb_rendering_events": adsb_support,
            "evidence_units_separated": True,
            "assessment_scope": "supporting_only",
            "is_official_window": False,
            "compliance_status": "unknown",
            "official_assessment": "manual_sampled_capture_check_required",
            "do_not_convert_to_official_burn": True,
        }
    )

    return {
        "youtube_availability": {
            **availability,
            "raw_watchdog_evidence": raw_evidence,
        },
        "same_url_preservation": {
            "raw_metric": same_url_raw,
            "actual_url_evidence": raw_evidence,
            "transition_ledger_evidence": ledger_evidence,
            "policy_status": same_url_status,
            "actual_burn_minutes": actual_burn,
            "raw_zero_is_not_automatic_url_replacement": True,
        },
        "upload_ceiling": upload,
        "youtube_input_quality": {
            **input_quality,
            "selected_measurement_source": selected_input_quality_source,
            "raw_oauth_measurement": raw_input_quality,
            "prometheus_crosscheck": prometheus_input_quality,
            "legacy_watchdog_warning_signal": {
                **legacy_watchdog_warning,
                "metric": 'stream_v3_youtube_warn_count{window_hours="1"}',
                "formal_sli": False,
                "meaning": "watchdog warnings retained in the preceding one-hour window",
            },
            "raw_warning_evidence": {
                key: raw_evidence[key]
                for key in (
                    "warning_row_count",
                    "warning_ingest_false_count",
                    "warning_public_false_count",
                    "warning_local_pipeline_count",
                )
            },
        },
        "visual_correctness": visual_support,
        "audio_correctness": audio,
    }


def build_sli_report(
    ctx: SliReportContext,
    *,
    windows: str = DEFAULT_WINDOWS,
    prometheus_url: str = "",
    end_time: str = "",
    step_sec: int = DEFAULT_STEP_SEC,
    timeout_sec: float = 15.0,
    query_range: Callable[[str, str, int, int, int, float], list[dict]] = prometheus_query_range,
    now: Callable[[], float] = time.time,
) -> dict:
    specs = parse_windows(windows)
    end_ts = parse_end_time(end_time, now=now, step_sec=step_sec)
    earliest_ts = end_ts - max(spec.duration_sec for spec in specs)
    endpoint = (prometheus_url or os.environ.get("STREAM_V3_PROMETHEUS_URL") or ctx.prometheus_url).rstrip("/")
    metrics: dict[str, dict[int, float]] = {}
    errors: dict[str, str] = {}
    for name, query in METRIC_QUERIES.items():
        try:
            metrics[name] = fetch_metric_points(
                endpoint,
                query,
                earliest_ts,
                end_ts,
                step_sec=step_sec,
                timeout_sec=timeout_sec,
                query_range=query_range,
            )
        except SliReportError as exc:
            metrics[name] = {}
            errors[name] = str(exc)
    events = load_watchdog_events(
        ctx.youtube_watchdog_events_file,
        start_ts=earliest_ts - INPUT_QUALITY_MAX_SAMPLE_SPAN_SEC,
        end_ts=end_ts,
    )
    viewer_events = load_timestamped_events(
        ctx.viewer_synthetic_events_file,
        timestamp_field="checked_at_utc",
        start_ts=earliest_ts,
        end_ts=end_ts,
        project=_viewer_event_projection,
    )
    subsystem_events = load_timestamped_events(
        ctx.subsystems_status_events_file,
        timestamp_field="ts_utc",
        start_ts=earliest_ts,
        end_ts=end_ts,
        project=_rendering_event_projection,
    )
    ledger_by_window: dict[str, dict] = {}
    if ctx.operational_reliability_db_file is not None:
        try:
            from stream_core.operational_reliability.evidence_store import same_url_ledger_evidence

            for spec in specs:
                ledger_by_window[spec.label] = same_url_ledger_evidence(
                    ctx.operational_reliability_db_file,
                    start_ts=end_ts - spec.duration_sec,
                    end_ts=end_ts,
                )
        except Exception as exc:
            errors["same_url_transition_ledger"] = str(exc)

    per_window: dict[str, dict] = {}
    for spec in specs:
        per_window[spec.label] = _window_payload(
            spec,
            end_ts=end_ts,
            step_sec=step_sec,
            metrics=metrics,
            events=events,
            viewer_events=viewer_events,
            subsystem_events=subsystem_events,
            same_url_ledger=ledger_by_window.get(spec.label),
        )
    return {
        "schema_version": 4,
        "source": "stream-prod sli-report",
        "generated_at_utc": utc_text_from_ts(end_ts),
        "generated_at_jst": jst_text_or_unknown(end_ts),
        "prometheus_endpoint": _safe_endpoint(endpoint),
        "step_sec": step_sec,
        "requested_windows": [spec.label for spec in specs],
        "policy_source": "docs/v3/25_decisions/2026-05-30_01_slo_error_budget_policy.md",
        "policy_note": (
            "Only each SLI's defined official_window receives a met/breached decision. "
            "Other windows and measurements below freshness/coverage gates are unknown."
        ),
        "policies": POLICIES,
        "metric_queries": METRIC_QUERIES,
        "metric_errors": errors,
        "raw_watchdog_event_count": len(events),
        "windows": per_window,
    }


def render_text(payload: dict) -> str:
    lines = [
        "[sli-report] "
        f"generated_at={payload.get('generated_at_jst')} "
        f"windows={','.join(payload.get('requested_windows', []))} "
        f"metric_errors={len(payload.get('metric_errors', {}))}"
    ]
    for label, window in payload.get("windows", {}).items():
        availability = window.get("youtube_availability", {}).get("selected", {})
        same_url = window.get("same_url_preservation", {})
        upload = window.get("upload_ceiling", {})
        input_quality = window.get("youtube_input_quality", {})
        visual = window.get("visual_correctness", {})
        audio = window.get("audio_correctness", {})
        lines.append(
            "[sli-report] "
            f"window={label} "
            f"availability={availability.get('sli_pct')}%/{availability.get('compliance_status')} "
            f"same_url={same_url.get('policy_status')}/{same_url.get('raw_metric', {}).get('compliance_status')} "
            f"upload={upload.get('sli_pct')}%/{upload.get('compliance_status')} "
            f"input_quality={input_quality.get('sli_pct')}%/{input_quality.get('compliance_status')} "
            f"input_source={input_quality.get('selected_measurement_source')} "
            f"viewer_visual_support={visual.get('sli_pct')}% "
            f"viewer_probe_events={visual.get('total_probe_events')} "
            f"viewer_probe_unknown={visual.get('unknown_points')} "
            f"adsb_rendering_support={visual.get('adsb_rendering_events', {}).get('sli_pct')}% "
            f"audio={audio.get('sli_pct')}%/{audio.get('compliance_status')}"
        )
        lines.append(
            "[sli-report] "
            f"window={label} measurement_coverage "
            f"availability={availability.get('coverage_pct')}% "
            f"same_url={same_url.get('raw_metric', {}).get('coverage_pct')}% "
            f"upload={upload.get('coverage_pct')}% "
            f"input_quality={input_quality.get('coverage_pct')}% "
            f"audio={audio.get('coverage_pct')}%"
        )
    lines.append(
        "[sli-report] input quality uses fresh active OAuth health evidence; "
        "the preceding-hour watchdog warn gauge is supporting history only"
    )
    lines.append(
        "[sli-report] visual correctness remains a sampled manual checklist; "
        "viewer probe events and ADS-B rendering events are separate supporting evidence"
    )
    return "\n".join(lines)


def sli_report(
    ctx: SliReportContext,
    *,
    windows: str = DEFAULT_WINDOWS,
    prometheus_url: str = "",
    end_time: str = "",
    timeout_sec: float = 15.0,
    json_output: bool = False,
) -> int:
    try:
        payload = build_sli_report(
            ctx,
            windows=windows,
            prometheus_url=prometheus_url,
            end_time=end_time,
            timeout_sec=timeout_sec,
        )
    except (ValueError, SliReportError) as exc:
        print(f"[sli-report] error: {exc}")
        return 2
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    else:
        print(render_text(payload))
    return 0
