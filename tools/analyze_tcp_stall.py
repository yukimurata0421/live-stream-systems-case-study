#!/usr/bin/env python3
"""Offline statistical analysis for FFmpeg RTMPS TCP delivery observations.

This tool is deliberately disconnected from recovery decisions.  It reads
append-only JSONL evidence, derives sanitized time-series rows, and writes only
offline CSV/JSON/SVG artifacts.  It has no deploy, signal, restart, database,
network, or notification path.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, deque
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PERCENTILES = (0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0)
LOWER_PERCENTILES = (5.0, 2.5, 1.0, 0.5, 0.25, 0.1)
PERSISTENCE_VALUES = (1, 2, 3, 5)
QUEUE_NOTSENT_THRESHOLD = 524_288
QUEUE_UNACKED_THRESHOLD = 256
QUEUE_LASTSND_THRESHOLD_MS = 1_000
MIN_INTERVAL_SEC = 50.0
MAX_INTERVAL_SEC = 90.0
EVENT_LOOKBACK = timedelta(minutes=10)
EVENT_LOOKAHEAD = timedelta(minutes=5)
STRICT_EVENT_LOOKBACK = timedelta(minutes=2)
STRICT_EVENT_LOOKAHEAD = timedelta(minutes=5)
EVENT_CLUSTER_GAP = timedelta(minutes=10)
OTHER_FAILURE_PADDING = timedelta(minutes=2)
JST = ZoneInfo("Asia/Tokyo")


@dataclass(frozen=True)
class RawSample:
    ts: datetime
    pid: int
    bytes_acked: int
    bytes_sent: int
    send_q: int
    notsent: int
    unacked: int
    lastsnd_ms: int
    rto_ms: int
    source_epoch: str


@dataclass
class DerivedSample:
    ts: datetime
    pid: int
    source_epoch: str
    interval_sec: float
    bytes_acked_delta: int
    bytes_per_10s: float
    bytes_per_60s: float
    rate_mbps: float
    sent_rate_mbps: float
    delta_rate_mbps: float | None
    negative_drop_mbps: float | None
    relative_drop: float | None
    rolling_3m_mbps: float
    rolling_5m_mbps: float
    send_q: int
    notsent: int
    unacked: int
    lastsnd_ms: int
    rto_ms: int
    queue_pressure: bool
    label: str = "UNKNOWN"
    baseline_state: str = "BOOTSTRAP"
    baseline_count: int = 0
    baseline_median_mbps: float | None = None
    baseline_p5_mbps: float | None = None
    baseline_p2_5_mbps: float | None = None
    baseline_p1_mbps: float | None = None
    baseline_p0_5_mbps: float | None = None
    rate_ratio_to_baseline: float | None = None
    drop_ratio: float | None = None


@dataclass
class StallEvent:
    event_id: str
    pid: int
    anchor_ts: datetime
    last_evidence_ts: datetime
    request_count: int = 1
    source_files: set[str] = field(default_factory=set)

    @property
    def window_start(self) -> datetime:
        return self.anchor_ts - EVENT_LOOKBACK

    @property
    def window_end(self) -> datetime:
        return self.last_evidence_ts + EVENT_LOOKAHEAD


@dataclass(frozen=True)
class FailureWindow:
    start: datetime
    end: datetime
    reason: str


@dataclass(frozen=True)
class RtmpsInterval:
    ts: datetime
    pid: int
    interval_sec: float
    ack_rate_mbps: float
    retrans_delta: int | None
    rtt_ms: float | None
    send_q: int
    notsent: int
    unacked: int
    lastsnd_ms: int
    queue_pressure: bool
    sample_reason: str


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def iso_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def strict_nonnegative_int(value: Any) -> int | None:
    if type(value) is not int or value < 0:
        return None
    return value


def percentile_sorted(ordered: Sequence[float], percentile_value: float) -> float:
    """Return an empirical Type-7 linearly interpolated percentile."""
    if not ordered:
        raise ValueError("percentile requires at least one value")
    if not 0.0 <= percentile_value <= 100.0:
        raise ValueError("percentile must be between 0 and 100")
    position = (len(ordered) - 1) * percentile_value / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


def percentile(values: Sequence[float], percentile_value: float) -> float:
    return percentile_sorted(sorted(values), percentile_value)


def describe(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    median = percentile_sorted(ordered, 50.0)
    deviations = sorted(abs(value - median) for value in ordered)
    result: dict[str, float | int | None] = {
        "count": len(ordered),
        "mean": statistics.fmean(ordered),
        "std_population": statistics.pstdev(ordered),
        "minimum": ordered[0],
        "maximum": ordered[-1],
        "mad": percentile_sorted(deviations, 50.0),
        "iqr": percentile_sorted(ordered, 75.0) - percentile_sorted(ordered, 25.0),
    }
    for value in PERCENTILES:
        result[percentile_key(value)] = percentile_sorted(ordered, value)
    return result


def percentile_key(value: float) -> str:
    return f"p{str(value).replace('.', '_')}"


def queue_pressure(*, notsent: int, unacked: int, lastsnd_ms: int) -> bool:
    return notsent >= QUEUE_NOTSENT_THRESHOLD or unacked >= QUEUE_UNACKED_THRESHOLD or lastsnd_ms >= QUEUE_LASTSND_THRESHOLD_MS


def canonical_digest(rows: Iterable[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def file_metadata(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes_at_open": stat.st_size,
        "mtime_utc_at_open": iso_z(datetime.fromtimestamp(stat.st_mtime, tz=UTC)),
    }


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                yield row


def read_primary_samples(path: Path, *, cutoff: datetime) -> tuple[list[RawSample], dict[str, Any]]:
    samples: list[RawSample] = []
    raw_tcp_rows = 0
    no_ack_rows = 0
    rejected_rows = 0
    selected_digest_rows: list[dict[str, Any]] = []
    for row in iter_jsonl(path):
        if row.get("kind") != "tcp_send_sample":
            continue
        raw_tcp_rows += 1
        ts = parse_timestamp(row.get("ts_utc"))
        if ts is None or ts > cutoff:
            continue
        acked = strict_nonnegative_int(row.get("bytes_acked"))
        if acked is None:
            no_ack_rows += 1
            continue
        fields = {
            name: strict_nonnegative_int(row.get(name))
            for name in ("ffmpeg_pid", "bytes_sent", "send_q", "notsent", "unacked", "lastsnd_ms", "rto_ms")
        }
        validated_fields = {name: value for name, value in fields.items() if value is not None}
        if len(validated_fields) != len(fields) or validated_fields["ffmpeg_pid"] <= 1:
            rejected_rows += 1
            continue
        source_epoch = "generation_bound_event_time" if row.get("ffmpeg_generation") else "pid_bound_event_time"
        sample = RawSample(
            ts=ts,
            pid=validated_fields["ffmpeg_pid"],
            bytes_acked=acked,
            bytes_sent=validated_fields["bytes_sent"],
            send_q=validated_fields["send_q"],
            notsent=validated_fields["notsent"],
            unacked=validated_fields["unacked"],
            lastsnd_ms=validated_fields["lastsnd_ms"],
            rto_ms=validated_fields["rto_ms"],
            source_epoch=source_epoch,
        )
        samples.append(sample)
        selected_digest_rows.append(
            {
                "ts_utc": iso_z(ts),
                "pid": sample.pid,
                "bytes_acked": sample.bytes_acked,
                "bytes_sent": sample.bytes_sent,
                "send_q": sample.send_q,
                "notsent": sample.notsent,
                "unacked": sample.unacked,
                "lastsnd_ms": sample.lastsnd_ms,
                "rto_ms": sample.rto_ms,
                "source_epoch": source_epoch,
            }
        )
    samples.sort(key=lambda item: item.ts)
    summary = {
        **file_metadata(path),
        "raw_tcp_send_sample_rows": raw_tcp_rows,
        "rows_without_bytes_acked": no_ack_rows,
        "rejected_rows": rejected_rows,
        "selected_ack_rows": len(samples),
        "selected_first_utc": iso_z(samples[0].ts) if samples else None,
        "selected_last_utc": iso_z(samples[-1].ts) if samples else None,
        "selected_sanitized_rows_sha256": canonical_digest(selected_digest_rows),
    }
    return samples, summary


def derive_samples(raw: Sequence[RawSample]) -> tuple[list[DerivedSample], dict[str, int]]:
    derived: list[DerivedSample] = []
    previous: RawSample | None = None
    previous_derived: DerivedSample | None = None
    rate_windows: dict[int, deque[tuple[datetime, float]]] = {}
    discontinuities: Counter[str] = Counter()
    for current in raw:
        if previous is None:
            discontinuities["initial_warmup"] += 1
            previous = current
            continue
        elapsed = (current.ts - previous.ts).total_seconds()
        if current.pid != previous.pid:
            discontinuities["pid_change"] += 1
            rate_windows.pop(current.pid, None)
            previous = current
            previous_derived = None
            continue
        if current.bytes_acked < previous.bytes_acked or current.bytes_sent < previous.bytes_sent:
            discontinuities["counter_regression"] += 1
            rate_windows.pop(current.pid, None)
            previous = current
            previous_derived = None
            continue
        if not MIN_INTERVAL_SEC <= elapsed <= MAX_INTERVAL_SEC:
            discontinuities["interval_out_of_range"] += 1
            rate_windows.pop(current.pid, None)
            previous = current
            previous_derived = None
            continue
        ack_delta = current.bytes_acked - previous.bytes_acked
        sent_delta = current.bytes_sent - previous.bytes_sent
        rate = ack_delta * 8.0 / (elapsed * 1_000_000.0)
        sent_rate = sent_delta * 8.0 / (elapsed * 1_000_000.0)
        delta_rate: float | None = None
        negative_drop: float | None = None
        relative_drop: float | None = None
        if previous_derived is not None and previous_derived.pid == current.pid:
            delta_rate = rate - previous_derived.rate_mbps
            negative_drop = previous_derived.rate_mbps - rate
            if previous_derived.rate_mbps > 0:
                relative_drop = negative_drop / previous_derived.rate_mbps
        window = rate_windows.setdefault(current.pid, deque())
        window.append((current.ts, rate))
        while window and current.ts - window[0][0] > timedelta(minutes=5):
            window.popleft()
        rates_3m = [value for ts, value in window if current.ts - ts <= timedelta(minutes=3)]
        rates_5m = [value for _, value in window]
        pressure = queue_pressure(notsent=current.notsent, unacked=current.unacked, lastsnd_ms=current.lastsnd_ms)
        sample = DerivedSample(
            ts=current.ts,
            pid=current.pid,
            source_epoch=current.source_epoch,
            interval_sec=elapsed,
            bytes_acked_delta=ack_delta,
            bytes_per_10s=ack_delta * 10.0 / elapsed,
            bytes_per_60s=ack_delta * 60.0 / elapsed,
            rate_mbps=rate,
            sent_rate_mbps=sent_rate,
            delta_rate_mbps=delta_rate,
            negative_drop_mbps=negative_drop,
            relative_drop=relative_drop,
            rolling_3m_mbps=statistics.fmean(rates_3m),
            rolling_5m_mbps=statistics.fmean(rates_5m),
            send_q=current.send_q,
            notsent=current.notsent,
            unacked=current.unacked,
            lastsnd_ms=current.lastsnd_ms,
            rto_ms=current.rto_ms,
            queue_pressure=pressure,
        )
        derived.append(sample)
        previous = current
        previous_derived = sample
    return derived, dict(discontinuities)


def read_stall_events(paths: Sequence[Path], *, cutoff: datetime) -> tuple[list[StallEvent], dict[str, Any]]:
    requests: list[tuple[datetime, int, str]] = []
    source_counts: Counter[str] = Counter()
    for path in paths:
        for row in iter_jsonl(path):
            if row.get("kind") != "recovery_requested" or not str(row.get("message") or "").startswith("tcp stall:"):
                continue
            ts = parse_timestamp(row.get("ts_utc"))
            pid = strict_nonnegative_int(row.get("ffmpeg_pid"))
            if ts is None or ts > cutoff or pid is None or pid <= 1:
                continue
            requests.append((ts, pid, str(path)))
            source_counts[str(path)] += 1
    requests.sort()
    events: list[StallEvent] = []
    by_pid: dict[int, StallEvent] = {}
    for ts, pid, source in requests:
        current = by_pid.get(pid)
        if current is not None and ts - current.last_evidence_ts <= EVENT_CLUSTER_GAP:
            if ts > current.last_evidence_ts:
                current.last_evidence_ts = ts
            current.request_count += 1
            current.source_files.add(source)
            continue
        event = StallEvent(
            event_id="pending",
            pid=pid,
            anchor_ts=ts,
            last_evidence_ts=ts,
            source_files={source},
        )
        events.append(event)
        by_pid[pid] = event
    events.sort(key=lambda item: item.anchor_ts)
    for index, event in enumerate(events, start=1):
        event.event_id = f"stall-{event.anchor_ts.strftime('%Y%m%dT%H%M%SZ')}-{index:02d}"
    return events, {
        "input_files": [file_metadata(path) for path in paths],
        "tcp_stall_request_rows": len(requests),
        "clustered_events": len(events),
        "request_rows_by_source": dict(source_counts),
        "cluster_rule": "same ffmpeg_pid and each consecutive request gap <= 600 seconds",
    }


def read_stream_engine(path: Path | None, *, cutoff: datetime) -> tuple[list[FailureWindow], dict[str, Any]]:
    if path is None:
        return [], {"available": False}
    windows: list[FailureWindow] = []
    profiles: Counter[str] = Counter()
    event_types: Counter[str] = Counter()
    profile_first: datetime | None = None
    profile_last: datetime | None = None
    for row in iter_jsonl(path):
        ts = parse_timestamp(row.get("ts_utc"))
        if ts is None or ts > cutoff:
            continue
        event_type = row.get("event_type")
        if isinstance(event_type, str):
            event_types[event_type] += 1
        profile = row.get("encoder_profile")
        if isinstance(profile, dict):
            safe_profile = {
                key: profile.get(key)
                for key in (
                    "video_codec",
                    "video_bitrate",
                    "video_maxrate",
                    "video_bufsize",
                    "audio_codec",
                    "audio_bitrate",
                    "frame_rate",
                    "gop",
                )
                if key in profile
            }
            profiles[json.dumps(safe_profile, sort_keys=True, separators=(",", ":"))] += 1
            profile_first = profile_first or ts
            profile_last = ts
        if event_type in {
            "ffmpeg_exited",
            "ffmpeg_starting",
            "ffmpeg_restart_scheduled",
            "ffmpeg_stop_requested",
            "engine_start",
            "engine_stopping",
        }:
            windows.append(FailureWindow(ts - OTHER_FAILURE_PADDING, ts + OTHER_FAILURE_PADDING, str(event_type)))
    return windows, {
        **file_metadata(path),
        "available": True,
        "event_type_counts": dict(event_types),
        "encoder_profiles": [{"profile": json.loads(profile), "row_count": count} for profile, count in profiles.items()],
        "encoder_profile_first_utc": iso_z(profile_first),
        "encoder_profile_last_utc": iso_z(profile_last),
        "other_failure_windows": len(windows),
    }


def read_ffmpeg_stderr(path: Path | None, *, cutoff: datetime) -> tuple[list[tuple[datetime, int | None]], dict[str, Any]]:
    if path is None:
        return [], {"available": False}
    patterns = ("connection timed out", "connection reset by peer", "broken pipe", "i/o error", "network is unreachable")
    matched: list[tuple[datetime, int | None]] = []
    total = 0
    for row in iter_jsonl(path):
        ts = parse_timestamp(row.get("ts_utc"))
        if ts is None or ts > cutoff:
            continue
        total += 1
        line = str(row.get("line") or "").lower()
        if any(pattern in line for pattern in patterns):
            matched.append((ts, strict_nonnegative_int(row.get("ffmpeg_pid"))))
    return matched, {
        **file_metadata(path),
        "available": True,
        "rows_before_cutoff": total,
        "network_error_rows": len(matched),
        "raw_lines_exported": False,
    }


def _rtmps_identity(sock: dict[str, Any]) -> tuple[int, str, str] | None:
    pid = strict_nonnegative_int(sock.get("pid"))
    if pid is None or pid <= 1:
        return None
    local = sock.get("local")
    peer = sock.get("peer")
    if not isinstance(local, str) or not isinstance(peer, str):
        return None
    return pid, local, peer


def read_rtmps_intervals(path: Path | None, *, cutoff: datetime) -> tuple[list[RtmpsInterval], dict[str, Any]]:
    if path is None:
        return [], {"available": False}
    intervals: list[RtmpsInterval] = []
    previous: dict[tuple[int, str, str], tuple[datetime, int, int | None]] = {}
    rows = socket_rows = rejected = 0
    first: datetime | None = None
    last: datetime | None = None
    reasons: Counter[str] = Counter()
    for row in iter_jsonl(path):
        ts = parse_timestamp(row.get("ts_utc"))
        if ts is None or ts > cutoff:
            continue
        rows += 1
        first = first or ts
        last = ts
        reason = str(row.get("sample_reason") or "")
        reasons[reason] += 1
        sockets = row.get("sockets")
        if not isinstance(sockets, list):
            continue
        for sock in sockets:
            if not isinstance(sock, dict) or sock.get("state") != "ESTAB":
                continue
            socket_rows += 1
            identity = _rtmps_identity(sock)
            metrics = sock.get("metrics")
            if identity is None or not isinstance(metrics, dict):
                rejected += 1
                continue
            acked = strict_nonnegative_int(metrics.get("bytes_acked"))
            retrans_total = strict_nonnegative_int(metrics.get("retrans_total"))
            send_q = strict_nonnegative_int(sock.get("send_q")) or 0
            notsent = strict_nonnegative_int(metrics.get("notsent")) or 0
            unacked = strict_nonnegative_int(metrics.get("unacked")) or 0
            lastsnd = strict_nonnegative_int(metrics.get("lastsnd")) or 0
            if acked is None:
                rejected += 1
                continue
            saved = previous.get(identity)
            previous[identity] = (ts, acked, retrans_total)
            if saved is None:
                continue
            previous_ts, previous_ack, previous_retrans = saved
            elapsed = (ts - previous_ts).total_seconds()
            if not 0.2 <= elapsed <= 30.0 or acked < previous_ack:
                rejected += 1
                continue
            retrans_delta = None
            if retrans_total is not None and previous_retrans is not None and retrans_total >= previous_retrans:
                retrans_delta = retrans_total - previous_retrans
            rtt = metrics.get("rtt_ms")
            rtt_ms = float(rtt) if isinstance(rtt, (int, float)) and not isinstance(rtt, bool) and math.isfinite(float(rtt)) else None
            intervals.append(
                RtmpsInterval(
                    ts=ts,
                    pid=identity[0],
                    interval_sec=elapsed,
                    ack_rate_mbps=(acked - previous_ack) * 8.0 / (elapsed * 1_000_000.0),
                    retrans_delta=retrans_delta,
                    rtt_ms=rtt_ms,
                    send_q=send_q,
                    notsent=notsent,
                    unacked=unacked,
                    lastsnd_ms=lastsnd,
                    queue_pressure=queue_pressure(notsent=notsent, unacked=unacked, lastsnd_ms=lastsnd),
                    sample_reason=reason,
                )
            )
    scheduled = [
        item for item in intervals if item.sample_reason == "jst_0800_0820_burst" and item.ack_rate_mbps > 0 and not item.queue_pressure
    ]
    return intervals, {
        **file_metadata(path),
        "available": True,
        "rows_before_cutoff": rows,
        "socket_rows": socket_rows,
        "derived_intervals": len(intervals),
        "rejected_or_warmup_rows": rejected,
        "first_utc": iso_z(first),
        "last_utc": iso_z(last),
        "sample_reason_counts": dict(reasons),
        "scheduled_0800_0820_normal_candidate_count": len(scheduled),
        "scheduled_0800_0820_ack_rate_distribution_mbps": describe([item.ack_rate_mbps for item in scheduled]),
        "scheduled_window_bias": True,
        "endpoint_fields_exported": False,
    }


def in_window(ts: datetime, start: datetime, end: datetime) -> bool:
    return start <= ts <= end


def label_samples(samples: Sequence[DerivedSample], events: Sequence[StallEvent], failures: Sequence[FailureWindow]) -> Counter[str]:
    labels: Counter[str] = Counter()
    for sample in samples:
        if any(sample.pid == event.pid and in_window(sample.ts, event.window_start, event.window_end) for event in events):
            sample.label = "KNOWN_TCP_STALL"
        elif any(in_window(sample.ts, failure.start, failure.end) for failure in failures):
            sample.label = "OTHER_FAILURE"
        elif sample.rate_mbps > 0 and not sample.queue_pressure:
            sample.label = "NORMAL_CANDIDATE"
        else:
            sample.label = "UNKNOWN"
        labels[sample.label] += 1
    return labels


def add_causal_baseline(samples: Sequence[DerivedSample], *, window_days: int = 7) -> dict[str, Any]:
    horizon = timedelta(days=window_days)
    history: deque[tuple[datetime, float]] = deque()
    ordered: list[float] = []
    ready_count = 0
    first_ready: datetime | None = None
    minimum_samples = int(window_days * 24 * 60 * 0.75)
    minimum_span = horizon - timedelta(seconds=2 * MAX_INTERVAL_SEC)
    for sample in samples:
        while history and sample.ts - history[0][0] > horizon:
            _, old_value = history.popleft()
            index = bisect.bisect_left(ordered, old_value)
            if index < len(ordered) and ordered[index] == old_value:
                ordered.pop(index)
        span = history[-1][0] - history[0][0] if len(history) >= 2 else timedelta(0)
        if not history or sample.ts - history[0][0] < timedelta(days=1):
            state = "BOOTSTRAP"
        elif len(history) < minimum_samples or span < minimum_span:
            state = "WARMING"
        else:
            state = "READY"
            ready_count += 1
            first_ready = first_ready or sample.ts
        sample.baseline_state = state
        sample.baseline_count = len(history)
        if ordered:
            sample.baseline_median_mbps = percentile_sorted(ordered, 50.0)
            sample.baseline_p5_mbps = percentile_sorted(ordered, 5.0)
            sample.baseline_p2_5_mbps = percentile_sorted(ordered, 2.5)
            sample.baseline_p1_mbps = percentile_sorted(ordered, 1.0)
            sample.baseline_p0_5_mbps = percentile_sorted(ordered, 0.5)
            if sample.baseline_median_mbps > 0:
                sample.rate_ratio_to_baseline = sample.rate_mbps / sample.baseline_median_mbps
                sample.drop_ratio = 1.0 - sample.rate_ratio_to_baseline
        if sample.label == "NORMAL_CANDIDATE" and sample.rate_mbps > 0 and not sample.queue_pressure:
            history.append((sample.ts, sample.rate_mbps))
            bisect.insort(ordered, sample.rate_mbps)
    return {
        "window_days": window_days,
        "minimum_samples": minimum_samples,
        "minimum_span_days": minimum_span.total_seconds() / 86_400.0,
        "ready_sample_count": ready_count,
        "first_ready_utc": iso_z(first_ready),
        "training_gate": "NORMAL_CANDIDATE and rate>0 and no queue pressure; append after scoring current sample",
        "self_contamination_controls": [
            "known stall windows excluded",
            "FFmpeg lifecycle failure windows excluded",
            "zero ACK and queue-pressure samples excluded",
            "current sample is scored before it can enter history",
            "profile/identity change requires a separate baseline epoch in production",
        ],
    }


def continuity_break(previous: DerivedSample | None, current: DerivedSample) -> bool:
    return previous is None or previous.pid != current.pid or (current.ts - previous.ts).total_seconds() > MAX_INTERVAL_SEC + 5.0


def condition_runs(
    samples: Sequence[DerivedSample], predicate: Callable[[DerivedSample], bool], *, label: str | None = None
) -> list[list[DerivedSample]]:
    runs: list[list[DerivedSample]] = []
    current_run: list[DerivedSample] = []
    previous: DerivedSample | None = None
    for sample in samples:
        eligible = label is None or sample.label == label
        if (continuity_break(previous, sample) or not eligible or not predicate(sample)) and current_run:
            runs.append(current_run)
            current_run = []
        if eligible and predicate(sample):
            current_run.append(sample)
        previous = sample
    if current_run:
        runs.append(current_run)
    return runs


def event_rule_runs(
    event: StallEvent,
    samples: Sequence[DerivedSample],
    predicate: Callable[[DerivedSample], bool],
    *,
    entry_slope_threshold: float | None = None,
    strict_window: bool = False,
) -> list[list[DerivedSample]]:
    window_start = event.anchor_ts - STRICT_EVENT_LOOKBACK if strict_window else event.window_start
    window_end = event.anchor_ts + STRICT_EVENT_LOOKAHEAD if strict_window else event.window_end
    window_samples = [sample for sample in samples if sample.pid == event.pid and in_window(sample.ts, window_start, window_end)]
    runs = condition_runs(window_samples, predicate)
    if entry_slope_threshold is None:
        return runs
    return [run for run in runs if run and run[0].delta_rate_mbps is not None and run[0].delta_rate_mbps < entry_slope_threshold]


def threshold_evaluation(
    samples: Sequence[DerivedSample],
    events: Sequence[StallEvent],
    normal_rates: Sequence[float],
    normal_deltas: Sequence[float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not normal_rates or not normal_deltas:
        return [], [], {}
    thresholds = {value: percentile(normal_rates, value) for value in LOWER_PERCENTILES}
    delta_p1 = percentile(normal_deltas, 1.0)
    start = min(sample.ts for sample in samples)
    end = max(sample.ts for sample in samples)
    span_days = max((end - start).total_seconds() / 86_400.0, 1e-9)
    normal_intervals = [sample.interval_sec for sample in samples if sample.label == "NORMAL_CANDIDATE"]
    median_interval = percentile(normal_intervals, 50.0)
    lower_tail_rows: list[dict[str, Any]] = []
    evaluation_rows: list[dict[str, Any]] = []
    background_samples = [sample for sample in samples if sample.label in {"NORMAL_CANDIDATE", "UNKNOWN"}]
    for percentile_value, threshold in thresholds.items():

        def rate_predicate(sample: DerivedSample, limit: float = threshold) -> bool:
            return sample.rate_mbps < limit

        def rate_slope_predicate(sample: DerivedSample, limit: float = threshold) -> bool:
            return sample.rate_mbps < limit and sample.delta_rate_mbps is not None and sample.delta_rate_mbps < delta_p1

        def rate_pressure_predicate(sample: DerivedSample, limit: float = threshold) -> bool:
            return sample.rate_mbps < limit and sample.queue_pressure

        normal_runs = condition_runs(samples, rate_predicate, label="NORMAL_CANDIDATE")
        run_lengths = [len(run) for run in normal_runs]
        below_count = sum(run_lengths)
        lower_tail_rows.append(
            {
                "percentile": percentile_value,
                "threshold_mbps": threshold,
                "normal_samples_below_threshold": below_count,
                "normal_minutes_per_day_equivalent": below_count * median_interval / 60.0 / span_days,
                "abnormal_runs": len(normal_runs),
                "false_runs_per_day": len(normal_runs) / span_days,
                "maximum_consecutive_run": max(run_lengths, default=0),
                "median_run_length": percentile(run_lengths, 50.0) if run_lengths else 0,
                "p95_run_length": percentile(run_lengths, 95.0) if run_lengths else 0,
            }
        )
        rules: dict[str, tuple[Callable[[DerivedSample], bool], float | None]] = {
            "rate_only": (rate_predicate, None),
            "rate_plus_slope_same_sample": (rate_slope_predicate, None),
            "rate_plus_entry_slope": (rate_predicate, delta_p1),
            "rate_plus_queue_pressure": (rate_pressure_predicate, None),
        }
        for rule_name, (predicate, entry_slope) in rules.items():
            base_runs = condition_runs(samples, predicate, label="NORMAL_CANDIDATE")
            background_runs = condition_runs(background_samples, predicate)
            if entry_slope is not None:
                base_runs = [
                    run for run in base_runs if run and run[0].delta_rate_mbps is not None and run[0].delta_rate_mbps < entry_slope
                ]
                background_runs = [
                    run for run in background_runs if run and run[0].delta_rate_mbps is not None and run[0].delta_rate_mbps < entry_slope
                ]
            for persistence in PERSISTENCE_VALUES:
                qualifying_normal = [run for run in base_runs if len(run) >= persistence]
                qualifying_background = [run for run in background_runs if len(run) >= persistence]
                normal_candidate_points = sum(len(run) - persistence + 1 for run in qualifying_normal)
                background_candidate_points = sum(len(run) - persistence + 1 for run in qualifying_background)
                captured = 0
                offsets: list[float] = []
                duplicate_runs = 0
                strict_captured = 0
                strict_offsets: list[float] = []
                strict_duplicate_runs = 0
                for event in events:
                    event_runs = event_rule_runs(event, samples, predicate, entry_slope_threshold=entry_slope)
                    qualified = [run for run in event_runs if len(run) >= persistence]
                    if qualified:
                        captured += 1
                        first_candidate = min(run[persistence - 1].ts for run in qualified)
                        offsets.append((first_candidate - event.anchor_ts).total_seconds())
                        duplicate_runs += max(0, len(qualified) - 1)
                    strict_runs = event_rule_runs(
                        event,
                        samples,
                        predicate,
                        entry_slope_threshold=entry_slope,
                        strict_window=True,
                    )
                    strict_qualified = [run for run in strict_runs if len(run) >= persistence]
                    if strict_qualified:
                        strict_captured += 1
                        strict_first = min(run[persistence - 1].ts for run in strict_qualified)
                        strict_offsets.append((strict_first - event.anchor_ts).total_seconds())
                        strict_duplicate_runs += max(0, len(strict_qualified) - 1)
                evaluation_rows.append(
                    {
                        "percentile": percentile_value,
                        "threshold_mbps": threshold,
                        "rule": rule_name,
                        "persistence_n": persistence,
                        "normal_false_candidate_points": normal_candidate_points,
                        "normal_false_candidate_points_per_day": normal_candidate_points / span_days,
                        "normal_false_runs": len(qualifying_normal),
                        "normal_false_runs_per_day": len(qualifying_normal) / span_days,
                        "longest_false_positive_run": max((len(run) for run in qualifying_normal), default=0),
                        "background_candidate_points": background_candidate_points,
                        "background_candidate_points_per_day": background_candidate_points / span_days,
                        "background_candidate_runs": len(qualifying_background),
                        "background_candidate_runs_per_day": len(qualifying_background) / span_days,
                        "longest_background_candidate_run": max((len(run) for run in qualifying_background), default=0),
                        "background_definition": "NORMAL_CANDIDATE plus UNKNOWN; excludes labeled stalls and lifecycle failures",
                        "known_event_count": len(events),
                        "captured_events": captured,
                        "capture_rate": captured / len(events) if events else None,
                        "detection_offset_median_sec": percentile(offsets, 50.0) if offsets else None,
                        "detection_offset_p95_sec": percentile(offsets, 95.0) if offsets else None,
                        "event_duplicate_runs": duplicate_runs,
                        "strict_event_window": "anchor - 120 seconds through anchor + 300 seconds",
                        "strict_captured_events": strict_captured,
                        "strict_capture_rate": strict_captured / len(events) if events else None,
                        "strict_detection_offset_median_sec": percentile(strict_offsets, 50.0) if strict_offsets else None,
                        "strict_detection_offset_p95_sec": percentile(strict_offsets, 95.0) if strict_offsets else None,
                        "strict_event_duplicate_runs": strict_duplicate_runs,
                        "delta_p1_mbps": delta_p1,
                    }
                )
    return lower_tail_rows, evaluation_rows, {"delta_p1_mbps": delta_p1, "thresholds_mbps": thresholds}


def _daily_evaluation_times(samples: Sequence[DerivedSample], window_days: int) -> list[datetime]:
    first = min(sample.ts for sample in samples)
    last = max(sample.ts for sample in samples)
    cursor = datetime.combine((first + timedelta(days=window_days)).date(), datetime.min.time(), tzinfo=UTC)
    result: list[datetime] = []
    while cursor <= last:
        result.append(cursor)
        cursor += timedelta(days=1)
    return result


def baseline_stability(samples: Sequence[DerivedSample]) -> list[dict[str, Any]]:
    normal = [sample for sample in samples if sample.label == "NORMAL_CANDIDATE"]
    rows: list[dict[str, Any]] = []
    for days in (1, 3, 7, 14, 30):
        evaluations: list[dict[str, float]] = []
        for end in _daily_evaluation_times(samples, days):
            start = end - timedelta(days=days)
            values = [sample.rate_mbps for sample in normal if start <= sample.ts < end]
            if len(values) < days * 24 * 60 * 0.5:
                continue
            evaluations.append(
                {
                    "median": percentile(values, 50.0),
                    "p5": percentile(values, 5.0),
                    "p1": percentile(values, 1.0),
                    "p0_5": percentile(values, 0.5),
                }
            )
        row: dict[str, Any] = {"window_days": days, "evaluation_count": len(evaluations)}
        if not evaluations:
            row["status"] = "INSUFFICIENT_DATA"
        else:
            row["status"] = "AVAILABLE"
            for metric in ("median", "p5", "p1", "p0_5"):
                values = [item[metric] for item in evaluations]
                central = percentile(values, 50.0)
                row[f"{metric}_median_mbps"] = central
                row[f"{metric}_minimum_mbps"] = min(values)
                row[f"{metric}_maximum_mbps"] = max(values)
                row[f"{metric}_spread_pct"] = (max(values) - min(values)) / central * 100.0 if central > 0 else None
        rows.append(row)
    return rows


def hour_of_day_analysis(samples: Sequence[DerivedSample]) -> list[dict[str, Any]]:
    buckets: dict[int, list[float]] = {hour: [] for hour in range(24)}
    for sample in samples:
        if sample.label == "NORMAL_CANDIDATE":
            buckets[sample.ts.astimezone(JST).hour].append(sample.rate_mbps)
    rows: list[dict[str, Any]] = []
    for hour, values in buckets.items():
        rows.append(
            {
                "hour_jst": hour,
                "count": len(values),
                "median_mbps": percentile(values, 50.0) if values else None,
                "p5_mbps": percentile(values, 5.0) if values else None,
                "p1_mbps": percentile(values, 1.0) if values else None,
            }
        )
    return rows


def analyze_events(
    events: Sequence[StallEvent],
    samples: Sequence[DerivedSample],
    rtmps: Sequence[RtmpsInterval],
    stderr_errors: Sequence[tuple[datetime, int | None]],
    *,
    p0_1_threshold_mbps: float,
    delta_p1_mbps: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        primary = [sample for sample in samples if sample.pid == event.pid and in_window(sample.ts, event.window_start, event.window_end)]
        high_resolution = [
            sample for sample in rtmps if sample.pid == event.pid and in_window(sample.ts, event.window_start, event.window_end)
        ]
        stderr_count = sum(1 for ts, pid in stderr_errors if pid == event.pid and in_window(ts, event.window_start, event.window_end))
        stalled_high_resolution = [sample for sample in high_resolution if sample.ack_rate_mbps <= 0.01 and sample.queue_pressure]
        evidence_grade = "MULTI_PLANE_TCP" if stalled_high_resolution or stderr_count else "CONTROLLER_TCP_PRESSURE"
        baseline_values = [sample.baseline_median_mbps for sample in primary if sample.baseline_median_mbps is not None]
        strict_primary = [
            sample
            for sample in primary
            if in_window(sample.ts, event.anchor_ts - STRICT_EVENT_LOOKBACK, event.anchor_ts + STRICT_EVENT_LOOKAHEAD)
        ]

        def first_offset(
            predicate: Callable[[DerivedSample], bool],
            strict_primary: Sequence[DerivedSample] = strict_primary,
            event: StallEvent = event,
        ) -> float | None:
            matched = [sample for sample in strict_primary if predicate(sample)]
            return (matched[0].ts - event.anchor_ts).total_seconds() if matched else None

        rows.append(
            {
                "event_id": event.event_id,
                "anchor_utc": iso_z(event.anchor_ts),
                "anchor_jst": event.anchor_ts.astimezone(JST).isoformat(timespec="seconds"),
                "last_hard_stall_evidence_utc": iso_z(event.last_evidence_ts),
                "request_count": event.request_count,
                "source_file_count": len(event.source_files),
                "evidence_grade": evidence_grade,
                "primary_interval_count": len(primary),
                "minimum_ack_rate_mbps": min((sample.rate_mbps for sample in primary), default=None),
                "median_pre_event_baseline_mbps": percentile(baseline_values, 50.0) if baseline_values else None,
                "minimum_rate_ratio_to_baseline": min(
                    (sample.rate_ratio_to_baseline for sample in primary if sample.rate_ratio_to_baseline is not None), default=None
                ),
                "maximum_notsent_bytes": max((sample.notsent for sample in primary), default=None),
                "maximum_unacked": max((sample.unacked for sample in primary), default=None),
                "maximum_lastsnd_ms": max((sample.lastsnd_ms for sample in primary), default=None),
                "maximum_rto_ms": max((sample.rto_ms for sample in primary), default=None),
                "rtmps_high_resolution_interval_count": len(high_resolution),
                "rtmps_stalled_pressure_interval_count": len(stalled_high_resolution),
                "rtmps_maximum_rtt_ms": max((sample.rtt_ms for sample in high_resolution if sample.rtt_ms is not None), default=None),
                "rtmps_retransmit_delta_sum": sum(sample.retrans_delta or 0 for sample in high_resolution),
                "ffmpeg_network_error_rows": stderr_count,
                "p0_1_threshold_mbps": p0_1_threshold_mbps,
                "p0_1_rate_first_offset_sec_strict": first_offset(lambda sample: sample.rate_mbps < p0_1_threshold_mbps),
                "p0_1_rate_plus_slope_first_offset_sec_strict": first_offset(
                    lambda sample: (
                        sample.rate_mbps < p0_1_threshold_mbps
                        and sample.delta_rate_mbps is not None
                        and sample.delta_rate_mbps < delta_p1_mbps
                    )
                ),
                "p0_1_rate_plus_pressure_first_offset_sec_strict": first_offset(
                    lambda sample: sample.rate_mbps < p0_1_threshold_mbps and sample.queue_pressure
                ),
            }
        )
    return rows


def run_synthetic_suite() -> list[dict[str, Any]]:
    scenarios = {
        "stable_bitrate": ([4.8] * 8, [False] * 8, ["a"] * 8, False),
        "minor_jitter": ([4.8, 4.7, 4.9, 4.75, 4.85, 4.8], [False] * 6, ["a"] * 6, False),
        "single_low_outlier": ([4.8, 4.8, 0.2, 4.8, 4.8], [False, False, True, False, False], ["a"] * 5, False),
        "slow_degradation": ([4.8, 4.2, 3.0, 1.0, 0.2, 0.1], [False, False, False, True, True, True], ["a"] * 6, True),
        "sudden_drop": ([4.8, 4.8, 0.1, 0.0, 0.0], [False, False, True, True, True], ["a"] * 5, True),
        "sustained_near_zero": ([4.8, 0.05, 0.04, 0.03, 0.02], [False, True, True, True, True], ["a"] * 5, True),
        "bitrate_step_change": ([4.8, 4.8, 3.2, 3.2, 3.2], [False] * 5, ["a", "a", "b", "b", "b"], False),
        "long_degraded_period": ([4.8] + [0.2] * 12, [False] + [True] * 12, ["a"] * 13, True),
        "recovery": ([4.8, 0.1, 0.0, 0.0, 4.7, 4.8], [False, True, True, True, False, False], ["a"] * 6, True),
    }
    rows: list[dict[str, Any]] = []
    for name, (rates, pressures, profiles, expected_candidate) in scenarios.items():
        streak = 0
        states: list[str] = []
        previous_profile = profiles[0]
        for rate, pressure, profile in zip(rates, pressures, profiles, strict=True):
            if profile != previous_profile:
                streak = 0
                states.append("PROFILE_RESET")
                previous_profile = profile
                continue
            if rate < 1.0:
                streak += 1
                if pressure and streak >= 2:
                    states.append("STALL_CANDIDATE")
                elif rate < 0.5:
                    states.append("STRONG_DEGRADATION")
                else:
                    states.append("DEGRADED")
            else:
                streak = 0
                states.append("NORMAL")
        observed = "STALL_CANDIDATE" in states
        recovered = name != "recovery" or states[-1] == "NORMAL"
        rows.append(
            {
                "scenario": name,
                "expected_stall_candidate": expected_candidate,
                "observed_stall_candidate": observed,
                "recovered_to_normal": recovered,
                "pass": observed == expected_candidate and recovered,
                "states": states,
                "physical_effect_count": 0,
            }
        )
    return rows


def choose_recommendations(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    def exact(rule: str, persistence: int, percentile_value: float) -> dict[str, Any] | None:
        return next(
            (
                row
                for row in rows
                if row["rule"] == rule and row["persistence_n"] == persistence and float(row["percentile"]) == percentile_value
            ),
            None,
        )

    early = exact("rate_only", 1, 0.1)
    strong = exact("rate_plus_queue_pressure", 1, 0.1)
    stall = exact("rate_plus_queue_pressure", 1, 0.1)
    return {
        "early_warning": {
            **(early or {}),
            "condition": "rate < normal P0.1 for one historical 60-66 second interval",
            "semantics": "TX_RATE_ANOMALY",
        },
        "strong_anomaly": {
            **(strong or {}),
            "condition": "rate < normal P0.1 and queue pressure in the same interval",
            "semantics": "STRONG_DEGRADATION",
        },
        "stall_candidate": {
            **(stall or {}),
            "condition": (
                "strong anomaly plus application still producing, network_down=false, tcp_probe_ok=true, and current TCP corroboration"
            ),
            "required_corroborating_evidence": [
                "FFmpeg/application output progression",
                "ACK progression low or stopped",
                "send queue, retransmission, RTT, or RTO abnormality",
                "application stop and maintenance excluded",
            ],
            "semantics": "STALL_CANDIDATE_NOT_TCP_STALL_CONFIRMED",
        },
    }


def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def sanitized_sample_rows(
    samples: Sequence[DerivedSample],
    *,
    normal_rates: Sequence[float],
    anomaly_threshold_mbps: float,
) -> list[dict[str, Any]]:
    ordered_normal_rates = sorted(normal_rates)
    rows: list[dict[str, Any]] = []
    for sample in samples:
        row = asdict(sample)
        row.pop("pid", None)
        row["ts_utc"] = iso_z(sample.ts)
        row["ts_jst"] = sample.ts.astimezone(JST).isoformat(timespec="seconds")
        row.pop("ts", None)
        row["normal_empirical_percentile_rank"] = (
            bisect.bisect_right(ordered_normal_rates, sample.rate_mbps) / len(ordered_normal_rates) * 100.0
            if ordered_normal_rates
            else None
        )
        if sample.label == "OTHER_FAILURE":
            row["candidate_state"] = "EXCLUDED_OTHER_FAILURE"
            row["candidate_reason"] = "FFmpeg lifecycle failure window"
        elif sample.rate_mbps < anomaly_threshold_mbps and sample.queue_pressure:
            row["candidate_state"] = "STRONG_DEGRADATION"
            row["candidate_reason"] = "ACK rate below normal P0.1 with same-sample TCP queue pressure"
        elif sample.rate_mbps < anomaly_threshold_mbps:
            row["candidate_state"] = "DEGRADED"
            row["candidate_reason"] = "ACK rate below normal P0.1 without same-sample TCP corroboration"
        elif sample.label == "NORMAL_CANDIDATE":
            row["candidate_state"] = "NORMAL"
            row["candidate_reason"] = "positive ACK progression without queue pressure or labeled failure"
        else:
            row["candidate_state"] = "UNKNOWN"
            row["candidate_reason"] = "insufficient evidence for a rate-based semantic state"
        rows.append(row)
    return rows


def _svg_escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def write_bar_svg(path: Path, *, title: str, labels: Sequence[str], values: Sequence[float], y_label: str) -> None:
    width, height = 960, 520
    margin_left, margin_right, margin_top, margin_bottom = 90, 30, 60, 100
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    maximum = max(values, default=1.0) or 1.0
    bar_width = plot_width / max(len(values), 1) * 0.65
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fff"/>',
        f'<text x="{width / 2}" y="32" text-anchor="middle" font-family="sans-serif" font-size="20">{_svg_escape(title)}</text>',
        (
            f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{margin_left + plot_width}" '
            f'y2="{margin_top + plot_height}" stroke="#333"/>'
        ),
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" stroke="#333"/>',
        (
            f'<text x="20" y="{margin_top + plot_height / 2}" '
            f'transform="rotate(-90 20 {margin_top + plot_height / 2})" text-anchor="middle" '
            f'font-family="sans-serif" font-size="13">{_svg_escape(y_label)}</text>'
        ),
    ]
    for index, (label, value) in enumerate(zip(labels, values, strict=True)):
        center = margin_left + (index + 0.5) * plot_width / max(len(values), 1)
        bar_height = value / maximum * plot_height
        y = margin_top + plot_height - bar_height
        parts.append(
            f'<rect x="{center - bar_width / 2:.2f}" y="{y:.2f}" width="{bar_width:.2f}" height="{bar_height:.2f}" fill="#2474b5"/>'
        )
        parts.append(
            f'<text x="{center:.2f}" y="{y - 6:.2f}" text-anchor="middle" font-family="monospace" font-size="11">{value:.3f}</text>'
        )
        parts.append(
            f'<text x="{center:.2f}" y="{margin_top + plot_height + 22}" text-anchor="middle" '
            f'font-family="sans-serif" font-size="12">{_svg_escape(label)}</text>'
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def write_line_svg(path: Path, *, title: str, series: Sequence[tuple[str, Sequence[float]]], y_label: str) -> None:
    width, height = 960, 520
    left, right, top, bottom = 90, 30, 60, 80
    plot_width, plot_height = width - left - right, height - top - bottom
    all_values = [value for _, values in series for value in values]
    minimum = min(all_values, default=0.0)
    maximum = max(all_values, default=1.0)
    if math.isclose(minimum, maximum):
        maximum = minimum + 1.0
    colors = ("#2474b5", "#e07a1f", "#2b9348", "#8f3fb0")
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fff"/>',
        f'<text x="{width / 2}" y="32" text-anchor="middle" font-family="sans-serif" font-size="20">{_svg_escape(title)}</text>',
        f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#333"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#333"/>',
        (
            f'<text x="20" y="{top + plot_height / 2}" transform="rotate(-90 20 {top + plot_height / 2})" '
            f'text-anchor="middle" font-family="sans-serif" font-size="13">{_svg_escape(y_label)}</text>'
        ),
    ]
    for index, (name, values) in enumerate(series):
        if not values:
            continue
        points = []
        for point_index, value in enumerate(values):
            x = left + point_index * plot_width / max(len(values) - 1, 1)
            y = top + (maximum - value) / (maximum - minimum) * plot_height
            points.append(f"{x:.2f},{y:.2f}")
        color = colors[index % len(colors)]
        parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="2"/>')
        parts.append(
            f'<text x="{left + 12 + index * 190}" y="{height - 28}" font-family="sans-serif" '
            f'font-size="12" fill="{color}">{_svg_escape(name)}</text>'
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def refresh_manifest(output: Path) -> None:
    entries: list[str] = []
    for path in sorted(output.iterdir()):
        if not path.is_file() or path.name == "manifest.sha256":
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        entries.append(f"{digest}  {path.name}")
    (output / "manifest.sha256").write_text("\n".join(entries) + "\n", encoding="utf-8")


def run_analysis(args: argparse.Namespace) -> dict[str, Any]:
    cutoff = parse_timestamp(args.analysis_end)
    if cutoff is None:
        raise ValueError("--analysis-end must be a timezone-aware ISO timestamp")
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(f"output path already exists: {output}")
    output.mkdir(parents=True)
    primary_path = Path(args.primary)
    label_paths = [Path(value) for value in args.label_events]
    raw, primary_source = read_primary_samples(primary_path, cutoff=cutoff)
    derived, discontinuities = derive_samples(raw)
    events, event_source = read_stall_events(label_paths, cutoff=cutoff)
    failure_windows, engine_source = read_stream_engine(
        Path(args.stream_engine_events) if args.stream_engine_events else None, cutoff=cutoff
    )
    stderr_errors, stderr_source = read_ffmpeg_stderr(Path(args.ffmpeg_stderr) if args.ffmpeg_stderr else None, cutoff=cutoff)
    rtmps, rtmps_source = read_rtmps_intervals(Path(args.rtmps_observations) if args.rtmps_observations else None, cutoff=cutoff)
    labels = label_samples(derived, events, failure_windows)
    baseline = add_causal_baseline(derived)
    normal = [sample for sample in derived if sample.label == "NORMAL_CANDIDATE"]
    known_stall = [sample for sample in derived if sample.label == "KNOWN_TCP_STALL"]
    normal_rates = [sample.rate_mbps for sample in normal]
    normal_deltas = [sample.delta_rate_mbps for sample in normal if sample.delta_rate_mbps is not None]
    normal_negative_drops = [sample.negative_drop_mbps for sample in normal if sample.negative_drop_mbps is not None]
    lower_tail, evaluations, threshold_context = threshold_evaluation(derived, events, normal_rates, normal_deltas)
    stability = baseline_stability(derived)
    hours = hour_of_day_analysis(derived)
    event_rows = analyze_events(
        events,
        derived,
        rtmps,
        stderr_errors,
        p0_1_threshold_mbps=float(threshold_context["thresholds_mbps"][0.1]),
        delta_p1_mbps=float(threshold_context["delta_p1_mbps"]),
    )
    synthetic = run_synthetic_suite()
    recommendations = choose_recommendations(evaluations)
    observation_start = raw[0].ts if raw else None
    observation_end = raw[-1].ts if raw else None
    analysis = {
        "schema": "tcp_stall_statistical_analysis.v1",
        "generated_at_utc": iso_z(datetime.now(tz=UTC)),
        "analysis_cutoff_utc": iso_z(cutoff),
        "repository_head": args.repository_head,
        "production_connection": "NONE",
        "production_logic_mutated": False,
        "production_detector_enabled": False,
        "restart_logic_mutated": False,
        "runtime_restarted": False,
        "primary_signal": "FFmpeg-selected RTMPS socket cumulative bytes_acked event-time difference",
        "primary_signal_specificity": "HIGH_SOCKET_SELECTED_EVENT_TIME_PROXY",
        "observation_start_utc": iso_z(observation_start),
        "observation_end_utc": iso_z(observation_end),
        "observation_span_days": (observation_end - observation_start).total_seconds() / 86_400.0
        if observation_start and observation_end
        else None,
        "configuration_epochs": engine_source.get("encoder_profiles", []),
        "source_measurement_epochs": dict(Counter(sample.source_epoch for sample in raw)),
        "source_lineage": {
            "primary": primary_source,
            "stall_labels": event_source,
            "stream_engine": engine_source,
            "ffmpeg_stderr": stderr_source,
            "rtmps_burst": rtmps_source,
        },
        "derivation": {
            "valid_interval_seconds": [MIN_INTERVAL_SEC, MAX_INTERVAL_SEC],
            "rate_mbps": "delta(bytes_acked) * 8 / delta(event_ts_microseconds)",
            "bytes_per_10s": "delta(bytes_acked) * 10 / delta(event_ts_seconds)",
            "bytes_per_60s": "delta(bytes_acked) * 60 / delta(event_ts_seconds)",
            "delta_rate_mbps": "rate_t - rate_previous_contiguous_same_pid",
            "drop_ratio": "1 - rate_t / trailing_baseline_median",
            "percentile_implementation": "Hyndman-Fan Type 7 linear interpolation",
            "discontinuities": discontinuities,
        },
        "sample_counts": {
            "raw_ack_rows": len(raw),
            "derived_valid_intervals": len(derived),
            **dict(labels),
        },
        "normal_rate_distribution_mbps": describe(normal_rates),
        "known_stall_rate_distribution_mbps": describe([sample.rate_mbps for sample in known_stall]),
        "normal_delta_rate_distribution_mbps": describe(normal_deltas),
        "normal_negative_drop_distribution_mbps": describe(normal_negative_drops),
        "known_stall_events": len(events),
        "event_evidence_grades": dict(Counter(row["evidence_grade"] for row in event_rows)),
        "threshold_context": threshold_context,
        "causal_rolling_baseline": baseline,
        "recommendations": recommendations,
        "synthetic_suite": {
            "scenario_count": len(synthetic),
            "passed": sum(1 for row in synthetic if row["pass"]),
            "physical_effect_count": 0,
        },
        "limitations": [
            "primary historical rows use controller event timestamps, not atomic runtime observation source timestamps",
            "per-row pending-effect and maintenance eligibility is not available for the full historical period",
            "TCP ACK proves peer TCP progression, not YouTube ingest or viewer playback",
            "the high-resolution RTMPS source is burst/event sampled and is not an unbiased continuous baseline",
            "30-day Prometheus upload metrics are bytes-sent support signals and are not merged with bytes_acked",
            "14-day stability has few independent daily evaluations and 30-day ACK stability is unavailable",
            "controller tcp-stall requests are operational labels, not an independent external ground-truth oracle",
        ],
        "offline_detector_semantics": {
            "empirical_rank": "inclusive empirical CDF against NORMAL_CANDIDATE ACK rates",
            "NORMAL": "positive ACK progression outside labeled failures and without queue pressure",
            "DEGRADED": "ACK rate below normal P0.1 without same-sample queue pressure",
            "STRONG_DEGRADATION": "ACK rate below normal P0.1 with same-sample queue pressure",
            "STALL_CANDIDATE": "not emitted without application-output and independent TCP corroboration",
        },
    }
    write_csv(output / "tcp-stall-percentiles.csv", [{"population": "NORMAL_CANDIDATE", **analysis["normal_rate_distribution_mbps"]}])
    write_csv(output / "tcp-stall-lower-tail.csv", lower_tail)
    write_csv(output / "tcp-stall-threshold-evaluation.csv", evaluations)
    write_csv(output / "tcp-stall-event-analysis.csv", event_rows)
    write_csv(output / "tcp-stall-baseline-stability.csv", stability)
    write_csv(output / "tcp-stall-hour-of-day.csv", hours)
    write_csv(
        output / "tcp-stall-derived-samples.csv",
        sanitized_sample_rows(
            derived,
            normal_rates=normal_rates,
            anomaly_threshold_mbps=float(threshold_context["thresholds_mbps"][0.1]),
        ),
    )
    write_csv(output / "tcp-stall-synthetic-scenarios.csv", synthetic)
    (output / "tcp-stall-analysis.json").write_text(json.dumps(analysis, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_bar_svg(
        output / "tcp-stall-lower-tail-thresholds.svg",
        title="Normal ACK-rate lower-tail thresholds",
        labels=[f"P{row['percentile']:g}" for row in lower_tail],
        values=[float(row["threshold_mbps"]) for row in lower_tail],
        y_label="ACK delivery rate (Mbps)",
    )
    write_bar_svg(
        output / "tcp-stall-false-runs.svg",
        title="Single-sample false candidate runs per day",
        labels=[f"P{row['percentile']:g}" for row in lower_tail],
        values=[float(row["false_runs_per_day"]) for row in lower_tail],
        y_label="Normal candidate runs/day",
    )
    available_stability = [row for row in stability if row["status"] == "AVAILABLE"]
    write_line_svg(
        output / "tcp-stall-window-stability.svg",
        title="Window-length stability across daily evaluations",
        series=[(metric, [float(row[f"{metric}_spread_pct"]) for row in available_stability]) for metric in ("median", "p5", "p1", "p0_5")],
        y_label="min-max spread / median (%)",
    )
    refresh_manifest(output)
    return analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary", required=True, help="JSONL containing tcp_send_sample rows")
    parser.add_argument("--label-events", required=True, nargs="+", help="JSONL sources containing recovery_requested evidence")
    parser.add_argument("--stream-engine-events", help="Optional stream_engine_events.jsonl")
    parser.add_argument("--ffmpeg-stderr", help="Optional structured ffmpeg_stderr.jsonl")
    parser.add_argument("--rtmps-observations", help="Optional RTMPS ss -ti burst JSONL")
    parser.add_argument("--analysis-end", required=True, help="Fixed inclusive UTC/JST cutoff")
    parser.add_argument("--repository-head", required=True, help="Code identity used for this analysis")
    parser.add_argument("--output", required=True, help="New revisioned output directory; must not already exist")
    parser.add_argument("--refresh-manifest", action="store_true", help="Only refresh manifest.sha256 in --output")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.refresh_manifest:
        refresh_manifest(Path(args.output))
        return 0
    run_analysis(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
