#!/usr/bin/env python3
"""Build OpenMetrics backfill data for stream_v2 Prometheus dashboards."""

from __future__ import annotations

import argparse
import bisect
import gzip
import json
import math
import statistics
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


CURRENT_LOG_ROOT = Path("/home/yuki/projects/stream_v2/.state/adsb-streamnew-v2/logs")
LEGACY_LOG_ROOT = Path("/home/yuki/.local/state/adsb-streamnew/logs")


def parse_utc(value: Any) -> int | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp())
    except ValueError:
        return None


def iter_log_paths(root: Path, base: str) -> list[Path]:
    if not root.exists():
        return []
    paths = [p for p in root.glob(base + "*") if p.name == base or p.name.startswith(base + ".")]
    return sorted(paths, key=lambda p: p.stat().st_mtime)


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def iter_events(base: str, roots: Iterable[Path], *, cutoff_ts: int) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen: set[tuple[int, str, str]] = set()
    for root in roots:
        for path in iter_log_paths(root, base):
            try:
                fh = open_text(path)
            except OSError:
                continue
            with fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(event, dict):
                        continue
                    ts = parse_utc(event.get("ts_utc") or event.get("generated_at_utc"))
                    if ts is None or ts > cutoff_ts:
                        continue
                    event["_ts"] = ts
                    key = (ts, str(event.get("event_id", "")), json.dumps(event, sort_keys=True, default=str)[:200])
                    if key in seen:
                        continue
                    seen.add(key)
                    events.append(event)
    events.sort(key=lambda item: int(item["_ts"]))
    return events


def value_to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * pct
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[int(index)]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


class OpenMetrics:
    def __init__(self) -> None:
        self.samples: dict[tuple[str, tuple[tuple[str, str], ...]], list[tuple[int, float]]] = defaultdict(list)

    def add(self, name: str, value: Any, ts: int, labels: dict[str, Any] | None = None) -> None:
        parsed = value_to_float(value)
        if parsed is None:
            return
        label_tuple = tuple(sorted((str(k), str(v)) for k, v in (labels or {}).items()))
        self.samples[(name, label_tuple)].append((ts, parsed))

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        rows: list[tuple[int, str, tuple[tuple[str, str], ...], float]] = []
        for (name, labels), samples in self.samples.items():
            last_ts = -1
            for ts, value in sorted(samples):
                if ts <= last_ts:
                    continue
                last_ts = ts
                rows.append((ts, name, labels, value))
        with path.open("w", encoding="utf-8") as fh:
            for name in sorted({name for _, name, _, _ in rows}):
                fh.write(f"# TYPE {name} gauge\n")
            for ts, name, labels, value in sorted(rows):
                label_text = ""
                if labels:
                    pairs = [f'{k}="{escape_label(v)}"' for k, v in labels]
                    label_text = "{" + ",".join(pairs) + "}"
                fh.write(f"{name}{label_text} {value:.6g} {ts}\n")
            fh.write("# EOF\n")


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def add_api_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    totals: dict[str, float] = defaultdict(float)
    exceeded: dict[str, float] = defaultdict(float)
    for event in events:
        ts = int(event["_ts"])
        day = datetime.fromtimestamp(ts, timezone.utc).astimezone().date().isoformat()
        # YouTube quota day is PT. Approximate with America/Los_Angeles if zoneinfo exists.
        try:
            from zoneinfo import ZoneInfo

            day = datetime.fromtimestamp(ts, timezone.utc).astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat()
        except Exception:
            pass
        totals[day] += float(event.get("cost_units") or 0)
        if event.get("quota_exceeded"):
            exceeded[day] += 1
        labels = {"pt_day": day}
        out.add("stream_v2_youtube_api_units", totals[day], ts, labels)
        out.add("stream_v2_youtube_api_quota_exceeded_events", exceeded[day], ts, labels)
        for hours in ("1", "8", "24"):
            out.add("stream_v2_api_open_day_units", totals[day], ts, {"window_hours": hours})


def add_fast_recovery_metrics(out: OpenMetrics, events: list[dict[str, Any]], *, start_ts: int, end_ts: int, step_sec: int) -> None:
    samples = [(int(e["_ts"]), float(e["mbps"])) for e in events if e.get("kind") == "tcp_send_sample" and value_to_float(e.get("mbps")) is not None]
    restarts = [int(e["_ts"]) for e in events if str(e.get("kind")) == "restart"]
    sample_times = [ts for ts, _ in samples]
    restart_times = sorted(restarts)
    for ts in range(start_ts, end_ts + 1, step_sec):
        window_start = ts - 24 * 3600
        left = bisect.bisect_left(sample_times, window_start)
        right = bisect.bisect_right(sample_times, ts)
        values = [v for _, v in samples[left:right]]
        if values:
            labels = {"window_hours": "24"}
            out.add("stream_v2_upload_p95_mbps", percentile(values, 0.95), ts, labels)
            out.add("stream_v2_upload_max_mbps", max(values), ts, labels)
            out.add("stream_v2_upload_over_budget_seconds", sum(60 for v in values if v > 5.0), ts, labels)
            out.add("stream_v2_upload_within_5mbps_ratio_pct", 100.0 * sum(1 for v in values if v <= 5.0) / len(values), ts)
        count = bisect.bisect_right(restart_times, ts) - bisect.bisect_left(restart_times, window_start)
        for hours in ("1", "8", "24"):
            window = int(hours) * 3600
            count_h = bisect.bisect_right(restart_times, ts) - bisect.bisect_left(restart_times, ts - window)
            out.add("stream_v2_fast_recovery_restart_count", count_h, ts, {"window_hours": hours})


def add_engine_metrics(out: OpenMetrics, events: list[dict[str, Any]], *, start_ts: int, end_ts: int, step_sec: int) -> None:
    ffmpeg_restart = sorted(int(e["_ts"]) for e in events if str(e.get("event_type")) in {"ffmpeg_restart_scheduled", "ffmpeg_exited"})
    for ts in range(start_ts, end_ts + 1, step_sec):
        for hours in ("1", "8", "24"):
            window = int(hours) * 3600
            count = bisect.bisect_right(ffmpeg_restart, ts) - bisect.bisect_left(ffmpeg_restart, ts - window)
            out.add("stream_v2_ffmpeg_restart_incident_clusters", count, ts, {"window_hours": hours})


def add_youtube_health_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    bad_window: deque[int] = deque()
    warn_window: deque[int] = deque()
    for event in events:
        ts = int(event["_ts"])
        bad = not bool(event.get("healthy", event.get("status") == "ok"))
        warn = str(event.get("status")) == "warn"
        if bad:
            bad_window.append(ts)
        if warn:
            warn_window.append(ts)
        while bad_window and bad_window[0] < ts - 24 * 3600:
            bad_window.popleft()
        while warn_window and warn_window[0] < ts - 24 * 3600:
            warn_window.popleft()
        for hours in ("1", "8", "24"):
            window = int(hours) * 3600
            bad_count = sum(1 for item in bad_window if item >= ts - window)
            warn_count = sum(1 for item in warn_window if item >= ts - window)
            out.add("stream_v2_current_fail", 1 if bad else 0, ts, {"window_hours": hours})
            out.add("stream_v2_historical_degraded", 1 if bad_count else 0, ts, {"window_hours": hours})
            out.add("stream_v2_youtube_warn_count", warn_count, ts, {"window_hours": hours})


def add_memory_metrics(out: OpenMetrics, events: list[dict[str, Any]], *, start_ts: int, end_ts: int, step_sec: int) -> None:
    rows: list[tuple[int, float, float, str]] = []
    for event in events:
        ts = int(event["_ts"])
        host = event.get("host") if isinstance(event.get("host"), dict) else {}
        overall = event.get("overall") if isinstance(event.get("overall"), dict) else {}
        non_reclaim = value_to_float(host.get("non_reclaimable_estimate_bytes"))
        available = value_to_float(host.get("mem_available_bytes"))
        if non_reclaim is None or available is None:
            continue
        rows.append((ts, non_reclaim / 1024 / 1024, available / 1024 / 1024, str(overall.get("severity", "unknown"))))
    times = [row[0] for row in rows]
    for ts in range(start_ts, end_ts + 1, step_sec):
        for window_name, window_sec in (("rolling_1h", 3600), ("rolling_8h", 8 * 3600), ("rolling_24h", 24 * 3600)):
            left = bisect.bisect_left(times, ts - window_sec)
            right = bisect.bisect_right(times, ts)
            values = rows[left:right]
            if not values:
                continue
            non_reclaim = [row[1] for row in values]
            available = [row[2] for row in values]
            severities = [row[3] for row in values]
            labels = {"window": window_name}
            out.add("stream_v2_memory_non_reclaimable_p95_mib", percentile(non_reclaim, 0.95), ts, labels)
            out.add("stream_v2_memory_available_min_mib", min(available), ts, labels)
            out.add("stream_v2_memory_warn_count", severities.count("warn"), ts, labels)
            out.add("stream_v2_memory_critical_count", severities.count("critical"), ts, labels)


def child_dict(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def add_youtube_watchdog_detail_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    for event in events:
        ts = int(event["_ts"])
        out.add("stream_v2_youtube_watchdog_healthy", event.get("healthy"), ts)
        out.add("stream_v2_youtube_public_ok", event.get("public_ok"), ts)
        out.add("stream_v2_youtube_api_ok", event.get("api_ok"), ts)
        out.add("stream_v2_youtube_oauth_ok", event.get("oauth_ok"), ts)
        out.add("stream_v2_youtube_local_ok", event.get("local_ok"), ts)
        out.add("stream_v2_youtube_ingest_connected", event.get("ingest_connected"), ts)
        out.add("stream_v2_youtube_stream_active", event.get("stream_active"), ts)
        out.add("stream_v2_youtube_fail_count", event.get("fail_count"), ts)
        out.add("stream_v2_youtube_degraded_public_count", event.get("degraded_public_count"), ts)
        out.add("stream_v2_youtube_ffmpeg_uptime_seconds", event.get("ffmpeg_uptime_sec"), ts)
        out.add("stream_v2_youtube_api_projected_units_per_day", event.get("api_cost_projected_units_per_day"), ts)
        out.add("stream_v2_youtube_api_threshold_units_per_day", event.get("api_cost_threshold_units_per_day"), ts)
        out.add("stream_v2_youtube_api_burn_rate_active", event.get("api_cost_burn_rate_active"), ts)
        out.add("stream_v2_youtube_url_recovery_elapsed_seconds", event.get("url_recovery_elapsed_sec"), ts)
        out.add("stream_v2_youtube_candidate_new_url_found", event.get("candidate_new_url_found"), ts)
        out.add("stream_v2_youtube_stats_age_seconds", 0, ts)


def add_subsystem_detail_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    for event in events:
        ts = int(event["_ts"])
        overall = child_dict(event, "overall")
        out.add("stream_v2_subsystems_healthy", 1 if overall.get("state") == "healthy" else 0, ts)
        out.add("stream_v2_same_url_live", 1 if overall.get("stream_public_state") == "same_url_live" else 0, ts)
        out.add("stream_v2_subsystems_degraded_count", len(overall.get("degraded_subsystems") or []), ts)


def add_memory_current_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    for event in events:
        ts = int(event["_ts"])
        overall = child_dict(event, "overall")
        host = child_dict(event, "host")
        out.add("stream_v2_memory_current_ok", 1 if overall.get("severity") == "ok" else 0, ts)
        available = value_to_float(host.get("mem_available_bytes"))
        swap_used = value_to_float(host.get("swap_used_bytes"))
        if available is not None:
            out.add("stream_v2_host_mem_available_mib", available / 1024 / 1024, ts)
        if swap_used is not None:
            out.add("stream_v2_host_swap_used_mib", swap_used / 1024 / 1024, ts)


def add_network_detail_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    for event in events:
        ts = int(event["_ts"])
        classification = child_dict(event, "classification")
        route = child_dict(event, "route")
        addresses = child_dict(event, "addresses")
        dns = child_dict(event, "dns")
        tcp4 = child_dict(event, "tcp_connect_ipv4")
        tcp6 = child_dict(event, "tcp_connect_ipv6")
        ffmpeg_socket = child_dict(event, "ffmpeg_socket")
        out.add("stream_v2_network_ok", 1 if classification.get("status") == "ok" else 0, ts)
        out.add("stream_v2_network_ipv4_route_ok", child_dict(route, "ipv4_default").get("ok"), ts)
        out.add("stream_v2_network_ipv6_route_ok", child_dict(route, "ipv6_default").get("ok"), ts)
        out.add("stream_v2_network_addresses_ok", addresses.get("ok"), ts)
        out.add("stream_v2_network_dns_ok", dns.get("ok"), ts)
        out.add("stream_v2_network_tcp_connect_ok", tcp4.get("ok"), ts, {"family": "ipv4"})
        out.add("stream_v2_network_tcp_connect_elapsed_ms", tcp4.get("elapsed_ms"), ts, {"family": "ipv4"})
        out.add("stream_v2_network_tcp_connect_ok", tcp6.get("ok"), ts, {"family": "ipv6"})
        out.add("stream_v2_network_tcp_connect_elapsed_ms", tcp6.get("elapsed_ms"), ts, {"family": "ipv6"})
        out.add("stream_v2_network_ffmpeg_socket_connected", ffmpeg_socket.get("connected"), ts)
        out.add("stream_v2_network_ffmpeg_socket_notsent_bytes", ffmpeg_socket.get("notsent"), ts)
        out.add("stream_v2_network_ffmpeg_socket_unacked", ffmpeg_socket.get("unacked"), ts)
        out.add("stream_v2_network_ffmpeg_socket_lastsnd_ms", ffmpeg_socket.get("lastsnd_ms"), ts)
        out.add("stream_v2_network_observer_age_seconds", 0, ts)


def add_resource_memory_detail_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    for event in events:
        ts = int(event["_ts"])
        host_mem = child_dict(event, "host_memory")
        mem_pressure = child_dict(event, "memory_pressure")
        vm_activity = child_dict(event, "vm_activity")
        cgroups = child_dict(event, "cgroups")
        out.add("stream_v2_host_mem_available_mib", host_mem.get("mem_available_mb"), ts)
        out.add("stream_v2_host_mem_available_ratio", host_mem.get("mem_available_ratio"), ts)
        out.add("stream_v2_host_swap_used_mib", host_mem.get("swap_used_mb"), ts)
        out.add("stream_v2_host_swap_used_ratio", host_mem.get("swap_used_ratio"), ts)
        out.add("stream_v2_host_memory_pressure_some_avg10", mem_pressure.get("some_avg10"), ts)
        out.add("stream_v2_host_memory_pressure_full_avg10", mem_pressure.get("full_avg10"), ts)
        out.add("stream_v2_host_pgmajfault_delta_per_min", vm_activity.get("pgmajfault_delta_per_min"), ts)
        out.add("stream_v2_host_pswpin_delta_per_min", vm_activity.get("pswpin_delta_per_min"), ts)
        out.add("stream_v2_resource_memory_age_seconds", 0, ts)
        for unit, payload in cgroups.items():
            if not isinstance(payload, dict):
                continue
            labels = {"unit": unit}
            out.add("stream_v2_cgroup_memory_current_mib", payload.get("memory_current_mb"), ts, labels)
            out.add("stream_v2_cgroup_memory_peak_mib", payload.get("memory_peak_mb"), ts, labels)
            out.add("stream_v2_cgroup_swap_current_mib", payload.get("memory_swap_current_mb"), ts, labels)


def add_watchdog_detail_metrics(
    out: OpenMetrics,
    timeline_events: list[dict[str, Any]],
    watchdog_events: list[dict[str, Any]],
) -> None:
    for event in timeline_events:
        ts = int(event["_ts"])
        runtime = child_dict(event, "runtime_snapshot")
        slo = child_dict(event, "slo_state")
        ffmpeg_count = value_to_float(event.get("ffmpeg_count"))
        stream_running = event.get("stream_service_substate") == "running"
        runtime_running = runtime.get("status") == "running"
        ok = stream_running and runtime_running and (ffmpeg_count is None or ffmpeg_count >= 1)
        out.add("stream_v2_stream_watchdog_ok", 1 if ok else 0, ts)
        out.add("stream_v2_stream_watchdog_ffmpeg_count", event.get("ffmpeg_count"), ts)
        out.add("stream_v2_stream_watchdog_runtime_snapshot_age_seconds", runtime.get("age_sec"), ts)
        out.add("stream_v2_stream_watchdog_stats_age_seconds", 0, ts)
        out.add("stream_v2_slo_pulse_unavailable_count", slo.get("pulse_unavailable_count"), ts)
        out.add("stream_v2_slo_restart_trigger_count", slo.get("restart_trigger_count"), ts)

    last_change_ts: int | None = None
    for event in watchdog_events:
        ts = int(event["_ts"])
        current_messages = value_to_float(event.get("current_messages"))
        previous_messages = value_to_float(event.get("previous_messages"))
        if current_messages is None or previous_messages is None:
            continue
        if current_messages != previous_messages:
            last_change_ts = ts
        if last_change_ts is not None:
            out.add("stream_v2_adsb_messages_last_change_age_seconds", max(0, ts - last_change_ts), ts)


def add_recovery_detail_metrics(out: OpenMetrics, events: list[dict[str, Any]]) -> None:
    for event in events:
        ts = int(event["_ts"])
        out.add("stream_v2_recovery_action_pending", 1 if event.get("action") not in ("", "none", None) else 0, ts)
        out.add("stream_v2_recovery_action_executable", event.get("executable"), ts)
        out.add("stream_v2_recovery_action_blocked_count", len(event.get("blocked_by") or []), ts)
        out.add("stream_v2_recovery_plan_age_seconds", 0, ts)


def filter_start(events: list[dict[str, Any]], *, start_ts: int) -> list[dict[str, Any]]:
    return [event for event in events if int(event["_ts"]) >= start_ts]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cutoff-sec", type=int, default=int(time.time()) - 3 * 3600)
    parser.add_argument("--step-sec", type=int, default=600)
    parser.add_argument("--lookback-days", type=float, default=0.0, help="Limit output start to cutoff minus this many days.")
    parser.add_argument(
        "--metric-set",
        choices=("baseline", "expanded", "all"),
        default="all",
        help="baseline is the original dashboard backfill; expanded adds detailed exporter-aligned metrics.",
    )
    args = parser.parse_args()

    roots = [LEGACY_LOG_ROOT, CURRENT_LOG_ROOT]
    fast = iter_events("fast_recovery_events.jsonl", roots, cutoff_ts=args.cutoff_sec)
    api = iter_events("youtube_api_calls.jsonl", roots, cutoff_ts=args.cutoff_sec)
    engine = iter_events("stream_engine_events.jsonl", roots, cutoff_ts=args.cutoff_sec)
    youtube = iter_events("youtube_watchdog.jsonl", roots, cutoff_ts=args.cutoff_sec)
    memory = iter_events("memory_status.jsonl", roots, cutoff_ts=args.cutoff_sec)
    network = iter_events("network_observer.jsonl", roots, cutoff_ts=args.cutoff_sec)
    resource_memory = iter_events("resource_memory.jsonl", roots, cutoff_ts=args.cutoff_sec)
    recovery_plan = iter_events("recovery_action_plan.jsonl", roots, cutoff_ts=args.cutoff_sec)
    subsystems = iter_events("subsystems_status.jsonl", roots, cutoff_ts=args.cutoff_sec)
    stream_watchdog = iter_events("stream_watchdog_events.jsonl", roots, cutoff_ts=args.cutoff_sec)
    watchdog_timeline = iter_events("watchdog_state_timeline.jsonl", roots, cutoff_ts=args.cutoff_sec)

    source_series = (
        fast,
        api,
        engine,
        youtube,
        memory,
        network,
        resource_memory,
        recovery_plan,
        subsystems,
        stream_watchdog,
        watchdog_timeline,
    )
    all_ts = [int(e["_ts"]) for series in source_series for e in series]
    if not all_ts:
        raise SystemExit("no source events found")
    start_ts = min(all_ts)
    end_ts = min(max(all_ts), args.cutoff_sec)
    if args.lookback_days > 0:
        start_ts = max(start_ts, int(args.cutoff_sec - args.lookback_days * 24 * 3600))
    fast = filter_start(fast, start_ts=start_ts)
    api = filter_start(api, start_ts=start_ts)
    engine = filter_start(engine, start_ts=start_ts)
    youtube = filter_start(youtube, start_ts=start_ts)
    memory = filter_start(memory, start_ts=start_ts)
    network = filter_start(network, start_ts=start_ts)
    resource_memory = filter_start(resource_memory, start_ts=start_ts)
    recovery_plan = filter_start(recovery_plan, start_ts=start_ts)
    subsystems = filter_start(subsystems, start_ts=start_ts)
    stream_watchdog = filter_start(stream_watchdog, start_ts=start_ts)
    watchdog_timeline = filter_start(watchdog_timeline, start_ts=start_ts)

    out = OpenMetrics()
    if args.metric_set in ("baseline", "all"):
        add_api_metrics(out, api)
        add_fast_recovery_metrics(out, fast, start_ts=start_ts, end_ts=end_ts, step_sec=args.step_sec)
        add_engine_metrics(out, engine, start_ts=start_ts, end_ts=end_ts, step_sec=args.step_sec)
        add_youtube_health_metrics(out, youtube)
        add_memory_metrics(out, memory, start_ts=start_ts, end_ts=end_ts, step_sec=args.step_sec)
    if args.metric_set in ("expanded", "all"):
        add_youtube_watchdog_detail_metrics(out, youtube)
        add_subsystem_detail_metrics(out, subsystems)
        add_memory_current_metrics(out, memory)
        add_network_detail_metrics(out, network)
        add_resource_memory_detail_metrics(out, resource_memory)
        add_watchdog_detail_metrics(out, watchdog_timeline, stream_watchdog)
        add_recovery_detail_metrics(out, recovery_plan)
    out.write(args.output)
    print(f"wrote {args.output}")
    print(f"metric_set {args.metric_set}")
    print(f"range {datetime.fromtimestamp(start_ts, timezone.utc).isoformat()} -> {datetime.fromtimestamp(end_ts, timezone.utc).isoformat()}")
    print(f"series {len(out.samples)} samples {sum(len(v) for v in out.samples.values())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
