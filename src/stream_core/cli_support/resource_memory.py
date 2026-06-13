from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

try:
    from stream_core.common.json_io import append_jsonl, atomic_write_json_file, iter_jsonl, read_json_file
    from stream_core.common.timeutil import parse_utc_ts, utc_text_from_ts
except ModuleNotFoundError:
    from common.json_io import append_jsonl, atomic_write_json_file, iter_jsonl, read_json_file
    from common.timeutil import parse_utc_ts, utc_text_from_ts


MIB = 1024 * 1024
BASELINE_READY_SEC = 7 * 24 * 3600
PROCESS_LEAK_MIN_GROWTH_8H_BYTES = 256 * MIB
PROCESS_LEAK_MIN_RATE_BYTES_PER_HOUR = 32 * MIB
HOST_SWAP_GROWTH_1H_BYTES = 256 * MIB
PSWP_ACTIVITY_PER_MIN_FLOOR = 0.0
PSI_AVG300_FLOOR = 0.0
SCHEMA_VERSION = "resource_memory.v1"


PROCESS_GROUP_RULES: dict[str, tuple[str, ...]] = {
    "chromium": ("chromium", "chrome"),
    "xvfb": ("xvfb",),
    "stream_engine": ("stream_engine.py",),
    "overlay_server": ("overlay_server.py",),
    "tar1090_proxy": ("tar1090", "stream1090", "readsb"),
    "auto_dj": ("auto_dj.py",),
    "audio_player": ("adsb-streamnew-auto-dj", "/ncs_music/", "time_tags"),
    "watchdogs": ("stream_watchdog.py", "youtube_watchdog.py", "fast_recovery.py", "youtube_video_id_resolver.py"),
    "ffmpeg": ("ffmpeg",),
}

SUBSYSTEM_MEMBERS: dict[str, tuple[str, ...]] = {
    "rendering": ("chromium", "xvfb", "overlay_server", "tar1090_proxy"),
    "delivery": ("ffmpeg", "stream_engine"),
    "music": ("auto_dj", "audio_player"),
    "monitoring": ("watchdogs",),
}


@dataclass(frozen=True)
class ResourceMemoryContext:
    resource_memory_file: Path
    resource_memory_events_file: Path
    resource_memory_assessment_file: Path
    memory_status_events_file: Path
    service_units: tuple[str, ...]
    run_systemctl_readonly: Callable[[list[str], bool], object]
    state_base_dir: Path
    log_base_dir: Path
    proc_root: Path = Path("/proc")
    cgroup_root: Path = Path("/sys/fs/cgroup")


def mb(value: int | float | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / MIB, 3)


def ratio(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return round(float(numerator) / float(denominator), 6)


def read_key_value_bytes(path: Path, *, kb_units: bool = False) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return values
    multiplier = 1024 if kb_units else 1
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            values[parts[0].rstrip(":")] = int(parts[1]) * multiplier
        except ValueError:
            continue
    return values


def read_key_value_ints(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return values
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            values[parts[0]] = int(parts[1])
        except ValueError:
            continue
    return values


def parse_psi_text(text: str) -> dict[str, float | int | None]:
    out: dict[str, float | int | None] = {
        "some_avg10": None,
        "some_avg60": None,
        "some_avg300": None,
        "some_total_us": None,
        "full_avg10": None,
        "full_avg60": None,
        "full_avg300": None,
        "full_total_us": None,
    }
    for line in text.splitlines():
        parts = line.split()
        if not parts or parts[0] not in {"some", "full"}:
            continue
        prefix = parts[0]
        for part in parts[1:]:
            if "=" not in part:
                continue
            key, raw = part.split("=", 1)
            out_key = f"{prefix}_{'total_us' if key == 'total' else key}"
            try:
                out[out_key] = int(raw) if key == "total" else float(raw)
            except ValueError:
                continue
    return out


def read_psi(path: Path) -> dict[str, float | int | None]:
    try:
        return parse_psi_text(path.read_text(encoding="utf-8"))
    except OSError:
        return parse_psi_text("")


def host_memory(meminfo: dict[str, int]) -> dict:
    total = meminfo.get("MemTotal")
    free = meminfo.get("MemFree")
    available = meminfo.get("MemAvailable")
    buffers = meminfo.get("Buffers")
    cached = meminfo.get("Cached")
    swap_total = meminfo.get("SwapTotal")
    swap_free = meminfo.get("SwapFree")
    swap_used = max(0, swap_total - swap_free) if swap_total is not None and swap_free is not None else None
    slab_reclaimable = meminfo.get("SReclaimable")
    slab_unreclaimable = meminfo.get("SUnreclaim")
    return {
        "mem_total_mb": mb(total),
        "mem_available_mb": mb(available),
        "mem_available_ratio": ratio(available, total),
        "mem_free_mb": mb(free),
        "buffers_mb": mb(buffers),
        "cached_mb": mb(cached),
        "swap_total_mb": mb(swap_total),
        "swap_used_mb": mb(swap_used),
        "swap_used_ratio": ratio(swap_used, swap_total),
        "dirty_mb": mb(meminfo.get("Dirty")),
        "writeback_mb": mb(meminfo.get("Writeback")),
        "slab_mb": mb(meminfo.get("Slab")),
        "slab_reclaimable_mb": mb(slab_reclaimable),
        "slab_unreclaimable_mb": mb(slab_unreclaimable),
        "_bytes": {
            "mem_total": total,
            "mem_available": available,
            "swap_used": swap_used,
        },
    }


def latest_snapshot(path: Path) -> dict:
    return read_json_file(path)


def delta_per_min(current: int | None, previous: int | None, elapsed_sec: float | None) -> float | None:
    if current is None or previous is None or not elapsed_sec or elapsed_sec <= 0:
        return None
    return round(max(0.0, float(current - previous)) * 60.0 / elapsed_sec, 3)


def vm_activity(vmstat: dict[str, int], previous: dict, *, elapsed_sec: float | None) -> dict:
    prev_vm = previous.get("vmstat_raw") if isinstance(previous.get("vmstat_raw"), dict) else {}
    oom_delta = 0
    if elapsed_sec and elapsed_sec > 0:
        oom_delta = max(0, int(vmstat.get("oom_kill", 0) - int(prev_vm.get("oom_kill", 0) or 0)))
    return {
        "pgmajfault_delta_per_min": delta_per_min(vmstat.get("pgmajfault"), prev_vm.get("pgmajfault"), elapsed_sec),
        "pswpin_delta_per_min": delta_per_min(vmstat.get("pswpin"), prev_vm.get("pswpin"), elapsed_sec),
        "pswpout_delta_per_min": delta_per_min(vmstat.get("pswpout"), prev_vm.get("pswpout"), elapsed_sec),
        "oom_kill_count_delta": oom_delta,
    }


def parse_systemctl_show(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def int_or_none(raw: object) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text in {"[not set]", "n/a", "infinity"}:
        return None
    try:
        value = int(text)
    except ValueError:
        return None
    return value if 0 <= value < 2**60 else None


def cgroup_path(root: Path, control_group: str) -> Path | None:
    if not control_group or control_group == "/":
        return None
    return root / control_group.lstrip("/")


def cgroup_memory(path: Path | None, previous: dict | None = None) -> dict:
    previous = previous or {}
    if path is None or not path.exists():
        return {
            "available": False,
            "path": str(path) if path else "",
            "memory_current_mb": None,
            "memory_peak_mb": None,
            "memory_swap_current_mb": None,
            "memory_events": {},
            "memory_stat": {},
            "pressure": parse_psi_text(""),
        }
    stat = read_key_value_bytes(path / "memory.stat")
    events = read_key_value_ints(path / "memory.events")
    previous_events = previous.get("memory_events") if isinstance(previous.get("memory_events"), dict) else {}
    previous_stat = previous.get("_stat_raw") if isinstance(previous.get("_stat_raw"), dict) else {}
    previous_has_counters = bool(previous_events or previous_stat)
    pressure = read_psi(path / "memory.pressure")
    return {
        "available": True,
        "path": str(path),
        "memory_current_mb": mb(int_or_none(read_text(path / "memory.current"))),
        "memory_peak_mb": mb(int_or_none(read_text(path / "memory.peak"))),
        "memory_swap_current_mb": mb(int_or_none(read_text(path / "memory.swap.current"))),
        "memory_events": {
            "low": events.get("low", 0),
            "high": events.get("high", 0),
            "max": events.get("max", 0),
            "oom": events.get("oom", 0),
            "oom_kill": events.get("oom_kill", 0),
            "oom_group_kill": events.get("oom_group_kill", 0),
            "delta": {
                key: max(0, events.get(key, 0) - int(previous_events.get(key, 0) or 0)) if previous_has_counters else 0
                for key in ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")
            },
        },
        "memory_stat": {
            "anon_mb": mb(stat.get("anon")),
            "file_mb": mb(stat.get("file")),
            "kernel_mb": mb(stat.get("kernel")),
            "slab_reclaimable_mb": mb(stat.get("slab_reclaimable")),
            "slab_unreclaimable_mb": mb(stat.get("slab_unreclaimable")),
            "sock_mb": mb(stat.get("sock")),
            "shmem_mb": mb(stat.get("shmem")),
            "pgmajfault_delta": max(0, stat.get("pgmajfault", 0) - int(previous_stat.get("pgmajfault", 0) or 0))
            if previous_has_counters
            else 0,
        },
        "pressure": pressure,
        "_events_raw": events,
        "_stat_raw": stat,
    }


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def service_cgroups(ctx: ResourceMemoryContext, previous: dict) -> dict[str, dict]:
    previous_by_unit = previous.get("cgroups") if isinstance(previous.get("cgroups"), dict) else {}
    out: dict[str, dict] = {}
    properties = [
        "LoadState",
        "ActiveState",
        "SubState",
        "ControlGroup",
        "MainPID",
        "NRestarts",
    ]
    for unit in ctx.service_units:
        cp = ctx.run_systemctl_readonly(
            ["show", unit, *[f"--property={prop}" for prop in properties], "--no-pager"],
            False,
        )
        stdout = getattr(cp, "stdout", "") or ""
        returncode = int(getattr(cp, "returncode", 1) or 0)
        show = parse_systemctl_show(stdout) if returncode == 0 else {"LoadState": "not-found", "ActiveState": "unknown"}
        cg = cgroup_memory(cgroup_path(ctx.cgroup_root, show.get("ControlGroup", "")), previous_by_unit.get(unit, {}))
        cg.update(
            {
                "unit": unit,
                "active_state": show.get("ActiveState", ""),
                "sub_state": show.get("SubState", ""),
                "load_state": show.get("LoadState", ""),
                "main_pid": int_or_none(show.get("MainPID")),
                "n_restarts": int_or_none(show.get("NRestarts")),
            }
        )
        out[unit] = cg
    return out


def proc_pids(proc_root: Path) -> list[Path]:
    try:
        return [p for p in proc_root.iterdir() if p.name.isdigit()]
    except OSError:
        return []


def status_value(text: str, key: str) -> int | None:
    prefix = key + ":"
    for line in text.splitlines():
        if not line.startswith(prefix):
            continue
        parts = line.split()
        if len(parts) < 2:
            return None
        try:
            return int(parts[1]) * 1024
        except ValueError:
            return None
    return None


def status_int(text: str, key: str) -> int | None:
    prefix = key + ":"
    for line in text.splitlines():
        if not line.startswith(prefix):
            continue
        parts = line.split()
        if len(parts) < 2:
            return None
        try:
            return int(parts[1])
        except ValueError:
            return None
    return None


def smaps_rollup_value(pid_dir: Path, key: str) -> int | None:
    try:
        text = (pid_dir / "smaps_rollup").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    return status_value(text, key)


def process_start_time(pid_dir: Path) -> int | None:
    try:
        stat = (pid_dir / "stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    end = stat.rfind(")")
    if end < 0:
        return None
    parts = stat[end + 2 :].split()
    if len(parts) <= 19:
        return None
    try:
        return int(parts[19])
    except ValueError:
        return None


def process_uptime_sec(proc_root: Path, pid_dir: Path) -> int | None:
    start_ticks = process_start_time(pid_dir)
    if start_ticks is None:
        return None
    try:
        uptime = float((proc_root / "uptime").read_text(encoding="utf-8").split()[0])
        sysconf = getattr(os, "sysconf", None)
        sysconf_names = getattr(os, "sysconf_names", {})
        hz = sysconf(sysconf_names["SC_CLK_TCK"]) if sysconf is not None and "SC_CLK_TCK" in sysconf_names else 100
    except (OSError, ValueError, KeyError):
        return None
    return max(0, int(uptime - (float(start_ticks) / float(hz))))


def proc_info(proc_root: Path, pid_dir: Path) -> dict | None:
    try:
        cmdline = (pid_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
        comm = (pid_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
        status = (pid_dir / "status").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    pid = int(pid_dir.name)
    rss = status_value(status, "VmRSS")
    pss = smaps_rollup_value(pid_dir, "Pss")
    swap = smaps_rollup_value(pid_dir, "Swap")
    threads = status_int(status, "Threads")
    try:
        fd_count = len(list((pid_dir / "fd").iterdir()))
    except OSError:
        fd_count = None
    start_ticks = process_start_time(pid_dir)
    return {
        "pid": pid,
        "comm": comm,
        "cmdline": cmdline,
        "rss_bytes": rss,
        "pss_bytes": pss,
        "swap_bytes": swap,
        "threads": threads,
        "fd_count": fd_count,
        "uptime_sec": process_uptime_sec(proc_root, pid_dir),
        "start_ticks": start_ticks,
        "cmdline_hash": hashlib.sha1((cmdline or comm).encode("utf-8", errors="replace")).hexdigest()[:12],
    }


def process_matches(info: dict, tokens: tuple[str, ...]) -> bool:
    text = f"{info.get('comm', '')} {info.get('cmdline', '')}".lower()
    return any(token.lower() in text for token in tokens)


def collect_process_groups(proc_root: Path) -> dict[str, dict]:
    groups: dict[str, list[dict]] = {name: [] for name in PROCESS_GROUP_RULES}
    for pid_dir in proc_pids(proc_root):
        info = proc_info(proc_root, pid_dir)
        if not info:
            continue
        for name, tokens in PROCESS_GROUP_RULES.items():
            if process_matches(info, tokens):
                groups[name].append(info)
                break

    out: dict[str, dict] = {}
    for name, rows in groups.items():
        pids = sorted(int(row["pid"]) for row in rows)
        generation_source = "|".join(f"{row['pid']}:{row.get('start_ticks')}" for row in sorted(rows, key=lambda item: item["pid"]))
        out[name] = {
            "pids": pids,
            "process_count": len(rows),
            "rss_mb": mb(sum(int(row.get("rss_bytes") or 0) for row in rows)),
            "pss_mb": mb(sum(int(row.get("pss_bytes") or row.get("rss_bytes") or 0) for row in rows)),
            "swap_mb": mb(sum(int(row.get("swap_bytes") or 0) for row in rows)),
            "threads": sum(int(row.get("threads") or 0) for row in rows),
            "fd_count": sum(int(row.get("fd_count") or 0) for row in rows),
            "uptime_sec": min((int(row["uptime_sec"]) for row in rows if row.get("uptime_sec") is not None), default=None),
            "restart_generation": hashlib.sha1(generation_source.encode("utf-8")).hexdigest()[:12] if rows else "",
            "cmdline_hashes": sorted({str(row.get("cmdline_hash") or "") for row in rows if row.get("cmdline_hash")}),
        }
    return out


def history_items(path: Path, now_ts: int, max_window_sec: int = 24 * 3600) -> list[dict]:
    cutoff = now_ts - max_window_sec - 3600
    rows: list[dict] = []
    for item in iter_jsonl(path):
        ts = parse_utc_ts(str(item.get("ts_utc", "") or item.get("generated_at_utc", "") or ""))
        if ts >= cutoff:
            row = dict(item)
            row["_ts"] = ts
            rows.append(row)
    return rows


def nearest_baseline(rows: list[dict], now_ts: int, window_sec: int) -> dict | None:
    target = now_ts - window_sec
    candidates = [row for row in rows if int(row.get("_ts") or 0) <= target]
    if candidates:
        return max(candidates, key=lambda row: int(row.get("_ts") or 0))
    candidates = [row for row in rows if int(row.get("_ts") or 0) < now_ts]
    return min(candidates, key=lambda row: abs(int(row.get("_ts") or 0) - target), default=None)


def nested_number(payload: dict, path: tuple[str, ...]) -> float | None:
    value: object = payload
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def growth_mb(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return round(current - previous, 3)


def process_group_trend(name: str, current: dict, rows: list[dict], now_ts: int) -> dict:
    current_pss = nested_number(current, ("process_groups", name, "pss_mb"))
    current_rss = nested_number(current, ("process_groups", name, "rss_mb"))
    current_gen = str(current.get("process_groups", {}).get(name, {}).get("restart_generation") or "")
    out: dict[str, object] = {}
    for label, sec in (("15m", 900), ("1h", 3600), ("8h", 8 * 3600), ("24h", 24 * 3600)):
        baseline = nearest_baseline(rows, now_ts, sec)
        baseline_gen = str((baseline or {}).get("process_groups", {}).get(name, {}).get("restart_generation") or "")
        if current_gen and baseline_gen and current_gen != baseline_gen:
            pss_growth = None
            rss_growth = None
        else:
            pss_growth = growth_mb(current_pss, nested_number(baseline or {}, ("process_groups", name, "pss_mb")))
            rss_growth = growth_mb(current_rss, nested_number(baseline or {}, ("process_groups", name, "rss_mb")))
        out[f"pss_growth_{label}_mb"] = pss_growth
        out[f"rss_growth_{label}_mb"] = rss_growth
    baseline = nearest_baseline(rows, now_ts, 8 * 3600)
    elapsed_h = None
    if baseline and baseline.get("_ts"):
        elapsed_h = max(1 / 60, float(now_ts - int(baseline["_ts"])) / 3600.0)
    pss_growth_8h = out.get("pss_growth_8h_mb")
    rss_growth_8h = out.get("rss_growth_8h_mb")
    pss_rate = round(float(pss_growth_8h) / elapsed_h, 3) if pss_growth_8h is not None and elapsed_h else None
    rss_rate = round(float(rss_growth_8h) / elapsed_h, 3) if rss_growth_8h is not None and elapsed_h else None
    leak_suspect = (
        pss_growth_8h is not None
        and float(pss_growth_8h) > mb(PROCESS_LEAK_MIN_GROWTH_8H_BYTES)
        and pss_rate is not None
        and pss_rate > mb(PROCESS_LEAK_MIN_RATE_BYTES_PER_HOUR)
    )
    out.update(
        {
            "pss_growth_rate_mb_per_hour": pss_rate,
            "rss_growth_rate_mb_per_hour": rss_rate,
            "leak_suspect": leak_suspect,
            "threshold_basis": "initial report-only leak suspect when 8h PSS growth >256MiB and rate >32MiB/h until 7d baseline exists",
        }
    )
    return out


def subsystem_snapshot(process_groups: dict[str, dict], trends: dict[str, dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for subsystem, members in SUBSYSTEM_MEMBERS.items():
        rss = sum(float(process_groups.get(member, {}).get("rss_mb") or 0.0) for member in members)
        pss = sum(float(process_groups.get(member, {}).get("pss_mb") or 0.0) for member in members)
        swap = sum(float(process_groups.get(member, {}).get("swap_mb") or 0.0) for member in members)
        process_count = sum(int(process_groups.get(member, {}).get("process_count") or 0) for member in members)
        leak = any(bool(trends.get(member, {}).get("leak_suspect")) for member in members)
        out[subsystem] = {
            "members": list(members),
            "rss_mb": round(rss, 3),
            "pss_mb": round(pss, 3),
            "swap_mb": round(swap, 3),
            "process_count": process_count,
            "pss_growth_1h_mb": sum_growth(trends, members, "pss_growth_1h_mb"),
            "pss_growth_8h_mb": sum_growth(trends, members, "pss_growth_8h_mb"),
            "pss_growth_24h_mb": sum_growth(trends, members, "pss_growth_24h_mb"),
            "pss_growth_rate_mb_per_hour": sum_growth(trends, members, "pss_growth_rate_mb_per_hour"),
            "leak_suspect": leak,
            "pressure_status": "observe" if leak else "ok",
        }
    return out


def sum_growth(trends: dict[str, dict], members: tuple[str, ...], key: str) -> float | None:
    values = [trends.get(member, {}).get(key) for member in members]
    numbers = [float(value) for value in values if value is not None]
    if not numbers:
        return None
    return round(sum(numbers), 3)


def runtime_correlation(ctx: ResourceMemoryContext, process_groups: dict[str, dict], now_ts: int) -> dict:
    ytw = read_json_file(ctx.state_base_dir / "youtube_watchdog_stats.json")
    latest_overlay = latest_jsonl(ctx.log_base_dir / "stream1090_report.jsonl")
    latest_play = latest_jsonl(ctx.log_base_dir / "play_history.jsonl")
    latest_watchdog = latest_jsonl(ctx.log_base_dir / "watchdog_state_timeline.jsonl")
    latest_engine = latest_jsonl(ctx.log_base_dir / "stream_engine_events.jsonl")
    restart_reason = read_json_file(ctx.state_base_dir / "restart_reason.json")

    overlay_ts = parse_any_utc_ts(str(latest_overlay.get("ts_utc", "") or ""))
    play_ts = parse_any_utc_ts(str(latest_play.get("ts_utc", "") or latest_play.get("ts_jst", "") or ""))
    watchdog_ts = parse_any_utc_ts(str(latest_watchdog.get("ts_utc", "") or ""))
    engine_ts = parse_any_utc_ts(str(latest_engine.get("ts_utc", "") or ""))
    runtime_snapshot = latest_watchdog.get("runtime_snapshot") if isinstance(latest_watchdog.get("runtime_snapshot"), dict) else {}
    now_playing = latest_watchdog.get("now_playing_state") if isinstance(latest_watchdog.get("now_playing_state"), dict) else {}
    checks = latest_overlay.get("checks") if isinstance(latest_overlay.get("checks"), dict) else {}
    return {
        "stream_session_id": str(runtime_snapshot.get("run_id") or latest_engine.get("run_id") or ""),
        "runtime_generation": str(ytw.get("ffmpeg_generation") or ""),
        "boot_id": read_text(ctx.proc_root / "sys" / "kernel" / "random" / "boot_id"),
        "sample_seq": 0,
        "monotonic_sec": int(time.monotonic()),
        "wall_time_jst": jst_iso_from_ts(now_ts),
        "wall_time_utc": utc_text_from_ts(now_ts),
        "current_runtime_state": {
            "youtube_strict_ok": tri_bool(ytw.get("local_ok")) and (tri_bool(ytw.get("oauth_ok")) or tri_bool(ytw.get("api_ok"))) and tri_bool(ytw.get("public_ok")),
            "same_watch_url_ok": tri_bool(bool(ytw.get("expected_video_id")) and str(ytw.get("expected_video_id")) == str(ytw.get("video_id"))),
            "ffmpeg_alive": int(process_groups.get("ffmpeg", {}).get("process_count") or 0) > 0,
            "ffmpeg_rtmp_connected": ytw.get("ingest_connected") if isinstance(ytw.get("ingest_connected"), bool) else None,
            "overlay_fresh": bool(overlay_ts and now_ts - overlay_ts <= 1800 and latest_overlay.get("judgment") == "report_only_ok"),
            "aircraft_json_fresh": bool(overlay_ts and now_ts - overlay_ts <= 1800 and checks.get("aircraft_json_ok") is True),
            "audio_active": bool(play_ts and now_ts - play_ts <= 900),
            "now_playing_fresh": bool(watchdog_ts and now_ts - watchdog_ts <= 180 and now_playing.get("status") == "playing"),
            "stream_engine_runtime_snapshot_fresh": bool(runtime_snapshot.get("age_sec") is not None and float(runtime_snapshot.get("age_sec") or 999999) <= 180),
        },
        "recent_events": {
            "restart_reason": restart_reason.get("reason") if restart_reason else None,
            "last_restart_at": restart_reason.get("ts_utc") if restart_reason else None,
            "last_browser_reset_at": None,
            "last_ffmpeg_restart_at": latest_engine.get("ts_utc") if latest_engine.get("event_type") in {"ffmpeg_restart_scheduled", "ffmpeg_started"} and engine_ts else None,
            "last_oom_event_at": None,
        },
    }


def tri_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def latest_jsonl(path: Path) -> dict:
    latest: dict = {}
    for item in iter_jsonl(path):
        latest = item
    return latest


def parse_any_utc_ts(text: str) -> int:
    value = parse_utc_ts(text)
    if value:
        return value
    try:
        return int(datetime.fromisoformat(text).astimezone(timezone.utc).timestamp())
    except Exception:
        return 0


def jst_iso_from_ts(ts: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+09:00", time.gmtime(ts + 9 * 3600))


def trend_host(current: dict, rows: list[dict], now_ts: int) -> dict:
    out: dict[str, object] = {}
    for label, sec in (("1h", 3600), ("8h", 8 * 3600), ("24h", 24 * 3600)):
        baseline = nearest_baseline(rows, now_ts, sec)
        out[f"mem_available_delta_{label}_mb"] = growth_mb(
            nested_number(current, ("host_memory", "mem_available_mb")),
            nested_number(baseline or {}, ("host_memory", "mem_available_mb")),
        )
        out[f"swap_used_delta_{label}_mb"] = growth_mb(
            nested_number(current, ("host_memory", "swap_used_mb")),
            nested_number(baseline or {}, ("host_memory", "swap_used_mb")),
        )
    latest_some = nested_number(current, ("memory_pressure", "some_avg300"))
    baseline = nearest_baseline(rows, now_ts, 3600)
    old_some = nested_number(baseline or {}, ("memory_pressure", "some_avg300"))
    trend = "flat"
    if latest_some is not None and old_some is not None:
        if latest_some > old_some:
            trend = "up"
        elif latest_some < old_some:
            trend = "down"
    out["psi_memory_some_avg300_trend"] = trend
    return out


def baseline_coverage_sec(rows: list[dict], now_ts: int) -> int:
    timestamps = [int(row.get("_ts") or 0) for row in rows if row.get("_ts")]
    if not timestamps:
        return 0
    return max(0, now_ts - min(timestamps))


def assess(payload: dict, rows: list[dict]) -> dict:
    host = payload["host_memory"]
    pressure = payload["memory_pressure"]
    vm = payload["vm_activity"]
    runtime = payload["correlation"]["current_runtime_state"]
    subsystems = payload["subsystems"]
    cgroups = payload["cgroups"]
    total_mb = host.get("mem_total_mb")
    low_available_floor_mb = max(1500.0, float(total_mb or 0.0) * 0.10) if total_mb is not None else 1500.0
    mem_low = host.get("mem_available_mb") is not None and float(host["mem_available_mb"]) < low_available_floor_mb
    swap_growth = (
        payload["trends"]["host"].get("swap_used_delta_1h_mb") is not None
        and float(payload["trends"]["host"]["swap_used_delta_1h_mb"]) > mb(HOST_SWAP_GROWTH_1H_BYTES)
        and float(vm.get("pswpout_delta_per_min") or 0.0) > PSWP_ACTIVITY_PER_MIN_FLOOR
    )
    psi_some = float(pressure.get("some_avg300") or 0.0) > PSI_AVG300_FLOOR
    psi_full = float(pressure.get("full_avg300") or 0.0) > PSI_AVG300_FLOOR
    oom_delta = int(vm.get("oom_kill_count_delta") or 0)
    for cg in cgroups.values():
        delta = cg.get("memory_events", {}).get("delta", {}) if isinstance(cg.get("memory_events"), dict) else {}
        oom_delta += int(delta.get("oom", 0) or 0) + int(delta.get("oom_kill", 0) or 0)

    rendering_degraded = bool(subsystems["rendering"]["leak_suspect"]) and (
        runtime.get("overlay_fresh") is False or runtime.get("aircraft_json_fresh") is False
    )
    delivery_degraded = (
        bool(subsystems["delivery"]["leak_suspect"]) or bool(swap_growth) or (mem_low and psi_some)
    ) and (runtime.get("ffmpeg_alive") is False or runtime.get("ffmpeg_rtmp_connected") is False)
    runtime_degraded = rendering_degraded or delivery_degraded or runtime.get("audio_active") is False
    coverage_sec = baseline_coverage_sec(rows, parse_utc_ts(payload["ts_utc"]))

    reasons: list[str] = []
    status = "ok"
    if any(item.get("leak_suspect") for item in subsystems.values()):
        status = "observe"
        reasons.append("subsystem PSS growth crossed initial leak-suspect floor")
    if mem_low:
        status = max_status(status, "warn")
        reasons.append(f"MemAvailable below max(1500MiB,total*10%) floor ({low_available_floor_mb:.1f}MiB)")
    if swap_growth or psi_some:
        status = max_status(status, "warn")
        reasons.append("swap growth or PSI memory pressure observed")
    if (mem_low and (swap_growth or psi_full)) or rendering_degraded or delivery_degraded:
        status = max_status(status, "degraded")
        reasons.append("memory pressure correlates with runtime degradation candidate")
    if oom_delta > 0:
        status = "critical"
        reasons.append("OOM event delta observed in vmstat or cgroup memory.events")

    baseline_ready = coverage_sec >= BASELINE_READY_SEC
    if not baseline_ready and status in {"warn", "degraded"} and oom_delta == 0:
        status = "observe"
        reasons.append("7d baseline not ready; keeping non-OOM memory findings report-only")

    supporting = status in {"degraded", "critical"} and bool(runtime_degraded or oom_delta > 0)
    primary_suspect = None
    if rendering_degraded:
        primary_suspect = "rendering"
    elif delivery_degraded:
        primary_suspect = "delivery"
    elif oom_delta > 0:
        primary_suspect = "oom"
    elif any(item.get("leak_suspect") for item in subsystems.values()):
        primary_suspect = next(name for name, item in subsystems.items() if item.get("leak_suspect"))

    return {
        "status": status,
        "memory_is_sli": False,
        "restart_allowed_by_memory_alone": False,
        "supporting_evidence_for_recovery": supporting,
        "primary_suspect": primary_suspect,
        "baseline_ready": baseline_ready,
        "baseline_coverage_sec": coverage_sec,
        "initial_thresholds": {
            "mem_available_low": "mem_available_mb < max(1500MiB, total_memory * 0.10)",
            "swap_growth": "swap_used_delta_1h_mb > 256MiB and pswpout_delta_per_min > 0",
            "memory_pressure": "memory.some_avg300 > 0",
            "leak_suspect": "8h PSS growth > 256MiB and > 32MiB/hour until 7d baseline is available",
        },
        "reason": "; ".join(reasons) if reasons else "No sustained growth, swap usage, PSI pressure, OOM event, or runtime correlation observed.",
        "prohibited_actions": [
            "YouTube broadcast replacement",
            "same watch URL reset",
            "stream-wide restart",
            "destructive recovery",
        ],
    }


def max_status(current: str, candidate: str) -> str:
    order = {"ok": 0, "observe": 1, "warn": 2, "degraded": 3, "critical": 4}
    return candidate if order[candidate] > order[current] else current


def resource_memory_payload(ctx: ResourceMemoryContext, *, now_ts: int | None = None) -> dict:
    now_ts = int(time.time() if now_ts is None else now_ts)
    previous = latest_snapshot(ctx.resource_memory_file)
    prev_ts = parse_utc_ts(str(previous.get("ts_utc", "") or ""))
    elapsed_sec = float(now_ts - prev_ts) if prev_ts else None
    history = history_items(ctx.resource_memory_events_file, now_ts)

    meminfo = read_key_value_bytes(ctx.proc_root / "meminfo", kb_units=True)
    vmstat = read_key_value_ints(ctx.proc_root / "vmstat")
    process_groups = collect_process_groups(ctx.proc_root)
    correlation = runtime_correlation(ctx, process_groups, now_ts)
    current_base = {
        "host_memory": host_memory(meminfo),
        "memory_pressure": read_psi(ctx.proc_root / "pressure" / "memory"),
        "process_groups": process_groups,
    }
    group_trends = {name: process_group_trend(name, current_base, history, now_ts) for name in PROCESS_GROUP_RULES}
    subsystems = subsystem_snapshot(process_groups, group_trends)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "ts_utc": utc_text_from_ts(now_ts),
        "ts_jst": jst_iso_from_ts(now_ts),
        "boot_id": read_text(ctx.proc_root / "sys" / "kernel" / "random" / "boot_id"),
        "stream_session_id": correlation.get("stream_session_id", ""),
        "runtime_generation": correlation.get("runtime_generation", ""),
        "sample_seq": int(previous.get("sample_seq") or 0) + 1,
        "monotonic_sec": correlation.get("monotonic_sec"),
        "wall_time_jst": correlation.get("wall_time_jst"),
        "wall_time_utc": correlation.get("wall_time_utc"),
        "host_memory": current_base["host_memory"],
        "memory_pressure": current_base["memory_pressure"],
        "vm_activity": vm_activity(vmstat, previous, elapsed_sec=elapsed_sec),
        "cgroups": service_cgroups(ctx, previous),
        "process_groups": process_groups,
        "subsystems": subsystems,
        "trends": {
            "windows": ["15m", "1h", "8h", "24h"],
            "host": trend_host(current_base, history, now_ts),
            "process_groups": group_trends,
            "subsystems": {
                name: {
                    "pss_growth_rate_mb_per_hour": item.get("pss_growth_rate_mb_per_hour"),
                    "leak_suspect": item.get("leak_suspect"),
                }
                for name, item in subsystems.items()
            },
        },
        "correlation": correlation,
        "current_runtime_state": correlation.get("current_runtime_state", {}),
        "recent_events": correlation.get("recent_events", {}),
        "vmstat_raw": vmstat,
        "policy": {
            "positioning": "diagnostic_metric_not_primary_sli",
            "baseline_collection_sec": BASELINE_READY_SEC,
            "recovery_rule": "memory alone never triggers destructive recovery; it can only support subsystem recovery when runtime degradation correlates",
        },
    }
    payload["correlation"]["sample_seq"] = payload["sample_seq"]
    payload["assessment"] = assess(payload, history)
    return payload


def public_snapshot(payload: dict) -> dict:
    out = json.loads(json.dumps(payload))
    out.pop("vmstat_raw", None)
    for cg in out.get("cgroups", {}).values():
        if isinstance(cg, dict):
            cg.pop("_events_raw", None)
            cg.pop("_stat_raw", None)
    if isinstance(out.get("host_memory"), dict):
        out["host_memory"].pop("_bytes", None)
    return out


def save_resource_memory(ctx: ResourceMemoryContext, payload: dict) -> None:
    atomic_write_json_file(ctx.resource_memory_file, payload, indent=2)
    atomic_write_json_file(ctx.resource_memory_assessment_file, payload.get("assessment", {}), indent=2)
    append_jsonl(ctx.resource_memory_events_file, public_snapshot(payload))


def fmt(value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def resource_memory(ctx: ResourceMemoryContext, *, json_output: bool = False, record: bool = True) -> int:
    payload = resource_memory_payload(ctx)
    snapshot = public_snapshot(payload)
    if record:
        save_resource_memory(ctx, payload)
    if json_output:
        print(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")))
        return 0
    assessment = snapshot["assessment"]
    host = snapshot["host_memory"]
    print(
        "[resource-memory] "
        f"ts={snapshot['ts_jst']} "
        f"status={assessment['status']} "
        f"baseline_ready={assessment['baseline_ready']} "
        f"available_mb={fmt(host.get('mem_available_mb'))} "
        f"swap_used_mb={fmt(host.get('swap_used_mb'))} "
        f"psi_some300={fmt(snapshot['memory_pressure'].get('some_avg300'))} "
        f"suspect={assessment.get('primary_suspect') or '-'}"
    )
    if record:
        print(
            "[resource-memory] "
            f"snapshot={ctx.resource_memory_file} "
            f"history={ctx.resource_memory_events_file} "
            f"assessment={ctx.resource_memory_assessment_file}"
        )
    return 0
