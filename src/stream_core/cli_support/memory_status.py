from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    from stream_core.common.json_io import append_jsonl, atomic_write_json_file
    from stream_core.common.timeutil import jst_text_or_unknown, utc_text_from_ts
except ModuleNotFoundError:
    from common.json_io import append_jsonl, atomic_write_json_file
    from common.timeutil import jst_text_or_unknown, utc_text_from_ts


MIB = 1024 * 1024
GIB = 1024 * MIB
MEMORY_STATUS_POLICY_VERSION = 5


SERVICE_ROLES: dict[str, str] = {
    "adsb-streamnew-youtube-stream.service": "stream_pipeline",
    "adsb-streamnew-auto-dj.service": "music",
    "adsb-streamnew-watchdog.service": "local_watchdog",
    "adsb-streamnew-youtube-monitor.service": "youtube_monitor",
    "adsb-streamnew-youtube-video-resolver.service": "youtube_lifecycle",
    "adsb-streamnew-fast-recovery.service": "fast_recovery",
    "adsb-streamnew-stream1090-report.service": "visual_report",
    "adsb-streamnew-upstream-report.service": "upstream_report",
    "adsb-streamnew-subsystems-status.service": "subsystem_shadow",
    "adsb-streamnew-recovery-orchestrator.service": "recovery_shadow",
    "adsb-streamnew-notify.service": "notification",
    "adsb-streamnew-youtube-api-cost-open-day-report.service": "youtube_api_quota",
    "adsb-streamnew-youtube-api-cost-report.service": "youtube_api_quota",
}


ANON_THRESHOLDS: dict[str, tuple[int, int]] = {
    "adsb-streamnew-youtube-stream.service": (1 * GIB, 2 * GIB),
    "adsb-streamnew-auto-dj.service": (512 * MIB, 1 * GIB),
}
DEFAULT_ANON_THRESHOLD = (512 * MIB, 1 * GIB)
LONG_RUNNING_UNITS = {
    "adsb-streamnew-youtube-stream.service",
    "adsb-streamnew-auto-dj.service",
}
ACTIVE_RUNTIME_STATES = {"active", "activating", "reloading"}
ONESHOT_PEAK_WARN_BYTES = 1 * GIB
ONESHOT_PEAK_CRITICAL_BYTES = 4 * GIB
HOST_AVAILABLE_WARN_BYTES = 2 * GIB
HOST_AVAILABLE_CRITICAL_BYTES = 1 * GIB
SWAP_USED_WARN_BYTES = 512 * MIB
SWAP_USED_CRITICAL_BYTES = 1 * GIB
HOST_NON_RECLAIMABLE_WARN_BYTES = 10 * GIB
HOST_NON_RECLAIMABLE_CRITICAL_BYTES = 12 * GIB
HOST_AVAILABLE_ADEQUACY_WARN_BYTES = 4 * GIB
HOST_AVAILABLE_ADEQUACY_CRITICAL_BYTES = 2 * GIB


@dataclass(frozen=True)
class MemoryStatusContext:
    memory_status_file: Path
    memory_status_events_file: Path
    service_units: tuple[str, ...]
    run_systemctl_readonly: Callable[[list[str], bool], subprocess.CompletedProcess[str]]
    proc_meminfo_path: Path = Path("/proc/meminfo")
    cgroup_root: Path = Path("/sys/fs/cgroup")


def bytes_or_none(raw: str | None) -> int | None:
    if raw is None:
        return None
    value = raw.strip()
    if not value or value in {"[not set]", "n/a", "infinity"}:
        return None
    try:
        number = int(value)
    except ValueError:
        return None
    if number < 0 or number >= 2**60:
        return None
    return number


def parse_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except ValueError:
        return None


def ratio_pct(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return round((float(numerator) / float(denominator)) * 100.0, 3)


def host_non_reclaimable_estimate(meminfo: dict[str, int]) -> dict:
    total = meminfo.get("MemTotal")
    free = meminfo.get("MemFree")
    buffers = meminfo.get("Buffers") or 0
    cached = meminfo.get("Cached") or 0
    sreclaimable = meminfo.get("SReclaimable") or 0
    shmem = meminfo.get("Shmem") or 0
    if total is None or free is None:
        return {
            "host_used_reference_bytes": None,
            "reclaimable_estimate_bytes": None,
            "non_reclaimable_estimate_bytes": None,
            "non_reclaimable_reference_pct": None,
            "formula": "unavailable_without_MemTotal_and_MemFree",
        }
    used = max(0, total - free)
    reclaimable = max(0, buffers + cached + sreclaimable - shmem)
    non_reclaimable = max(0, used - buffers - cached - sreclaimable + shmem)
    return {
        "host_used_reference_bytes": used,
        "reclaimable_estimate_bytes": reclaimable,
        "non_reclaimable_estimate_bytes": non_reclaimable,
        "non_reclaimable_reference_pct": ratio_pct(non_reclaimable, total),
        "formula": "MemTotal - MemFree - Buffers - Cached - SReclaimable + Shmem",
    }


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
        key = parts[0].rstrip(":")
        try:
            values[key] = int(parts[1]) * multiplier
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


def parse_systemctl_show(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def host_memory_status(meminfo: dict[str, int]) -> dict:
    total = meminfo.get("MemTotal")
    available = meminfo.get("MemAvailable")
    free = meminfo.get("MemFree")
    swap_total = meminfo.get("SwapTotal")
    swap_free = meminfo.get("SwapFree")
    non_reclaimable = host_non_reclaimable_estimate(meminfo)
    swap_used = None
    if swap_total is not None and swap_free is not None:
        swap_used = max(0, swap_total - swap_free)
    severity = "ok"
    reasons: list[str] = []
    if available is None:
        severity = "unknown"
        reasons.append("MemAvailable missing")
    elif available < HOST_AVAILABLE_CRITICAL_BYTES:
        severity = "critical"
        reasons.append("host MemAvailable below 1GiB absolute floor")
    elif available < HOST_AVAILABLE_WARN_BYTES:
        severity = "warn"
        reasons.append("host MemAvailable below 2GiB absolute watch floor")
    if swap_used is not None:
        if swap_used >= SWAP_USED_CRITICAL_BYTES:
            severity = max_severity(severity, "critical")
            reasons.append("swap used above 1GiB absolute floor")
        elif swap_used >= SWAP_USED_WARN_BYTES:
            severity = max_severity(severity, "warn")
            reasons.append("swap used above 512MiB watch floor")
    return {
        "severity": severity,
        "reasons": reasons,
        "evaluation_basis": "absolute_bytes_primary; host_percentages_reference_only",
        "mem_total_bytes": total,
        "mem_free_bytes": free,
        "mem_available_bytes": available,
        "mem_available_reference_pct": ratio_pct(available, total),
        "mem_used_reference_pct": round(100.0 - ratio_pct(available, total), 3)
        if ratio_pct(available, total) is not None
        else None,
        "host_used_reference_bytes": non_reclaimable["host_used_reference_bytes"],
        "reclaimable_estimate_bytes": non_reclaimable["reclaimable_estimate_bytes"],
        "non_reclaimable_estimate_bytes": non_reclaimable["non_reclaimable_estimate_bytes"],
        "non_reclaimable_reference_pct": non_reclaimable["non_reclaimable_reference_pct"],
        "non_reclaimable_budget_bytes": HOST_NON_RECLAIMABLE_WARN_BYTES,
        "non_reclaimable_critical_bytes": HOST_NON_RECLAIMABLE_CRITICAL_BYTES,
        "non_reclaimable_formula": non_reclaimable["formula"],
        "swap_total_bytes": swap_total,
        "swap_used_bytes": swap_used,
        "swap_used_reference_pct": ratio_pct(swap_used, swap_total),
        "cached_bytes": meminfo.get("Cached"),
        "buffers_bytes": meminfo.get("Buffers"),
        "sreclaimable_bytes": meminfo.get("SReclaimable"),
        "shmem_bytes": meminfo.get("Shmem"),
    }


def cgroup_path(root: Path, control_group: str) -> Path | None:
    if not control_group or control_group == "/":
        return None
    return root / control_group.lstrip("/")


def cgroup_memory(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {
            "available": False,
            "path": str(path) if path is not None else "",
            "current_bytes": None,
            "peak_bytes": None,
            "stat": {},
            "events": {},
        }
    try:
        current = (
            bytes_or_none((path / "memory.current").read_text(encoding="utf-8").strip())
            if (path / "memory.current").exists()
            else None
        )
        peak = (
            bytes_or_none((path / "memory.peak").read_text(encoding="utf-8").strip())
            if (path / "memory.peak").exists()
            else None
        )
    except OSError:
        current = None
        peak = None
    return {
        "available": True,
        "path": str(path),
        "current_bytes": current,
        "peak_bytes": peak,
        "stat": read_key_value_bytes(path / "memory.stat"),
        "events": read_key_value_ints(path / "memory.events"),
    }


def max_severity(current: str, candidate: str) -> str:
    order = {"unknown": 0, "ok": 1, "warn": 2, "critical": 3}
    return candidate if order.get(candidate, 0) > order.get(current, 0) else current


def dominant_category(*, anon: int | None, file_bytes: int | None, kernel: int | None, current: int | None, active: bool) -> str:
    anon = anon or 0
    file_bytes = file_bytes or 0
    kernel = kernel or 0
    if not active and current is None:
        return "systemd_peak_only"
    if file_bytes >= max(64 * MIB, anon * 3):
        return "file_cache_reclaimable"
    if anon >= max(64 * MIB, file_bytes):
        return "anonymous_working_set"
    if kernel >= max(64 * MIB, anon, file_bytes):
        return "kernel_overhead"
    return "mixed_or_small"


def one_shot_peak_classification(unit: str, *, peak: int | None, runtime_active: bool, cgroup_peak: int | None) -> dict:
    if unit in LONG_RUNNING_UNITS or peak is None:
        return {
            "severity": "ok",
            "reasons": [],
            "scope": "not_applicable" if unit in LONG_RUNNING_UNITS else "not_available",
            "source": "none",
            "contributes_to_current_severity": False,
        }

    severity = "ok"
    reasons: list[str] = []
    if peak >= ONESHOT_PEAK_CRITICAL_BYTES:
        severity = "critical"
        reasons.append("oneshot peak above 4GiB absolute ceiling")
    elif peak >= ONESHOT_PEAK_WARN_BYTES:
        severity = "warn"
        reasons.append("oneshot peak above 1GiB watch floor")

    scope = "active_oneshot_run" if runtime_active else "systemd_memory_peak_history"
    return {
        "severity": severity,
        "reasons": reasons,
        "scope": scope,
        "source": "cgroup_memory_peak" if cgroup_peak is not None else "systemd_memory_peak",
        "contributes_to_current_severity": False,
    }


def service_kernel_unreclaimable_estimate(stat: dict[str, int]) -> tuple[int | None, str]:
    kernel = stat.get("kernel")
    slab_reclaimable = stat.get("slab_reclaimable") or 0
    if kernel is not None:
        return (
            max(0, kernel - slab_reclaimable),
            "anon + max(kernel - slab_reclaimable, 0); cgroup v2 kernel normally includes slab",
        )
    fallback_keys = ("kernel_stack", "pagetables", "percpu", "sock", "slab_unreclaimable")
    if any(key in stat for key in fallback_keys):
        return (
            sum(stat.get(key) or 0 for key in fallback_keys),
            "anon + kernel_stack + pagetables + percpu + sock + slab_unreclaimable fallback",
        )
    return None, "unavailable_without_cgroup_kernel_or_fallback_fields"


def classify_service(unit: str, show: dict[str, str], cg: dict, host_total_bytes: int | None) -> dict:
    active = show.get("ActiveState") in ACTIVE_RUNTIME_STATES
    memory_current = bytes_or_none(show.get("MemoryCurrent"))
    memory_peak = bytes_or_none(show.get("MemoryPeak"))
    cgroup_current = cg.get("current_bytes")
    cgroup_peak = cg.get("peak_bytes")
    current = cgroup_current if cgroup_current is not None else memory_current
    peak = cgroup_peak if cgroup_peak is not None else memory_peak
    stat = cg.get("stat") if isinstance(cg.get("stat"), dict) else {}
    events = cg.get("events") if isinstance(cg.get("events"), dict) else {}
    anon = stat.get("anon")
    file_bytes = stat.get("file")
    inactive_file = stat.get("inactive_file")
    active_file = stat.get("active_file")
    kernel = stat.get("kernel")
    slab = stat.get("slab")
    slab_reclaimable = stat.get("slab_reclaimable")
    slab_unreclaimable = stat.get("slab_unreclaimable")
    kernel_unreclaimable, service_non_reclaimable_basis = service_kernel_unreclaimable_estimate(stat)
    service_non_reclaimable = None
    if anon is not None or kernel_unreclaimable is not None:
        service_non_reclaimable = (anon or 0) + (kernel_unreclaimable or 0)
    reclaimable = inactive_file if inactive_file is not None else file_bytes
    unclassified = None
    if current is not None:
        unclassified = max(0, current - sum(v or 0 for v in (anon, file_bytes, kernel)))

    severity = "ok"
    reasons: list[str] = []
    if show.get("LoadState") not in {None, "", "loaded"}:
        severity = "unknown"
        reasons.append(f"unit LoadState={show.get('LoadState')}")
    if events.get("oom_kill", 0) > 0 or events.get("oom", 0) > 0:
        severity = max_severity(severity, "critical")
        reasons.append("cgroup memory.events oom/oom_kill incremented")
    if events.get("max", 0) > 0:
        severity = max_severity(severity, "critical")
        reasons.append("cgroup memory.events max incremented")
    if events.get("high", 0) > 0:
        severity = max_severity(severity, "warn")
        reasons.append("cgroup memory.events high incremented")

    warn_anon, critical_anon = ANON_THRESHOLDS.get(unit, DEFAULT_ANON_THRESHOLD)
    if unit in LONG_RUNNING_UNITS and anon is not None:
        if anon >= critical_anon:
            severity = max_severity(severity, "critical")
            reasons.append("anonymous memory above critical absolute threshold")
        elif anon >= warn_anon:
            severity = max_severity(severity, "warn")
            reasons.append("anonymous memory above watch absolute threshold")

    peak_classification = one_shot_peak_classification(
        unit,
        peak=peak,
        runtime_active=active,
        cgroup_peak=cgroup_peak,
    )
    if peak_classification["severity"] != "ok":
        reasons.append("oneshot peak tracked as peak guardrail; excluded from current severity")

    if not reasons and file_bytes and anon is not None and file_bytes >= max(64 * MIB, anon * 3):
        reasons.append("file cache dominant; do not classify as leak without anon/events pressure")

    return {
        "unit": unit,
        "role": SERVICE_ROLES.get(unit, "other"),
        "active_state": show.get("ActiveState", ""),
        "sub_state": show.get("SubState", ""),
        "load_state": show.get("LoadState", ""),
        "control_group": show.get("ControlGroup", ""),
        "tasks_current": parse_int(show.get("TasksCurrent")),
        "n_restarts": parse_int(show.get("NRestarts")),
        "exec_main_status": parse_int(show.get("ExecMainStatus")),
        "memory_current_bytes": current,
        "memory_peak_bytes": peak,
        "host_total_reference_pct": ratio_pct(current, host_total_bytes),
        "memory_breakdown": {
            "anonymous_bytes": anon,
            "file_cache_bytes": file_bytes,
            "inactive_file_bytes": inactive_file,
            "active_file_bytes": active_file,
            "kernel_bytes": kernel,
            "slab_bytes": slab,
            "slab_reclaimable_bytes": slab_reclaimable,
            "slab_unreclaimable_bytes": slab_unreclaimable,
            "kernel_unreclaimable_estimate_bytes": kernel_unreclaimable,
            "non_reclaimable_estimate_bytes": service_non_reclaimable,
            "non_reclaimable_estimate_basis": service_non_reclaimable_basis,
            "reclaimable_file_cache_bytes": reclaimable,
            "unclassified_bytes": unclassified,
            "dominant_category": dominant_category(
                anon=anon,
                file_bytes=file_bytes,
                kernel=kernel,
                current=current,
                active=active,
            ),
        },
        "memory_events": {
            "low": events.get("low", 0),
            "high": events.get("high", 0),
            "max": events.get("max", 0),
            "oom": events.get("oom", 0),
            "oom_kill": events.get("oom_kill", 0),
            "oom_group_kill": events.get("oom_group_kill", 0),
            "sock_throttled": events.get("sock_throttled", 0),
        },
        "classification": {
            "severity": severity,
            "reasons": reasons,
            "evaluation_basis": "current cgroup/host pressure primary; inactive systemd MemoryPeak is historical",
        },
        "peak_classification": peak_classification,
    }


def operational_adequacy_status(host: dict, services: list[dict]) -> dict:
    severity = "ok"
    reasons: list[str] = []

    host_non_reclaimable = host.get("non_reclaimable_estimate_bytes")
    if host_non_reclaimable is None:
        reasons.append("host non-reclaimable estimate unavailable; 10GiB budget not evaluated")
    elif host_non_reclaimable >= HOST_NON_RECLAIMABLE_CRITICAL_BYTES:
        severity = max_severity(severity, "critical")
        reasons.append("host non-reclaimable estimate above 12GiB operational ceiling")
    elif host_non_reclaimable > HOST_NON_RECLAIMABLE_WARN_BYTES:
        severity = max_severity(severity, "warn")
        reasons.append("host non-reclaimable estimate above 10GiB operational budget")

    available = host.get("mem_available_bytes")
    if available is not None:
        if available < HOST_AVAILABLE_ADEQUACY_CRITICAL_BYTES:
            severity = max_severity(severity, "critical")
            reasons.append("host MemAvailable below 2GiB operational adequacy floor")
        elif available < HOST_AVAILABLE_ADEQUACY_WARN_BYTES:
            severity = max_severity(severity, "warn")
            reasons.append("host MemAvailable below 4GiB operational adequacy watch floor")

    swap_used = host.get("swap_used_bytes")
    if swap_used is not None:
        if swap_used >= SWAP_USED_CRITICAL_BYTES:
            severity = max_severity(severity, "critical")
            reasons.append("swap used above 1GiB operational ceiling")
        elif swap_used >= SWAP_USED_WARN_BYTES:
            severity = max_severity(severity, "warn")
            reasons.append("swap used above 512MiB operational watch floor")

    for item in services:
        unit = str(item.get("unit") or "")
        breakdown = item.get("memory_breakdown") or {}
        events = item.get("memory_events") or {}
        anon = breakdown.get("anonymous_bytes")
        warn_anon, critical_anon = ANON_THRESHOLDS.get(unit, DEFAULT_ANON_THRESHOLD)
        if unit in LONG_RUNNING_UNITS and anon is not None:
            if anon >= critical_anon:
                severity = max_severity(severity, "critical")
                reasons.append(f"{unit} anonymous memory above operational critical threshold")
            elif anon >= warn_anon:
                severity = max_severity(severity, "warn")
                reasons.append(f"{unit} anonymous memory above operational watch threshold")

        if int(events.get("oom_kill") or 0) > 0 or int(events.get("oom") or 0) > 0:
            severity = max_severity(severity, "critical")
            reasons.append(f"{unit} cgroup memory.events oom/oom_kill incremented")
        if int(events.get("max") or 0) > 0:
            severity = max_severity(severity, "critical")
            reasons.append(f"{unit} cgroup memory.events max incremented")
        if int(events.get("high") or 0) > 0:
            severity = max_severity(severity, "warn")
            reasons.append(f"{unit} cgroup memory.events high incremented")

        peak_classification = item.get("peak_classification") or {}
        if peak_classification.get("scope") == "active_oneshot_run":
            peak_severity = str(peak_classification.get("severity") or "ok")
            if peak_severity == "critical":
                severity = max_severity(severity, "critical")
                reasons.append(f"{unit} active one-shot peak above operational critical threshold")
            elif peak_severity == "warn":
                severity = max_severity(severity, "warn")
                reasons.append(f"{unit} active one-shot peak above operational watch threshold")

    return {
        "severity": severity,
        "warn": severity == "warn",
        "critical": severity == "critical",
        "budget_violation": severity in {"warn", "critical"},
        "reasons": reasons,
        "host_non_reclaimable_budget_bytes": HOST_NON_RECLAIMABLE_WARN_BYTES,
        "host_non_reclaimable_critical_bytes": HOST_NON_RECLAIMABLE_CRITICAL_BYTES,
        "host_mem_available_watch_floor_bytes": HOST_AVAILABLE_ADEQUACY_WARN_BYTES,
        "host_mem_available_critical_floor_bytes": HOST_AVAILABLE_ADEQUACY_CRITICAL_BYTES,
        "contributes_to_current_incident": False,
        "evaluation_basis": "operational adequacy guardrail; not primary availability and not htop used",
    }


def systemctl_show(ctx: MemoryStatusContext, unit: str) -> dict[str, str]:
    properties = [
        "LoadState",
        "ActiveState",
        "SubState",
        "ControlGroup",
        "MemoryCurrent",
        "MemoryPeak",
        "MemorySwapCurrent",
        "MemorySwapPeak",
        "TasksCurrent",
        "NRestarts",
        "ExecMainCode",
        "ExecMainStatus",
    ]
    cp = ctx.run_systemctl_readonly(
        ["show", unit, *[f"--property={prop}" for prop in properties], "--no-pager"],
        False,
    )
    if cp.returncode != 0:
        return {"LoadState": "not-found", "ActiveState": "unknown", "SubState": "unknown", "error": cp.stderr.strip()}
    return parse_systemctl_show(cp.stdout or "")


def memory_status_payload(ctx: MemoryStatusContext, *, now_ts: int | None = None) -> dict:
    now_ts = int(time.time() if now_ts is None else now_ts)
    meminfo = read_key_value_bytes(ctx.proc_meminfo_path, kb_units=True)
    host = host_memory_status(meminfo)
    services: list[dict] = []
    for unit in ctx.service_units:
        show = systemctl_show(ctx, unit)
        cg = cgroup_memory(cgroup_path(ctx.cgroup_root, show.get("ControlGroup", "")))
        services.append(classify_service(unit, show, cg, host.get("mem_total_bytes")))

    severity = host["severity"]
    peak_guardrail_severity = "ok"
    active_oneshot_peak_severity = "ok"
    systemd_peak_history_severity = "ok"
    for item in services:
        severity = max_severity(severity, item["classification"]["severity"])
        peak_classification = item.get("peak_classification") or {}
        peak_guardrail_severity = max_severity(
            peak_guardrail_severity,
            str(peak_classification.get("severity") or "ok"),
        )
        if peak_classification.get("scope") == "systemd_memory_peak_history":
            systemd_peak_history_severity = max_severity(
                systemd_peak_history_severity,
                str(peak_classification.get("severity") or "ok"),
            )
        if peak_classification.get("scope") == "active_oneshot_run":
            active_oneshot_peak_severity = max_severity(
                active_oneshot_peak_severity,
                str(peak_classification.get("severity") or "ok"),
            )

    operational_adequacy = operational_adequacy_status(host, services)
    current_consumers = sorted(
        (
            {
                "unit": item["unit"],
                "role": item["role"],
                "memory_current_bytes": item["memory_current_bytes"],
                "anonymous_bytes": item["memory_breakdown"]["anonymous_bytes"],
                "file_cache_bytes": item["memory_breakdown"]["file_cache_bytes"],
                "non_reclaimable_estimate_bytes": item["memory_breakdown"]["non_reclaimable_estimate_bytes"],
                "dominant_category": item["memory_breakdown"]["dominant_category"],
                "severity": item["classification"]["severity"],
                "peak_severity": (item.get("peak_classification") or {}).get("severity", "ok"),
                "peak_scope": (item.get("peak_classification") or {}).get("scope", "not_available"),
            }
            for item in services
            if item.get("memory_current_bytes") is not None
        ),
        key=lambda item: int(item["memory_current_bytes"] or 0),
        reverse=True,
    )
    non_reclaimable_consumers = sorted(
        (
            {
                "unit": item["unit"],
                "role": item["role"],
                "memory_current_bytes": item["memory_current_bytes"],
                "non_reclaimable_estimate_bytes": item["memory_breakdown"]["non_reclaimable_estimate_bytes"],
                "anonymous_bytes": item["memory_breakdown"]["anonymous_bytes"],
                "kernel_unreclaimable_estimate_bytes": item["memory_breakdown"]["kernel_unreclaimable_estimate_bytes"],
                "dominant_category": item["memory_breakdown"]["dominant_category"],
                "severity": item["classification"]["severity"],
            }
            for item in services
            if item["memory_breakdown"].get("non_reclaimable_estimate_bytes") is not None
        ),
        key=lambda item: int(item["non_reclaimable_estimate_bytes"] or 0),
        reverse=True,
    )
    peak_consumers = sorted(
        (
            {
                "unit": item["unit"],
                "role": item["role"],
                "memory_peak_bytes": item["memory_peak_bytes"],
                "dominant_category": item["memory_breakdown"]["dominant_category"],
                "severity": (item.get("peak_classification") or {}).get("severity", item["classification"]["severity"]),
                "current_severity": item["classification"]["severity"],
                "peak_scope": (item.get("peak_classification") or {}).get("scope", "not_available"),
                "contributes_to_current_severity": (item.get("peak_classification") or {}).get(
                    "contributes_to_current_severity",
                    False,
                ),
            }
            for item in services
            if item.get("memory_peak_bytes") is not None
        ),
        key=lambda item: int(item["memory_peak_bytes"] or 0),
        reverse=True,
    )

    return {
        "schema_version": 3,
        "classification_policy_version": MEMORY_STATUS_POLICY_VERSION,
        "generated_at_utc": utc_text_from_ts(now_ts),
        "generated_at_jst": jst_text_or_unknown(now_ts),
        "source": "stream-new memory-status",
        "metric_classification": "guardrail",
        "evaluation_policy": {
            "primary_basis": "absolute bytes, cgroup anon/file split, memory.events, and per-service peaks",
            "current_severity_rule": "overall.severity uses host pressure, long-running anon, and memory.events only",
            "oneshot_peak_rule": "active and inactive oneshot peaks are retained as peak guardrail fields but do not raise current severity",
            "historical_peak_rule": "inactive systemd MemoryPeak is retained as peak history but does not raise current severity",
            "reference_only": "host/system percentage fields are included only for operator orientation",
            "page_cache_rule": "file/inactive_file growth without anon/events pressure is page cache, not a leak",
            "non_reclaimable_budget_rule": "10GiB budget applies to host non-reclaimable estimate, not htop used or file cache",
            "operational_adequacy_rule": "operational_adequacy detects structurally heavy operation separately from current incident",
            "availability_rule": "memory status is not mixed into primary availability percentage",
        },
        "overall": {
            "severity": severity,
            "current_incident": severity == "critical",
            "warn": severity == "warn",
            "operational_adequacy_severity": operational_adequacy["severity"],
            "operational_adequacy_warn": operational_adequacy["severity"] == "warn",
            "operational_adequacy_critical": operational_adequacy["severity"] == "critical",
            "peak_guardrail_severity": peak_guardrail_severity,
            "active_oneshot_peak_severity": active_oneshot_peak_severity,
            "systemd_peak_history_severity": systemd_peak_history_severity,
            "oneshot_peak_warn": peak_guardrail_severity == "warn",
            "historical_peak_warn": systemd_peak_history_severity == "warn",
        },
        "operational_adequacy": operational_adequacy,
        "host": host,
        "services": services,
        "top_current_consumers": current_consumers[:5],
        "top_non_reclaimable_consumers": non_reclaimable_consumers[:5],
        "top_peak_consumers": peak_consumers[:5],
    }


def save_memory_status(ctx: MemoryStatusContext, payload: dict) -> None:
    atomic_write_json_file(ctx.memory_status_file, payload, indent=2)
    append_jsonl(
        ctx.memory_status_events_file,
        {
            "ts_utc": payload.get("generated_at_utc", utc_text_from_ts(int(time.time()))),
            "kind": "memory_status_snapshot",
            "snapshot_path": str(ctx.memory_status_file),
            "schema_version": payload.get("schema_version", 1),
            "classification_policy_version": payload.get("classification_policy_version", MEMORY_STATUS_POLICY_VERSION),
            "overall": payload.get("overall", {}),
            "operational_adequacy": payload.get("operational_adequacy", {}),
            "host": payload.get("host", {}),
            "top_current_consumers": payload.get("top_current_consumers", []),
            "top_non_reclaimable_consumers": payload.get("top_non_reclaimable_consumers", []),
            "top_peak_consumers": payload.get("top_peak_consumers", []),
            "services": payload.get("services", []),
        },
    )


def fmt_mib(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / MIB:.1f}MiB"


def memory_status(ctx: MemoryStatusContext, *, json_output: bool = False, record: bool = True) -> int:
    payload = memory_status_payload(ctx)
    if record:
        save_memory_status(ctx, payload)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    host = payload["host"]
    top_current = payload["top_current_consumers"][:3]
    top_peak = payload["top_peak_consumers"][:3]
    top_current_text = ",".join(
        f"{item['unit']}={fmt_mib(item['memory_current_bytes'])}/{item['dominant_category']}"
        for item in top_current
    )
    top_peak_text = ",".join(
        f"{item['unit']}={fmt_mib(item['memory_peak_bytes'])}/{item['dominant_category']}"
        for item in top_peak
    )
    print(
        "[memory-status] "
        f"generated_at={payload['generated_at_jst']} "
        f"severity={payload['overall']['severity']} "
        f"adequacy={payload['operational_adequacy']['severity']} "
        f"host_available={fmt_mib(host.get('mem_available_bytes'))} "
        f"host_non_reclaimable={fmt_mib(host.get('non_reclaimable_estimate_bytes'))} "
        f"swap_used={fmt_mib(host.get('swap_used_bytes'))} "
        f"top_current={top_current_text or '-'} "
        f"top_peak={top_peak_text or '-'}"
    )
    if record:
        print(f"[memory-status] snapshot={ctx.memory_status_file} history={ctx.memory_status_events_file}")
    return 0
