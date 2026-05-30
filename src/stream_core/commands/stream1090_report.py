from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class Stream1090ReportContext:
    stream1090_report_events_file: Path
    upstream_report_events_file: Path
    stream1090_visual_dir: Path
    run: Callable[..., subprocess.CompletedProcess[str]]
    append_jsonl: Callable[[Path, dict], None]
    iter_jsonl: Callable[[Path], object]
    parse_utc_ts: Callable[[str], int]
    default_upstream_url: Callable[[], str]


def url_at(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + "/" + path.lstrip("/")


def stream1090_resource_url(base_url: str, map_path: str, resource_path: str = "") -> str:
    return url_at(url_at(base_url, map_path), resource_path)


def fetch_url_text(url: str, timeout: float) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        data = response.read()
    return data.decode("utf-8", errors="replace")


def fetch_url_json(url: str, timeout: float) -> object:
    return json.loads(fetch_url_text(url, timeout))


def aircraft_position_map(payload: object) -> dict[str, tuple[float, float]]:
    if not isinstance(payload, dict):
        return {}
    out: dict[str, tuple[float, float]] = {}
    aircraft = payload.get("aircraft")
    if not isinstance(aircraft, list):
        return out
    for index, ac in enumerate(aircraft):
        if not isinstance(ac, dict):
            continue
        lat = ac.get("lat")
        lon = ac.get("lon")
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            continue
        ident = str(ac.get("hex") or ac.get("flight") or index)
        out[ident] = (float(lat), float(lon))
    return out


def outline_points_count(payload: object) -> int:
    if not isinstance(payload, dict):
        return 0
    points = (
        payload.get("actualRange", {})
        if isinstance(payload.get("actualRange"), dict)
        else {}
    ).get("last24h", {})
    if isinstance(points, dict):
        raw_points = points.get("points", [])
        return len(raw_points) if isinstance(raw_points, list) else 0
    return 0


def chromium_binary() -> str:
    configured = os.environ.get("STREAM1090_CHROMIUM_BIN", "").strip()
    if configured:
        return configured
    for name in ("chromium", "chromium-browser", "google-chrome", "google-chrome-stable"):
        path = shutil.which(name)
        if path:
            return path
    for path in ("/snap/bin/chromium",):
        if Path(path).exists():
            return path
    return ""


def screenshot_mean_luma(path: Path) -> int | None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not path.exists():
        return None
    cp = subprocess.run(
        [
            ffmpeg,
            "-v",
            "error",
            "-i",
            str(path),
            "-vf",
            "scale=1:1,format=gray",
            "-frames:v",
            "1",
            "-f",
            "rawvideo",
            "-",
        ],
        capture_output=True,
        check=False,
    )
    data = cp.stdout or b""
    if cp.returncode != 0 or not data:
        return None
    return data[0]


def visual_probe_payload(
    ctx: Stream1090ReportContext,
    *,
    page_url: str,
    target: str,
    timeout: float,
    screenshot_dir: Path | None = None,
    chromium_binary_func: Callable[[], str] = chromium_binary,
    screenshot_mean_luma_func: Callable[[Path], int | None] = screenshot_mean_luma,
) -> dict:
    chromium = chromium_binary_func()
    warnings: list[str] = []
    if not chromium:
        return {
            "enabled": True,
            "available": False,
            "warnings": ["chromium_not_found"],
            "judgment": "visual_probe_unavailable",
        }
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output_dir = screenshot_dir or ctx.stream1090_visual_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    screenshot_path = output_dir / f"{target}_{ts}.png"
    budget_ms = max(1000, int(timeout * 1000))
    common = [
        chromium,
        "--headless",
        "--no-sandbox",
        "--disable-gpu",
        "--window-size=1280,720",
        f"--virtual-time-budget={budget_ms}",
    ]
    safe_target = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in target)[:80]
    tmp_screenshot_path = Path.home() / f"stream1090_visual_{os.getpid()}_{safe_target}_{ts}.png"
    try:
        tmp_screenshot_path.unlink(missing_ok=True)
    except OSError:
        pass
    shot = ctx.run([*common, f"--screenshot={tmp_screenshot_path}", page_url], check=False)
    if tmp_screenshot_path.exists():
        try:
            shutil.copy2(tmp_screenshot_path, screenshot_path)
        except OSError as exc:
            warnings.append(f"screenshot_copy_failed:{str(exc)[:120]}")
        try:
            tmp_screenshot_path.unlink()
        except OSError:
            pass
    dom = ctx.run([*common, "--dump-dom", page_url], check=False)
    dom_text = dom.stdout or ""
    if shot.returncode != 0:
        warnings.append(f"screenshot_failed:{(shot.stderr or shot.stdout or '').strip()[:160]}")
    if dom.returncode != 0:
        warnings.append(f"dom_dump_failed:{(dom.stderr or dom.stdout or '').strip()[:160]}")
    screenshot_bytes = screenshot_path.stat().st_size if screenshot_path.exists() else 0
    if screenshot_bytes <= 0:
        warnings.append("screenshot_empty")
    tile_dom_count = sum(
        dom_text.count(marker)
        for marker in (
            "leaflet-tile",
            "tile.openstreetmap",
            "/tiles/",
            "ol-viewport",
            "ol-layer",
            "<canvas",
        )
    )
    if tile_dom_count <= 0:
        warnings.append("tile_dom_markers_missing")
    mean_luma = screenshot_mean_luma_func(screenshot_path)
    if mean_luma is not None and mean_luma <= 3:
        warnings.append("screenshot_luma_near_black")
    return {
        "enabled": True,
        "available": True,
        "page_url": page_url,
        "screenshot_path": str(screenshot_path),
        "screenshot_bytes": screenshot_bytes,
        "tile_dom_count": tile_dom_count,
        "mean_luma": mean_luma,
        "warnings": warnings,
        "judgment": "visual_probe_ok" if not warnings else "visual_probe_warn",
    }


def report_history_summary(
    ctx: Stream1090ReportContext,
    log_file: Path,
    *,
    target: str,
    hours: int = 24,
    include_payload: dict | None = None,
) -> dict:
    now = int(time.time())
    cutoff = now - max(1, int(hours)) * 3600
    rows = []
    for item in ctx.iter_jsonl(log_file):
        ts = ctx.parse_utc_ts(str(item.get("ts_utc", "")))
        if ts >= cutoff and (not target or str(item.get("target", "")) == target):
            rows.append(item)
    if include_payload:
        rows.append(include_payload)
    total = len(rows)
    warn_rows = [item for item in rows if str(item.get("judgment", "")).endswith("warn") or item.get("warnings")]
    warning_counts: dict[str, int] = {}
    for item in warn_rows:
        for warning in item.get("warnings", []) if isinstance(item.get("warnings"), list) else []:
            warning_counts[str(warning)] = warning_counts.get(str(warning), 0) + 1
        visual = item.get("visual_probe") if isinstance(item.get("visual_probe"), dict) else {}
        for warning in visual.get("warnings", []) if isinstance(visual.get("warnings"), list) else []:
            warning_counts[f"visual:{warning}"] = warning_counts.get(f"visual:{warning}", 0) + 1
    warn_rate = (len(warn_rows) / total) if total > 0 else 0.0
    alert = total >= 3 and (len(warn_rows) >= 2 or warn_rate >= 0.5)
    return {
        "hours": max(1, int(hours)),
        "sample_count": total,
        "warn_count": len(warn_rows),
        "warn_rate": round(warn_rate, 6),
        "alert": alert,
        "warning_counts": warning_counts,
    }


def stream1090_report_payload(
    *,
    base_url: str = "http://127.0.0.1:18080",
    map_path: str = "/stream1090/",
    target: str = "overlay_stream1090",
    sample_sec: float = 5.0,
    timeout: float = 5.0,
    sleep_func=time.sleep,
    fetch_text_func=fetch_url_text,
    fetch_json_func=fetch_url_json,
    visual: bool = False,
    visual_fetch_func: Callable[..., dict] | None = None,
) -> dict:
    warnings: list[str] = []
    html = ""
    outline: object = {}
    aircraft_1: object = {}
    aircraft_2: object = {}

    try:
        html = str(fetch_text_func(stream1090_resource_url(base_url, map_path), timeout))
    except Exception as e:
        warnings.append(f"stream1090_html_fetch_failed: {e}")

    try:
        outline = fetch_json_func(stream1090_resource_url(base_url, map_path, "/data/outline.json"), timeout)
    except Exception as e:
        warnings.append(f"outline_json_fetch_failed: {e}")

    try:
        aircraft_1 = fetch_json_func(stream1090_resource_url(base_url, map_path, "/data/aircraft.json"), timeout)
    except Exception as e:
        warnings.append(f"aircraft_json_first_fetch_failed: {e}")

    if sample_sec > 0:
        sleep_func(sample_sec)

    try:
        aircraft_2 = fetch_json_func(stream1090_resource_url(base_url, map_path, "/data/aircraft.json"), timeout)
    except Exception as e:
        warnings.append(f"aircraft_json_second_fetch_failed: {e}")

    movement_retry_count = 0
    movement_sample_elapsed_sec = sample_sec
    html_lower = html.lower()
    pos_1 = aircraft_position_map(aircraft_1)
    pos_2 = aircraft_position_map(aircraft_2)
    moved = 0
    for ident, first in pos_1.items():
        second = pos_2.get(ident)
        if second and second != first:
            moved += 1

    messages_1 = aircraft_1.get("messages") if isinstance(aircraft_1, dict) else None
    messages_2 = aircraft_2.get("messages") if isinstance(aircraft_2, dict) else None
    messages_delta = None
    if isinstance(messages_1, int) and isinstance(messages_2, int):
        messages_delta = messages_2 - messages_1
    if (
        sample_sec > 0
        and isinstance(aircraft_2, dict)
        and messages_delta is not None
        and messages_delta <= 0
        and moved <= 0
    ):
        sleep_func(sample_sec)
        try:
            aircraft_3 = fetch_json_func(stream1090_resource_url(base_url, map_path, "/data/aircraft.json"), timeout)
            movement_retry_count = 1
            movement_sample_elapsed_sec += sample_sec
            pos_3 = aircraft_position_map(aircraft_3)
            retry_moved = 0
            for ident, first in pos_2.items():
                second = pos_3.get(ident)
                if second and second != first:
                    retry_moved += 1
            messages_3 = aircraft_3.get("messages") if isinstance(aircraft_3, dict) else None
            retry_messages_delta = None
            if isinstance(messages_2, int) and isinstance(messages_3, int):
                retry_messages_delta = messages_3 - messages_2
            if retry_moved > 0 or (retry_messages_delta is not None and retry_messages_delta > 0):
                aircraft_1 = aircraft_2
                aircraft_2 = aircraft_3
                pos_1 = pos_2
                pos_2 = pos_3
                moved = retry_moved
                messages_1 = messages_2
                messages_2 = messages_3
                messages_delta = retry_messages_delta
        except Exception as e:
            warnings.append(f"aircraft_json_retry_fetch_failed: {e}")

    checks = {
        "html_reachable": bool(html),
        "html_length": len(html),
        "html_has_map_markers": any(marker in html_lower for marker in ("leaflet", "tar1090", "map")),
        "outline_json_ok": isinstance(outline, dict),
        "actual_range_points": outline_points_count(outline),
        "aircraft_json_ok": isinstance(aircraft_1, dict) and isinstance(aircraft_2, dict),
        "aircraft_count_first": len(aircraft_1.get("aircraft", []))
        if isinstance(aircraft_1, dict) and isinstance(aircraft_1.get("aircraft"), list)
        else 0,
        "aircraft_count_second": len(aircraft_2.get("aircraft", []))
        if isinstance(aircraft_2, dict) and isinstance(aircraft_2.get("aircraft"), list)
        else 0,
        "position_count_first": len(pos_1),
        "position_count_second": len(pos_2),
        "position_change_count": moved,
        "messages_delta": messages_delta,
        "sample_sec": sample_sec,
        "movement_retry_count": movement_retry_count,
        "movement_sample_elapsed_sec": movement_sample_elapsed_sec,
    }
    if not checks["html_has_map_markers"]:
        warnings.append("stream1090_html_map_markers_missing")
    if checks["actual_range_points"] <= 0:
        warnings.append("actual_range_points_missing")
    if messages_delta is not None and messages_delta <= 0 and checks["position_change_count"] <= 0:
        warnings.append("aircraft_messages_and_positions_not_moving_in_sample")
    visual_payload = (
        visual_fetch_func(
            page_url=stream1090_resource_url(base_url, map_path),
            target=target,
            timeout=timeout,
        )
        if visual and visual_fetch_func is not None
        else {"enabled": False}
    )
    if visual and isinstance(visual_payload, dict) and visual_payload.get("warnings"):
        warnings.append("visual_probe_warn")

    return {
        "mode": "report_only",
        "affects_restart": False,
        "affects_stream_restart": False,
        "target": target,
        "base_url": base_url,
        "map_path": map_path,
        "checks": checks,
        "visual_probe": visual_payload,
        "warnings": warnings,
        "judgment": "report_only_ok" if not warnings else "report_only_warn",
    }


def print_report_payload(prefix: str, payload: dict) -> None:
    checks = payload["checks"]
    if prefix == "stream1090-report":
        print(
            "[stream1090-report] "
            f"mode=report_only affects_restart=false judgment={payload['judgment']} base_url={payload['base_url']}"
        )
    else:
        print(
            "[upstream-report] "
            f"mode=report_only affects_stream_restart=false judgment={payload['judgment']} "
            f"upstream_url={payload['upstream_url']}"
        )
    print(
        f"[{prefix}] "
        f"html_reachable={checks['html_reachable']} html_has_map_markers={checks['html_has_map_markers']} "
        f"outline_json_ok={checks['outline_json_ok']} actual_range_points={checks['actual_range_points']}"
    )
    print(
        f"[{prefix}] "
        f"aircraft_count_first={checks['aircraft_count_first']} aircraft_count_second={checks['aircraft_count_second']} "
        f"position_change_count={checks['position_change_count']} messages_delta={checks['messages_delta']}"
    )
    baseline = payload["baseline"]
    print(
        f"[{prefix}] "
        f"baseline_samples_24h={baseline['sample_count']} baseline_warns_24h={baseline['warn_count']} "
        f"baseline_warn_rate_24h={baseline['warn_rate']} baseline_alert={baseline['alert']}"
    )
    visual_payload = payload.get("visual_probe") if isinstance(payload.get("visual_probe"), dict) else {}
    if visual_payload.get("enabled"):
        print(
            f"[{prefix}] "
            f"visual_judgment={visual_payload.get('judgment')} screenshot_bytes={visual_payload.get('screenshot_bytes')} "
            f"tile_dom_count={visual_payload.get('tile_dom_count')} mean_luma={visual_payload.get('mean_luma')}"
        )
    if payload["warnings"]:
        print(f"[{prefix}] warnings={json.dumps(payload['warnings'], ensure_ascii=False)}")


def stream1090_report(
    ctx: Stream1090ReportContext,
    *,
    payload_func: Callable[..., dict],
    base_url: str = "http://127.0.0.1:18080",
    sample_sec: float = 5.0,
    timeout: float = 5.0,
    visual: bool = False,
    record: bool = True,
    json_output: bool = False,
) -> int:
    payload = payload_func(base_url=base_url, sample_sec=sample_sec, timeout=timeout, visual=visual)
    payload["ts_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload["baseline"] = report_history_summary(
        ctx,
        ctx.stream1090_report_events_file,
        target="overlay_stream1090",
        include_payload=payload,
    )
    if record:
        ctx.append_jsonl(ctx.stream1090_report_events_file, payload)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    print_report_payload("stream1090-report", payload)
    return 0


def upstream_report(
    ctx: Stream1090ReportContext,
    *,
    split_url_root_and_path: Callable[[str, str], tuple[str, str]],
    payload_func: Callable[..., dict],
    upstream_url: str = "",
    sample_sec: float = 5.0,
    timeout: float = 5.0,
    visual: bool = False,
    record: bool = True,
    json_output: bool = False,
) -> int:
    raw_url = upstream_url.strip() or ctx.default_upstream_url()
    base_url, map_path = split_url_root_and_path(raw_url, "/stream1090/")
    payload = payload_func(
        base_url=base_url,
        map_path=map_path,
        target="upstream_readsb_tar1090_stream1090",
        sample_sec=sample_sec,
        timeout=timeout,
        visual=visual,
    )
    payload["upstream_url"] = raw_url
    payload["ts_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload["baseline"] = report_history_summary(
        ctx,
        ctx.upstream_report_events_file,
        target="upstream_readsb_tar1090_stream1090",
        include_payload=payload,
    )
    if record:
        ctx.append_jsonl(ctx.upstream_report_events_file, payload)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    print_report_payload("upstream-report", payload)
    return 0
