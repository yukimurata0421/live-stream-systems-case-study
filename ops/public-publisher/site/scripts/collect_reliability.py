#!/usr/bin/env python3
"""Publish a compact, public-safe reliability snapshot from the monitoring host."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


SITE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = SITE_DIR / "public" / "reliability-indicators.json"
DEFAULT_SOURCE_DIR = "/opt/stream_v3"
DEFAULT_STATE_DIR = "/var/lib/stream-v3/observability-monitor"
DEFAULT_CACHE_SEC = 60


def as_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


def rounded(value: Any, digits: int = 3) -> float | None:
    number = as_number(value)
    return round(number, digits) if number is not None else None


def fact(label: str, value: Any, unit: str) -> dict[str, Any]:
    return {"label": label, "value": rounded(value), "unit": unit}


def metric_item(
    *,
    item_id: str,
    label: str,
    window: str,
    value: Any,
    unit: str,
    reference: Any,
    summary: dict[str, Any],
    extra_facts: list[dict[str, Any]],
    note: str,
) -> dict[str, Any]:
    value_number = rounded(value)
    reference_number = rounded(reference)
    facts: list[dict[str, Any]] = []
    if reference_number is not None:
        facts.append(fact("Reference", reference_number, "%"))
        if value_number is not None:
            facts.append(fact("Difference", value_number - reference_number, "pt"))
    facts.extend(
        [
            fact("Observed", summary.get("observed_points"), "samples"),
            fact("Bad samples", summary.get("bad_points"), "samples"),
            fact("Missing", summary.get("missing_points"), "samples"),
            fact("Coverage", summary.get("coverage_pct"), "%"),
        ]
    )
    facts.extend(extra_facts)
    return {
        "id": item_id,
        "label": label,
        "window": window,
        "value": value_number,
        "unit": unit,
        "facts": facts,
        "note": note,
    }


def visual_support_item(visual: dict[str, Any]) -> dict[str, Any]:
    viewer = visual.get("viewer_probe_events")
    if not isinstance(viewer, dict):
        return metric_item(
            item_id="visual_supporting_samples",
            label="Visual supporting samples",
            window="available 7d evidence",
            value=visual.get("sli_pct"),
            unit="%",
            reference=None,
            summary=visual,
            extra_facts=[],
            note=(
                "Legacy minute-grid supporting evidence pending the event-based source release. "
                "The formal visual check remains a sampled 1920x1080 capture checklist."
            ),
        )
    adsb = visual.get("adsb_rendering_events") or {}
    return {
        "id": "visual_supporting_samples",
        "label": "Viewer evidence coverage",
        "window": "rolling 7d scheduled probes",
        "value": rounded(viewer.get("coverage_pct")),
        "unit": "%",
        "facts": [
            fact("Eligible-frame result", viewer.get("sli_pct"), "%"),
            fact("Scheduled probes", viewer.get("expected_points"), "events"),
            fact("Viewer probe events", viewer.get("total_probe_events"), "events"),
            fact("Eligible frames", viewer.get("observed_points"), "events"),
            fact("Confirmed visual bad", viewer.get("bad_points"), "events"),
            fact("Probe unavailable", viewer.get("unknown_points"), "events"),
            fact("Scheduled missing", viewer.get("missing_points"), "events"),
            fact("Probe event coverage", viewer.get("event_coverage_pct"), "%"),
            fact("ADS-B monitor events", adsb.get("total_monitor_events"), "events"),
            fact("ADS-B supporting bad", adsb.get("bad_points"), "events"),
            fact("ADS-B supporting unknown", adsb.get("unknown_points"), "events"),
            fact("ADS-B support rate", adsb.get("sli_pct"), "%"),
        ],
        "note": (
            "The primary value is eligible-frame evidence coverage over scheduled five-minute probes, "
            "not visual correctness. Eligible-frame result is conditional on captured frames; capture "
            "failures are unavailable evidence, not confirmed bad frames. ADS-B monitor events use a "
            "separate denominator. The formal visual check remains a sampled 1920x1080 capture checklist."
        ),
    }


def build_public_payload(raw: dict[str, Any], *, now: float | None = None) -> dict[str, Any]:
    windows = raw.get("windows") or {}
    week = windows.get("7d") or {}
    month = windows.get("30d") or {}

    availability = (week.get("youtube_availability") or {}).get("selected") or {}
    availability_root = week.get("youtube_availability") or {}
    same_url = month.get("same_url_preservation") or {}
    same_url_raw = same_url.get("raw_metric") or {}
    same_url_actual = same_url.get("actual_url_evidence") or {}
    upload = week.get("upload_ceiling") or {}
    input_quality = week.get("youtube_input_quality") or {}
    input_raw = input_quality.get("raw_warning_evidence") or {}
    audio = week.get("audio_correctness") or {}
    visual = week.get("visual_correctness") or {}

    generated_iso = str(raw.get("generated_at_utc") or "")
    try:
        generated_at = datetime.fromisoformat(generated_iso.replace("Z", "+00:00")).timestamp()
    except ValueError:
        generated_at = now if now is not None else time.time()

    items = [
        metric_item(
            item_id="delivery_availability_estimate",
            label="Delivery availability estimate",
            window="rolling 7d",
            value=availability.get("sli_pct"),
            unit="%",
            reference=availability.get("target_pct"),
            summary=availability,
            extra_facts=[
                fact("Raw bad", (availability_root.get("raw_composite") or {}).get("bad_points"), "samples"),
                fact("Excluded public-only", availability_root.get("public_only_excluded_points"), "samples"),
            ],
            note="Public live, ingest and watchdog evidence; uncorroborated public-only status gaps stay separate.",
        ),
        {
            "id": "same_url_observation",
            "label": "Same URL observation",
            "window": "rolling 30d",
            "value": rounded(same_url_actual.get("live_url_transition_count")),
            "unit": "transitions",
            "facts": [
                fact("Distinct URLs", same_url_actual.get("distinct_live_url_count"), "URLs"),
                fact("Evidence rows", same_url_actual.get("row_count"), "events"),
                fact("Raw metric", same_url_raw.get("sli_pct"), "%"),
                fact("Raw bad", same_url_raw.get("bad_points"), "samples"),
            ],
            "note": "Observed transition and URL counts are shown separately from the raw monitoring ratio.",
        },
        metric_item(
            item_id="upload_within_ceiling",
            label="Upload within ceiling",
            window="rolling 7d",
            value=upload.get("sli_pct"),
            unit="%",
            reference=upload.get("target_pct"),
            summary=upload,
            extra_facts=[
                fact("Ceiling", 5.0, "Mbps"),
                fact("Max p95", upload.get("max_value"), "Mbps"),
            ],
            note=(
                "Seven-day trend of one-minute observations of the rolling 1h upload p95 against "
                "the 5.0 Mbps engineering ceiling. The formal upload SLO window remains rolling 24h."
            ),
        ),
        {
            "id": "youtube_input_quality",
            "label": "YouTube input quality evidence",
            "window": "rolling 7d",
            "value": rounded(input_quality.get("sli_pct")),
            "unit": "%",
            "facts": [
                fact("Reference", input_quality.get("target_pct"), "%"),
                fact(
                    "Difference",
                    (
                        as_number(input_quality.get("sli_pct"))
                        - as_number(input_quality.get("target_pct"))
                        if as_number(input_quality.get("sli_pct")) is not None
                        and as_number(input_quality.get("target_pct")) is not None
                        else None
                    ),
                    "pt",
                ),
                fact("Eligible duration", input_quality.get("observed_points"), "min"),
                fact("Bad quality", input_quality.get("bad_minutes"), "min"),
                fact("Evidence coverage", input_quality.get("coverage_pct"), "%"),
                fact(
                    "Outside denominator",
                    (
                        as_number(input_quality.get("excluded_seconds")) / 60.0
                        if as_number(input_quality.get("excluded_seconds")) is not None
                        else None
                    ),
                    "min",
                ),
                fact(
                    "Missing evidence",
                    (
                        as_number(input_quality.get("timeline_missing_seconds")) / 60.0
                        if as_number(input_quality.get("timeline_missing_seconds")) is not None
                        else None
                    ),
                    "min",
                ),
                fact("Recovery warnings (separate)", input_raw.get("warning_row_count"), "events"),
            ],
            "note": (
                "Fresh active YouTube OAuth health evidence. Local ingest disconnects and stale probes "
                "are outside this denominator and remain separate availability or coverage evidence."
            ),
        },
        metric_item(
            item_id="audio_check_samples",
            label="Audio check samples",
            window="rolling 7d",
            value=audio.get("sli_pct"),
            unit="%",
            reference=audio.get("target_pct"),
            summary=audio,
            extra_facts=[],
            note="Pulse monitor energy and audio fault evidence observed by the monitoring layer.",
        ),
        visual_support_item(visual),
    ]

    return {
        "schema": "stream-v3-reliability-public.v4",
        "generated_at": generated_at,
        "generated_at_iso": generated_iso,
        "cadence_sec": DEFAULT_CACHE_SEC,
        "scope": "operational_reliability_indicators",
        "interpretation": "Measured observations and internal reference lines; no pass/fail or availability guarantee.",
        "items": items,
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    temporary.replace(path)


def cache_is_fresh(path: Path, *, cache_sec: int, now: float | None = None) -> bool:
    current = time.time() if now is None else now
    try:
        return current - path.stat().st_mtime < cache_sec
    except OSError:
        return False


def fetch_raw_report(
    target: str,
    source_dir: str,
    state_dir: str,
    *,
    timeout_sec: float,
) -> dict[str, Any]:
    remote = (
        f"cd {shlex.quote(source_dir)} && "
        f"STREAM_RUNTIME_STATE_DIR={shlex.quote(state_dir)} "
        f"STREAM_RUNTIME_LOG_DIR={shlex.quote(str(Path(state_dir) / 'logs'))} "
        "bin/stream-prod sli-report --windows 24h,7d,30d --json"
    )
    completed = subprocess.run(
        [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=8",
            "-o", "ServerAliveInterval=10",
            "-o", "ServerAliveCountMax=2",
            target,
            remote,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict) or payload.get("metric_errors"):
        raise RuntimeError("SLI report is unavailable or contains metric errors")
    return payload


def unavailable_payload(*, now: float, reason: str) -> dict[str, Any]:
    return {
        "schema": "stream-v3-reliability-public.v4",
        "generated_at": now,
        "generated_at_iso": datetime.fromtimestamp(now).astimezone().isoformat(),
        "cadence_sec": DEFAULT_CACHE_SEC,
        "scope": "operational_reliability_indicators",
        "interpretation": "Measured observations and internal reference lines; no pass/fail or availability guarantee.",
        "items": [],
        "availability": "measurement unavailable",
        "reason": reason,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect public-safe Stream v3 reliability indicators")
    parser.add_argument("--force", action="store_true", help="ignore the 60-second cache")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cache-sec", type=int, default=DEFAULT_CACHE_SEC)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.force and cache_is_fresh(args.output, cache_sec=args.cache_sec):
        return 0

    target = os.environ.get("STREAM_V3_RELIABILITY_SSH_TARGET", "").strip()
    source_dir = os.environ.get("STREAM_V3_RELIABILITY_SOURCE_DIR", DEFAULT_SOURCE_DIR).strip()
    state_dir = os.environ.get("STREAM_V3_RELIABILITY_STATE_DIR", DEFAULT_STATE_DIR).strip()
    now = time.time()
    try:
        if not target:
            raise RuntimeError("reliability source target is not configured")
        raw = fetch_raw_report(target, source_dir, state_dir, timeout_sec=args.timeout)
        payload = build_public_payload(raw, now=now)
        write_json_atomic(args.output, payload)
    except (OSError, ValueError, json.JSONDecodeError, subprocess.SubprocessError, RuntimeError) as exc:
        if args.output.exists():
            print(f"reliability refresh deferred; keeping cached snapshot: {exc}", file=sys.stderr)
            return 0
        write_json_atomic(args.output, unavailable_payload(now=now, reason="source unavailable"))
        print(f"reliability source unavailable; published empty snapshot: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
