#!/usr/bin/env python3
"""Publish a compact, public-safe reliability snapshot from arena-server."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SITE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = SITE_DIR / "public" / "reliability-indicators.json"
DEFAULT_TWITCH_SLI = SITE_DIR / "public" / "twitch-sli.json"
SCHEMA = "stream-v3-reliability-public.v6"
INTERPRETATION = (
    "Viewer-facing SLO rows carry a target, error budget and assessment; quality rows are references; "
    "guardrail rows are constraints and invariants. Targets are internal engineering objectives, not guarantees."
)
# Twitch delivery through the OCI fan-out started here; earlier minutes are outside the SLO period.
TWITCH_SLO_START = os.environ.get("STREAM_V3_TWITCH_SLO_START", "2026-10-20T08:43:00+09:00")
TWITCH_SLI_MAX_AGE_SEC = 180
# The relay-output clock classification metric exists from here; earlier minutes are not yet measured.
PICTURE_SLO_START = os.environ.get("STREAM_V3_PICTURE_SLO_START", "2026-10-21T16:50:00+09:00")
JST = timezone(timedelta(hours=9))
PICTURE_TARGET_PCT = 99.0
# Provisional: OCR leaves ~5% of minutes unread, so a 99% gate would hide every result as unknown.
PICTURE_COVERAGE_GATE_PCT = 90.0
VIEWER_COVERAGE_GATE_PCT = 95.0
ADSB_COVERAGE_GATE_PCT = 99.0
WEEK_MIN = 7 * 1440
DAY_MIN = 1440
MEASUREMENT_REASONS = {
    "source_disagreement": "sources disagree",
    "source_freshness_not_met": "source freshness below gate",
    "coverage_not_met": "coverage below gate",
    "insufficient_coverage": "coverage below gate",
}
# One character per minute from the relay-output clock check: good, bad, unread, standby, no sample.
PICTURE_REMOTE_SCRIPT = """
import json,time,urllib.parse,urllib.request
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
end=int(time.time()//60)*60;start=end-60*(10080-1)
q='max by (state) (stream_v3_monitor_probe_classification{job="stream_v3_arena_monitor",target="oci_relay"})'
u='http://127.0.0.1:9090/api/v1/query_range?'+urllib.parse.urlencode({'query':q,'start':start,'end':end,'step':60})
with op.open(u,timeout=20) as r:body=json.load(r)
if body.get('status')!='success':raise SystemExit(2)
codes=['-']*10080;rank={'target_bad':0,'target_good':1,'observation_unknown':2,'standby':3}
char={'target_bad':'b','target_good':'g','observation_unknown':'u','standby':'s'}
best=[9]*10080
for s in body['data']['result']:
    st=s['metric'].get('state')
    if st not in rank:continue
    for t,v in s['values']:
        i=int(round((float(t)-start)/60))
        if 0<=i<10080 and float(v)==1 and rank[st]<best[i]:best[i]=rank[st];codes[i]=char[st]
print(json.dumps({'end':end,'step':60,'codes':''.join(codes)},separators=(',',':')))
""".strip()
DEFAULT_CACHE_SEC = 60
PUBLIC_INCIDENT_LABELS = {
    "reliability:upload_ceiling_multi_window_burn": "Upload ceiling multi-window burn",
}
ACTIVE_INCIDENTS_REMOTE_SCRIPT = """
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)

active = payload.get("active") if isinstance(payload, dict) else {}
items = []
for incident_id, state in sorted((active or {}).items()):
    if not isinstance(state, dict):
        continue
    incident = state.get("last_incident")
    if not isinstance(incident, dict):
        incident = {}
    items.append({
        "id": incident_id,
        "severity": incident.get("severity"),
        "recovery_type": incident.get("recovery_type"),
        "observed_ts": incident.get("observed_ts"),
    })

print(json.dumps({"items": items}, separators=(",", ":")))
""".strip()


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


def minutes_from(seconds: Any) -> float | None:
    number = as_number(seconds)
    return number / 60.0 if number is not None else None


def parse_time(value: Any) -> float | None:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp.timestamp() if stamp.tzinfo else None


def jst_label(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=JST).strftime("%m-%d %H:%M JST")


def assessment(status: str, label: str) -> dict[str, str]:
    return {"status": status, "label": label}


def judged(
    value: Any,
    target: Any,
    *,
    coverage: Any = None,
    gate: float | None = None,
    reasons: list[str] | None = None,
    collecting_until: float | None = None,
    good: str = "MET",
    bad: str = "BREACHED",
    good_label: str = "meets target",
    bad_label: str = "below target",
) -> dict[str, str]:
    """One rule for every row: collecting, then evidence gates, then the value against its target."""
    coverage_number, value_number, target_number = as_number(coverage), as_number(value), as_number(target)
    if collecting_until is not None:
        label = f"collecting until {jst_label(collecting_until)}"
        # A provisional 24 h reading, never a pass/fail of the SLO itself.
        gated = gate is not None and (coverage_number is None or coverage_number < gate)
        if not reasons and not gated and value_number is not None and target_number is not None:
            label += " · 24 h " + ("meets target" if value_number >= target_number else "below target")
        return assessment("COLLECTING", label)
    if reasons:
        return assessment("UNKNOWN", "not judged: " + ", ".join(dict.fromkeys(reasons)))
    if gate is not None and (coverage_number is None or coverage_number < gate):
        shown = f"{coverage_number:.0f}%" if coverage_number is not None else "unknown"
        return assessment("UNKNOWN", f"not judged: coverage {shown} < {gate:g}%")
    if value_number is None or target_number is None:
        return assessment("UNKNOWN", "no result")
    return assessment(good, good_label) if value_number >= target_number else assessment(bad, bad_label)


def report_reasons(summary: dict[str, Any]) -> list[str]:
    """The report's own measurement verdict; anything other than valid withholds pass/fail."""
    status = summary.get("measurement_status")
    if status in (None, "valid"):
        return []
    reasons = [MEASUREMENT_REASONS.get(str(r), "measurement incomplete") for r in summary.get("measurement_unknown_reasons") or []]
    return reasons or ["measurement incomplete"]


def budget_facts(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        fact("Conservative value", summary.get("conservative_sli_pct"), "%"),
        fact("Coverage", summary.get("coverage_pct"), "%"),
        fact("Coverage gate", summary.get("minimum_coverage_pct"), "%"),
        fact("Bad minutes", summary.get("bad_minutes"), "min"),
        fact("Interruptions", summary.get("bad_run_count"), "episodes"),
        fact("Longest interruption", summary.get("max_bad_run_minutes"), "min"),
        fact("Error budget", summary.get("nominal_budget_minutes"), "min"),
        fact("Budget remaining", summary.get("budget_remaining_minutes"), "min"),
    ]


def row(
    *,
    item_id: str,
    tier: str,
    label: str,
    window: str,
    value: Any,
    unit: str,
    target: Any,
    verdict: dict[str, str],
    facts: list[dict[str, Any]],
    note: str,
    recent: Any = None,
) -> dict[str, Any]:
    item = {
        "id": item_id,
        "tier": tier,
        "label": label,
        "window": window,
        "value": rounded(value),
        "unit": unit,
        "target": rounded(target),
        "assessment": verdict,
        "facts": [f for f in facts if f["value"] is not None],
        "note": note,
    }
    if as_number(recent) is not None:
        item["recent_24h"] = rounded(recent)
    return item


def youtube_delivery_item(week: dict[str, Any], day: dict[str, Any]) -> dict[str, Any]:
    root = week.get("youtube_availability") or {}
    selected = root.get("selected") or {}
    recent = ((day.get("youtube_availability") or {}).get("selected") or {}).get("sli_pct")
    return row(
        item_id="delivery_availability_estimate",
        tier="slo",
        label="YouTube delivery availability",
        window="rolling 7d",
        value=selected.get("sli_pct"),
        unit="%",
        target=selected.get("target_pct"),
        verdict=judged(selected.get("sli_pct"), selected.get("target_pct"), reasons=report_reasons(selected)),
        facts=budget_facts(selected)
        + [
            fact("Raw bad", (root.get("raw_composite") or {}).get("bad_points"), "samples"),
            fact("Excluded public-only", root.get("public_only_excluded_points"), "samples"),
        ],
        note=(
            "Minutes where YouTube is publicly live, the ingest is connected and the watchdog is healthy. "
            "Uncorroborated public-only status gaps stay separate. An interruption is a run of consecutive bad minutes."
        ),
        recent=recent,
    )


def twitch_windows(tsli: Any, *, now: float) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    """The 24 h and 7 d Twitch windows, or the reason they cannot be used."""
    if not isinstance(tsli, dict) or tsli.get("schema") != "stream.twitch_sli_public.v1":
        return {}, {}, "Twitch SLI unavailable"
    generated = parse_time(tsli.get("generated_at"))
    if generated is None or not 0 <= now - generated <= TWITCH_SLI_MAX_AGE_SEC:
        return {}, {}, "Twitch SLI updates stopped"
    windows = {w.get("id"): w for w in tsli.get("windows") or [] if isinstance(w, dict)}
    return windows.get("24h") or {}, windows.get("7d") or {}, None


def twitch_metric(window: dict[str, Any], metric_id: str) -> dict[str, Any]:
    return next((m for m in window.get("metrics") or [] if isinstance(m, dict) and m.get("id") == metric_id), {})


def twitch_collecting_until(week: dict[str, Any]) -> float | None:
    start, service = parse_time(week.get("start_at")), parse_time(TWITCH_SLO_START)
    if start is None or service is None or start >= service:
        return None
    return service + WEEK_MIN * 60


def twitch_delivery_item(tsli: Any, *, now: float) -> dict[str, Any]:
    day, week, problem = twitch_windows(tsli, now=now)
    until = twitch_collecting_until(week) if not problem else None
    # While the 7 d window still reaches before the SLO period, its value mixes in unserved minutes; lead with 24 h.
    primary = day if until is not None else week
    live = twitch_metric(primary, "provider_live")
    relay = twitch_metric(primary, "relay_flow")
    api = twitch_metric(primary, "api_measurement")
    recent = twitch_metric(day, "provider_live").get("value_percent")
    budget, bad = as_number(live.get("error_budget_minutes")), as_number(live.get("bad_minutes"))
    reasons = [problem] if problem else []
    if not problem and until is None and live.get("assessment") in ("UNKNOWN", "COLLECTING"):
        reasons = ["coverage below gate"]
    verdict = judged(live.get("value_percent"), live.get("target_percent"), reasons=reasons, collecting_until=until)
    return row(
        item_id="twitch_delivery",
        tier="slo",
        label="Twitch delivery availability",
        window="rolling 24h (7d collecting)" if until is not None else "rolling 7d",
        value=None if problem else live.get("value_percent"),
        unit="%",
        target=live.get("target_percent") if live else 99.0,
        verdict=verdict,
        facts=[
            fact("Conservative value", live.get("conservative_percent"), "%"),
            fact("Coverage", live.get("coverage_percent"), "%"),
            fact("Bad minutes", live.get("bad_minutes"), "min"),
            fact("Unknown minutes", live.get("unknown_minutes"), "min"),
            fact("Error budget", budget, "min"),
            fact("Budget remaining", budget - bad if budget is not None and bad is not None else None, "min"),
            fact("Relay continuity", relay.get("value_percent"), "%"),
            fact("API observation", api.get("value_percent"), "%"),
        ],
        note=(
            "Helix reports the channel LIVE, over known provider minutes; relay continuity and API observation are "
            f"shown beside it. The SLO period starts at {TWITCH_SLO_START} (OCI fan-out)."
        ),
        recent=None if problem or until is not None else recent,
    )


def twitch_renewal_item(tsli: Any, *, now: float) -> dict[str, Any]:
    day, week, problem = twitch_windows(tsli, now=now)
    until = twitch_collecting_until(week) if not problem else None
    window = day if until is not None else week
    renewal = twitch_metric(window, "daily_renewal")
    good, failed = renewal.get("good_events"), renewal.get("bad_events")
    unknown = (renewal.get("unknown_events") or 0) + (renewal.get("pending_events") or 0)
    if problem:
        verdict = assessment("UNKNOWN", f"not judged: {problem}")
    elif not renewal.get("expected_events"):
        verdict = assessment("UNKNOWN", "no renewal due yet")
    elif failed:
        verdict = assessment("EXCEEDED", f"{failed} renewal failed")
    elif unknown:
        verdict = assessment("UNKNOWN", f"not judged: {unknown} renewal unknown")
    else:
        verdict = assessment("HOLDING", "every renewal on time")
    return row(
        item_id="twitch_daily_renewal",
        tier="guardrail",
        label="Twitch daily session renewal",
        window="rolling 24h (7d collecting)" if until is not None else "rolling 7d",
        value=None if problem or (unknown and not failed) else renewal.get("value_percent"),
        unit="%",
        target=renewal.get("target_percent") if renewal else 100.0,
        verdict=verdict,
        facts=[
            fact("On time", good, "events"),
            fact("Failed", failed, "events"),
            fact("Unknown or pending", unknown if renewal else None, "events"),
            fact("Expected", renewal.get("expected_events"), "events"),
        ],
        note="OFFLINE, then a new LIVE stream ID and start time within 600 s of 00:00 JST.",
    )


def runs(codes: str, char: str) -> tuple[int, int]:
    count = longest = current = 0
    for c in codes:
        if c == char:
            current += 1
            if current == 1:
                count += 1
            longest = max(longest, current)
        else:
            current = 0
    return count, longest


def minute_summary(codes: str) -> dict[str, Any]:
    good, bad, unread, standby = (codes.count(c) for c in "gbus")
    missing = len(codes) - good - bad - unread - standby
    judged_minutes = good + bad
    in_service = len(codes) - standby
    count, longest = runs(codes, "b")
    return {
        "good": good, "bad": bad, "unread": unread, "standby": standby, "missing": missing,
        "value": 100.0 * good / judged_minutes if judged_minutes else None,
        "coverage": 100.0 * judged_minutes / in_service if in_service else None,
        "budget": (1 - PICTURE_TARGET_PCT / 100) * judged_minutes,
        "runs": count, "longest": longest,
    }


def picture_freshness_item(source: Any, *, now: float) -> dict[str, Any]:
    codes = source.get("codes") if isinstance(source, dict) else None
    end = as_number(source.get("end")) if isinstance(source, dict) else None
    valid = (isinstance(codes, str) and len(codes) == WEEK_MIN and set(codes) <= set("gbus-")
             and end is not None and -120 <= now - end <= 180)
    note = (
        "Minutes where the observer saw the on-screen clock advancing in the relay output, over minutes it could "
        "judge. Unread OCR minutes and missing samples lower coverage, not the value; relay standby is outside the "
        f"denominator. The {PICTURE_COVERAGE_GATE_PCT:g}% coverage gate is provisional while OCR leaves about 5% of "
        "minutes unread. Checks the picture the relay sends, not what viewers receive."
    )
    if not valid:
        return row(item_id="picture_freshness", tier="slo", label="Picture freshness (relay output)", window="rolling 7d",
                   value=None, unit="%", target=PICTURE_TARGET_PCT, verdict=assessment("UNKNOWN", "not judged: clock evidence unavailable"),
                   facts=[], note=note)
    start, since = end - 60 * (WEEK_MIN - 1), parse_time(PICTURE_SLO_START)
    until = since + WEEK_MIN * 60 if since is not None and start < since else None
    week, day = minute_summary(codes), minute_summary(codes[-DAY_MIN:])
    primary = day if until is not None else week
    verdict = judged(primary["value"], PICTURE_TARGET_PCT, coverage=primary["coverage"], gate=PICTURE_COVERAGE_GATE_PCT,
                     collecting_until=until)
    return row(
        item_id="picture_freshness",
        tier="slo",
        label="Picture freshness (relay output)",
        window="rolling 24h (7d collecting)" if until is not None else "rolling 7d",
        value=primary["value"],
        unit="%",
        target=PICTURE_TARGET_PCT,
        verdict=verdict,
        facts=[
            fact("Clock moving", primary["good"], "min"),
            fact("Frozen or black", primary["bad"], "min"),
            fact("Unread (OCR)", primary["unread"], "min"),
            fact("No sample", primary["missing"], "min"),
            fact("Relay standby", primary["standby"], "min"),
            fact("Coverage", primary["coverage"], "%"),
            fact("Coverage gate", PICTURE_COVERAGE_GATE_PCT, "%"),
            fact("Interruptions", primary["runs"], "episodes"),
            fact("Longest interruption", primary["longest"], "min"),
            fact("Error budget", primary["budget"], "min"),
            fact("Budget remaining", primary["budget"] - primary["bad"], "min"),
        ],
        note=note,
        recent=None if until is not None else day["value"],
    )


def viewer_support_item(visual: dict[str, Any]) -> dict[str, Any]:
    viewer = visual.get("viewer_probe_events")
    note = (
        "Share of captured frames from scheduled five-minute viewer probes that passed the visual checks. "
        "Capture failures and missed probes lower coverage, not the result. New evidence counts as Full-HD only "
        "when the selected source and saved frame are both at least 1920x1080; legacy 320x180 samples stay separate."
    )
    if not isinstance(viewer, dict):
        return row(
            item_id="visual_supporting_samples", tier="quality", label="Viewer frame checks",
            window="rolling 7d scheduled probes", value=None, unit="%", target=None,
            verdict=assessment("UNKNOWN", "not judged: event evidence unavailable"),
            facts=[
                fact("Legacy grid points withheld", visual.get("observed_points"), "samples"),
                fact("Legacy bad points withheld", visual.get("bad_points"), "samples"),
            ],
            note=(
                "Event-based Viewer evidence is unavailable. The legacy minute-grid composite is "
                "withheld because it mixes Viewer and ADS-B signals."
            ),
        )
    target = visual.get("target_pct", 99.0)
    return row(
        item_id="visual_supporting_samples",
        tier="quality",
        label="Viewer frame checks",
        window="rolling 7d scheduled probes",
        value=viewer.get("sli_pct"),
        unit="%",
        target=target,
        verdict=judged(viewer.get("sli_pct"), target, coverage=viewer.get("coverage_pct"), gate=VIEWER_COVERAGE_GATE_PCT,
                       good="MET", bad="BELOW", good_label="at reference", bad_label="below reference"),
        facts=[
            fact("Probe coverage", viewer.get("coverage_pct"), "%"),
            fact("Coverage gate", VIEWER_COVERAGE_GATE_PCT, "%"),
            fact("Scheduled probes", viewer.get("expected_points"), "events"),
            fact("Viewer probe events", viewer.get("total_probe_events"), "events"),
            fact("Eligible frames", viewer.get("observed_points"), "events"),
            fact("Confirmed visual bad", viewer.get("bad_points"), "events"),
            fact("Probe unavailable", viewer.get("unknown_points"), "events"),
            fact("Scheduled missing", viewer.get("missing_points"), "events"),
            fact("Probe event coverage", viewer.get("event_coverage_pct"), "%"),
            fact("Full-HD evidence", viewer.get("high_resolution_points"), "events"),
            fact("Legacy low-resolution", viewer.get("low_resolution_points"), "events"),
            fact("Resolution unknown", viewer.get("resolution_unknown_points"), "events"),
            fact("Full-HD scheduled coverage", viewer.get("high_resolution_scheduled_coverage_pct"), "%"),
        ],
        note=note,
    )


def adsb_source_item(visual: dict[str, Any]) -> dict[str, Any]:
    adsb = visual.get("adsb_source_events") or visual.get("adsb_rendering_events")
    note = (
        "Active and quiet are both healthy when the aircraft.json source heartbeat is fresh. Zero traffic is activity "
        "information, not a source failure. Coverage is the share of time with a current classified monitor event "
        "(each event stays current until the next, for at most its stated span); below the "
        f"{ADSB_COVERAGE_GATE_PCT:g}% coverage gate the result is not judged, so missing evidence never reads as healthy."
    )
    if not isinstance(adsb, dict):
        return row(
            item_id="adsb_source_health", tier="guardrail", label="ADS-B source availability",
            window="rolling 7d monitor events", value=None, unit="%", target=None,
            verdict=assessment("UNKNOWN", "not judged: event evidence unavailable"), facts=[],
            note="Event-based ADS-B source evidence is unavailable; the legacy mixed composite is not substituted.",
        )
    return row(
        item_id="adsb_source_health",
        tier="guardrail",
        label="ADS-B source availability",
        window="rolling 7d monitor events",
        value=adsb.get("sli_pct"),
        unit="%",
        target=99.0,
        verdict=judged(adsb.get("sli_pct"), 99.0, coverage=adsb.get("coverage_pct"), gate=ADSB_COVERAGE_GATE_PCT,
                       good="HOLDING", bad="EXCEEDED", good_label="source fresh", bad_label="source stale"),
        facts=[
            fact("Evidence coverage", adsb.get("coverage_pct"), "%"),
            fact("Coverage gate", ADSB_COVERAGE_GATE_PCT, "%"),
        ]
        + (
            # Time basis: an event stays current until the next one (at most the span); gaps are time, not events.
            [
                fact("Uncovered time", minutes_from(adsb.get("uncovered_seconds")), "min"),
                fact("Unclassified time", minutes_from(adsb.get("unclassified_seconds")), "min"),
                fact("Event valid for up to", adsb.get("max_event_span_sec"), "s"),
            ]
            if adsb.get("coverage_basis") == "event_time_span"
            else [
                fact("Scheduled events", adsb.get("expected_points"), "events"),
                fact("Missing", adsb.get("missing_points"), "events"),
            ]
        )
        + [
            fact("Monitor events", adsb.get("total_monitor_events"), "events"),
            fact("Classified events", adsb.get("observed_points"), "events"),
            fact("Active", adsb.get("active_points"), "events"),
            fact("Quiet", adsb.get("quiet_points"), "events"),
            fact("Stale", adsb.get("stale_points", adsb.get("bad_points")), "events"),
            fact("Unknown", adsb.get("unknown_points"), "events"),
            fact("Report issues (diagnostic)", adsb.get("report_issue_points"), "events"),
        ],
        note=note,
    )


def safe_incident_identifier(value: Any, *, max_length: int = 160) -> str:
    text = str(value or "").strip()
    if not text or len(text) > max_length:
        return ""
    if not all(character.isascii() and (character.isalnum() or character in ":_-") for character in text):
        return ""
    return text


def normalize_active_incidents(raw_items: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_items, list):
        return []
    incidents: list[dict[str, Any]] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        incident_id = safe_incident_identifier(raw.get("id"))
        if not incident_id:
            continue
        severity = str(raw.get("severity") or "unknown").strip().lower()
        if severity not in {"critical", "warning", "info"}:
            severity = "unknown"
        recovery_type = safe_incident_identifier(raw.get("recovery_type")) or "unknown"
        observed_ts = as_number(raw.get("observed_at", raw.get("observed_ts")))
        incidents.append(
            {
                "id": incident_id,
                "label": PUBLIC_INCIDENT_LABELS.get(incident_id, incident_id),
                "severity": severity,
                "recovery_type": recovery_type,
                "observed_at": int(observed_ts) if observed_ts is not None else None,
            }
        )
    return incidents


def active_incidents_payload(raw_items: Any, *, status: str) -> dict[str, Any]:
    incidents = normalize_active_incidents(raw_items)
    available = status == "ok"
    return {
        "status": "ok" if available else "unavailable",
        "count": len(incidents) if available else None,
        "items": incidents if available else [],
        "interpretation": "Current incidents are separate from notification delivery and recovery execution.",
    }


def upload_item(week: dict[str, Any], day: dict[str, Any]) -> dict[str, Any]:
    # The upload policy's official window is 24 h; the 7 d trend stays visible as facts.
    official, trend = day.get("upload_ceiling") or {}, week.get("upload_ceiling") or {}
    return row(
        item_id="upload_within_ceiling",
        tier="guardrail",
        label="Upload within 5 Mbps ceiling",
        window="rolling 24h",
        value=official.get("sli_pct"),
        unit="%",
        target=official.get("target_pct"),
        verdict=judged(official.get("sli_pct"), official.get("target_pct"), reasons=report_reasons(official),
                       good="HOLDING", bad="EXCEEDED", good_label="within ceiling", bad_label="over ceiling"),
        facts=[
            fact("Ceiling", 5.0, "Mbps"),
            fact("Max p95 (24 h)", official.get("max_value"), "Mbps"),
            fact("Minutes over (24 h)", official.get("bad_minutes"), "min"),
            fact("7 d trend", trend.get("sli_pct"), "%"),
            fact("Minutes over (7 d)", trend.get("bad_minutes"), "min"),
            fact("Longest over (7 d)", trend.get("max_bad_run_minutes"), "min"),
            fact("Max p95 (7 d)", trend.get("max_value"), "Mbps"),
        ],
        note=(
            "One-minute observations of the rolling 1 h upload p95 against the 5.0 Mbps home-uplink ceiling. "
            "This is a bandwidth constraint, not availability."
        ),
    )


def same_url_item(month: dict[str, Any]) -> dict[str, Any]:
    same_url = month.get("same_url_preservation") or {}
    raw_metric, actual = same_url.get("raw_metric") or {}, same_url.get("actual_url_evidence") or {}
    transitions = as_number(actual.get("live_url_transition_count"))
    verdict = (assessment("UNKNOWN", "no result") if transitions is None
               else assessment("HOLDING", "URL unchanged") if transitions == 0 else assessment("EXCEEDED", "URL changed"))
    return row(
        item_id="same_url_observation",
        tier="guardrail",
        label="Same YouTube URL",
        window="rolling 30d",
        value=transitions,
        unit="transitions",
        target=None,
        verdict=verdict,
        facts=[
            fact("Distinct URLs", actual.get("distinct_live_url_count"), "URLs"),
            fact("Evidence rows", actual.get("row_count"), "events"),
            fact("Raw metric", raw_metric.get("sli_pct"), "%"),
            fact("Raw bad", raw_metric.get("bad_points"), "samples"),
        ],
        note="The public watch URL stays the same; observed transition and URL counts are shown apart from the raw monitoring ratio.",
    )


def quality_item(week: dict[str, Any], day: dict[str, Any], key: str, *, item_id: str, label: str, note: str,
                 extra: list[dict[str, Any]]) -> dict[str, Any]:
    summary = week.get(key) or {}
    return row(
        item_id=item_id,
        tier="quality",
        label=label,
        window="rolling 7d",
        value=summary.get("sli_pct"),
        unit="%",
        target=summary.get("target_pct"),
        verdict=judged(summary.get("sli_pct"), summary.get("target_pct"), reasons=report_reasons(summary),
                       good="MET", bad="BELOW", good_label="at reference", bad_label="below reference"),
        facts=budget_facts(summary) + extra,
        note=note,
        recent=(day.get(key) or {}).get("sli_pct"),
    )


def build_public_payload(
    raw: dict[str, Any],
    *,
    active_incidents: Any = None,
    active_incidents_status: str = "unavailable",
    twitch_sli: Any = None,
    picture: Any = None,
    now: float | None = None,
) -> dict[str, Any]:
    current = time.time() if now is None else now
    windows = raw.get("windows") or {}
    week = windows.get("7d") or {}
    day = windows.get("24h") or {}
    month = windows.get("30d") or {}
    input_quality = week.get("youtube_input_quality") or {}

    generated_iso = str(raw.get("generated_at_utc") or "")
    generated_at = parse_time(generated_iso)
    if generated_at is None:
        generated_at = current

    def minutes(value: Any) -> float | None:
        number = as_number(value)
        return number / 60.0 if number is not None else None

    items = [
        youtube_delivery_item(week, day),
        twitch_delivery_item(twitch_sli, now=current),
        picture_freshness_item(picture, now=current),
        quality_item(week, day, "audio_correctness", item_id="audio_check_samples", label="Audio checks",
                     note="Pulse monitor energy and audio fault evidence observed by the monitoring layer.", extra=[]),
        viewer_support_item(week.get("visual_correctness") or {}),
        quality_item(
            week, day, "youtube_input_quality", item_id="youtube_input_quality", label="YouTube input quality",
            note=(
                "Fresh active YouTube OAuth health evidence. Local ingest disconnects and stale probes "
                "are outside this denominator and remain separate availability or coverage evidence."
            ),
            extra=[
                fact("Eligible duration", input_quality.get("observed_points"), "min"),
                fact("Outside denominator", minutes(input_quality.get("excluded_seconds")), "min"),
                fact("Missing evidence", minutes(input_quality.get("timeline_missing_seconds")), "min"),
                fact("Recovery warnings (separate)", (input_quality.get("raw_warning_evidence") or {}).get("warning_row_count"), "events"),
            ],
        ),
        upload_item(week, day),
        same_url_item(month),
        twitch_renewal_item(twitch_sli, now=current),
        adsb_source_item(week.get("visual_correctness") or {}),
    ]

    return {
        "schema": SCHEMA,
        "generated_at": generated_at,
        "generated_at_iso": generated_iso,
        "cadence_sec": DEFAULT_CACHE_SEC,
        "scope": "operational_reliability_indicators",
        "interpretation": INTERPRETATION,
        "tiers": [
            {"id": "slo", "label": "Viewer-facing SLOs"},
            {"id": "quality", "label": "Quality references"},
            {"id": "guardrail", "label": "Guardrails & invariants"},
        ],
        "active_incidents": active_incidents_payload(
            active_incidents,
            status=active_incidents_status,
        ),
        "items": items,
    }


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    temporary.replace(path)
