"""Qualify producer facts before generic compatibility notifications.

This adapter reads existing snapshots only. It neither queries a provider nor
changes a recovery decision, SLO, service or delivery route.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

try:
    from . import observation_time as timing
    from .viewer_evidence import probe_state, visual_state
except ImportError:
    import observation_time as timing
    from viewer_evidence import probe_state, visual_state


# Media snapshots use this gate. Independent notifier/exporter liveness rules
# retain their existing Prometheus/heartbeat owner and escalation policy.
SPECS = {
    "StreamV3CurrentFail": ("youtube_watchdog_stats.json", "youtube_watchdog", 300, "healthy"),
    "StreamV3YouTubeWatchdogUnhealthy": ("youtube_watchdog_stats.json", "youtube_watchdog", 300, "healthy"),
    "StreamV3IngestDisconnected": ("youtube_watchdog_stats.json", "youtube_watchdog", 300, "ingest_connected"),
    "StreamV3HealthPassMissingOrFalse": ("health_summary_snapshot.json", "health_snapshot", 600, "health_pass"),
    "StreamV3SubsystemsUnhealthy": ("subsystems_status.json", "subsystems", 180, "overall"),
    "StreamV3SameUrlNotLive": ("subsystems_status.json", "subsystems", 180, "same_url"),
    "StreamV3RecoveryActionPending": ("recovery_plan_observation.json", "recovery_plan", 180, "action"),
    "StreamV3AdsbEvidenceMissingOrStale": ("watchdog/adsb_freshness_state.json", "adsb_overlay_probe", 180, "adsb"),
    "StreamV3AudioEvidenceMissingOrFaulted": ("watchdog/pulse_health_state.json", "audio_probe", 180, "audio"),
    "StreamV3MapDeliveryCriticalUnhealthy": ("map_runtime_status.json", "map_runtime", 360, "delivery_critical_ok"),
    "StreamV3MapMonitorSampleMissingOrStale": ("map_runtime_status.json", "map_runtime", 360, "monitor"),
    "StreamV3MapBrowserWebGLFailure": ("map_runtime_status.json", "map_runtime", 360, "webgl"),
    "StreamV3MapPrecipitationUnavailable": ("map_runtime_status.json", "map_runtime", 360, "weather_ok"),
    "StreamV3MapRuntimeContainerRestarted": ("map_runtime_status.json", "map_runtime", 360, "restart"),
    "StreamV3ViewerSyntheticSampleMissingOrStale": ("viewer_synthetic_status.json", "viewer_synthetic", 600, "monitor"),
    "StreamV3ViewerSyntheticProbeFailed": ("viewer_synthetic_status.json", "viewer_synthetic", 600, "probe_ok"),
    "StreamV3ViewerSyntheticVisualFailure": ("viewer_synthetic_status.json", "viewer_synthetic", 600, "visual"),
}


def assess(payload, condition, *, now=None):
    if condition in {"probe_ok", "visual"}:
        if payload.get("probe_errors"):
            return "UNKNOWN"
        value = probe_state(payload) if condition == "probe_ok" else visual_state(payload)
        return "GOOD" if value is True else "BAD" if value is False else "UNKNOWN"
    if condition in {"healthy", "ingest_connected", "delivery_critical_ok", "weather_ok"}:
        if payload.get("probe_errors"):
            return "UNKNOWN"
        value = payload.get(condition)
        return "GOOD" if value is True else "BAD" if value is False else "UNKNOWN"
    if condition == "monitor":
        return "GOOD" if payload.get("status") in {"ok", "healthy", "failed", "degraded", "visual_failure"} else "UNKNOWN"
    if condition == "health_pass":
        if payload.get("ok") is not True:
            return "UNKNOWN"
        values = [x.get("pass") for x in payload.get("payload", {}).get("windows", [])]
        return "GOOD" if values and all(x is True for x in values) else "BAD" if any(x is False for x in values) else "UNKNOWN"
    if condition == "overall":
        value = payload.get("overall", {}).get("state")
        return "GOOD" if value == "healthy" else "BAD" if value in {"failed", "degraded"} else "UNKNOWN"
    if condition == "same_url":
        value = payload.get("overall", {}).get("stream_public_state")
        return "GOOD" if value == "same_url_live" else "BAD" if value in {"offline", "same_url_offline"} else "UNKNOWN"
    if condition == "action":
        value = payload.get("action")
        return "UNKNOWN" if not isinstance(value, str) else "GOOD" if value in {"", "none"} else "BAD"
    if condition == "adsb":
        bounds = timing.source_lag_bounds_at_collection(payload, now=now)
        if bounds is None:
            return "UNKNOWN"
        if type(payload.get("source_heartbeat_ok")) is not bool:
            return "UNKNOWN"
        low, high = bounds
        if high <= 30 and payload["source_heartbeat_ok"] is True:
            return "GOOD"
        return "BAD" if low > 30 else "UNKNOWN"
    if condition == "audio":
        keys = ("dj_missing_count", "capture_missing_count", "dj_latency_high_count", "capture_latency_high_count")
        values = [payload.get(k) for k in keys]
        if not all(timing.finite(x) and x >= 0 for x in values):
            return "UNKNOWN"
        return "GOOD" if all(x == 0 for x in values) else "BAD"
    if condition == "webgl":
        value = payload.get("browser", {})
        flags = [value.get("webgl2_blocklisted"), value.get("context_fatal_failure")]
        return "GOOD" if all(x is False for x in flags) else "BAD" if any(x is True for x in flags) else "UNKNOWN"
    if condition == "restart":
        counts = [v.get("restart_count") for v in payload.get("pod", {}).get("containers", {}).values()]
        return "GOOD" if counts and all(x == 0 for x in counts) else "BAD" if any(timing.finite(x) and x > 0 for x in counts) else "UNKNOWN"
    return "UNKNOWN"


class Evidence(dict):
    """Keep original metadata so input collection cannot renew a sample's TTL."""
    def __init__(self, payloads, *, now):
        self.payloads = payloads
        super().__init__(self.at(now))

    def at(self, now):
        result = {}
        for name, payload in self.payloads.items():
            if name not in SPECS:
                result[name] = (timing.Qualified("UNVERIFIABLE", "notification_rule_not_mapped"), "UNKNOWN")
                continue
            _file, source, max_age, condition = SPECS[name]
            sample = timing.qualify(payload.get("observation_time"), now=now, source=source, max_age=max_age)
            try:
                verdict = assess(payload, condition, now=now) if sample.valid else "UNKNOWN"
            except (AttributeError, TypeError, ValueError, KeyError):
                verdict = "UNKNOWN"
            result[name] = (sample, verdict)
        return result


def read(root, names, *, now):
    payloads, cache = {}, {}
    for name in names:
        if name not in SPECS:
            payloads[name] = {}
            continue
        file, source, max_age, condition = SPECS[name]
        if file not in cache:
            try:
                with (Path(root) / file).open("rb") as handle:
                    raw = handle.read(1024 * 1024 + 1)
                if len(raw) > 1024 * 1024:
                    raise ValueError("oversized_notification_source")
                parsed = json.loads(raw)
                cache[file] = parsed if isinstance(parsed, dict) else {}
            except (OSError, ValueError):
                cache[file] = {}
        payloads[name] = cache[file]
    return Evidence(payloads, now=now)


def _timing_limits(name):
    # Keep legacy stored episodes readable if a rule is no longer mapped.
    max_age = SPECS.get(name, (None, None, 180, None))[2]
    return {"max_gap": max_age, "lost_after": max(300, max_age)}


def _advance(state, sample, verdict, *, name, now):
    return timing.advance(state, sample, verdict, now=now, **_timing_limits(name))


def gate(alerts, state, *, now, evidence, alert_key):
    """Split held/pending episodes from the legacy router; never synthesize health."""
    current_alerts = {alert_key(x): x for x in alerts if x.get("state") == "firing"}
    allowed_alerts, allowed_state, held = [], {}, {}
    for key in state.keys() | current_alerts.keys():
        prior = state.get(key, {})
        alert = current_alerts.get(key)
        name = (alert.get("labels", {}).get("alertname") if alert else prior.get("name")) or ""
        if name not in SPECS:
            if prior:
                allowed_state[key] = prior
            if alert:
                allowed_alerts.append(alert)
            continue
        sample, verdict = evidence.get(name, (timing.Qualified("UNVERIFIABLE", "notification_source_missing"), "UNKNOWN"))
        slot, ready, lost = _advance(prior.get("observation_continuity"), sample, verdict, name=name, now=now)
        row = {**prior, "name": name, "observation_continuity": slot,
               "observation_lost": lost, "observation_reason": sample.reason}
        source_slot, source_ready, _ = _advance(prior.get("source_continuity"), sample,
            "GOOD" if sample.valid and verdict != "UNKNOWN" else "UNKNOWN", name=name, now=now)
        row.update(source_continuity=source_slot, observation_restored=source_ready == "GOOD")
        if not prior:
            row.update(since=now.wall, last_sent=0, channel="discord", title=name, observation_pending=True)
        if ready == "BAD" and alert:
            allowed_alerts.append(alert)
            if not row.get("observation_pending"):
                allowed_state[key] = row
        elif ready == "GOOD":
            if not row.get("observation_pending"):
                allowed_state[key] = row
        else:
            held[key] = row
    return allowed_alerts, allowed_state, held


def decorate(current, held, evidence, *, now):
    for key, row in current.items():
        name = row.get("name")
        if name not in SPECS:
            continue
        sample, verdict = evidence.get(name, (timing.Qualified("UNVERIFIABLE", "notification_source_missing"), "UNKNOWN"))
        slot, _ready, lost = _advance(row.get("observation_continuity"), sample, verdict, name=name, now=now)
        row.update(observation_continuity=slot, observation_lost=lost, observation_reason=sample.reason)
        source_slot, source_ready, _ = _advance(row.get("source_continuity"), sample,
            "GOOD" if sample.valid and verdict != "UNKNOWN" else "UNKNOWN", name=name, now=now)
        # gate() may already have counted this sample. Its duplicate read here
        # must preserve the restoration proved earlier in this planning cycle.
        row.update(source_continuity=source_slot,
                   observation_restored=row.get("observation_restored") is True or source_ready == "GOOD")
        row.pop("observation_pending", None)
    return {**current, **held}


def loss_messages(current, previous, *, now):
    messages = []
    for key, row in current.items():
        if row.get("observation_lost") and not row.get("observation_loss_notified"):
            seconds = _timing_limits(row["name"])["lost_after"]
            messages.append({"key": key, "channel": "discord", "content":
                f"🟠 No new qualified monitoring observation for at least {seconds:g} seconds\nTarget: " + row["name"] +
                "\nDelivery failure or recovery is unconfirmed.\nReason: " + row.get("observation_reason", "unknown")})
            row["observation_loss_notified"] = True
        elif row.get("observation_loss_notified") and row.get("observation_restored"):
            messages.append({"key": key, "channel": "discord", "content":
                "✅ Monitoring resumed with distinct fresh observations\nTarget: " + row["name"] +
                "\nThis does not assert media recovery."})
            row.pop("observation_loss_notified", None)
    for key, row in previous.items():
        if key not in current and row.get("observation_loss_notified"):
            messages.append({"key": key, "channel": "discord", "content":
                "✅ Monitoring resumed with distinct fresh observations\nTarget: " + row["name"] +
                "\nThis does not assert media recovery."})
    return messages
