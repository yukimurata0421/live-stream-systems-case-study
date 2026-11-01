"""Bridge legacy incident IDs to producer observations, without recovery effects."""
from __future__ import annotations

from pathlib import Path
import os

from stream_core.common.json_io import read_json_file
from stream_core.common import observation_time as timing


class Collection(list):
    # A list-compatible return keeps existing callers and pinned extensions valid.
    def __init__(self, items, observations, reading):
        list.__init__(self, items)
        self.observations, self.reading = observations, reading


SOURCES = {
    "youtube_watchdog": ("youtube_watchdog_stats.json", 300),
    "youtube_oauth": ("youtube_watchdog_stats.json", 600),
    "map_runtime": ("map_runtime_status.json", 360),
    "viewer_synthetic": ("viewer_synthetic_status.json", 600),
    "reliability_rollup": ("operational_reliability_status.json", 7200),
    "reliability_burn": ("operational_reliability_burn_status.json", 7200),
    "external_blackbox": ("external_blackbox_status.json", 900),
    "adsb_overlay_probe": ("watchdog/adsb_freshness_state.json", 180),
    "audio_probe": ("watchdog/pulse_health_state.json", 180),
    "subsystems": ("subsystems_status.json", 180),
    "recovery_plan": ("recovery_plan_observation.json", 180),
    "observe_health": (None, 180),
    "observe_execution": (None, 180),
    "stream1090_overlay_report": (None, 1800),
    "stream1090_upstream_report": (None, 1800),
}


def source_for(ident):
    if ident.startswith("observe:"):
        return "observe_execution"
    if ident.startswith(("api_report:", "ffmpeg:", "remote_warning:")):
        return "observe_health"
    if ident == "stream1090:overlay_report":
        return "stream1090_overlay_report"
    if ident == "stream1090:upstream_report":
        return "stream1090_upstream_report"
    if ident.startswith("map:"):
        return "map_runtime"
    if ident.startswith("viewer:"):
        return "viewer_synthetic"
    if ident.startswith("external:"):
        return "external_blackbox"
    if ident.startswith("reliability:"):
        # Formal coverage/current-source reconciliation has its own owner.
        # Measurement validity alone is not that owner's business verdict.
        if ident.endswith(("_coverage_or_freshness", "_source_disagreement")):
            return None
        return "reliability_rollup" if ident.endswith("rollup_missing_or_stale") else "reliability_burn"
    if ident.startswith(("stream:", "youtube:", "resolver:", "public_probe:")):
        return "youtube_watchdog"
    return None


def complete(source, payload):
    """Absent incidents alone are not a healthy producer result."""
    if source == "youtube_watchdog":
        return (type(payload.get("healthy")) is bool and type(payload.get("local_ok")) is bool
                and payload.get("status") in ("ok", "warn", "error"))
    if source == "youtube_oauth":
        return payload.get("oauth_probe_ok") is True and payload.get("oauth_stream_health_status") in ("good", "ok", "bad", "error", "warning")
    if source == "map_runtime":
        return (payload.get("status") in ("healthy", "failed", "degraded")
                and not payload.get("probe_errors") and type(payload.get("delivery_critical_ok")) is bool)
    if source == "viewer_synthetic":
        return payload.get("probe_ok") is True or payload.get("status") in ("ok", "healthy", "visual_failure")
    if source.startswith("reliability_"):
        return (isinstance(payload.get("multi_window_burn_alerts"), list)
                and isinstance(payload.get("formal_gates", payload.get("fast_feedback")), dict))
    if source == "external_blackbox":
        return payload.get("status") in ("ok", "failed")
    if source == "adsb_overlay_probe":
        return type(payload.get("source_heartbeat_ok")) is bool and payload.get("status") in ("ok", "unhealthy")
    if source == "audio_probe":
        return all(type(payload.get(k)) in (int, float) for k in ("dj_missing_count", "capture_missing_count"))
    if source == "subsystems":
        return payload.get("overall", {}).get("state") in ("healthy", "failed", "degraded")
    if source == "recovery_plan":
        return payload.get("action") is not None and type(payload.get("execute")) is bool
    if source == "observe_health":
        return isinstance(payload.get("checks"), dict) and type(payload.get("pass")) is bool
    if source == "observe_execution":
        return type(payload.get("success")) is bool
    if source.startswith("stream1090_"):
        return payload.get("judgment") in ("report_only_ok", "report_only_warn", "report_only_fail")
    return False


def normal_for(ident, source, payload, observe):
    if source == "observe_execution":
        return payload.get("success") is True
    if source == "observe_health":
        if ident.startswith("api_report:"):
            return payload.get("api_report_timers_active") is True and payload.get("api_report_judgment") == "ok"
        return payload.get("pass") is True
    if source.startswith("stream1090_"):
        return payload.get("judgment") == "report_only_ok"
    if source == "youtube_watchdog":
        checks = observe.get("checks") if isinstance(observe.get("checks"), dict) else {}
        if ident == "stream:current_fail":
            return (checks.get("current_fail") is False and payload.get("healthy") is True
                    and payload.get("local_ok") is True and payload.get("ingest_connected") is True)
        if ident.startswith("resolver:"):
            return (observe.get("fast_mode_current_active") is False
                    or checks.get("fast_mode_current_active") is False) and payload.get("healthy") is True
        if ident.startswith("public_probe:"):
            return payload.get("healthy") is True and payload.get("local_ok") is True
        return payload.get("healthy") is True
    if source == "map_runtime":
        if ident == "map:container_restart":
            containers = (payload.get("pod") or {}).get("containers", {})
            return isinstance(containers, dict) and bool(containers) and all(isinstance(x, dict) and x.get("restart_count") == 0 for x in containers.values())
        if "precipitation" in ident:
            return payload.get("weather_ok") is True
        return payload.get("delivery_critical_ok") is True
    if source == "viewer_synthetic":
        return payload.get("probe_ok") is True and payload.get("frame_ok") is True
    if source.startswith("reliability_"):
        family = ident.removeprefix("reliability:").split("_multi_window_burn")[0].split("_coverage_or_freshness")[0]
        if ident.endswith("_multi_window_burn") and any(
                isinstance(alert, dict) and alert.get("family") == family
                for alert in payload.get("multi_window_burn_alerts", [])):
            return False
        gates = payload.get("formal_gates", payload.get("fast_feedback", {}))
        if ident.endswith("missing_or_stale"):
            return True
        gate = gates.get(family, {})
        if "youtube_input_quality" in ident:
            feedback = payload.get("fast_feedback", {}).get("youtube_input_quality", {})
            if "fast_feedback" in ident:
                return feedback.get("raw_current", {}).get("good") is True
            gate = gate or feedback
        return (gate.get("measurement_status") == "valid"
                or payload.get("measurement_status_by_family", {}).get(family) == "valid")
    if source == "external_blackbox":
        return payload.get("status") == "ok"
    return False


def _complete(source, payload):
    try:
        return complete(source, payload)
    except (AttributeError, TypeError, ValueError, KeyError):
        return False


def _normal(ident, source, payload, observe):
    try:
        return normal_for(ident, source, payload, observe)
    except (AttributeError, TypeError, ValueError, KeyError):
        return False


def collect(root, items, observe, *, now=None, report_samples=None, execution_ok=None):
    now = timing.capture() if now is None else now
    result = {"sources": {}, "bad_ids": {x["id"] for x in items}, "observe": observe,
              "optional_unconfigured_sources": set()}
    payloads = {}
    for name, (file, max_age) in SOURCES.items():
        if file is None:
            continue
        if file not in payloads:
            payloads[file] = read_json_file(Path(root) / file)
        payload = payloads[file]
        # K8s observes audio through its own subsystem owner. A local PulseAudio
        # producer may never have existed here. Once registered, its loss is
        # still tracked by the durable source slot in reconcile().
        if name == "audio_probe" and not payload and os.environ.get("STREAM_RUNTIME_SUPERVISOR") == "k8s":
            result["optional_unconfigured_sources"].add(name)
        if name == "youtube_oauth":
            payload = {**payload, "observation_time": payload.get("oauth_observation_time")}
        sample = timing.qualify(payload.get("observation_time"), now=now, max_age=max_age, source=name)
        result["sources"][name] = (sample, payload, _complete(name, payload))
    dynamic = {"observe_health": observe,
               "observe_execution": timing.attach({"success": execution_ok}, "observe_execution", now=now)}
    for ident, payload in (report_samples or {}).items():
        dynamic[source_for(ident)] = payload
    for name, payload in dynamic.items():
        sample = timing.qualify(payload.get("observation_time"), now=now, source=name, max_age=SOURCES[name][1])
        result["sources"][name] = (sample, payload, _complete(name, payload))
    return Collection(items, result, now)


def evidence(collection, ident, *, now):
    source = source_for(ident)
    value = collection.get("sources", {}).get(source)
    if value is None:
        return timing.Qualified("UNVERIFIABLE", "notification_source_not_mapped"), "UNKNOWN"
    sample, payload, is_complete = value
    if ident == "reliability:youtube_input_quality_fast_feedback":
        feedback = payload.get("fast_feedback")
        feedback = feedback if isinstance(feedback, dict) else {}
        current = feedback.get("youtube_input_quality")
        current = current if isinstance(current, dict) else {}
        raw = current.get("raw_current")
        raw = raw if isinstance(raw, dict) else {}
        sample = timing.qualify(raw.get("observation_time"), now=now, max_age=600., source="youtube_oauth")
        if sample.valid and raw.get("available") is True and raw.get("eligible") is True:
            if raw.get("good") is True:
                return sample, "GOOD"
            if raw.get("good") is False and ident in collection["bad_ids"]:
                return sample, "BAD"
        return sample, "UNKNOWN"
    # Qualify again at the actual decision point; a long collection cannot renew TTL.
    sample = timing.qualify(payload.get("observation_time"), now=now, max_age=SOURCES[source][1], source=source)
    if not sample.valid or not is_complete:
        return sample, "UNKNOWN"
    if ident in collection["bad_ids"]:
        if _normal(ident, source, payload, collection["observe"]):
            return sample, "UNKNOWN"
        return sample, "BAD"
    return sample, "GOOD" if _normal(ident, source, payload, collection["observe"]) else "UNKNOWN"


def reconcile(items, active, state, collection, *, now, config):
    """Keep open episodes through missing samples; close only on verified health."""
    slots = state.setdefault("observation_continuity", {})
    incoming = {item["id"]: item for item in items}
    output = []
    recoveries = state.setdefault("observation_recoveries", {})
    good_hold = float(config.get("recovery_confirmation_sec", 45))
    lost_after = float(config.get("observation_lost_after_sec", 300))
    quality = "reliability:youtube_input_quality_fast_feedback"
    for ident in incoming.keys() | active.keys():
        if ident.startswith("observation:"):
            continue
        if source_for(ident) is None:
            # Legacy owner-specific adapters retain their own evidence rules.
            if ident in incoming:
                output.append(incoming[ident])
            continue
        item = incoming.get(ident)
        sample, verdict = evidence(collection, ident, now=now)
        hold = float(config.get("quality_warning_hold_sec", 600)) if ident == quality and item and item.get("severity") == "warning" else 0.
        slot, ready, _lost = timing.advance(slots.get(ident), sample, verdict, now=now,
            bad_hold=hold, good_hold=good_hold, max_gap=SOURCES[source_for(ident)][1],
            lost_after=max(lost_after, SOURCES[source_for(ident)][1]))
        slots[ident] = slot
        if verdict == "BAD" and sample.valid and (ready == "BAD" or ident in active):
            metadata=collection["sources"][source_for(ident)][1]["observation_time"]
            if ident==quality:
                metadata=collection["sources"][source_for(ident)][1]["fast_feedback"]["youtube_input_quality"]["raw_current"]["observation_time"]
            output.append({**item, "observation_time": metadata,
                           "_active_for_sec": max(0., now.mono - slot.get("since", now.mono))})
        elif ident in active and ready != "GOOD":
            previous = active[ident].get("last_incident")
            if previous:
                output.append({**previous, "_observation_pending": True})
        elif ident in active:
            recoveries[ident] = {"sample_id": sample.sample, "source": sample.source,
                                 "age_upper_sec": sample.age_upper, "utc_status": sample.utc_status}
        else:
            recoveries.pop(ident, None)
    # Track source loss independently of any particular media incident.
    for source, (sample, payload, complete_source) in collection.get("sources", {}).items():
        ident = "observation:" + source + ":unavailable"
        key = "source:" + source
        if source in collection.get("optional_unconfigured_sources", ()) and key not in slots:
            continue
        slot, ready, lost = timing.advance(slots.get(key), sample,
            "GOOD" if complete_source else "UNKNOWN", now=now, good_hold=good_hold,
            max_gap=SOURCES[source][1], lost_after=max(lost_after, SOURCES[source][1]))
        slots[key] = slot
        if lost or ident in active and ready != "GOOD":
            output.append({"id": ident, "severity": "warning", "component": "observation_source",
                "summary": "Fresh monitoring evidence is unavailable (delivery failure is unconfirmed)",
                "evidence": f"source={source} status={sample.status} reason={sample.reason} age_upper={sample.age_upper}",
                "recovery_type": "measurement_source_recovery", "observed_ts": int(now.wall),
                "follow_up": "Check the original sample, boot, clock diagnostics and collection path", "repeat_sec": 900})
    # One physical clock failure produces one notice, not one per component.
    diagnostic = timing.clock_diagnostic(now)
    clock_id, clock_key = "observation:host_clock:unavailable", "clock:local"
    observed = diagnostic.get("mono")
    clock_sample = (timing.Qualified("VALID", "clock_observed", "host_clock", now.host, now.boot,
                    str(observed), observed, now.mono-observed)
                    if timing.finite(observed) else timing.Qualified("UNVERIFIABLE", "clock_observation_unavailable"))
    step = diagnostic.get("wall_step_seconds")
    fault = diagnostic.get("synchronized") is False or timing.finite(step) and abs(step) > .050
    clock_slot, clock_ready, clock_lost = timing.advance(slots.get(clock_key), clock_sample,
        "BAD" if fault else "GOOD" if diagnostic.get("synchronized") is True else "UNKNOWN", now=now,
        bad_hold=lost_after, good_hold=good_hold)
    slots[clock_key] = clock_slot
    if clock_ready == "BAD" or clock_lost or clock_id in active and clock_ready != "GOOD":
        output.append({"id": clock_id, "severity": "warning", "component": "observation_clock",
            "summary": "Check UTC synchronization or the clock observer (delivery failure is unconfirmed)",
            "evidence": f"host={now.host} ntp={diagnostic}",
            "recovery_type": "clock_observation_recovery", "observed_ts": int(now.wall),
            "follow_up": "Check NTP, boot and clock corrections; this diagnosis is separate from boottime freshness", "repeat_sec": 900})
    # Bound persistent candidates independently of the lifetime of the service.
    keep = set(incoming) | set(active) | {"source:" + name for name in SOURCES} | {clock_key}
    for key in list(slots):
        if key not in keep:
            slots.pop(key)
    return output
