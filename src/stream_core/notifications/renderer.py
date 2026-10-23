from __future__ import annotations

try:
    from stream_core.common.timeutil import jst_text, jst_text_or_unknown
except ModuleNotFoundError:
    from common.timeutil import jst_text, jst_text_or_unknown


def seconds_to_human(seconds: int | float | None) -> str:
    try:
        total = max(0, int(seconds or 0))
    except Exception:
        total = 0
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def format_discord_message(*, phase: str, incidents: list[dict], state: dict, now_ts: int) -> str:
    recovery_titles = {
        "restart_observed": "Restart observed; publishing recovery pending",
        "recovery_unconfirmed": "Publishing recovery unconfirmed after restart",
        "auto_recovered": "Automatic recovery confirmed",
        "planned_rollout": "Planned rollout completed",
    }
    if phase in recovery_titles:
        title = recovery_titles[phase]
        components = {str(item.get("component", "")) for item in incidents}
        if phase == "auto_recovered" and components == {"stream_engine_ffmpeg"}:
            title = "FFmpeg self-restart event"
        lines = [
            f"[ADS-B Stream] {title}",
            f"time={jst_text(now_ts)}",
            f"active_incidents={len(state.get('active') or {})}",
            f"events={len(incidents)}",
        ]
        for item in incidents[:8]:
            lines.extend(
                [
                    "",
                    f"- component={item.get('component')} severity={item.get('severity')}",
                    f"  event_at={jst_text_or_unknown(int(item.get('observed_ts', 0) or 0))}",
                    f"  trigger={item.get('trigger', '')}",
                    f"  action={item.get('summary')}",
                    f"  recovery_type={item.get('recovery_type')}",
                    f"  evidence={item.get('evidence')}",
                ]
            )
            if "restart_observed" in item:
                lines.extend(
                    [
                        f"  restart_observed={str(bool(item.get('restart_observed'))).lower()}",
                        f"  recovery_confirmed={str(bool(item.get('recovery_confirmed'))).lower()}",
                        f"  recovery_lag_sec={item.get('recovery_lag_sec') if item.get('recovery_lag_sec') is not None else 'unknown'}",
                    ]
                )
            if item.get("diagnostic_context"):
                lines.append(f"  context={item.get('diagnostic_context')}")
            lines.append(f"  follow_up={item.get('follow_up')}")
        if len(incidents) > 8:
            lines.append(f"... {len(incidents) - 8} more events omitted")
        content = "\n".join(lines)
        if len(content) > 1900:
            content = content[:1850] + "\n... truncated"
        return content

    if phase == "maintenance":
        item = incidents[0] if incidents else {}
        first_seen = int(item.get("_first_seen_ts", now_ts) or now_ts)
        duration = seconds_to_human(now_ts - first_seen)
        lines = [
            "[ADS-B Stream] Maintenance ongoing",
            f"time={jst_text(now_ts)}",
            "maintenance=on",
            f"window={jst_text_or_unknown(first_seen)} -> ongoing duration={duration}",
            "",
            "component=maintenance_mode severity=info",
            "status=Automatic recovery, monitoring, and report timers are paused. Streaming and AutoDJ remain running.",
            "notify=Discord reminders remain active.",
            f"evidence={item.get('evidence', '')}",
            f"follow_up={item.get('follow_up', '')}",
        ]
        return "\n".join(lines)

    title = {
        "detected": "Incident detected",
        "status": "Ongoing incident status",
        "recovered": "Recovery follow-up",
        "test": "Notification test",
    }.get(phase, phase)
    active = state.get("active") if isinstance(state.get("active"), dict) else {}
    active_count = len(active)
    lines = [f"[ADS-B Stream] {title}", f"time={jst_text(now_ts)}", f"active_incidents={active_count}"]
    if phase == "recovered":
        lines.append(f"resolved_incidents={len(incidents)}")
    if phase == "test":
        lines.append("Discord webhook connectivity check from stream-new notify-status.")
    for item in incidents[:8]:
        ident = str(item.get("id", ""))
        active_state = state.get("active", {}).get(ident, {}) if isinstance(state.get("active"), dict) else {}
        first_seen = int(item.get("_first_seen_ts", active_state.get("first_seen_ts", now_ts)) or now_ts)
        first_notified = int(item.get("_first_notified_ts", active_state.get("first_notified_ts", first_seen)) or first_seen)
        last_bad = int(item.get("_last_bad_ts", active_state.get("last_bad_ts", item.get("observed_ts", 0))) or 0)
        recovered_ts = int(item.get("_recovered_ts", 0) or 0)
        end_ts = recovered_ts if phase == "recovered" and recovered_ts > 0 else now_ts
        duration = seconds_to_human(end_ts - first_seen)
        window_end = jst_text_or_unknown(recovered_ts) if phase == "recovered" else "ongoing"
        evidence_label = "last_bad_evidence" if phase == "recovered" else "evidence"
        lines.extend(
            [
                "",
                f"- component={item.get('component')} severity={item.get('severity')}",
                f"  window={jst_text_or_unknown(first_seen)} -> {window_end} duration={duration}",
                f"  detected_at={jst_text_or_unknown(first_notified)} last_bad_sample_at={jst_text_or_unknown(last_bad)}",
                f"  issue={item.get('summary')}",
                f"  recovery_type={item.get('recovery_type')}",
                f"  {evidence_label}={item.get('evidence')}",
            ]
        )
        if item.get("diagnostic_context"):
            lines.append(f"  context={item.get('diagnostic_context')}")
        if phase == "recovered":
            lines.append(f"  recovery_evidence={item.get('_recovery_evidence', '')}")
        lines.append(f"  follow_up={item.get('follow_up')}")
    if len(incidents) > 8:
        lines.append(f"... {len(incidents) - 8} more incidents omitted")
    content = "\n".join(lines)
    if len(content) > 1900:
        content = content[:1850] + "\n... truncated"
    return content
