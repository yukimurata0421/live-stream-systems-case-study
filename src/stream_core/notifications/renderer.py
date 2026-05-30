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
    if phase == "auto_recovered":
        lines = [
            "[ADS-B Stream] 自動復旧イベント",
            f"time={jst_text(now_ts)}",
            "active_incidents=0",
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
                    f"  follow_up={item.get('follow_up')}",
                ]
            )
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
            "[ADS-B Stream] メンテナンス継続中",
            f"time={jst_text(now_ts)}",
            "maintenance=on",
            f"window={jst_text_or_unknown(first_seen)} -> ongoing duration={duration}",
            "",
            "component=maintenance_mode severity=info",
            "status=自動復旧・監視・report timer を一時停止中です。配信本体と AutoDJ は停止対象外です。",
            "notify=Discord reminder は継続しています。",
            f"evidence={item.get('evidence', '')}",
            f"follow_up={item.get('follow_up', '')}",
        ]
        return "\n".join(lines)

    title = {
        "detected": "障害検知",
        "status": "障害継続ステータス",
        "recovered": "復旧フォローアップ",
        "test": "通知テスト",
    }.get(phase, phase)
    active_count = 0 if phase == "recovered" else len(incidents)
    lines = [f"[ADS-B Stream] {title}", f"time={jst_text(now_ts)}", f"active_incidents={active_count}"]
    if phase == "recovered":
        lines.append(f"resolved_incidents={len(incidents)}")
    if phase == "test":
        lines.append("stream-new notify-status の Discord webhook 接続確認です。")
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
        if phase == "recovered":
            lines.append(f"  recovery_evidence={item.get('_recovery_evidence', '')}")
        lines.append(f"  follow_up={item.get('follow_up')}")
    if len(incidents) > 8:
        lines.append(f"... {len(incidents) - 8} more incidents omitted")
    content = "\n".join(lines)
    if len(content) > 1900:
        content = content[:1850] + "\n... truncated"
    return content
