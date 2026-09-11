from __future__ import annotations

from datetime import timedelta, timezone

from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.time import parse_utc, unix_ts


JST = timezone(timedelta(hours=9))
PHASE_LABELS = {"detected": "障害検知", "repeat": "障害継続ステータス", "recovered": "復旧通知"}
TEMPLATE_REVISION = "monitoring-v4-ja-r5.2"


def _jst(value: str) -> str:
    return parse_utc(value).astimezone(JST).strftime("%Y-%m-%d %H:%M:%S JST")


def render(transition: IncidentTransition, episode: IncidentEpisode) -> tuple[str, str]:
    ffmpeg_self_restart = (
        transition.phase == "recovered"
        and "ffmpeg_child_auto_recovered" in transition.reason_codes
    )
    label = "FFmpeg自己再起動イベント" if ffmpeg_self_restart else PHASE_LABELS[transition.phase]
    subject = f"[ADS-B Stream] {label}: {transition.domain}"
    duration = max(0, unix_ts(transition.occurred_at) - unix_ts(episode.opened_at))
    reasons = ",".join(transition.reason_codes) or "none"
    recovery_type = (
        "ffmpeg_child_self_recovery"
        if "ffmpeg_child_auto_recovered" in transition.reason_codes
        else "human_review_no_automatic_restart"
    )
    content = "\n".join(
        (
            f"[ADS-B Stream] {label}",
            f"time={_jst(transition.occurred_at)}",
            f"component={transition.domain} severity={transition.severity}",
            f"episode_id={transition.episode_id} phase={transition.phase} duration_sec={duration}",
            f"issue={transition.summary}",
            f"reason_codes={reasons}",
            f"recovery_type={recovery_type}",
        )
    )
    return subject, content
