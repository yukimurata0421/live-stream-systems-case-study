from __future__ import annotations

from ..model import ActionCandidate, SubsystemsSnapshot


class ActionProposer:
    """Pure action proposal from subsystem state."""

    def propose(self, snapshot: SubsystemsSnapshot) -> list[ActionCandidate]:
        candidates: list[ActionCandidate] = []
        rendering = snapshot.rendering
        music = snapshot.music
        local = snapshot.local_delivery
        youtube = snapshot.youtube_lifecycle
        monitoring = snapshot.monitoring

        if rendering.state == "failed":
            candidates.append(ActionCandidate("restart_browser", "rendering", 20, "low", True, True))
        elif rendering.state == "degraded":
            candidates.append(ActionCandidate("reload_overlay", "rendering", 10, "low", True, True))

        local_failure_active = local.state in {"failed", "degraded"}
        if music.state in {"failed", "degraded"} and not local_failure_active:
            candidates.append(ActionCandidate("restart_dj", "music", 20, "low", True, True))

        if local.state == "failed":
            if local.recommended_action == "restart_stream":
                candidates.append(ActionCandidate("restart_stream", "stream_all", 30, "high", True, True))
                candidates.append(ActionCandidate("restart_ffmpeg", "local_delivery", 40, "medium", True, True))
            else:
                candidates.append(ActionCandidate("restart_ffmpeg", "local_delivery", 30, "medium", True, True))
                candidates.append(ActionCandidate("restart_stream", "stream_all", 40, "high", True, True))
        elif local.state == "degraded":
            candidates.append(ActionCandidate("restart_ffmpeg", "local_delivery", 30, "medium", True, True))

        if youtube.state in {"degraded", "unknown"}:
            if "inconsistent_remote" in youtube.evidence or youtube.recommended_action == "resync_resolver":
                candidates.append(ActionCandidate("resync_resolver", "youtube_lifecycle", 15, "low", True, True))
            else:
                candidates.append(ActionCandidate("retry_probe", "youtube_lifecycle", 10, "low", True, True))
        if youtube.state == "failed":
            if youtube.extra.get("current_url_recoverable"):
                candidates.append(ActionCandidate("force_current_broadcast_live", "youtube_lifecycle", 50, "high", True, True))
            else:
                candidates.append(ActionCandidate("retry_probe", "youtube_lifecycle", 10, "low", True, True))

        replacement_policy = youtube.extra.get("replacement_policy") if isinstance(youtube.extra.get("replacement_policy"), dict) else {}
        replacement_allowed = bool(replacement_policy.get("allowed"))
        replacement_blocked_by = [] if replacement_allowed else [str(replacement_policy.get("reason") or "replacement_not_allowed")]
        replacement_blocked_by.extend([str(x) for x in replacement_policy.get("required_missing", []) if x])
        candidates.append(ActionCandidate("create_replacement_broadcast", "youtube_lifecycle", 90, "very_high", False, replacement_allowed, replacement_blocked_by))

        if monitoring.state in {"degraded", "unknown"}:
            candidates.append(ActionCandidate("alert", "monitoring", 5, "none", True, True))

        if not candidates:
            candidates.append(ActionCandidate("none", "none", 0, "none", True, True))
        return sorted(candidates, key=lambda c: c.priority)
