from __future__ import annotations

SUBSYSTEM = "rendering"

FAILURE_OVERLAY_UNAVAILABLE = "overlay_unavailable"
FAILURE_STREAM1090_UNAVAILABLE = "stream1090_unavailable"
FAILURE_ADSB_FRESHNESS_STALL = "adsb_freshness_stall"
FAILURE_VIDEO_FRAME_UNHEALTHY = "video_frame_unhealthy"
FAILURE_RUNTIME_SNAPSHOT_STALE = "runtime_snapshot_stale"
FAILURE_UPSTREAM_STREAM1090_UNAVAILABLE = "upstream_stream1090_unavailable"

ACTION_NONE = "none"
ACTION_RELOAD_OVERLAY = "reload_overlay"
ACTION_RESTART_BROWSER = "restart_browser"

RECOVERY_ORDER = (
    ACTION_RELOAD_OVERLAY,
    ACTION_RESTART_BROWSER,
)

BLOCK_YOUTUBE_LIFECYCLE = "rendering_failure_never_authorizes_youtube_lifecycle_action"
