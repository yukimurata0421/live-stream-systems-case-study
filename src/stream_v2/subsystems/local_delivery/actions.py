from __future__ import annotations

SUBSYSTEM = "local_delivery"

FAILURE_FFMPEG_MISSING = "ffmpeg_missing"
FAILURE_INGEST_DISCONNECTED = "ingest_disconnected"
FAILURE_RUNTIME_HEARTBEAT_STALE = "runtime_heartbeat_stale"
FAILURE_TCP_STALL = "tcp_stall"
FAILURE_STREAM_FFMPEG_DUPLICATE = "stream_ffmpeg_duplicate"

ACTION_NONE = "none"
ACTION_RESTART_FFMPEG = "restart_ffmpeg"
ACTION_RESTART_STREAM = "restart_stream"

RECOVERY_ORDER = (
    ACTION_RESTART_FFMPEG,
    ACTION_RESTART_STREAM,
)

BLOCK_REPLACEMENT = "local_delivery_failure_never_authorizes_replacement_broadcast"
