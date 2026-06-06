from __future__ import annotations

SUBSYSTEM = "local_delivery"

FAILURE_FFMPEG_MISSING = "ffmpeg_missing"
FAILURE_INGEST_DISCONNECTED = "ingest_disconnected"
FAILURE_RUNTIME_HEARTBEAT_STALE = "runtime_heartbeat_stale"
FAILURE_TCP_STALL = "tcp_stall"
FAILURE_STREAM_FFMPEG_DUPLICATE = "stream_ffmpeg_duplicate"
FAILURE_FAST_RECOVERY_STREAM_RESTART_TCP_STALL = "fast_recovery_stream_restart_tcp_stall"
FAILURE_FAST_RECOVERY_STREAM_RESTART_NETWORK_DOWN = "fast_recovery_stream_restart_network_down"
FAILURE_FAST_RECOVERY_STREAM_RESTART_LOW_UPLOAD_PRESSURE = "fast_recovery_stream_restart_low_upload_pressure"
FAILURE_FAST_RECOVERY_STREAM_RESTART_REMOTE_WARNING = "fast_recovery_stream_restart_remote_warning"

FAST_RECOVERY_STREAM_RESTART_FAILURE_BY_TRIGGER = {
    "tcp_stall": FAILURE_FAST_RECOVERY_STREAM_RESTART_TCP_STALL,
    "network_down": FAILURE_FAST_RECOVERY_STREAM_RESTART_NETWORK_DOWN,
    "low_upload_pressure": FAILURE_FAST_RECOVERY_STREAM_RESTART_LOW_UPLOAD_PRESSURE,
    "remote_warning": FAILURE_FAST_RECOVERY_STREAM_RESTART_REMOTE_WARNING,
}
FAST_RECOVERY_STREAM_RESTART_FAILURES = frozenset(FAST_RECOVERY_STREAM_RESTART_FAILURE_BY_TRIGGER.values())

ACTION_NONE = "none"
ACTION_RESTART_FFMPEG = "restart_ffmpeg"
ACTION_RESTART_STREAM = "restart_stream"

RECOVERY_ORDER = (
    ACTION_RESTART_FFMPEG,
    ACTION_RESTART_STREAM,
)

BLOCK_REPLACEMENT = "local_delivery_failure_never_authorizes_replacement_broadcast"
