from __future__ import annotations

SUBSYSTEM = "music"

FAILURE_AUDIO_ENERGY_LOW = "audio_energy_low"
FAILURE_AUDIO_ENERGY_LOW_TRANSITION_GRACE = "audio_energy_low_transition_grace"
FAILURE_PULSE_SOURCE_MISSING = "pulse_source_missing"
FAILURE_NOW_PLAYING_STALE = "now_playing_stale"
FAILURE_PULSE_ROUTE_ANOMALY = "pulse_route_anomaly"

ACTION_NONE = "none"
ACTION_DEFER = "defer"
ACTION_RESTART_DJ = "restart_dj"
ACTION_REPAIR_PULSE = "repair_pulse"

RECOVERY_ORDER = (
    ACTION_DEFER,
    ACTION_RESTART_DJ,
    ACTION_REPAIR_PULSE,
)

BLOCK_YOUTUBE_LIFECYCLE = "audio_failure_never_authorizes_youtube_lifecycle_action"
