from __future__ import annotations

import re


SAFE_JSON_FILES = (
    "youtube_watchdog_stats.json",
    "youtube_video_id_resolver_state.json",
    "map_runtime_status.json",
    "subsystems_status.json",
    "viewer_synthetic_status.json",
    "external_blackbox_status.json",
    "monitoring_watchdog_state.json",
    "operational_reliability_burn_status.json",
    "operational_reliability_status.json",
    "network_observer.json",
    "resource_memory.json",
    "memory_status.json",
    "recovery_action_plan.json",
    "notification_state.json",
    "adsb_freshness_state.json",
    "youtube_api_quota_state.json",
    "control_loop_state.json",
)
RAW_JSON_SOURCES = {
    "youtube_watchdog_stats.json": "youtube_watchdog_stats.json",
    "youtube_video_id_resolver_state.json": "youtube_video_id_resolver_state.json",
    "map_runtime_status.json": "map_runtime_status.json",
    "subsystems_status.json": "subsystems_status.json",
    "viewer_synthetic_status.json": "viewer_synthetic_status.json",
    "external_blackbox_status.json": "external_blackbox_status.json",
    "monitoring_watchdog_state.json": "monitoring_watchdog_state.json",
    "operational_reliability_burn_status.json": "operational_reliability_burn_status.json",
    "operational_reliability_status.json": "operational_reliability_status.json",
    "network_observer.json": "network_observer_latest.json",
    "resource_memory.json": "resource_memory.json",
    "memory_status.json": "memory_status.json",
    "recovery_action_plan.json": "recovery_action_plan.json",
    "notification_state.json": "stream_notify_state.json",
    "adsb_freshness_state.json": "watchdog/adsb_freshness_state.json",
    "youtube_api_quota_state.json": "reports/youtube_api_cost/open_day_latest.json",
    "control_loop_state.json": "v3_control_state.json",
}
SAFE_OUTBOX_FILE = "notification_outbox_status.json"
RAW_OUTBOX_FILE = "stream_notify_outbox.jsonl"
SAFE_REVISION_FILE = "deployed-revision.env"
SAFE_LIFECYCLE_FILE = "runtime_lifecycle_events.json"
SAFE_ROLLOUT_FILE = "runtime_rollout_evidence.json"
RAW_LIFECYCLE_FILES = (
    "logs/stream_engine_events.jsonl.1",
    "logs/stream_engine_events.jsonl",
)
RAW_ROLLOUT_FILE = "watchdog/k8s_container_restart_counts.json"
PLANNED_ROLLOUT_ANNOTATIONS = {
    "id": "stream-v3.yukimurata.dev/planned-rollout-id",
    "at": "stream-v3.yukimurata.dev/planned-rollout-at",
    "expires": "stream-v3.yukimurata.dev/planned-rollout-expires-at",
    "reason": "stream-v3.yukimurata.dev/planned-rollout-reason",
}
TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
REASON_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{1,95}$")
LIFECYCLE_TAIL_BYTES = 16 * 1024 * 1024
LIFECYCLE_MAX_LINE_BYTES = 1024 * 1024
LIFECYCLE_MAX_LINES = 20_000
