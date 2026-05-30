from __future__ import annotations

SUBSYSTEM = "monitoring"

FAILURE_WATCHDOG_STALE = "watchdog_stats_stale"
FAILURE_RESOLVER_STALE = "resolver_cache_stale"
FAILURE_COST_REPORT_DEGRADED = "cost_report_degraded"
FAILURE_QUOTA_GUARD_ACTIVE = "quota_guard_active"
FAILURE_STREAM_WATCHDOG_STALE = "stream_watchdog_stale"

ACTION_NONE = "none"
ACTION_ALERT = "alert"

BLOCK_DESTRUCTIVE = "monitoring_unknown_never_authorizes_destructive_action"
BLOCK_YOUTUBE_API = "youtube_api_destructive_action_blocked"
