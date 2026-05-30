from __future__ import annotations


STREAM_SERVICE = "adsb-streamnew-youtube-stream.service"
DJ_SERVICE = "adsb-streamnew-auto-dj.service"
WATCHDOG_SERVICE = "adsb-streamnew-watchdog.service"
WATCHDOG_TIMER = "adsb-streamnew-watchdog.timer"
YTW_MONITOR_SERVICE = "adsb-streamnew-youtube-monitor.service"
YTW_MONITOR_TIMER = "adsb-streamnew-youtube-monitor.timer"
YTW_VIDEO_RESOLVER_SERVICE = "adsb-streamnew-youtube-video-resolver.service"
YTW_VIDEO_RESOLVER_TIMER = "adsb-streamnew-youtube-video-resolver.timer"
FAST_RECOVERY_SERVICE = "adsb-streamnew-fast-recovery.service"
FAST_RECOVERY_TIMER = "adsb-streamnew-fast-recovery.timer"
API_COST_REPORT_TIMER = "adsb-streamnew-youtube-api-cost-report.timer"
API_COST_REPORT_SERVICE = "adsb-streamnew-youtube-api-cost-report.service"
API_COST_OPEN_DAY_REPORT_TIMER = "adsb-streamnew-youtube-api-cost-open-day-report.timer"
API_COST_OPEN_DAY_REPORT_SERVICE = "adsb-streamnew-youtube-api-cost-open-day-report.service"
STREAM1090_REPORT_SERVICE = "adsb-streamnew-stream1090-report.service"
STREAM1090_REPORT_TIMER = "adsb-streamnew-stream1090-report.timer"
UPSTREAM_REPORT_SERVICE = "adsb-streamnew-upstream-report.service"
UPSTREAM_REPORT_TIMER = "adsb-streamnew-upstream-report.timer"
NOTIFY_SERVICE = "adsb-streamnew-notify.service"
NOTIFY_TIMER = "adsb-streamnew-notify.timer"
SUBSYSTEMS_STATUS_SERVICE = "adsb-streamnew-subsystems-status.service"
SUBSYSTEMS_STATUS_TIMER = "adsb-streamnew-subsystems-status.timer"
RECOVERY_ORCHESTRATOR_SERVICE = "adsb-streamnew-recovery-orchestrator.service"
RECOVERY_ORCHESTRATOR_TIMER = "adsb-streamnew-recovery-orchestrator.timer"
MEMORY_STATUS_SERVICE = "adsb-streamnew-memory-status.service"
MEMORY_STATUS_TIMER = "adsb-streamnew-memory-status.timer"
RESOURCE_MEMORY_SERVICE = "adsb-streamnew-resource-memory.service"
RESOURCE_MEMORY_TIMER = "adsb-streamnew-resource-memory.timer"
LEGACY_STREAM_SERVICE = "adsb-youtube-stream.service"

MAINTENANCE_TIMERS = (
    FAST_RECOVERY_TIMER,
    WATCHDOG_TIMER,
    YTW_MONITOR_TIMER,
    YTW_VIDEO_RESOLVER_TIMER,
    STREAM1090_REPORT_TIMER,
    UPSTREAM_REPORT_TIMER,
    SUBSYSTEMS_STATUS_TIMER,
    RECOVERY_ORCHESTRATOR_TIMER,
    MEMORY_STATUS_TIMER,
    RESOURCE_MEMORY_TIMER,
    API_COST_OPEN_DAY_REPORT_TIMER,
    API_COST_REPORT_TIMER,
)

MAINTENANCE_SERVICES = (
    FAST_RECOVERY_SERVICE,
    WATCHDOG_SERVICE,
    YTW_MONITOR_SERVICE,
    YTW_VIDEO_RESOLVER_SERVICE,
    STREAM1090_REPORT_SERVICE,
    UPSTREAM_REPORT_SERVICE,
    SUBSYSTEMS_STATUS_SERVICE,
    RECOVERY_ORCHESTRATOR_SERVICE,
    MEMORY_STATUS_SERVICE,
    RESOURCE_MEMORY_SERVICE,
)

ALL_UNITS = (
    DJ_SERVICE,
    STREAM_SERVICE,
    WATCHDOG_TIMER,
    YTW_MONITOR_TIMER,
    YTW_VIDEO_RESOLVER_TIMER,
    FAST_RECOVERY_TIMER,
    STREAM1090_REPORT_TIMER,
    UPSTREAM_REPORT_TIMER,
    SUBSYSTEMS_STATUS_TIMER,
    RECOVERY_ORCHESTRATOR_TIMER,
    MEMORY_STATUS_TIMER,
    RESOURCE_MEMORY_TIMER,
    NOTIFY_TIMER,
)

SYSTEM_UNITS = (
    STREAM_SERVICE,
    DJ_SERVICE,
    WATCHDOG_SERVICE,
    WATCHDOG_TIMER,
    YTW_MONITOR_SERVICE,
    YTW_MONITOR_TIMER,
    YTW_VIDEO_RESOLVER_SERVICE,
    YTW_VIDEO_RESOLVER_TIMER,
    FAST_RECOVERY_SERVICE,
    FAST_RECOVERY_TIMER,
    STREAM1090_REPORT_SERVICE,
    STREAM1090_REPORT_TIMER,
    UPSTREAM_REPORT_SERVICE,
    UPSTREAM_REPORT_TIMER,
    SUBSYSTEMS_STATUS_SERVICE,
    SUBSYSTEMS_STATUS_TIMER,
    RECOVERY_ORCHESTRATOR_SERVICE,
    RECOVERY_ORCHESTRATOR_TIMER,
    MEMORY_STATUS_SERVICE,
    MEMORY_STATUS_TIMER,
    RESOURCE_MEMORY_SERVICE,
    RESOURCE_MEMORY_TIMER,
    NOTIFY_SERVICE,
    NOTIFY_TIMER,
)

INSTALL_TARGETS = {
    "adsb-streamnew-youtube-stream.service": "adsb-streamnew-youtube-stream.service",
    "adsb-streamnew-auto-dj.service": "adsb-streamnew-auto-dj.service",
    "adsb-streamnew-watchdog.service": "adsb-streamnew-watchdog.service",
    "adsb-streamnew-watchdog.timer": "adsb-streamnew-watchdog.timer",
    "adsb-streamnew-youtube-monitor.service": "adsb-streamnew-youtube-monitor.service",
    "adsb-streamnew-youtube-monitor.timer": "adsb-streamnew-youtube-monitor.timer",
    "adsb-streamnew-youtube-video-resolver.service": "adsb-streamnew-youtube-video-resolver.service",
    "adsb-streamnew-youtube-video-resolver.timer": "adsb-streamnew-youtube-video-resolver.timer",
    "adsb-streamnew-fast-recovery.service": "adsb-streamnew-fast-recovery.service",
    "adsb-streamnew-fast-recovery.timer": "adsb-streamnew-fast-recovery.timer",
    "adsb-streamnew-stream1090-report.service": "adsb-streamnew-stream1090-report.service",
    "adsb-streamnew-stream1090-report.timer": "adsb-streamnew-stream1090-report.timer",
    "adsb-streamnew-upstream-report.service": "adsb-streamnew-upstream-report.service",
    "adsb-streamnew-upstream-report.timer": "adsb-streamnew-upstream-report.timer",
    "adsb-streamnew-subsystems-status.service": "adsb-streamnew-subsystems-status.service",
    "adsb-streamnew-subsystems-status.timer": "adsb-streamnew-subsystems-status.timer",
    "adsb-streamnew-recovery-orchestrator.service": "adsb-streamnew-recovery-orchestrator.service",
    "adsb-streamnew-recovery-orchestrator.timer": "adsb-streamnew-recovery-orchestrator.timer",
    "adsb-streamnew-memory-status.service": "adsb-streamnew-memory-status.service",
    "adsb-streamnew-memory-status.timer": "adsb-streamnew-memory-status.timer",
    "adsb-streamnew-resource-memory.service": "adsb-streamnew-resource-memory.service",
    "adsb-streamnew-resource-memory.timer": "adsb-streamnew-resource-memory.timer",
    "adsb-streamnew-notify.service": "adsb-streamnew-notify.service",
    "adsb-streamnew-notify.timer": "adsb-streamnew-notify.timer",
}
