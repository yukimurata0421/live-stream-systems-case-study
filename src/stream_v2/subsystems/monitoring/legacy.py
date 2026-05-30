from __future__ import annotations

from ...legacy_component import LegacyComponent
from ...stream_app import stream_app_root

SUBSYSTEM = "monitoring"


def components() -> list[LegacyComponent]:
    root = stream_app_root()
    return [
        LegacyComponent(SUBSYSTEM, "stream_watchdog", root / "src" / "watchers" / "stream_watchdog.py", "script", "local stream/render/audio watchdog", destructive=True, url_risk="same_url_preserving", default_command=("python3", "src/watchers/stream_watchdog.py"), service_unit="adsb-streamnew-watchdog.service"),
        LegacyComponent(SUBSYSTEM, "stream_watchdog_shell", root / "src" / "watchers" / "stream_watchdog.sh", "script", "legacy shell wrapper for stream watchdog"),
        LegacyComponent(SUBSYSTEM, "fast_recovery", root / "src" / "watchers" / "fast_recovery.py", "script", "short-window gateway/RTMP/encoder recovery", destructive=True, url_risk="same_url_preserving", default_command=("python3", "src/watchers/fast_recovery.py"), service_unit="adsb-streamnew-fast-recovery.service"),
        LegacyComponent(SUBSYSTEM, "youtube_api_cost_guard", root / "src" / "watchers" / "youtube_api_cost_guard.py", "library", "quota/burn-rate guard feeding destructive-action gates", shared_with=("youtube_lifecycle",)),
        LegacyComponent(SUBSYSTEM, "observe_stream_health", root / "ops" / "scripts" / "observe_stream_health.py", "script", "operator health summary", default_command=("python3", "ops/scripts/observe_stream_health.py")),
        LegacyComponent(SUBSYSTEM, "report_youtube_api_cost", root / "ops" / "scripts" / "report_youtube_api_cost.py", "script", "quota telemetry report generator", default_command=("python3", "ops/scripts/report_youtube_api_cost.py")),
        LegacyComponent(SUBSYSTEM, "watchdog_service", root / "ops" / "systemd" / "adsb-streamnew-watchdog.service", "systemd_unit", "stream watchdog service", destructive=True, url_risk="same_url_preserving", service_unit="adsb-streamnew-watchdog.service"),
        LegacyComponent(SUBSYSTEM, "watchdog_timer", root / "ops" / "systemd" / "adsb-streamnew-watchdog.timer", "systemd_timer", "stream watchdog cadence", service_unit="adsb-streamnew-watchdog.timer"),
        LegacyComponent(SUBSYSTEM, "fast_recovery_service", root / "ops" / "systemd" / "adsb-streamnew-fast-recovery.service", "systemd_unit", "fast recovery service", destructive=True, url_risk="same_url_preserving", service_unit="adsb-streamnew-fast-recovery.service"),
        LegacyComponent(SUBSYSTEM, "fast_recovery_timer", root / "ops" / "systemd" / "adsb-streamnew-fast-recovery.timer", "systemd_timer", "fast recovery cadence", service_unit="adsb-streamnew-fast-recovery.timer"),
        LegacyComponent(SUBSYSTEM, "cost_report_open_day_service", root / "ops" / "systemd" / "adsb-streamnew-youtube-api-cost-open-day-report.service", "systemd_unit", "open-day quota telemetry service", service_unit="adsb-streamnew-youtube-api-cost-open-day-report.service"),
        LegacyComponent(SUBSYSTEM, "cost_report_open_day_timer", root / "ops" / "systemd" / "adsb-streamnew-youtube-api-cost-open-day-report.timer", "systemd_timer", "open-day quota telemetry cadence", service_unit="adsb-streamnew-youtube-api-cost-open-day-report.timer"),
        LegacyComponent(SUBSYSTEM, "logrotate", root / "ops" / "logrotate" / "adsb-streamnew", "ops_config", "log rotation for legacy JSONL logs"),
    ]
