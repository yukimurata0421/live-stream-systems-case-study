from __future__ import annotations

from ...legacy_component import LegacyComponent
from ...stream_app import stream_app_root

SUBSYSTEM = "youtube_lifecycle"


def components() -> list[LegacyComponent]:
    root = stream_app_root()
    return [
        LegacyComponent(SUBSYSTEM, "youtube_watchdog", root / "src" / "watchers" / "youtube_watchdog.py", "script", "evaluates YouTube public/API/OAuth lifecycle and same-URL recovery", destructive=True, url_risk="can_change_youtube_lifecycle", default_command=("python3", "src/watchers/youtube_watchdog.py"), service_unit="adsb-streamnew-youtube-monitor.service"),
        LegacyComponent(SUBSYSTEM, "youtube_api", root / "src" / "watchers" / "youtube_api.py", "library", "YouTube Data API/OAuth mutation and probe wrapper", destructive=True, url_risk="can_change_youtube_lifecycle"),
        LegacyComponent(SUBSYSTEM, "youtube_health", root / "src" / "watchers" / "youtube_health.py", "library", "restart and YouTube lifecycle decision helpers", destructive=True, url_risk="same_url_preserving"),
        LegacyComponent(SUBSYSTEM, "youtube_watchdog_config", root / "src" / "watchers" / "youtube_watchdog_config.py", "config_module", "watchdog policy/env parsing"),
        LegacyComponent(SUBSYSTEM, "youtube_video_id_resolver", root / "src" / "watchers" / "youtube_video_id_resolver.py", "script", "resolves expected/candidate live video IDs without promoting candidates blindly", destructive=False, url_risk="none", service_unit="adsb-streamnew-youtube-video-resolver.service"),
        LegacyComponent(SUBSYSTEM, "youtube_monitor_service", root / "ops" / "systemd" / "adsb-streamnew-youtube-monitor.service", "systemd_unit", "YouTube monitor timer service", destructive=True, url_risk="can_change_youtube_lifecycle", service_unit="adsb-streamnew-youtube-monitor.service"),
        LegacyComponent(SUBSYSTEM, "youtube_monitor_timer", root / "ops" / "systemd" / "adsb-streamnew-youtube-monitor.timer", "systemd_timer", "YouTube monitor cadence", service_unit="adsb-streamnew-youtube-monitor.timer"),
        LegacyComponent(SUBSYSTEM, "youtube_video_resolver_service", root / "ops" / "systemd" / "adsb-streamnew-youtube-video-resolver.service", "systemd_unit", "video resolver timer service", service_unit="adsb-streamnew-youtube-video-resolver.service"),
        LegacyComponent(SUBSYSTEM, "youtube_video_resolver_timer", root / "ops" / "systemd" / "adsb-streamnew-youtube-video-resolver.timer", "systemd_timer", "video resolver cadence", service_unit="adsb-streamnew-youtube-video-resolver.timer"),
        LegacyComponent(SUBSYSTEM, "youtube_monitor_env_example", root / "ops" / "systemd" / "adsb-streamnew-youtube-monitor.env.example", "config", "YouTube lifecycle env template"),
    ]
