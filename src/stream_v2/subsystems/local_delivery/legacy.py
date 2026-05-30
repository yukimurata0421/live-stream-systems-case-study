from __future__ import annotations

from ...legacy_component import LegacyComponent
from ...stream_app import stream_app_root

SUBSYSTEM = "local_delivery"


def components() -> list[LegacyComponent]:
    root = stream_app_root()
    return [
        LegacyComponent(SUBSYSTEM, "stream_engine", root / "src" / "stream_core" / "stream_engine.py", "script", "owns ffmpeg/RTMP send loop and runtime heartbeat", destructive=True, url_risk="same_url_preserving", default_command=("python3", "src/stream_core/stream_engine.py"), shared_with=("rendering", "music")),
        LegacyComponent(SUBSYSTEM, "stream_shell", root / "src" / "stream_core" / "stream.sh", "script", "legacy ffmpeg shell entrypoint", destructive=True, url_risk="same_url_preserving"),
        LegacyComponent(SUBSYSTEM, "stream_cli", root / "bin" / "stream-new", "script", "migrated operator CLI for stream start/stop/status", destructive=True, url_risk="same_url_preserving"),
        LegacyComponent(SUBSYSTEM, "youtube_stream_service", root / "ops" / "systemd" / "adsb-streamnew-youtube-stream.service", "systemd_unit", "systemd stream service", destructive=True, url_risk="same_url_preserving", service_unit="adsb-streamnew-youtube-stream.service"),
        LegacyComponent(SUBSYSTEM, "default_env", root / "configs" / "default.env", "config", "default runtime config for local delivery"),
        LegacyComponent(SUBSYSTEM, "production_env_example", root / "configs" / "production.env.example", "config", "production config template"),
    ]
