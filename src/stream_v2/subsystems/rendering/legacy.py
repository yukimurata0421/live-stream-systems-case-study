from __future__ import annotations

from ...legacy_component import LegacyComponent
from ...stream_app import stream_app_root

SUBSYSTEM = "rendering"


def components() -> list[LegacyComponent]:
    root = stream_app_root()
    return [
        LegacyComponent(SUBSYSTEM, "overlay_server", root / "src" / "stream_core" / "overlay_server.py", "script", "serves overlay assets and ADS-B/tar1090 proxy checks", default_command=("python3", "src/stream_core/overlay_server.py")),
        LegacyComponent(SUBSYSTEM, "overlay_html", root / "ui" / "overlay" / "index.html", "asset", "viewer-facing ADS-B overlay document"),
        LegacyComponent(SUBSYSTEM, "overlay_now_playing_bridge", root / "ui" / "overlay" / "now_playing.json", "runtime_asset", "overlay-side music metadata bridge", shared_with=("music",)),
        LegacyComponent(SUBSYSTEM, "stream_engine_browser_stack", root / "src" / "stream_core" / "stream_engine.py", "script", "launches Xvfb/Chromium capture helpers used by rendering", destructive=True, url_risk="same_url_preserving", shared_with=("local_delivery", "music")),
        LegacyComponent(SUBSYSTEM, "stream_service_unit", root / "ops" / "systemd" / "adsb-streamnew-youtube-stream.service", "systemd_unit", "systemd owner for rendering helper lifecycle through stream_engine", destructive=True, url_risk="same_url_preserving", service_unit="adsb-streamnew-youtube-stream.service", shared_with=("local_delivery",)),
    ]
