from __future__ import annotations

from ...legacy_component import LegacyComponent
from ...stream_app import stream_app_root

SUBSYSTEM = "music"


def components() -> list[LegacyComponent]:
    root = stream_app_root()
    return [
        LegacyComponent(SUBSYSTEM, "auto_dj", root / "src" / "dj" / "auto_dj.py", "script", "selects and plays NCS tracks", default_command=("python3", "src/dj/auto_dj.py")),
        LegacyComponent(SUBSYSTEM, "music_time_classifier", root / "src" / "dj" / "music_time_classifier.py", "script", "classifies tracks for JST time-aware rotation"),
        LegacyComponent(SUBSYSTEM, "ncs_music", root / "ncs_music", "local_asset", "local NCS music library copied into stream_v2"),
        LegacyComponent(SUBSYSTEM, "now_playing_text", root / "now_playing.txt", "runtime_asset", "plain now-playing bridge"),
        LegacyComponent(SUBSYSTEM, "overlay_now_playing_json", root / "ui" / "overlay" / "now_playing.json", "runtime_asset", "overlay now-playing bridge", shared_with=("rendering",)),
        LegacyComponent(SUBSYSTEM, "auto_dj_service", root / "ops" / "systemd" / "adsb-streamnew-auto-dj.service", "systemd_unit", "systemd owner for Auto DJ", destructive=True, url_risk="none", service_unit="adsb-streamnew-auto-dj.service"),
    ]
