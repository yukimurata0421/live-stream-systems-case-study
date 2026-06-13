from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Mapping


REQUIRED_COMMANDS = ("ffmpeg", "pactl", "xdpyinfo", "sha256sum")


def ensure_commands(required: tuple[str, ...] = REQUIRED_COMMANDS) -> None:
    for cmd in required:
        if shutil.which(cmd) is None:
            raise RuntimeError(f"Required command not found: {cmd}")


def assert_systemd_launch(cfg, *, env: Mapping[str, str] | None = None) -> None:
    source = os.environ if env is None else env
    if not cfg.require_systemd_launch or cfg.allow_direct_stream_sh:
        return
    if source.get("INVOCATION_ID") or source.get("STREAM_LAUNCH_MODE") == "systemd":
        return
    raise RuntimeError("Direct stream launch is disabled. Start via 'stream-new' command.")


def pick_font_file(configured_font_file: str) -> str:
    system_root = os.environ.get("SystemRoot", r"C:\Windows")
    candidates = [
        configured_font_file,
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        str(Path(system_root) / "Fonts" / "meiryo.ttc"),
        str(Path(system_root) / "Fonts" / "segoeui.ttf"),
        str(Path(system_root) / "Fonts" / "arial.ttf"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    raise RuntimeError("No usable font file found.")


def prepare_runtime_paths(cfg) -> None:
    cfg.base_dir.mkdir(parents=True, exist_ok=True)
    (cfg.base_dir / "logs").mkdir(parents=True, exist_ok=True)
    (cfg.base_dir / "state" / "runtime").mkdir(parents=True, exist_ok=True)
    (cfg.base_dir / "runtime").mkdir(parents=True, exist_ok=True)
    cfg.overlay_dir.mkdir(parents=True, exist_ok=True)
    if not cfg.now_playing_file.exists():
        cfg.now_playing_file.write_text("▶ Now Playing: Preparing audio...\n", encoding="utf-8")
