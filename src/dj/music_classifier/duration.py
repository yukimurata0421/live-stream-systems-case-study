from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .constants import DURATION_PROBE_PATTERNS, MAX_TRACK_SEC
from .text import normalize_genre

try:
    import mutagen
except ImportError:
    mutagen = None


def read_audio(path: Path) -> Any:
    if mutagen is None:
        return None
    try:
        return mutagen.File(path)
    except Exception:
        return None


def track_duration_sec(path: Path, allow_ffprobe: bool = True) -> float | None:
    audio = read_audio(path)
    info = getattr(audio, "info", None)
    duration = getattr(info, "length", None)
    if duration is not None:
        try:
            return float(duration)
        except (TypeError, ValueError):
            pass
    if not allow_ffprobe:
        return None
    return ffprobe_duration_sec(path)


def ffprobe_duration_sec(path: Path) -> float | None:
    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return None
    try:
        result = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        return float(result.stdout.strip())
    except (TypeError, ValueError):
        return None


def is_long_track(path: Path) -> bool:
    probe_all = os.environ.get("MUSIC_CLASSIFIER_PROBE_ALL_DURATIONS") == "1"
    duration = track_duration_sec(path, allow_ffprobe=probe_all or should_probe_duration(path))
    return duration is not None and duration > MAX_TRACK_SEC


def should_probe_duration(path: Path) -> bool:
    normalized_name = normalize_genre(path.stem)
    if any(pattern in normalized_name for pattern in DURATION_PROBE_PATTERNS):
        return True
    raw_stem = path.stem.replace("｜", "|")
    return raw_stem.lower().startswith("ncs") and " - " not in raw_stem
