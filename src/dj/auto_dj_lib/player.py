from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Callable


def resolve_player(player: str) -> str:
    if player in {"ffmpeg", "mpv", "ffplay"}:
        if shutil.which(player) is None:
            raise RuntimeError(f"Requested player '{player}' is not installed.")
        return player

    for candidate in ("ffmpeg", "mpv", "ffplay"):
        if shutil.which(candidate):
            return candidate
    raise RuntimeError("No player found. Install 'ffmpeg', 'mpv', or 'ffplay'.")


def player_command(
    *,
    player: str,
    track: Path,
    track_duration_sec: float,
    force_pulse_ao: bool,
    pulse_sink: str | None,
    pulse_buffer_duration_ms: int,
) -> list[str]:
    if player == "mpv":
        cmd = [
            "mpv",
            "--no-video",
            "--really-quiet",
            "--no-terminal",
            str(track),
        ]
        if force_pulse_ao or pulse_sink:
            cmd.insert(1, "--ao=pulse")
        return cmd
    if player == "ffmpeg":
        ff_cmd = [
            "ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-i",
            str(track),
            "-vn",
        ]
        if track_duration_sec >= 20.0:
            fade_duration = 4.0
            fade_start = max(track_duration_sec - 5.0, 0.0)
            ff_cmd.extend(
                [
                    "-af",
                    f"afade=t=out:st={fade_start:.3f}:d={fade_duration:.3f}",
                ]
            )
        ff_cmd.extend(
            [
                "-f",
                "pulse",
                "-stream_name",
                "adsb-streamnew-auto-dj",
                "-buffer_duration",
                str(pulse_buffer_duration_ms),
                pulse_sink or "default",
            ]
        )
        return ff_cmd
    return [
        "ffplay",
        "-nodisp",
        "-autoexit",
        "-loglevel",
        "error",
        str(track),
    ]


def player_env(*, pulse_sink: str | None) -> dict[str, str]:
    env = os.environ.copy()
    if env.get("AUTO_DJ_KEEP_PULSE_SERVER", "").strip().lower() not in {"1", "true", "yes", "on"}:
        env.pop("PULSE_SERVER", None)
    env.setdefault("PULSE_SHM", "0")
    if pulse_sink:
        env["PULSE_SINK"] = pulse_sink
    return env


def pulse_server_ready() -> bool:
    try:
        subprocess.run(
            ["pactl", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=2,
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


def pulse_sink_ready(pulse_sink: str | None) -> bool:
    if not pulse_sink:
        return True
    try:
        out = subprocess.check_output(
            ["pactl", "list", "short", "sinks"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    return any(line.split()[1] == pulse_sink for line in out.splitlines() if len(line.split()) >= 2)


def ensure_pulse_sink(pulse_sink: str | None) -> bool:
    if not pulse_sink:
        return True
    if not pulse_server_ready():
        return False
    if pulse_sink_ready(pulse_sink):
        return True
    try:
        subprocess.run(
            [
                "pactl",
                "load-module",
                "module-null-sink",
                f"sink_name={pulse_sink}",
                f"sink_properties=device.description={pulse_sink}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=3,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    return pulse_sink_ready(pulse_sink)


def wait_for_pulse_sink(
    pulse_sink: str | None,
    *,
    stop_requested: Callable[[], bool],
    timeout_sec: float = 30.0,
) -> bool:
    deadline = time.monotonic() + timeout_sec
    while not stop_requested() and time.monotonic() < deadline:
        if ensure_pulse_sink(pulse_sink):
            return True
        time.sleep(1)
    return ensure_pulse_sink(pulse_sink)


def load_duration_cache(path: Path | None, *, library_root: Path) -> dict[Path, float]:
    if path is None:
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logging.warning("Duration cache not found: %s", path)
        return {}
    except Exception:
        logging.exception("Failed to load duration cache: %s", path)
        return {}

    durations = raw.get("durations", raw) if isinstance(raw, dict) else {}
    if not isinstance(durations, dict):
        logging.warning("Duration cache has no durations object: %s", path)
        return {}

    cache: dict[Path, float] = {}
    for key, value in durations.items():
        if not isinstance(key, str) or not key:
            continue
        duration = value.get("duration_sec") if isinstance(value, dict) else value
        if not isinstance(duration, (int, float)) or duration < 0:
            continue
        track = Path(key)
        if not track.is_absolute():
            track = library_root / track
        cache[track] = float(duration)
    logging.info("Loaded %s track durations from %s", len(cache), path)
    return cache


def track_duration_sec(track: Path, duration_cache: dict[Path, float]) -> float:
    cached = duration_cache.get(track)
    if cached is not None:
        return cached
    try:
        out = subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(track),
            ],
            text=True,
            timeout=5,
        ).strip()
        dur = float(out) if out else 0.0
    except Exception:
        dur = 0.0
    duration_cache[track] = dur
    return dur
