from __future__ import annotations

import argparse
import os
from pathlib import Path


def build_parser(base_dir: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Auto DJ for 24/7 stream audio.")
    runtime_root = os.environ.get("STREAM_RUNTIME_STATE_DIR", "").strip()
    default_history_file = (
        Path(runtime_root) / "logs" / "play_history.jsonl"
        if runtime_root
        else (base_dir / "logs" / "play_history.jsonl")
    )
    parser.add_argument(
        "--music-root",
        type=Path,
        default=Path(os.environ.get("MUSIC_ROOT", str(base_dir / "ncs_music" / "time_tags"))),
        help="Root directory containing morning/day/evening/night folders.",
    )
    parser.add_argument(
        "--now-playing-file",
        type=Path,
        default=Path(os.environ.get("NOW_PLAYING_FILE", str(base_dir / "now_playing.txt"))),
        help="Text file consumed by FFmpeg drawtext.",
    )
    parser.add_argument(
        "--snapshot-file",
        type=Path,
        default=Path(os.environ.get("NOW_PLAYING_SNAPSHOT_FILE", str(base_dir / "ui" / "overlay" / "now_playing.json"))),
        help="JSON snapshot file for browser overlay and monitoring.",
    )
    parser.add_argument(
        "--history-jsonl-file",
        type=Path,
        default=Path(os.environ.get("PLAY_HISTORY_JSONL_FILE", str(default_history_file))),
        help="Append-only JSONL history file for selected tracks (timestamp in JST).",
    )
    parser.add_argument(
        "--player",
        choices=["auto", "ffmpeg", "mpv", "ffplay"],
        default="auto",
        help="Audio player command. 'auto' tries ffmpeg first, then mpv, then ffplay.",
    )
    parser.add_argument(
        "--retry-sleep-sec",
        type=int,
        default=5,
        help="Sleep seconds after an error before retrying.",
    )
    parser.add_argument(
        "--player-fail-sleep-sec",
        type=int,
        default=10,
        help="Sleep seconds after a player startup/playback failure.",
    )
    parser.add_argument(
        "--pulse-sink",
        default="stream_sink",
        help="Pulse sink name for playback routing (sets PULSE_SINK). Empty string disables it.",
    )
    parser.add_argument(
        "--force-pulse-ao",
        action="store_true",
        help="Force mpv audio backend to PulseAudio (recommended for virtual sink routing).",
    )
    parser.add_argument(
        "--snapshot-heartbeat-sec",
        type=int,
        default=10,
        help="Periodic JSON snapshot refresh interval while a track is playing.",
    )
    parser.add_argument(
        "--max-track-sec",
        type=int,
        default=600,
        help="Exclude tracks longer than this duration in seconds. Set 0 to disable.",
    )
    parser.add_argument(
        "--duration-cache-file",
        type=Path,
        default=Path(os.environ["AUTO_DJ_DURATION_CACHE_FILE"]) if os.environ.get("AUTO_DJ_DURATION_CACHE_FILE") else None,
        help="Optional JSON cache of track durations, keyed relative to --music-root.",
    )
    parser.add_argument(
        "--pulse-buffer-duration-ms",
        type=int,
        default=int(os.environ.get("PULSE_BUFFER_DURATION_MS", "250")),
        help="Pulse output buffer duration in milliseconds for ffmpeg player (lower reduces latency/cut perception).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser


def parse_args(base_dir: Path) -> argparse.Namespace:
    return build_parser(base_dir).parse_args()
