#!/usr/bin/env python3
"""Auto DJ for 24/7 streaming.

Selects tracks from time-based directories, updates a now-playing text file,
and plays tracks continuously using mpv or ffplay.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from auto_dj_lib import (
        FolderState,
        JST,
        NOW_PLAYING_PREFIX,
        PATTERN,
        SNAPSHOT_SCHEMA,
        TIME_BANDS,
    )
    from auto_dj_lib import io as dj_io
    from auto_dj_lib import cli as cli_args
    from auto_dj_lib import player as player_runtime
    from auto_dj_lib import rotation, snapshot, text, time_policy
except ModuleNotFoundError:
    from dj.auto_dj_lib import (
        FolderState,
        JST,
        NOW_PLAYING_PREFIX,
        PATTERN,
        SNAPSHOT_SCHEMA,
        TIME_BANDS,
    )
    from dj.auto_dj_lib import io as dj_io
    from dj.auto_dj_lib import cli as cli_args
    from dj.auto_dj_lib import player as player_runtime
    from dj.auto_dj_lib import rotation, snapshot, text, time_policy

SCRIPT_PATH = Path(__file__).resolve()
BASE_DIR = SCRIPT_PATH.parents[2]


class AutoDJ:
    def __init__(
        self,
        library_root: Path,
        now_playing_file: Path,
        snapshot_file: Path,
        history_jsonl_file: Path,
        player: str,
        retry_sleep_sec: int,
        pulse_sink: Optional[str],
        player_fail_sleep_sec: int,
        force_pulse_ao: bool,
        snapshot_heartbeat_sec: int,
        max_track_sec: int,
        pulse_buffer_duration_ms: int,
        duration_cache_file: Optional[Path] = None,
    ) -> None:
        self.library_root = library_root
        self.now_playing_file = now_playing_file
        self.snapshot_file = snapshot_file
        self.history_jsonl_file = history_jsonl_file
        self.retry_sleep_sec = retry_sleep_sec
        self.pulse_sink = pulse_sink
        self.player_fail_sleep_sec = max(player_fail_sleep_sec, 1)
        self.force_pulse_ao = force_pulse_ao
        self.snapshot_heartbeat_sec = max(snapshot_heartbeat_sec, 1)
        self.max_track_sec = max_track_sec
        self.pulse_buffer_duration_ms = max(pulse_buffer_duration_ms, 0)
        self.stop_requested = False
        self.current_process: Optional[subprocess.Popen] = None
        self.state_by_folder: dict[str, FolderState] = {}
        self.snapshot_seq = 0
        self.player = self._resolve_player(player)
        self.duration_cache = player_runtime.load_duration_cache(duration_cache_file, library_root=self.library_root)
        self.pattern_state_file = self.snapshot_file.with_name("pattern_state.json")
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}"
        self._load_pattern_state()

    def _resolve_player(self, player: str) -> str:
        return player_runtime.resolve_player(player)

    def _current_bucket(self) -> str:
        return time_policy.current_bucket()

    def _list_tracks(self, folder: Path) -> list[Path]:
        return rotation.list_tracks(
            folder,
            max_track_sec=self.max_track_sec,
            duration_lookup=self._track_duration_sec,
        )

    @staticmethod
    def _track_prefix(track: Path) -> str:
        return rotation.track_prefix(track)

    def _pick_track(self, folder_name: str) -> Path:
        selected = rotation.pick_track(
            library_root=self.library_root,
            folder_name=folder_name,
            state_by_folder=self.state_by_folder,
            max_track_sec=self.max_track_sec,
            duration_lookup=self._track_duration_sec,
        )
        self._save_pattern_state()
        return selected

    def _load_pattern_state(self) -> None:
        self.state_by_folder.update(
            rotation.load_pattern_state(self.pattern_state_file, library_root=self.library_root)
        )

    def _save_pattern_state(self) -> None:
        try:
            self._atomic_write_text(
                self.pattern_state_file,
                json.dumps(
                    rotation.pattern_state_payload(self.state_by_folder),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )
        except Exception:
            logging.exception("Failed to save pattern state: %s", self.pattern_state_file)

    @staticmethod
    def _beautify_title(filename: str) -> str:
        return text.beautify_title(filename)

    @staticmethod
    def _utc_now_iso() -> str:
        return dj_io.utc_now_iso()

    @staticmethod
    def _jst_now_iso() -> str:
        return dj_io.jst_now_iso()

    def _atomic_write_text(self, path: Path, content: str) -> None:
        dj_io.atomic_write_text(path, content)

    def _write_now_playing(self, title: str) -> None:
        self._atomic_write_text(self.now_playing_file, snapshot.now_playing_line(title))

    def _append_history_jsonl(
        self,
        *,
        track: Path,
        title: str,
        bucket: str,
        retry_count: int,
    ) -> None:
        try:
            payload = snapshot.history_event(
                run_id=self.run_id,
                sequence=self.snapshot_seq,
                track=track,
                title=title,
                bucket=bucket,
                player=self.player,
                retry_count=retry_count,
            )
            self.history_jsonl_file.parent.mkdir(parents=True, exist_ok=True)
            with self.history_jsonl_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            logging.exception("Failed to append play history JSONL: %s", self.history_jsonl_file)

    def _write_snapshot(
        self,
        *,
        status: str,
        track: Optional[Path],
        title: str,
        bucket: str,
        retry_count: int,
        player_exit_code: Optional[int],
        note: Optional[str] = None,
    ) -> None:
        payload = snapshot.snapshot_payload(
            run_id=self.run_id,
            sequence=self.snapshot_seq,
            status=status,
            track=track,
            title=title,
            bucket=bucket,
            player=self.player,
            force_pulse_ao=self.force_pulse_ao,
            pulse_sink=self.pulse_sink,
            retry_count=retry_count,
            player_exit_code=player_exit_code,
            player_fail_sleep_sec=self.player_fail_sleep_sec,
            note=note,
        )
        self.snapshot_seq += 1
        self._atomic_write_text(
            self.snapshot_file,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    def _player_command(self, track: Path, track_duration_sec: float) -> list[str]:
        return player_runtime.player_command(
            player=self.player,
            track=track,
            track_duration_sec=track_duration_sec,
            force_pulse_ao=self.force_pulse_ao,
            pulse_sink=self.pulse_sink,
            pulse_buffer_duration_ms=self.pulse_buffer_duration_ms,
        )

    def _player_env(self) -> dict[str, str]:
        return player_runtime.player_env(pulse_sink=self.pulse_sink)

    def _pulse_server_ready(self) -> bool:
        return player_runtime.pulse_server_ready()

    def _pulse_sink_ready(self) -> bool:
        return player_runtime.pulse_sink_ready(self.pulse_sink)

    def _ensure_pulse_sink(self) -> bool:
        return player_runtime.ensure_pulse_sink(self.pulse_sink)

    def _wait_for_pulse_sink(self, timeout_sec: float = 30.0) -> bool:
        return player_runtime.wait_for_pulse_sink(
            self.pulse_sink,
            stop_requested=lambda: self.stop_requested,
            timeout_sec=timeout_sec,
        )

    def _track_duration_sec(self, track: Path) -> float:
        return player_runtime.track_duration_sec(track, self.duration_cache)

    def _play_track(
        self,
        track: Path,
        *,
        bucket: str,
        title: str,
        retry_count: int,
    ) -> int:
        if self.player == "mpv" and (self.force_pulse_ao or self.pulse_sink):
            if not self._pulse_server_ready():
                logging.error(
                    "Pulse server is unavailable. Start PulseAudio/PipeWire first, "
                    "or disable --force-pulse-ao."
                )
                return 98
        if self.player == "ffmpeg" and self.pulse_sink:
            if not self._wait_for_pulse_sink():
                logging.error("Pulse sink is unavailable: %s", self.pulse_sink)
                return 96

        expected = self._track_duration_sec(track)
        cmd = self._player_command(track, expected)
        logging.info("Playing: %s", track)
        started_at = time.monotonic()
        try:
            with subprocess.Popen(cmd, env=self._player_env()) as proc:
                self.current_process = proc
                next_heartbeat = time.monotonic() + self.snapshot_heartbeat_sec
                while not self.stop_requested:
                    rc = proc.poll()
                    if rc is not None:
                        elapsed = max(time.monotonic() - started_at, 0.0)
                        if (
                            rc == 0
                            and not self.stop_requested
                            and expected >= 30.0
                            and elapsed < expected * 0.95
                        ):
                            logging.warning(
                                "Player ended too early (elapsed=%.1fs expected=%.1fs) for: %s",
                                elapsed,
                                expected,
                                track,
                            )
                            return 97
                        if (
                            rc != 0
                            and self.player == "ffmpeg"
                            and self.pulse_sink
                            and elapsed < 5.0
                        ):
                            logging.warning(
                                "Player failed immediately on Pulse output (rc=%s elapsed=%.1fs sink=%s). "
                                "Treating this as an audio route failure, not a track failure.",
                                rc,
                                elapsed,
                                self.pulse_sink,
                            )
                            return 96
                        logging.info(
                            "Track finished (rc=%s elapsed=%.1fs expected=%.1fs): %s",
                            rc,
                            elapsed,
                            expected,
                            track,
                        )
                        return rc
                    now = time.monotonic()
                    if now >= next_heartbeat:
                        self._write_snapshot(
                            status="playing",
                            track=track,
                            title=title,
                            bucket=bucket,
                            retry_count=retry_count,
                            player_exit_code=None,
                            note="Heartbeat update while track is playing.",
                        )
                        next_heartbeat = now + self.snapshot_heartbeat_sec
                    time.sleep(0.5)

                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
                return proc.returncode or 0
        finally:
            self.current_process = None

    def _signal_handler(self, signum: int, _frame: object) -> None:
        logging.info("Received signal %s. Stopping Auto DJ...", signum)
        self.stop_requested = True
        if self.current_process and self.current_process.poll() is None:
            self.current_process.terminate()

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logging.info("Auto DJ started. Player: %s", self.player)
        retry_track: Optional[Path] = None
        retry_bucket: Optional[str] = None
        retry_count = 0
        current_title = "Initializing..."
        current_track: Optional[Path] = None
        current_bucket = "unknown"

        self._write_snapshot(
            status="starting",
            track=None,
            title=current_title,
            bucket=current_bucket,
            retry_count=retry_count,
            player_exit_code=None,
            note="Auto DJ process started.",
        )

        while not self.stop_requested:
            try:
                if retry_track is None or retry_bucket is None:
                    bucket = self._current_bucket()
                    track = self._pick_track(bucket)
                    title = self._beautify_title(track.name)
                    self._write_now_playing(title)
                    self._append_history_jsonl(
                        track=track,
                        title=title,
                        bucket=bucket,
                        retry_count=retry_count,
                    )
                    self._write_snapshot(
                        status="playing",
                        track=track,
                        title=title,
                        bucket=bucket,
                        retry_count=retry_count,
                        player_exit_code=None,
                    )
                else:
                    bucket = retry_bucket
                    track = retry_track
                    title = self._beautify_title(track.name)
                    logging.info(
                        "Retrying same track after player failure (%s/%s): %s",
                        retry_count + 1,
                        3,
                        track,
                    )
                    self._write_snapshot(
                        status="retrying",
                        track=track,
                        title=title,
                        bucket=bucket,
                        retry_count=retry_count + 1,
                        player_exit_code=None,
                        note="Retrying current track after player failure.",
                    )

                current_track = track
                current_title = title
                current_bucket = bucket

                rc = self._play_track(
                    track,
                    bucket=bucket,
                    title=title,
                    retry_count=retry_count,
                )
                if rc == 96 and not self.stop_requested:
                    retry_track = track
                    retry_bucket = bucket
                    logging.warning("Audio route is unavailable. Holding current track: %s", track)
                    self._write_snapshot(
                        status="audio_route_wait",
                        track=track,
                        title=title,
                        bucket=bucket,
                        retry_count=retry_count,
                        player_exit_code=rc,
                        note="Waiting for PulseAudio stream sink; current track will not be skipped.",
                    )
                    for _ in range(self.player_fail_sleep_sec):
                        if self.stop_requested:
                            break
                        time.sleep(1)
                    continue
                if rc != 0 and not self.stop_requested:
                    retry_track = track
                    retry_bucket = bucket
                    retry_count += 1
                    logging.warning("Player exited with code %s for track: %s", rc, track)
                    self._write_snapshot(
                        status="player_error",
                        track=track,
                        title=title,
                        bucket=bucket,
                        retry_count=retry_count,
                        player_exit_code=rc,
                        note="Player returned non-zero exit code.",
                    )

                    if retry_count >= 3:
                        logging.error("Skipping track after %s failures: %s", retry_count, track)
                        self._write_snapshot(
                            status="skipped",
                            track=track,
                            title=title,
                            bucket=bucket,
                            retry_count=retry_count,
                            player_exit_code=rc,
                            note="Track skipped after repeated playback failures.",
                        )
                        retry_track = None
                        retry_bucket = None
                        retry_count = 0

                    for _ in range(self.player_fail_sleep_sec):
                        if self.stop_requested:
                            break
                        time.sleep(1)
                    continue

                retry_track = None
                retry_bucket = None
                retry_count = 0
            except Exception:
                logging.exception(
                    "Loop error. Waiting %s seconds before retry.",
                    self.retry_sleep_sec,
                )
                self._write_snapshot(
                    status="error",
                    track=current_track,
                    title=current_title,
                    bucket=current_bucket,
                    retry_count=retry_count,
                    player_exit_code=None,
                    note="Unhandled exception in Auto DJ loop.",
                )
                for _ in range(self.retry_sleep_sec):
                    if self.stop_requested:
                        break
                    time.sleep(1)

        self._write_snapshot(
            status="stopped",
            track=current_track,
            title=current_title,
            bucket=current_bucket,
            retry_count=retry_count,
            player_exit_code=None,
            note="Auto DJ process stopped.",
        )
        logging.info("Auto DJ stopped.")


def parse_args() -> argparse.Namespace:
    return cli_args.parse_args(BASE_DIR)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    dj = AutoDJ(
        library_root=args.music_root,
        now_playing_file=args.now_playing_file,
        snapshot_file=args.snapshot_file,
        history_jsonl_file=args.history_jsonl_file,
        player=args.player,
        retry_sleep_sec=max(args.retry_sleep_sec, 1),
        pulse_sink=args.pulse_sink.strip() or None,
        player_fail_sleep_sec=max(args.player_fail_sleep_sec, 1),
        force_pulse_ao=args.force_pulse_ao,
        snapshot_heartbeat_sec=max(args.snapshot_heartbeat_sec, 1),
        max_track_sec=args.max_track_sec,
        pulse_buffer_duration_ms=max(args.pulse_buffer_duration_ms, 0),
        duration_cache_file=args.duration_cache_file,
    )
    dj.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
