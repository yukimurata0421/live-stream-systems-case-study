from __future__ import annotations

import sys
import tempfile
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "dj"))

import auto_dj  # type: ignore
from auto_dj import AutoDJ, FolderState, parse_args  # type: ignore
from auto_dj_lib import player as player_runtime  # type: ignore


class AutoDJMajorRotationResetTests(unittest.TestCase):
    def _create_tracks(self, base: Path) -> Path:
        day = base / "day"
        day.mkdir(parents=True, exist_ok=True)
        for name in (
            "major_alpha.mp3",
            "major_beta.mp3",
            "minor_one.mp3",
            "minor_two.mp3",
        ):
            (day / name).write_bytes(b"")
        return day

    def test_major_exhaustion_falls_back_to_unplayed_any(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            day = self._create_tracks(root)
            with mock.patch.object(AutoDJ, "_resolve_player", return_value="ffmpeg"):
                dj = AutoDJ(
                    library_root=root,
                    now_playing_file=root / "now_playing.txt",
                    snapshot_file=root / "snapshot.json",
                    history_jsonl_file=root / "history.jsonl",
                    player="ffmpeg",
                    retry_sleep_sec=1,
                    pulse_sink=None,
                    player_fail_sleep_sec=1,
                    force_pulse_ao=False,
                    snapshot_heartbeat_sec=10,
                    max_track_sec=0,
                    pulse_buffer_duration_ms=250,
                )

            state = dj.state_by_folder.setdefault("day", FolderState())
            state.pattern_index = 3  # PATTERN -> major
            major_tracks = {day / "major_alpha.mp3", day / "major_beta.mp3"}
            minor_played = day / "minor_one.mp3"
            state.played = set(major_tracks)
            state.played.add(minor_played)

            with mock.patch("random.choice", side_effect=lambda seq: seq[0]):
                with mock.patch("logging.info") as info_mock:
                    selected = dj._pick_track("day")

            self.assertEqual(selected, day / "minor_two.mp3")
            self.assertIn(minor_played, state.played)
            self.assertTrue(set(major_tracks).issubset(state.played))
            info_mock.assert_called()

    def test_minor_exhaustion_falls_back_to_unplayed_any(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            day = self._create_tracks(root)
            with mock.patch.object(AutoDJ, "_resolve_player", return_value="ffmpeg"):
                dj = AutoDJ(
                    library_root=root,
                    now_playing_file=root / "now_playing.txt",
                    snapshot_file=root / "snapshot.json",
                    history_jsonl_file=root / "history.jsonl",
                    player="ffmpeg",
                    retry_sleep_sec=1,
                    pulse_sink=None,
                    player_fail_sleep_sec=1,
                    force_pulse_ao=False,
                    snapshot_heartbeat_sec=10,
                    max_track_sec=0,
                    pulse_buffer_duration_ms=250,
                )

            state = dj.state_by_folder.setdefault("day", FolderState())
            state.pattern_index = 0  # PATTERN -> minor
            minor_tracks = {day / "minor_one.mp3", day / "minor_two.mp3"}
            major_played = day / "major_alpha.mp3"
            state.played = set(minor_tracks)
            state.played.add(major_played)

            with mock.patch("random.choice", side_effect=lambda seq: seq[0]):
                with mock.patch("logging.info") as info_mock:
                    selected = dj._pick_track("day")

            self.assertEqual(selected, day / "major_beta.mp3")
            self.assertIn(major_played, state.played)
            self.assertTrue(set(minor_tracks).issubset(state.played))
            info_mock.assert_called()

    def test_pattern_state_persists_played_filenames(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            day = self._create_tracks(root)
            snapshot = root / "snapshot.json"
            with mock.patch.object(AutoDJ, "_resolve_player", return_value="ffmpeg"):
                dj = AutoDJ(
                    library_root=root,
                    now_playing_file=root / "now_playing.txt",
                    snapshot_file=snapshot,
                    history_jsonl_file=root / "history.jsonl",
                    player="ffmpeg",
                    retry_sleep_sec=1,
                    pulse_sink=None,
                    player_fail_sleep_sec=1,
                    force_pulse_ao=False,
                    snapshot_heartbeat_sec=10,
                    max_track_sec=0,
                    pulse_buffer_duration_ms=250,
                )
            state = dj.state_by_folder.setdefault("day", FolderState())
            state.pattern_index = 2
            state.played = {day / "major_alpha.mp3", day / "minor_one.mp3"}
            dj._save_pattern_state()

            with mock.patch.object(AutoDJ, "_resolve_player", return_value="ffmpeg"):
                restored = AutoDJ(
                    library_root=root,
                    now_playing_file=root / "now_playing.txt",
                    snapshot_file=snapshot,
                    history_jsonl_file=root / "history.jsonl",
                    player="ffmpeg",
                    retry_sleep_sec=1,
                    pulse_sink=None,
                    player_fail_sleep_sec=1,
                    force_pulse_ao=False,
                    snapshot_heartbeat_sec=10,
                    max_track_sec=0,
                    pulse_buffer_duration_ms=250,
                )

            restored_state = restored.state_by_folder["day"]
            self.assertEqual(restored_state.pattern_index, 2)
            self.assertEqual(
                restored_state.played,
                {day / "major_alpha.mp3", day / "minor_one.mp3"},
            )

    def test_current_bucket_uses_jst_hour(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with mock.patch.object(AutoDJ, "_resolve_player", return_value="ffmpeg"):
                dj = AutoDJ(
                    library_root=root,
                    now_playing_file=root / "now_playing.txt",
                    snapshot_file=root / "snapshot.json",
                    history_jsonl_file=root / "history.jsonl",
                    player="ffmpeg",
                    retry_sleep_sec=1,
                    pulse_sink=None,
                    player_fail_sleep_sec=1,
                    force_pulse_ao=False,
                    snapshot_heartbeat_sec=10,
                    max_track_sec=0,
                    pulse_buffer_duration_ms=250,
                )

            with mock.patch("auto_dj.time_policy.datetime") as datetime_mock:
                datetime_mock.now.return_value.hour = 16
                bucket = dj._current_bucket()

            self.assertEqual(bucket, "evening")
            datetime_mock.now.assert_called_with(auto_dj.JST)


class AutoDJCliDefaultsTests(unittest.TestCase):
    def test_history_default_uses_stream_runtime_state_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runtime_root = Path(td) / "runtime-state"
            env = {
                "STREAM_RUNTIME_STATE_DIR": str(runtime_root),
            }
            with mock.patch.dict("os.environ", env, clear=True):
                with mock.patch.object(sys, "argv", ["auto_dj.py"]):
                    args = parse_args()
            self.assertEqual(
                args.history_jsonl_file,
                runtime_root / "logs" / "play_history.jsonl",
            )

    def test_history_env_override_has_priority(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runtime_root = Path(td) / "runtime-state"
            explicit_history = Path(td) / "custom" / "history.jsonl"
            env = {
                "STREAM_RUNTIME_STATE_DIR": str(runtime_root),
                "PLAY_HISTORY_JSONL_FILE": str(explicit_history),
            }
            with mock.patch.dict("os.environ", env, clear=True):
                with mock.patch.object(sys, "argv", ["auto_dj.py"]):
                    args = parse_args()
            self.assertEqual(args.history_jsonl_file, explicit_history)


class AutoDJFFmpegCommandTests(unittest.TestCase):
    def test_ffmpeg_player_uses_pulse_buffer_without_readrate_flags(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            track = root / "minor_test.mp3"
            track.write_bytes(b"")
            with mock.patch.object(AutoDJ, "_resolve_player", return_value="ffmpeg"):
                dj = AutoDJ(
                    library_root=root,
                    now_playing_file=root / "now_playing.txt",
                    snapshot_file=root / "snapshot.json",
                    history_jsonl_file=root / "history.jsonl",
                    player="ffmpeg",
                    retry_sleep_sec=1,
                    pulse_sink="stream_sink",
                    player_fail_sleep_sec=1,
                    force_pulse_ao=False,
                    snapshot_heartbeat_sec=10,
                    max_track_sec=0,
                    pulse_buffer_duration_ms=250,
                )

            cmd = dj._player_command(track, track_duration_sec=120.0)
            self.assertNotIn("-re", cmd)
            self.assertNotIn("-readrate_catchup", cmd)
            self.assertIn("-buffer_duration", cmd)
            bid = cmd.index("-buffer_duration")
            self.assertEqual(cmd[bid + 1], "250")

    def test_player_env_drops_pulse_server_by_default(self) -> None:
        with mock.patch.dict("os.environ", {"PULSE_SERVER": "unix:/stale/native"}, clear=True):
            env = player_runtime.player_env(pulse_sink="stream_sink")

        self.assertNotIn("PULSE_SERVER", env)
        self.assertEqual(env["PULSE_SINK"], "stream_sink")

    def test_player_env_can_keep_explicit_pulse_server_for_k3s(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "PULSE_SERVER": "unix:/run/stream-pulse/native",
                "AUTO_DJ_KEEP_PULSE_SERVER": "1",
            },
            clear=True,
        ):
            env = player_runtime.player_env(pulse_sink="stream_v3_sink")

        self.assertEqual(env["PULSE_SERVER"], "unix:/run/stream-pulse/native")
        self.assertEqual(env["PULSE_SINK"], "stream_v3_sink")


if __name__ == "__main__":
    unittest.main()
