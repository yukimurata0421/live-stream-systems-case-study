from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest import mock
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.engine import process_discovery  # noqa: E402


def cp(stdout: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["pgrep"], returncode=0, stdout=stdout, stderr="")


class ProcessDiscoveryTests(unittest.TestCase):
    def test_parse_pgrep_output_ignores_non_pid_rows(self) -> None:
        rows = process_discovery.parse_pgrep_output("123 ffmpeg -i x\nnot-a-pid bad\n456\n")
        self.assertEqual(rows, [(123, "ffmpeg -i x"), (456, "")])

    def test_foreign_rtmp_pids_ignores_test_mode_and_current_pid(self) -> None:
        run_cmd = mock.Mock(return_value=cp("111 ffmpeg rtmps://example/live/key\n222 ffmpeg other\n333 ffmpeg rtmps://example/live/key\n"))

        self.assertEqual(
            process_discovery.foreign_rtmp_pids(
                rtmp_url="rtmps://example/live/key",
                test_mode=True,
                current_pid=111,
                run_cmd=run_cmd,
            ),
            [],
        )
        run_cmd.assert_not_called()

        self.assertEqual(
            process_discovery.foreign_rtmp_pids(
                rtmp_url="rtmps://example/live/key",
                test_mode=False,
                current_pid=111,
                run_cmd=run_cmd,
            ),
            [333],
        )

    def test_stale_capture_helper_pids_matches_owned_resources(self) -> None:
        base_dir = Path("/tmp/stream-v2")
        overlay_dir = base_dir / "ui" / "overlay"
        profile_dir = base_dir / ".state" / "profile"
        outputs = [
            cp("111 Xvfb :99 -screen 0 1920x1080x24\n112 Xvfb :98 -screen 0 1920x1080x24\n"),
            cp(
                f"221 python {base_dir / 'src' / 'stream_core' / 'overlay_server.py'} "
                f"--port 18080 --directory {overlay_dir}\n"
            ),
            cp(
                f"331 chromium --app=http://127.0.0.1:18080/index.html --user-data-dir={profile_dir}\n"
                "332 chromium --user-data-dir=/tmp/other\n"
            ),
        ]
        run_cmd = mock.Mock(side_effect=outputs)

        stale = process_discovery.stale_capture_helper_pids(
            base_dir=base_dir,
            overlay_dir=overlay_dir,
            browser_profile_dir=profile_dir,
            display_name=":99",
            overlay_port=18080,
            current_pid=999,
            run_cmd=run_cmd,
        )

        self.assertEqual(stale, {"xvfb": [111], "overlay": [221], "browser": [331]})

    def test_terminate_stale_pids_escalates_only_alive_processes(self) -> None:
        events: list[tuple[str, dict]] = []
        kills: list[tuple[int, int]] = []

        def append_event(event_type: str, **fields: object) -> str:
            events.append((event_type, fields))
            return event_type

        process_discovery.terminate_stale_pids(
            "browser",
            [1, 200, 200, 300],
            current_pid=300,
            pid_alive=lambda pid: pid == 200,
            append_event=append_event,
            log=lambda _msg: None,
            kill=lambda pid, sig: kills.append((pid, sig)),
            sleep=lambda _sec: None,
        )

        self.assertEqual([event_type for event_type, _fields in events], ["stale_capture_helper_kill", "stale_capture_helper_kill"])
        self.assertEqual(events[0][1]["signal"], "TERM")
        self.assertEqual(events[1][1]["signal"], "KILL")
        self.assertEqual([pid for pid, _sig in kills], [200, 200])


if __name__ == "__main__":
    unittest.main()
