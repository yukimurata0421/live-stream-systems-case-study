from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest import mock
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.engine import ffmpeg_lifecycle  # noqa: E402


class FfmpegLifecycleTests(unittest.TestCase):
    def test_wait_until_exit_returns_child_exit_code_without_heartbeat_action(self) -> None:
        proc = mock.Mock()
        proc.wait.return_value = 224
        heartbeat_action = mock.Mock(return_value=ffmpeg_lifecycle.HeartbeatAction())
        stop_process = mock.Mock()

        rc = ffmpeg_lifecycle.wait_until_exit_or_action(
            proc,
            heartbeat_sec=5,
            heartbeat_action=heartbeat_action,
            stop_process=stop_process,
        )

        self.assertEqual(rc, 224)
        heartbeat_action.assert_not_called()
        stop_process.assert_not_called()

    def test_wait_until_exit_continues_after_healthy_heartbeat(self) -> None:
        proc = mock.Mock()
        proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=5),
            0,
        ]
        heartbeat_action = mock.Mock(return_value=ffmpeg_lifecycle.HeartbeatAction())
        stop_process = mock.Mock()

        rc = ffmpeg_lifecycle.wait_until_exit_or_action(
            proc,
            heartbeat_sec=5,
            heartbeat_action=heartbeat_action,
            stop_process=stop_process,
        )

        self.assertEqual(rc, 0)
        heartbeat_action.assert_called_once()
        stop_process.assert_not_called()

    def test_wait_until_exit_stops_child_when_heartbeat_requests_restart(self) -> None:
        proc = mock.Mock()
        proc.wait.side_effect = subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=5)
        proc.poll.return_value = None
        heartbeat_action = mock.Mock(return_value=ffmpeg_lifecycle.HeartbeatAction(stop_reason="capture display restarted"))
        stop_process = mock.Mock()

        rc = ffmpeg_lifecycle.wait_until_exit_or_action(
            proc,
            heartbeat_sec=5,
            heartbeat_action=heartbeat_action,
            stop_process=stop_process,
        )

        self.assertEqual(rc, 0)
        stop_process.assert_called_once_with("capture display restarted")

    def test_wait_until_exit_raises_when_stop_process_leaves_child_running(self) -> None:
        proc = mock.Mock()
        proc.wait.side_effect = subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=5)
        proc.poll.return_value = None
        heartbeat_action = mock.Mock(
            return_value=ffmpeg_lifecycle.HeartbeatAction(
                stop_reason="capture display restarted",
                exit_code_if_still_running=96,
            )
        )
        stop_process = mock.Mock(return_value=False)

        with self.assertRaisesRegex(RuntimeError, "ffmpeg still running"):
            ffmpeg_lifecycle.wait_until_exit_or_action(
                proc,
                heartbeat_sec=5,
                heartbeat_action=heartbeat_action,
                stop_process=stop_process,
            )

        stop_process.assert_called_once_with("capture display restarted")

    def test_stop_for_shutdown_escalates_after_grace_timeout(self) -> None:
        proc = mock.Mock()
        proc.pid = 123
        proc.poll.return_value = None
        proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=1),
            -9,
        ]
        events: list[tuple[str, dict]] = []

        def append_event(event_type: str, **fields: object) -> str:
            events.append((event_type, fields))
            return event_type

        ffmpeg_lifecycle.stop_for_shutdown(
            proc,
            reason="stop signal",
            signum=15,
            grace_sec=1,
            append_event=append_event,
            log=lambda _msg: None,
        )

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(
            [event_type for event_type, _fields in events],
            ["ffmpeg_stop_requested", "ffmpeg_stop_timeout_kill", "ffmpeg_stop_killed"],
        )
        self.assertEqual(events[0][1]["signal"], 15)
        self.assertEqual(events[1][1]["grace_sec"], 1)

    def test_stop_for_shutdown_reports_false_when_kill_wait_times_out(self) -> None:
        proc = mock.Mock()
        proc.pid = 123
        proc.poll.return_value = None
        proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=1),
            subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=1),
        ]
        events: list[tuple[str, dict]] = []

        def append_event(event_type: str, **fields: object) -> str:
            events.append((event_type, fields))
            return event_type

        stopped = ffmpeg_lifecycle.stop_for_shutdown(
            proc,
            reason="stop signal",
            signum=15,
            grace_sec=1,
            append_event=append_event,
            log=lambda _msg: None,
        )

        self.assertFalse(stopped)
        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(
            [event_type for event_type, _fields in events],
            ["ffmpeg_stop_requested", "ffmpeg_stop_timeout_kill", "ffmpeg_stop_kill_wait_timeout"],
        )


if __name__ == "__main__":
    unittest.main()
