"""Recognize direct YouTube backup delivery in the watchdog local-send check."""
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

from youtube_watchdog_core import process  # type: ignore  # noqa: E402

FFMPEG_443 = 'ESTAB 0      0        192.0.2.10:35916      198.51.100.7:443 users:(("ffmpeg",pid=91,fd=8))'
FFMPEG_4430 = 'ESTAB 0      0        192.0.2.10:35917      198.51.100.7:4430 users:(("ffmpeg",pid=91,fd=9))'
CHROMIUM_443 = 'ESTAB 0      0        192.0.2.10:53282      198.51.100.8:443 users:(("chromium",pid=239,fd=52))'
FFMPEG_443_CLOSING = 'CLOSE-WAIT 0  0        192.0.2.10:35918      198.51.100.7:443 users:(("ffmpeg",pid=91,fd=10))'


def done(stdout: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="")


class EstablishedConnectionTests(unittest.TestCase):
    def test_matches_only_established_ffmpeg_on_the_exact_peer_port(self):
        self.assertEqual(process.established_ffmpeg_connection(FFMPEG_443, [443]), FFMPEG_443)
        for line in (FFMPEG_4430, CHROMIUM_443, FFMPEG_443_CLOSING, "", "garbage"):
            with self.subTest(line=line):
                self.assertEqual(process.established_ffmpeg_connection(line, [443]), "")


class FallbackIngestTests(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.dict("os.environ", {"STREAM_RUNTIME_SUPERVISOR": "k8s"}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_connected_when_fallback_ready_and_ffmpeg_established(self):
        with mock.patch.object(process, "run", side_effect=[done("1/1"), done(CHROMIUM_443 + "\n" + FFMPEG_443)]) as run:
            self.assertEqual(process.fallback_ingest_connection(), (True, FFMPEG_443))
        get, exec_ = run.call_args_list
        self.assertIn("deployment/stream-v3-youtube-backup", get.args[0])
        self.assertEqual(exec_.args[0][-2:], ["ss", "-tpn"])

    def test_not_connected_when_scaled_down_or_not_ready_without_exec(self):
        for status in ("0/", "1/", "1/0", "", "x/y"):
            with self.subTest(status=status), mock.patch.object(process, "run", return_value=done(status)) as run:
                self.assertEqual(process.fallback_ingest_connection(), (False, ""))
                self.assertEqual(run.call_count, 1)

    def test_not_connected_when_kubectl_fails_or_no_ffmpeg_connection(self):
        with mock.patch.object(process, "run", return_value=done("", returncode=1)):
            self.assertEqual(process.fallback_ingest_connection(), (False, ""))
        with mock.patch.object(process, "run", side_effect=[done("1/1"), done("", returncode=1)]):
            self.assertEqual(process.fallback_ingest_connection(), (False, ""))
        with mock.patch.object(process, "run", side_effect=[done("1/1"), done(CHROMIUM_443)]):
            self.assertEqual(process.fallback_ingest_connection(), (False, ""))

    def test_systemd_mode_never_probes_kubernetes(self):
        with mock.patch.dict("os.environ", {"STREAM_RUNTIME_SUPERVISOR": "systemd"}), \
                mock.patch.object(process, "run") as run:
            self.assertEqual(process.fallback_ingest_connection(), (False, ""))
            run.assert_not_called()

    def test_ports_are_configurable(self):
        with mock.patch.dict("os.environ", {"YTW_FALLBACK_INGEST_TCP_PORTS": "4430, bad"}), \
                mock.patch.object(process, "run", side_effect=[done("1/1"), done(FFMPEG_4430)]):
            self.assertEqual(process.fallback_ingest_connection(), (True, FFMPEG_4430))


if __name__ == "__main__":
    unittest.main()
