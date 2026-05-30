from __future__ import annotations

import json
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import fast_recovery  # type: ignore


class FastRecoveryYoutubeWarningTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._stats_path = Path(self._tmpdir.name) / "youtube_watchdog_stats.json"
        self._quota_path = Path(self._tmpdir.name) / "youtube_quota_state.json"

        self._orig_stats_file = fast_recovery.YTW_STATS_FILE
        self._orig_quota_file = fast_recovery.QUOTA_STATE_FILE
        self._orig_url_mode = fast_recovery.URL_PRESERVATION_MODE
        self._orig_age = fast_recovery.YTW_STATUS_MAX_AGE_SEC
        self._orig_remote_require_local_ok = fast_recovery.REMOTE_WARNING_REQUIRE_LOCAL_OK
        self._orig_remote_distinct_stats = fast_recovery.REMOTE_WARNING_CONFIRM_DISTINCT_STATS

        fast_recovery.YTW_STATS_FILE = self._stats_path
        fast_recovery.QUOTA_STATE_FILE = self._quota_path
        fast_recovery.URL_PRESERVATION_MODE = True
        fast_recovery.YTW_STATUS_MAX_AGE_SEC = 180
        fast_recovery.REMOTE_WARNING_REQUIRE_LOCAL_OK = True
        fast_recovery.REMOTE_WARNING_CONFIRM_DISTINCT_STATS = True

    def tearDown(self) -> None:
        fast_recovery.YTW_STATS_FILE = self._orig_stats_file
        fast_recovery.QUOTA_STATE_FILE = self._orig_quota_file
        fast_recovery.URL_PRESERVATION_MODE = self._orig_url_mode
        fast_recovery.YTW_STATUS_MAX_AGE_SEC = self._orig_age
        fast_recovery.REMOTE_WARNING_REQUIRE_LOCAL_OK = self._orig_remote_require_local_ok
        fast_recovery.REMOTE_WARNING_CONFIRM_DISTINCT_STATS = self._orig_remote_distinct_stats
        self._tmpdir.cleanup()

    def _write_stats(
        self,
        *,
        ts_utc: str,
        remote_probe_ts_utc: str = "",
        remote_sample_id: str = "",
        remote_source_detail: str = "",
        api_live_state: str = "live",
        lifecycle: str = "live",
        stream_status: str = "inactive",
        stream_health: str = "noData",
        quota_guard_active: bool = False,
        remote_source: str = "",
        remote_status: str = "",
        remote_reason: str = "",
        stream_active: bool = True,
        ffmpeg_pid: int = 222,
        ingest_connected: bool = True,
        local_ok: bool = True,
    ) -> None:
        payload = {
            "ts_utc": ts_utc,
            "remote_probe_ts_utc": remote_probe_ts_utc,
            "remote_sample_id": remote_sample_id,
            "remote_sample_source": remote_source_detail,
            "api_live_state": api_live_state,
            "oauth_life_cycle_status": lifecycle,
            "oauth_stream_status": stream_status,
            "oauth_stream_health_status": stream_health,
            "quota_guard_active": quota_guard_active,
            "remote_source": remote_source,
            "remote_status": remote_status,
            "remote_reason": remote_reason,
            "stream_active": stream_active,
            "ffmpeg_pid": ffmpeg_pid,
            "ingest_connected": ingest_connected,
            "local_ok": local_ok,
        }
        self._stats_path.write_text(json.dumps(payload), encoding="utf-8")

    def _write_quota_state(
        self,
        *,
        quota_exhausted: bool,
        quota_exhausted_until_ts: int = 0,
        source: str = "data_api_search",
    ) -> None:
        payload = {
            "quota_exhausted": quota_exhausted,
            "quota_exhausted_until_ts": quota_exhausted_until_ts,
            "quota_exhausted_source": source,
        }
        self._quota_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_ignores_stats_older_than_last_restart(self) -> None:
        now_ts = 2_000
        # 1970-01-01T00:33:10Z -> unix 1990
        self._write_stats(ts_utc="1970-01-01T00:33:10Z")

        warning, reason, _payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )

        self.assertFalse(warning)
        self.assertIn("older than last restart", reason)

    def test_ignores_stats_when_remote_probe_is_older_than_last_restart(self) -> None:
        now_ts = 2_010
        self._write_stats(
            ts_utc="1970-01-01T00:33:28Z",
            remote_probe_ts_utc="1970-01-01T00:33:10Z",
        )

        warning, reason, _payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )

        self.assertFalse(warning)
        self.assertIn("remote probe older than last restart", reason)

    def test_detects_remote_warning_with_fresh_stats(self) -> None:
        now_ts = 2_000
        # 1970-01-01T00:33:18Z -> unix 1998
        self._write_stats(ts_utc="1970-01-01T00:33:18Z")

        warning, reason, _payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )

        self.assertTrue(warning)
        self.assertIn("streamStatus=inactive", reason)

    def test_remote_warning_confirm_uses_remote_sample_id_before_stats_timestamp(self) -> None:
        state = {"remote_warning_streak": 0, "remote_warning_last_stats_ts": 0}
        payload1 = {
            "ts_utc": "1970-01-01T00:33:18Z",
            "remote_probe_ts_utc": "1970-01-01T00:33:18Z",
            "remote_sample_id": "sample-a",
            "recovery_episode_id": "episode-1",
            "ffmpeg_generation": "stream_pid=1:ffmpeg_pid=222",
            "stream_active": True,
            "ffmpeg_pid": 222,
            "ingest_connected": True,
            "local_ok": True,
        }
        payload2 = {
            "ts_utc": "1970-01-01T00:33:19Z",
            "remote_probe_ts_utc": "1970-01-01T00:33:18Z",
            "remote_sample_id": "sample-a",
            "recovery_episode_id": "episode-1",
            "ffmpeg_generation": "stream_pid=1:ffmpeg_pid=222",
            "stream_active": True,
            "ffmpeg_pid": 222,
            "ingest_connected": True,
            "local_ok": True,
        }
        payload3 = {
            "ts_utc": "1970-01-01T00:33:19Z",
            "remote_probe_ts_utc": "1970-01-01T00:33:19Z",
            "remote_sample_id": "sample-b",
            "recovery_episode_id": "episode-1",
            "ffmpeg_generation": "stream_pid=1:ffmpeg_pid=222",
            "stream_active": True,
            "ffmpeg_pid": 222,
            "ingest_connected": True,
            "local_ok": True,
        }

        self.assertEqual(fast_recovery.update_remote_warning_streak(state, True, payload1), 1)
        self.assertEqual(fast_recovery.update_remote_warning_streak(state, True, payload2), 1)
        self.assertEqual(fast_recovery.update_remote_warning_streak(state, True, payload3), 2)
        self.assertEqual(state.get("remote_warning_last_probe_ts"), 1999)

    def test_remote_warning_confirm_resets_across_episode_or_ffmpeg_generation(self) -> None:
        state = {"remote_warning_streak": 0}

        def payload(sample_id: str, episode_id: str, generation: str) -> dict:
            return {
                "ts_utc": "1970-01-01T00:33:18Z",
                "remote_probe_ts_utc": "1970-01-01T00:33:18Z",
                "remote_sample_id": sample_id,
                "recovery_episode_id": episode_id,
                "ffmpeg_generation": generation,
                "stream_active": True,
                "ffmpeg_pid": 222,
                "ingest_connected": True,
                "local_ok": True,
            }

        self.assertEqual(
            fast_recovery.update_remote_warning_streak(
                state,
                True,
                payload("sample-a", "episode-1", "stream_pid=1:ffmpeg_pid=222"),
            ),
            1,
        )
        self.assertEqual(
            fast_recovery.update_remote_warning_streak(
                state,
                True,
                payload("sample-b", "episode-2", "stream_pid=1:ffmpeg_pid=222"),
            ),
            1,
        )
        self.assertEqual(
            fast_recovery.update_remote_warning_streak(
                state,
                True,
                payload("sample-c", "episode-2", "stream_pid=1:ffmpeg_pid=333"),
            ),
            1,
        )

    def test_remote_warning_confirm_requires_local_ok_sample_state(self) -> None:
        state = {"remote_warning_streak": 1}
        payload = {
            "ts_utc": "1970-01-01T00:33:18Z",
            "remote_probe_ts_utc": "1970-01-01T00:33:18Z",
            "remote_sample_id": "sample-a",
            "recovery_episode_id": "episode-1",
            "ffmpeg_generation": "stream_pid=1:ffmpeg_pid=222",
            "stream_active": True,
            "ffmpeg_pid": 222,
            "ingest_connected": False,
            "local_ok": False,
        }
        self.assertEqual(fast_recovery.update_remote_warning_streak(state, True, payload), 0)

    def test_ignores_remote_warning_until_local_ingest_reestablished(self) -> None:
        now_ts = 2_000
        self._write_stats(
            ts_utc="1970-01-01T00:33:18Z",
            stream_active=True,
            ffmpeg_pid=0,
            ingest_connected=False,
            local_ok=False,
        )

        warning, reason, _payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )

        self.assertFalse(warning)
        self.assertIn("local ingest re-established", reason)

    def test_ignores_remote_warning_while_quota_guard_active(self) -> None:
        now_ts = 2_000
        self._write_quota_state(quota_exhausted=False)
        self._write_stats(ts_utc="1970-01-01T00:33:18Z", quota_guard_active=True)

        warning, reason, _payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )

        self.assertFalse(warning)
        self.assertIn("quota guard active", reason)

    def test_non_api_remote_source_not_suppressed_by_quota_guard(self) -> None:
        now_ts = 2_000
        self._write_quota_state(quota_exhausted=False)
        self._write_stats(
            ts_utc="1970-01-01T00:33:18Z",
            quota_guard_active=True,
            remote_source="channel_live_page",
            remote_status="warning",
            remote_reason="channel live page says offline",
        )

        warning, reason, payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )

        self.assertTrue(warning)
        self.assertIn("channel live page says offline", reason)
        self.assertEqual(payload.get("remote_source"), "channel_live_page")

    def test_quota_guard_suppresses_oauth_source_alias(self) -> None:
        now_ts = 2_000
        self._write_quota_state(quota_exhausted=False)
        self._write_stats(
            ts_utc="1970-01-01T00:33:18Z",
            quota_guard_active=True,
            remote_source="oauth",
            remote_status="warning",
            remote_reason="oauth unavailable",
        )
        warning, reason, _payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )
        self.assertFalse(warning)
        self.assertIn("suppressed source=oauth", reason)

    def test_reads_quota_state_directly_before_stats_flag_updates(self) -> None:
        now_ts = 2_000
        self._write_quota_state(quota_exhausted=True, quota_exhausted_until_ts=9_999)
        self._write_stats(
            ts_utc="1970-01-01T00:33:18Z",
            quota_guard_active=False,
            remote_source="data_api_search",
            remote_status="warning",
            remote_reason="data api ended",
        )
        warning, reason, _payload = fast_recovery.read_youtube_live_warning(
            now_ts=now_ts,
            last_restart_ts=1_995,
        )
        self.assertFalse(warning)
        self.assertIn("quota state active", reason)


class FastRecoveryBackoffTests(unittest.TestCase):
    def test_restart_failure_backoff_left_when_active(self) -> None:
        left = fast_recovery.restart_failure_backoff_left(
            now_ts=1_000,
            last_restart_failure_ts=980,
            backoff_sec=30,
        )
        self.assertEqual(left, 10)

    def test_restart_failure_backoff_left_when_expired(self) -> None:
        left = fast_recovery.restart_failure_backoff_left(
            now_ts=1_000,
            last_restart_failure_ts=960,
            backoff_sec=30,
        )
        self.assertEqual(left, 0)


class FastRecoveryMainBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._state_path = Path(self._tmpdir.name) / "fast_recovery_state.json"
        self._events_path = Path(self._tmpdir.name) / "fast_recovery_events.jsonl"
        self._stats_path = Path(self._tmpdir.name) / "youtube_watchdog_stats.json"
        self._restart_reason_path = Path(self._tmpdir.name) / "restart_reason.json"

        self._orig_values = {
            "STATE_FILE": fast_recovery.STATE_FILE,
            "EVENT_LOG_FILE": fast_recovery.EVENT_LOG_FILE,
            "YTW_STATS_FILE": fast_recovery.YTW_STATS_FILE,
            "RESTART_REASON_FILE": fast_recovery.RESTART_REASON_FILE,
            "URL_PRESERVATION_MODE": fast_recovery.URL_PRESERVATION_MODE,
            "YTW_STATUS_MAX_AGE_SEC": fast_recovery.YTW_STATUS_MAX_AGE_SEC,
            "NET_FAIL_CONFIRM": fast_recovery.NET_FAIL_CONFIRM,
            "STALL_CONFIRM": fast_recovery.STALL_CONFIRM,
            "REMOTE_WARNING_CONFIRM": fast_recovery.REMOTE_WARNING_CONFIRM,
            "REMOTE_WARNING_REQUIRE_LOCAL_OK": fast_recovery.REMOTE_WARNING_REQUIRE_LOCAL_OK,
            "REMOTE_WARNING_CONFIRM_DISTINCT_STATS": fast_recovery.REMOTE_WARNING_CONFIRM_DISTINCT_STATS,
            "MIN_FFMPEG_UPTIME_SEC": fast_recovery.MIN_FFMPEG_UPTIME_SEC,
            "FFMPEG_MISSING_RESTART_SEC": fast_recovery.FFMPEG_MISSING_RESTART_SEC,
            "FFMPEG_MISSING_SUCCESS_BACKOFF_SEC": fast_recovery.FFMPEG_MISSING_SUCCESS_BACKOFF_SEC,
            "RESTART_GUARD_SEC": fast_recovery.RESTART_GUARD_SEC,
            "RESTART_FAILURE_BACKOFF_SEC": fast_recovery.RESTART_FAILURE_BACKOFF_SEC,
            "HOURLY_DOWNTIME_BUDGET_SEC": fast_recovery.HOURLY_DOWNTIME_BUDGET_SEC,
            "DAILY_DOWNTIME_BUDGET_SEC": fast_recovery.DAILY_DOWNTIME_BUDGET_SEC,
            "RESTART_DOWNTIME_COST_SEC": fast_recovery.RESTART_DOWNTIME_COST_SEC,
            "BUDGET_EMERGENCY_OVERRIDE_SEC": fast_recovery.BUDGET_EMERGENCY_OVERRIDE_SEC,
            "TCP_SEND_SAMPLE_LOG_SEC": fast_recovery.TCP_SEND_SAMPLE_LOG_SEC,
            "LOW_UPLOAD_PRESSURE_ENABLED": fast_recovery.LOW_UPLOAD_PRESSURE_ENABLED,
            "LOW_UPLOAD_PRESSURE_CONFIRM": fast_recovery.LOW_UPLOAD_PRESSURE_CONFIRM,
            "LOW_UPLOAD_PRESSURE_MAX_MBPS": fast_recovery.LOW_UPLOAD_PRESSURE_MAX_MBPS,
            "LOW_UPLOAD_PRESSURE_NOTSENT_BYTES": fast_recovery.LOW_UPLOAD_PRESSURE_NOTSENT_BYTES,
            "LOW_UPLOAD_PRESSURE_UNACKED": fast_recovery.LOW_UPLOAD_PRESSURE_UNACKED,
            "LOW_UPLOAD_PRESSURE_LASTSND_MS": fast_recovery.LOW_UPLOAD_PRESSURE_LASTSND_MS,
            "EMERGENCY_LOW_UPLOAD_ENABLED": fast_recovery.EMERGENCY_LOW_UPLOAD_ENABLED,
            "EMERGENCY_LOW_UPLOAD_TRIGGERS": fast_recovery.EMERGENCY_LOW_UPLOAD_TRIGGERS,
            "EMERGENCY_LOW_UPLOAD_DURATION_SEC": fast_recovery.EMERGENCY_LOW_UPLOAD_DURATION_SEC,
            "EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE": fast_recovery.EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE,
            "EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE": fast_recovery.EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE,
            "EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE": fast_recovery.EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE,
            "EMERGENCY_LOW_UPLOAD_AUDIO_BITRATE": fast_recovery.EMERGENCY_LOW_UPLOAD_AUDIO_BITRATE,
        }

        fast_recovery.STATE_FILE = self._state_path
        fast_recovery.EVENT_LOG_FILE = self._events_path
        fast_recovery.YTW_STATS_FILE = self._stats_path
        fast_recovery.RESTART_REASON_FILE = self._restart_reason_path

        fast_recovery.URL_PRESERVATION_MODE = True
        fast_recovery.YTW_STATUS_MAX_AGE_SEC = 180
        fast_recovery.NET_FAIL_CONFIRM = 1
        fast_recovery.STALL_CONFIRM = 2
        fast_recovery.REMOTE_WARNING_CONFIRM = 1
        fast_recovery.REMOTE_WARNING_REQUIRE_LOCAL_OK = True
        fast_recovery.REMOTE_WARNING_CONFIRM_DISTINCT_STATS = True
        fast_recovery.MIN_FFMPEG_UPTIME_SEC = 20
        fast_recovery.FFMPEG_MISSING_RESTART_SEC = 20
        fast_recovery.FFMPEG_MISSING_SUCCESS_BACKOFF_SEC = 60
        fast_recovery.RESTART_GUARD_SEC = 5
        fast_recovery.RESTART_FAILURE_BACKOFF_SEC = 30
        fast_recovery.HOURLY_DOWNTIME_BUDGET_SEC = 300
        fast_recovery.DAILY_DOWNTIME_BUDGET_SEC = 1800
        fast_recovery.RESTART_DOWNTIME_COST_SEC = 30
        fast_recovery.BUDGET_EMERGENCY_OVERRIDE_SEC = 90
        fast_recovery.TCP_SEND_SAMPLE_LOG_SEC = 60
        fast_recovery.LOW_UPLOAD_PRESSURE_ENABLED = True
        fast_recovery.LOW_UPLOAD_PRESSURE_CONFIRM = 3
        fast_recovery.LOW_UPLOAD_PRESSURE_MAX_MBPS = 3.2
        fast_recovery.LOW_UPLOAD_PRESSURE_NOTSENT_BYTES = 524288
        fast_recovery.LOW_UPLOAD_PRESSURE_UNACKED = 256
        fast_recovery.LOW_UPLOAD_PRESSURE_LASTSND_MS = 1000
        fast_recovery.EMERGENCY_LOW_UPLOAD_ENABLED = True
        fast_recovery.EMERGENCY_LOW_UPLOAD_TRIGGERS = {"network_down", "low_upload_pressure"}
        fast_recovery.EMERGENCY_LOW_UPLOAD_DURATION_SEC = 900
        fast_recovery.EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE = "2500k"
        fast_recovery.EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE = "2500k"
        fast_recovery.EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE = "5000k"
        fast_recovery.EMERGENCY_LOW_UPLOAD_AUDIO_BITRATE = ""

    def tearDown(self) -> None:
        for name, value in self._orig_values.items():
            setattr(fast_recovery, name, value)
        self._tmpdir.cleanup()

    def _iso(self, unix_ts: int) -> str:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _write_stats(
        self,
        *,
        ts_unix: int,
        remote_probe_ts_unix: int = 0,
        remote_sample_id: str = "",
        api_live_state: str = "live",
        lifecycle: str = "live",
        stream_status: str = "inactive",
        stream_health: str = "noData",
    ) -> None:
        payload = {
            "ts_utc": self._iso(ts_unix),
            "remote_probe_ts_utc": self._iso(remote_probe_ts_unix) if remote_probe_ts_unix > 0 else "",
            "remote_sample_id": remote_sample_id,
            "api_live_state": api_live_state,
            "oauth_life_cycle_status": lifecycle,
            "oauth_stream_status": stream_status,
            "oauth_stream_health_status": stream_health,
            "stream_active": True,
            "ffmpeg_pid": 222,
            "ingest_connected": True,
            "local_ok": True,
        }
        self._stats_path.write_text(json.dumps(payload), encoding="utf-8")

    def _default_state(self) -> dict[str, object]:
        return {
            "last_pid": 0,
            "last_bytes_sent": 0,
            "last_bytes_sent_ts": 0,
            "net_fail_streak": 0,
            "stall_streak": 0,
            "low_upload_pressure_streak": 0,
            "remote_warning_streak": 0,
            "last_restart_ts": 0,
            "last_restart_failure_ts": 0,
            "restart_failure_count": 0,
            "last_reason": "",
            "last_budget_block_key": "",
            "last_budget_block_ts": 0,
            "last_tcp_send_sample_ts": 0,
            "last_tcp_send_sample_pid": 0,
            "last_tcp_send_sample_bytes_sent": 0,
            "restart_events": [],
            "samples": [],
        }

    def _write_state(self, **overrides: object) -> None:
        payload = self._default_state()
        payload.update(overrides)
        self._state_path.write_text(json.dumps(payload), encoding="utf-8")

    def _read_state(self) -> dict[str, object]:
        if not self._state_path.exists():
            return self._default_state()
        return json.loads(self._state_path.read_text(encoding="utf-8"))

    def _read_events(self) -> list[dict[str, object]]:
        if not self._events_path.exists():
            return []
        out: list[dict[str, object]] = []
        for line in self._events_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                out.append(json.loads(line))
        return out

    def _read_restart_reason(self) -> dict[str, object]:
        if not self._restart_reason_path.exists():
            return {}
        return json.loads(self._restart_reason_path.read_text(encoding="utf-8"))

    def _invoke_main(
        self,
        *,
        now_ts: int,
        dns_ok_value: bool,
        tcp_ok_value: bool,
        ping_results: bool | dict[str, bool],
        restart_return: tuple[bool, str],
        youtube_warning: tuple[bool, str, dict[str, object]] | None,
        ffmpeg_pid: int = 222,
        ffmpeg_uptime_sec: int = 120,
        tcp_metrics: dict[str, int | str] | None = None,
    ):
        if isinstance(ping_results, bool):
            def ping_side_effect(_target: str, _timeout_sec: int = 1) -> bool:
                return ping_results
        else:
            def ping_side_effect(target: str, _timeout_sec: int = 1) -> bool:
                return bool(ping_results.get(target, False))

        with ExitStack() as stack:
            stack.enter_context(patch.object(fast_recovery.time, "time", return_value=now_ts))
            stack.enter_context(patch.object(fast_recovery, "get_main_pid", return_value=111))
            stack.enter_context(patch.object(fast_recovery, "get_child_ffmpeg_pid", return_value=ffmpeg_pid))
            stack.enter_context(patch.object(fast_recovery, "get_process_elapsed_sec", return_value=ffmpeg_uptime_sec))
            stack.enter_context(
                patch.object(
                    fast_recovery,
                    "parse_ffmpeg_tcp_metrics",
                    return_value=tcp_metrics
                    if tcp_metrics is not None
                    else {"bytes_sent": 100, "notsent": 0, "unacked": 0, "lastsnd_ms": 0},
                )
            )
            stack.enter_context(patch.object(fast_recovery, "get_default_gateway", return_value="192.0.2.1"))
            stack.enter_context(patch.object(fast_recovery, "ping_ok", side_effect=ping_side_effect))
            stack.enter_context(patch.object(fast_recovery, "dns_ok", return_value=dns_ok_value))
            stack.enter_context(patch.object(fast_recovery, "tcp_probe_ok", return_value=tcp_ok_value))
            restart_mock = stack.enter_context(
                patch.object(fast_recovery, "restart_stream", return_value=restart_return)
            )
            if youtube_warning is not None:
                if youtube_warning[0] and not youtube_warning[2]:
                    youtube_warning = (
                        youtube_warning[0],
                        youtube_warning[1],
                        {
                            "ts_utc": self._iso(now_ts),
                            "remote_probe_ts_utc": self._iso(now_ts),
                            "remote_sample_id": f"sample-{now_ts}",
                            "recovery_episode_id": "episode-main",
                            "ffmpeg_generation": f"stream_pid=111:ffmpeg_pid={ffmpeg_pid}",
                            "stream_active": True,
                            "ffmpeg_pid": ffmpeg_pid,
                            "ingest_connected": True,
                            "local_ok": True,
                        },
                    )
                stack.enter_context(
                    patch.object(fast_recovery, "read_youtube_live_warning", return_value=youtube_warning)
                )

            rc = fast_recovery.main()

        return rc, restart_mock

    def test_main_remote_warning_with_old_stats_does_not_restart(self) -> None:
        self._write_state(last_restart_ts=1_995)
        self._write_stats(ts_unix=1_990)

        rc, restart_mock = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_not_called()

        state = self._read_state()
        self.assertEqual(state.get("remote_warning_streak"), 0)
        self.assertEqual(state.get("last_reason"), "healthy")
        self.assertEqual(self._read_events(), [])

    def test_main_remote_warning_with_new_stats_restarts(self) -> None:
        self._write_state(last_restart_ts=1_995)
        self._write_stats(ts_unix=1_998)

        rc, restart_mock = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_called_once()
        self.assertIn(
            "youtube pre-loss warning while broadcast live",
            restart_mock.call_args.args[0],
        )

        state = self._read_state()
        self.assertEqual(state.get("last_restart_ts"), 2_000)

        events = self._read_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].get("kind"), "restart")
        self.assertEqual(events[0].get("trigger"), "remote_warning")

    def test_replay_tcp_stall_two_confirmations_restart_stream(self) -> None:
        fast_recovery.STALL_CONFIRM = 2
        stall_metrics = {
            "bytes_sent": 500,
            "notsent": fast_recovery.STALL_NOTSENT_BYTES,
            "unacked": 0,
            "lastsnd_ms": fast_recovery.STALL_LASTSND_MS,
        }
        self._write_state(last_pid=222, last_bytes_sent=500, last_restart_ts=1_000)

        rc1, restart_mock1 = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(False, "youtube ok", {}),
            tcp_metrics=stall_metrics,
        )
        self.assertEqual(rc1, 0)
        restart_mock1.assert_not_called()
        self.assertEqual(self._read_state().get("stall_streak"), 1)

        rc2, restart_mock2 = self._invoke_main(
            now_ts=2_005,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(False, "youtube ok", {}),
            tcp_metrics=stall_metrics,
        )
        self.assertEqual(rc2, 0)
        restart_mock2.assert_called_once()
        self.assertIn("tcp stall", restart_mock2.call_args.args[0])

        state = self._read_state()
        self.assertEqual(state.get("last_restart_ts"), 2_005)
        events = self._read_events()
        self.assertEqual(events[-1].get("kind"), "restart")
        self.assertEqual(events[-1].get("trigger"), "tcp_stall")

    def test_tcp_send_sample_logs_effective_mbps_without_restart(self) -> None:
        self._write_state(
            last_pid=222,
            last_bytes_sent=1_000_000,
            last_bytes_sent_ts=1_940,
            last_tcp_send_sample_ts=1_940,
            last_tcp_send_sample_pid=222,
            last_tcp_send_sample_bytes_sent=1_000_000,
        )
        rc, restart_mock = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(False, "youtube ok", {}),
            tcp_metrics={
                "bytes_sent": 46_000_000,
                "notsent": 0,
                "unacked": 0,
                "lastsnd_ms": 10,
                "conn": "ESTAB tcp 0 0 local remote users:(('ffmpeg',pid=222,fd=1))",
            },
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_not_called()
        events = self._read_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].get("kind"), "tcp_send_sample")
        self.assertEqual(events[0].get("sample_interval_sec"), 60)
        self.assertEqual(events[0].get("bytes_sent_delta"), 45_000_000)
        self.assertEqual(events[0].get("mbps"), 6.0)

    def test_low_upload_pressure_restarts_with_emergency_profile(self) -> None:
        fast_recovery.LOW_UPLOAD_PRESSURE_CONFIRM = 3
        fast_recovery.LOW_UPLOAD_PRESSURE_MAX_MBPS = 3.2
        self._write_state(
            last_pid=222,
            last_bytes_sent=10_000_000,
            last_bytes_sent_ts=1_995,
            last_restart_ts=1_000,
        )
        common_metrics = {
            "notsent": fast_recovery.LOW_UPLOAD_PRESSURE_NOTSENT_BYTES,
            "unacked": 0,
            "lastsnd_ms": 100,
        }

        for now_ts, bytes_sent in ((2_000, 11_250_000), (2_005, 12_500_000)):
            rc, restart_mock = self._invoke_main(
                now_ts=now_ts,
                dns_ok_value=True,
                tcp_ok_value=True,
                ping_results=True,
                restart_return=(True, ""),
                youtube_warning=(False, "youtube ok", {}),
                tcp_metrics={**common_metrics, "bytes_sent": bytes_sent},
            )
            self.assertEqual(rc, 0)
            restart_mock.assert_not_called()

        rc3, restart_mock3 = self._invoke_main(
            now_ts=2_010,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(False, "youtube ok", {}),
            tcp_metrics={**common_metrics, "bytes_sent": 13_750_000},
        )
        self.assertEqual(rc3, 0)
        restart_mock3.assert_called_once()
        self.assertIn("low upload pressure", restart_mock3.call_args.args[0])

        events = self._read_events()
        self.assertEqual(events[-1].get("kind"), "restart")
        self.assertEqual(events[-1].get("trigger"), "low_upload_pressure")
        self.assertEqual(events[-1].get("metrics", {}).get("send_mbps"), 2.0)

        restart_reason = self._read_restart_reason()
        self.assertEqual(restart_reason.get("trigger"), "low_upload_pressure")
        profile = restart_reason.get("emergency_low_upload_profile")
        self.assertIsInstance(profile, dict)
        self.assertEqual(profile.get("name"), "low_upload_pressure_low_upload")
        self.assertEqual(profile.get("video_bitrate"), "2500k")

    def test_low_upload_pressure_requires_queue_pressure(self) -> None:
        fast_recovery.LOW_UPLOAD_PRESSURE_CONFIRM = 1
        self._write_state(
            last_pid=222,
            last_bytes_sent=10_000_000,
            last_bytes_sent_ts=1_995,
            last_restart_ts=1_000,
        )

        rc, restart_mock = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(False, "youtube ok", {}),
            tcp_metrics={
                "bytes_sent": 11_250_000,
                "notsent": 0,
                "unacked": 0,
                "lastsnd_ms": 10,
            },
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_not_called()
        self.assertEqual(self._read_state().get("low_upload_pressure_streak"), 0)

    def test_replay_tcp_stall_is_preferred_after_remote_warning_is_not_confirmed(self) -> None:
        fast_recovery.STALL_CONFIRM = 2
        fast_recovery.REMOTE_WARNING_CONFIRM = 2
        stall_metrics = {
            "bytes_sent": 700,
            "notsent": fast_recovery.STALL_NOTSENT_BYTES,
            "unacked": 0,
            "lastsnd_ms": fast_recovery.STALL_LASTSND_MS,
        }
        self._write_state(last_pid=222, last_bytes_sent=700, last_restart_ts=1_000, stall_streak=1)

        rc, restart_mock = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(True, "streamStatus=inactive", {}),
            tcp_metrics=stall_metrics,
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_called_once()
        self.assertIn("tcp stall", restart_mock.call_args.args[0])
        self.assertEqual(self._read_events()[-1].get("trigger"), "tcp_stall")

    def test_main_remote_warning_confirm_requires_distinct_stats_timestamp(self) -> None:
        fast_recovery.REMOTE_WARNING_CONFIRM = 2
        self._write_state(last_restart_ts=1_995)
        self._write_stats(ts_unix=1_998)

        rc1, restart_mock1 = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )
        self.assertEqual(rc1, 0)
        restart_mock1.assert_not_called()
        self.assertEqual(self._read_state().get("remote_warning_streak"), 1)

        rc2, restart_mock2 = self._invoke_main(
            now_ts=2_005,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )
        self.assertEqual(rc2, 0)
        restart_mock2.assert_not_called()
        self.assertEqual(self._read_state().get("remote_warning_streak"), 1)

        self._write_stats(ts_unix=2_006)
        rc3, restart_mock3 = self._invoke_main(
            now_ts=2_010,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )
        self.assertEqual(rc3, 0)
        restart_mock3.assert_called_once()
        self.assertEqual(self._read_state().get("last_restart_ts"), 2_010)

    def test_main_remote_warning_confirm_advances_on_distinct_remote_sample_same_stats_timestamp(self) -> None:
        fast_recovery.REMOTE_WARNING_CONFIRM = 2
        self._write_state(last_restart_ts=1_995)
        self._write_stats(ts_unix=2_006, remote_probe_ts_unix=1_998, remote_sample_id="sample-a")

        rc1, restart_mock1 = self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )
        self.assertEqual(rc1, 0)
        restart_mock1.assert_not_called()
        self.assertEqual(self._read_state().get("remote_warning_streak"), 1)

        self._write_stats(ts_unix=2_006, remote_probe_ts_unix=2_005, remote_sample_id="sample-b")
        rc2, restart_mock2 = self._invoke_main(
            now_ts=2_010,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )
        self.assertEqual(rc2, 0)
        restart_mock2.assert_called_once()
        self.assertEqual(self._read_state().get("last_restart_ts"), 2_010)

    def test_main_remote_warning_does_not_advance_when_only_stats_file_timestamp_changes(self) -> None:
        fast_recovery.REMOTE_WARNING_CONFIRM = 2
        self._write_state(last_restart_ts=1_995)
        self._write_stats(ts_unix=2_006, remote_probe_ts_unix=1_998, remote_sample_id="sample-a")

        self._invoke_main(
            now_ts=2_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )
        self._write_stats(ts_unix=2_008, remote_probe_ts_unix=1_998, remote_sample_id="sample-a")
        rc2, restart_mock2 = self._invoke_main(
            now_ts=2_010,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
        )
        self.assertEqual(rc2, 0)
        restart_mock2.assert_not_called()
        self.assertEqual(self._read_state().get("remote_warning_streak"), 1)

    def test_main_ping_only_fail_does_not_restart(self) -> None:
        self._write_state()

        rc, restart_mock = self._invoke_main(
            now_ts=3_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=False,
            restart_return=(True, ""),
            youtube_warning=(False, "ok", {}),
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_not_called()

        state = self._read_state()
        self.assertEqual(state.get("net_fail_streak"), 0)
        self.assertEqual(state.get("last_reason"), "healthy")

    def test_main_dns_and_rtmp_fail_triggers_network_down(self) -> None:
        self._write_state()

        rc, restart_mock = self._invoke_main(
            now_ts=3_000,
            dns_ok_value=False,
            tcp_ok_value=False,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(False, "ok", {}),
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_called_once()
        self.assertIn("network down", restart_mock.call_args.args[0])
        self.assertIn("dns_ok=False", restart_mock.call_args.args[0])
        self.assertIn("tcp_probe_ok=False", restart_mock.call_args.args[0])

        events = self._read_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].get("kind"), "restart")
        self.assertEqual(events[0].get("trigger"), "network_down")
        restart_reason = self._read_restart_reason()
        self.assertEqual(restart_reason.get("source"), "fast_recovery")
        self.assertEqual(restart_reason.get("trigger"), "network_down")
        profile = restart_reason.get("emergency_low_upload_profile")
        self.assertIsInstance(profile, dict)
        self.assertEqual(profile.get("video_bitrate"), "2500k")
        self.assertEqual(profile.get("video_maxrate"), "2500k")
        self.assertEqual(profile.get("video_bufsize"), "5000k")

    def test_main_restart_failed_enters_30s_backoff(self) -> None:
        self._write_state()

        rc1, restart_mock1 = self._invoke_main(
            now_ts=4_000,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(False, "unit failed"),
            youtube_warning=(True, "streamStatus=inactive", {}),
        )
        self.assertEqual(rc1, 0)
        restart_mock1.assert_called_once()

        state1 = self._read_state()
        self.assertEqual(state1.get("last_restart_failure_ts"), 4_000)
        self.assertEqual(state1.get("restart_failure_count"), 1)

        rc2, restart_mock2 = self._invoke_main(
            now_ts=4_010,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(True, "streamStatus=inactive", {}),
        )
        self.assertEqual(rc2, 0)
        restart_mock2.assert_not_called()

        state2 = self._read_state()
        self.assertEqual(state2.get("restart_failure_count"), 1)
        self.assertIn("restart failure backoff active", str(state2.get("last_reason")))

        rc3, restart_mock3 = self._invoke_main(
            now_ts=4_031,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(True, "streamStatus=inactive", {}),
        )
        self.assertEqual(rc3, 0)
        restart_mock3.assert_called_once()

    def test_main_budget_exceeded_records_restart_budget_block(self) -> None:
        now_ts = 5_000
        restart_events = [
            {"ts": now_ts - 60 - i, "downtime_sec": 30, "reason": "remote_warning"}
            for i in range(10)
        ]
        self._write_state(restart_events=restart_events)

        rc, restart_mock = self._invoke_main(
            now_ts=now_ts,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(True, "streamStatus=inactive", {}),
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_not_called()

        state = self._read_state()
        self.assertIn("hourly downtime budget exceeded", str(state.get("last_reason")))

        events = self._read_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].get("kind"), "restart_budget_block")
        self.assertEqual(events[0].get("trigger"), "remote_warning")

    def test_main_budget_exceeded_allows_sustained_emergency_override(self) -> None:
        now_ts = 8_000
        restart_events = [
            {"ts": now_ts - 60 - i, "downtime_sec": 30, "reason": "remote_warning"}
            for i in range(10)
        ]
        self._write_state(
            restart_events=restart_events,
            active_reason_kind="remote_warning",
            active_reason_first_ts=now_ts - 91,
        )

        rc, restart_mock = self._invoke_main(
            now_ts=now_ts,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=(True, "streamStatus=inactive", {}),
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_called_once()

        events = self._read_events()
        self.assertGreaterEqual(len(events), 1)
        self.assertEqual(events[0].get("kind"), "restart_budget_override")
        self.assertEqual(events[0].get("trigger"), "remote_warning")

    def test_main_restarts_when_ffmpeg_missing_persists(self) -> None:
        now_ts = 9_000
        self._write_state(ffmpeg_missing_first_ts=now_ts - 21)

        rc, restart_mock = self._invoke_main(
            now_ts=now_ts,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
            ffmpeg_pid=0,
            ffmpeg_uptime_sec=0,
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_called_once()
        state = self._read_state()
        self.assertEqual(state.get("last_restart_ts"), now_ts)
        self.assertEqual(state.get("ffmpeg_missing_first_ts"), 0)
        self.assertEqual(state.get("ffmpeg_missing_success_backoff_until"), now_ts + 60)
        self.assertEqual(state.get("restart_events", [{}])[-1].get("reason"), "ffmpeg_missing")

    def test_main_ffmpeg_missing_success_backoff_prevents_restart_loop(self) -> None:
        now_ts = 9_100
        self._write_state(
            ffmpeg_missing_first_ts=now_ts - 21,
            ffmpeg_missing_success_backoff_until=now_ts + 30,
            last_restart_ts=now_ts - 40,
        )

        rc, restart_mock = self._invoke_main(
            now_ts=now_ts,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
            ffmpeg_pid=0,
            ffmpeg_uptime_sec=0,
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_not_called()
        self.assertIn("success backoff active", str(self._read_state().get("last_reason")))

    def test_main_ffmpeg_missing_respects_downtime_budget_until_override(self) -> None:
        now_ts = 9_200
        restart_events = [
            {"ts": now_ts - 60 - i, "downtime_sec": 30, "reason": "ffmpeg_missing"}
            for i in range(10)
        ]
        self._write_state(ffmpeg_missing_first_ts=now_ts - 21, restart_events=restart_events)

        rc, restart_mock = self._invoke_main(
            now_ts=now_ts,
            dns_ok_value=True,
            tcp_ok_value=True,
            ping_results=True,
            restart_return=(True, ""),
            youtube_warning=None,
            ffmpeg_pid=0,
            ffmpeg_uptime_sec=0,
        )

        self.assertEqual(rc, 0)
        restart_mock.assert_not_called()
        self.assertIn("hourly downtime budget exceeded", str(self._read_state().get("last_reason")))


if __name__ == "__main__":
    unittest.main()
