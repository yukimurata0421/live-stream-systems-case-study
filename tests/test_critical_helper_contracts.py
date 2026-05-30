from __future__ import annotations

import json
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "stream_core"))
sys.path.insert(0, str(ROOT / "src" / "watchers"))

from stream_core.common import ffmpeg_restarts  # type: ignore
from stream_core.cli_support import objective_sli as objective_sli_cli  # type: ignore
from stream_core.engine import ingest as engine_ingest  # type: ignore
from stream_core.engine import runtime_state as engine_runtime_state  # type: ignore
from stream_core.notifications import outbox as notify_outbox  # type: ignore
from stream_core.notifications import status_loop  # type: ignore
from stream_core.ops_health import judgments as health_judgments  # type: ignore
from fast_recovery_core import decision as recovery_decision  # type: ignore
from video_resolver import session as resolver_session  # type: ignore
from youtube_api_lib import data_api, public_probe  # type: ignore
from youtube_api_lib import live_api  # type: ignore


class FfmpegRestartAggregationContractTests(unittest.TestCase):
    def _attempt(self, ts: int, **payload) -> tuple[int, dict]:
        body = {"event_type": "ffmpeg_restart_scheduled", "ts": ts}
        body.update(payload)
        return ts, body

    def test_episode_and_incident_gap_boundaries_are_inclusive(self) -> None:
        summary = ffmpeg_restarts.summarize_ffmpeg_restart_attempts(
            [
                self._attempt(100, exit_code=251, reason="Cannot open connection tls://a.rtmps.youtube.com:443"),
                self._attempt(160, exit_code=251, reason="Cannot open connection tls://a.rtmps.youtube.com:443"),
                self._attempt(221, exit_code=146, reason="Cannot open connection tls://a.rtmps.youtube.com:443"),
                self._attempt(821, exit_code=146, reason="Cannot open connection tls://a.rtmps.youtube.com:443"),
                self._attempt(1422, exit_code=224, reason="Broken pipe"),
            ],
            episode_gap_sec=60,
            incident_gap_sec=600,
        )

        self.assertEqual(summary["attempt_count"], 5)
        self.assertEqual(summary["retry_episode_count"], 4)
        self.assertEqual(summary["incident_cluster_count"], 2)
        self.assertEqual(summary["max_episode_duration_sec"], 60)
        self.assertEqual(summary["max_attempts_per_episode"], 2)
        self.assertEqual(summary["incident_root_causes"]["rtmps_tls_connect_cluster"], 1)
        self.assertEqual(summary["incident_root_causes"]["rtmp_broken_pipe_self_recovery"], 1)

    def test_low_upload_root_cause_takes_precedence_over_tls_exit_code(self) -> None:
        cause = ffmpeg_restarts.classify_ffmpeg_restart_root_cause(
            [
                {
                    "event_type": "ffmpeg_restart_scheduled",
                    "exit_code": 146,
                    "trigger": "low_upload_pressure",
                    "reason": "Cannot open connection tls://a.rtmps.youtube.com:443",
                }
            ]
        )

        self.assertEqual(cause, "low_upload_pressure_cluster")


class FastRecoveryDecisionContractTests(unittest.TestCase):
    def _tcp(self, *, low_upload_pressure_now: bool = False) -> recovery_decision.TcpObservation:
        return recovery_decision.TcpObservation(
            metrics={"bytes_sent": 1000, "notsent": 2000, "unacked": 3, "lastsnd_ms": 12000},
            bytes_sent=1000,
            prev_bytes_sent=1000,
            prev_bytes_ts=10,
            bytes_delta=0,
            bytes_elapsed_sec=5,
            send_mbps=0.0,
            notsent=2000,
            unacked=3,
            lastsnd_ms=12000,
            stall_now=True,
            low_upload_pressure_now=low_upload_pressure_now,
        )

    def test_select_restart_reason_prefers_remote_warning_in_url_preservation_mode(self) -> None:
        state = {"net_fail_streak": 3, "stall_streak": 3, "low_upload_pressure_streak": 3}
        network = recovery_decision.NetworkObservation(
            gateway="192.0.2.1",
            gateway_ok=False,
            public_ok_count=0,
            dns_ok=False,
            tcp_probe_ok=False,
            network_down=True,
        )

        kind, reason = recovery_decision.select_restart_reason(
            state,
            url_preservation_mode=True,
            remote_warning_streak=2,
            remote_warning_confirm=2,
            remote_warning_reason="streamStatus=inactive healthStatus=noData",
            network=network,
            net_fail_confirm=2,
            stall_confirm=2,
            low_upload_confirm=2,
            low_upload_max_mbps=1.0,
            tcp=self._tcp(low_upload_pressure_now=True),
        )

        self.assertEqual(kind, "remote_warning")
        self.assertIn("youtube pre-loss warning", reason)

    def test_select_restart_reason_prefers_network_then_tcp_before_low_upload(self) -> None:
        network = recovery_decision.NetworkObservation(
            gateway="192.0.2.1",
            gateway_ok=False,
            public_ok_count=0,
            dns_ok=False,
            tcp_probe_ok=False,
            network_down=True,
        )

        kind, reason = recovery_decision.select_restart_reason(
            {"net_fail_streak": 2, "stall_streak": 2, "low_upload_pressure_streak": 2},
            url_preservation_mode=False,
            remote_warning_streak=0,
            remote_warning_confirm=2,
            remote_warning_reason="",
            network=network,
            net_fail_confirm=2,
            stall_confirm=2,
            low_upload_confirm=2,
            low_upload_max_mbps=1.0,
            tcp=self._tcp(low_upload_pressure_now=True),
        )
        self.assertEqual(kind, "network_down")
        self.assertIn("dns_ok=False", reason)

        kind, reason = recovery_decision.select_restart_reason(
            {"net_fail_streak": 0, "stall_streak": 2, "low_upload_pressure_streak": 2},
            url_preservation_mode=False,
            remote_warning_streak=0,
            remote_warning_confirm=2,
            remote_warning_reason="",
            network=network,
            net_fail_confirm=2,
            stall_confirm=2,
            low_upload_confirm=2,
            low_upload_max_mbps=1.0,
            tcp=self._tcp(low_upload_pressure_now=True),
        )
        self.assertEqual(kind, "tcp_stall")
        self.assertIn("bytes_delta=0", reason)

    def test_pid_change_resets_tcp_streaks_but_not_network_history(self) -> None:
        state = {
            "last_pid": 111,
            "stall_streak": 2,
            "low_upload_pressure_streak": 2,
            "net_fail_streak": 2,
            "last_bytes_sent": 1234,
            "last_bytes_sent_ts": 55,
        }

        recovery_decision.reset_pid_dependent_state(state, 222)

        self.assertEqual(state["stall_streak"], 0)
        self.assertEqual(state["low_upload_pressure_streak"], 0)
        self.assertEqual(state["last_bytes_sent"], 0)
        self.assertEqual(state["last_bytes_sent_ts"], 0)
        self.assertEqual(state["net_fail_streak"], 2)


class VideoResolverSessionContractTests(unittest.TestCase):
    def test_configured_cached_video_is_not_runtime_cache(self) -> None:
        context = resolver_session.cached_video_context(
            {"video_id": "CONFIGURED", "source": "configured", "resolved_ts": 990},
            now_ts=1000,
            max_age_sec=60,
        )

        self.assertEqual(context.cached_fresh_video_id, "CONFIGURED")
        self.assertEqual(context.cached_runtime_video_id, "")

    def test_remote_ended_requires_confirm_window_and_resets_on_good_sample(self) -> None:
        state: dict = {}

        first = resolver_session.update_remote_ended_state(
            state,
            now_ts=100,
            raw=True,
            raw_reason="api live state ended",
            confirm_sec=30,
        )
        self.assertFalse(first.confirmed)
        self.assertEqual(first.since_ts, 100)
        self.assertIn("waiting confirm", first.reason)

        confirmed = resolver_session.update_remote_ended_state(
            state,
            now_ts=131,
            raw=True,
            raw_reason="api live state ended",
            confirm_sec=30,
        )
        self.assertTrue(confirmed.confirmed)
        self.assertEqual(confirmed.elapsed_sec, 31)

        cleared = resolver_session.update_remote_ended_state(
            state,
            now_ts=132,
            raw=False,
            raw_reason="remote live",
            confirm_sec=30,
        )
        self.assertFalse(cleared.raw)
        self.assertEqual(cleared.since_ts, 0)
        self.assertEqual(state["remote_ended_since_ts"], 0)

    def test_fast_search_window_waits_for_ingest_and_survives_same_episode_flap(self) -> None:
        state = {"fast_search_window_start_ts": 500, "fast_search_episode_calls": 4}

        continuing = resolver_session.update_fast_search_episode(
            state,
            now_ts=540,
            fast_mode=True,
            prev_fast_mode=True,
            ingest_ready_for_search=False,
        )
        self.assertEqual(continuing.window_start_ts, 500)
        self.assertEqual(continuing.episode_calls, 4)
        self.assertEqual(continuing.recovery_episode_id, "fast-500")

        state = {}
        waiting = resolver_session.update_fast_search_episode(
            state,
            now_ts=600,
            fast_mode=True,
            prev_fast_mode=False,
            ingest_ready_for_search=False,
        )
        self.assertEqual(waiting.window_start_ts, 0)
        self.assertEqual(waiting.episode_calls, 0)

        ready = resolver_session.update_fast_search_episode(
            state,
            now_ts=620,
            fast_mode=True,
            prev_fast_mode=True,
            ingest_ready_for_search=True,
        )
        self.assertEqual(ready.window_start_ts, 620)
        self.assertEqual(ready.recovery_episode_id, "fast-620")


class YoutubeLiveApiAccountingContractTests(unittest.TestCase):
    def _deps(self, events: list[dict], quota_events: list[dict]):
        def append_api_call_event(**kwargs) -> None:
            events.append(kwargs)

        def mark_quota_exhausted(source, detail, reason_hint=""):
            quota_events.append({"source": source, "detail": detail, "reason_hint": reason_hint})
            return True, "quota latched"

        return {
            "append_api_call_event": append_api_call_event,
            "mark_quota_exhausted": mark_quota_exhausted,
            "http_error_body": lambda _e: json.dumps({"error": {"errors": [{"reason": "quotaExceeded"}]}}),
            "is_quota_exceeded_error": lambda code, _body: int(code) == 403,
            "extract_google_error_reason": lambda _body: "quotaExceeded",
        }

    def test_api_post_quota_http_error_records_latch_context_before_reraising(self) -> None:
        events: list[dict] = []
        quota_events: list[dict] = []
        deps = self._deps(events, quota_events)
        error = urllib.error.HTTPError("https://example.invalid", 403, "Forbidden", {}, None)

        with mock.patch("youtube_api_lib.live_api.urllib.request.urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError):
                live_api.api_post(
                    "liveBroadcasts/transition",
                    "TOKEN",
                    {"part": "status", "id": "BID", "broadcastStatus": "live"},
                    oauth_timeout_sec=3,
                    **deps,
                )

        self.assertEqual(
            quota_events,
            [
                {
                    "source": "oauth_liveBroadcasts.transition",
                    "detail": 'oauth liveBroadcasts.transition http 403: {"error": {"errors": [{"reason": "quotaExceeded"}]}}',
                    "reason_hint": "quotaExceeded",
                }
            ],
        )
        self.assertEqual(events[0]["method"], "liveBroadcasts.transition")
        self.assertEqual(events[0]["status"], "http_error")
        self.assertEqual(events[0]["http_code"], 403)
        self.assertTrue(events[0]["quota_exceeded"])
        self.assertEqual(events[0]["source"], "youtube_live_api_post")
        self.assertIn("quota latched", events[0]["detail"])

    def test_api_post_json_success_records_method_and_sends_json_body(self) -> None:
        events: list[dict] = []
        quota_events: list[dict] = []
        deps = self._deps(events, quota_events)
        captured: dict[str, object] = {}

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return b'{"id":"broadcast-1"}'

        def fake_urlopen(req, timeout):
            captured["method"] = req.get_method()
            captured["url"] = req.full_url
            captured["headers"] = dict(req.header_items())
            captured["data"] = req.data
            captured["timeout"] = timeout
            return Response()

        with mock.patch("youtube_api_lib.live_api.urllib.request.urlopen", side_effect=fake_urlopen):
            result = live_api.api_post_json(
                "liveBroadcasts",
                "TOKEN",
                {"part": "snippet,status,contentDetails"},
                {"snippet": {"title": "ADS-B"}, "status": {"privacyStatus": "unlisted"}},
                oauth_timeout_sec=5,
                **deps,
            )

        self.assertEqual(result, {"id": "broadcast-1"})
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["timeout"], 5)
        self.assertIn("liveBroadcasts?", str(captured["url"]))
        self.assertIn(b'"title": "ADS-B"', captured["data"])
        self.assertEqual(events, [{"method": "liveBroadcasts.insert", "status": "ok", "source": "youtube_live_api_post_json"}])
        self.assertEqual(quota_events, [])


class YoutubeDataApiAndPublicProbeContractTests(unittest.TestCase):
    def test_check_data_api_flags_started_stream_without_live_marker_as_inconsistent(self) -> None:
        events: list[dict] = []
        result = data_api.check_data_api(
            "VID12345678",
            "APIKEY",
            fetch_text=lambda *_args, **_kwargs: json.dumps(
                {
                    "items": [
                        {
                            "snippet": {"liveBroadcastContent": "none"},
                            "liveStreamingDetails": {"actualStartTime": "2026-05-20T00:00:00Z"},
                        }
                    ]
                }
            ),
            append_api_call_event=lambda **kwargs: events.append(kwargs),
            mark_quota_exhausted=lambda *_args, **_kwargs: self.fail("quota latch should not be touched"),
        )

        self.assertTrue(result.checked)
        self.assertFalse(result.api_ok)
        self.assertEqual(result.live_state, "inconsistent_live_details")
        self.assertIn("actualStart exists", result.reason)
        self.assertEqual(events, [{"method": "videos.list", "status": "ok", "source": "check_data_api"}])

    def test_resolve_live_video_id_records_search_and_skips_items_without_video_id(self) -> None:
        events: list[dict] = []
        video_id, reason = data_api.resolve_live_video_id(
            "UC123",
            "APIKEY",
            fetch_text=lambda *_args, **_kwargs: json.dumps(
                {
                    "items": [
                        {"id": {"kind": "youtube#playlist"}},
                        {"id": {"videoId": "VID_LIVE_123"}},
                    ]
                }
            ),
            append_api_call_event=lambda **kwargs: events.append(kwargs),
            mark_quota_exhausted=lambda *_args, **_kwargs: self.fail("quota latch should not be touched"),
        )

        self.assertEqual(video_id, "VID_LIVE_123")
        self.assertEqual(reason, "live search resolved video id")
        self.assertEqual(events, [{"method": "search.list", "status": "ok", "source": "resolve_live_video_id"}])

    def test_public_watch_page_live_marker_beats_short_html_noise(self) -> None:
        ok, reason = public_probe.check_public_watch_page(
            "https://youtube.com/watch?v=VID",
            fetch_text=lambda _url: '{"isLiveNow":true}',
        )

        self.assertTrue(ok)
        self.assertIn("live marker detected", reason)

    def test_public_watch_page_unknown_verdict_is_nonfatal_not_live_ok(self) -> None:
        result = public_probe.check_public_watch_page_verdict(
            "https://youtube.com/watch?v=VID",
            fetch_text=lambda _url: "x" * 4096,
        )

        self.assertTrue(result.checked)
        self.assertEqual(result.verdict, "unknown")
        self.assertFalse(result.fatal)
        self.assertTrue(result.ok_for_availability)

        ok, reason = public_probe.check_public_watch_page(
            "https://youtube.com/watch?v=VID",
            fetch_text=lambda _url: "x" * 4096,
        )
        self.assertTrue(ok)
        self.assertIn("inconclusive", reason)

    def test_public_watch_page_fetch_failure_is_unknown_fatal(self) -> None:
        result = public_probe.check_public_watch_page_verdict(
            "https://youtube.com/watch?v=VID",
            fetch_text=mock.Mock(side_effect=urllib.error.URLError("too many requests")),
        )

        self.assertTrue(result.checked)
        self.assertEqual(result.verdict, "unknown")
        self.assertTrue(result.fatal)
        self.assertFalse(result.ok_for_availability)
        self.assertIn("fetch failed", result.reason)

    def test_public_live_probe_nonzero_not_currently_live_is_not_live_not_unknown(self) -> None:
        cp = SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="ERROR: This live event is not currently live",
        )
        result = public_probe.probe_public_live_status(
            "https://youtube.com/watch?v=VID",
            public_live_probe_timeout_sec=10,
            run_cmd=lambda *_args, **_kwargs: cp,
        )

        self.assertTrue(result.checked)
        self.assertEqual(result.verdict, "not_live")
        self.assertIn("not_live", result.reason)


class NotificationAutoRecoveredContractTests(unittest.TestCase):
    @staticmethod
    def _utc(ts: int) -> str:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_auto_recovered_event_filtering_uses_trigger_ack_cutoff_and_future_guard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            events_file = Path(td) / "fast_recovery_events.jsonl"
            rows = [
                {"ts_utc": self._utc(1300), "kind": "restart", "trigger": "tcp_stall", "message": "too old"},
                {"ts_utc": self._utc(1500), "kind": "restart", "trigger": "low_upload_pressure", "message": "ignored trigger"},
                {"ts_utc": self._utc(1600), "kind": "restart", "trigger": "tcp_stall", "message": "already acked"},
                {"ts_utc": self._utc(1700), "kind": "sample", "trigger": "network_down", "message": "not restart"},
                {"ts_utc": self._utc(1800), "kind": "restart", "trigger": "network_down", "message": "network recovered"},
                {"ts_utc": self._utc(2065), "kind": "restart", "trigger": "tcp_stall", "message": "future ignored"},
            ]
            events_file.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            state = {"fast_recovery_auto_recovered_notified": {f"{self._utc(1600)}|tcp_stall": 1601}}

            events = status_loop.fast_recovery_auto_recovered_events(
                state=state,
                now_ts=2000,
                recent_sec=600,
                triggers=["tcp_stall", "network_down"],
                events_file=events_file,
            )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["trigger"], "network_down")
        self.assertEqual(events[0]["evidence"], "network recovered")
        self.assertEqual(events[0]["recovery_type"], "fast_recovery_restart:network_down")

    def test_mark_auto_recovered_notifications_compacts_old_acknowledgements(self) -> None:
        state = {
            "fast_recovery_auto_recovered_notified": {
                "old|tcp_stall": 100,
                "recent|tcp_stall": 90_000,
            }
        }
        status_loop.mark_fast_recovery_auto_recovered_events_notified(
            state,
            [{"_event_key": "new|network_down"}],
            now_ts=100_000,
        )

        self.assertNotIn("old|tcp_stall", state["fast_recovery_auto_recovered_notified"])
        self.assertEqual(state["fast_recovery_auto_recovered_notified"]["recent|tcp_stall"], 90_000)
        self.assertEqual(state["fast_recovery_auto_recovered_notified"]["new|network_down"], 100_000)


class OpsHealthJudgmentContractTests(unittest.TestCase):
    def test_public_probe_reason_requires_public_context_and_classifies_429(self) -> None:
        self.assertEqual(
            health_judgments.public_probe_degraded_reason(
                {
                    "status": "warn",
                    "health_source": "public_probe",
                    "reason": "HTTP Error 429: Too Many Requests",
                }
            ),
            "public_probe_429",
        )
        self.assertEqual(
            health_judgments.public_probe_degraded_reason(
                {
                    "status": "warn",
                    "health_source": "oauth_live_api",
                    "reason": "HTTP Error 429: Too Many Requests",
                }
            ),
            "",
        )

    def test_public_probe_authoritative_live_ok_requires_oauth_api_and_local_evidence(self) -> None:
        self.assertTrue(
            health_judgments.public_probe_authoritative_live_ok(
                {
                    "oauth_stream_status": "active",
                    "api_live_state": "live",
                    "ingest_connected": True,
                }
            )
        )
        self.assertFalse(
            health_judgments.public_probe_authoritative_live_ok(
                {
                    "oauth_stream_status": "active",
                    "api_live_state": "live",
                    "ingest_connected": False,
                }
            )
        )
        self.assertFalse(
            health_judgments.public_probe_authoritative_live_ok(
                {
                    "oauth_stream_status": "inactive",
                    "api_live_state": "live",
                    "ingest_connected": True,
                }
            )
        )

    def test_public_probe_judgment_precedence_keeps_noise_cluster_visible(self) -> None:
        self.assertEqual(
            health_judgments.public_probe_judgment(2, 2, 2)[0],
            "observe_public_probe_noise_clustered",
        )
        self.assertEqual(
            health_judgments.public_probe_judgment(0, 4, 4)[0],
            "observe_public_probe_noise_frequent",
        )
        self.assertEqual(
            health_judgments.public_probe_judgment(0, 3, 3)[0],
            "observe_public_probe_noise_authoritative_live_ok",
        )

    def test_encoder_gap_active_requires_remote_live_and_local_encoder_gap(self) -> None:
        base = {
            "oauth_enable_auto_stop": False,
            "api_live_state": "live",
            "stream_active": False,
            "ingest_connected": False,
            "local_ok": False,
            "ffmpeg_pid": 0,
        }
        self.assertTrue(health_judgments.encoder_gap_active(base))

        self.assertFalse(health_judgments.encoder_gap_active({**base, "oauth_enable_auto_stop": True}))
        self.assertFalse(
            health_judgments.encoder_gap_active(
                {
                    **base,
                    "stream_active": True,
                    "ingest_connected": True,
                    "local_ok": True,
                    "ffmpeg_pid": 1234,
                }
            )
        )
        self.assertFalse(health_judgments.encoder_gap_active({**base, "api_live_state": "none"}))

    def test_sample_duration_caps_sparse_active_intervals(self) -> None:
        active_count, duration = health_judgments.sample_duration(
            [(100, True), (500, True), (2000, False)],
            now_ts=3000,
            max_step_sec=600,
        )

        self.assertEqual(active_count, 2)
        self.assertEqual(duration, 1000)

    def test_fast_mode_judgment_thresholds_explain_severity(self) -> None:
        self.assertEqual(health_judgments.fast_mode_judgment(0, 1800, 0)[0], "investigate_fast_mode_runaway")
        self.assertEqual(health_judgments.fast_mode_judgment(4, 60, 0)[0], "observe_fast_mode_repeated")
        self.assertEqual(health_judgments.fast_mode_judgment(1, 60, 0)[0], "ok_short_fast_mode_episode")
        self.assertEqual(health_judgments.fast_mode_judgment(0, 0, 0)[0], "ok_none")

    def test_api_report_judgment_timer_health_takes_precedence_over_freshness(self) -> None:
        self.assertEqual(
            health_judgments.api_report_judgment(open_fresh=True, closed_fresh=True, timers_active=False)[0],
            "api_report_timer_attention",
        )
        self.assertEqual(
            health_judgments.api_report_judgment(open_fresh=False, closed_fresh=True, timers_active=True)[0],
            "api_open_day_report_stale",
        )

    def test_ssl_tls_reason_flattens_nested_errors_but_ignores_plain_transport(self) -> None:
        self.assertEqual(
            health_judgments.ssl_tls_reason({"error": [{"message": "OpenSSL unsupported protocol"}]}),
            "ssl_tls_protocol_error",
        )
        self.assertEqual(health_judgments.ssl_tls_reason({"reason": "Broken pipe"}), "")


class ObjectiveSliContractTests(unittest.TestCase):
    @staticmethod
    def _ts(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
        return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp())

    def test_upload_budget_sli_accepts_send_mbps_and_mbps_aliases(self) -> None:
        result = objective_sli_cli.upload_budget_sli(
            [
                (100, {"kind": "tcp_send_sample", "send_mbps": 4.0, "sample_interval_sec": 60}),
                (160, {"kind": "tcp_send_sample", "mbps": 6.0, "sample_interval_sec": 30}),
                (190, {"kind": "restart", "send_mbps": 10.0, "sample_interval_sec": 30}),
            ]
        )

        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["sampled_sec"], 90.0)
        self.assertEqual(result["over_5mbps_sec"], 30.0)
        self.assertEqual(result["within_5mbps_ratio_pct"], 66.667)
        self.assertEqual(result["p50_mbps"], 5.0)

    def test_stream_engine_sli_exposes_attempt_episode_cluster_metadata(self) -> None:
        result = objective_sli_cli.stream_engine_sli(
            [
                (100, {"event_type": "ffmpeg_restart_scheduled", "exit_code": 251, "reason": "Cannot open connection tls://a.rtmps.youtube.com:443"}),
                (160, {"event_type": "ffmpeg_restart_scheduled", "exit_code": 146, "reason": "Cannot open connection tls://a.rtmps.youtube.com:443"}),
                (900, {"event_type": "ffmpeg_restart_scheduled", "exit_code": 224, "reason": "Broken pipe"}),
            ]
        )

        self.assertEqual(result["ffmpeg_restart_attempt_count"], 3)
        self.assertEqual(result["ffmpeg_restart_episode_count"], 2)
        self.assertEqual(result["ffmpeg_restart_incident_cluster_count"], 2)
        self.assertEqual(result["ffmpeg_restart_episodes_root_cause"]["rtmps_tls_connect_cluster"], 1)
        self.assertEqual(result["ffmpeg_restart_incident_root_causes"]["rtmp_broken_pipe_self_recovery"], 1)

    def test_report_only_sli_filters_target_and_closes_degraded_incident_on_ok(self) -> None:
        result = objective_sli_cli.report_only_sli(
            [
                (100, {"target": "overlay_stream1090", "judgment": "report_only_warn"}),
                (160, {"target": "upstream_readsb_tar1090_stream1090", "judgment": "report_only_warn"}),
                (180, {"target": "overlay_stream1090", "judgment": "report_only_warn"}),
                (220, {"target": "overlay_stream1090", "judgment": "report_only_ok"}),
            ],
            expected_target="overlay_stream1090",
        )

        self.assertEqual(result["sample_count"], 3)
        self.assertEqual(result["incident_count"], 1)
        self.assertEqual(result["degraded_sec"], 120)
        self.assertEqual(result["time_availability_pct"], 0.0)

    def test_discord_notify_sli_counts_legacy_send_ok_shape_and_kind_shape(self) -> None:
        result = objective_sli_cli.discord_notify_sli(
            [
                (100, {"phase": "detected", "send_ok": True}),
                (200, {"kind": "send_failed", "send_ok": False}),
                (300, {"kind": "ignored_without_delivery"}),
            ]
        )

        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["send_ok_count"], 1)
        self.assertEqual(result["send_failed_count"], 1)
        self.assertEqual(result["delivery_ratio_pct"], 50.0)
        self.assertEqual(result["by_kind"]["detected"], 1)
        self.assertEqual(result["by_kind"]["send_failed"], 1)

    def test_api_usage_sli_groups_by_pacific_day_not_utc_day(self) -> None:
        result = objective_sli_cli.api_usage_sli(
            [
                (self._ts(2026, 5, 20, 6, 30), {"cost_units": 100, "quota_exceeded": True}),
                (self._ts(2026, 5, 20, 8, 30), {"units": 1, "error_reason": "quotaExceeded"}),
            ]
        )

        self.assertEqual(
            result["by_pt_day"],
            [
                {"pt_day": "2026-05-19", "calls": 1, "units": 100, "quota_exceeded_events": 1},
                {"pt_day": "2026-05-20", "calls": 1, "units": 1, "quota_exceeded_events": 1},
            ],
        )

    def test_memory_guardrail_sli_separates_operational_adequacy_and_current_severity(self) -> None:
        result = objective_sli_cli.memory_guardrail_sli(
            [
                (
                    100,
                    {
                        "classification_policy_version": 5,
                        "overall": {"severity": "ok", "operational_adequacy_severity": "ok"},
                        "operational_adequacy": {"severity": "ok"},
                        "host": {
                            "non_reclaimable_estimate_bytes": 8 * 1024 * 1024 * 1024,
                            "mem_available_bytes": 8 * 1024 * 1024 * 1024,
                            "swap_used_bytes": 0,
                        },
                        "top_non_reclaimable_consumers": [
                            {"unit": "adsb-streamnew-youtube-stream.service", "non_reclaimable_estimate_bytes": 300 * 1024 * 1024}
                        ],
                    },
                ),
                (
                    160,
                    {
                        "classification_policy_version": 5,
                        "overall": {"severity": "ok", "operational_adequacy_severity": "warn"},
                        "operational_adequacy": {"severity": "warn"},
                        "host": {
                            "non_reclaimable_estimate_bytes": 11 * 1024 * 1024 * 1024,
                            "mem_available_bytes": 5 * 1024 * 1024 * 1024,
                            "swap_used_bytes": 600 * 1024 * 1024,
                        },
                        "top_non_reclaimable_consumers": [
                            {"unit": "adsb-streamnew-youtube-stream.service", "non_reclaimable_estimate_bytes": 400 * 1024 * 1024}
                        ],
                    },
                ),
            ]
        )

        self.assertEqual(result["latest_severity"], "ok")
        self.assertEqual(result["latest_operational_adequacy_severity"], "warn")
        self.assertEqual(result["critical_count"], 0)
        self.assertEqual(result["operational_adequacy_warn_count"], 1)
        self.assertEqual(result["operational_adequacy_warn_or_critical_sample_ratio_pct"], 50.0)
        self.assertEqual(result["host_non_reclaimable_estimate_mib_max"], 11264.0)
        self.assertEqual(result["host_mem_available_mib_min"], 5120.0)


class NotificationOutboxContractTests(unittest.TestCase):
    def test_load_notify_outbox_filters_expired_nonpending_and_invalid_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            outbox_file = Path(td) / "outbox.jsonl"
            outbox_file.write_text(
                "\n".join(
                    [
                        json.dumps({"message_id": "old", "status": "pending", "created_ts": 100}),
                        json.dumps({"message_id": "sent", "status": "sent", "created_ts": 990}),
                        "not json",
                        json.dumps({"message_id": "recent", "status": "pending", "created_ts": 950}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            rows = notify_outbox.load_notify_outbox(outbox_file, now_ts=1000, ttl_sec=100)

        self.assertEqual([item["message_id"] for item in rows], ["recent"])

    def test_enqueue_notify_messages_deduplicates_status_and_bounds_pending(self) -> None:
        rows = notify_outbox.enqueue_notify_messages(
            [
                {"message_id": "old|drop", "status": "pending", "content": "old"},
                {"message_id": "status|incident-a", "status": "pending", "content": "stale", "attempts": 2},
            ],
            [
                ("status", [{"id": "incident-a"}], "updated"),
                ("detected", [{"id": "incident-b", "_first_seen_ts": 500}], "new"),
            ],
            username="bot",
            now_ts=1000,
            max_pending=2,
        )

        self.assertEqual([item["content"] for item in rows], ["updated", "new"])
        self.assertEqual(rows[0]["attempts"], 2)
        self.assertEqual(rows[0]["username"], "bot")
        self.assertTrue(rows[1]["message_id"].startswith("detected|incident-b|500"))

    def test_flush_notify_outbox_respects_flush_limit_and_leaves_unattempted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            outbox_file = root / "outbox.jsonl"
            events_file = root / "events.jsonl"
            notify_outbox.save_notify_outbox(
                outbox_file,
                [
                    {"message_id": "m1", "phase": "detected", "content": "one", "username": "bot", "status": "pending", "attempts": 0, "created_ts": 900},
                    {"message_id": "m2", "phase": "status", "content": "two", "username": "bot", "status": "pending", "attempts": 0, "created_ts": 900},
                ],
            )
            cfg = {"enabled": True, "webhook_url": "https://discord.example", "username": "bot", "outbox_ttl_sec": 86400, "outbox_flush_limit": 1}

            sent, failures, pending = notify_outbox.flush_notify_outbox(
                outbox_path=outbox_file,
                events_path=events_file,
                cfg=cfg,
                now_ts=1000,
                send_webhook=lambda *_args, **_kwargs: (True, "ok"),
            )
            remaining = notify_outbox.load_notify_outbox(outbox_file, now_ts=1000, ttl_sec=86400)
            event = json.loads(events_file.read_text(encoding="utf-8").strip())

        self.assertEqual((sent, failures, pending), (1, 0, 1))
        self.assertEqual([item["message_id"] for item in remaining], ["m2"])
        self.assertEqual(event["message_id"], "m1")
        self.assertTrue(event["send_ok"])

    def test_flush_notify_outbox_records_failure_attempt_and_last_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            outbox_file = root / "outbox.jsonl"
            events_file = root / "events.jsonl"
            notify_outbox.save_notify_outbox(
                outbox_file,
                [
                    {"message_id": "m1", "phase": "detected", "content": "one", "username": "bot", "status": "pending", "attempts": 1, "created_ts": 900},
                ],
            )
            cfg = {"enabled": True, "webhook_url": "https://discord.example", "username": "bot", "outbox_ttl_sec": 86400, "outbox_flush_limit": 10}

            sent, failures, pending = notify_outbox.flush_notify_outbox(
                outbox_path=outbox_file,
                events_path=events_file,
                cfg=cfg,
                now_ts=1000,
                send_webhook=lambda *_args, **_kwargs: (False, "timeout"),
            )
            remaining = notify_outbox.load_notify_outbox(outbox_file, now_ts=1000, ttl_sec=86400)
            event = json.loads(events_file.read_text(encoding="utf-8").strip())

        self.assertEqual((sent, failures, pending), (0, 1, 1))
        self.assertEqual(remaining[0]["attempts"], 2)
        self.assertEqual(remaining[0]["last_error"], "timeout")
        self.assertFalse(event["send_ok"])
        self.assertEqual(event["outbox_attempt"], 2)


class IngestRuntimeStateContractTests(unittest.TestCase):
    def test_resolve_rtmp_url_normalizes_placeholder_to_rtmps_443_and_does_not_duplicate_key(self) -> None:
        self.assertEqual(
            engine_ingest.resolve_rtmp_url("rtmps://a.rtmps.youtube.com/live2/YOUR_STREAM_KEY", "KEY123"),
            "rtmps://a.rtmps.youtube.com:443/live2/KEY123",
        )
        self.assertEqual(
            engine_ingest.resolve_rtmp_url("rtmps://a.rtmps.youtube.com:443/live2/KEY123", "KEY123"),
            "rtmps://a.rtmps.youtube.com:443/live2/KEY123",
        )

    def test_validate_rtmp_url_accepts_current_contract_and_rejects_placeholders_or_wrong_hosts(self) -> None:
        engine_ingest.validate_rtmp_url("rtmps://a.rtmps.youtube.com:443/live2/KEY123", "KEY123")

        with self.assertRaisesRegex(RuntimeError, "placeholder"):
            engine_ingest.validate_rtmp_url("rtmps://a.rtmps.youtube.com:443/live2/YOUR_STREAM_KEY", "YOUR_STREAM_KEY")
        with self.assertRaisesRegex(RuntimeError, "invalid"):
            engine_ingest.validate_rtmp_url("rtmps://example.invalid/live2/KEY123", "KEY123")

    def test_mask_rtmp_url_never_exposes_stream_key(self) -> None:
        masked = engine_ingest.mask_rtmp_url("rtmps://a.rtmps.youtube.com:443/live2/SECRETKEY")

        self.assertEqual(masked, "rtmps://a.rtmps.youtube.com:443/live2/***")
        self.assertNotIn("SECRETKEY", masked)

    def test_runtime_state_hash_only_rewrites_default_filename(self) -> None:
        self.assertEqual(
            engine_runtime_state.hashed_runtime_state_file(Path("/tmp/stream_runtime_state.json"), "abc123"),
            Path("/tmp/stream_runtime_state_abc123.json"),
        )
        self.assertEqual(
            engine_runtime_state.hashed_runtime_state_file(Path("/tmp/custom_state.json"), "abc123"),
            Path("/tmp/custom_state.json"),
        )


if __name__ == "__main__":
    unittest.main()
