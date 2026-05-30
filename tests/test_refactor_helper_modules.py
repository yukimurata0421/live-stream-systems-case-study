from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "watchers"))

from fast_recovery_core import remote_warning  # type: ignore
from stream_watchdog_core import audio_transition, overlay_health, pulse_metrics, pulse_routes  # type: ignore
from video_resolver import probe_flow  # type: ignore
from youtube_api_lib import broadcasts  # type: ignore


class YoutubeApiBroadcastHelperTests(unittest.TestCase):
    def test_safe_category_snippet_preserves_only_update_safe_fields(self) -> None:
        snippet = broadcasts.build_safe_video_snippet_for_category(
            {
                "title": "ADS-B live",
                "description": "desc",
                "tags": ["adsb", "live"],
                "defaultLanguage": "ja",
                "localized": {"title": "ignored"},
            },
            "10",
        )

        self.assertEqual(snippet["title"], "ADS-B live")
        self.assertEqual(snippet["description"], "desc")
        self.assertEqual(snippet["tags"], ["adsb", "live"])
        self.assertEqual(snippet["defaultLanguage"], "ja")
        self.assertEqual(snippet["categoryId"], "10")
        self.assertNotIn("localized", snippet)

    def test_recovery_broadcast_body_keeps_contract_flags_explicit(self) -> None:
        body = broadcasts.recovery_broadcast_body(
            {
                "snippet": {"title": "Source", "description": "Existing"},
                "status": {"privacyStatus": "unlisted", "selfDeclaredMadeForKids": False},
                "contentDetails": {"enableEmbed": False, "latencyPreference": "low"},
            },
            enable_auto_start=True,
            enable_auto_stop=False,
            now_utc=datetime(2026, 5, 20, 1, 2, 3, tzinfo=timezone.utc),
        )

        self.assertEqual(body["snippet"]["scheduledStartTime"], "2026-05-20T01:03:03Z")
        self.assertEqual(body["status"]["privacyStatus"], "unlisted")
        self.assertTrue(body["contentDetails"]["enableAutoStart"])
        self.assertFalse(body["contentDetails"]["enableAutoStop"])
        self.assertFalse(body["contentDetails"]["enableEmbed"])
        self.assertEqual(body["contentDetails"]["latencyPreference"], "low")


class StreamWatchdogPulseMetricsTests(unittest.TestCase):
    def test_parse_pactl_entries_extracts_properties_and_latencies(self) -> None:
        entries = pulse_metrics.parse_pactl_entries(
            """
Sink Input #42
    Buffer Latency: 1200 usec
    Sink Latency: 3400 usec
    Properties:
        media.name = "adsb-streamnew-auto-dj"
        application.process.id = "123"
""",
            "Sink Input #",
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["id"], 42)
        self.assertEqual(entries[0]["buffer_latency_usec"], 1200)
        self.assertEqual(entries[0]["peer_latency_usec"], 3400)
        self.assertEqual(entries[0]["properties"]["media.name"], "adsb-streamnew-auto-dj")


class StreamWatchdogPulseRouteHelperTests(unittest.TestCase):
    def test_update_health_state_tracks_missing_and_latency_counts(self) -> None:
        state = pulse_routes.update_health_state(
            {"dj_missing_count": 1, "capture_missing_count": 0, "dj_latency_high_count": 0, "capture_latency_high_count": 1},
            {
                "dj_sink_input_present": False,
                "capture_source_output_present": True,
                "dj_buffer_latency_usec": 0,
                "capture_buffer_latency_usec": 200_000,
            },
            dj_latency_crit_usec=350_000,
            capture_latency_crit_usec=120_000,
        )

        self.assertEqual(state["dj_missing_count"], 2)
        self.assertEqual(state["capture_missing_count"], 0)
        self.assertEqual(state["dj_latency_high_count"], 0)
        self.assertEqual(state["capture_latency_high_count"], 2)

    def test_anomaly_decision_prefers_dj_route_before_latency(self) -> None:
        decision = pulse_routes.anomaly_decision(
            {
                "dj_missing_count": 2,
                "capture_missing_count": 0,
                "dj_latency_high_count": 0,
                "capture_latency_high_count": 2,
            },
            {"capture_buffer_latency_usec": 200_000},
            threshold=2,
            dj_latency_crit_usec=350_000,
            capture_latency_crit_usec=120_000,
        )

        self.assertIsNotNone(decision)
        assert decision is not None
        self.assertEqual(decision["case"], "dj_sink_input_missing")
        self.assertEqual(decision["component"], "dj")


class StreamWatchdogOverlayHealthHelperTests(unittest.TestCase):
    def test_adsb_freshness_judgment_reports_counter_reset_state_and_event(self) -> None:
        ok, reason, next_state, event = overlay_health.adsb_freshness_judgment(
            {"now": 1130, "messages": 12},
            now_ts=1130,
            state={"last_messages": 1000, "last_change_ts": 1000},
            max_age_sec=30,
            message_stall_sec=120,
        )

        self.assertTrue(ok)
        self.assertIn("counter reset", reason)
        self.assertEqual(next_state, {"last_messages": 12, "last_change_ts": 1130, "counter_reset": True})
        self.assertEqual(event, {"previous_messages": 1000, "current_messages": 12})

    def test_outline_json_validation_rejects_invalid_coordinates(self) -> None:
        ok, reason = overlay_health.check_overlay_outline_json(
            {"actualRange": {"last24h": {"points": [["bad", 140.0, 30000]]}}}
        )

        self.assertFalse(ok)
        self.assertIn("invalid point coordinates", reason)


class StreamWatchdogAudioTransitionHelperTests(unittest.TestCase):
    def test_now_playing_heartbeat_skips_transition_age_but_keeps_bucket_boundary(self) -> None:
        current_ts = int(datetime(2026, 5, 8, 7, 0, 18, tzinfo=timezone.utc).timestamp())
        detail = audio_transition.now_playing_transition_detail(
            {
                "updated_at_utc": "2026-05-08T06:58:00Z",
                "note": "Heartbeat update while track is playing.",
                "now_playing": {"title": "test", "bucket": "day", "prefix": "minor"},
            },
            current_ts=current_ts,
            transition_grace_sec=30,
            boundary_grace_sec=90,
        )

        self.assertEqual(detail["bucket_boundary_nearest"], "evening")
        self.assertEqual(detail["bucket_boundary_delta_sec"], 18)
        self.assertTrue(detail["bucket_boundary_within_grace"])
        self.assertTrue(detail["now_playing_heartbeat"])
        self.assertIsNone(detail["track_transition_age_sec"])


class FastRecoveryRemoteWarningHelperTests(unittest.TestCase):
    @staticmethod
    def _parse(raw: str) -> int:
        if raw == "2026-05-20T01:02:03Z":
            return 1_779_241_323
        if raw == "2026-05-20T01:02:04Z":
            return 1_779_241_324
        return 0

    def test_remote_warning_sample_key_prefers_explicit_sample_id(self) -> None:
        self.assertEqual(
            remote_warning.remote_warning_sample_key(
                {"remote_sample_id": "sample-a", "remote_probe_ts_utc": "2026-05-20T01:02:03Z"},
                self._parse,
            ),
            "id:sample-a",
        )

    def test_remote_warning_sample_key_falls_back_to_probe_context(self) -> None:
        key = remote_warning.remote_warning_sample_key(
            {
                "remote_probe_ts_utc": "2026-05-20T01:02:04Z",
                "remote_source": "data_api_oauth",
                "recovery_episode_id": "episode-1",
                "ffmpeg_generation": "stream_pid=1:ffmpeg_pid=2",
            },
            self._parse,
        )

        self.assertEqual(key, "probe:1779241324:data_api_oauth:episode-1:stream_pid=1:ffmpeg_pid=2")


class VideoResolverProbeFlowHelperTests(unittest.TestCase):
    def test_oauth_probe_defers_in_fast_mode_until_ingest_or_remote_ended(self) -> None:
        class OAuthResult:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        calls: list[str] = []
        oauth = probe_flow.resolve_oauth_probe(
            quota_guard_active=False,
            quota_guard_reason="inactive",
            fast_mode=True,
            fast_remote_probe=False,
            ingest_ready=False,
            remote_ended_confirmed=False,
            api_cost_guard=SimpleNamespace(active=False, reason=""),
            stats={},
            now_ts=100,
            reuse_oauth_sec=60,
            oauth_result_cls=OAuthResult,
            oauth_cache_func=lambda *_args: calls.append("cache"),
            probe_func=lambda: calls.append("probe"),
        )

        self.assertEqual(calls, [])
        self.assertEqual(oauth.reason, "oauth probe deferred: fast mode ingest not ready")
        self.assertEqual(oauth.mode, "shadow")

    def test_data_api_search_records_attempt_and_fast_episode_call(self) -> None:
        state: dict = {}
        result = probe_flow.run_data_api_search(
            state,
            should_search=True,
            search_gate_reason="video id unresolved",
            live_page_video_id="",
            live_page_strong=False,
            quota_guard_active=False,
            quota_guard_reason="inactive",
            fast_mode=True,
            now_ts=200,
            episode_window_start_ts=180,
            episode_calls=2,
            fast_window_sec=180,
            fast_max_calls=10,
            search_min_interval_sec=5,
            effective_channel_id="UC123",
            api_key="APIKEY",
            fast_timeout_sec=2.0,
            resolve_func=lambda *_args, **_kwargs: ("VID_API", "live search resolved"),
        )

        self.assertEqual(result.video_id, "VID_API")
        self.assertEqual(result.reason, "live search resolved")
        self.assertEqual(result.episode_calls, 3)
        self.assertEqual(state["last_data_api_search_ts"], 200)
        self.assertEqual(state["fast_search_episode_calls"], 3)

    def test_selected_video_api_check_reuses_matching_cache(self) -> None:
        result = probe_flow.check_selected_video_api(
            selected_video_id="VID123",
            api_key="APIKEY",
            quota_guard_active=False,
            quota_guard_reason="inactive",
            api_cost_guard=SimpleNamespace(active=False, reason=""),
            stats={"video_id": "VID123"},
            now_ts=100,
            max_cache_age_sec=60,
            fast_mode=False,
            fast_timeout_sec=2.0,
            data_api_cache_func=lambda *_args, **_kwargs: (True, "data api stats cache", "live"),
            check_func=lambda *_args, **_kwargs: self.fail("unexpected live API call"),
            utc_now_func=lambda: "2026-05-20T00:00:00Z",
        )

        self.assertEqual(result.reason, "data api stats cache; reused watchdog stats cache")
        self.assertEqual(result.live_state, "live")
        self.assertTrue(result.checked)
        self.assertTrue(result.ok)


if __name__ == "__main__":
    unittest.main()
