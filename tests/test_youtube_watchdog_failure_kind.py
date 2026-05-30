from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog  # type: ignore
import youtube_health  # type: ignore


class YouTubeWatchdogFailureKindTests(unittest.TestCase):
    def test_rate_limited_is_transient_net(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        kind = mod.detect_failure_kind(
            stream_active=True,
            ingest_connected=True,
            selected_video_id="VID123",
            api_live_state="rate_limited",
            api_reason="data api http 403: rateLimitExceeded",
            watch_reason="watch page live marker inconclusive (treated as unknown)",
            oauth_reason="oauth disabled",
            oauth_life_cycle_status="",
            oauth_video_id="",
        )
        self.assertEqual(kind, "transient_net")

    def test_public_live_probe_live_is_watch_live_evidence(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        reason = (
            "watch page live marker inconclusive (treated as unknown); "
            "public live probe verdict=live video_id=VID123 live_status=is_live "
            "is_live=True was_live=False availability=public"
        )
        self.assertEqual(mod.verdict_from_watch_reason(reason), "live")

    def test_remote_ended_remains_remote_ended(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        kind = mod.detect_failure_kind(
            stream_active=True,
            ingest_connected=True,
            selected_video_id="VID123",
            api_live_state="ended",
            api_reason="data api stream ended",
            watch_reason="watch page live marker inconclusive (treated as unknown)",
            oauth_reason="oauth complete",
            oauth_life_cycle_status="complete",
            oauth_video_id="VID123",
        )
        self.assertEqual(kind, "remote_ended")

    def test_remote_ended_requires_multiple_correlated_sources(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        base = {
            "stream_active": True,
            "ingest_connected": True,
            "selected_video_id": "VID_SELECTED",
            "api_live_state": "ended",
            "api_reason": "data api stream ended",
            "watch_reason": "watch page live marker inconclusive (treated as unknown)",
            "oauth_reason": "oauth ready",
            "oauth_life_cycle_status": "ready",
            "oauth_video_id": "VID_SELECTED",
        }
        self.assertEqual(mod.detect_failure_kind(**base), "unknown")

        complete_but_mismatch = dict(base, oauth_life_cycle_status="complete", oauth_video_id="VID_OTHER")
        self.assertEqual(mod.detect_failure_kind(**complete_but_mismatch), "unknown")

        watch_says_live = dict(
            base,
            oauth_life_cycle_status="complete",
            oauth_video_id="VID_SELECTED",
            watch_reason='watch page live marker detected ("isLiveNow":true)',
        )
        self.assertEqual(mod.detect_failure_kind(**watch_says_live), "unknown")

    def test_rate_limit_error_is_degraded_not_restart_signal(self) -> None:
        wd = importlib.reload(youtube_watchdog)
        health = importlib.reload(youtube_health)
        failure_kind = wd.detect_failure_kind(
            stream_active=True,
            ingest_connected=True,
            selected_video_id="VID123",
            api_live_state="rate_limited",
            api_reason="data api http 403: rateLimitExceeded",
            watch_reason="watch page live marker inconclusive (treated as unknown)",
            oauth_reason="oauth disabled",
            oauth_life_cycle_status="",
            oauth_video_id="",
        )
        self.assertEqual(failure_kind, "transient_net")

        decision = health.decide_restart_action(
            fail_count=3,
            max_fails=3,
            enforce_restart=True,
            skip_restart_if_ingest_connected=False,
            stream_active=True,
            ingest_connected=False,
            failure_kind=failure_kind,
            incident_stage="confirmed",
            stream_uptime_sec=300,
            min_restart_uptime_sec=60,
            restart_budget_hourly=3,
            restart_budget_daily=12,
            restart_history_ts=[],
            last_restart_ts=0,
            restart_cooldown_sec=900,
            now_ts=1_000,
            stream_service="stream.service",
        )
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.action, "restart deferred: transient network signal")

    def test_active_evidence_timer_resets_when_target_key_changes(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        prev = {
            "active_evidence_state": "remote_ended_confirmed",
            "active_evidence_key": "remote_ended_confirmed|channel=UC|video=OLD|broadcast=|sources=data_api,oauth",
            "active_evidence_first_ts": 1000,
        }
        decision = SimpleNamespace(
            state="remote_ended_confirmed",
            target=SimpleNamespace(channel_id="UC", video_id="NEW", broadcast_id=""),
            contributing_sources=("data_api", "oauth"),
        )
        key = mod.active_evidence_key(decision)
        self.assertEqual(mod.active_evidence_first_ts(prev, decision.state, 1200, key), 1200)

    def test_active_evidence_timer_reuses_same_target_key(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        decision = SimpleNamespace(
            state="remote_ended_confirmed",
            target=SimpleNamespace(channel_id="UC", video_id="VID", broadcast_id=""),
            contributing_sources=("oauth", "data_api"),
        )
        key = mod.active_evidence_key(decision)
        prev = {
            "active_evidence_state": "remote_ended_confirmed",
            "active_evidence_key": key,
            "active_evidence_first_ts": 1000,
        }
        self.assertEqual(mod.active_evidence_first_ts(prev, decision.state, 1200, key), 1000)

    def test_detect_transient_subkind_rate_limited(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        subkind = mod.detect_transient_subkind(
            api_live_state="rate_limited",
            api_reason="data api http 403: rateLimitExceeded",
            watch_reason="watch page live marker inconclusive (treated as unknown)",
            oauth_reason="oauth disabled",
        )
        self.assertEqual(subkind, "rate_limited")

    def test_detect_transient_subkind_network_timeout(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        subkind = mod.detect_transient_subkind(
            api_live_state="error",
            api_reason="data api fetch failed: timed out",
            watch_reason="watch page fetch failed: timed out",
            oauth_reason="oauth disabled",
        )
        self.assertEqual(subkind, "network_timeout")

    def test_detect_transient_subkind_api_5xx(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        subkind = mod.detect_transient_subkind(
            api_live_state="error",
            api_reason="data api http 503: backendError",
            watch_reason="watch page live marker inconclusive (treated as unknown)",
            oauth_reason="oauth disabled",
        )
        self.assertEqual(subkind, "api_5xx")


if __name__ == "__main__":
    unittest.main()
