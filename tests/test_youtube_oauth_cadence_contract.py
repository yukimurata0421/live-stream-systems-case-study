from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock

from stream_v3.control_loop import monitor_tasks
from watchers.youtube_monitor.stats_writer import record_status
from watchers.youtube_oauth.config import OAuthConfig
from watchers.youtube_oauth.token import get_oauth_access_token

ROOT = Path(__file__).resolve().parents[1]


class OAuthCadenceContractTests(unittest.TestCase):
    def test_healthy_log_throttle_never_suppresses_current_stats(self) -> None:
        stats, event = Mock(), Mock()
        for _ in range(5):
            record_status(
                {"status": "ok", "healthy": True},
                enrich_status_with_recovery_context=lambda p: p,
                classify_judgment=lambda *_: ("ok", "availability_healthy"),
                write_stats=stats,
                append_event=event,
                should_emit_ok_event=lambda: False,
            )
        self.assertEqual(stats.call_count, 5)
        event.assert_not_called()

    def test_repeated_api_polls_reuse_unexpired_access_token(self) -> None:
        config = OAuthConfig(
            True,
            "example-client",
            "",
            "example-refresh",
            "https://example.invalid/token",
            8,
            90,
        )
        refresh, save = Mock(), Mock()
        state = {"access_token": "example-cached", "expires_at": 4600}
        for now in (1000, 1060, 1120, 1180, 1240):
            token, _, reason = get_oauth_access_token(
                config=config,
                now_ts=now,
                load_state=lambda: state,
                save_state=save,
                post_form_json=refresh,
                utc_now=lambda: "unused",
            )
            self.assertEqual(token, "example-cached")
            self.assertEqual(reason, "oauth access token cached")
        refresh.assert_not_called()
        save.assert_not_called()

    def test_candidate_profile_decouples_poll_cache_and_event_log(self) -> None:
        path = ROOT / "ops/systemd/arena-server/youtube-oauth-one-minute.conf.example"
        settings = dict(
            line.removeprefix("Environment=").split("=", 1)
            for line in path.read_text().splitlines()
            if line.startswith("Environment=")
        )
        task = next(t for t in monitor_tasks(settings) if t.name == "youtube_monitor")
        self.assertEqual(task.interval_sec, 60)
        self.assertEqual(settings["YTW_OAUTH_PROBE_MIN_INTERVAL_SEC"], "0")
        self.assertEqual(settings["YTW_OK_LOG_EVERY_SEC"], "300")
        self.assertEqual(settings["YTW_API_COST_BURN_RATE_ENABLE"], "1")
        self.assertEqual(settings["YTW_API_COST_BURN_RATE_FAIL_CLOSED"], "1")
        self.assertFalse(
            any(
                "TOKEN" in key or "RESTART" in key or "FORCE_LIVE" in key
                for key in settings
            )
        )


if __name__ == "__main__":
    unittest.main()
