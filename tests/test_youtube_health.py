from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_health  # type: ignore


class JudgeIncidentStageTests(unittest.TestCase):
    def test_healthy_is_none(self) -> None:
        decision = youtube_health.judge_incident_stage(
            healthy=True,
            fail_count=0,
            stream_active=True,
            ingest_connected=True,
            availability_signal_ok=True,
            oauth_probe_ok=True,
            oauth_life_cycle_status="live",
            oauth_stream_status_required=True,
            oauth_stream_status="active",
            incident_confirm_fails=2,
        )
        self.assertEqual(decision.stage, "none")

    def test_local_pipeline_unhealthy_is_confirmed(self) -> None:
        decision = youtube_health.judge_incident_stage(
            healthy=False,
            fail_count=1,
            stream_active=False,
            ingest_connected=False,
            availability_signal_ok=False,
            oauth_probe_ok=False,
            oauth_life_cycle_status="",
            oauth_stream_status_required=False,
            oauth_stream_status="",
            incident_confirm_fails=2,
        )
        self.assertEqual(decision.stage, "confirmed")

    def test_remote_unhealthy_confirmed_after_threshold(self) -> None:
        decision = youtube_health.judge_incident_stage(
            healthy=False,
            fail_count=2,
            stream_active=True,
            ingest_connected=True,
            availability_signal_ok=False,
            oauth_probe_ok=True,
            oauth_life_cycle_status="ready",
            oauth_stream_status_required=True,
            oauth_stream_status="inactive",
            incident_confirm_fails=2,
        )
        self.assertEqual(decision.stage, "confirmed")

    def test_remote_mismatch_early_is_suspected(self) -> None:
        decision = youtube_health.judge_incident_stage(
            healthy=False,
            fail_count=1,
            stream_active=True,
            ingest_connected=True,
            availability_signal_ok=True,
            oauth_probe_ok=True,
            oauth_life_cycle_status="ready",
            oauth_stream_status_required=True,
            oauth_stream_status="inactive",
            incident_confirm_fails=2,
        )
        self.assertEqual(decision.stage, "suspected")


class RestartDecisionTests(unittest.TestCase):
    def _decision(self, **kwargs):
        params = {
            "fail_count": 3,
            "max_fails": 3,
            "enforce_restart": True,
            "skip_restart_if_ingest_connected": True,
            "stream_active": True,
            "ingest_connected": True,
            "failure_kind": "unknown",
            "incident_stage": "confirmed",
            "stream_uptime_sec": 360,
            "min_restart_uptime_sec": 120,
            "restart_budget_hourly": 3,
            "restart_budget_daily": 12,
            "restart_history_ts": [],
            "last_restart_ts": 0,
            "restart_cooldown_sec": 900,
            "now_ts": 1_000,
            "stream_service": "stream.service",
        }
        params.update(kwargs)
        return youtube_health.decide_restart_action(**params)

    def test_below_threshold(self) -> None:
        decision = self._decision(fail_count=1)
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.action, "none")

    def test_threshold_with_restart_disabled(self) -> None:
        decision = self._decision(enforce_restart=False)
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.action, "threshold reached; restart disabled")

    def test_suppressed_when_ingest_connected(self) -> None:
        decision = self._decision()
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.action, "restart suppressed: ingest tcp connected")

    def test_suppressed_by_cooldown(self) -> None:
        decision = self._decision(
            skip_restart_if_ingest_connected=False,
            ingest_connected=False,
            last_restart_ts=950,
            restart_cooldown_sec=100,
        )
        self.assertFalse(decision.should_restart)
        self.assertTrue(decision.action.startswith("restart cooldown active"))
        self.assertEqual(decision.cooldown_left, 50)

    def test_restart_when_threshold_reached(self) -> None:
        decision = self._decision(skip_restart_if_ingest_connected=False, ingest_connected=False)
        self.assertTrue(decision.should_restart)
        self.assertEqual(decision.action, "restart stream.service")

    def test_remote_ended_bypasses_ingest_suppression(self) -> None:
        decision = self._decision(failure_kind="remote_ended")
        self.assertTrue(decision.should_restart)

    def test_transient_network_does_not_restart(self) -> None:
        decision = self._decision(
            failure_kind="transient_net",
            skip_restart_if_ingest_connected=False,
            ingest_connected=False,
        )
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.action, "restart deferred: transient network signal")

    def test_requires_confirmed_incident(self) -> None:
        decision = self._decision(
            incident_stage="suspected",
            skip_restart_if_ingest_connected=False,
            ingest_connected=False,
        )
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.action, "restart deferred: incident not confirmed")

    def test_respects_minimum_uptime(self) -> None:
        decision = self._decision(
            stream_uptime_sec=30,
            min_restart_uptime_sec=120,
            skip_restart_if_ingest_connected=False,
            ingest_connected=False,
        )
        self.assertFalse(decision.should_restart)
        self.assertTrue(decision.action.startswith("restart deferred: minimum uptime not met"))

    def test_hourly_budget_blocks_restart(self) -> None:
        decision = self._decision(
            skip_restart_if_ingest_connected=False,
            ingest_connected=False,
            restart_history_ts=[10, 100, 400],
            now_ts=1000,
            restart_budget_hourly=3,
        )
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.action, "restart budget exceeded: hourly (3/3)")

    def test_daily_budget_release_requires_reconfirm_window(self) -> None:
        now_ts = 100_000
        history = [now_ts - 86_410]
        history.extend(now_ts - 80_000 - i for i in range(11))
        decision = self._decision(
            skip_restart_if_ingest_connected=False,
            ingest_connected=False,
            restart_history_ts=history,
            now_ts=now_ts,
            restart_budget_daily=12,
            restart_budget_release_reconfirm_sec=300,
        )
        self.assertFalse(decision.should_restart)
        self.assertEqual(decision.reason, "restart budget slot recently released (daily)")
        self.assertEqual(decision.cooldown_left, 290)

    def test_budget_release_reconfirm_expires_before_restart(self) -> None:
        now_ts = 100_000
        history = [now_ts - 86_401 - 300]
        history.extend(now_ts - 80_000 - i for i in range(11))
        decision = self._decision(
            skip_restart_if_ingest_connected=False,
            ingest_connected=False,
            restart_history_ts=history,
            now_ts=now_ts,
            restart_budget_daily=12,
            restart_budget_release_reconfirm_sec=300,
        )
        self.assertTrue(decision.should_restart)


if __name__ == "__main__":
    unittest.main()
