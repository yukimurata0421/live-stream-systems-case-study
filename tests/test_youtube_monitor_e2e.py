from __future__ import annotations

import contextlib
import sys
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

from decision.action_gate import GateContext, decide_action  # type: ignore
from decision.evaluator import evaluate  # type: ignore
from decision.policy import Policy  # type: ignore
from evidence.identity import TargetIdentity  # type: ignore
from evidence.ledger import LedgerSnapshot  # type: ignore
from evidence.sources import EvidenceRecord, SourceKind  # type: ignore
import youtube_api  # type: ignore


def make_broadcast(
    broadcast_id: str,
    video_id: str,
    lifecycle: str,
    *,
    stream_id: str = "STREAM123",
    enable_auto_start: bool = False,
    enable_auto_stop: bool = False,
) -> dict:
    return {
        "id": broadcast_id,
        "snippet": {
            "resourceId": {"videoId": video_id},
            "scheduledStartTime": "2026-05-03T00:00:00Z",
            "title": "Tokyo ADS-B 24/7",
            "description": "ADSB + NCS test broadcast",
        },
        "status": {"lifeCycleStatus": lifecycle, "privacyStatus": "public"},
        "contentDetails": {
            "boundStreamId": stream_id,
            "enableAutoStart": enable_auto_start,
            "enableAutoStop": enable_auto_stop,
            "enableEmbed": True,
            "latencyPreference": "normal",
        },
    }


class YoutubeMonitorE2ETests(unittest.TestCase):
    def _policy(self) -> Policy:
        return Policy.from_values(
            data_api_ttl_sec=60,
            oauth_ttl_sec=60,
            watch_ttl_sec=60,
            resolver_ttl_sec=150,
            ingest_ttl_sec=15,
            api_cost_ttl_sec=120,
            budget_release_reconfirm_sec=300,
            min_restart_interval_sec=300,
        )

    def _target(self, video_id: str = "VID", broadcast_id: str = "BID") -> TargetIdentity:
        return TargetIdentity(
            channel_id="UC",
            video_id=video_id,
            broadcast_id=broadcast_id,
            bound_stream_id="STREAM123",
            target_epoch=1,
            restart_epoch=1,
        )

    def _ev(
        self,
        source: SourceKind,
        verdict: str,
        target: TargetIdentity,
        *,
        observed_at: int,
        ttl_sec: float = 60.0,
        raw: dict | None = None,
    ) -> EvidenceRecord:
        return EvidenceRecord(
            source=source,
            verdict=verdict,
            target=target,
            observed_at=float(observed_at),
            target_epoch=1,
            restart_epoch=1,
            ttl_sec=ttl_sec,
            raw=raw or {},
        )

    def _snap(self, records: list[EvidenceRecord], *, now: int, canonical: TargetIdentity) -> LedgerSnapshot:
        return LedgerSnapshot(
            latest_by_source={ev.source: ev for ev in records},
            recent_records=tuple(records),
            current_target_epoch=1,
            current_restart_epoch=1,
            last_restart_at=0.0,
            canonical_target=canonical,
            taken_at=float(now),
        )

    def _ctx(self, *, api_cost_degraded: bool = False, fail_count: int = 3) -> GateContext:
        return GateContext(
            fail_count=fail_count,
            max_fails=3,
            enforce_restart=True,
            stream_uptime_sec=600,
            min_restart_uptime_sec=60,
            restart_budget_hourly=3,
            restart_budget_daily=12,
            restart_history_ts=(),
            last_restart_ts=0,
            restart_cooldown_sec=300,
            budget_release_reconfirm_sec=300,
            budget_emergency_override_sec=90,
            active_state_first_ts=0,
            api_cost_degraded=api_cost_degraded,
            stream_service="adsb-streamnew-youtube-stream.service",
        )

    def _oauth_ready_active(self) -> youtube_api.OAuthProbeResult:
        return youtube_api.OAuthProbeResult(
            enabled=True,
            configured=True,
            probe_ok=True,
            healthy=False,
            reason="oauth ready; stream active/good",
            mode="enforced",
            life_cycle_status="ready",
            broadcast_id="OLD",
            video_id="OLDVID",
            channel_id="UC",
            bound_stream_id="STREAM123",
            stream_status="active",
            stream_health_status="good",
            remote_checked=True,
        )

    def test_e2e_replacement_broadcast_recovers_to_available_evidence(self) -> None:
        source = make_broadcast("OLD", "OLDVID", "ready", enable_auto_start=True, enable_auto_stop=True)
        saved_states: list[dict] = []
        post_calls: list[tuple[str, str, str]] = []
        delete_calls: list[tuple[str, dict]] = []

        def fake_post_json(endpoint: str, _token: str, _params: dict[str, str], body: dict) -> dict:
            post_calls.append((endpoint, "create", str(body["contentDetails"].get("enableAutoStart"))))
            self.assertTrue(body["contentDetails"].get("enableAutoStart"))
            self.assertFalse(body["contentDetails"].get("enableAutoStop"))
            return {"id": "NEW"}

        def fake_post(endpoint: str, _token: str, params: dict[str, str]) -> dict:
            if endpoint == "liveBroadcasts/bind":
                post_calls.append((endpoint, params["id"], params["streamId"]))
                return {"id": params["id"]}
            if endpoint == "liveBroadcasts/transition":
                step = params["broadcastStatus"]
                post_calls.append((endpoint, params["id"], step))
                lifecycle = "testing" if step == "testing" else "live"
                return {"status": {"lifeCycleStatus": lifecycle}}
            return {}

        def fake_delete(endpoint: str, _token: str, params: dict[str, str]) -> None:
            delete_calls.append((endpoint, dict(params)))

        def fake_find_owned(_token: str, broadcast_id: str) -> dict | None:
            if broadcast_id == "OLD":
                return source
            if broadcast_id == "NEW":
                return make_broadcast("NEW", "NEWVID", "ready", enable_auto_start=False)
            return None

        patches = [
            mock.patch.object(youtube_api, "FORCE_LIVE_MIN_FAILS", 3),
            mock.patch.object(youtube_api, "FORCE_LIVE_REQUIRE_INGEST", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_MIN_STREAM_UPTIME_SEC", 45),
            mock.patch.object(youtube_api, "FORCE_LIVE_REQUIRE_OAUTH_STREAM_ACTIVE", True),
            mock.patch.object(youtube_api, "OAUTH_ENABLE", True),
            mock.patch.object(youtube_api, "OAUTH_REQUIRE_CHANNEL_MATCH", True),
            mock.patch.object(youtube_api, "CHANNEL_ID", "UC"),
            mock.patch.object(youtube_api, "FORCE_LIVE_TARGET_STATUS", "live"),
            mock.patch.object(youtube_api, "FORCE_LIVE_ON_UPCOMING_ONCE", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_AUTO_RECOVERY", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_CATEGORY_ID", ""),
            mock.patch.object(youtube_api, "FORCE_LIVE_SUCCESS_COOLDOWN_SEC", 1800),
            mock.patch.object(youtube_api, "FORCE_LIVE_MAX_ATTEMPTS_PER_DAY", 24),
            mock.patch.object(youtube_api, "FORCE_LIVE_BROADCAST_ID", ""),
            mock.patch.object(youtube_api, "oauth_is_configured", return_value=True),
            mock.patch.object(youtube_api, "get_oauth_access_token", return_value=("TOKEN", 999999, "token ok")),
            mock.patch.object(youtube_api, "load_force_live_state", return_value={}),
            mock.patch.object(youtube_api, "save_force_live_state", side_effect=lambda state: saved_states.append(dict(state))),
            mock.patch.object(youtube_api, "find_owned_broadcast", side_effect=fake_find_owned),
            mock.patch.object(youtube_api, "youtube_live_api_post_json", side_effect=fake_post_json),
            mock.patch.object(youtube_api, "youtube_live_api_post", side_effect=fake_post),
            mock.patch.object(youtube_api, "youtube_live_api_delete", side_effect=fake_delete),
            mock.patch.object(youtube_api, "wait_for_broadcast_lifecycle", return_value="testing"),
            mock.patch.object(youtube_api, "time", mock.Mock(time=mock.Mock(return_value=1_000), sleep=mock.Mock())),
        ]
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                feature_enabled=True,
                fail_count=3,
                video_id="OLDVID",
                api_reason="data api liveBroadcastContent=upcoming; existing transition invalidTransition",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth_ready_active(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
                url_recovery_elapsed_sec=180,
                replacement_min_elapsed_sec=180,
            )

        self.assertTrue(ok, reason)
        self.assertEqual(
            post_calls,
            [
                ("liveBroadcasts", "create", "True"),
                ("liveBroadcasts/bind", "NEW", "STREAM123"),
                ("liveBroadcasts/transition", "NEW", "testing"),
                ("liveBroadcasts/transition", "NEW", "live"),
            ],
        )
        self.assertEqual(delete_calls, [("liveBroadcasts", {"id": "OLD"})])
        self.assertTrue(saved_states[-1]["ok"])
        self.assertEqual(saved_states[-1]["target_broadcast_id"], "NEW")
        self.assertEqual(saved_states[-1]["cleanup_reason"], "deleted stale source broadcast OLD")

        target = self._target("NEWVID", "NEW")
        snap = self._snap(
            [
                self._ev(SourceKind.RESOLVER, "live", target, observed_at=1_005),
                self._ev(SourceKind.WATCH_PAGE, "live", target, observed_at=1_005, raw={"public": "was_live"}),
                self._ev(SourceKind.OAUTH, "live", target, observed_at=1_005, raw={"stream_status": "active", "health": "good"}),
                self._ev(SourceKind.INGEST_LOCAL, "live", target, observed_at=1_005),
            ],
            now=1_005,
            canonical=target,
        )
        decision = evaluate(snap, self._policy())
        action = decide_action(decision, snap, self._policy(), self._ctx())
        self.assertEqual(decision.state, "available")
        self.assertEqual(action.action, "none")

    def test_e2e_replacement_broadcast_waits_for_180s_cutover(self) -> None:
        source = make_broadcast("OLD", "OLDVID", "ready", enable_auto_start=True, enable_auto_stop=True)
        saved_states: list[dict] = []
        create_mock = mock.Mock()
        transition_mock = mock.Mock()
        patches = [
            mock.patch.object(youtube_api, "FORCE_LIVE_MIN_FAILS", 3),
            mock.patch.object(youtube_api, "FORCE_LIVE_REQUIRE_INGEST", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_MIN_STREAM_UPTIME_SEC", 45),
            mock.patch.object(youtube_api, "FORCE_LIVE_REQUIRE_OAUTH_STREAM_ACTIVE", True),
            mock.patch.object(youtube_api, "OAUTH_ENABLE", True),
            mock.patch.object(youtube_api, "OAUTH_REQUIRE_CHANNEL_MATCH", True),
            mock.patch.object(youtube_api, "CHANNEL_ID", "UC"),
            mock.patch.object(youtube_api, "FORCE_LIVE_TARGET_STATUS", "live"),
            mock.patch.object(youtube_api, "FORCE_LIVE_ON_UPCOMING_ONCE", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_AUTO_RECOVERY", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_MAX_ATTEMPTS_PER_DAY", 24),
            mock.patch.object(youtube_api, "FORCE_LIVE_CATEGORY_ID", ""),
            mock.patch.object(youtube_api, "oauth_is_configured", return_value=True),
            mock.patch.object(youtube_api, "get_oauth_access_token", return_value=("TOKEN", 999999, "token ok")),
            mock.patch.object(youtube_api, "load_force_live_state", return_value={}),
            mock.patch.object(youtube_api, "save_force_live_state", side_effect=lambda state: saved_states.append(dict(state))),
            mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
            mock.patch.object(youtube_api, "create_recovery_broadcast", create_mock),
            mock.patch.object(youtube_api, "youtube_live_api_post", transition_mock),
            mock.patch.object(youtube_api, "time", mock.Mock(time=mock.Mock(return_value=1_000), sleep=mock.Mock())),
        ]
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                feature_enabled=True,
                fail_count=3,
                video_id="OLDVID",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth_ready_active(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
                url_recovery_elapsed_sec=179,
                replacement_min_elapsed_sec=180,
            )

        self.assertFalse(ok)
        self.assertIn("url preservation window active", reason)
        create_mock.assert_not_called()
        transition_mock.assert_not_called()
        self.assertEqual(saved_states[-1]["operation"], "create_replacement_broadcast")
        self.assertFalse(saved_states[-1]["replacement_allowed"])
        self.assertEqual(saved_states[-1]["url_recovery_elapsed_sec"], 179)

    def test_e2e_public_live_overrides_stale_remote_ended_without_restart(self) -> None:
        now = 10_000
        current = self._target("LIVEVID", "LIVEBID")
        stale_old = self._target("OLDVID", "OLDBID")
        snap = self._snap(
            [
                self._ev(SourceKind.RESOLVER, "live", current, observed_at=now),
                self._ev(SourceKind.WATCH_PAGE, "live", current, observed_at=now, raw={"public": "fresh_live"}),
                self._ev(SourceKind.INGEST_LOCAL, "live", current, observed_at=now),
                self._ev(SourceKind.DATA_API, "ended", stale_old, observed_at=now - 600, ttl_sec=60),
                self._ev(SourceKind.OAUTH, "ended", stale_old, observed_at=now - 600, ttl_sec=60),
            ],
            now=now,
            canonical=current,
        )
        decision = evaluate(snap, self._policy())
        action = decide_action(decision, snap, self._policy(), self._ctx())
        self.assertEqual(decision.state, "available")
        self.assertEqual(action.action, "none")

        fresh_conflict = self._snap(
            [
                self._ev(SourceKind.RESOLVER, "live", current, observed_at=now),
                self._ev(SourceKind.WATCH_PAGE, "live", current, observed_at=now, raw={"public": "fresh_live"}),
                self._ev(SourceKind.INGEST_LOCAL, "live", current, observed_at=now),
                self._ev(SourceKind.DATA_API, "ended", stale_old, observed_at=now, ttl_sec=60),
                self._ev(SourceKind.OAUTH, "ended", stale_old, observed_at=now, ttl_sec=60),
            ],
            now=now,
            canonical=current,
        )
        conflict_decision = evaluate(fresh_conflict, self._policy())
        conflict_action = decide_action(conflict_decision, fresh_conflict, self._policy(), self._ctx())
        self.assertEqual(conflict_decision.state, "inconsistent_remote")
        self.assertEqual(conflict_action.action, "resync_resolver")

    def test_e2e_quota_guard_public_not_live_does_not_restart_without_local_failure(self) -> None:
        now = 20_000
        target = self._target("VID", "BID")
        snap = self._snap(
            [
                self._ev(SourceKind.WATCH_PAGE, "not_live", target, observed_at=now, raw={"quota_guard": True}),
                self._ev(SourceKind.INGEST_LOCAL, "live", target, observed_at=now),
                self._ev(SourceKind.API_COST, "degraded", target, observed_at=now, raw={"quota_guard": True}),
            ],
            now=now,
            canonical=target,
        )
        decision = evaluate(snap, self._policy())
        action = decide_action(decision, snap, self._policy(), self._ctx(api_cost_degraded=True))
        self.assertEqual(decision.state, "public_degraded")
        self.assertEqual(action.action, "alert")

        local_failure = self._snap(
            [
                self._ev(SourceKind.WATCH_PAGE, "not_live", target, observed_at=now, raw={"quota_guard": True}),
                self._ev(SourceKind.INGEST_LOCAL, "inactive", target, observed_at=now),
                self._ev(SourceKind.API_COST, "degraded", target, observed_at=now, raw={"quota_guard": True}),
            ],
            now=now,
            canonical=target,
        )
        local_decision = evaluate(local_failure, self._policy())
        local_action = decide_action(local_decision, local_failure, self._policy(), self._ctx(api_cost_degraded=True))
        self.assertEqual(local_decision.state, "local_unhealthy")
        self.assertEqual(local_action.action, "restart_stream")


if __name__ == "__main__":
    unittest.main()
