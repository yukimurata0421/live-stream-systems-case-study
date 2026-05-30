from __future__ import annotations

import contextlib
import io
import sys
import urllib.error
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_api  # type: ignore
import youtube_watchdog  # type: ignore


def make_broadcast(
    broadcast_id: str,
    video_id: str,
    lifecycle: str,
    scheduled_start: str = "2026-05-03T00:00:00Z",
) -> dict:
    return {
        "id": broadcast_id,
        "snippet": {
            "resourceId": {"videoId": video_id},
            "scheduledStartTime": scheduled_start,
            "title": "Test stream",
            "description": "desc",
        },
        "status": {"lifeCycleStatus": lifecycle, "privacyStatus": "public"},
        "contentDetails": {
            "boundStreamId": "STREAM123",
            "enableAutoStart": False,
            "enableEmbed": True,
            "latencyPreference": "normal",
        },
    }


class BroadcastSelectionTests(unittest.TestCase):
    def test_select_primary_prefers_preferred_video(self) -> None:
        items = [
            make_broadcast("b1", "v1", "ready"),
            make_broadcast("b2", "v2", "live"),
        ]
        selected = youtube_watchdog.select_primary_broadcast(items, preferred_video_id="v1")
        assert selected is not None
        self.assertEqual(selected.get("id"), "b1")

    def test_choose_transition_target_prefers_video(self) -> None:
        items = [
            make_broadcast("b1", "v1", "ready"),
            make_broadcast("b2", "v2", "testing"),
        ]
        bid, vid, lifecycle, _reason = youtube_watchdog.choose_transition_target_broadcast(
            items,
            preferred_video_id="v2",
            preferred_broadcast_id="",
        )
        self.assertEqual((bid, vid, lifecycle), ("b2", "v2", "testing"))

    def test_choose_transition_target_ignores_non_transitionable(self) -> None:
        items = [
            make_broadcast("b1", "v1", "complete"),
            make_broadcast("b2", "v2", "ready"),
        ]
        bid, vid, lifecycle, _reason = youtube_watchdog.choose_transition_target_broadcast(
            items,
            preferred_video_id="",
            preferred_broadcast_id="",
        )
        self.assertEqual((bid, vid, lifecycle), ("b2", "v2", "ready"))

    def test_choose_transition_target_respects_forced_broadcast_id(self) -> None:
        bid, vid, lifecycle, reason = youtube_watchdog.choose_transition_target_broadcast(
            [],
            preferred_video_id="",
            preferred_broadcast_id="forced-123",
        )
        self.assertEqual((bid, vid, lifecycle), ("forced-123", "", ""))
        self.assertIn("YTW_FORCE_LIVE_BROADCAST_ID", reason)

    def test_force_live_ready_uses_testing_then_live(self) -> None:
        self.assertEqual(
            youtube_watchdog.force_live_transition_statuses("ready", "live"),
            ["testing", "live"],
        )

    def test_force_live_testing_goes_direct_to_live(self) -> None:
        self.assertEqual(
            youtube_watchdog.force_live_transition_statuses("testing", "live"),
            ["live"],
        )


class ForceLiveAutoRecoveryTests(unittest.TestCase):
    def _oauth(self) -> youtube_api.OAuthProbeResult:
        return youtube_api.OAuthProbeResult(
            enabled=True,
            configured=True,
            probe_ok=True,
            healthy=False,
            reason="oauth ready",
            mode="shadow",
            life_cycle_status="ready",
            broadcast_id="OLD",
            video_id="",
            channel_id="UC",
            bound_stream_id="STREAM123",
            stream_status="active",
            stream_health_status="good",
            remote_checked=True,
        )

    def _base_patches(self):
        return [
            mock.patch.object(youtube_api, "FORCE_LIVE_MIN_FAILS", 3),
            mock.patch.object(youtube_api, "FORCE_LIVE_REQUIRE_INGEST", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_MIN_STREAM_UPTIME_SEC", 45),
            mock.patch.object(youtube_api, "FORCE_LIVE_REQUIRE_OAUTH_STREAM_ACTIVE", True),
            mock.patch.object(youtube_api, "OAUTH_ENABLE", True),
            mock.patch.object(youtube_api, "OAUTH_REQUIRE_CHANNEL_MATCH", True),
            mock.patch.object(youtube_api, "CHANNEL_ID", "UC"),
            mock.patch.object(youtube_api, "FORCE_LIVE_TARGET_STATUS", "live"),
            mock.patch.object(youtube_api, "FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_CATEGORY_ID", ""),
            mock.patch.object(youtube_api, "FORCE_LIVE_BROADCAST_ID", ""),
            mock.patch.object(youtube_api, "FORCE_LIVE_ON_UPCOMING_ONCE", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_AUTO_RECOVERY", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_SUCCESS_COOLDOWN_SEC", 1800),
            mock.patch.object(youtube_api, "oauth_is_configured", return_value=True),
            mock.patch.object(youtube_api, "get_oauth_access_token", return_value=("TOKEN", 999999, "token ok")),
            mock.patch.object(youtube_api, "load_force_live_state", return_value={}),
            mock.patch.object(youtube_api, "time", mock.Mock(time=mock.Mock(return_value=1_000), sleep=mock.Mock())),
        ]

    def test_auto_recovery_replaces_autostart_ready_broadcast(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        source["contentDetails"]["enableAutoStart"] = True
        source["contentDetails"]["enableAutoStop"] = True
        saved: list[dict] = []
        post_calls: list[tuple[str, str]] = []

        def fake_post(endpoint: str, _token: str, params: dict[str, str]) -> dict:
            if endpoint == "liveBroadcasts/transition":
                post_calls.append((params["id"], params["broadcastStatus"]))
                return {"status": {"lifeCycleStatus": "testing" if params["broadcastStatus"] == "testing" else "liveStarting"}}
            return {}

        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST", True),
                mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
                mock.patch.object(
                    youtube_api,
                    "create_recovery_broadcast",
                    return_value=("NEW", "created replacement broadcast"),
                ),
                mock.patch.object(youtube_api, "cleanup_replaced_broadcast", return_value="deleted stale source broadcast OLD"),
                mock.patch.object(youtube_api, "find_broadcast_lifecycle", return_value="ready"),
                mock.patch.object(youtube_api, "wait_for_broadcast_lifecycle", return_value="testing"),
                mock.patch.object(youtube_api, "youtube_live_api_post", side_effect=fake_post),
                mock.patch.object(youtube_api, "save_force_live_state", side_effect=lambda state: saved.append(dict(state))),
            ]
        )
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
                url_recovery_elapsed_sec=180,
                replacement_min_elapsed_sec=180,
            )

        self.assertTrue(ok, reason)
        self.assertEqual(post_calls, [("NEW", "testing"), ("NEW", "live")])
        self.assertTrue(saved)
        self.assertTrue(saved[-1]["ok"])
        self.assertEqual(saved[-1]["target_broadcast_id"], "NEW")
        self.assertIn("created replacement broadcast", saved[-1]["target_reason"])
        self.assertEqual(saved[-1]["cleanup_reason"], "deleted stale source broadcast OLD")

    def test_auto_recovery_records_replacement_creation_failure(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        source["contentDetails"]["enableAutoStart"] = True
        source["contentDetails"]["enableAutoStop"] = True
        saved: list[dict] = []
        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST", True),
                mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
                mock.patch.object(youtube_api, "create_recovery_broadcast", side_effect=RuntimeError("bind exploded")),
                mock.patch.object(youtube_api, "save_force_live_state", side_effect=lambda state: saved.append(dict(state))),
            ]
        )
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
                url_recovery_elapsed_sec=180,
                replacement_min_elapsed_sec=180,
            )

        self.assertFalse(ok)
        self.assertIn("create/bind failed", reason)
        self.assertTrue(saved)
        self.assertFalse(saved[-1]["ok"])
        self.assertIn("bind exploded", saved[-1]["error"])

    def test_auto_recovery_defers_replacement_before_url_preservation_window(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        source["contentDetails"]["enableAutoStart"] = True
        source["contentDetails"]["enableAutoStop"] = True
        create_mock = mock.Mock()
        transition_mock = mock.Mock()
        saved: list[dict] = []
        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST", True),
                mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
                mock.patch.object(youtube_api, "create_recovery_broadcast", create_mock),
                mock.patch.object(youtube_api, "youtube_live_api_post", transition_mock),
                mock.patch.object(youtube_api, "save_force_live_state", side_effect=lambda state: saved.append(dict(state))),
            ]
        )
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
                url_recovery_elapsed_sec=120,
                replacement_min_elapsed_sec=180,
            )

        self.assertFalse(ok)
        self.assertIn("replacement broadcast deferred", reason)
        create_mock.assert_not_called()
        transition_mock.assert_not_called()
        self.assertTrue(saved)
        self.assertFalse(saved[-1]["ok"])
        self.assertFalse(saved[-1]["replacement_allowed"])
        self.assertEqual(saved[-1]["url_recovery_elapsed_sec"], 120)
        self.assertEqual(saved[-1]["replacement_min_elapsed_sec"], 180)

    def test_recovery_broadcast_disables_auto_stop_and_enables_auto_start_by_default(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        captured: list[dict] = []

        def fake_post_json(_endpoint: str, _token: str, _params: dict[str, str], body: dict) -> dict:
            captured.append(body)
            return {"id": "NEW"}

        with (
            mock.patch.object(youtube_api, "FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_CATEGORY_ID", ""),
            mock.patch.object(youtube_api, "youtube_live_api_post_json", side_effect=fake_post_json),
            mock.patch.object(youtube_api, "youtube_live_api_post", return_value={}),
        ):
            bid, reason = youtube_api.create_recovery_broadcast("TOKEN", source, "STREAM123")

        self.assertEqual(bid, "NEW")
        self.assertIn("created replacement broadcast", reason)
        self.assertTrue(captured)
        self.assertIs(captured[-1]["contentDetails"]["enableAutoStart"], True)
        self.assertIs(captured[-1]["contentDetails"]["enableAutoStop"], False)

    def test_recovery_broadcast_retries_without_auto_start_when_invalid(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        captured: list[bool] = []

        def fake_post_json(_endpoint: str, _token: str, _params: dict[str, str], body: dict) -> dict:
            auto_start = bool(body["contentDetails"].get("enableAutoStart"))
            captured.append(auto_start)
            if auto_start:
                payload = b'{"error":{"errors":[{"reason":"invalidAutoStart"}]}}'
                err = urllib.error.HTTPError(
                    "https://www.googleapis.com/youtube/v3/liveBroadcasts",
                    400,
                    "Bad Request",
                    {},
                    io.BytesIO(payload),
                )
                self.addCleanup(err.close)
                raise err
            return {"id": "NEW"}

        with (
            mock.patch.object(youtube_api, "FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_START", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_CATEGORY_ID", ""),
            mock.patch.object(youtube_api, "youtube_live_api_post_json", side_effect=fake_post_json),
            mock.patch.object(youtube_api, "youtube_live_api_post", return_value={}),
        ):
            bid, reason = youtube_api.create_recovery_broadcast("TOKEN", source, "STREAM123")

        self.assertEqual(bid, "NEW")
        self.assertEqual(captured, [True, False])
        self.assertIn("retried enableAutoStart=false", reason)
        self.assertIn("enableAutoStop=false", reason)

    def test_recovery_broadcast_can_enable_auto_stop_explicitly(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        captured: list[dict] = []

        def fake_post_json(_endpoint: str, _token: str, _params: dict[str, str], body: dict) -> dict:
            captured.append(body)
            return {"id": "NEW"}

        with (
            mock.patch.object(youtube_api, "FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP", True),
            mock.patch.object(youtube_api, "FORCE_LIVE_CATEGORY_ID", ""),
            mock.patch.object(youtube_api, "youtube_live_api_post_json", side_effect=fake_post_json),
            mock.patch.object(youtube_api, "youtube_live_api_post", return_value={}),
        ):
            bid, _reason = youtube_api.create_recovery_broadcast("TOKEN", source, "STREAM123")

        self.assertEqual(bid, "NEW")
        self.assertTrue(captured)
        self.assertIs(captured[-1]["contentDetails"]["enableAutoStop"], True)

    def test_recovery_broadcast_updates_category_from_existing_snippet_safely(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        captured_update: list[dict] = []

        def fake_get(endpoint: str, _token: str, params: dict[str, str]) -> dict:
            self.assertEqual(endpoint, "videos")
            self.assertEqual(params, {"part": "snippet", "id": "NEW"})
            return {
                "items": [
                    {
                        "id": "NEW",
                        "snippet": {
                            "publishedAt": "readonly",
                            "channelId": "readonly-channel",
                            "title": "Tokyo ADS-B 24/7",
                            "description": "ADSB + NCS",
                            "tags": ["adsb", "ncs"],
                            "categoryId": "10",
                            "localized": {"title": "readonly"},
                        },
                    }
                ]
            }

        def fake_update(_token: str, params: dict[str, str], body: dict) -> dict:
            self.assertEqual(params, {"part": "snippet"})
            captured_update.append(body)
            return {"id": "NEW"}

        with (
            mock.patch.object(youtube_api, "FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP", False),
            mock.patch.object(youtube_api, "FORCE_LIVE_CATEGORY_ID", "28"),
            mock.patch.object(youtube_api, "youtube_live_api_post_json", return_value={"id": "NEW"}),
            mock.patch.object(youtube_api, "youtube_live_api_post", return_value={}),
            mock.patch.object(youtube_api, "youtube_live_api_get", side_effect=fake_get),
            mock.patch.object(youtube_api, "youtube_videos_api_update", side_effect=fake_update),
        ):
            bid, reason = youtube_api.create_recovery_broadcast("TOKEN", source, "STREAM123")

        self.assertEqual(bid, "NEW")
        self.assertIn("category updated 10->28", reason)
        self.assertEqual(captured_update[0]["id"], "NEW")
        self.assertEqual(
            captured_update[0]["snippet"],
            {
                "title": "Tokyo ADS-B 24/7",
                "description": "ADSB + NCS",
                "tags": ["adsb", "ncs"],
                "categoryId": "28",
            },
        )

    def test_force_live_blocks_destructive_action_when_oauth_channel_mismatches(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        source["contentDetails"]["enableAutoStart"] = False
        saved: list[dict] = []
        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "CHANNEL_ID", "UC_EXPECTED"),
                mock.patch.object(youtube_api, "OAUTH_REQUIRE_CHANNEL_MATCH", True),
                mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
                mock.patch.object(youtube_api, "youtube_live_api_post"),
                mock.patch.object(youtube_api, "save_force_live_state", side_effect=lambda state: saved.append(dict(state))),
            ]
        )
        oauth = self._oauth()
        oauth.channel_id = "UC_WRONG"
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=oauth,
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
            )

        self.assertFalse(ok)
        self.assertIn("oauth channel validation failed", reason)
        self.assertTrue(saved)
        self.assertIn("UC_EXPECTED", saved[-1]["error"])

    def test_auto_recovery_does_not_replace_manual_ready_broadcast(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        source["contentDetails"]["enableAutoStart"] = False
        post_calls: list[tuple[str, str]] = []

        def fake_post(endpoint: str, _token: str, params: dict[str, str]) -> dict:
            if endpoint == "liveBroadcasts/transition":
                post_calls.append((params["id"], params["broadcastStatus"]))
            return {"status": {"lifeCycleStatus": "testing"}}

        create_mock = mock.Mock()
        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
                mock.patch.object(youtube_api, "create_recovery_broadcast", create_mock),
                mock.patch.object(youtube_api, "find_broadcast_lifecycle", return_value="ready"),
                mock.patch.object(youtube_api, "wait_for_broadcast_lifecycle", return_value="testing"),
                mock.patch.object(youtube_api, "youtube_live_api_post", side_effect=fake_post),
                mock.patch.object(youtube_api, "save_force_live_state"),
            ]
        )
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
            )

        self.assertTrue(ok, reason)
        self.assertEqual(post_calls, [("OLD", "testing"), ("OLD", "live")])
        create_mock.assert_not_called()

    def test_auto_recovery_manually_transitions_persistent_scheduled_broadcast(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        source["contentDetails"]["enableAutoStart"] = True
        source["contentDetails"]["enableAutoStop"] = False
        post_calls: list[tuple[str, str]] = []

        def fake_post(endpoint: str, _token: str, params: dict[str, str]) -> dict:
            if endpoint == "liveBroadcasts/transition":
                post_calls.append((params["id"], params["broadcastStatus"]))
            return {"status": {"lifeCycleStatus": "testing"}}

        create_mock = mock.Mock()
        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST", True),
                mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
                mock.patch.object(youtube_api, "create_recovery_broadcast", create_mock),
                mock.patch.object(youtube_api, "find_broadcast_lifecycle", return_value="ready"),
                mock.patch.object(youtube_api, "wait_for_broadcast_lifecycle", return_value="testing"),
                mock.patch.object(youtube_api, "youtube_live_api_post", side_effect=fake_post),
                mock.patch.object(youtube_api, "save_force_live_state"),
            ]
        )
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
                url_recovery_elapsed_sec=600,
                replacement_min_elapsed_sec=180,
            )

        self.assertTrue(ok, reason)
        self.assertEqual(post_calls, [("OLD", "testing"), ("OLD", "live")])
        create_mock.assert_not_called()
        self.assertIn("persistent scheduled broadcast enableAutoStop=false", reason)

    def test_auto_recovery_refuses_replacement_when_not_allowed(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        source["contentDetails"]["enableAutoStart"] = True
        source["contentDetails"]["enableAutoStop"] = True
        create_mock = mock.Mock()
        transition_mock = mock.Mock()
        saved: list[dict] = []
        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "find_owned_broadcast", return_value=source),
                mock.patch.object(youtube_api, "create_recovery_broadcast", create_mock),
                mock.patch.object(youtube_api, "youtube_live_api_post", transition_mock),
                mock.patch.object(youtube_api, "save_force_live_state", side_effect=lambda state: saved.append(dict(state))),
            ]
        )
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
            )

        self.assertFalse(ok)
        self.assertIn("replacement broadcast disabled", reason)
        create_mock.assert_not_called()
        transition_mock.assert_not_called()
        self.assertTrue(saved)
        self.assertFalse(saved[-1]["ok"])
        self.assertIn("replacement broadcast disabled", saved[-1]["error"])

    def test_cleanup_replaced_broadcast_deletes_ready_source_only(self) -> None:
        source = make_broadcast("OLD", "", "ready")
        with mock.patch.object(youtube_api, "youtube_live_api_delete") as delete_mock:
            reason = youtube_api.cleanup_replaced_broadcast("TOKEN", source, "NEW")
        self.assertEqual(reason, "deleted stale source broadcast OLD")
        delete_mock.assert_called_once_with("liveBroadcasts", "TOKEN", {"id": "OLD"})

    def test_cleanup_replaced_broadcast_never_deletes_live_source(self) -> None:
        source = make_broadcast("OLD", "", "live")
        with mock.patch.object(youtube_api, "youtube_live_api_delete") as delete_mock:
            reason = youtube_api.cleanup_replaced_broadcast("TOKEN", source, "NEW")
        self.assertIn("cleanup skipped", reason)
        delete_mock.assert_not_called()

    def test_auto_recovery_still_respects_backoff(self) -> None:
        token_mock = mock.Mock()
        patches = self._base_patches()
        patches.extend(
            [
                mock.patch.object(youtube_api, "load_force_live_state", return_value={"next_allowed_ts": 1_500}),
                mock.patch.object(youtube_api, "get_oauth_access_token", token_mock),
            ]
        )
        with contextlib.ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            ok, reason = youtube_api.force_transition_live_once(
                True,
                fail_count=3,
                video_id="",
                api_reason="data api liveBroadcastContent=upcoming",
                stream_active=True,
                ingest_connected=True,
                oauth=self._oauth(),
                ffmpeg_uptime_sec=120,
                force_live_once_cli=False,
            )
        self.assertFalse(ok)
        self.assertIn("backoff active", reason)
        token_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
