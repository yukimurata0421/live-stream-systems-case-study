from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.adapters.lifecycle import RuntimeLifecycleAdapter
from stream_monitoring_v4.adapters.youtube import YouTubeStateAdapter
from stream_monitoring_v4.commands.shadow_once import main as shadow_main
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.incidents.policy import DEFAULT_INCIDENT_POLICIES
from stream_monitoring_v4.incidents.lifecycle import RuntimeLifecycleIncidentService
from stream_monitoring_v4.incidents.service import IncidentService
from stream_monitoring_v4.notifications.dispatcher import NotificationDispatcher
from stream_monitoring_v4.notifications.providers.recording import RecordingProvider
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.runtime.isolation import validate_isolated_database_path
from stream_monitoring_v4.runtime.observer import Observer
from stream_monitoring_v4.runtime.pipeline import ShadowPipeline
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, write_json


class PipelineEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source_repository = self.root / "read-only-stream-v3-repository"
        self.source_repository.mkdir()
        self.state = self.root / "read-only-v3-state"
        self.state.mkdir()
        timestamp = utc_text(BASE_TS)
        write_json(
            self.state / "youtube_watchdog_stats.json",
            {
                "stats_file_updated_at_utc": timestamp,
                "remote_probe_ts_utc": timestamp,
                "oauth_checked_ts_utc": timestamp,
                "remote_sample_id": "rps-e2e",
                "api_live_state": "live",
                "oauth_life_cycle_status": "live",
                "oauth_stream_health_status": "good",
                "oauth_probe_ok": True,
                "oauth_stream_status": "active",
                "stream_active": False,
                "ingest_connected": False,
                "local_ok": False,
                "video_id": "same-url-video",
                "expected_video_id": "same-url-video",
                "private_token": "not-exported",
            },
        )
        write_json(
            self.state / "youtube_video_id_resolver_state.json",
            {
                "ts_utc": timestamp,
                "video_id": "same-url-video",
                "expected_video_id": "same-url-video",
                "url_preservation_active": True,
            },
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_reliability_sources(self, checked_at: str) -> None:
        formal_gates = {
            objective: {
                "compliance_status": "met",
                "measurement_status": "valid",
                "measurement_unknown_reasons": [],
                "sli_pct": 100.0,
                "coverage_pct": 100.0,
                "source_freshness_pct": 100.0,
                "source_disagreement": False,
            }
            for objective in (
                "youtube_availability",
                "same_url_preservation",
                "upload_ceiling",
                "youtube_input_quality",
                "audio_correctness",
            )
        }
        write_json(
            self.state / "operational_reliability_status.json",
            {
                "checked_at_utc": checked_at,
                "no_automatic_recovery": True,
                "formal_gates": formal_gates,
            },
        )
        write_json(
            self.state / "operational_reliability_burn_status.json",
            {
                "checked_at_utc": checked_at,
                "no_automatic_recovery": True,
                "fast_feedback": {
                    "youtube_input_quality": {
                        "measurement_status": "valid",
                        "measurement_unknown_reasons": [],
                        "target_met_on_observed_samples": True,
                        "sli_pct": 100.0,
                        "coverage_pct": 100.0,
                        "source_freshness_pct": 100.0,
                        "source_disagreement": False,
                    }
                },
            },
        )

    def _pipeline(
        self,
        database: Path,
        *,
        delivery_enabled: bool = False,
    ) -> tuple[MonitoringRepository, ShadowPipeline, YouTubeStateAdapter]:
        repository = MonitoringRepository(database)
        repository.initialize(applied_at=utc_text(BASE_TS))
        delivery_epoch = (
            repository.activate_delivery_epoch(
                cutover_at=utc_text(BASE_TS),
                activated_at=utc_text(BASE_TS),
                policy_revision="test-e2e-cutover-r1",
                writer_identity="isolated-test-writer",
                note="isolated end-to-end provider test",
            )
            if delivery_enabled
            else None
        )
        adapter = YouTubeStateAdapter(
            watchdog_stats_file=self.state / "youtube_watchdog_stats.json",
            resolver_state_file=self.state / "youtube_video_id_resolver_state.json",
        )
        pipeline = ShadowPipeline(
            Observer(repository),
            CurrentReducerService(repository, DEFAULT_POLICIES),
            IncidentService(
                repository,
                DEFAULT_INCIDENT_POLICIES,
                RoutePolicy(),
                delivery_epoch_id=delivery_epoch,
            ),
        )
        return repository, pipeline, adapter

    def test_read_only_source_to_current_incident_outbox_and_fake_delivery(self) -> None:
        source_before = {
            path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in self.state.iterdir()
        }
        repository, pipeline, adapter = self._pipeline(
            self.root / "isolated" / "monitoring.sqlite3",
            delivery_enabled=True,
        )
        result = pipeline.run_once(
            [adapter],
            ["youtube_lifecycle", "youtube_input_quality", "delivery"],
            now_ts=BASE_TS,
            now_at=utc_text(BASE_TS),
        )
        currents = {item.domain: item.state for item in result.currents}
        self.assertEqual(
            currents,
            {
                "youtube_lifecycle": "good",
                "youtube_input_quality": "unknown",
                "delivery": "bad",
            },
        )
        self.assertEqual([item.transition.phase for item in result.incidents if item.transition], ["detected"])
        self.assertEqual({item.route for item in repository.intents()}, {"discord", "slack"})

        discord = RecordingProvider()
        slack = RecordingProvider()
        summary = NotificationDispatcher(
            repository,
            {"discord": discord, "slack": slack},
            owner="isolated-test-writer",
        ).dispatch_once(now_ts=BASE_TS)
        self.assertEqual((summary.sent, summary.failed, summary.pending), (2, 0, 0))
        self.assertEqual(repository.integrity_check(), "ok")
        for path, (content, mtime_ns) in source_before.items():
            self.assertEqual(path.read_bytes(), content)
            self.assertEqual(path.stat().st_mtime_ns, mtime_ns)

    def test_same_snapshot_rerun_is_idempotent_end_to_end(self) -> None:
        repository, pipeline, adapter = self._pipeline(self.root / "isolated.sqlite3")
        first = pipeline.run_once(
            [adapter],
            ["youtube_lifecycle", "youtube_input_quality", "delivery"],
            now_ts=BASE_TS,
            now_at=utc_text(BASE_TS),
        )
        second = pipeline.run_once(
            [adapter],
            ["youtube_lifecycle", "youtube_input_quality", "delivery"],
            now_ts=BASE_TS,
            now_at=utc_text(BASE_TS),
        )
        self.assertEqual(first.observer.inserted_observations, 4)
        self.assertEqual(second.observer.duplicate_observations, 4)
        self.assertEqual([item.phase for item in repository.transitions()], ["detected"])
        self.assertEqual(len(repository.intents()), 2)
        self.assertTrue(all(item.duplicate for item in second.incidents))

    def test_completed_runtime_edge_reaches_recovery_outbox_end_to_end(self) -> None:
        timestamp = utc_text(BASE_TS + 20)
        write_json(
            self.state / "youtube_watchdog_stats.json",
            {
                "stats_file_updated_at_utc": timestamp,
                "remote_probe_ts_utc": timestamp,
                "oauth_checked_ts_utc": timestamp,
                "remote_sample_id": "rps-lifecycle-e2e",
                "api_live_state": "live",
                "oauth_life_cycle_status": "live",
                "oauth_stream_health_status": "good",
                "oauth_probe_ok": True,
                "oauth_stream_status": "active",
                "stream_active": True,
                "ingest_connected": True,
                "local_ok": True,
            },
        )
        write_json(
            self.state / "youtube_video_id_resolver_state.json",
            {
                "ts_utc": timestamp,
                "video_id": "same-url-video",
                "expected_video_id": "same-url-video",
            },
        )
        write_json(
            self.state / "runtime_lifecycle_events.json",
            {
                "schema": "monitoring_v4.runtime_lifecycle_projection.v1",
                "events": [
                    {
                        "schema": "monitoring_v4.runtime_lifecycle_event.v1",
                        "event_id": "evt-scheduled-e2e",
                        "event_type": "ffmpeg_auto_recovered",
                        "opened_at": utc_text(BASE_TS),
                        "recovered_at": utc_text(BASE_TS + 5),
                        "run_id": "run-e2e",
                        "restart_count": 1,
                        "exit_code": 1,
                        "delay_sec": 5,
                        "scheduled_event_id": "evt-scheduled-e2e",
                        "recovered_event_id": "evt-started-e2e",
                    }
                ],
            },
        )
        repository = MonitoringRepository(self.root / "lifecycle.sqlite3")
        repository.initialize(applied_at=utc_text(BASE_TS))
        route_policy = RoutePolicy()
        pipeline = ShadowPipeline(
            Observer(repository),
            CurrentReducerService(repository, DEFAULT_POLICIES),
            IncidentService(repository, DEFAULT_INCIDENT_POLICIES, route_policy),
            RuntimeLifecycleIncidentService(repository, route_policy),
        )
        adapters = [
            YouTubeStateAdapter(
                watchdog_stats_file=self.state / "youtube_watchdog_stats.json",
                resolver_state_file=self.state / "youtube_video_id_resolver_state.json",
            ),
            RuntimeLifecycleAdapter(
                state_file=self.state / "runtime_lifecycle_events.json"
            ),
        ]
        first = pipeline.run_once(
            adapters,
            ["youtube_lifecycle", "youtube_input_quality", "delivery"],
            now_ts=BASE_TS + 20,
            now_at=timestamp,
        )
        second = pipeline.run_once(
            adapters,
            ["youtube_lifecycle", "youtube_input_quality", "delivery"],
            now_ts=BASE_TS + 21,
            now_at=utc_text(BASE_TS + 21),
        )

        self.assertEqual({item.domain: item.state for item in first.currents}["delivery"], "good")
        self.assertEqual([item.phase for item in repository.transitions()], ["recovered"])
        self.assertEqual({item.route for item in repository.intents()}, {"discord", "slack"})
        self.assertEqual(first.to_dict()["transitions"][0]["phase"], "recovered")
        self.assertTrue(any(item.duplicate for item in second.incidents))
        self.assertEqual(len(repository.intents()), 2)

    def test_cli_runs_only_with_explicit_isolated_database(self) -> None:
        database = self.root / "v4" / "state.sqlite3"
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = shadow_main(
                [
                    "--db",
                    str(database),
                    "--state-root",
                    str(self.state),
                    "--source-repo",
                    str(self.source_repository),
                    "--now-ts",
                    str(BASE_TS),
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(result, 0)
        self.assertTrue(database.is_file())
        self.assertEqual(len(payload["notification_intents"]), 2)

    def test_incomplete_projection_keeps_last_good_public_shadow_unchanged(self) -> None:
        self._write_reliability_sources(utc_text(BASE_TS))
        database = self.root / "v4-public" / "state.sqlite3"
        public_output = self.root / "v4-public" / "public-safe-shadow.json"
        common = [
            "--db",
            str(database),
            "--state-root",
            str(self.state),
            "--source-repo",
            str(self.source_repository),
            "--public-shadow-output",
            str(public_output),
            "--summary-only",
        ]
        first_stdout = io.StringIO()
        with contextlib.redirect_stdout(first_stdout):
            self.assertEqual(shadow_main([*common, "--now-ts", str(BASE_TS)]), 0)
        first_summary = json.loads(first_stdout.getvalue())
        before = public_output.read_bytes()
        before_mtime = public_output.stat().st_mtime_ns
        self.assertTrue(first_summary["public_shadow_written"])

        second_stdout = io.StringIO()
        with contextlib.redirect_stdout(second_stdout):
            self.assertEqual(
                shadow_main([*common, "--now-ts", str(BASE_TS + 2 * 3600 + 1)]),
                0,
            )
        second_summary = json.loads(second_stdout.getvalue())
        self.assertFalse(second_summary["public_shadow_written"])
        self.assertFalse(second_summary["projection_integrity"]["complete"])
        self.assertEqual(public_output.read_bytes(), before)
        self.assertEqual(public_output.stat().st_mtime_ns, before_mtime)

    def test_database_inside_source_state_or_stream_v3_is_rejected_before_write(self) -> None:
        for database in (
            self.state / "monitoring.sqlite3",
            self.source_repository / ".state" / "monitoring-v4.sqlite3",
        ):
            with self.subTest(database=database):
                with self.assertRaisesRegex(ValueError, "must not be placed"):
                    validate_isolated_database_path(
                        database,
                        source_state_root=self.state,
                        migration_source_repository=self.source_repository,
                    )
                self.assertFalse(database.exists())

    def test_public_output_isolation_is_rejected_before_cycle_writes(self) -> None:
        database = self.root / "isolated-v4" / "monitoring.sqlite3"
        with self.assertRaisesRegex(ValueError, "must not be placed"):
            shadow_main(
                [
                    "--db",
                    str(database),
                    "--state-root",
                    str(self.state),
                    "--source-repo",
                    str(self.source_repository),
                    "--public-shadow-output",
                    str(self.state / "public-safe-shadow.json"),
                    "--now-ts",
                    str(BASE_TS),
                ]
            )
        repository = MonitoringRepository(database)
        with repository.connection(read_only=True) as connection:
            observations = connection.execute(
                "SELECT COUNT(*) AS count FROM observations"
            ).fetchone()["count"]
            cycles = connection.execute(
                "SELECT COUNT(*) AS count FROM shadow_cycles"
            ).fetchone()["count"]
        self.assertEqual(observations, 0)
        self.assertEqual(cycles, 0)


if __name__ == "__main__":
    unittest.main()
