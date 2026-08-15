from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from stream_core.operational_reliability import evidence_store
from stream_core.notifications import cli_adapter, incidents


SPEC = importlib.util.spec_from_file_location(
    "stream_v3_operational_reliability_rollup",
    ROOT / "ops" / "scripts" / "stream_v3_operational_reliability_rollup.py",
)
assert SPEC and SPEC.loader
rollup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rollup)

IMPORT_SPEC = importlib.util.spec_from_file_location(
    "stream_v3_external_blackbox_import",
    ROOT / "ops" / "scripts" / "stream_v3_external_blackbox_import.py",
)
assert IMPORT_SPEC and IMPORT_SPEC.loader
blackbox_import = importlib.util.module_from_spec(IMPORT_SPEC)
IMPORT_SPEC.loader.exec_module(blackbox_import)


class OperationalReliabilityTests(unittest.TestCase):
    def test_fast_feedback_incident_tracks_current_bad_and_repeats_every_ten_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_burn_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:10:00Z",
                        "fast_feedback": {
                            "youtube_input_quality": {
                                "measurement_status": "valid",
                                "target_met_on_observed_samples": True,
                                "sli_pct": 100.0,
                                "coverage_pct": 100.0,
                                "source_disagreement_current": False,
                                "raw_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-10T00:09:05Z",
                                    "classification": "bad_health_warning",
                                    "eligible": True,
                                    "good": False,
                                },
                                "prometheus_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-10T00:10:00Z",
                                    "eligible": True,
                                    "good": False,
                                },
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        current = next(item for item in found if item["id"].endswith("fast_feedback"))
        self.assertEqual(current["repeat_sec"], 600)
        self.assertEqual(current["observed_ts"], 1_786_320_545)
        self.assertIn("current=bad_health_warning", current["evidence"])

    def test_fast_feedback_below_target_closes_when_current_quality_is_good(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_burn_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:10:00Z",
                        "fast_feedback": {
                            "youtube_input_quality": {
                                "measurement_status": "valid",
                                "target_met_on_observed_samples": False,
                                "sli_pct": 89.387,
                                "coverage_pct": 100.0,
                                "source_disagreement_current": False,
                                "raw_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-10T00:09:41Z",
                                    "classification": "good",
                                    "eligible": True,
                                    "good": True,
                                },
                                "prometheus_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-10T00:10:00Z",
                                    "eligible": True,
                                    "good": True,
                                },
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertNotIn(
            "reliability:youtube_input_quality_fast_feedback",
            {item["id"] for item in found},
        )

    def test_fast_feedback_recovery_uses_current_youtube_health_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stats_file = root / "youtube_watchdog_stats.json"
            stats_file.write_text(
                json.dumps(
                    {
                        "ts_utc": "2026-08-10T00:09:41Z",
                        "status": "ok",
                        "healthy": True,
                        "oauth_stream_health_status": "good",
                        "oauth_stream_health_issues": 0,
                        "ingest_connected": True,
                        "action": "none",
                    }
                ),
                encoding="utf-8",
            )
            context = cli_adapter.NotifyCliContext(
                base_dir=root,
                state_base_dir=root,
                notify_env_file=root / "notify.env",
                notify_timer="notify.timer",
                maintenance_state_file=root / "maintenance.json",
                stream1090_report_events_file=root / "stream1090.jsonl",
                upstream_report_events_file=root / "upstream.jsonl",
                youtube_watchdog_stats_file=stats_file,
                read_env_file=lambda _path: {},
                read_json_file=lambda path: json.loads(path.read_text(encoding="utf-8")),
                parse_bool=lambda _value: None,
                parse_utc_ts=incidents.parse_utc_ts,
                read_maintenance_state=lambda _path: {},
                observe_payload=lambda _hours: (0, {}, ""),
            )
            recovered_ts, evidence = cli_adapter.recovery_observation_for_incident(
                context,
                "reliability:youtube_input_quality_fast_feedback",
                1_786_320_600,
            )

        self.assertEqual(recovered_ts, 1_786_320_581)
        self.assertIn("oauth_health=good", evidence)
        self.assertIn("oauth_issues=0", evidence)

    def test_planned_rollout_suppresses_transient_resolver_fast_mode_incident(self) -> None:
        observe_payload = {
            "pass": True,
            "checks": {
                "current_fail": False,
                "youtube_current_degraded": False,
                "youtube_observability_current_fail": False,
                "fast_mode_current_active": True,
            },
            "api_report_judgment": "ok",
            "fast_mode_current_active": True,
            "fast_mode_judgment": "ok_short_fast_mode_episode",
            "fast_mode_episode_count_24h": 1,
            "fast_mode_active_duration_sec_24h": 30,
            "fast_mode_api_units_estimated_24h": 20,
            "encoder_gap_enable_auto_stop_false_judgment": "ok_none",
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "public_probe_judgment": "ok_none",
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with mock.patch.object(incidents, "planned_rollout_context", return_value={"rollout_id": "test"}):
                found = incidents.collect_notification_incidents(
                    observe_payload=lambda _hours: (0, observe_payload, ""),
                    stream1090_report_events_file=root / "stream1090.jsonl",
                    upstream_report_events_file=root / "upstream.jsonl",
                    runtime_state_base_dir=root,
                    now_ts=1_786_320_600,
                    bootstrap_grace_active=True,
                )

        self.assertNotIn("resolver:fast_mode_active_or_runaway", {item["id"] for item in found})

    def test_notification_release_keeps_inline_single_writer_and_arena_state(self) -> None:
        deploy = (ROOT / "ops" / "scripts" / "deploy_stream_v3_monitoring_release.sh").read_text(
            encoding="utf-8"
        )
        unit = (ROOT / "ops" / "systemd" / "adsb-streamnew-notify.service").read_text(
            encoding="utf-8"
        )

        self.assertIn("systemctl disable --now adsb-streamnew-notify.timer", deploy)
        enable_block = deploy.split("sudo systemctl enable --now", 1)[1].split(
            "sudo systemctl restart", 1
        )[0]
        self.assertNotIn("adsb-streamnew-notify.timer", enable_block)
        self.assertIn("EnvironmentFile=-/etc/default/adsb-streamnew-notify", unit)
        self.assertIn(
            "Environment=STREAM_RUNTIME_STATE_DIR=/var/lib/stream-v3/observability-monitor",
            unit,
        )
        self.assertNotIn("stream_v2/.state/env/adsb-streamnew.env", unit)

    def test_missing_reliability_artifacts_have_distinct_ids_and_unknown_age(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            rollup = incidents.operational_reliability_incidents(
                status_file=root / "operational_reliability_status.json",
                now_ts=1_786_320_600,
            )
            burn = incidents.operational_reliability_incidents(
                status_file=root / "operational_reliability_burn_status.json",
                now_ts=1_786_320_600,
            )

        self.assertEqual(rollup[0]["id"], "reliability:rollup_missing_or_stale")
        self.assertEqual(burn[0]["id"], "reliability:burn_status_missing_or_stale")
        self.assertEqual({item[0]["evidence"] for item in (rollup, burn)}, {
            "sample_age=unknown source=operational_reliability_status.json",
            "sample_age=unknown source=operational_reliability_burn_status.json",
        })

    def test_release_deploy_immediately_refreshes_rollup_burn_and_external_evidence(self) -> None:
        deploy = (ROOT / "ops" / "scripts" / "deploy_stream_v3_monitoring_release.sh").read_text(
            encoding="utf-8"
        )

        start_block = deploy.split("sudo systemctl start", 1)[1]
        self.assertIn("stream-v3-operational-reliability-rollup.service", start_block)
        self.assertIn("stream-v3-reliability-burn-evaluator.service", start_block)
        self.assertIn("stream-v3-external-blackbox-import.service", start_block)

    def test_release_deploy_pins_public_current_link_to_immutable_release(self) -> None:
        deploy = (ROOT / "ops" / "scripts" / "deploy_stream_v3_monitoring_release.sh").read_text(
            encoding="utf-8"
        )
        unit = (ROOT / "ops" / "systemd" / "stream-v3-observability-monitor.service").read_text(
            encoding="utf-8"
        )

        self.assertIn('current_link="${STREAM_V3_CURRENT_LINK:-/opt/stream_v3}"', deploy)
        self.assertIn('release_dir="${release_root}/${tag}"', deploy)
        self.assertIn('ln -s "${release_dir}" "${temporary_link}"', deploy)
        self.assertIn('mv -Tf "${temporary_link}" "${current_link}"', deploy)
        self.assertIn("Environment=STREAM_V3_REPO_DIR=/opt/stream_v3", unit)
        self.assertIn(
            'STREAM_V3_STREAM_CLI_BIN="$${STREAM_V3_STREAM_CLI_BIN:-$${STREAM_V3_REPO_DIR}/bin/stream-prod}"',
            unit,
        )

    def test_accepted_arena_host_unit_drift_is_captured_by_repository_contract(self) -> None:
        deploy = (ROOT / "ops" / "scripts" / "deploy_stream_v3_monitoring_release.sh").read_text(
            encoding="utf-8"
        )
        systemd = ROOT / "ops" / "systemd"
        remote_recovery = (systemd / "stream-v3-remote-recovery.service").read_text(
            encoding="utf-8"
        )
        network_observer = (systemd / "adsb-streamnew-network-observer.service").read_text(
            encoding="utf-8"
        )
        stream1090 = (systemd / "adsb-streamnew-stream1090-report.service").read_text(
            encoding="utf-8"
        )
        upstream = (systemd / "adsb-streamnew-upstream-report.service").read_text(
            encoding="utf-8"
        )
        anchor_dropin = (
            systemd
            / "arena-server"
            / "stream-v3-persistent-anchor-observer.service.d"
            / "10-host-role.conf"
        ).read_text(encoding="utf-8")

        self.assertIn("stream_v3 observability/arena-monitor network observer", network_observer)
        self.assertNotIn("EnvironmentFile=-/etc/default/adsb-streamnew\n", stream1090)
        self.assertNotIn("EnvironmentFile=-/etc/default/adsb-streamnew\n", upstream)
        self.assertIn("EnvironmentFile=-/etc/default/stream-v3-observability-monitor", stream1090)
        self.assertIn("EnvironmentFile=-/etc/default/stream-v3-observability-monitor", upstream)
        self.assertIn('"$${STREAM_V3_REPO_DIR}/bin/stream-prod"', stream1090)
        self.assertIn('"$${STREAM_V3_REPO_DIR}/bin/stream-prod"', upstream)
        self.assertIn("STREAM_RUNTIME_STATE_DIR=/var/lib/stream-v3/observability-monitor", stream1090)
        self.assertIn("STREAM_RUNTIME_STATE_DIR=/var/lib/stream-v3/observability-monitor", upstream)
        self.assertIn("stream1090-report --record --base-url", stream1090)
        self.assertIn("upstream-report --record --upstream-url", upstream)
        self.assertIn("adsb-streamnew-stream1090-report.service", deploy)
        self.assertIn("adsb-streamnew-stream1090-report.timer", deploy)
        self.assertIn("adsb-streamnew-upstream-report.service", deploy)
        self.assertIn("adsb-streamnew-upstream-report.timer", deploy)
        self.assertIn("STREAM_V3_REMOTE_RECOVERY_APPLY_ACTION_PLAN=0", remote_recovery)
        self.assertIn("STREAM_V3_REMOTE_RECOVERY_ACTION_PLAN_MAX_AGE_SEC=180", remote_recovery)
        self.assertIn("WAO_PERSISTENT_TRIGGER_WAN_SNAPSHOT=0", anchor_dropin)
        self.assertIn("WAO_PERSISTENT_TRIGGER_RTMPS_BURST=0", anchor_dropin)
        self.assertIn("arena_anchor_dropin_dir", deploy)
        self.assertIn("10-host-role.conf", deploy)

    def test_same_url_ledger_keeps_baseline_daily_checkpoint_and_transition_without_plain_url(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            database = Path(td) / "evidence.sqlite3"
            rows = [
                {
                    "event_id": "a",
                    "ts_utc": "2026-08-01T00:00:00Z",
                    "live_url": "https://youtube.test/watch?v=one",
                    "video_id": "one",
                    "expected_video_id": "one",
                },
                {
                    "event_id": "b",
                    "ts_utc": "2026-08-01T00:05:00Z",
                    "live_url": "https://youtube.test/watch?v=one",
                },
                {
                    "event_id": "c",
                    "ts_utc": "2026-08-02T00:00:00Z",
                    "live_url": "https://youtube.test/watch?v=one",
                },
                {
                    "event_id": "d",
                    "ts_utc": "2026-08-02T01:00:00Z",
                    "live_url": "https://youtube.test/watch?v=two",
                    "controlled_transition": False,
                },
            ]
            inserted = evidence_store.backfill_same_url_events(database, rows, revision="v1")
            with sqlite3.connect(database) as db:
                stored = db.execute(
                    "SELECT event_type, live_url_sha256, controlled FROM same_url_transition_ledger ORDER BY observed_ts"
                ).fetchall()

        self.assertEqual(inserted, 3)
        self.assertEqual([row[0] for row in stored], ["baseline", "daily_checkpoint", "url_transition"])
        self.assertTrue(all("youtube.test" not in row[1] for row in stored))
        self.assertEqual(stored[-1][2], 0)

    def test_retention_is_400_days_and_prunes_older_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            database = Path(td) / "evidence.sqlite3"
            evidence_store.record_same_url_event(
                database,
                {
                    "event_id": "old",
                    "ts_utc": "2020-01-01T00:00:00Z",
                    "live_url": "https://youtube.test/old",
                },
            )
            result = evidence_store.prune(database, now_ts=2_000_000_000)
            remaining = evidence_store.counts(database)

        self.assertEqual(evidence_store.RETENTION_DAYS, 400)
        self.assertEqual(result["same_url_transition_ledger"], 1)
        self.assertEqual(remaining["same_url_transition_ledger"], 0)

    def test_same_url_ledger_reports_coverage_and_uncontrolled_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            database = Path(td) / "evidence.sqlite3"
            rows = [
                {
                    "event_id": "baseline",
                    "ts_utc": "2026-08-01T00:00:00Z",
                    "live_url": "https://youtube.test/one",
                },
                {
                    "event_id": "transition",
                    "ts_utc": "2026-08-02T00:00:00Z",
                    "live_url": "https://youtube.test/two",
                    "controlled_transition": False,
                },
            ]
            evidence_store.backfill_same_url_events(database, rows)
            result = evidence_store.same_url_ledger_evidence(
                database,
                start_ts=1_785_542_400,
                end_ts=1_785_715_200,
            )

        self.assertTrue(result["available"])
        self.assertEqual(result["transition_count"], 1)
        self.assertEqual(result["uncontrolled_transition_count"], 1)
        self.assertFalse(result["plain_url_retained"])

    def test_same_url_backfill_is_idempotent_and_compacts_duplicate_daily_checkpoints(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            database = Path(td) / "evidence.sqlite3"
            rows = [
                {
                    "event_id": f"event-{index}",
                    "ts_utc": f"2026-08-01T00:{index:02d}:00Z",
                    "live_url": "https://youtube.test/one",
                }
                for index in range(5)
            ]
            self.assertEqual(evidence_store.backfill_same_url_events(database, rows), 1)
            self.assertEqual(evidence_store.backfill_same_url_events(database, rows), 0)
            with sqlite3.connect(database) as db:
                db.execute(
                    """
                    INSERT INTO same_url_transition_ledger(
                      event_id,observed_ts,observed_day_jst,event_type,live_url_sha256,
                      previous_live_url_sha256,video_id_sha256,expected_video_id_sha256,
                      controlled,candidate_new_url,force_live_triggered,source,revision
                    ) SELECT 'duplicate',observed_ts+60,observed_day_jst,'daily_checkpoint',
                      live_url_sha256,previous_live_url_sha256,'','',NULL,0,0,'test',''
                    FROM same_url_transition_ledger LIMIT 1
                    """
                )
            self.assertEqual(evidence_store.compact_duplicate_daily_checkpoints(database), 1)
            remaining = evidence_store.counts(database)

        self.assertEqual(remaining["same_url_transition_ledger"], 1)

    def test_multi_window_burn_requires_two_valid_windows(self) -> None:
        def item(sli: float, status: str = "valid") -> dict:
            return {"sli_pct": sli, "measurement_status": status}

        report = {
            "windows": {
                "15m": {"youtube_input_quality": item(80.0)},
                "1h": {"youtube_input_quality": item(80.0)},
                "6h": {"youtube_input_quality": item(100.0)},
            }
        }
        alerts = rollup.multi_window_burn_alerts(report)
        self.assertEqual(alerts[0]["family"], "youtube_input_quality")
        self.assertEqual(alerts[0]["severity"], "critical")
        self.assertFalse(alerts[0]["automatic_recovery"])

        report["windows"]["15m"]["youtube_input_quality"]["measurement_status"] = "unknown"
        self.assertEqual(rollup.multi_window_burn_alerts(report), [])

    def test_notification_incidents_route_coverage_and_burn_but_not_projection_disagreement(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:00:00Z",
                        "formal_gates": {
                            "youtube_input_quality": {
                                "compliance_status": "unknown",
                                "measurement_unknown_reasons": [
                                    "minimum_coverage_not_met",
                                    "source_disagreement",
                                ],
                                "coverage_pct": 80.0,
                                "source_freshness_pct": 80.0,
                                "source_disagreement": True,
                                "source_disagreement_current": True,
                            }
                        },
                        "multi_window_burn_alerts": [
                            {
                                "family": "youtube_input_quality",
                                "severity": "critical",
                                "rule": "fast_15m_1h",
                                "burn_rates": {"15m": 20.0, "1h": 20.0},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(
            {item["id"] for item in found},
            {
                "reliability:youtube_input_quality_coverage_or_freshness",
                "reliability:youtube_input_quality_multi_window_burn",
            },
        )
        self.assertEqual(
            next(item for item in found if item["id"].endswith("multi_window_burn"))["severity"],
            "critical",
        )

    def test_historical_formal_disagreement_does_not_repeat_as_current_incident(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:00:00Z",
                        "formal_gates": {
                            "same_url_preservation": {
                                "compliance_status": "unknown",
                                "measurement_unknown_reasons": ["source_disagreement"],
                                "source_disagreement": True,
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertFalse(any(item["id"].endswith("source_disagreement") for item in found))

    def test_non_input_quality_current_source_disagreement_keeps_existing_incident(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:00:00Z",
                        "formal_gates": {
                            "same_url_preservation": {
                                "compliance_status": "unknown",
                                "measurement_unknown_reasons": ["source_disagreement"],
                                "source_disagreement": True,
                                "source_disagreement_current": True,
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(
            {item["id"] for item in found},
            {"reliability:same_url_preservation_source_disagreement"},
        )

    def test_fast_feedback_historical_disagreement_is_not_a_current_incident(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_burn_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:00:00Z",
                        "fast_feedback": {
                            "youtube_input_quality": {
                                "measurement_status": "unknown",
                                "measurement_unknown_reasons": ["source_disagreement"],
                                "source_disagreement": True,
                                "source_disagreement_current": False,
                                "current_crosscheck_reason": "current_states_agree",
                                "coverage_pct": 100.0,
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(found, [])

    def test_fast_feedback_same_producer_current_disagreement_is_diagnostic_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_burn_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:00:00Z",
                        "fast_feedback": {
                            "youtube_input_quality": {
                                "measurement_status": "unknown",
                                "measurement_unknown_reasons": ["source_disagreement"],
                                "source_disagreement": True,
                                "source_disagreement_current": True,
                                "current_crosscheck_reason": "current_state_disagreement",
                                "raw_current": {"classification": "good", "good": True},
                                "prometheus_current": {"good": False},
                                "coverage_pct": 100.0,
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(found, [])

    def test_fast_feedback_raw_warning_remains_actionable_when_projection_lags_good(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_burn_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:10:00Z",
                        "fast_feedback": {
                            "youtube_input_quality": {
                                "measurement_status": "unknown",
                                "measurement_unknown_reasons": ["source_disagreement"],
                                "source_disagreement": True,
                                "source_disagreement_current": True,
                                "raw_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-10T00:09:41Z",
                                    "classification": "bad_health_warning",
                                    "eligible": True,
                                    "good": False,
                                },
                                "prometheus_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-10T00:10:00Z",
                                    "eligible": True,
                                    "good": True,
                                },
                                "coverage_pct": 100.0,
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(
            {item["id"] for item in found},
            {"reliability:youtube_input_quality_fast_feedback"},
        )

    def test_fast_feedback_nodata_is_not_input_quality_incident(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_burn_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-11T13:19:00Z",
                        "fast_feedback": {
                            "youtube_input_quality": {
                                "measurement_status": "valid",
                                "measurement_unknown_reasons": [],
                                "source_disagreement_current": False,
                                "raw_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-11T13:18:16Z",
                                    "classification": "bad_health_nodata",
                                    "eligible": True,
                                    "good": False,
                                },
                                "prometheus_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-11T13:19:00Z",
                                    "eligible": True,
                                    "good": False,
                                },
                                "coverage_pct": 100.0,
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_454_400,
            )

        self.assertEqual(found, [])

    def test_fast_feedback_prometheus_only_bad_projection_is_not_current_incident(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "operational_reliability_burn_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-11T13:19:00Z",
                        "fast_feedback": {
                            "youtube_input_quality": {
                                "measurement_status": "valid",
                                "measurement_unknown_reasons": [],
                                "source_disagreement_current": False,
                                "raw_current": {"available": False},
                                "prometheus_current": {
                                    "available": True,
                                    "ts_utc": "2026-08-11T13:19:00Z",
                                    "eligible": True,
                                    "good": False,
                                },
                                "coverage_pct": 100.0,
                            }
                        },
                        "multi_window_burn_alerts": [],
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.operational_reliability_incidents(
                status_file=status_file,
                now_ts=1_786_454_400,
            )

        self.assertEqual(found, [])

    def test_external_blackbox_import_uses_fresh_multi_region_quorum_as_supporting_evidence(self) -> None:
        def payload(*values: bool) -> dict:
            return {
                "timeSeries": [
                    {
                        "metric": {"labels": {"checker_location": f"region-{index}"}},
                        "points": [
                            {
                                "interval": {"endTime": "2026-08-10T00:09:00Z"},
                                "value": {"boolValue": value},
                            }
                        ],
                    }
                    for index, value in enumerate(values)
                ]
            }

        result = blackbox_import.build_status(
            {
                "public_status": ("status.test", payload(True, True, True)),
                "youtube_public_live": ("youtube.test", payload(True, True, False)),
            },
            project="test-project",
            now_ts=1_786_320_600,
            stale_sec=600,
            minimum_locations=3,
            minimum_pass_ratio=2.0 / 3.0,
        )
        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["formal_sli"])
        self.assertEqual(result["execution_boundary"], "google_cloud_monitoring_public_uptime_checkers")
        self.assertEqual(result["targets"]["youtube_public_live"]["pass_ratio"], 0.666667)
        self.assertTrue(result["do_not_merge_with_internal_availability_sli"])

    def test_external_blackbox_import_returns_unknown_when_checker_coverage_is_insufficient(self) -> None:
        payload = {
            "timeSeries": [
                {
                    "metric": {"labels": {"checker_location": "region-1"}},
                    "points": [
                        {
                            "interval": {"endTime": "2026-08-10T00:09:00Z"},
                            "value": {"boolValue": True},
                        }
                    ],
                }
            ]
        }
        result = blackbox_import.build_status(
            {"public_status": ("status.test", payload)},
            project="test-project",
            now_ts=1_786_320_600,
            stale_sec=600,
            minimum_locations=3,
            minimum_pass_ratio=2.0 / 3.0,
        )

        self.assertEqual(result["status"], "unknown")
        self.assertEqual(
            result["targets"]["public_status"]["reason"],
            "minimum_fresh_checker_locations_not_met",
        )

    def test_external_blackbox_import_fails_when_external_quorum_fails(self) -> None:
        payload = {
            "timeSeries": [
                {
                    "metric": {"labels": {"checker_location": f"region-{index}"}},
                    "points": [
                        {
                            "interval": {"endTime": "2026-08-10T00:09:00Z"},
                            "value": {"boolValue": value},
                        }
                    ],
                }
                for index, value in enumerate((False, False, False))
            ]
        }
        result = blackbox_import.build_status(
            {"public_status": ("status.test", payload)},
            project="test-project",
            now_ts=1_786_320_600,
            stale_sec=600,
            minimum_locations=3,
            minimum_pass_ratio=2.0 / 3.0,
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["targets"]["public_status"]["passed_locations"], 0)

    def test_external_blackbox_import_keeps_regional_disagreement_unknown(self) -> None:
        payload = {
            "timeSeries": [
                {
                    "metric": {"labels": {"checker_location": f"region-{index}"}},
                    "points": [
                        {
                            "interval": {"endTime": "2026-08-10T00:09:00Z"},
                            "value": {"boolValue": value},
                        }
                    ],
                }
                for index, value in enumerate((False, False, True))
            ]
        }
        result = blackbox_import.build_status(
            {"youtube_public_live": ("youtube.test", payload)},
            project="test-project",
            now_ts=1_786_320_600,
            stale_sec=600,
            minimum_locations=3,
            minimum_pass_ratio=2.0 / 3.0,
        )

        self.assertEqual(result["status"], "unknown")
        self.assertEqual(
            result["targets"]["youtube_public_live"]["reason"],
            "external_checker_source_disagreement",
        )

    def test_external_blackbox_failure_is_warning_without_restart_authority(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "external_blackbox_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:09:00Z",
                        "status": "failed",
                        "reason": "one_or_more_external_targets_failed",
                        "targets": {
                            "public_status": {"status": "ok"},
                            "youtube_public_live": {"status": "failed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.external_blackbox_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["id"], "external:blackbox_failed")
        self.assertEqual(found[0]["severity"], "warning")
        self.assertNotIn("restart", found[0]["recovery_type"])


if __name__ == "__main__":
    unittest.main()
