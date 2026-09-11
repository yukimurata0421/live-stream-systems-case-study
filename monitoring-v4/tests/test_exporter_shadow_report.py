from __future__ import annotations

import json
import tempfile
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.runtime_evidence import VerifiedRolloutEvidence
from stream_contracts.monitoring_v4.sli import SLIProjection
from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.commands.shadow_report import (
    SOURCE_CADENCES,
    _parity_payload_valid,
    build_report,
)
from stream_monitoring_v4.commands import shadow_report
from stream_monitoring_v4.commands.exporter import _render_safely
from stream_monitoring_v4.compatibility.parity_convergence import (
    PARITY_CONVERGENCE_POLICY_REVISION,
)
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.exporter.metrics import LEGACY_COMPATIBILITY_METRICS, render_metrics
from stream_monitoring_v4.reporting.accumulator import ShadowReportAccumulator
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS


class ExporterAndShadowReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repository = MonitoringRepository(self.root / "monitoring.sqlite3")
        self.repository.initialize(applied_at=utc_text(BASE_TS))

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _accumulator(
        rows: list[dict],
        *,
        start_ts: int,
        end_ts: int,
    ) -> ShadowReportAccumulator:
        accumulator = ShadowReportAccumulator(
            coverage_start_ts=start_ts,
            parity_start_ts=start_ts,
            end_ts=end_ts,
        )
        for row in rows:
            accumulator.add(row)
        return accumulator

    def test_exporter_turns_metric_render_failure_into_explicit_unavailable(self) -> None:
        with patch(
            "stream_monitoring_v4.commands.exporter.render_metrics",
            side_effect=ValueError("corrupt persisted JSON"),
        ):
            ok, body = _render_safely(self.repository, "test-revision")
        self.assertFalse(ok)
        self.assertEqual(body, b"metrics unavailable\n")

    def test_retention_affected_row_counts_are_gauges_not_counters(self) -> None:
        output = render_metrics(
            self.repository,
            now_ts=BASE_TS,
            build_revision="test-revision",
        )
        self.assertIn(
            "# TYPE stream_v3_monitoring_v4_schema_rejections_retained gauge",
            output,
        )
        self.assertIn(
            "# TYPE stream_v3_monitoring_v4_shadow_cycles_retained gauge",
            output,
        )
        self.assertNotIn("stream_v3_monitoring_v4_schema_rejected_total", output)
        self.assertNotIn("stream_v3_monitoring_v4_shadow_cycles_total", output)

    def test_exporter_covers_every_contract_domain_and_distinguishes_absence(self) -> None:
        output = render_metrics(
            self.repository,
            now_ts=BASE_TS,
            build_revision="test-revision",
        )
        domains = {
            "youtube_lifecycle",
            "youtube_input_quality",
            "delivery",
            "rendering",
            "audio",
            "viewer_external",
            "monitoring_platform",
            "network_transport",
            "runtime_resource",
            "adsb_source",
            "api_quota",
            "recovery_policy",
            "notification_delivery",
            "control_loop",
        }
        for domain in domains:
            with self.subTest(domain=domain):
                labels = f'{{domain="{domain}"}}'
                self.assertIn(
                    f"stream_v3_monitoring_v4_current_snapshot_available{labels} 0",
                    output,
                )
                self.assertIn(
                    f'stream_v3_monitoring_v4_domain_state{{domain="{domain}",state="unknown"}} 1',
                    output,
                )
                self.assertIn(
                    f"stream_v3_monitoring_v4_current_snapshot_age_seconds{labels} 0",
                    output,
                )
        self.assertEqual(
            output.count("stream_v3_monitoring_v4_current_snapshot_available{"),
            len(domains),
        )

    def _observation(
        self,
        *,
        domain: str,
        source: str,
        role: str,
        payload: dict,
        status: str = "good",
    ) -> ObservationEnvelope:
        return ObservationEnvelope.create(
            domain=domain,
            source=source,
            source_event_id=f"safe-{domain}-{source}",
            source_generation="safe-generation",
            evidence_role=role,
            status=status,
            reason_code=f"safe_{status}",
            observed_at=utc_text(BASE_TS),
            received_at=utc_text(BASE_TS),
            freshness_limit_sec=900,
            producer_revision="test-r6",
            payload=payload,
        )

    def _seed(self) -> list[ObservationEnvelope]:
        items = [
            self._observation(
                domain="rendering",
                source="map_runtime",
                role="current_authoritative",
                payload={
                    "status": "healthy",
                    "delivery_critical_ok": True,
                    "weather_ok": True,
                    "conditions": {
                        "asset_identity": True,
                        "precipitation_data_ok": True,
                        "precipitation_generation_integrity": True,
                        "precipitation_render_applied": True,
                        "precipitation_validtime_match": True,
                        "semantic_visual_contract": True,
                    },
                },
            ),
            self._observation(
                domain="viewer_external",
                source="viewer_synthetic",
                role="supporting",
                payload={
                    "status": "healthy",
                    "frame_ok": True,
                    "black_detected": False,
                    "freeze_detected": False,
                    "consecutive_probe_failures": 0,
                    "consecutive_visual_failures": 0,
                },
            ),
            self._observation(
                domain="monitoring_platform",
                source="monitoring_self",
                role="current_authoritative",
                payload={"bad_count": 0, "repair_enabled": True},
            ),
            self._observation(
                domain="viewer_external",
                source="external_blackbox",
                role="supporting",
                payload={
                    "status": "ok",
                    "checked_at_utc": utc_text(BASE_TS),
                    "evidence_at_utc": utc_text(BASE_TS),
                },
            ),
            self._observation(
                domain="youtube_input_quality",
                source="youtube_input_quality_oauth",
                role="current_authoritative",
                payload={
                    "oauth_probe_ok": True,
                    "oauth_stream_status": "active",
                    "ingest_connected": True,
                    "oauth_stream_health_status": "good",
                    "oauth_stream_health_issue_details": [],
                },
            ),
        ]
        for item in items:
            self.repository.append_observation(item)
        CurrentReducerService(self.repository, DEFAULT_POLICIES).reduce(
            (
                "youtube_lifecycle",
                "youtube_input_quality",
                "delivery",
                "rendering",
                "audio",
                "viewer_external",
                "monitoring_platform",
            ),
            now_ts=BASE_TS,
        )
        projection = SLIProjection.create(
            objective_id="youtube_input_quality",
            window="rolling_7d",
            assessment_scope="formal",
            is_official_window=True,
            observed=None,
            eligible=None,
            bad=None,
            missing=None,
            coverage_pct=100,
            source_freshness_pct=100,
            source_disagreement=False,
            compliance_status="met",
            measurement_unknown_reasons=(),
            window_start=utc_text(BASE_TS - 7 * 86400),
            window_end=utc_text(BASE_TS),
            evaluated_at=utc_text(BASE_TS),
            policy_revision="test-r6",
            evidence_ids=(stable_id("evd", "safe"),),
            payload={"sli_pct": 100.0, "target_pct": 99.0},
        )
        self.repository.save_sli_projection(projection)
        return items

    def test_metrics_are_db_read_only_and_cover_only_declared_legacy_subset(self) -> None:
        self._seed()
        before = self.repository.path.read_bytes()
        output = render_metrics(
            self.repository,
            now_ts=BASE_TS + 30,
            build_revision="safe-revision",
        )
        self.assertIn("stream_v3_monitoring_v4_build_info", output)
        self.assertEqual(len(LEGACY_COMPATIBILITY_METRICS), 33)
        for name in LEGACY_COMPATIBILITY_METRICS:
            self.assertIn(name, output)
        self.assertIn('mode="credential_isolated_shadow"', output)
        self.assertIn("stream_v3_monitoring_v4_real_delivery_enabled 0", output)
        self.assertIn("stream_v3_monitoring_v4_runtime_mutation_enabled 0", output)
        self.assertIn("stream_v3_map_precipitation_data_ok 1", output)
        self.assertIn("stream_v3_map_precipitation_generation_integrity 1", output)
        self.assertIn("stream_v3_map_precipitation_render_applied 1", output)
        self.assertIn("stream_v3_map_precipitation_validtime_match 1", output)
        self.assertIn("stream_v3_map_semantic_visual_contract_ok 1", output)
        self.assertIn("stream_v3_map_asset_identity_ok 1", output)
        self.assertNotIn("safe-generation", output)
        self.assertNotIn("evd:", output)
        self.assertEqual(self.repository.path.read_bytes(), before)

    def test_future_observation_is_not_exported_as_fresh_or_good(self) -> None:
        future = ObservationEnvelope.create(
            domain="rendering",
            source="map_runtime",
            source_event_id="future-map",
            source_generation="future-map-generation",
            evidence_role="current_authoritative",
            status="good",
            reason_code="future_map_good",
            observed_at=utc_text(BASE_TS + 60),
            received_at=utc_text(BASE_TS + 60),
            freshness_limit_sec=180,
            producer_revision="test-future",
            payload={
                "status": "healthy",
                "delivery_critical_ok": True,
                "weather_ok": True,
            },
        )
        self.repository.append_observation(future)
        output = render_metrics(
            self.repository,
            now_ts=BASE_TS,
            build_revision="safe-revision",
        )
        self.assertIn("stream_v3_map_monitor_sample_available 0", output)
        self.assertIn("stream_v3_map_monitor_delivery_critical_ok 0", output)
        self.assertIn('stream_v3_map_monitor_status{status="unknown"} 1', output)

    def test_shadow_report_is_revision_pinned_and_time_gates_stay_open(self) -> None:
        items = self._seed()
        events = [
            {
                "source": item.source,
                "observation_id": item.observation_id,
                "observed_at": item.observed_at,
            }
            for item in items
        ]
        observer = {
            "source_results": [
                {
                    "source": "youtube_state_files",
                    "outcome": "observed",
                    "observations": len(events),
                    "rejections": 0,
                    "observation_events": events,
                }
            ]
        }
        parity = {
            "equivalent": True,
            "accepted_difference_count": 0,
            "unclassified_contract_difference_count": 0,
        }
        self.repository.append_shadow_cycle(
            cycle_id=stable_id("cyc", "current"),
            started_at=utc_text(BASE_TS),
            completed_at=utc_text(BASE_TS),
            build_revision="a" * 40,
            source_revision="source-current",
            observer=observer,
            current_states={"rendering": "good"},
            parity=parity,
            notification_intent_count=0,
        )
        self.repository.append_shadow_cycle(
            cycle_id=stable_id("cyc", "old"),
            started_at=utc_text(BASE_TS - 14 * 86400),
            completed_at=utc_text(BASE_TS - 14 * 86400),
            build_revision="b" * 40,
            source_revision="source-current",
            observer=observer,
            current_states={"rendering": "good"},
            parity=parity,
            notification_intent_count=0,
        )
        self.repository.append_shadow_cycle(
            cycle_id=stable_id("cyc", "old-source"),
            started_at=utc_text(BASE_TS - 60),
            completed_at=utc_text(BASE_TS - 60),
            build_revision="a" * 40,
            source_revision="source-old",
            observer=observer,
            current_states={"rendering": "good"},
            parity=parity,
            notification_intent_count=0,
        )
        report = build_report(
            self.repository,
            build_revision="a" * 40,
            source_revision="source-current",
            now_ts=BASE_TS + 60,
        )
        self.assertEqual(report["schema"], "monitoring_v4.shadow_evidence_report.v4")
        self.assertTrue(report["build_revision_immutable"])
        self.assertTrue(report["source_revision_known"])
        self.assertEqual(report["source_revision"], "source-current")
        self.assertEqual(report["parity_14d"]["cycles"], 1)
        self.assertFalse(report["coverage_7d"]["elapsed"])
        self.assertFalse(report["coverage_7d"]["gate_met"])
        self.assertFalse(report["parity_14d"]["elapsed"])
        self.assertFalse(report["parity_14d"]["gate_met"])
        self.assertTrue(report["revision_window"]["revision_change_resets_time_gates"])
        self.assertTrue(
            report["revision_window"]["source_revision_change_resets_time_gates"]
        )
        self.assertFalse(report["safety_boundary"]["real_delivery_enabled"])

        unknown_source = build_report(
            self.repository,
            build_revision="a" * 40,
            source_revision="unknown-source-revision",
            now_ts=BASE_TS + 60,
        )
        self.assertFalse(unknown_source["source_revision_known"])
        self.assertFalse(unknown_source["coverage_7d"]["gate_met"])
        self.assertFalse(unknown_source["parity_14d"]["gate_met"])

    def test_source_coverage_cadences_match_deployed_producer_and_freshness_boundaries(self) -> None:
        expected = {
            "youtube_watchdog": 180,
            "youtube_input_quality_oauth": 180,
            "runtime_delivery_watchdog": 120,
            "youtube_video_resolver": 60,
            "map_runtime": 120,
            "legacy_subsystem_audio": 120,
            "viewer_synthetic": 360,
            "external_blackbox": 360,
            "monitoring_self": 120,
            "youtube_api_direct_lifecycle": 180,
            "youtube_input_quality_api_direct": 180,
        }
        self.assertEqual(
            {source: policy.cadence_sec for source, policy in SOURCE_CADENCES.items()},
            expected,
        )
        self.assertTrue(all(policy.basis.strip() for policy in SOURCE_CADENCES.values()))

        self._seed()
        report = build_report(
            self.repository,
            build_revision="missing-revision",
            source_revision="source-current",
            now_ts=BASE_TS + 60,
        )
        self.assertFalse(report["build_revision_immutable"])
        self.assertFalse(report["coverage_7d"]["gate_met"])
        self.assertFalse(report["parity_14d"]["gate_met"])
        policies = {
            item["source"]: (item["cadence_sec"], item["cadence_basis"])
            for item in report["coverage_7d"]["journal_sources"]
        }
        self.assertEqual({source: value[0] for source, value in policies.items()}, expected)
        self.assertTrue(all(value[1] for value in policies.values()))

    def test_source_coverage_uses_cycle_receipt_and_keeps_event_gap_diagnostic(self) -> None:
        rows = []
        observed_offsets = (0, 61, 122, 181)
        for index, observed_offset in enumerate(observed_offsets):
            cycle_ts = BASE_TS + index * 60
            observed_ts = BASE_TS + observed_offset
            rows.append(
                {
                    "started_ts": cycle_ts,
                    "observer": {
                        "source_results": [
                            {
                                "source": "youtube_state_files",
                                "outcome": "observed",
                                "observation_events": [
                                    {
                                        "source": "youtube_video_resolver",
                                        "observation_id": stable_id("obs", index),
                                        "observed_at": utc_text(observed_ts),
                                        "received_at": utc_text(max(cycle_ts, observed_ts)),
                                        "freshness_limit_sec": 90,
                                    }
                                ],
                            }
                        ]
                    },
                    "parity": {},
                }
            )
        accumulator = self._accumulator(
            rows,
            start_ts=BASE_TS,
            end_ts=BASE_TS + 240,
        )
        coverage = {
            item["source"]: item
            for item in accumulator.source_cycle_coverage()
        }
        resolver = coverage["youtube_video_resolver"]
        self.assertEqual(resolver["expected"], 4)
        self.assertEqual(resolver["observed"], 4)
        self.assertEqual(resolver["coverage_pct"], 100.0)
        self.assertEqual(resolver["fresh_coverage_pct"], 100.0)

        cadence = {
            item["source"]: item
            for item in accumulator.production_cadence()
        }
        self.assertEqual(cadence["youtube_video_resolver"]["max_gap_sec"], 61)
        self.assertEqual(cadence["youtube_video_resolver"]["gaps_over_declared_cadence"], 2)
        self.assertIn("excluded from the coverage gate", cadence["youtube_video_resolver"]["basis"])

    def test_partial_adapter_cycle_does_not_satisfy_collector_coverage(self) -> None:
        rows = [
            {
                "started_ts": BASE_TS,
                "observer": {
                    "source_results": [
                        {
                            "source": "youtube_state_files",
                            "outcome": "partial",
                            "observations": 1,
                            "rejections": 1,
                        }
                    ]
                },
                "parity": {},
            }
        ]
        accumulator = self._accumulator(
            rows,
            start_ts=BASE_TS,
            end_ts=BASE_TS + 60,
        )
        coverage = {
            item["source"]: item
            for item in accumulator.collector_coverage()
        }
        youtube = coverage["youtube_state_files"]
        self.assertEqual(youtube["expected"], 1)
        self.assertEqual(youtube["attempted"], 1)
        self.assertEqual(youtube["observed"], 0)
        self.assertEqual(youtube["coverage_pct"], 0.0)

    def test_report_command_closes_owned_repository_on_success_and_failure(self) -> None:
        class CloseTrackingRepository:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        arguments = [
            "--db",
            str(self.root / "unused.sqlite3"),
            "--build-revision",
            "a" * 40,
            "--source-revision",
            "source-current",
            "--now-ts",
            str(BASE_TS),
        ]
        for failure in (False, True):
            with self.subTest(failure=failure):
                repository = CloseTrackingRepository()
                outcome = RuntimeError("report failure") if failure else {"schema": "test"}
                with patch.object(
                    shadow_report,
                    "repository_from_args",
                    return_value=repository,
                ), patch.object(
                    shadow_report,
                    "build_report",
                    side_effect=outcome if failure else None,
                    return_value=None if failure else outcome,
                ), patch("builtins.print"):
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, "report failure"):
                            shadow_report.main(arguments)
                    else:
                        self.assertEqual(shadow_report.main(arguments), 0)
                self.assertTrue(repository.closed)

    def test_report_file_is_group_read_only_for_capability_free_sentinel(self) -> None:
        output = self.root / "report" / "shadow-evidence-report.json"
        shadow_report._write_atomic(output, {"schema": "test"})
        self.assertEqual(output.stat().st_mode & 0o777, 0o640)
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), {"schema": "test"})

    def test_report_read_model_filters_in_sql_without_wide_sort(self) -> None:
        parity = {
            "equivalent": True,
            "accepted_difference_count": 0,
            "unclassified_contract_difference_count": 0,
        }
        self.repository.append_shadow_cycle(
            cycle_id=stable_id("cyc", "narrow-report"),
            started_at=utc_text(BASE_TS),
            completed_at=utc_text(BASE_TS),
            build_revision="a" * 40,
            source_revision="source-current",
            observer={"source_results": []},
            current_states={"delivery": "good"},
            parity=parity,
            notification_intent_count=0,
        )
        rows = self.repository.shadow_cycle_report_rows(
            build_revision="a" * 40,
            source_revision="source-current",
            start_ts=BASE_TS - 1,
            end_ts=BASE_TS + 1,
        )
        self.assertEqual(set(rows[0]), {"started_ts", "observer", "parity"})
        with self.repository.connection(read_only=True) as connection:
            plan = connection.execute(
                """EXPLAIN QUERY PLAN
                SELECT started_ts, observer_json, parity_json FROM shadow_cycles
                WHERE build_revision=? AND source_revision=?
                  AND started_ts>=? AND started_ts<?""",
                ("a" * 40, "source-current", BASE_TS - 1, BASE_TS + 1),
            ).fetchall()
        self.assertNotIn("USE TEMP B-TREE", " ".join(str(row["detail"]) for row in plan))

    def test_fourteen_day_report_reduces_rows_without_retaining_json_payloads(self) -> None:
        class StreamingRepository:
            def iter_shadow_cycle_report_rows(self, **kwargs):
                del kwargs
                for index in range(14 * 24 * 60):
                    yield {
                        "started_ts": BASE_TS + index * 60,
                        "observer": {"unused_large_payload": "x" * 16384},
                        "parity": {
                            "equivalent": True,
                            "accepted_difference_count": 0,
                            "unclassified_contract_difference_count": 0,
                        },
                    }

            def shadow_cycle_report_rows(self, **kwargs):
                del kwargs
                raise AssertionError("build_report must use the bounded iterator")

        now_ts = BASE_TS + 14 * 86400
        tracemalloc.start()
        try:
            report = build_report(
                StreamingRepository(),
                build_revision="a" * 40,
                source_revision="source-current",
                now_ts=now_ts,
            )
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        self.assertEqual(report["parity_14d"]["cycles"], 14 * 24 * 60)
        self.assertLess(peak, 32 * 1024 * 1024)

    def test_duplicate_rows_cannot_dilute_a_bad_parity_cycle(self) -> None:
        projection = {
            "complete": True,
            "expected_count": 6,
            "projection_count": 6,
            "rejection_count": 0,
        }

        def parity(*, equivalent: bool) -> dict:
            return {
                "schema": "monitoring_v4.live_parity.v2",
                "equivalent": equivalent,
                "accepted_difference_count": 0,
                "unclassified_contract_difference_count": 0 if equivalent else 1,
                "verified_rollouts": [],
                "projection_integrity": projection,
                "domains": {
                    "delivery": {
                        "match": equivalent,
                        "classification": "equivalent" if equivalent else "contract_mismatch",
                        "actual_observed_at": utc_text(BASE_TS),
                    }
                },
            }

        class DuplicateCycleRepository:
            def iter_shadow_cycle_report_rows(self, **kwargs):
                del kwargs
                yield {"started_ts": BASE_TS, "observer": {}, "parity": parity(equivalent=True)}
                yield {"started_ts": BASE_TS, "observer": {}, "parity": parity(equivalent=False)}
                yield {
                    "started_ts": BASE_TS + 60,
                    "observer": {},
                    "parity": parity(equivalent=True),
                }

        report = build_report(
            DuplicateCycleRepository(),  # type: ignore[arg-type]
            build_revision="a" * 40,
            source_revision="source-current",
            now_ts=BASE_TS + 120,
        )
        parity_report = report["parity_14d"]
        self.assertEqual(parity_report["raw_rows"], 3)
        self.assertEqual(parity_report["cycles"], 2)
        self.assertEqual(parity_report["equivalent_cycles"], 1)
        self.assertEqual(parity_report["equivalent_cycle_pct"], 50.0)
        self.assertEqual(parity_report["unclassified_contract_difference_count"], 1)

    def test_parity_payload_rejects_numeric_and_classification_coercion(self) -> None:
        payload = {
            "schema": "monitoring_v4.live_parity.v2",
            "equivalent": True,
            "accepted_difference_count": 0,
            "unclassified_contract_difference_count": 0,
            "domains": {
                "delivery": {"match": True, "classification": "equivalent"}
            },
        }
        self.assertTrue(_parity_payload_valid(payload))
        for field, replacement in (
            ("accepted_difference_count", "0"),
            ("unclassified_contract_difference_count", False),
        ):
            malformed = {**payload, field: replacement}
            with self.subTest(field=field):
                self.assertFalse(_parity_payload_valid(malformed))
        malformed = {
            **payload,
            "domains": {"delivery": {"match": True, "classification": ["equivalent"]}},
        }
        self.assertFalse(_parity_payload_valid(malformed))
        self_authorized = {
            **payload,
            "equivalent": False,
            "accepted_difference_count": 1,
            "domains": {
                "delivery": {
                    "match": False,
                    "classification": "accepted_source_snapshot_skew",
                }
            },
        }
        self.assertFalse(_parity_payload_valid(self_authorized))

    def test_report_reconciles_only_later_versioned_rollout_evidence(self) -> None:
        evidence = VerifiedRolloutEvidence.create(
            rollout_id="rollout-delayed-proof",
            planned_at=utc_text(BASE_TS),
            expires_at=utc_text(BASE_TS + 900),
            pod_started_at=utc_text(BASE_TS + 90),
            pod_uid="pod-delayed-proof",
            reason="planned_runtime_rollout",
        )
        mismatch = {
            "started_ts": BASE_TS + 60,
            "observer": {},
            "parity": {
                "schema": "monitoring_v4.live_parity.v2",
                "equivalent": False,
                "accepted_difference_count": 0,
                "unclassified_contract_difference_count": 1,
                "verified_rollouts": [],
                "domains": {
                    "delivery": {
                        "match": False,
                        "classification": "candidate_verified_planned_rollout_convergence",
                        "expected_state": "good",
                        "actual_state": "bad",
                        "expected_source": "subsystems_status.local_delivery",
                        "expected_observed_at": utc_text(BASE_TS + 50),
                        "actual_observed_at": utc_text(BASE_TS + 60),
                        "actual_sources": ["runtime_delivery_watchdog"],
                        "normalization_error": "",
                        "rollout_evidence_id": evidence.evidence_id,
                    }
                },
            },
        }
        later_proof = {
            "started_ts": BASE_TS + 120,
            "observer": {},
            "parity": {
                "schema": "monitoring_v4.live_parity.v2",
                "equivalent": True,
                "accepted_difference_count": 0,
                "unclassified_contract_difference_count": 0,
                "verified_rollouts": [evidence.to_dict()],
                "domains": {
                    "delivery": {
                        "match": True,
                        "classification": "equivalent",
                        "expected_state": "good",
                        "actual_state": "good",
                        "expected_source": "subsystems_status.local_delivery",
                        "expected_observed_at": utc_text(BASE_TS + 110),
                        "actual_observed_at": utc_text(BASE_TS + 120),
                        "actual_sources": ["runtime_delivery_watchdog"],
                        "normalization_error": "",
                    }
                },
            },
        }
        before = self._accumulator(
            [mismatch],
            start_ts=BASE_TS,
            end_ts=BASE_TS + 180,
        ).parity_totals()
        after = self._accumulator(
            [mismatch, later_proof],
            start_ts=BASE_TS,
            end_ts=BASE_TS + 180,
        ).parity_totals()

        self.assertEqual(before["violations"], 1)
        self.assertEqual(after["violations"], 0)
        self.assertEqual(after["accepted"], 1)
        self.assertEqual(after["retrospectively_verified"], 1)
        self.assertEqual(after["verified_rollout_evidence"], 1)

        malformed = {
            **later_proof,
            "parity": {
                **later_proof["parity"],
                "verified_rollouts": [{**evidence.to_dict(), "verification": "unverified"}],
            },
        }
        malformed_totals = self._accumulator(
            [mismatch, malformed],
            start_ts=BASE_TS,
            end_ts=BASE_TS + 180,
        ).parity_totals()
        self.assertEqual(malformed_totals["violations"], 1)

        with self.assertRaisesRegex(ValueError, "does not match"):
            VerifiedRolloutEvidence.from_dict(
                {
                    **evidence.to_dict(),
                    "expires_at": utc_text(BASE_TS + 1200),
                }
            )

        forged = {
            **evidence.to_dict(),
            "reason": "different_planned_reason",
        }
        conflicting = {
            **later_proof,
            "parity": {
                **later_proof["parity"],
                "verified_rollouts": [evidence.to_dict(), forged],
            },
        }
        conflicting_totals = self._accumulator(
            [mismatch, conflicting],
            start_ts=BASE_TS,
            end_ts=BASE_TS + 180,
        ).parity_totals()
        self.assertEqual(conflicting_totals["violations"], 1)
        self.assertEqual(conflicting_totals["accepted"], 0)
        self.assertEqual(conflicting_totals["conflicting_rollout_evidence"], 1)

    def test_snapshot_convergence_rejects_invalid_wrong_source_outside_and_late_proof(
        self,
    ) -> None:
        candidate = {
            "started_ts": BASE_TS + 60,
            "observer": {},
            "parity": {
                "schema": "monitoring_v4.live_parity.v2",
                "parity_policy_revision": PARITY_CONVERGENCE_POLICY_REVISION,
                "equivalent": False,
                "accepted_difference_count": 0,
                "unclassified_contract_difference_count": 1,
                "verified_rollouts": [],
                "domains": {
                    "youtube_input_quality": {
                        "match": False,
                        "classification": "candidate_source_snapshot_skew_convergence",
                        "expected_state": "good",
                        "actual_state": "bad",
                        "expected_source": "operational_reliability_burn_status.raw_current",
                        "expected_observed_at": utc_text(BASE_TS - 60),
                        "actual_observed_at": utc_text(BASE_TS),
                        "actual_sources": ["youtube_input_quality_oauth"],
                        "snapshot_skew_sec": -60,
                        "normalization_error": "",
                        "convergence_policy_revision": PARITY_CONVERGENCE_POLICY_REVISION,
                        "maximum_absolute_skew_sec": 600,
                        "convergence_timeout_sec": 600,
                    }
                },
            },
        }

        def proof(
            *,
            started_ts: int = BASE_TS + 120,
            expected_ts: int = BASE_TS + 1,
            source: str = "youtube_input_quality_oauth",
        ) -> dict:
            return {
                "started_ts": started_ts,
                "observer": {},
                "parity": {
                    "schema": "monitoring_v4.live_parity.v2",
                    "equivalent": True,
                    "accepted_difference_count": 0,
                    "unclassified_contract_difference_count": 0,
                    "verified_rollouts": [],
                    "domains": {
                        "youtube_input_quality": {
                            "match": True,
                            "classification": "equivalent",
                            "expected_state": "good",
                            "actual_state": "good",
                            "expected_source": "operational_reliability_burn_status.raw_current",
                            "expected_observed_at": utc_text(expected_ts),
                            "actual_observed_at": utc_text(expected_ts + 1),
                            "actual_sources": [source],
                            "normalization_error": "",
                        }
                    },
                },
            }

        invalid = proof()
        invalid["parity"] = {
            **invalid["parity"],
            "accepted_difference_count": "0",
        }
        cases = {
            "invalid_payload": ([candidate, invalid], BASE_TS + 180),
            "wrong_source": (
                [candidate, proof(source="youtube_input_quality_api_direct")],
                BASE_TS + 180,
            ),
            "lagging_view_not_past_ahead_timestamp": (
                [candidate, proof(expected_ts=BASE_TS)],
                BASE_TS + 180,
            ),
            "proof_after_policy_deadline": (
                [
                    candidate,
                    proof(started_ts=BASE_TS + 700, expected_ts=BASE_TS + 601),
                ],
                BASE_TS + 800,
            ),
            "proof_outside_report_window": (
                [candidate, proof(started_ts=BASE_TS + 180)],
                BASE_TS + 180,
            ),
        }
        for name, (rows, end_ts) in cases.items():
            with self.subTest(name=name):
                totals = self._accumulator(
                    rows,
                    start_ts=BASE_TS,
                    end_ts=end_ts,
                ).parity_totals()
                self.assertEqual(totals["accepted"], 0)
                self.assertEqual(totals["violations"], 1)

        valid_totals = self._accumulator(
            [candidate, proof()],
            start_ts=BASE_TS,
            end_ts=BASE_TS + 180,
        ).parity_totals()
        self.assertEqual(valid_totals["accepted"], 1)
        self.assertEqual(valid_totals["violations"], 0)
        self.assertEqual(valid_totals["retrospectively_verified_snapshot"], 1)


if __name__ == "__main__":
    unittest.main()
