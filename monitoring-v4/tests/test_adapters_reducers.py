from __future__ import annotations

import json
import tempfile
import time
import multiprocessing
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.observation import ObservationRejection
from stream_contracts.monitoring_v4.time import unix_ts, utc_text
from stream_contracts.monitoring_v4.runtime_evidence import (
    RuntimeRolloutProjection,
    VerifiedRolloutEvidence,
)
from stream_monitoring_v4.adapters.base import AdapterBatch
from stream_monitoring_v4.adapters.youtube import (
    YouTubeStateAdapter,
    resolver_status,
    watchdog_delivery_status,
    watchdog_input_quality_status,
    watchdog_lifecycle_status,
)
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.compatibility.current_diff import compare_current_projection
from stream_monitoring_v4.compatibility.legacy_current import live_parity_report
from stream_monitoring_v4.domains.reducer import reduce_domain
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.reporting.parity import ParityAccumulator
from stream_monitoring_v4.runtime.observer import Observer
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, current, observation, write_json


class YouTubeAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.watchdog = self.root / "youtube_watchdog_stats.json"
        self.resolver = self.root / "youtube_video_id_resolver_state.json"
        self.received_at = utc_text(BASE_TS)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_healthy(self) -> None:
        write_json(
            self.watchdog,
            {
                "stats_file_updated_at_utc": self.received_at,
                "remote_probe_ts_utc": self.received_at,
                "oauth_checked_ts_utc": self.received_at,
                "remote_sample_id": "rps-sanitized",
                "status": "ok",
                "healthy": True,
                "api_live_state": "live",
                "oauth_life_cycle_status": "live",
                "oauth_stream_status": "active",
                "oauth_probe_ok": True,
                "oauth_stream_health_status": "good",
                "oauth_stream_health_issues": [],
                "stream_active": True,
                "ingest_connected": True,
                "local_ok": True,
                "ffmpeg_generation": "ffmpeg_pid=123",
                "video_id": "video-current",
                "expected_video_id": "video-current",
                "access_token": "must-not-leave-adapter",
                "ingest_connection": "rtmps://secret-like-value",
            },
        )
        write_json(
            self.resolver,
            {
                "ts_utc": self.received_at,
                "video_id": "video-current",
                "expected_video_id": "video-current",
                "url_preservation_active": True,
                "oauth_token": "must-not-leave-adapter",
            },
        )

    def test_reads_four_sanitized_observations_without_modifying_sources(self) -> None:
        self._write_healthy()
        before = {
            path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in (self.watchdog, self.resolver)
        }
        batch = YouTubeStateAdapter(
            watchdog_stats_file=self.watchdog,
            resolver_state_file=self.resolver,
        ).collect(received_at=self.received_at)
        self.assertEqual(len(batch.observations), 4)
        self.assertEqual(batch.rejections, ())
        states = {(item.domain, item.source): item.status for item in batch.observations}
        self.assertEqual(states[("youtube_lifecycle", "youtube_watchdog")], "good")
        self.assertEqual(states[("youtube_input_quality", "youtube_input_quality_oauth")], "good")
        self.assertEqual(states[("delivery", "runtime_delivery_watchdog")], "good")
        self.assertEqual(states[("youtube_lifecycle", "youtube_video_resolver")], "good")
        rendered = repr([dict(item.payload) for item in batch.observations])
        self.assertNotIn("access_token", rendered)
        self.assertNotIn("ingest_connection", rendered)
        self.assertNotIn("oauth_token", rendered)
        for path, (content, mtime_ns) in before.items():
            self.assertEqual(path.read_bytes(), content)
            self.assertEqual(path.stat().st_mtime_ns, mtime_ns)

    def test_missing_source_timestamp_is_rejected_not_filled_with_now(self) -> None:
        write_json(self.watchdog, {"api_live_state": "live", "stream_active": True})
        write_json(self.resolver, {"video_id": "video-current"})
        batch = YouTubeStateAdapter(
            watchdog_stats_file=self.watchdog,
            resolver_state_file=self.resolver,
        ).collect(received_at=self.received_at)
        self.assertEqual(batch.observations, ())
        self.assertEqual(len(batch.rejections), 4)
        self.assertEqual({item.reason_code for item in batch.rejections}, {"source_timestamp_invalid"})

    def test_any_future_timestamp_is_rejected(self) -> None:
        future = utc_text(BASE_TS + 1)
        write_json(
            self.watchdog,
            {
                "remote_probe_ts_utc": future,
                "oauth_checked_ts_utc": future,
                "stats_file_updated_at_utc": future,
                "api_live_state": "live",
                "stream_active": True,
                "ingest_connected": True,
                "local_ok": True,
            },
        )
        write_json(self.resolver, {"ts_utc": future, "video_id": "video-current"})
        batch = YouTubeStateAdapter(
            watchdog_stats_file=self.watchdog,
            resolver_state_file=self.resolver,
        ).collect(received_at=self.received_at)
        self.assertEqual(batch.observations, ())
        self.assertEqual(len(batch.rejections), 4)
        self.assertEqual({item.reason_code for item in batch.rejections}, {"source_timestamp_future"})

    def test_local_pipeline_failure_does_not_reclassify_remote_lifecycle(self) -> None:
        payload = {
            "failure_kind": "local_pipeline",
            "api_live_state": "live",
            "oauth_life_cycle_status": "live",
            "stream_active": False,
            "ingest_connected": False,
            "local_ok": False,
        }
        self.assertEqual(watchdog_lifecycle_status(payload), "good")
        self.assertEqual(watchdog_delivery_status(payload), "bad")
        self.assertEqual(
            watchdog_input_quality_status(
                {
                    "oauth_probe_ok": True,
                    "oauth_stream_status": "active",
                    "oauth_stream_health_status": "good",
                    "ingest_connected": False,
                }
            ),
            ("not_applicable", "input_quality_local_ingest_disconnected"),
        )

    def test_input_quality_matches_current_oauth_eligibility(self) -> None:
        base = {
            "oauth_probe_ok": True,
            "oauth_stream_status": "active",
            "ingest_connected": True,
        }
        self.assertEqual(
            watchdog_input_quality_status({**base, "oauth_stream_health_status": "good"}),
            ("good", "input_quality_current_good"),
        )
        self.assertEqual(
            watchdog_input_quality_status({**base, "oauth_stream_health_status": "ok"}),
            ("bad", "input_quality_health_warning"),
        )
        self.assertEqual(
            watchdog_input_quality_status({**base, "oauth_stream_health_status": "bad"}),
            ("bad", "input_quality_health_error"),
        )
        self.assertEqual(
            watchdog_input_quality_status({**base, "oauth_stream_health_status": "noData"}),
            ("unknown", "input_quality_health_nodata"),
        )
        self.assertEqual(
            watchdog_input_quality_status({**base, "oauth_stream_health_status": "unexpected"}),
            ("unknown", "input_quality_health_unknown"),
        )
        self.assertEqual(
            watchdog_input_quality_status(
                {
                    **base,
                    "oauth_stream_health_status": "good",
                    "oauth_stream_health_issue_details": [
                        {"type": "configuration", "severity": "warning", "secret": "discard"}
                    ],
                }
            ),
            ("bad", "input_quality_configuration_issue"),
        )

    def test_remote_ended_and_same_url_resolver_are_separate_evidence(self) -> None:
        self.assertEqual(watchdog_lifecycle_status({"failure_kind": "remote_ended"}), "bad")
        self.assertEqual(
            resolver_status(
                {
                    "video_id": "same-url-video",
                    "expected_video_id": "same-url-video",
                    "url_preservation_active": True,
                }
            ),
            "good",
        )
        self.assertEqual(
            resolver_status({"video_id": 123, "expected_video_id": 123}),
            "unknown",
        )
        self.assertEqual(
            watchdog_lifecycle_status({"failure_kind": ["remote_ended"]}),
            "unknown",
        )


class ReducerTests(unittest.TestCase):
    def test_latest_event_time_wins_even_if_input_is_out_of_order(self) -> None:
        older_bad = observation(status="bad", observed_ts=BASE_TS, event="older")
        newer_good = observation(status="good", observed_ts=BASE_TS + 30, event="newer")
        current = reduce_domain(
            [newer_good, older_bad],
            DEFAULT_POLICIES["delivery"],
            now_ts=BASE_TS + 30,
        )
        self.assertEqual(current.state, "good")
        self.assertEqual(current.source_observation_ids, (newer_good.observation_id,))

    def test_stale_source_becomes_unknown(self) -> None:
        item = observation(status="good", observed_ts=BASE_TS)
        current = reduce_domain([item], DEFAULT_POLICIES["delivery"], now_ts=BASE_TS + 181)
        self.assertEqual(current.state, "unknown")
        self.assertEqual(current.reason_codes, ("missing_current_evidence",))
        self.assertEqual(current.payload["ignored"], [{"reason": "stale", "source": item.source}])

    def test_supporting_or_historical_evidence_cannot_override_current(self) -> None:
        authoritative = observation(status="good", observed_ts=BASE_TS)
        historical = observation(
            source="rtmps_tcp",
            status="bad",
            observed_ts=BASE_TS,
            role="historical",
            event="rolling-low",
        )
        current = reduce_domain(
            [historical, authoritative],
            DEFAULT_POLICIES["delivery"],
            now_ts=BASE_TS,
        )
        self.assertEqual(current.state, "good")
        self.assertIn({"source": "rtmps_tcp", "reason": "role_not_current"}, current.payload["ignored"])

    def test_input_quality_rolling_low_cannot_reopen_current_good(self) -> None:
        current_good = observation(
            domain="youtube_input_quality",
            source="youtube_input_quality_oauth",
            status="good",
            observed_ts=BASE_TS,
            event="oauth-current-good",
        )
        rolling_low = observation(
            domain="youtube_input_quality",
            source="youtube_input_quality_rolling",
            status="bad",
            observed_ts=BASE_TS,
            role="historical",
            event="one-hour-low",
            payload={"sli_pct": 89.387},
        )
        reduced = reduce_domain(
            [rolling_low, current_good],
            DEFAULT_POLICIES["youtube_input_quality"],
            now_ts=BASE_TS,
        )
        self.assertEqual(reduced.state, "good")
        self.assertEqual(reduced.source_observation_ids, (current_good.observation_id,))

    def test_input_quality_same_producer_projection_is_diagnostic_only(self) -> None:
        cases = (("good", "bad"), ("bad", "good"))
        for raw_status, prometheus_status in cases:
            with self.subTest(raw=raw_status, prometheus=prometheus_status):
                raw = observation(
                    domain="youtube_input_quality",
                    source="youtube_input_quality_oauth",
                    status=raw_status,
                    observed_ts=BASE_TS,
                    event=f"raw-{raw_status}",
                )
                projection = observation(
                    domain="youtube_input_quality",
                    source="youtube_input_quality_prometheus",
                    status=prometheus_status,
                    observed_ts=BASE_TS,
                    role="current_correlated",
                    event=f"prometheus-{prometheus_status}",
                )
                reduced = reduce_domain(
                    [projection, raw],
                    DEFAULT_POLICIES["youtube_input_quality"],
                    now_ts=BASE_TS,
                )
                self.assertEqual(reduced.state, raw_status)
                self.assertEqual(reduced.source_observation_ids, (raw.observation_id,))
                self.assertTrue(reduced.payload["same_producer_projection_disagreement"])
                self.assertEqual(
                    [item["source"] for item in reduced.payload["diagnostics"]],
                    ["youtube_input_quality_prometheus"],
                )
                self.assertIn(
                    {
                        "source": "youtube_input_quality_prometheus",
                        "reason": "projection_not_current_authority",
                    },
                    reduced.payload["ignored"],
                )

    def test_input_quality_prometheus_only_bad_cannot_define_current(self) -> None:
        projection = observation(
            domain="youtube_input_quality",
            source="youtube_input_quality_prometheus",
            status="bad",
            observed_ts=BASE_TS,
            role="current_correlated",
            event="prometheus-only-bad",
        )
        reduced = reduce_domain(
            [projection],
            DEFAULT_POLICIES["youtube_input_quality"],
            now_ts=BASE_TS,
        )
        self.assertEqual(reduced.state, "unknown")
        self.assertEqual(reduced.reason_codes, ("missing_current_evidence",))
        self.assertEqual(reduced.source_observation_ids, ())
        self.assertEqual(len(reduced.payload["diagnostics"]), 1)

    def test_equal_priority_current_disagreement_is_unknown(self) -> None:
        watchdog = observation(status="good", observed_ts=BASE_TS, event="watchdog")
        tcp = observation(
            source="rtmps_tcp",
            status="bad",
            observed_ts=BASE_TS,
            event="tcp",
        )
        current = reduce_domain([watchdog, tcp], DEFAULT_POLICIES["delivery"], now_ts=BASE_TS)
        self.assertEqual(current.state, "unknown")
        self.assertEqual(current.reason_codes, ("source_disagreement",))

    def test_not_applicable_observation_does_not_become_current_good(self) -> None:
        item = observation(status="not_applicable", observed_ts=BASE_TS, event="na")
        current = reduce_domain([item], DEFAULT_POLICIES["delivery"], now_ts=BASE_TS)
        self.assertEqual(current.state, "unknown")
        self.assertEqual(current.reason_codes, ("current_evidence_not_applicable",))

    def test_planned_rollout_context_is_preserved_without_forcing_good(self) -> None:
        item = observation(
            status="bad",
            observed_ts=BASE_TS,
            event="planned-bad",
            payload={"planned_rollout": True},
        )
        reduced = reduce_domain([item], DEFAULT_POLICIES["delivery"], now_ts=BASE_TS)
        self.assertEqual(reduced.state, "bad")
        self.assertTrue(reduced.payload["planned_rollout"])

    def test_reduction_is_semantically_deterministic(self) -> None:
        items = [
            observation(status="good", observed_ts=BASE_TS, event="a"),
            observation(source="rtmps_tcp", status="good", observed_ts=BASE_TS, event="b"),
        ]
        first = reduce_domain(items, DEFAULT_POLICIES["delivery"], now_ts=BASE_TS)
        second = reduce_domain(list(reversed(items)), DEFAULT_POLICIES["delivery"], now_ts=BASE_TS)
        self.assertEqual(first.to_dict(), second.to_dict())

    def test_fixed_time_current_diff_requires_explicit_semantic_match(self) -> None:
        item = observation(status="good", observed_ts=BASE_TS)
        reduced = reduce_domain([item], DEFAULT_POLICIES["delivery"], now_ts=BASE_TS)
        equivalent = compare_current_projection(
            [reduced],
            {
                "delivery": {
                    "state": "good",
                    "reason_codes": ["current_good_evidence"],
                    "observed_at": utc_text(BASE_TS),
                }
            },
        )
        mismatch = compare_current_projection(
            [reduced],
            {
                "delivery": {
                    "state": "bad",
                    "reason_codes": ["current_bad_evidence"],
                    "observed_at": utc_text(BASE_TS),
                }
            },
        )
        self.assertTrue(equivalent.equivalent)
        self.assertFalse(mismatch.equivalent)
        self.assertEqual({item.field for item in mismatch.differences}, {"state", "reason_codes"})

    def test_live_parity_marks_newer_bounded_legacy_snapshot_skew_as_convergence_candidate(self) -> None:
        for expected_ts in (BASE_TS - 179, BASE_TS + 179):
            with self.subTest(expected_ts=expected_ts):
                actual = current(
                    state="good",
                    observed_ts=BASE_TS,
                    payload={"selected": [{"source": "runtime_delivery_watchdog"}]},
                )
                expected = {
                    "delivery": {
                        "state": "bad",
                        "source": "subsystems_status.local_delivery",
                        "observed_at": utc_text(expected_ts),
                        "normalization_error": "",
                    }
                }
                report = live_parity_report([actual], expected)
                self.assertEqual(report["accepted_difference_count"], 0)
                self.assertEqual(report["unclassified_contract_difference_count"], 1)
                self.assertEqual(
                    report["domains"]["delivery"]["classification"],
                    "candidate_source_snapshot_skew_convergence",
                )
                self.assertEqual(
                    report["domains"]["delivery"]["snapshot_skew_sec"],
                    expected_ts - BASE_TS,
                )

    def test_live_parity_keeps_unbounded_untrusted_or_untimed_difference_unclassified(self) -> None:
        for observed_at, selected_source, normalization_error in (
            (utc_text(BASE_TS - 181), "runtime_delivery_watchdog", ""),
            (utc_text(BASE_TS + 181), "runtime_delivery_watchdog", ""),
            (utc_text(BASE_TS), "runtime_delivery_watchdog", ""),
            ("", "runtime_delivery_watchdog", ""),
            ("invalid", "runtime_delivery_watchdog", ""),
            (utc_text(BASE_TS + 30), "rtmps_tcp", ""),
            (utc_text(BASE_TS + 30), "runtime_delivery_watchdog", "source_missing"),
        ):
            with self.subTest(
                observed_at=observed_at,
                selected_source=selected_source,
                normalization_error=normalization_error,
            ):
                actual = current(
                    state="good",
                    observed_ts=BASE_TS,
                    payload={"selected": [{"source": selected_source}]},
                )
                report = live_parity_report(
                    [actual],
                    {
                        "delivery": {
                            "state": "bad",
                            "source": "subsystems_status.local_delivery",
                            "observed_at": observed_at,
                            "normalization_error": normalization_error,
                        }
                    },
                )
                self.assertEqual(
                    report["domains"]["delivery"]["classification"],
                    "unclassified_contract_difference",
                )

    def test_live_parity_rejects_duplicate_actual_domain_instead_of_overwriting(self) -> None:
        first = current(
            state="good",
            observed_ts=BASE_TS,
            marker="first",
            payload={"selected": [{"source": "runtime_delivery_watchdog"}]},
        )
        second = current(
            state="bad",
            observed_ts=BASE_TS,
            marker="second",
            payload={"selected": [{"source": "runtime_delivery_watchdog"}]},
        )
        report = live_parity_report(
            [first, second],
            {
                "delivery": {
                    "state": "good",
                    "source": "subsystems_status.local_delivery",
                    "observed_at": utc_text(BASE_TS),
                    "normalization_error": "",
                }
            }
        )
        self.assertFalse(report["equivalent"])
        self.assertEqual(report["unclassified_contract_difference_count"], 1)
        self.assertEqual(
            report["domains"]["delivery"]["classification"],
            "invalid_actual_domain_duplicate",
        )
        self.assertEqual(
            report["input_integrity_errors"],
            ["duplicate_actual_domain:delivery"],
        )

    def test_live_parity_fixture_reclassifies_all_22_rows_only_after_bounded_proof(self) -> None:
        fixture_path = (
            Path(__file__).parent
            / "fixtures"
            / "monitoring_v4"
            / "2026-08-17_bidirectional_snapshot_convergence.json"
        )
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        accumulator = ParityAccumulator()
        for row in fixture["differences"]:
            actual = current(
                domain=row["domain"],
                state=row["actual_state"],
                observed_ts=unix_ts(row["actual_observed_at"]),
                reduced_ts=unix_ts(row["cycle_started_at"]),
                payload={
                    "selected": [
                        {"source": source} for source in row["actual_sources"]
                    ]
                },
            )
            report = live_parity_report(
                [actual],
                {
                    row["domain"]: {
                        "state": row["expected_state"],
                        "source": row["expected_source"],
                        "observed_at": row["expected_observed_at"],
                        "normalization_error": "",
                    }
                },
            )
            detail = report["domains"][row["domain"]]
            self.assertEqual(detail["snapshot_skew_sec"], row["snapshot_skew_sec"])
            self.assertEqual(
                detail["classification"],
                "candidate_source_snapshot_skew_convergence",
            )
            accumulator.add(
                report,
                cycle_in_window=True,
                bucket_id=unix_ts(row["cycle_started_at"]),
            )

        before = accumulator.totals()
        self.assertEqual(before["accepted"], 0)
        self.assertEqual(before["violations"], 22)
        self.assertEqual(before["unconverged_candidates"], 22)

        for row in fixture["equivalent_proofs"]:
            actual = current(
                domain=row["domain"],
                state=row["actual_state"],
                observed_ts=unix_ts(row["actual_observed_at"]),
                reduced_ts=unix_ts(row["cycle_started_at"]),
                payload={
                    "selected": [
                        {"source": source} for source in row["actual_sources"]
                    ]
                },
            )
            report = live_parity_report(
                [actual],
                {
                    row["domain"]: {
                        "state": row["expected_state"],
                        "source": row["expected_source"],
                        "observed_at": row["expected_observed_at"],
                        "normalization_error": "",
                    }
                },
            )
            accumulator.add(
                report,
                cycle_in_window=True,
                bucket_id=unix_ts(row["cycle_started_at"]),
            )

        after = accumulator.totals()
        self.assertEqual(after["accepted"], 22)
        self.assertEqual(after["violations"], 0)
        self.assertEqual(after["retrospectively_verified_snapshot"], 22)

    def test_live_parity_requires_convergence_after_versioned_rollout_evidence(self) -> None:
        actual = current(
            state="bad",
            observed_ts=BASE_TS + 30,
            payload={"selected": [{"source": "runtime_delivery_watchdog"}]},
        )
        expected = {
            "delivery": {
                "state": "good",
                "source": "subsystems_status.local_delivery",
                "observed_at": utc_text(BASE_TS + 10),
                "normalization_error": "",
            }
        }
        evidence = VerifiedRolloutEvidence.create(
            rollout_id="rollout-verified-1",
            planned_at=utc_text(BASE_TS),
            expires_at=utc_text(BASE_TS + 900),
            pod_started_at=utc_text(BASE_TS + 20),
            pod_uid="pod-uid-verified-1",
            reason="test_rollout",
        )
        projection = RuntimeRolloutProjection(
            observed_at=utc_text(BASE_TS + 40),
            evidence=(evidence,),
        )
        report = live_parity_report([actual], expected, projection)
        self.assertEqual(report["accepted_difference_count"], 0)
        self.assertEqual(report["unclassified_contract_difference_count"], 1)
        self.assertEqual(
            report["domains"]["delivery"]["classification"],
            "candidate_verified_planned_rollout_convergence",
        )
        self.assertEqual(report["domains"]["delivery"]["rollout_evidence_id"], evidence.evidence_id)

        outside = current(
            state="bad",
            observed_ts=BASE_TS + 901,
            payload={"selected": [{"source": "runtime_delivery_watchdog"}]},
        )
        outside_report = live_parity_report([outside], expected, projection)
        self.assertEqual(outside_report["accepted_difference_count"], 0)
        self.assertEqual(outside_report["unclassified_contract_difference_count"], 1)

        with self.assertRaisesRegex(ValueError, "newer than its projection"):
            RuntimeRolloutProjection(
                observed_at=utc_text(BASE_TS + 10),
                evidence=(evidence,),
            )

    def test_input_quality_same_timestamp_mismatch_is_not_blanket_accepted(self) -> None:
        actual = current(
            domain="youtube_input_quality",
            state="bad",
            observed_ts=BASE_TS,
        )
        report = live_parity_report(
            [actual],
            {
                "youtube_input_quality": {
                    "state": "good",
                    "source": "operational_reliability_burn_status.raw_current",
                    "observed_at": utc_text(BASE_TS),
                    "normalization_error": "",
                }
            },
        )
        self.assertEqual(report["accepted_difference_count"], 0)
        self.assertEqual(report["unclassified_contract_difference_count"], 1)
        self.assertEqual(
            report["domains"]["youtube_input_quality"]["classification"],
            "unclassified_contract_difference",
        )

    def test_unavailable_legacy_source_does_not_exempt_a_state_mismatch(self) -> None:
        actual = current(state="good", observed_ts=BASE_TS)
        report = live_parity_report(
            [actual],
            {
                "delivery": {
                    "state": "unknown",
                    "source": "subsystems_status.local_delivery",
                    "observed_at": "",
                    "normalization_error": "source_missing",
                }
            },
        )
        self.assertEqual(report["accepted_difference_count"], 0)
        self.assertEqual(report["unclassified_contract_difference_count"], 1)

    def test_current_service_persists_all_domains_after_reads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repository = MonitoringRepository(Path(td) / "db.sqlite3")
            repository.initialize(applied_at=utc_text(BASE_TS))
            repository.append_observation(observation())
            service = CurrentReducerService(repository, DEFAULT_POLICIES)
            snapshots = service.reduce(("delivery", "youtube_lifecycle"), now_ts=BASE_TS)
            self.assertEqual(len(snapshots), 2)
            self.assertEqual(repository.current("delivery").state, "good")
            self.assertEqual(repository.current("youtube_lifecycle").state, "unknown")


class _FailingAdapter:
    source = "failing_adapter"
    deadline_sec = 0.1

    def collect(self, *, received_at: str) -> AdapterBatch:
        raise RuntimeError("injected failure")


class _SlowAdapter:
    source = "slow_adapter"
    deadline_sec = 0.005

    def collect(self, *, received_at: str) -> AdapterBatch:
        time.sleep(0.05)
        return AdapterBatch(self.source)


class _PartialAdapter:
    source = "partial_adapter"
    deadline_sec = 0.1

    def collect(self, *, received_at: str) -> AdapterBatch:
        return AdapterBatch(
            self.source,
            observations=(observation(event="partial-valid"),),
            rejections=(
                ObservationRejection.create(
                    source=self.source,
                    reason_code="partial_invalid_input",
                    detail="one input failed validation",
                    received_at=received_at,
                ),
            ),
        )


class _BlockingAdapter:
    source = "blocking_adapter"
    deadline_sec = 0.01

    def collect(self, *, received_at: str) -> AdapterBatch:
        del received_at
        time.sleep(10)
        return AdapterBatch(self.source)


class ObserverTests(unittest.TestCase):
    def test_exceptions_and_per_source_deadlines_are_ledgered(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repository = MonitoringRepository(Path(td) / "db.sqlite3")
            repository.initialize(applied_at=utc_text(BASE_TS))
            result = Observer(repository).run_once([_FailingAdapter(), _SlowAdapter()], now_ts=BASE_TS)
            self.assertEqual(result.inserted_observations, 0)
            self.assertEqual(result.inserted_rejections, 2)
            self.assertEqual(result.timed_out_sources, ("slow_adapter",))
            with repository.connection(read_only=True) as connection:
                reasons = {
                    row[0] for row in connection.execute("SELECT reason_code FROM rejections").fetchall()
                }
            self.assertEqual(reasons, {"adapter_exception", "adapter_deadline_exceeded"})

    def test_repeated_timeouts_leave_no_adapter_processes_running(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repository = MonitoringRepository(Path(td) / "db.sqlite3")
            repository.initialize(applied_at=utc_text(BASE_TS))
            observer = Observer(repository)
            for offset in range(6):
                result = observer.run_once(
                    [_BlockingAdapter()],
                    now_ts=BASE_TS + offset,
                )
                self.assertEqual(result.timed_out_sources, ("blocking_adapter",))
            leaked = [
                process.name
                for process in multiprocessing.active_children()
                if process.name.startswith("monitoring-v4-adapter-")
            ]
            self.assertEqual(leaked, [])

    def test_partial_batch_is_not_reported_as_successful_or_healthy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repository = MonitoringRepository(Path(td) / "db.sqlite3")
            repository.initialize(applied_at=utc_text(BASE_TS))
            observer = Observer(repository)

            first = observer.run_once([_PartialAdapter()], now_ts=BASE_TS)
            second = observer.run_once([_PartialAdapter()], now_ts=BASE_TS)

            self.assertEqual(first.source_results[0]["outcome"], "partial")
            self.assertEqual(first.inserted_observations, 1)
            self.assertEqual(first.inserted_rejections, 1)
            self.assertEqual(second.duplicate_observations, 1)
            self.assertEqual(second.inserted_rejections, 0)
            self.assertEqual(second.source_results[0]["outcome"], "partial")
            with repository.connection(read_only=True) as connection:
                health = connection.execute(
                    "SELECT status, detail FROM component_health WHERE component=?",
                    ("observer",),
                ).fetchone()
            self.assertEqual(health["status"], "unknown")
            self.assertIn("rejections_seen=1", health["detail"])
            self.assertIn("rejections_inserted=0", health["detail"])


if __name__ == "__main__":
    unittest.main()
