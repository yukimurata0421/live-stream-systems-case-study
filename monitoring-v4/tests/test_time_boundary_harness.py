from __future__ import annotations

import unittest
from datetime import datetime, timezone
from fractions import Fraction
import os
from pathlib import Path
import tempfile

# The release-source validator rejects test caches in the repository root.
_HYPOTHESIS_STORAGE = tempfile.TemporaryDirectory(
    prefix="stream-v4-time-boundary-hypothesis-"
)
os.environ.setdefault("HYPOTHESIS_STORAGE_DIRECTORY", _HYPOTHESIS_STORAGE.name)

from hypothesis import HealthCheck, given, note, settings
from hypothesis.strategies import integers, sampled_from

from stream_monitoring_v4.commands.wall_clock_loop import next_run_epoch
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES

from tests.time_boundary_harness import (
    compress_events,
    epoch,
    load_fixture,
    manifest_startup_budget,
    next_daily_epoch,
    readable_sequence,
    replay_integrity_retry,
    replay_periodic_boundary,
    replay_video_resolver_ttl,
    startup_budget_sequence,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = (
    ROOT
    / "tests"
    / "fixtures"
    / "monitoring_v4"
    / "2026-08-12_time_boundary_replay.json"
)
PROPERTY_SETTINGS = settings(
    max_examples=100,
    deadline=None,
    database=None,
    derandomize=True,
    suppress_health_check=(HealthCheck.too_slow,),
)


class HistoricalTimeBoundaryRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = load_fixture(FIXTURE_PATH)

    def test_historical_startup_timeline_replays_without_sleep_under_compression(self) -> None:
        events = self.fixture["historical_timelines"]["startup_recovery"]["events"]
        original_offsets = [0, 3, 84, 88, 292, 294, 384, 385]
        for factor in self.fixture["boundary_transform"]["compression_factors"]:
            with self.subTest(factor=factor):
                compressed = compress_events(events, factor=factor)
                self.assertEqual(
                    [item.original_offset_sec for item in compressed], original_offsets
                )
                self.assertEqual(
                    [item.compressed_offset_sec for item in compressed],
                    [Fraction(offset, factor) for offset in original_offsets],
                )
                self.assertEqual(
                    [item.kind for item in compressed],
                    [item["kind"] for item in events],
                )

    def test_documented_retry_probe_revisions_preserve_equal_boundary_failures(self) -> None:
        outcomes = {}
        for revision in self.fixture["startup_budget_revisions"]:
            sequence = startup_budget_sequence(
                retry_timeout_sec=revision["retry_timeout_sec"],
                post_retry_wait_sec=revision["post_retry_wait_bound_sec"],
                probe_budget_sec=revision["nominal_startup_probe_budget_sec"],
            )
            outcomes[(revision["revision"], revision["component"])] = sequence
            expected = (
                "equal_boundary_race"
                if "equal_boundary" in revision["documented_disposition"]
                else "margin"
            )
            self.assertEqual(
                sequence["outcome"],
                expected,
                readable_sequence("startup_budget", {**revision, **sequence}),
            )
        final_core = outcomes[
            ("ef2f0b0d5ed9e0f5fefac6d4ef1f72d508ae2afa", "core")
        ]
        self.assertEqual(final_core["margin_sec"], 60)

    def test_accepted_startup_budget_matches_current_manifests(self) -> None:
        accepted = {
            item["component"]: item
            for item in self.fixture["startup_budget_revisions"]
            if item["revision"] == "ef2f0b0d5ed9e0f5fefac6d4ef1f72d508ae2afa"
        }
        for component in ("core", "exporter"):
            with self.subTest(component=component):
                manifest = manifest_startup_budget(ROOT / "deploy" / "k3s" / f"{component}.yaml")
                self.assertEqual(
                    manifest["retry_timeout_sec"], accepted[component]["retry_timeout_sec"]
                )
                self.assertEqual(
                    manifest["nominal_startup_probe_budget_sec"],
                    accepted[component]["nominal_startup_probe_budget_sec"],
                )

    def test_retry_boundary_uses_virtual_monotonic_time(self) -> None:
        results = {
            ready_at: replay_integrity_retry(
                timeout_sec=120, dependency_ready_at_sec=ready_at
            )
            for ready_at in (119, 120, 121)
        }
        self.assertTrue(results[119].succeeded)
        self.assertTrue(results[120].succeeded)
        self.assertFalse(results[121].succeeded)
        self.assertEqual(results[120].completed_at_sec, 120)
        self.assertEqual(results[121].completed_at_sec, 120)
        self.assertEqual(sum(results[120].virtual_sleeps), 120)

    def test_ttl_boundary_replays_historical_85_second_gap_and_epsilon(self) -> None:
        historical = replay_video_resolver_ttl(age_sec=85)
        self.assertEqual(historical["state"], "good")
        transformed = {age: replay_video_resolver_ttl(age_sec=age) for age in (89, 90, 91)}
        self.assertEqual(transformed[89]["state"], "good")
        self.assertEqual(transformed[90]["state"], "good")
        self.assertEqual(transformed[91]["state"], "unknown")
        self.assertEqual(
            transformed[91]["ignored"],
            [{"source": "youtube_video_resolver", "reason": "stale"}],
        )

    def test_periodic_backup_history_and_current_contracts_are_wall_clock_anchored(self) -> None:
        historical = self.fixture["historical_timelines"]["periodic_backup"]
        scheduled = epoch(historical["scheduled_at"])
        self.assertEqual(epoch(historical["completed_at"]) - scheduled, 5)
        backup_events = [
            {"at": historical["scheduled_at"], "kind": "backup_scheduled"},
            {"at": historical["completed_at"], "kind": "backup_completed"},
        ]
        for factor in self.fixture["boundary_transform"]["compression_factors"]:
            compressed = compress_events(backup_events, factor=factor)
            self.assertEqual(
                [item.compressed_offset_sec for item in compressed],
                [Fraction(0), Fraction(5, factor)],
            )
        self.assertEqual(
            next_daily_epoch(scheduled - 1, schedule=historical["schedule"]), scheduled
        )
        self.assertEqual(
            next_daily_epoch(scheduled, schedule=historical["schedule"]),
            scheduled + 86400,
        )

        for contract in self.fixture["periodic_contracts"]:
            if contract["kind"] != "daily_cron":
                continue
            manifest = (ROOT / contract["manifest"]).read_text(encoding="utf-8")
            self.assertIn(f'schedule: "{contract["schedule"]}"', manifest)
            for phase in (-1, 0, 1):
                replay = replay_periodic_boundary(
                    scheduled_epoch=next_daily_epoch(
                        scheduled - 86400, schedule=contract["schedule"]
                    ),
                    phase_sec=phase,
                    kind="daily_cron",
                    schedule=contract["schedule"],
                )
                self.assertTrue(
                    replay["strictly_future"], readable_sequence(contract["name"], replay)
                )


class TimeBoundaryPropertyTests(unittest.TestCase):
    @PROPERTY_SETTINGS
    @given(
        timeout_sec=sampled_from((120, 180)),
        epsilon=sampled_from((-1, 0, 1)),
        interval_sec=sampled_from((0.25, 0.5, 1.0, 2.0)),
    )
    def test_retry_transform_matches_current_sut_boundary(
        self, timeout_sec: int, epsilon: int, interval_sec: float
    ) -> None:
        replay = replay_integrity_retry(
            timeout_sec=timeout_sec,
            dependency_ready_at_sec=timeout_sec + epsilon,
            retry_interval_sec=interval_sec,
        )
        sequence = replay.sequence()
        note(readable_sequence("retry", sequence))
        self.assertEqual(
            replay.succeeded,
            epsilon <= 0,
            readable_sequence("minimal_retry_sequence", sequence),
        )
        self.assertLessEqual(replay.completed_at_sec, timeout_sec)

    @PROPERTY_SETTINGS
    @given(
        retry_timeout_sec=sampled_from((120, 180)),
        post_retry_wait_sec=integers(min_value=0, max_value=60),
        margin_sec=integers(min_value=-2, max_value=120),
        compression_factor=sampled_from((1, 10, 60, 3600)),
    )
    def test_probe_budget_transform_preserves_order_under_time_compression(
        self,
        retry_timeout_sec: int,
        post_retry_wait_sec: int,
        margin_sec: int,
        compression_factor: int,
    ) -> None:
        ready_at = retry_timeout_sec + post_retry_wait_sec
        probe_budget = ready_at + margin_sec
        sequence = startup_budget_sequence(
            retry_timeout_sec=retry_timeout_sec,
            post_retry_wait_sec=post_retry_wait_sec,
            probe_budget_sec=probe_budget,
        )
        note(readable_sequence("probe", sequence))
        expected = (
            "margin"
            if margin_sec > 0
            else "equal_boundary_race"
            if margin_sec == 0
            else "probe_precedes_ready"
        )
        self.assertEqual(
            sequence["outcome"], expected, readable_sequence("minimal_probe_sequence", sequence)
        )
        self.assertEqual(
            Fraction(probe_budget - ready_at, compression_factor),
            Fraction(sequence["margin_sec"], compression_factor),
        )

    @PROPERTY_SETTINGS
    @given(
        envelope_limit_sec=sampled_from(
            tuple(
                sorted(
                    {
                        rule.ttl_sec
                        for policy in DEFAULT_POLICIES.values()
                        for rule in policy.sources
                    }
                )
            )
        ),
        epsilon=sampled_from((-1, 0, 1)),
    )
    def test_ttl_boundary_relation_is_closed_at_equality(
        self, envelope_limit_sec: int, epsilon: int
    ) -> None:
        replay = replay_video_resolver_ttl(
            age_sec=envelope_limit_sec + epsilon,
            freshness_limit_sec=envelope_limit_sec,
        )
        sequence = {
            "source_ttl_sec": 90,
            "envelope_freshness_limit_sec": envelope_limit_sec,
            "age_sec": envelope_limit_sec + epsilon,
            "effective_ttl_sec": min(90, envelope_limit_sec),
            "state": replay["state"],
        }
        note(readable_sequence("ttl", sequence))
        expected_fresh = envelope_limit_sec + epsilon <= min(90, envelope_limit_sec)
        self.assertEqual(
            replay["state"] == "good",
            expected_fresh,
            readable_sequence("minimal_ttl_sequence", sequence),
        )

    @PROPERTY_SETTINGS
    @given(
        minute_modulo=integers(min_value=1, max_value=60),
        second=integers(min_value=0, max_value=59),
        phase_sec=integers(min_value=-1, max_value=1),
        day_offset=integers(min_value=0, max_value=14),
    )
    def test_periodic_schedule_transform_stays_strictly_future_and_anchored(
        self, minute_modulo: int, second: int, phase_sec: int, day_offset: int
    ) -> None:
        anchor = int(
            datetime(2026, 8, 12 + day_offset, 18, 0, second, tzinfo=timezone.utc).timestamp()
        )
        scheduled = next_run_epoch(
            anchor - 1, second=second, minute_modulo=minute_modulo
        )
        replay = replay_periodic_boundary(
            scheduled_epoch=scheduled,
            phase_sec=phase_sec,
            kind="minute_modulo",
            second=second,
            minute_modulo=minute_modulo,
        )
        sequence = {
            **replay,
            "second": second,
            "minute_modulo": minute_modulo,
        }
        note(readable_sequence("periodic", sequence))
        next_value = datetime.fromtimestamp(replay["next_epoch"], tz=timezone.utc)
        self.assertTrue(
            replay["strictly_future"], readable_sequence("minimal_periodic_sequence", sequence)
        )
        self.assertEqual(next_value.second, second)
        self.assertEqual(next_value.minute % minute_modulo, 0)
        self.assertLessEqual(replay["next_epoch"] - replay["now_epoch"], minute_modulo * 60)


if __name__ == "__main__":
    unittest.main()
