from __future__ import annotations

import copy
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from watchers import fast_recovery
from watchers.fast_recovery_core import tcp_send_sample as sampler


class SourceTimedSampleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state = {"last_reason": "unchanged", "last_tcp_send_sample_ts": 900}

    def metrics(self, seconds=0.0, *, sequence=1, counter=1_000_000, **overrides):
        observed = datetime(2026, 9, 1, tzinfo=timezone.utc) + timedelta(
            seconds=seconds
        )
        metrics = {
            "bytes_sent": counter,
            "bytes_acked": counter - 50,
            "conn": "ESTAB 0 123 local:1234 peer:443 users:ffmpeg",
            "sample_observed_at": observed.isoformat(timespec="microseconds"),
            "sample_producer_instance_id": "producer-1",
            "sample_ffmpeg_generation": "generation-1",
            "sample_source_sequence": sequence,
            "sample_consumer_monotonic_ns": 10**12 + round(seconds * 10**9),
        }
        metrics.update(overrides)
        return metrics

    def sample(self, metrics, *, pid=222):
        return sampler.source_timed_sample(
            self.state,
            ffmpeg_pid=pid,
            bytes_sent=metrics.get("bytes_sent", 0),
            metrics=metrics,
            interval_seconds=60,
        )

    def seed(self):
        self.assertIsNone(self.sample(self.metrics()))

    def test_corrects_63_seconds_misreported_as_62_without_clipping(self):
        self.seed()
        value = self.sample(
            self.metrics(
                63.125,
                sequence=55,
                counter=40_137_500,
                sample_consumer_monotonic_ns=10**12 + 62 * 10**9,
            )
        )
        self.assertEqual(value["sample_interval_sec"], 63.125)
        self.assertEqual(value["mbps"], 4.96)
        self.assertGreater(value["bytes_sent_delta"] * 8 / 62_000_000, 5)
        self.assertEqual(value["sample_time_source"], sampler.TIME_SOURCE)
        self.assertEqual(value["sample_source_sequence_start"], 1)
        self.assertEqual(value["sample_source_sequence_end"], 55)
        self.assertEqual(self.state["last_reason"], "unchanged")
        self.assertEqual(self.state["last_tcp_send_sample_ts"], 900)

    def test_real_exceedance_is_preserved(self):
        self.seed()
        value = self.sample(self.metrics(60, sequence=52, counter=39_400_000))
        self.assertEqual(value["mbps"], 5.12)

    def test_zero_send_is_a_valid_zero_not_missing(self):
        self.seed()
        self.assertEqual(self.sample(self.metrics(60, sequence=52))["mbps"], 0)

    def test_old_consumer_time_baseline_is_not_reused(self):
        self.state.update(
            last_tcp_send_sample_pid=222, last_tcp_send_sample_bytes_sent=1
        )
        self.seed()

    def test_subsecond_precision_survives_state_serialization(self):
        self.sample(self.metrics(0.125))
        self.state = json.loads(json.dumps(self.state))
        value = self.sample(self.metrics(60.625, sequence=53, counter=38_510_000))
        self.assertEqual(value["sample_interval_sec"], 60.5)
        self.assertEqual(value["mbps"], 4.96)

    def test_source_time_controls_sample_cadence(self):
        self.seed()
        self.assertIsNone(
            self.sample(
                self.metrics(
                    59.5,
                    sequence=51,
                    counter=37_890_000,
                    sample_consumer_monotonic_ns=10**12 + 61 * 10**9,
                )
            )
        )
        value = self.sample(self.metrics(70, sequence=60, counter=44_400_000))
        self.assertEqual(value["sample_interval_sec"], 70)
        self.assertEqual(value["mbps"], 4.96)

    def test_queue_changes_do_not_change_socket_identity(self):
        self.seed()
        value = self.sample(
            self.metrics(
                60,
                sequence=52,
                counter=38_200_000,
                conn="ESTAB 9 456 local:1234 peer:443 users:ffmpeg",
            )
        )
        self.assertEqual(value["mbps"], 4.96)

    def test_generation_producer_socket_and_pid_changes_rebaseline(self):
        for changes in (
            {"sample_producer_instance_id": "producer-2"},
            {"sample_ffmpeg_generation": "generation-2"},
            {"conn": "ESTAB 0 0 local:5678 peer:443 users:ffmpeg"},
            {"pid": 333},
        ):
            with self.subTest(changes=changes):
                self.state = {}
                self.seed()
                changes = dict(changes)
                pid = changes.pop("pid", 222)
                self.assertIsNone(
                    self.sample(
                        self.metrics(60, sequence=52, counter=40_000_000, **changes),
                        pid=pid,
                    )
                )

    def test_counter_reset_rebaselines(self):
        self.seed()
        self.assertIsNone(self.sample(self.metrics(60, sequence=52, counter=1)))
        self.assertEqual(
            self.sample(self.metrics(120, sequence=103, counter=37_200_001))["mbps"],
            4.96,
        )

    def test_duplicate_and_reordered_observations_do_not_move_baseline(self):
        self.seed()
        self.sample(self.metrics(10, sequence=10, counter=7_200_000))
        before = copy.deepcopy(self.state)
        self.assertIsNone(self.sample(self.metrics(10, sequence=10, counter=7_200_000)))
        self.assertIsNone(self.sample(self.metrics(5, sequence=5, counter=4_100_000)))
        self.assertEqual(self.state, before)
        self.assertEqual(
            self.sample(self.metrics(60, sequence=52, counter=38_200_000))["mbps"], 4.96
        )

    def test_clock_steps_and_monotonic_reset_do_not_emit_rates(self):
        for source_seconds, monotonic_seconds in ((-1, 10), (160, 60), (60, -1)):
            with self.subTest(source=source_seconds, monotonic=monotonic_seconds):
                self.state = {}
                self.seed()
                self.assertIsNone(
                    self.sample(
                        self.metrics(
                            source_seconds,
                            sequence=52,
                            counter=40_000_000,
                            sample_consumer_monotonic_ns=10**12
                            + monotonic_seconds * 10**9,
                        )
                    )
                )

    def test_missing_malformed_or_naive_source_metadata_never_falls_back(self):
        invalid = (
            {"sample_observed_at": None},
            {"sample_observed_at": "invalid"},
            {"sample_observed_at": "2026-09-01T00:01:00"},
            {"sample_producer_instance_id": ""},
            {"sample_ffmpeg_generation": ""},
            {"sample_source_sequence": 0},
            {"sample_source_sequence": True},
            {"sample_consumer_monotonic_ns": None},
            {"conn": "malformed supplied connection"},
        )
        for changes in invalid:
            with self.subTest(changes=changes):
                self.state = {}
                self.seed()
                self.assertIsNone(
                    self.sample(
                        self.metrics(60, sequence=52, counter=40_000_000, **changes)
                    )
                )
                self.assertNotIn(sampler.STATE_KEY, self.state)
        self.seed()
        self.assertIsNone(self.sample({"bytes_sent": 40_000_000}))

    def test_empty_or_corrupt_saved_state_rebaselines(self):
        for saved in (None, [], {"baseline": {}}, {"baseline": {}, "seen": {}}):
            self.state = {sampler.STATE_KEY: saved}
            self.assertIsNone(
                self.sample(self.metrics(60, sequence=52, counter=40_000_000))
            )

    def test_disabled_sampler_and_invalid_pid_do_not_emit(self):
        self.assertIsNone(
            sampler.source_timed_sample(
                self.state,
                ffmpeg_pid=222,
                bytes_sent=1_000_000,
                metrics=self.metrics(),
                interval_seconds=0,
            )
        )
        self.assertIsNone(self.sample(self.metrics(), pid=0))

    def test_binding_does_not_mutate_source_and_keeps_original_metrics(self):
        original = {"bytes_sent": 123, "conn": "unchanged"}
        observation = {
            "tcp_metrics": original,
            "observed_at": "2026-09-01T00:00:00.125Z",
            "sequence": 123,
            "producer_instance_id": "p",
            "ffmpeg_generation": "g",
        }
        bound = sampler.bind_runtime_metrics(observation, consumer_monotonic_ns=987)
        self.assertEqual(bound["bytes_sent"], 123)
        self.assertEqual(bound["sample_observed_at"], observation["observed_at"])
        self.assertEqual(bound["sample_source_sequence"], 123)
        self.assertEqual(original, {"bytes_sent": 123, "conn": "unchanged"})
        self.assertEqual(
            sampler.bind_runtime_metrics({}, consumer_monotonic_ns=987), {}
        )

    def test_actual_runtime_v1_shape_without_conn_produces_source_timed_rate(self):
        # Exact field contract of runtime_boundary.observation._tcp_metrics:
        # unlike the legacy direct ss parser, this producer does NOT emit conn.
        source = {
            "schema_version": "runtime.ffmpeg_observation.v1",
            "observed_at": "2026-09-01T22:00:00.125Z",
            "producer_instance_id": "runtime-producer",
            "ffmpeg_generation": "runtime-generation:1",
            "protocol_ffmpeg_pid": 222,
            "sequence": 100,
            "tcp_metrics": {
                "bytes_sent": 1_000_000,
                "bytes_acked": 999_999,
                "send_q": 0,
                "notsent": 0,
                "unacked": 0,
                "lastsnd_ms": 100,
                "rto_ms": 300,
            },
        }
        self.assertIsNone(
            self.sample(
                sampler.bind_runtime_metrics(
                    source,
                    consumer_monotonic_ns=10**12,
                )
            )
        )
        source["observed_at"] = "2026-09-01T22:01:03.250Z"
        source["sequence"] = 155
        source["tcp_metrics"]["bytes_sent"] = 40_137_500
        value = self.sample(
            sampler.bind_runtime_metrics(
                source,
                consumer_monotonic_ns=10**12 + 62 * 10**9,
            )
        )
        self.assertEqual(value["mbps"], 4.96)
        self.assertEqual(value["sample_interval_sec"], 63.125)
        self.assertEqual(value["sample_identity_scope"], "ffmpeg_generation")
        self.assertIsNone(value["sample_connection_id"])

    def test_actual_v1_shape_still_fences_generation_and_counter_resets(self):
        self.sample(self.metrics(conn=None))
        self.assertIsNone(
            self.sample(
                self.metrics(
                    60,
                    sequence=52,
                    counter=40_000_000,
                    conn=None,
                    sample_ffmpeg_generation="generation-2",
                )
            )
        )
        self.assertIsNone(
            self.sample(
                self.metrics(
                    120,
                    sequence=104,
                    counter=100,
                    conn=None,
                    sample_ffmpeg_generation="generation-2",
                )
            )
        )

    def test_live_wrapper_reads_once_and_binds_same_snapshot(self):
        observation = {
            "protocol_ffmpeg_pid": 222,
            "tcp_metrics": {"bytes_sent": 123},
            "observed_at": "2026-09-01T00:00:00.125Z",
            "sequence": 123,
            "producer_instance_id": "p",
            "ffmpeg_generation": "g",
        }
        with (
            patch.object(fast_recovery, "EFFECT_EXECUTOR_SOCKET", "/test/socket"),
            patch.object(
                fast_recovery.effect_contract,
                "read_runtime_observation",
                return_value=observation,
            ) as read,
            patch.object(fast_recovery.time, "monotonic_ns", return_value=987),
        ):
            result = fast_recovery.parse_ffmpeg_tcp_metrics(222, [443])
            read.assert_called_once()
            self.assertEqual(result["sample_observed_at"], observation["observed_at"])
            self.assertEqual(result["sample_consumer_monotonic_ns"], 987)
            self.assertEqual(fast_recovery.parse_ffmpeg_tcp_metrics(333, [443]), {})

    def test_live_wrapper_ignores_consumer_wall_time_and_logs_auditable_sample(self):
        with (
            patch.object(fast_recovery, "EFFECT_EXECUTOR_SOCKET", "/test/socket"),
            patch.object(fast_recovery, "append_event") as append,
            patch.object(fast_recovery, "TCP_SEND_SAMPLE_LOG_SEC", 60),
        ):
            for now, metrics in (
                (1000, self.metrics()),
                (1062, self.metrics(63.125, sequence=55, counter=40_137_500)),
            ):
                fast_recovery.maybe_append_tcp_send_sample(
                    self.state,
                    now_ts=now,
                    ffmpeg_pid=222,
                    bytes_sent=metrics["bytes_sent"],
                    metrics=metrics,
                )
            append.assert_called_once()
            self.assertEqual(append.call_args.args[2]["mbps"], 4.96)
            self.assertEqual(append.call_args.args[2]["sample_interval_sec"], 63.125)


if __name__ == "__main__":
    unittest.main()
