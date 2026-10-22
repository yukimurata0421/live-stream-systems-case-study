"""Stall criterion against the shape of the 2026-10-22 06:31 JST RTMPS ACK stall.

During that stall bytes_sent kept growing by about 30 bytes per sample (retransmissions and
probes) while bytes_acked, notsent and unacked were frozen. The bytes_sent-only criterion
missed it; the ACK criterion must not.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from cra.decide import StallTracker
from cra.observe import parse_observation
from cra.policy import Policy

T0 = datetime(2026, 10, 21, 21, 31, 40, tzinfo=UTC)
POLICY = Policy(stall_confirm_samples=2)


def obs(seq: int, *, sent: int, acked: int | None, notsent: int, unacked: int, lastsnd: int, gen: str = "g2"):
    tcp = {"bytes_sent": sent, "notsent": notsent, "unacked": unacked, "lastsnd_ms": lastsnd}
    if acked is not None:
        tcp["bytes_acked"] = acked
    value, why = parse_observation(
        {
            "schema_version": "runtime.ffmpeg_observation.v1",
            "observation_id": f"o{seq}",
            "executor_instance_id": "exec",
            "sequence": seq,
            "observed_at": (T0 + timedelta(seconds=2 * seq)).isoformat(),
            "pod_uid": "pod",
            "ffmpeg_generation": gen,
            "ffmpeg_running": True,
            "ffmpeg_uptime_sec": 74000,
            "tcp_metrics": tcp,
        },
        max_age_sec=None,
    )
    assert value is not None, why
    return value


def run(samples) -> list[int]:
    tracker = StallTracker()
    return [tracker.update(sample, POLICY)[1] for sample in samples]


def test_ack_stall_with_creeping_bytes_sent_is_detected() -> None:
    samples = [
        obs(1, sent=39180315848, acked=39179152874, notsent=1162974, unacked=160, lastsnd=676),
        obs(2, sent=39180315878, acked=39179152874, notsent=1184530, unacked=160, lastsnd=3929),
        obs(3, sent=39180315908, acked=39179152874, notsent=1184530, unacked=160, lastsnd=5194),
    ]
    assert run(samples) == [0, 1, 2]


def test_bytes_sent_only_fallback_misses_that_stall() -> None:
    samples = [
        obs(1, sent=39180315848, acked=None, notsent=1162974, unacked=160, lastsnd=676),
        obs(2, sent=39180315878, acked=None, notsent=1184530, unacked=160, lastsnd=3929),
        obs(3, sent=39180315908, acked=None, notsent=1184530, unacked=160, lastsnd=5194),
    ]
    assert run(samples) == [0, 0, 0]


def test_acknowledged_progress_under_pressure_is_not_a_stall() -> None:
    samples = [obs(i, sent=1000 * i, acked=900 * i, notsent=500_000, unacked=200, lastsnd=10) for i in range(1, 6)]
    assert run(samples) == [0, 0, 0, 0, 0]


def test_idle_connection_without_queued_data_is_not_a_stall() -> None:
    samples = [obs(i, sent=5000, acked=5000, notsent=0, unacked=0, lastsnd=9000) for i in range(1, 6)]
    assert run(samples) == [0, 0, 0, 0, 0]


def test_new_generation_restarts_the_count() -> None:
    frozen = {"sent": 10, "acked": 5, "notsent": 100_000, "unacked": 100, "lastsnd": 5000}
    samples = [obs(1, **frozen), obs(2, **frozen), obs(3, **frozen, gen="g3"), obs(4, **frozen, gen="g3")]
    assert run(samples) == [0, 1, 0, 1]
