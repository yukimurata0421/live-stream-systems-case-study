from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from cra_no_action_soak.recovery_window import RecoveryObservation, RecoveryPolicy, RecoveryWindow

START = datetime(2026, 9, 4, tzinfo=UTC)


def item(seconds: int, **changes: object) -> RecoveryObservation:
    now = START + timedelta(seconds=seconds)
    return replace(
        RecoveryObservation(
            observed_at=now,
            network="UP",
            network_observed_at=now,
            target="target-1",
            transport_observed_at=now,
            bytes_acked=1000 + seconds * 100,
            stream_id="stream-1",
            platform="UP",
            platform_observed_at=now,
            platform_stream_id="stream-1",
            control_path="UP",
            unresolved_effect_count=0,
        ),
        **changes,
    )


def baseline() -> RecoveryWindow:
    window = RecoveryWindow()
    for t in (0, 15, 30):
        window.observe(item(t))
    assert window.current_health == "READY"
    return window


def test_network_recovery_is_not_delivery_recovery() -> None:
    window = baseline()
    window.observe(item(45, network="DOWN", platform="DOWN", bytes_acked=4000))
    window.observe(item(60, platform="DOWN", bytes_acked=4000))
    for t in range(75, 660, 15):
        window.observe(item(t, platform="DOWN", bytes_acked=4000))
    result = window.finish(START + timedelta(seconds=661))
    assert result["deadline_exceeded_count"] == 1
    assert result["active_episode"]["deadline_at"] == "2026-09-04T00:11:00.000Z"
    assert result["recovered_episode_count"] == 0


@pytest.mark.parametrize("completion,expected", [(659, False), (660, False), (661, True)])
def test_600_second_boundary_inclusive(completion: int, expected: bool) -> None:
    window = baseline()
    window.observe(item(45, network="DOWN", platform="DOWN"))
    window.observe(item(60, platform="DOWN"))
    for t in (completion - 30, completion - 15, completion):
        window.observe(item(t))
    result = window.finish(START + timedelta(seconds=completion))
    assert result["deadline_exceeded_count"] == int(expected)
    assert result["recovered_episode_count"] == 1


def test_network_flaps_and_target_changes_do_not_reset_deadline() -> None:
    window = baseline()
    window.observe(item(45, network="DOWN", platform="DOWN"))
    window.observe(item(60, platform="DOWN"))
    window.observe(item(450, network="DOWN", target="target-2", platform="DOWN"))
    window.observe(item(600, target="target-2", platform="DOWN"))
    assert window.active is not None
    assert window.active.network_recovered_at == START + timedelta(seconds=60)
    assert window.finish(START + timedelta(seconds=661))["deadline_exceeded_count"] == 1


def test_target_lifecycle_transition_opens_bounded_recovery_without_claiming_network_recovery() -> None:
    window = baseline()
    window.observe(item(45, target="target-2"))
    assert window.active is not None
    assert window.active.trigger == "TARGET_TRANSITION"
    assert window.active.deadline_at == START + timedelta(seconds=645)
    for seconds in (60, 75, 90):
        window.observe(item(seconds, target="target-2"))
    result = window.finish(START + timedelta(seconds=90))
    assert result["recovered_episode_count"] == 1
    assert result["verified_network_recovered_episode_count"] == 0


def test_owner_bound_successor_opens_recovery_before_transport_converges() -> None:
    window = baseline()
    window.observe(
        item(
            45,
            target=None,
            transport_observed_at=START + timedelta(seconds=30),
            bytes_acked=None,
            child_lifecycle="RUNNING",
            owner_target="target-2",
        )
    )
    assert window.active is not None
    assert window.active.trigger == "TARGET_TRANSITION"
    assert window.active.unknown_sample_count == 1
    assert window.current_health == "RECOVERING"
    for seconds in (60, 75, 90):
        window.observe(item(seconds, target="target-2", child_lifecycle="RUNNING", owner_target="target-2"))
    result = window.finish(START + timedelta(seconds=90))
    assert result["recovered_episode_count"] == 1
    assert result["verified_network_recovered_episode_count"] == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"owner_target": "target-1"},
        {"owner_target": None},
        {"owner_target": "target-2", "child_lifecycle": "UNKNOWN"},
        {"owner_target": "target-2", "bytes_acked": 5000},
    ],
)
def test_transport_gap_without_exact_successor_evidence_stays_unknown(changes: dict[str, object]) -> None:
    window = baseline()
    observation = {
        "target": None,
        "transport_observed_at": START + timedelta(seconds=30),
        "bytes_acked": None,
        "child_lifecycle": "RUNNING",
        **changes,
    }
    window.observe(item(45, **observation))
    assert window.active is None
    assert window.current_health == "UNKNOWN"


def test_long_network_outage_is_separate_from_post_recovery_deadline() -> None:
    window = baseline()
    window.observe(item(45, network="DOWN", platform="DOWN"))
    window.observe(item(3600, network="DOWN", platform="DOWN"))
    assert window.finish(START + timedelta(seconds=3600))["deadline_exceeded_count"] == 0
    for t in (3615, 3630, 3645):
        window.observe(item(t))
    assert window.recovered_count == 1
    assert window.maximum_observed_network_unavailable_seconds == 3570


def test_cached_acks_do_not_count_as_three_points() -> None:
    window = baseline()
    window.observe(item(45, network="DOWN", platform="DOWN"))
    window.observe(item(60))
    for t in (75, 90):
        window.observe(item(t, transport_observed_at=START + timedelta(seconds=60), bytes_acked=7000))
    assert window.recovered_count == 0


def test_stale_or_wrong_stream_platform_cannot_recover() -> None:
    window = baseline()
    window.observe(item(45, platform="DOWN"))
    for t in (60, 75, 90):
        window.observe(item(t, platform_stream_id="unrelated-stream"))
    assert window.recovered_count == 0
    for t in (600, 615, 630):
        window.observe(item(t, platform_observed_at=START + timedelta(seconds=30)))
    assert window.recovered_count == 0


def test_unresolved_effect_blocks_success_even_when_stream_is_up() -> None:
    window = baseline()
    window.observe(item(45, platform="DOWN"))
    for t in (60, 75, 90):
        window.observe(item(t, unresolved_effect_count=1))
    assert window.recovered_count == 0
    window.observe(item(105))
    assert window.recovered_count == 1


def test_deadline_failure_remains_after_late_recovery() -> None:
    window = baseline()
    window.observe(item(45, platform="DOWN"))
    for t in (660, 675, 690):
        window.observe(item(t))
    assert window.current_health == "READY"
    assert window.finish(START + timedelta(seconds=690))["blockers"] == ["RECOVERY_DEADLINE_EXCEEDED"]


def test_ack_regression_is_not_a_recovery() -> None:
    window = baseline()
    window.observe(item(45, bytes_acked=1))
    assert "ACK_COUNTER_REGRESSION_WITHIN_TARGET" in window.blockers


def test_source_future_and_clock_regression_fail_closed() -> None:
    window = baseline()
    window.observe(item(45, network_observed_at=START + timedelta(seconds=46)))
    assert window.current_health == "UNKNOWN"
    window.observe(item(30))
    assert "RECOVERY_OBSERVATION_TIME_INVALID" in window.blockers


def test_cannot_silently_relax_the_600_second_contract() -> None:
    with pytest.raises(ValueError, match="600"):
        RecoveryPolicy(deadline_seconds=601)


def test_pre_reconnect_transport_points_are_not_reused() -> None:
    window = baseline()
    window.observe(item(45, network="DOWN", platform="DOWN"))
    window.observe(item(60, transport_observed_at=START + timedelta(seconds=45), bytes_acked=5500))
    window.observe(item(75, transport_observed_at=START + timedelta(seconds=55), bytes_acked=6500))
    window.observe(item(90))
    window.observe(item(105))
    assert window.recovered_count == 0
    window.observe(item(120))
    assert window.recovered_count == window.network_recovered_count == 1


def test_durable_episode_covers_network_outage_between_collector_polls() -> None:
    window = baseline()
    completed = item(
        45,
        target="target-2",
        network_episode_id="episode-1",
        network_episode_sequence=1,
        network_episode_state="RECOVERED",
        network_episode_started_at=START + timedelta(seconds=34),
        network_episode_recovered_at=START + timedelta(seconds=43),
    )
    window.observe(completed)
    assert window.active is not None
    assert window.active.trigger == "NETWORK_DOWN"
    assert window.active.started_at == START + timedelta(seconds=34)
    assert window.active.network_recovered_at == START + timedelta(seconds=43)
    for seconds in (60, 75, 90):
        window.observe(
            item(
                seconds,
                target="target-2",
                network_episode_id="episode-1",
                network_episode_sequence=1,
                network_episode_state="RECOVERED",
                network_episode_started_at=START + timedelta(seconds=34),
                network_episode_recovered_at=START + timedelta(seconds=43),
            )
        )
    result = window.finish(START + timedelta(seconds=90))
    assert result["verified_network_recovered_episode_count"] == 1
    assert result["recent_episodes"][0]["network_unavailable_seconds"] == 9


def test_episode_from_before_baseline_is_not_reused_as_soak_exercise() -> None:
    window = RecoveryWindow()
    window.observe(
        item(
            0,
            network_episode_id="old-episode",
            network_episode_sequence=4,
            network_episode_state="RECOVERED",
            network_episode_started_at=START - timedelta(seconds=30),
            network_episode_recovered_at=START - timedelta(seconds=15),
        )
    )
    for seconds in (15, 30, 45):
        window.observe(
            item(
                seconds,
                network_episode_id="old-episode",
                network_episode_sequence=4,
                network_episode_state="RECOVERED",
                network_episode_started_at=START - timedelta(seconds=30),
                network_episode_recovered_at=START - timedelta(seconds=15),
            )
        )
    assert window.network_recovered_count == 0
    assert window.episode_count == 0


def test_episode_identity_conflict_is_a_terminal_blocker() -> None:
    window = baseline()
    common = {
        "network_episode_sequence": 1,
        "network_episode_state": "ACTIVE",
        "network_episode_started_at": START + timedelta(seconds=45),
    }
    window.observe(item(45, network="DOWN", network_episode_id="episode-1", **common))
    window.observe(item(60, network="DOWN", network_episode_id="episode-tampered", **common))
    assert "NETWORK_EPISODE_IDENTITY_CONFLICT" in window.blockers
