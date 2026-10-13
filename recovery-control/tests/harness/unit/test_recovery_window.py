from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from itertools import product

import pytest

from cra_no_action_soak.recovery_window import (
    MAXIMUM_TRACKED_ACK_TARGETS,
    RecoveryObservation,
    RecoveryPolicy,
    RecoveryWindow,
)

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
            cra_readiness="UP",
            cra_readiness_observed_at=now,
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


@pytest.mark.parametrize(
    ("changes", "trigger"),
    [
        ({"network": "DOWN", "network_observed_at": START + timedelta(seconds=35)}, "NETWORK_DOWN"),
        ({"platform": "DOWN", "platform_observed_at": START + timedelta(seconds=35)}, "PLATFORM_DOWN"),
        ({"control_path": "DOWN", "control_path_observed_at": START + timedelta(seconds=35)}, "CONTROL_PATH_DOWN"),
        ({"target": "target-2", "transport_observed_at": START + timedelta(seconds=35)}, "TARGET_TRANSITION"),
        ({"child_lifecycle": "ABSENT", "owner_observed_at": START + timedelta(seconds=35)}, "OWNER_CHILD_UNAVAILABLE"),
        ({"unresolved_effect_count": 1, "effects_observed_at": START + timedelta(seconds=35)}, "UNRESOLVED_EFFECT"),
    ],
    ids=["network", "platform", "control", "target", "owner", "effect"],
)
def test_late_signed_failure_does_not_shift_episode_onset_forward(
    changes: dict[str, object], trigger: str,
) -> None:
    window = baseline()
    window.observe(item(45, **changes))
    assert window.active is not None
    assert window.active.trigger == trigger
    assert window.active.started_at == START + timedelta(seconds=35)


def test_late_signed_network_recovery_uses_source_confirmation_for_deadline() -> None:
    window = baseline()
    window.observe(item(45, network="DOWN", network_observed_at=START + timedelta(seconds=35)))
    window.observe(item(60, network_observed_at=START + timedelta(seconds=50), platform="DOWN"))
    assert window.active is not None
    assert window.active.network_recovered_at == START + timedelta(seconds=50)
    assert window.active.deadline_at == START + timedelta(seconds=650)
    assert window.finish(START + timedelta(seconds=651))["deadline_exceeded_count"] == 1


@pytest.mark.parametrize("duration,blocked", [(60, False), (61, True)])
def test_cra_safe_block_has_an_independent_inclusive_duration_limit(duration: int, blocked: bool) -> None:
    window = baseline()
    window.observe(
        item(
            45,
            cra_readiness="DOWN",
            cra_readiness_observed_at=START + timedelta(seconds=45),
        )
    )
    result = window.finish(START + timedelta(seconds=45 + duration))

    assert window.active is None
    assert result["cra_safe_blocked_episode_count"] == 1
    assert result["cra_current_safe_blocked_duration_seconds"] == duration
    assert ("CRA_SAFE_BLOCKED_DURATION_EXCEEDED" in result["blockers"]) is blocked


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
    result = window.finish(START + timedelta(seconds=700))
    assert not result["blockers"]
    assert result["unknown_reasons"] == ["ACK_COUNTER_DISCONTINUITY_UNVERIFIED"]
    assert window.current_health == "UNKNOWN"


@pytest.mark.parametrize(
    "classification",
    ["RTMPS_ACK_STALL_FFMPEG_REPLACED", "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"],
)
def test_late_signed_replacement_resolves_prior_same_target_ack_reset(classification: str) -> None:
    window = baseline()
    window.observe(item(45, bytes_acked=0))
    window = RecoveryWindow.restore(window.checkpoint())
    assert window.finish(START + timedelta(seconds=179))["unknown_reasons"] == [
        "ACK_COUNTER_DISCONTINUITY_UNVERIFIED"
    ]

    window.observe(
        item(
            180,
            target="target-2",
            bytes_acked=20_000,
            rtmps_episode_id="replacement-1",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification=classification,
            rtmps_episode_started_at=START + timedelta(seconds=40),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=60),
            rtmps_episode_target="target-1",
            rtmps_episode_replacement_target="target-2",
        )
    )

    assert not window.ack_discontinuities
    assert not window.finish(START + timedelta(seconds=180))["unknown_reasons"]
    assert window.ack_high_watermarks == {"target-1": 4000, "target-2": 20_000}


def test_unrelated_signed_episode_cannot_resolve_ack_reset() -> None:
    window = baseline()
    window.observe(item(45, bytes_acked=0))
    window.observe(
        item(
            60,
            target="target-2",
            rtmps_episode_id="unrelated",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=40),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=55),
            rtmps_episode_target="other-target",
            rtmps_episode_replacement_target="target-2",
        )
    )
    assert set(window.ack_discontinuities) == {"target-1"}


def test_late_ack_reset_is_not_resolved_by_episode_starting_after_packet_visibility() -> None:
    window = baseline()
    late_reset = item(60, bytes_acked=0)
    late_reset = replace(
        late_reset,
        transport_observed_at=START + timedelta(seconds=45),
    )
    window.observe(late_reset)
    window.observe(
        item(
            75,
            target="target-2",
            rtmps_episode_id="later-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=65),
            rtmps_episode_detected_at=START + timedelta(seconds=66),
            rtmps_episode_recovered_at=START + timedelta(seconds=70),
            rtmps_episode_target="target-1",
            rtmps_episode_replacement_target="target-2",
        )
    )

    assert set(window.ack_discontinuities) == {"target-1"}
    assert window.finish(START + timedelta(seconds=75))["unknown_reasons"] == [
        "ACK_COUNTER_DISCONTINUITY_UNVERIFIED"
    ]


def test_ack_reset_source_before_episode_is_resolved_when_packet_became_visible_during_episode() -> None:
    window = baseline()
    window.observe(
        item(
            62,
            bytes_acked=0,
            transport_observed_at=START + timedelta(seconds=59),
        )
    )
    assert set(window.ack_discontinuities) == {"target-1"}
    window = RecoveryWindow.restore(window.checkpoint())

    window.observe(
        item(
            82,
            target="target-2",
            rtmps_episode_id="source-visibility-skew",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=61),
            rtmps_episode_detected_at=START + timedelta(seconds=61, milliseconds=320),
            rtmps_episode_recovered_at=START + timedelta(seconds=80),
            rtmps_episode_target="target-1",
            rtmps_episode_replacement_target="target-2",
        )
    )

    assert not window.ack_discontinuities
    assert not window.finish(START + timedelta(seconds=82))["unknown_reasons"]


def test_late_retired_target_updates_only_its_own_audit_high_water() -> None:
    window = baseline()
    window.observe(item(45, target="target-2", bytes_acked=3))
    current = window.checkpoint()
    window.observe(
        item(
            60,
            target="target-1",
            transport_observed_at=START + timedelta(seconds=45),
            bytes_acked=5000,
        )
    )
    assert window.last_target == "target-2"
    assert window.last_bytes_acked == current["last_bytes_acked"]
    assert window.ack_high_watermarks == {"target-1": 5000, "target-2": 3}
    assert not window.blockers


def test_unseen_late_target_cannot_replace_active_target() -> None:
    window = RecoveryWindow()
    for second, acknowledged in ((0, 1), (15, 2), (30, 3)):
        window.observe(item(second, target="target-current", bytes_acked=acknowledged))
    window = RecoveryWindow.restore(window.checkpoint())
    checkpoint = window.checkpoint()

    window.observe(
        item(
            45,
            target="target-unseen-predecessor",
            transport_observed_at=START + timedelta(seconds=10),
            bytes_acked=50_000,
        )
    )

    assert window.last_target == "target-current"
    assert window.last_transport_at == START + timedelta(seconds=30)
    assert window.episode_count == 0
    assert window.active is None
    assert "target-unseen-predecessor" not in window.ack_high_watermarks
    assert window.finish(START + timedelta(seconds=45))["unknown_reasons"] == [
        "TRANSPORT_TARGET_ORDER_UNVERIFIED"
    ]
    first_revision = window.finish(START + timedelta(seconds=45))["observation_revision"]["transport_target_order"]
    assert first_revision["revision"] == 1
    restored = RecoveryWindow.restore(window.checkpoint())
    assert restored.checkpoint() == window.checkpoint()
    assert restored.finish(START + timedelta(seconds=45)) == window.finish(START + timedelta(seconds=45))
    assert restored.last_target == checkpoint["last_target"]

    restored.observe(
        item(
            46,
            target="target-unseen-predecessor",
            transport_observed_at=START + timedelta(seconds=10),
            bytes_acked=50_000,
        )
    )
    assert restored.finish(START + timedelta(seconds=46))["observation_revision"]["transport_target_order"] == first_revision

    restored.observe(
        item(
            47,
            target="target-unseen-predecessor",
            transport_observed_at=START + timedelta(seconds=10),
            bytes_acked=50_001,
        )
    )
    advanced = restored.finish(START + timedelta(seconds=47))["observation_revision"]["transport_target_order"]
    assert advanced["revision"] == 2
    assert advanced["digest"] != first_revision["digest"]


def test_unseen_two_generations_old_target_cannot_replace_owner_successor() -> None:
    window = RecoveryWindow()
    for second, acknowledged in ((0, 1), (15, 2), (30, 3)):
        window.observe(item(second, target="target-current", bytes_acked=acknowledged))
    window.observe(
        item(
            45,
            target=None,
            transport_observed_at=None,
            bytes_acked=None,
            child_lifecycle="RUNNING",
            owner_target="target-owner-successor",
            owner_observed_at=START + timedelta(seconds=45),
        )
    )
    assert window.last_transport_at is None
    assert window.latest_transport_source_at == START + timedelta(seconds=30)

    window = RecoveryWindow.restore(window.checkpoint())
    window.observe(
        item(
            60,
            target="target-unseen-predecessor",
            transport_observed_at=START + timedelta(seconds=20),
            bytes_acked=50_000,
        )
    )

    assert window.last_target == "target-owner-successor"
    assert window.active is not None
    assert window.active.origin_target == "target-current"
    assert window.active.replacement_target == "target-owner-successor"
    assert window.finish(START + timedelta(seconds=60))["unknown_reasons"] == [
        "TRANSPORT_TARGET_ORDER_UNVERIFIED"
    ]


def test_transport_source_high_water_checkpoint_cannot_move_behind_active_target() -> None:
    checkpoint = baseline().checkpoint()
    checkpoint["latest_transport_source_at"] = "2026-09-03T23:59:59.000Z"

    with pytest.raises(ValueError, match="RECOVERY_TRANSPORT_SOURCE_HIGH_WATER_INVALID"):
        RecoveryWindow.restore(checkpoint)


def test_target_scoped_ack_history_is_bounded_and_fails_closed() -> None:
    window = baseline()
    for number in range(2, MAXIMUM_TRACKED_ACK_TARGETS + 1):
        window.observe(item(30 + number, target=f"target-{number}", bytes_acked=number))
    accepted_target = window.last_target
    window.observe(item(200, target="target-over-limit", bytes_acked=1))

    assert window.last_target == accepted_target
    assert "target-over-limit" not in window.ack_high_watermarks
    assert "target-over-limit" not in window.retired_targets
    assert window.finish(START + timedelta(seconds=200))["unknown_reasons"] == [
        "TRANSPORT_TARGET_HISTORY_EXHAUSTED"
    ]
    assert RecoveryWindow.restore(window.checkpoint()).last_target == accepted_target


def test_target_ack_state_machine_cross_product_is_checkpoint_stable() -> None:
    """Enumerate target, late-event, episode and restart combinations."""

    def run(*, checkpoint_mask: int, late_retired: bool, episode_kind: str) -> tuple[dict[str, object], dict[str, object]]:
        window = baseline()
        observations = [
            item(45, bytes_acked=0),
            item(60, target="target-2", bytes_acked=100),
            (
                item(
                    75,
                    target="target-1",
                    transport_observed_at=START + timedelta(seconds=50),
                    bytes_acked=5000,
                )
                if late_retired
                else item(75, target="target-2", bytes_acked=200)
            ),
        ]
        episode_changes: dict[str, object] = {}
        if episode_kind != "absent":
            episode_changes = {
                "rtmps_episode_id": "episode-1",
                "rtmps_episode_sequence": 1,
                "rtmps_episode_state": "RECOVERED",
                "rtmps_episode_classification": "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
                "rtmps_episode_started_at": START + timedelta(seconds=50 if episode_kind == "postdated" else 40),
                "rtmps_episode_detected_at": START + timedelta(seconds=55),
                "rtmps_episode_recovered_at": START + timedelta(seconds=60),
                "rtmps_episode_target": "other-target" if episode_kind == "unrelated" else "target-1",
                "rtmps_episode_replacement_target": "target-2",
            }
        observations.append(item(90, target="target-2", bytes_acked=300, **episode_changes))
        for index, observation in enumerate(observations):
            window.observe(observation)
            if checkpoint_mask & (1 << index):
                window = RecoveryWindow.restore(window.checkpoint())
        return window.checkpoint(), window.finish(START + timedelta(seconds=90))

    for late_retired, episode_kind in product(
        (False, True),
        ("covered", "postdated", "unrelated", "absent"),
    ):
        expected = run(checkpoint_mask=0, late_retired=late_retired, episode_kind=episode_kind)
        for checkpoint_mask in range(16):
            assert run(
                checkpoint_mask=checkpoint_mask,
                late_retired=late_retired,
                episode_kind=episode_kind,
            ) == expected
        unknowns = expected[1]["unknown_reasons"]
        if episode_kind == "covered":
            assert unknowns == []
        else:
            assert unknowns == ["ACK_COUNTER_DISCONTINUITY_UNVERIFIED"]


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


def test_durable_rtmps_episode_retimes_later_child_transition_without_claiming_wan_recovery() -> None:
    window = baseline()
    before = "target-1"
    after = "b" * 64
    window.observe(item(45, target=before, bytes_acked=4000))
    assert window.current_health == "UNKNOWN"
    window.observe(
        item(
            60,
            target=after,
            rtmps_episode_id="rtmps-episode-1",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="RTMPS_ACK_STALL_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=45),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=60),
            rtmps_episode_target=before,
            rtmps_episode_replacement_target=after,
        )
    )
    assert window.active is not None
    assert window.active.trigger == "RTMPS_SESSION_FAILURE"
    assert window.active.started_at == START + timedelta(seconds=45)
    assert window.active.deadline_at == START + timedelta(seconds=645)
    for seconds in (75, 90):
        window.observe(item(seconds, target=after))
    result = window.finish(START + timedelta(seconds=90))
    assert result["recovered_episode_count"] == 1
    assert result["verified_network_recovered_episode_count"] == 0


def test_late_matching_rtmps_episode_merges_platform_first_episode() -> None:
    window = baseline()
    before = "target-1"
    after = "b" * 64
    window.observe(
        item(
            45,
            platform="DOWN",
            platform_observed_at=START + timedelta(seconds=35),
        )
    )
    assert window.active is not None
    assert window.active.trigger == "PLATFORM_DOWN"

    window.observe(
        item(
            60,
            target=after,
            rtmps_episode_id="platform-first-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=35),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=60),
            rtmps_episode_target=before,
            rtmps_episode_replacement_target=after,
        )
    )

    assert window.active is not None
    assert window.active.trigger == "RTMPS_SESSION_FAILURE"
    assert window.active.rtmps_episode_id == "platform-first-episode"
    assert window.active.started_at == START + timedelta(seconds=35)
    assert window.active.deadline_at == START + timedelta(seconds=635)
    assert "RTMPS_EPISODE_OVERLAP" not in window.blockers


def test_active_rtmps_episode_provisionally_binds_platform_first_episode_until_replacement() -> None:
    window = baseline()
    before = "target-1"
    after = "b" * 64
    window.observe(
        item(
            45,
            platform="DOWN",
            platform_observed_at=START + timedelta(seconds=35),
        )
    )
    window.observe(
        item(
            60,
            platform="DOWN",
            rtmps_episode_id="active-platform-first-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="ACTIVE",
            rtmps_episode_classification="RTMPS_ACK_STALL",
            rtmps_episode_started_at=START + timedelta(seconds=30),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_target=before,
        )
    )
    assert window.active is not None
    assert window.active.trigger == "RTMPS_SESSION_FAILURE"
    assert window.active.rtmps_episode_id == "active-platform-first-episode"
    assert window.active.started_at == START + timedelta(seconds=30)
    assert window.active.deadline_at == START + timedelta(seconds=630)
    assert "RTMPS_EPISODE_OVERLAP" not in window.blockers

    window.observe(
        item(
            75,
            target=after,
            rtmps_episode_id="active-platform-first-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="RTMPS_ACK_STALL_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=30),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=70),
            rtmps_episode_target=before,
            rtmps_episode_replacement_target=after,
        )
    )
    assert window.active is not None
    assert window.active.replacement_target == after
    assert "RTMPS_EPISODE_OVERLAP" not in window.blockers


def test_active_rtmps_platform_binding_rejects_mismatched_replacement() -> None:
    window = baseline()
    window.observe(
        item(
            45,
            platform="DOWN",
            platform_observed_at=START + timedelta(seconds=35),
        )
    )
    window.observe(
        item(
            60,
            platform="DOWN",
            rtmps_episode_id="mismatched-platform-first-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="ACTIVE",
            rtmps_episode_classification="RTMPS_ACK_STALL",
            rtmps_episode_started_at=START + timedelta(seconds=30),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_target="target-1",
        )
    )
    assert "RTMPS_EPISODE_OVERLAP" not in window.blockers
    window.observe(
        item(
            75,
            target="target-2",
            rtmps_episode_id="mismatched-platform-first-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="RTMPS_ACK_STALL_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=30),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=70),
            rtmps_episode_target="target-1",
            rtmps_episode_replacement_target="target-3",
        )
    )
    assert "RTMPS_EPISODE_OVERLAP" in window.blockers


@pytest.mark.parametrize(
    ("changes", "trigger"),
    [
        (
            {"control_path": "DOWN", "control_path_observed_at": START + timedelta(seconds=35)},
            "CONTROL_PATH_DOWN",
        ),
        (
            {"unresolved_effect_count": 1, "effects_observed_at": START + timedelta(seconds=35)},
            "UNRESOLVED_EFFECT",
        ),
    ],
    ids=["control", "effect"],
)
def test_late_rtmps_episode_does_not_merge_other_failure_domains(
    changes: dict[str, object], trigger: str,
) -> None:
    window = baseline()
    window.observe(item(45, **changes))
    assert window.active is not None
    assert window.active.trigger == trigger

    window.observe(
        item(
            60,
            target="target-2",
            rtmps_episode_id="unrelated-domain-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=35),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=60),
            rtmps_episode_target="target-1",
            rtmps_episode_replacement_target="target-2",
        )
    )

    assert window.active is not None
    assert window.active.trigger == trigger
    assert "RTMPS_EPISODE_OVERLAP" in window.blockers


def test_late_matching_rtmps_episode_merges_recent_platform_episode_once() -> None:
    window = baseline()
    before = "target-1"
    after = "b" * 64
    window.observe(
        item(
            45,
            platform="DOWN",
            platform_observed_at=START + timedelta(seconds=35),
        )
    )
    for seconds in (60, 75, 90):
        window.observe(item(seconds, target=after))
    assert window.active is None
    assert window.recent_episodes[-1]["trigger"] == "PLATFORM_DOWN"

    window.observe(
        item(
            105,
            target=after,
            rtmps_episode_id="late-platform-first-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=35),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=60),
            rtmps_episode_target=before,
            rtmps_episode_replacement_target=after,
        )
    )

    assert window.episode_count == 1
    assert window.recent_episodes[-1]["trigger"] == "PLATFORM_DOWN"
    assert window.recent_episodes[-1]["rtmps_episode_id"] == "late-platform-first-episode"
    assert "RTMPS_EPISODE_OVERLAP" not in window.blockers


def test_late_rtmps_episode_does_not_merge_concurrent_platform_and_control_failure() -> None:
    window = baseline()
    window.observe(
        item(
            45,
            platform="DOWN",
            platform_observed_at=START + timedelta(seconds=35),
            control_path="DOWN",
            control_path_observed_at=START + timedelta(seconds=35),
        )
    )
    assert window.active is not None
    assert window.active.trigger == "MULTI_DOMAIN_DOWN"

    window.observe(
        item(
            60,
            target="target-2",
            rtmps_episode_id="multi-domain-episode",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=35),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=60),
            rtmps_episode_target="target-1",
            rtmps_episode_replacement_target="target-2",
        )
    )

    assert window.active is not None
    assert window.active.trigger == "MULTI_DOMAIN_DOWN"
    assert "RTMPS_EPISODE_OVERLAP" in window.blockers


def test_durable_session_loss_episode_retimes_abrupt_child_replacement_without_claiming_cause() -> None:
    window = baseline()
    before = "target-1"
    after = "b" * 64
    window.observe(item(45, target=before, bytes_acked=4000))
    assert window.current_health == "UNKNOWN"
    window.observe(
        item(
            60,
            target=after,
            rtmps_episode_id="session-loss-episode-1",
            rtmps_episode_sequence=1,
            rtmps_episode_state="RECOVERED",
            rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            rtmps_episode_started_at=START + timedelta(seconds=45),
            rtmps_episode_detected_at=START + timedelta(seconds=45),
            rtmps_episode_recovered_at=START + timedelta(seconds=60),
            rtmps_episode_target=before,
            rtmps_episode_replacement_target=after,
        )
    )
    assert window.active is not None
    assert window.active.trigger == "RTMPS_SESSION_FAILURE"
    assert window.active.started_at == START + timedelta(seconds=45)
    for seconds in (75, 90):
        window.observe(item(seconds, target=after))
    result = window.finish(START + timedelta(seconds=90))
    assert result["recovered_episode_count"] == 1
    assert result["verified_network_recovered_episode_count"] == 0


def test_microsecond_rtmps_identity_survives_every_sample_checkpoint() -> None:
    started = START + timedelta(seconds=40, microseconds=123456)
    recovered = START + timedelta(seconds=50, microseconds=234567)
    signed = dict(
        target="target-2",
        rtmps_episode_id="session-1",
        rtmps_episode_sequence=1,
        rtmps_episode_state="RECOVERED",
        rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
        rtmps_episode_started_at=started,
        rtmps_episode_detected_at=START + timedelta(seconds=45),
        rtmps_episode_recovered_at=recovered,
        rtmps_episode_target="target-1",
        rtmps_episode_replacement_target="target-2",
    )
    continuous = baseline()
    checkpointed = RecoveryWindow.restore(continuous.checkpoint())
    for seconds in (60, 75):
        observation = item(seconds, **signed)
        continuous.observe(observation)
        checkpointed.observe(observation)
        checkpointed = RecoveryWindow.restore(checkpointed.checkpoint())
    assert continuous.checkpoint() == checkpointed.checkpoint()
    assert not checkpointed.blockers
    assert checkpointed.last_rtmps_episode_started_at == started
    assert checkpointed.last_rtmps_episode_recovered_at == recovered
    tampered = RecoveryWindow.restore(checkpointed.checkpoint())
    tampered.observe(item(90, **dict(signed, target="target-3", rtmps_episode_replacement_target="target-3")))
    assert "RTMPS_EPISODE_IDENTITY_CONFLICT" in tampered.blockers


@pytest.mark.parametrize("state", ["ACTIVE", "RECOVERED"])
def test_microsecond_network_identity_survives_checkpoint(state: str) -> None:
    started = START + timedelta(seconds=40, microseconds=123456)
    recovered = START + timedelta(seconds=50, microseconds=234567) if state == "RECOVERED" else None
    signed = dict(
        network="DOWN" if state == "ACTIVE" else "UP",
        network_episode_id="network-1",
        network_episode_sequence=1,
        network_episode_state=state,
        network_episode_started_at=started,
        network_episode_recovered_at=recovered,
    )
    continuous = baseline()
    checkpointed = RecoveryWindow.restore(continuous.checkpoint())
    for seconds in (60, 75):
        observation = item(seconds, **signed)
        continuous.observe(observation)
        checkpointed.observe(observation)
        checkpointed = RecoveryWindow.restore(checkpointed.checkpoint())
    assert continuous.checkpoint() == checkpointed.checkpoint()
    assert not checkpointed.blockers


def test_same_microsecond_ack_source_conflict_survives_checkpoint() -> None:
    window = baseline()
    source_time = START + timedelta(seconds=45, microseconds=123456)
    window.observe(item(60, transport_observed_at=source_time, bytes_acked=9000))
    window = RecoveryWindow.restore(window.checkpoint())
    window.observe(item(75, transport_observed_at=source_time, bytes_acked=10000))
    assert "TRANSPORT_SOURCE_TIME_CONFLICT" in window.blockers


def test_same_microsecond_ack_source_repeat_preserves_progress() -> None:
    window = baseline()
    for seconds in (45, 60, 75):
        window.observe(item(seconds, transport_observed_at=START + timedelta(seconds=seconds - 1, microseconds=123456)))
        window = RecoveryWindow.restore(window.checkpoint())
    points = window.ack_point_count
    window.observe(item(90, transport_observed_at=START + timedelta(seconds=74, microseconds=123456), bytes_acked=8500))
    assert window.ack_point_count == points
    assert window.current_health == "READY"


def test_microsecond_deadline_does_not_expire_early_after_checkpoint() -> None:
    window = baseline()
    started = START + timedelta(seconds=40, microseconds=900)
    window.observe(item(60, target="target-2", transport_observed_at=started, network_observed_at=started))
    assert window.active is not None
    assert window.active.deadline_at == started + timedelta(seconds=600)
    window = RecoveryWindow.restore(window.checkpoint())
    assert "RECOVERY_DEADLINE_EXCEEDED" not in window.finish(started + timedelta(seconds=600, microseconds=-400))["blockers"]
    assert "RECOVERY_DEADLINE_EXCEEDED" in window.finish(started + timedelta(seconds=600, microseconds=1))["blockers"]


def test_late_session_loss_matches_owner_replacement_without_extending_deadline() -> None:
    window = baseline()
    window.observe(item(60, target=None, transport_observed_at=None, bytes_acked=None,
                        child_lifecycle="ABSENT", owner_observed_at=START + timedelta(seconds=45)))
    window = RecoveryWindow.restore(window.checkpoint())
    assert window.active is not None
    original_deadline = window.active.deadline_at
    window.observe(item(
        75, target="target-2", child_lifecycle="RUNNING", owner_target="target-2",
        rtmps_episode_id="session-1", rtmps_episode_sequence=1,
        rtmps_episode_state="RECOVERED", rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
        rtmps_episode_started_at=START + timedelta(seconds=46, microseconds=61621),
        rtmps_episode_detected_at=START + timedelta(seconds=50),
        rtmps_episode_recovered_at=START + timedelta(seconds=55),
        rtmps_episode_target="target-1", rtmps_episode_replacement_target="target-2",
    ))
    assert not window.blockers
    assert window.active is not None
    assert window.active.trigger == "RTMPS_SESSION_FAILURE"
    assert window.active.started_at == START + timedelta(seconds=45)
    assert window.active.deadline_at is not None and original_deadline is not None
    assert window.active.deadline_at <= original_deadline


def test_late_session_loss_after_owner_completion_is_not_a_second_recovery() -> None:
    window = baseline()
    window.observe(item(45, target=None, transport_observed_at=None, bytes_acked=None,
                        child_lifecycle="ABSENT", owner_observed_at=START + timedelta(seconds=40)))
    for seconds in (60, 75, 90):
        window.observe(item(seconds, target="target-2", child_lifecycle="RUNNING", owner_target="target-2"))
        window = RecoveryWindow.restore(window.checkpoint())
    assert window.active is None and window.recovered_count == 1
    late = item(
        105, target="target-2", child_lifecycle="RUNNING", owner_target="target-2",
        rtmps_episode_id="session-1", rtmps_episode_sequence=1, rtmps_episode_state="RECOVERED",
        rtmps_episode_classification="FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
        rtmps_episode_started_at=START + timedelta(seconds=41),
        rtmps_episode_detected_at=START + timedelta(seconds=41),
        rtmps_episode_recovered_at=START + timedelta(seconds=55),
        rtmps_episode_target="target-1", rtmps_episode_replacement_target="target-2",
    )
    window.observe(late)
    assert window.active is None
    assert window.episode_count == window.recovered_count == 1
    assert window.current_health == "READY"
    assert window.recent_episodes[-1]["rtmps_episode_id"] == "session-1"
    assert not window.blockers

    unrelated = RecoveryWindow.restore(window.checkpoint())
    unrelated.observe(replace(late, observed_at=START + timedelta(seconds=120),
                              rtmps_episode_id="session-2", rtmps_episode_sequence=2,
                              rtmps_episode_target="other-old"))
    assert unrelated.episode_count == 2
