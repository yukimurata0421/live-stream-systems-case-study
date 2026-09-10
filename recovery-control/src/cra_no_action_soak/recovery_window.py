"""Pure recovery-deadline oracle. No network, command dispatch, or effect path.

The caller supplies admitted, source-time observations, not dashboard health.
Pi publication observations are deliberately absent from this interface.
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta
from typing import Any, Literal

from cra_dell_recovery.time import isoformat_utc, parse_utc

Health = Literal["UP", "DOWN", "UNKNOWN"]


@dataclass(frozen=True)
class RecoveryPolicy:
    deadline_seconds: int = 600
    ack_stall_seconds: int = 45
    maximum_source_age_seconds: int = 45
    maximum_platform_age_seconds: int = 120
    minimum_ack_points: int = 3

    def __post_init__(self) -> None:
        if self.deadline_seconds != 600:
            raise ValueError("RECOVERY_DEADLINE_MUST_BE_600_SECONDS")
        for value in (self.ack_stall_seconds, self.maximum_source_age_seconds, self.maximum_platform_age_seconds):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value < self.deadline_seconds:
                raise ValueError("RECOVERY_SUB_DEADLINE_INVALID")
        if self.minimum_ack_points != 3:
            raise ValueError("RECOVERY_REQUIRES_THREE_ACK_POINTS")


@dataclass(frozen=True)
class RecoveryObservation:
    observed_at: datetime
    network: Health
    network_observed_at: datetime | None
    target: str | None
    transport_observed_at: datetime | None
    bytes_acked: int | None
    stream_id: str
    platform: Health
    platform_observed_at: datetime | None
    platform_stream_id: str | None
    control_path: Health
    unresolved_effect_count: int | None
    network_episode_id: str | None = None
    network_episode_sequence: int | None = None
    network_episode_state: str | None = None
    network_episode_started_at: datetime | None = None
    network_episode_recovered_at: datetime | None = None
    child_lifecycle: str | None = None
    owner_target: str | None = None


@dataclass
class RecoveryEpisode:
    episode_id: int
    started_at: datetime
    trigger: str
    network_recovered_at: datetime | None = None
    deadline_at: datetime | None = None
    transport_recovered_at: datetime | None = None
    platform_recovered_at: datetime | None = None
    completed_at: datetime | None = None
    timed_out: bool = False
    network_flap_count: int = 0
    unknown_sample_count: int = 0

    def value(self) -> dict[str, Any]:
        result = asdict(self)
        for name, item in result.items():
            if isinstance(item, datetime):
                result[name] = isoformat_utc(item)
        result["status"] = "DEADLINE_EXCEEDED" if self.timed_out else "RECOVERED" if self.completed_at else "OPEN"
        result["recovery_seconds"] = (
            (self.completed_at - self.network_recovered_at).total_seconds()
            if self.completed_at is not None and self.network_recovered_at is not None
            else None
        )
        result["network_unavailable_seconds"] = (
            (self.network_recovered_at - self.started_at).total_seconds() if self.network_recovered_at else None
        )
        return result


@dataclass
class RecoveryWindow:
    policy: RecoveryPolicy = field(default_factory=RecoveryPolicy)
    episode_count: int = 0
    recovered_count: int = 0
    network_recovered_count: int = 0
    exceeded_count: int = 0
    active: RecoveryEpisode | None = None
    recent_episodes: list[dict[str, Any]] = field(default_factory=list)
    maximum_recovery_seconds: float = 0.0
    maximum_observed_network_unavailable_seconds: float = 0.0
    baseline_at: datetime | None = None
    last_observed_at: datetime | None = None
    last_network: Health = "UNKNOWN"
    last_target: str | None = None
    last_transport_at: datetime | None = None
    last_bytes_acked: int | None = None
    last_ack_progress_at: datetime | None = None
    ack_point_count: int = 0
    current_health: str = "UNKNOWN"
    blockers: set[str] = field(default_factory=set)
    last_network_episode_id: str | None = None
    last_network_episode_sequence: int = 0
    last_network_episode_state: str | None = None
    last_network_episode_started_at: datetime | None = None
    last_network_episode_recovered_at: datetime | None = None

    def checkpoint(self) -> dict[str, Any]:
        def encode(value: Any) -> Any:
            if isinstance(value, datetime):
                return isoformat_utc(value)
            if isinstance(value, set):
                return sorted(value)
            if isinstance(value, dict):
                return {k: encode(v) for k, v in value.items()}
            if isinstance(value, list):
                return [encode(v) for v in value]
            return value

        return cast_checkpoint(encode(asdict(self)))

    @classmethod
    def restore(cls, checkpoint: dict[str, Any]) -> RecoveryWindow:
        if set(checkpoint) != {f.name for f in fields(cls)}:
            raise ValueError("RECOVERY_CHECKPOINT_FIELDS_INVALID")
        value = copy.deepcopy(checkpoint)
        value["policy"] = RecoveryPolicy(**value["policy"])
        for key in (
            "baseline_at",
            "last_observed_at",
            "last_transport_at",
            "last_ack_progress_at",
            "last_network_episode_started_at",
            "last_network_episode_recovered_at",
        ):
            value[key] = parse_utc(value[key]) if value[key] is not None else None
        if value["active"] is not None:
            episode = dict(value["active"])
            if set(episode) != {f.name for f in fields(RecoveryEpisode)}:
                raise ValueError("RECOVERY_EPISODE_CHECKPOINT_INVALID")
            for key in (
                "started_at",
                "network_recovered_at",
                "deadline_at",
                "transport_recovered_at",
                "platform_recovered_at",
                "completed_at",
            ):
                episode[key] = parse_utc(episode[key]) if episode[key] is not None else None
            value["active"] = RecoveryEpisode(**episode)
        value["blockers"] = set(value["blockers"])
        return cls(**value)

    @staticmethod
    def _fresh(source: datetime | None, now: datetime, maximum_age: int) -> bool:
        return source is not None and 0 <= (now - source).total_seconds() <= maximum_age

    def _transport(self, item: RecoveryObservation) -> Health:
        if (
            not self._fresh(item.transport_observed_at, item.observed_at, self.policy.maximum_source_age_seconds)
            or not item.target
            or isinstance(item.bytes_acked, bool)
            or not isinstance(item.bytes_acked, int)
            or item.bytes_acked < 0
        ):
            self.ack_point_count = 0
            return "UNKNOWN"
        assert item.transport_observed_at is not None
        if (
            self.active is not None
            and self.active.network_recovered_at is not None
            and item.transport_observed_at < self.active.network_recovered_at
        ):
            self.ack_point_count = 0
            return "UNKNOWN"
        if item.target != self.last_target:
            self.last_target = item.target
            self.last_transport_at = None
            self.last_bytes_acked = None
            self.last_ack_progress_at = item.transport_observed_at
            self.ack_point_count = 0
        if self.last_transport_at is not None and item.transport_observed_at < self.last_transport_at:
            self.blockers.add("TRANSPORT_SOURCE_TIME_REGRESSION")
            return "UNKNOWN"
        if item.transport_observed_at == self.last_transport_at:
            if item.bytes_acked != self.last_bytes_acked:
                self.blockers.add("TRANSPORT_SOURCE_TIME_CONFLICT")
                return "UNKNOWN"
        else:
            if (
                self.last_transport_at is not None
                and (item.transport_observed_at - self.last_transport_at).total_seconds() > self.policy.maximum_source_age_seconds
            ):
                self.ack_point_count = 0
            if self.last_bytes_acked is not None:
                if item.bytes_acked < self.last_bytes_acked:
                    self.blockers.add("ACK_COUNTER_REGRESSION_WITHIN_TARGET")
                    self.ack_point_count = 0
                    return "UNKNOWN"
                if item.bytes_acked > self.last_bytes_acked:
                    self.ack_point_count += 1
                    self.last_ack_progress_at = item.transport_observed_at
                else:
                    self.ack_point_count = 1
            else:
                self.ack_point_count = 1
                self.last_ack_progress_at = item.transport_observed_at
            self.last_transport_at = item.transport_observed_at
            self.last_bytes_acked = item.bytes_acked
        if (
            self.last_ack_progress_at is not None
            and (item.observed_at - self.last_ack_progress_at).total_seconds() >= self.policy.ack_stall_seconds
        ):
            return "DOWN"
        return "UP" if self.ack_point_count >= self.policy.minimum_ack_points else "UNKNOWN"

    def _durable_network_episode(self, item: RecoveryObservation) -> bool:
        values = (
            item.network_episode_id,
            item.network_episode_sequence,
            item.network_episode_state,
            item.network_episode_started_at,
            item.network_episode_recovered_at,
        )
        if all(value is None for value in values):
            return False
        if (
            not isinstance(item.network_episode_id, str)
            or not item.network_episode_id
            or isinstance(item.network_episode_sequence, bool)
            or not isinstance(item.network_episode_sequence, int)
            or item.network_episode_sequence < 1
            or item.network_episode_state not in {"ACTIVE", "RECOVERED"}
            or item.network_episode_started_at is None
            or item.network_episode_started_at > item.observed_at
            or (item.network_episode_state == "ACTIVE") != (item.network_episode_recovered_at is None)
            or (
                item.network_episode_recovered_at is not None
                and not item.network_episode_started_at <= item.network_episode_recovered_at <= item.observed_at
            )
        ):
            self.blockers.add("NETWORK_EPISODE_INVALID")
            return False

        sequence = item.network_episode_sequence
        episode_id = item.network_episode_id
        started_at = item.network_episode_started_at
        recovered_at = item.network_episode_recovered_at
        if sequence < self.last_network_episode_sequence:
            self.blockers.add("NETWORK_EPISODE_SEQUENCE_REGRESSION")
            return False
        new_episode = sequence > self.last_network_episode_sequence
        if not new_episode:
            if episode_id != self.last_network_episode_id or started_at != self.last_network_episode_started_at:
                self.blockers.add("NETWORK_EPISODE_IDENTITY_CONFLICT")
                return False
            if self.last_network_episode_state == "RECOVERED" and item.network_episode_state != "RECOVERED":
                self.blockers.add("NETWORK_EPISODE_STATE_REGRESSION")
                return False
            if self.last_network_episode_recovered_at is not None and recovered_at != self.last_network_episode_recovered_at:
                self.blockers.add("NETWORK_EPISODE_RECOVERY_CONFLICT")
                return False
        elif self.last_network_episode_sequence and sequence != self.last_network_episode_sequence + 1:
            self.blockers.add("NETWORK_EPISODE_SEQUENCE_GAP")

        self.last_network_episode_id = episode_id
        self.last_network_episode_sequence = sequence
        self.last_network_episode_state = item.network_episode_state
        self.last_network_episode_started_at = started_at
        self.last_network_episode_recovered_at = recovered_at

        eligible = self.baseline_at is not None and started_at >= self.baseline_at
        if new_episode and eligible:
            if self.active is None:
                self.episode_count += 1
                self.active = RecoveryEpisode(self.episode_count, started_at, "NETWORK_DOWN")
            elif self.active.trigger != "NETWORK_DOWN" and started_at <= self.active.started_at:
                # The signed Dell edge explains a point-in-time target/control
                # transition from the same outage; retain the earlier clock.
                self.active.started_at = started_at
                self.active.trigger = "NETWORK_DOWN"
            else:
                self.blockers.add("NETWORK_EPISODE_OVERLAP")
                return False
            self.ack_point_count = min(self.ack_point_count, 1)

        episode = self.active
        if (
            episode is not None
            and episode.trigger == "NETWORK_DOWN"
            and episode.started_at == started_at
            and item.network_episode_state == "RECOVERED"
            and recovered_at is not None
        ):
            if episode.network_recovered_at is not None and episode.network_recovered_at != recovered_at:
                self.blockers.add("NETWORK_EPISODE_RECOVERY_CONFLICT")
                return False
            recovered_now = episode.network_recovered_at is None
            if recovered_now:
                episode.network_recovered_at = recovered_at
                episode.deadline_at = recovered_at + timedelta(seconds=self.policy.deadline_seconds)
                self.maximum_observed_network_unavailable_seconds = max(
                    self.maximum_observed_network_unavailable_seconds,
                    (recovered_at - started_at).total_seconds(),
                )
                self.ack_point_count = (
                    min(self.ack_point_count, 1)
                    if item.transport_observed_at is not None and item.transport_observed_at >= recovered_at
                    else 0
                )
            return recovered_now or (new_episode and eligible)
        return new_episode and eligible

    def observe(self, item: RecoveryObservation) -> None:
        now = item.observed_at
        if now.tzinfo is None or (self.last_observed_at is not None and now <= self.last_observed_at):
            self.blockers.add("RECOVERY_OBSERVATION_TIME_INVALID")
            return
        self.last_observed_at = now
        network: Health = item.network if self._fresh(item.network_observed_at, now, self.policy.maximum_source_age_seconds) else "UNKNOWN"
        prior_target = self.last_target
        transport = self._transport(item)
        durable_network_transition = self._durable_network_episode(item)
        target_changed = prior_target is not None and item.target is not None and item.target != prior_target
        owner_target_changed_during_transport_convergence = (
            prior_target is not None
            and item.target is None
            and item.bytes_acked is None
            and item.child_lifecycle == "RUNNING"
            and isinstance(item.owner_target, str)
            and bool(item.owner_target)
            and item.owner_target != prior_target
        )
        if owner_target_changed_during_transport_convergence:
            # The signed owner has proved an exact successor before the
            # transport reader has produced its first target/ACK. Start the
            # bounded transition here, but never carry old transport progress
            # into the successor. A generic null or unverified lifecycle does
            # not enter this branch.
            self.last_target = item.owner_target
            self.last_transport_at = None
            self.last_bytes_acked = None
            self.last_ack_progress_at = None
            self.ack_point_count = 0
        platform: Health = (
            item.platform if self._fresh(item.platform_observed_at, now, self.policy.maximum_platform_age_seconds) else "UNKNOWN"
        )
        if not item.stream_id or item.platform_stream_id != item.stream_id:
            platform = "UNKNOWN"
        all_up = (
            network == transport == platform == item.control_path == "UP"
            and item.unresolved_effect_count == 0
            and item.child_lifecycle in (None, "RUNNING")
        )
        child_down = item.child_lifecycle in ("ABSENT", "TRANSITION")
        known_down = (
            child_down
            or target_changed
            or owner_target_changed_during_transport_convergence
            or "DOWN" in (network, transport, platform, item.control_path)
            or (type(item.unresolved_effect_count) is int and item.unresolved_effect_count > 0)
        )
        if self.active is None and known_down:
            self.episode_count += 1
            trigger = (
                "NETWORK_DOWN"
                if network == "DOWN"
                else "OWNER_CHILD_UNAVAILABLE"
                if child_down
                else "TARGET_TRANSITION"
                if target_changed or owner_target_changed_during_transport_convergence
                else "DELIVERY_OR_CONTROL_DOWN"
            )
            self.active = RecoveryEpisode(self.episode_count, now, trigger)
            # Recovery must prove three new ACK points after this episode, never
            # reuse pre-outage samples or restart the clock when a PID changes.
            self.ack_point_count = min(self.ack_point_count, 1)
            all_up = False
        if durable_network_transition:
            all_up = False
        episode = self.active
        if episode is not None:
            if network == "DOWN" and self.last_network == "UP":
                episode.network_flap_count += 1
            if episode.network_recovered_at is None and network == "UP":
                # Confirmation time, not a guessed start/end between polls.
                episode.network_recovered_at = now
                episode.deadline_at = now + timedelta(seconds=self.policy.deadline_seconds)
                self.ack_point_count = min(self.ack_point_count, 1) if item.transport_observed_at == now else 0
                all_up = False
                self.maximum_observed_network_unavailable_seconds = max(
                    self.maximum_observed_network_unavailable_seconds, (now - episode.started_at).total_seconds()
                )
            if "UNKNOWN" in (network, transport, platform, item.control_path) or item.unresolved_effect_count is None:
                episode.unknown_sample_count += 1
            if episode.deadline_at is not None and now > episode.deadline_at and not episode.timed_out:
                episode.timed_out = True
                self.exceeded_count += 1
                self.blockers.add("RECOVERY_DEADLINE_EXCEEDED")
            if episode.network_recovered_at is not None:
                if transport == "UP" and self.ack_point_count >= self.policy.minimum_ack_points:
                    episode.transport_recovered_at = now
                if platform == "UP" and item.platform_observed_at is not None and item.platform_observed_at >= episode.network_recovered_at:
                    episode.platform_recovered_at = now
                # Fresh current facts must agree in this observation. A past
                # good sample on either plane is not enough to close an episode.
                if all_up and episode.transport_recovered_at == now and episode.platform_recovered_at == now:
                    episode.completed_at = now
                    elapsed = (now - episode.network_recovered_at).total_seconds()
                    self.maximum_recovery_seconds = max(self.maximum_recovery_seconds, elapsed)
                    self.recovered_count += 1
                    if (
                        episode.trigger == "NETWORK_DOWN"
                        and self.baseline_at is not None
                        and episode.started_at >= self.baseline_at
                        and not episode.timed_out
                    ):
                        self.network_recovered_count += 1
                    self.recent_episodes.append(episode.value())
                    self.recent_episodes = self.recent_episodes[-128:]
                    self.active = None
        if self.baseline_at is None and all_up:
            self.baseline_at = now
        self.current_health = "READY" if all_up else "RECOVERING" if self.active else "UNKNOWN"
        self.last_network = network

    def finish(self, now: datetime) -> dict[str, Any]:
        if self.active is not None and self.active.deadline_at is not None and now > self.active.deadline_at:
            if not self.active.timed_out:
                self.active.timed_out = True
                self.exceeded_count += 1
            self.blockers.add("RECOVERY_DEADLINE_EXCEEDED")
        return {
            "deadline_seconds": self.policy.deadline_seconds,
            "episode_count": self.episode_count,
            "recovered_episode_count": self.recovered_count,
            "verified_network_recovered_episode_count": self.network_recovered_count,
            "deadline_exceeded_count": self.exceeded_count,
            "maximum_recovery_seconds": self.maximum_recovery_seconds,
            "maximum_observed_network_unavailable_seconds": self.maximum_observed_network_unavailable_seconds,
            "active_episode": self.active.value() if self.active else None,
            "recent_episodes": self.recent_episodes,
            "baseline_at": isoformat_utc(self.baseline_at) if self.baseline_at else None,
            "live_health": self.current_health,
            "blockers": sorted(self.blockers),
            "control_capability_count": 0,
        }


def cast_checkpoint(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("RECOVERY_CHECKPOINT_INVALID")
    return value
