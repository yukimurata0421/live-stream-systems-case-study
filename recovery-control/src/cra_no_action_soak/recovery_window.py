"""Pure recovery-deadline oracle. No network, command dispatch, or effect path.

The caller supplies admitted, source-time observations, not dashboard health.
Pi publication observations are deliberately absent from this interface.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from cra_dell_recovery.time import isoformat_utc, parse_utc

Health = Literal["UP", "DOWN", "UNKNOWN"]
MAXIMUM_TRACKED_ACK_TARGETS = 128
EMPTY_REVISION_DIGEST = hashlib.sha256(b"").hexdigest()


def _checkpoint_time(value: datetime) -> str:
    """Preserve the source clock used for identity, ACK and deadline decisions.

    Existing millisecond-aligned output retains its spelling.  This codec is
    deliberately local: signed packet and command timestamps use a separate
    canonicalization contract.
    """
    if value.tzinfo is None:
        raise ValueError("timezone-aware datetime is required")
    utc = value.astimezone(UTC)
    precision = "milliseconds" if utc.microsecond % 1000 == 0 else "microseconds"
    return utc.isoformat(timespec=precision).replace("+00:00", "Z")


@dataclass(frozen=True)
class RecoveryPolicy:
    deadline_seconds: int = 600
    ack_stall_seconds: int = 45
    maximum_source_age_seconds: int = 45
    maximum_platform_age_seconds: int = 120
    minimum_ack_points: int = 3
    maximum_cra_safe_blocked_seconds: int = 60

    def __post_init__(self) -> None:
        if self.deadline_seconds != 600:
            raise ValueError("RECOVERY_DEADLINE_MUST_BE_600_SECONDS")
        for value in (self.ack_stall_seconds, self.maximum_source_age_seconds, self.maximum_platform_age_seconds):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value < self.deadline_seconds:
                raise ValueError("RECOVERY_SUB_DEADLINE_INVALID")
        if self.minimum_ack_points != 3:
            raise ValueError("RECOVERY_REQUIRES_THREE_ACK_POINTS")
        if self.maximum_cra_safe_blocked_seconds != 60:
            raise ValueError("CRA_SAFE_BLOCKED_LIMIT_MUST_BE_60_SECONDS")


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
    cra_readiness: Health
    cra_readiness_observed_at: datetime | None
    unresolved_effect_count: int | None
    network_episode_id: str | None = None
    network_episode_sequence: int | None = None
    network_episode_state: str | None = None
    network_episode_started_at: datetime | None = None
    network_episode_recovered_at: datetime | None = None
    child_lifecycle: str | None = None
    owner_target: str | None = None
    rtmps_episode_id: str | None = None
    rtmps_episode_sequence: int | None = None
    rtmps_episode_state: str | None = None
    rtmps_episode_classification: str | None = None
    rtmps_episode_started_at: datetime | None = None
    rtmps_episode_detected_at: datetime | None = None
    rtmps_episode_recovered_at: datetime | None = None
    rtmps_episode_target: str | None = None
    rtmps_episode_replacement_target: str | None = None
    control_path_observed_at: datetime | None = None
    owner_observed_at: datetime | None = None
    effects_observed_at: datetime | None = None
    # Formal history replay can observe several signed transitions at one
    # canonical millisecond. Raw collector observations never set this field.
    replay_step: int = 0


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
    origin_target: str | None = None
    replacement_target: str | None = None
    rtmps_episode_id: str | None = None

    def value(self) -> dict[str, Any]:
        result = asdict(self)
        for name, item in result.items():
            if isinstance(item, datetime):
                result[name] = _checkpoint_time(item)
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
    latest_transport_source_at: datetime | None = None
    last_bytes_acked: int | None = None
    last_ack_progress_at: datetime | None = None
    ack_point_count: int = 0
    ack_high_watermarks: dict[str, int] = field(default_factory=dict)
    retired_targets: set[str] = field(default_factory=set)
    ack_discontinuities: dict[str, dict[str, Any]] = field(default_factory=dict)
    observation_unknowns: set[str] = field(default_factory=set)
    last_transport_input_fingerprint: str | None = None
    transport_order_unknown_revision: int = 0
    transport_order_unknown_digest: str = EMPTY_REVISION_DIGEST
    current_health: str = "UNKNOWN"
    current_stream_health: str = "UNKNOWN"
    blockers: set[str] = field(default_factory=set)
    last_network_episode_id: str | None = None
    last_network_episode_sequence: int = 0
    last_network_episode_state: str | None = None
    last_network_episode_started_at: datetime | None = None
    last_network_episode_recovered_at: datetime | None = None
    last_rtmps_episode_id: str | None = None
    last_rtmps_episode_sequence: int = 0
    last_rtmps_episode_state: str | None = None
    last_rtmps_episode_classification: str | None = None
    last_rtmps_episode_started_at: datetime | None = None
    last_rtmps_episode_recovered_at: datetime | None = None
    last_rtmps_episode_target: str | None = None
    last_rtmps_episode_replacement_target: str | None = None
    last_cra_readiness: Health = "UNKNOWN"
    cra_safe_blocked_episode_count: int = 0
    cra_max_safe_blocked_duration_seconds: float = 0.0
    cra_safe_blocked_started_at: datetime | None = None

    def checkpoint(self) -> dict[str, Any]:
        def encode(value: Any) -> Any:
            if isinstance(value, datetime):
                return _checkpoint_time(value)
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
            "latest_transport_source_at",
            "last_ack_progress_at",
            "last_network_episode_started_at",
            "last_network_episode_recovered_at",
            "last_rtmps_episode_started_at",
            "last_rtmps_episode_recovered_at",
            "cra_safe_blocked_started_at",
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
        value["retired_targets"] = set(value["retired_targets"])
        value["observation_unknowns"] = set(value["observation_unknowns"])
        high_watermarks = value["ack_high_watermarks"]
        if not isinstance(high_watermarks, dict) or any(
            not isinstance(target, str)
            or not target
            or isinstance(counter, bool)
            or not isinstance(counter, int)
            or counter < 0
            for target, counter in high_watermarks.items()
        ):
            raise ValueError("RECOVERY_ACK_HIGH_WATERMARKS_INVALID")
        if any(not isinstance(target, str) or not target for target in value["retired_targets"]):
            raise ValueError("RECOVERY_RETIRED_TARGETS_INVALID")
        if (
            (
                value["last_transport_input_fingerprint"] is not None
                and (
                    not isinstance(value["last_transport_input_fingerprint"], str)
                    or len(value["last_transport_input_fingerprint"]) != 64
                    or any(character not in "0123456789abcdef" for character in value["last_transport_input_fingerprint"])
                )
            )
            or isinstance(value["transport_order_unknown_revision"], bool)
            or not isinstance(value["transport_order_unknown_revision"], int)
            or value["transport_order_unknown_revision"] < 0
            or not isinstance(value["transport_order_unknown_digest"], str)
            or len(value["transport_order_unknown_digest"]) != 64
            or any(character not in "0123456789abcdef" for character in value["transport_order_unknown_digest"])
            or (
                value["transport_order_unknown_revision"] == 0
                and value["transport_order_unknown_digest"] != EMPTY_REVISION_DIGEST
            )
        ):
            raise ValueError("RECOVERY_TRANSPORT_ORDER_REVISION_INVALID")
        discontinuities = value["ack_discontinuities"]
        if not isinstance(discontinuities, dict):
            raise ValueError("RECOVERY_ACK_DISCONTINUITIES_INVALID")
        for target, discontinuity in discontinuities.items():
            if (
                not isinstance(target, str)
                or not target
                or not isinstance(discontinuity, dict)
                or set(discontinuity)
                != {"prior_bytes", "reset_bytes", "transport_at", "observed_at"}
                or isinstance(discontinuity["prior_bytes"], bool)
                or not isinstance(discontinuity["prior_bytes"], int)
                or isinstance(discontinuity["reset_bytes"], bool)
                or not isinstance(discontinuity["reset_bytes"], int)
            ):
                raise ValueError("RECOVERY_ACK_DISCONTINUITY_INVALID")
            discontinuity["transport_at"] = parse_utc(discontinuity["transport_at"])
            discontinuity["observed_at"] = parse_utc(discontinuity["observed_at"])
        if (
            value["last_transport_at"] is not None
            and (
                value["latest_transport_source_at"] is None
                or value["latest_transport_source_at"] < value["last_transport_at"]
            )
        ) or (
            value["latest_transport_source_at"] is not None
            and value["last_observed_at"] is not None
            and value["latest_transport_source_at"] > value["last_observed_at"]
        ):
            raise ValueError("RECOVERY_TRANSPORT_SOURCE_HIGH_WATER_INVALID")
        tracked_targets = set(high_watermarks) | value["retired_targets"] | set(discontinuities)
        if value["last_target"] is not None:
            tracked_targets.add(value["last_target"])
        if len(tracked_targets) > MAXIMUM_TRACKED_ACK_TARGETS:
            raise ValueError("RECOVERY_ACK_TARGET_HISTORY_LIMIT")
        return cls(**value)

    @staticmethod
    def _transport_input_fingerprint(item: RecoveryObservation) -> str:
        assert item.transport_observed_at is not None
        value = {
            "target": item.target,
            "transport_observed_at": _checkpoint_time(item.transport_observed_at),
            "bytes_acked": item.bytes_acked,
        }
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def _transport_order_unknown(self, item: RecoveryObservation, *, new_input: bool) -> None:
        """Record a monotonic revision for each newly visible ambiguous fact.

        The sticky reason remains fail-closed.  A formal replay may attest that
        one exact revision was resolved; a later distinct input advances this
        chain and invalidates that attestation without mutating old evidence.
        """
        self.observation_unknowns.add("TRANSPORT_TARGET_ORDER_UNVERIFIED")
        if not new_input:
            return
        assert item.transport_observed_at is not None
        self.transport_order_unknown_revision += 1
        event = {
            "previous_digest": self.transport_order_unknown_digest,
            "revision": self.transport_order_unknown_revision,
            "target": item.target,
            "transport_observed_at": _checkpoint_time(item.transport_observed_at),
            "bytes_acked": item.bytes_acked,
            "active_target": self.last_target,
            "transport_source_high_water": (
                _checkpoint_time(self.latest_transport_source_at)
                if self.latest_transport_source_at is not None
                else None
            ),
        }
        self.transport_order_unknown_digest = hashlib.sha256(
            json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @staticmethod
    def _fresh(source: datetime | None, now: datetime, maximum_age: int) -> bool:
        return source is not None and 0 <= (now - source).total_seconds() <= maximum_age

    def _cra_readiness(self, item: RecoveryObservation) -> Health:
        """Track Central input availability without relabelling the LAN path."""
        now = item.observed_at
        readiness: Health = (
            item.cra_readiness
            if self._fresh(item.cra_readiness_observed_at, now, self.policy.maximum_source_age_seconds)
            else "UNKNOWN"
        )
        if readiness == "DOWN":
            if self.cra_safe_blocked_started_at is None:
                self.cra_safe_blocked_episode_count += 1
                self.cra_safe_blocked_started_at = item.cra_readiness_observed_at or now
            duration = (now - self.cra_safe_blocked_started_at).total_seconds()
            self.cra_max_safe_blocked_duration_seconds = max(self.cra_max_safe_blocked_duration_seconds, duration)
            if duration > self.policy.maximum_cra_safe_blocked_seconds:
                self.blockers.add("CRA_SAFE_BLOCKED_DURATION_EXCEEDED")
        elif readiness == "UP" and self.cra_safe_blocked_started_at is not None:
            duration = (now - self.cra_safe_blocked_started_at).total_seconds()
            self.cra_max_safe_blocked_duration_seconds = max(self.cra_max_safe_blocked_duration_seconds, duration)
            if duration > self.policy.maximum_cra_safe_blocked_seconds:
                self.blockers.add("CRA_SAFE_BLOCKED_DURATION_EXCEEDED")
            self.cra_safe_blocked_started_at = None
        self.last_cra_readiness = readiness
        return readiness

    def _transport(self, item: RecoveryObservation) -> tuple[Health, bool]:
        """Evaluate current transport and identify a late retired-target packet.

        ACK state is scoped to the signed target identity.  A counter reset can
        precede the target snapshot update during a session replacement, so it
        remains an explicit unresolved discontinuity.  Only a matching signed
        RTMPS replacement episode may clear it, regardless of arrival order.
        """
        if (
            not self._fresh(item.transport_observed_at, item.observed_at, self.policy.maximum_source_age_seconds)
            or not item.target
            or isinstance(item.bytes_acked, bool)
            or not isinstance(item.bytes_acked, int)
            or item.bytes_acked < 0
        ):
            self.ack_point_count = 0
            return "UNKNOWN", False
        assert item.transport_observed_at is not None
        fingerprint = self._transport_input_fingerprint(item)
        new_input = fingerprint != self.last_transport_input_fingerprint
        self.last_transport_input_fingerprint = fingerprint
        tracked_targets = set(self.ack_high_watermarks) | self.retired_targets | set(self.ack_discontinuities)
        if self.last_target is not None:
            tracked_targets.add(self.last_target)
        if item.target not in tracked_targets and len(tracked_targets) >= MAXIMUM_TRACKED_ACK_TARGETS:
            self.observation_unknowns.add("TRANSPORT_TARGET_HISTORY_EXHAUSTED")
            self.ack_point_count = 0
            return "UNKNOWN", False
        if item.target != self.last_target and item.target in self.retired_targets:
            # A late packet from a retired target may extend that target's
            # audit high-water mark, but it cannot reactivate the target or
            # mutate the current target's progress/deadline state.
            self.ack_high_watermarks[item.target] = max(
                self.ack_high_watermarks.get(item.target, 0), item.bytes_acked,
            )
            if (
                self.latest_transport_source_at is not None
                and item.transport_observed_at > self.latest_transport_source_at
            ):
                self._transport_order_unknown(item, new_input=new_input)
            return "UNKNOWN", True
        if (
            self.active is not None
            and self.active.network_recovered_at is not None
            and item.transport_observed_at < self.active.network_recovered_at
        ):
            self.ack_point_count = 0
            if item.target != self.last_target:
                self._transport_order_unknown(item, new_input=new_input)
                return "UNKNOWN", True
            return "UNKNOWN", False
        if (
            item.target != self.last_target
            and self.latest_transport_source_at is not None
            and item.transport_observed_at < self.latest_transport_source_at
        ):
            # A checkpoint may begin after the predecessor target disappeared,
            # so a late packet can name a target that is not yet in the local
            # retired set.  Its older source clock proves that it cannot be a
            # successor.  Keep the active target unchanged and fail formal
            # eligibility closed instead of manufacturing a target transition.
            self._transport_order_unknown(item, new_input=new_input)
            self.ack_point_count = 0
            return "UNKNOWN", True
        if item.target != self.last_target:
            if self.last_target is not None:
                self.retired_targets.add(self.last_target)
            self.last_target = item.target
            self.last_transport_at = None
            self.last_bytes_acked = None
            self.last_ack_progress_at = item.transport_observed_at
            self.ack_point_count = 0
        if self.last_transport_at is not None and item.transport_observed_at < self.last_transport_at:
            self.blockers.add("TRANSPORT_SOURCE_TIME_REGRESSION")
            return "UNKNOWN", False
        if item.transport_observed_at == self.last_transport_at:
            if item.bytes_acked != self.last_bytes_acked:
                self.blockers.add("TRANSPORT_SOURCE_TIME_CONFLICT")
                return "UNKNOWN", False
        else:
            if (
                self.last_transport_at is not None
                and (item.transport_observed_at - self.last_transport_at).total_seconds() > self.policy.maximum_source_age_seconds
            ):
                self.ack_point_count = 0
            if self.last_bytes_acked is not None:
                if item.bytes_acked < self.last_bytes_acked:
                    self.ack_discontinuities.setdefault(
                        item.target,
                        {
                            "prior_bytes": self.last_bytes_acked,
                            "reset_bytes": item.bytes_acked,
                            "transport_at": item.transport_observed_at,
                            "observed_at": item.observed_at,
                        },
                    )
                    self.ack_point_count = 1
                    self.last_ack_progress_at = item.transport_observed_at
                    self.last_transport_at = item.transport_observed_at
                    self.last_bytes_acked = item.bytes_acked
                    self.latest_transport_source_at = max(
                        self.latest_transport_source_at or item.transport_observed_at,
                        item.transport_observed_at,
                    )
                    return "UNKNOWN", False
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
            self.ack_high_watermarks[item.target] = max(
                self.ack_high_watermarks.get(item.target, 0), item.bytes_acked,
            )
        self.latest_transport_source_at = max(
            self.latest_transport_source_at or item.transport_observed_at,
            item.transport_observed_at,
        )
        if (
            self.last_ack_progress_at is not None
            and (item.observed_at - self.last_ack_progress_at).total_seconds() >= self.policy.ack_stall_seconds
        ):
            return "DOWN", False
        return ("UP" if self.ack_point_count >= self.policy.minimum_ack_points else "UNKNOWN"), False

    def _resolve_pending_ack_regression(self, item: RecoveryObservation) -> None:
        target = item.rtmps_episode_target
        if not isinstance(target, str) or target not in self.ack_discontinuities:
            return
        discontinuity = self.ack_discontinuities[target]
        if (
            item.rtmps_episode_classification
            not in {"RTMPS_ACK_STALL_FFMPEG_REPLACED", "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"}
            or item.rtmps_episode_started_at is None
            or item.rtmps_episode_recovered_at is None
            or item.rtmps_episode_replacement_target in (None, target)
            # Transport source time and packet visibility time are separate
            # clocks.  The metric snapshot can predate a session-loss edge by
            # one collection interval, but the reset may be explained only
            # when its packet became visible during that exact signed episode.
            # An episode that starts after the reset was already observed, or
            # one that ended before visibility, cannot clear old uncertainty.
            or discontinuity["observed_at"] < item.rtmps_episode_started_at
            or discontinuity["observed_at"] > item.rtmps_episode_recovered_at
            or item.rtmps_episode_recovered_at < discontinuity["transport_at"]
        ):
            return
        del self.ack_discontinuities[target]

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

    @staticmethod
    def _same_rtmps_transition(
        item: RecoveryObservation,
        *,
        started_at: datetime,
        completed_at: datetime | None,
        origin_target: str | None,
        replacement_target: str | None,
        rtmps_episode_id: str | None,
    ) -> bool:
        """Bind a late signed session episode to its observed target change."""
        if (
            origin_target is None
            or item.rtmps_episode_target != origin_target
            or rtmps_episode_id not in (None, item.rtmps_episode_id)
            or item.rtmps_episode_started_at is None
            or (completed_at is not None and item.rtmps_episode_started_at > completed_at)
            or (item.rtmps_episode_recovered_at is not None and item.rtmps_episode_recovered_at < started_at)
            or (
                completed_at is not None
                and (item.rtmps_episode_recovered_at is None or item.rtmps_episode_recovered_at > completed_at)
            )
        ):
            return False
        successor = replacement_target
        if successor is None and item.target not in (None, origin_target):
            successor = item.target
        if successor is None and item.owner_target not in (None, origin_target):
            successor = item.owner_target
        return successor is not None and item.rtmps_episode_replacement_target == successor

    @staticmethod
    def _same_open_rtmps_interval(
        item: RecoveryObservation,
        *,
        started_at: datetime,
        origin_target: str | None,
        rtmps_episode_id: str | None,
    ) -> bool:
        """Provisionally bind an ACTIVE signed session to one platform gap.

        Replacement identity does not exist while the RTMPS episode is still
        ACTIVE.  The binding is therefore limited to a platform-only episode
        with the exact origin target and overlapping source-time intervals.
        A later RECOVERED packet must still prove the exact successor.
        """
        end = item.rtmps_episode_recovered_at or item.observed_at
        return (
            item.rtmps_episode_state == "ACTIVE"
            and origin_target is not None
            and item.rtmps_episode_target == origin_target
            and rtmps_episode_id in (None, item.rtmps_episode_id)
            and item.rtmps_episode_started_at is not None
            and item.rtmps_episode_started_at <= end
            and started_at <= end
        )

    def _merge_active_rtmps_episode(self, item: RecoveryObservation) -> None:
        assert self.active is not None
        assert item.rtmps_episode_started_at is not None
        self.active.started_at = min(self.active.started_at, item.rtmps_episode_started_at)
        self.active.trigger = "RTMPS_SESSION_FAILURE"
        self.active.rtmps_episode_id = item.rtmps_episode_id
        if item.rtmps_episode_replacement_target is not None:
            self.active.replacement_target = item.rtmps_episode_replacement_target
        recovered_at = self.active.network_recovered_at
        if recovered_at is None or item.rtmps_episode_started_at < recovered_at:
            recovered_at = item.rtmps_episode_started_at
        self.active.network_recovered_at = recovered_at
        deadline = recovered_at + timedelta(seconds=self.policy.deadline_seconds)
        if self.active.deadline_at is not None:
            deadline = min(deadline, self.active.deadline_at)
        self.active.deadline_at = deadline

    def _durable_rtmps_episode(self, item: RecoveryObservation) -> bool:
        values = (
            item.rtmps_episode_id,
            item.rtmps_episode_sequence,
            item.rtmps_episode_state,
            item.rtmps_episode_classification,
            item.rtmps_episode_started_at,
            item.rtmps_episode_detected_at,
            item.rtmps_episode_recovered_at,
            item.rtmps_episode_target,
        )
        if all(value is None for value in values):
            return False
        if (
            not isinstance(item.rtmps_episode_id, str)
            or not item.rtmps_episode_id
            or isinstance(item.rtmps_episode_sequence, bool)
            or not isinstance(item.rtmps_episode_sequence, int)
            or item.rtmps_episode_sequence < 1
            or item.rtmps_episode_state not in {"ACTIVE", "RECOVERED"}
            or item.rtmps_episode_classification
            not in {
                "RTMPS_ACK_STALL",
                "RTMPS_ACK_STALL_FFMPEG_REPLACED",
                "RTMPS_ACK_STALL_RECOVERED_SAME_TARGET",
                "FFMPEG_SESSION_LOSS",
                "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            }
            or item.rtmps_episode_started_at is None
            or item.rtmps_episode_detected_at is None
            or not item.rtmps_episode_started_at <= item.rtmps_episode_detected_at <= item.observed_at
            or not isinstance(item.rtmps_episode_target, str)
            or not item.rtmps_episode_target
            or (item.rtmps_episode_state == "ACTIVE") != (item.rtmps_episode_recovered_at is None)
            or (
                item.rtmps_episode_recovered_at is not None
                and not item.rtmps_episode_detected_at <= item.rtmps_episode_recovered_at <= item.observed_at
            )
        ):
            self.blockers.add("RTMPS_EPISODE_INVALID")
            return False
        sequence = item.rtmps_episode_sequence
        prior_rtmps_state = self.last_rtmps_episode_state
        if sequence < self.last_rtmps_episode_sequence:
            self.blockers.add("RTMPS_EPISODE_SEQUENCE_REGRESSION")
            return False
        new_episode = sequence > self.last_rtmps_episode_sequence
        if not new_episode:
            if item.rtmps_episode_id != self.last_rtmps_episode_id or item.rtmps_episode_started_at != self.last_rtmps_episode_started_at:
                self.blockers.add("RTMPS_EPISODE_IDENTITY_CONFLICT")
                return False
            if (
                item.rtmps_episode_target != self.last_rtmps_episode_target
                or self.last_rtmps_episode_replacement_target is not None
                and item.rtmps_episode_replacement_target != self.last_rtmps_episode_replacement_target
                or self.last_rtmps_episode_state == "RECOVERED"
                and item.rtmps_episode_classification != self.last_rtmps_episode_classification
            ):
                self.blockers.add("RTMPS_EPISODE_IDENTITY_CONFLICT")
                return False
            if self.last_rtmps_episode_state == "RECOVERED" and item.rtmps_episode_state != "RECOVERED":
                self.blockers.add("RTMPS_EPISODE_STATE_REGRESSION")
                return False
            if self.last_rtmps_episode_recovered_at is not None and item.rtmps_episode_recovered_at != self.last_rtmps_episode_recovered_at:
                self.blockers.add("RTMPS_EPISODE_RECOVERY_CONFLICT")
                return False
        elif self.last_rtmps_episode_sequence and sequence != self.last_rtmps_episode_sequence + 1:
            self.blockers.add("RTMPS_EPISODE_SEQUENCE_GAP")
        self.last_rtmps_episode_id = item.rtmps_episode_id
        self.last_rtmps_episode_sequence = sequence
        self.last_rtmps_episode_state = item.rtmps_episode_state
        self.last_rtmps_episode_classification = item.rtmps_episode_classification
        self.last_rtmps_episode_started_at = item.rtmps_episode_started_at
        self.last_rtmps_episode_recovered_at = item.rtmps_episode_recovered_at
        self.last_rtmps_episode_target = item.rtmps_episode_target
        self.last_rtmps_episode_replacement_target = item.rtmps_episode_replacement_target
        self._resolve_pending_ack_regression(item)

        eligible = self.baseline_at is not None and item.rtmps_episode_started_at >= self.baseline_at
        if (
            not new_episode
            and eligible
            and self.active is not None
            and self.active.trigger == "RTMPS_SESSION_FAILURE"
            and self.active.rtmps_episode_id == item.rtmps_episode_id
        ):
            if item.rtmps_episode_state == "ACTIVE":
                return True
            if item.rtmps_episode_classification in {
                "RTMPS_ACK_STALL_FFMPEG_REPLACED",
                "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            }:
                if not self._same_rtmps_transition(
                    item,
                    started_at=self.active.started_at,
                    completed_at=None,
                    origin_target=self.active.origin_target,
                    replacement_target=self.active.replacement_target,
                    rtmps_episode_id=self.active.rtmps_episode_id,
                ):
                    self.blockers.add("RTMPS_EPISODE_OVERLAP")
                    return False
                self.active.replacement_target = item.rtmps_episode_replacement_target
            elif (
                item.rtmps_episode_classification != "RTMPS_ACK_STALL_RECOVERED_SAME_TARGET"
                or item.rtmps_episode_target != self.active.origin_target
                or item.rtmps_episode_replacement_target is not None
                or item.target != self.active.origin_target
            ):
                self.blockers.add("RTMPS_EPISODE_OVERLAP")
                return False
            return prior_rtmps_state != "RECOVERED"
        if new_episode and eligible:
            if self.active is None:
                # A signed episode can arrive after its owner-observed child
                # replacement has already closed.  It must not become a second
                # recovery, but the signed identity is still remembered.
                for recent in reversed(self.recent_episodes):
                    if recent.get("trigger") not in {
                        "OWNER_CHILD_UNAVAILABLE",
                        "TARGET_TRANSITION",
                        "PLATFORM_DOWN",
                        "RTMPS_SESSION_FAILURE",
                    }:
                        continue
                    if self._same_rtmps_transition(
                        item,
                        started_at=parse_utc(recent["started_at"]),
                        completed_at=parse_utc(recent["completed_at"]),
                        origin_target=recent.get("origin_target"),
                        replacement_target=recent.get("replacement_target"),
                        rtmps_episode_id=recent.get("rtmps_episode_id"),
                    ):
                        recent["rtmps_episode_id"] = item.rtmps_episode_id
                        return False
                self.episode_count += 1
                self.active = RecoveryEpisode(
                    self.episode_count,
                    item.rtmps_episode_started_at,
                    "RTMPS_SESSION_FAILURE",
                    network_recovered_at=item.rtmps_episode_started_at,
                    deadline_at=item.rtmps_episode_started_at + timedelta(seconds=self.policy.deadline_seconds),
                    origin_target=item.rtmps_episode_target,
                    replacement_target=item.rtmps_episode_replacement_target,
                    rtmps_episode_id=item.rtmps_episode_id,
                )
            elif self.active.trigger == "NETWORK_DOWN" and self.active.started_at <= item.rtmps_episode_started_at:
                # A session stall can be a consequence of the already-proven
                # FULL_WAN episode. Keep the stronger causal episode and clock.
                pass
            elif (
                self.active.trigger in {"OWNER_CHILD_UNAVAILABLE", "TARGET_TRANSITION", "PLATFORM_DOWN"}
                and self._same_rtmps_transition(
                    item,
                    started_at=self.active.started_at,
                    completed_at=None,
                    origin_target=self.active.origin_target,
                    replacement_target=self.active.replacement_target,
                    rtmps_episode_id=self.active.rtmps_episode_id,
                )
                or self.active.trigger == "PLATFORM_DOWN"
                and self._same_open_rtmps_interval(
                    item,
                    started_at=self.active.started_at,
                    origin_target=self.active.origin_target,
                    rtmps_episode_id=self.active.rtmps_episode_id,
                )
            ):
                self._merge_active_rtmps_episode(item)
            else:
                self.blockers.add("RTMPS_EPISODE_OVERLAP")
                return False
            self.ack_point_count = min(self.ack_point_count, 1)
        return new_episode and eligible

    @staticmethod
    def _point_failure_trigger(
        *,
        network: Health,
        transport: Health,
        platform: Health,
        control_path: Health,
        unresolved_effect_count: int | None,
        child_down: bool,
        target_changed: bool,
    ) -> str:
        """Keep failure domains distinct so later evidence cannot cross-bind.

        A FULL_WAN edge and a signed owner/target transition remain the
        strongest exact identities.  Point-in-time facts from the remaining
        domains are merged only when exactly one domain is down.  Concurrent
        domains retain a fail-closed aggregate trigger.
        """
        if network == "DOWN":
            return "NETWORK_DOWN"
        if child_down:
            return "OWNER_CHILD_UNAVAILABLE"
        if target_changed:
            return "TARGET_TRANSITION"
        domains: list[str] = []
        if transport == "DOWN":
            domains.append("TRANSPORT_DOWN")
        if platform == "DOWN":
            domains.append("PLATFORM_DOWN")
        if control_path == "DOWN":
            domains.append("CONTROL_PATH_DOWN")
        if type(unresolved_effect_count) is int and unresolved_effect_count > 0:
            domains.append("UNRESOLVED_EFFECT")
        return domains[0] if len(domains) == 1 else "MULTI_DOMAIN_DOWN"

    def observe(self, item: RecoveryObservation) -> None:
        now = item.observed_at
        if now.tzinfo is None or (
            self.last_observed_at is not None
            and (now < self.last_observed_at or (now == self.last_observed_at and item.replay_step <= 0))
        ):
            self.blockers.add("RECOVERY_OBSERVATION_TIME_INVALID")
            return
        self.last_observed_at = now
        cra_readiness = self._cra_readiness(item)
        network: Health = item.network if self._fresh(item.network_observed_at, now, self.policy.maximum_source_age_seconds) else "UNKNOWN"
        prior_target = self.last_target
        transport, late_retired_target = self._transport(item)
        durable_network_transition = self._durable_network_episode(item)
        durable_rtmps_transition = self._durable_rtmps_episode(item)
        target_changed = (
            not late_retired_target
            and prior_target is not None
            and item.target is not None
            and item.target != prior_target
        )
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
            assert prior_target is not None
            self.retired_targets.add(prior_target)
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
        stream_all_up = (
            network == transport == platform == item.control_path == "UP"
            and item.unresolved_effect_count == 0
            and item.child_lifecycle in (None, "RUNNING")
        )
        all_up = stream_all_up and cra_readiness == "UP"
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
            trigger = self._point_failure_trigger(
                network=network,
                transport=transport,
                platform=platform,
                control_path=item.control_path,
                unresolved_effect_count=item.unresolved_effect_count,
                child_down=child_down,
                target_changed=target_changed or owner_target_changed_during_transport_convergence,
            )
            # The source may have signed a failure before its packet crossed
            # the relay. Use the earliest relevant signed observation time;
            # replay visibility is an upper bound, never a later onset clock.
            onset_sources = [now]
            if network == "DOWN" and item.network_observed_at is not None:
                onset_sources.append(item.network_observed_at)
            if (transport == "DOWN" or target_changed) and item.transport_observed_at is not None:
                onset_sources.append(item.transport_observed_at)
            if platform == "DOWN" and item.platform_observed_at is not None:
                onset_sources.append(item.platform_observed_at)
            if item.control_path == "DOWN" and item.control_path_observed_at is not None:
                onset_sources.append(item.control_path_observed_at)
            if (child_down or owner_target_changed_during_transport_convergence) and item.owner_observed_at is not None:
                onset_sources.append(item.owner_observed_at)
            if (
                type(item.unresolved_effect_count) is int
                and item.unresolved_effect_count > 0
                and item.effects_observed_at is not None
            ):
                onset_sources.append(item.effects_observed_at)
            self.active = RecoveryEpisode(
                self.episode_count,
                min(onset_sources),
                trigger,
                origin_target=prior_target or item.target,
                replacement_target=(
                    item.target if target_changed else item.owner_target if owner_target_changed_during_transport_convergence else None
                ),
            )
            # Recovery must prove three new ACK points after this episode, never
            # reuse pre-outage samples or restart the clock when a PID changes.
            self.ack_point_count = min(self.ack_point_count, 1)
            all_up = False
        if durable_network_transition or durable_rtmps_transition:
            all_up = False
        episode = self.active
        if episode is not None:
            if episode.origin_target is not None and episode.replacement_target is None:
                if target_changed and item.target != episode.origin_target:
                    episode.replacement_target = item.target
                elif owner_target_changed_during_transport_convergence and item.owner_target != episode.origin_target:
                    episode.replacement_target = item.owner_target
            if network == "DOWN" and self.last_network == "UP":
                episode.network_flap_count += 1
            if episode.network_recovered_at is None and network == "UP":
                # Confirmation time, not a guessed start/end between polls.
                confirmed_at = (
                    item.network_observed_at
                    if item.network_observed_at is not None and item.network_observed_at >= episode.started_at
                    else now
                )
                episode.network_recovered_at = confirmed_at
                episode.deadline_at = confirmed_at + timedelta(seconds=self.policy.deadline_seconds)
                self.ack_point_count = min(self.ack_point_count, 1) if item.transport_observed_at == now else 0
                all_up = False
                self.maximum_observed_network_unavailable_seconds = max(
                    self.maximum_observed_network_unavailable_seconds, (confirmed_at - episode.started_at).total_seconds()
                )
            if "UNKNOWN" in (network, transport, platform, item.control_path, cra_readiness) or item.unresolved_effect_count is None:
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
        self.current_stream_health = "READY" if stream_all_up else "RECOVERING" if self.active else "UNKNOWN"
        self.current_health = "READY" if all_up else "RECOVERING" if self.active else "UNKNOWN"
        self.last_network = network

    def finish(self, now: datetime) -> dict[str, Any]:
        current_cra_safe_blocked_duration = 0.0
        if self.cra_safe_blocked_started_at is not None:
            current_cra_safe_blocked_duration = max(0.0, (now - self.cra_safe_blocked_started_at).total_seconds())
            self.cra_max_safe_blocked_duration_seconds = max(
                self.cra_max_safe_blocked_duration_seconds,
                current_cra_safe_blocked_duration,
            )
            if current_cra_safe_blocked_duration > self.policy.maximum_cra_safe_blocked_seconds:
                self.blockers.add("CRA_SAFE_BLOCKED_DURATION_EXCEEDED")
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
            "streaming_health": self.current_stream_health,
            "blockers": sorted(self.blockers),
            "unknown_reasons": sorted(
                self.observation_unknowns
                | ({"ACK_COUNTER_DISCONTINUITY_UNVERIFIED"} if self.ack_discontinuities else set())
            ),
            "observation_revision": {
                "transport_target_order": {
                    "revision": self.transport_order_unknown_revision,
                    "digest": self.transport_order_unknown_digest,
                }
            },
            "cra_readiness": self.last_cra_readiness,
            "cra_safe_blocked_episode_count": self.cra_safe_blocked_episode_count,
            "cra_max_safe_blocked_duration_seconds": self.cra_max_safe_blocked_duration_seconds,
            "cra_current_safe_blocked_duration_seconds": current_cra_safe_blocked_duration,
            "control_capability_count": 0,
        }


def cast_checkpoint(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("RECOVERY_CHECKPOINT_INVALID")
    return value
