"""Pure decision logic. No I/O: every input is passed in, so every branch is unit-testable."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from cra.observe import Network, Observation
from cra.policy import Policy

RESTART = "RESTART"
WOULD_RESTART = "WOULD_RESTART"
NONE = "NONE"


@dataclass
class StallTracker:
    """Counts consecutive stalled samples of one FFmpeg generation.

    A sample only counts when the observation sequence advanced (distinct evidence) and
    bytes_sent did not move since the previous sample of the same generation.
    """

    generation_key: str = ""
    last_sequence: int = 0
    last_bytes_sent: int = -1
    streak: int = 0

    def update(self, obs: Observation, policy: Policy) -> tuple[bool, int]:
        """Return (stalled_now, streak)."""
        if obs.generation_key != self.generation_key or obs.sequence < self.last_sequence:
            self.generation_key = obs.generation_key
            self.last_sequence = obs.sequence
            self.last_bytes_sent = obs.tcp.bytes_sent if obs.tcp else -1
            self.streak = 0
            return False, 0
        if obs.sequence == self.last_sequence:
            return self.streak > 0, self.streak
        tcp = obs.tcp
        stalled = (
            tcp is not None
            and self.last_bytes_sent >= 0
            and tcp.bytes_sent == self.last_bytes_sent
            and tcp.lastsnd_ms >= policy.stall_lastsnd_ms
            and (tcp.notsent >= policy.stall_notsent_bytes or tcp.unacked >= policy.stall_unacked)
        )
        self.last_sequence = obs.sequence
        self.last_bytes_sent = tcp.bytes_sent if tcp else -1
        self.streak = self.streak + 1 if stalled else 0
        return stalled, self.streak


@dataclass(frozen=True)
class History:
    """Facts from the agent's own durable store."""

    generation_already_acted: bool
    last_action_at: datetime | None
    actions_last_hour: int
    actions_last_day: int


@dataclass(frozen=True)
class Decision:
    outcome: str  # RESTART | WOULD_RESTART | NONE
    reason: str
    blockers: tuple[str, ...] = field(default=())

    @property
    def summary(self) -> str:
        return f"{self.outcome}:{self.reason}:{','.join(self.blockers)}"


def decide(
    *,
    now: datetime,
    obs: Observation,
    stall_streak: int,
    network: Network | None,
    history: History,
    policy: Policy,
    producer_id: str,
    producer_generation: int,
    stop_file_present: bool,
) -> Decision:
    """`network` is None when probes were skipped because no stall is confirmed."""
    if stall_streak < policy.stall_confirm_samples:
        reason = "STALL_UNCONFIRMED" if stall_streak else ("NO_TCP_METRICS" if obs.tcp is None else "HEALTHY")
        return Decision(NONE, reason)

    blockers: list[str] = []
    if network is None:
        blockers.append("NETWORK_NOT_PROBED")
    elif network.network_down:
        blockers.append("NETWORK_DOWN")
    elif not network.ingest_ok:
        blockers.append("INGEST_UNREACHABLE")
    if not obs.ffmpeg_running or not obs.ffmpeg_generation or not obs.pod_uid:
        blockers.append("FFMPEG_NOT_RUNNING")
    if not obs.target_valid:
        blockers.append("TARGET_UNAVAILABLE")
    if obs.ffmpeg_uptime_sec < policy.min_ffmpeg_uptime_sec:
        blockers.append("FFMPEG_TOO_YOUNG")
    if obs.in_flight_count != 0:
        blockers.append("EXECUTOR_IN_FLIGHT")
    if history.generation_already_acted:
        blockers.append("GENERATION_ALREADY_ACTED")
    if history.last_action_at is not None and (now - history.last_action_at).total_seconds() < policy.cooldown_sec:
        blockers.append("COOLDOWN")
    if history.actions_last_hour >= policy.max_per_hour:
        blockers.append("HOURLY_LIMIT")
    if history.actions_last_day >= policy.max_per_day:
        blockers.append("DAILY_LIMIT")
    if stop_file_present:
        blockers.append("LOCAL_STOP")
    if policy.mode == "off":
        blockers.append("MODE_OFF")
    if policy.mode == "enforce" and (obs.active_producer_id != producer_id or obs.active_producer_generation != producer_generation):
        blockers.append("NOT_EXECUTOR_AUTHORITY")

    if blockers:
        return Decision(NONE, "STALL_CONFIRMED", tuple(blockers))
    return Decision(RESTART if policy.mode == "enforce" else WOULD_RESTART, "STALL_CONFIRMED")
