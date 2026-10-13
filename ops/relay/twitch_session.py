"""Twitch-only session renewal reducer for the future relay supervisor.

No timer, network, process control, credential access or automatic deployment.
The owner must persist each transition and command in its fenced outbox before
dispatching it, and supply verified authority/lease and provider/writer facts.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from enum import Enum
from typing import Mapping
from urllib.parse import urlsplit


TWITCH_MAX_BROADCAST_SECONDS = 48 * 3600
JST = timezone(timedelta(hours=9), "JST")


def _number(value: object) -> bool:
    try:
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    except OverflowError:
        return False


def _aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def _id(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9]+", value) is not None


@dataclass(frozen=True)
class RenewalPolicy:
    renew_time_jst: str = "00:00"
    # An age guard if the daily renewal cannot run; not the normal schedule.
    renew_after_seconds: int = 47 * 3600
    offline_wait_seconds: int = 120
    renewal_timeout_seconds: int = 600
    evidence_max_age_seconds: int = 60
    clock_skew_seconds: int = 5

    def __post_init__(self) -> None:
        if not isinstance(self.renew_time_jst, str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", self.renew_time_jst):
            raise ValueError("TWITCH_SESSION_RENEW_TIME_JST_INVALID")
        values = (self.renew_after_seconds, self.offline_wait_seconds, self.renewal_timeout_seconds,
                  self.evidence_max_age_seconds, self.clock_skew_seconds)
        if any(type(v) is not int or v <= 0 for v in values):
            raise ValueError("TWITCH_SESSION_POLICY_INVALID")
        if (
            self.renew_after_seconds < 3600
            or self.offline_wait_seconds <= 90
            or self.offline_wait_seconds >= self.renewal_timeout_seconds
            or self.renew_after_seconds + self.renewal_timeout_seconds + self.clock_skew_seconds
            >= TWITCH_MAX_BROADCAST_SECONDS
        ):
            raise ValueError("TWITCH_SESSION_POLICY_EXCEEDS_SAFE_WINDOW")

    @classmethod
    def from_env(cls, values: Mapping[str, str]) -> RenewalPolicy:
        names = {
            "renew_after_seconds": "TWITCH_SESSION_RENEW_AFTER_SEC",
            "offline_wait_seconds": "TWITCH_SESSION_OFFLINE_WAIT_SEC",
            "renewal_timeout_seconds": "TWITCH_SESSION_RENEWAL_TIMEOUT_SEC",
            "evidence_max_age_seconds": "TWITCH_SESSION_EVIDENCE_MAX_AGE_SEC",
            "clock_skew_seconds": "TWITCH_SESSION_CLOCK_SKEW_SEC",
        }
        options = {}
        if "TWITCH_SESSION_RENEW_TIME_JST" in values:
            raw_time = values["TWITCH_SESSION_RENEW_TIME_JST"]
            if not isinstance(raw_time, str):
                raise ValueError("TWITCH_SESSION_RENEW_TIME_JST_INVALID")
            options["renew_time_jst"] = raw_time.strip()
        for field, name in names.items():
            if name in values:
                if not isinstance(values[name], str):
                    raise ValueError("TWITCH_SESSION_POLICY_INVALID")
                raw = values[name].strip()
                if not re.fullmatch(r"[0-9]+", raw):
                    raise ValueError("TWITCH_SESSION_POLICY_INVALID")
                try:
                    options[field] = int(raw)
                except ValueError:
                    raise ValueError("TWITCH_SESSION_POLICY_INVALID") from None
        return cls(**options)


def next_daily_renewal_at(started_at: datetime, policy: RenewalPolicy = RenewalPolicy()) -> datetime:
    """First scheduled JST time strictly after this broadcast began, in UTC.

    Derive the deadline from the durable provider start, not process startup.
    A tick after the deadline still renews; no exact-second timer is required.
    """
    if not _aware(started_at):
        raise ValueError("TWITCH_BROADCAST_FACTS_INVALID")
    local = started_at.astimezone(JST)
    hour, minute = map(int, policy.renew_time_jst.split(":"))
    scheduled = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if scheduled <= local:
        scheduled += timedelta(days=1)
    return scheduled.astimezone(UTC)


class BroadcastStatus(str, Enum):
    LIVE = "LIVE"
    OFFLINE = "OFFLINE"
    UNKNOWN = "UNKNOWN"


class WriterStatus(str, Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    UNKNOWN = "UNKNOWN"


class Phase(str, Enum):
    TRACKING = "TRACKING"
    WAIT_STOP = "WAIT_STOP"
    WAIT_OFFLINE = "WAIT_OFFLINE"
    WAIT_START = "WAIT_START"
    VERIFYING = "VERIFYING"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class Action(str, Enum):
    STOP_TWITCH = "STOP_TWITCH"
    START_TWITCH = "START_TWITCH"


def twitch_forward_update(
    current: list[dict], *, twitch_destination: dict, action: Action,
) -> list[dict]:
    """Build only a forward-list update; never apply it or restart MediaMTX.

    MediaMTX v1.21.2 reuses an unchanged destination at the same index. Keep
    YouTube at index 0 and add/remove Twitch only at index 1. The future owner
    applies this under the same lease/fence, then obtains physical writer facts.
    """
    def provider(dest: object) -> str:
        if not isinstance(dest, dict) or not isinstance(dest.get("dest"), str):
            raise ValueError("TWITCH_FORWARD_CONFIG_INVALID")
        try:
            parsed = urlsplit(dest["dest"])
            port = parsed.port
        except ValueError:
            raise ValueError("TWITCH_FORWARD_CONFIG_INVALID") from None
        if parsed.scheme != "rtmps" or port not in (None, 443) or parsed.username is not None or parsed.query or not parsed.fragment:
            raise ValueError("TWITCH_FORWARD_CONFIG_INVALID")
        if parsed.hostname in {"a.rtmps.youtube.com", "b.rtmps.youtube.com"} and parsed.path == "/live2":
            return "youtube"
        if parsed.hostname and parsed.hostname.endswith(".live-video.net") and parsed.path == "/app":
            return "twitch"
        raise ValueError("TWITCH_FORWARD_CONFIG_INVALID")

    if (
        not isinstance(current, list) or len(current) not in {1, 2}
        or provider(current[0]) != "youtube" or provider(twitch_destination) != "twitch"
    ):
        raise ValueError("TWITCH_FORWARD_ORDER_OR_PROVIDER_INVALID")
    if len(current) == 2 and (provider(current[1]) != "twitch" or current[1] != twitch_destination):
        raise ValueError("TWITCH_FORWARD_DESTINATION_DRIFT")
    if not isinstance(action, Action):
        raise ValueError("TWITCH_FORWARD_ACTION_INVALID")
    forward = [deepcopy(current[0])]
    if action == Action.START_TWITCH:
        forward.append(deepcopy(twitch_destination))
    return forward


@dataclass(frozen=True)
class Clock:
    utc: datetime
    elapsed_seconds: float
    boot_id: str

    def __post_init__(self) -> None:
        if not _aware(self.utc) or not _number(self.elapsed_seconds) or self.elapsed_seconds < 0 or not self.boot_id:
            raise ValueError("TWITCH_SESSION_CLOCK_INVALID")


@dataclass(frozen=True)
class Session:
    stream_id: str
    started_at: datetime


@dataclass(frozen=True)
class BroadcastFacts:
    broadcaster_id: str
    observed_at: datetime
    status: BroadcastStatus
    session: Session | None = None


def parse_get_streams(
    payload: object, *, broadcaster_id: str, observed_at: datetime, http_status: int = 200
) -> BroadcastFacts:
    """Parse a GET /helix/streams filtered by this single broadcaster ID.

    The observer owns authenticated polling. Errors are UNKNOWN, never OFFLINE.
    No stream key, access token or provider error body is returned or logged.
    """
    unknown = BroadcastFacts(broadcaster_id, observed_at, BroadcastStatus.UNKNOWN)
    if not _id(broadcaster_id) or not _aware(observed_at):
        raise ValueError("TWITCH_BROADCAST_FACTS_INVALID")
    if http_status != 200 or not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        return unknown
    rows = payload["data"]
    if not rows:
        return BroadcastFacts(broadcaster_id, observed_at, BroadcastStatus.OFFLINE)
    if len(rows) != 1 or not isinstance(rows[0], dict):
        return unknown
    row = rows[0]
    if row.get("user_id") != broadcaster_id or row.get("type") != "live":
        return unknown
    stream_id, raw_start = row.get("id"), row.get("started_at")
    if not _id(stream_id) or not isinstance(raw_start, str):
        return unknown
    try:
        started_at = datetime.fromisoformat(raw_start.replace("Z", "+00:00"))
    except ValueError:
        return unknown
    if not _aware(started_at):
        return unknown
    return BroadcastFacts(broadcaster_id, observed_at, BroadcastStatus.LIVE, Session(stream_id, started_at.astimezone(UTC)))


@dataclass(frozen=True)
class WriterFacts:
    observed_at: datetime
    relay_instance_id: str
    generation: int
    status: WriterStatus
    progress_age_seconds: float | None = None


@dataclass(frozen=True)
class RouteContext:
    broadcaster_id: str
    route_state: str
    route_generation: int
    authority_epoch: int
    relay_instance_id: str
    lease_boot_id: str
    lease_expires_elapsed: float
    input_fresh: bool
    maintenance_authorized: bool = False
    effects_enabled: bool = False
    route_change_pending: bool = False


@dataclass(frozen=True)
class Binding:
    broadcaster_id: str
    route_generation: int
    authority_epoch: int
    relay_instance_id: str
    boot_id: str
    writer_generation: int


@dataclass(frozen=True)
class Command:
    action: Action
    request_id: str
    binding: Binding
    provider: str = "twitch"


@dataclass(frozen=True)
class Outcome:
    action: Action
    request_id: str
    binding: Binding
    status: str  # ACCEPTED, STOPPED, STARTED or UNKNOWN; each is distinct.
    observed_at: datetime


@dataclass(frozen=True)
class RenewalState:
    revision: int = 0
    phase: Phase = Phase.TRACKING
    session: Session | None = None
    binding: Binding | None = None
    operation_id: str = ""
    began_at: datetime | None = None
    stopped_clock: Clock | None = None
    start_requested_at: datetime | None = None
    last_clock: Clock | None = None
    last_completed_operation: str = ""


@dataclass(frozen=True)
class Decision:
    state: RenewalState
    status: str
    reason: str
    command: Command | None = None


def _fresh(at: object, now: Clock, policy: RenewalPolicy) -> bool:
    return _aware(at) and 0 <= (now.utc - at).total_seconds() <= policy.evidence_max_age_seconds


def _writer_progress(writer: WriterFacts, policy: RenewalPolicy) -> bool:
    return writer.status == WriterStatus.RUNNING and _number(writer.progress_age_seconds) and (
        0 <= writer.progress_age_seconds <= policy.evidence_max_age_seconds
    )


def advance(
    state: RenewalState, *, now: Clock, context: RouteContext,
    broadcast: BroadcastFacts, writer: WriterFacts,
    outcome: Outcome | None = None, policy: RenewalPolicy = RenewalPolicy(),
    validation_authorized: bool = False,
) -> Decision:
    """Evaluate one owner-loop tick; commands require a durable CAS + outbox.

    Re-evaluating a pre-commit state yields the same command ID. Evaluating the
    committed state emits no duplicate effect. UNKNOWN attempts are never retried
    here; only the existing owner may reconcile them against the physical writer.
    """
    def result(status: str, reason: str, *, command: Command | None = None, **changes: object) -> Decision:
        next_state = replace(state, revision=state.revision + 1, last_clock=now, **changes)
        return Decision(next_state, status, reason, command)

    if type(state.revision) is not int or state.revision < 0 or not isinstance(state.phase, Phase):
        return Decision(state, "UNKNOWN", "RENEWAL_STATE_CORRUPT")
    if (
        (state.began_at is not None and not _aware(state.began_at))
        or (state.start_requested_at is not None and not _aware(state.start_requested_at))
        or (state.last_clock is not None and not isinstance(state.last_clock, Clock))
        or (state.stopped_clock is not None and not isinstance(state.stopped_clock, Clock))
        or (state.session is not None and (not isinstance(state.session, Session)
                                           or not _id(state.session.stream_id) or not _aware(state.session.started_at)))
    ):
        return result("UNKNOWN", "RENEWAL_STATE_CORRUPT", phase=Phase.UNKNOWN)
    if state.binding is not None and (
        not isinstance(state.binding, Binding) or not _id(state.binding.broadcaster_id)
        or type(state.binding.writer_generation) is not int or state.binding.writer_generation <= 0
        or type(state.binding.route_generation) is not int or state.binding.route_generation <= 0
        or type(state.binding.authority_epoch) is not int or state.binding.authority_epoch <= 0
    ):
        return result("UNKNOWN", "RENEWAL_STATE_CORRUPT", phase=Phase.UNKNOWN)
    if state.phase in {Phase.UNKNOWN, Phase.CANCELLED}:
        return Decision(state, "UNKNOWN" if state.phase == Phase.UNKNOWN else "CANCELLED", "OWNER_RECONCILIATION_REQUIRED")
    if state.last_clock is not None:
        previous = state.last_clock
        if previous.boot_id != now.boot_id:
            return Decision(state, "UNKNOWN", "CLOCK_BOOT_CHANGED_RECONCILE_REQUIRED")
        elapsed = now.elapsed_seconds - previous.elapsed_seconds
        wall = (now.utc - previous.utc).total_seconds()
        if elapsed < 0 or abs(wall - elapsed) > policy.clock_skew_seconds:
            return Decision(state, "UNKNOWN", "CLOCK_DISCONTINUITY")
    if context.route_state not in {"PRIMARY", "PRIMARY_DEGRADED"} or context.route_change_pending:
        if state.binding is not None:
            return result("CANCELLED", "ROUTE_HANDOFF_PREEMPTS_TWITCH_RENEWAL", phase=Phase.CANCELLED)
        return result("BLOCKED", "OCI_ROUTE_INACTIVE")
    if state.binding is not None:
        bound = state.binding
        if (
            bound.broadcaster_id != context.broadcaster_id
            or bound.route_generation != context.route_generation
            or bound.authority_epoch != context.authority_epoch
            or bound.relay_instance_id != context.relay_instance_id
            or bound.boot_id != now.boot_id
        ):
            return result("CANCELLED", "RENEWAL_BINDING_SUPERSEDED", phase=Phase.CANCELLED)
    if context.effects_enabled is not True or context.maintenance_authorized is not True:
        return result("BLOCKED", "NO_ACTION_OR_MAINTENANCE_GRANT_MISSING")
    if (
        context.lease_boot_id != now.boot_id or not _number(context.lease_expires_elapsed)
        or context.lease_expires_elapsed <= now.elapsed_seconds or context.input_fresh is not True
    ):
        return result("BLOCKED", "RELAY_LEASE_OR_INPUT_INVALID")
    if (
        not _id(context.broadcaster_id)
        or type(context.route_generation) is not int or context.route_generation <= 0
        or type(context.authority_epoch) is not int or context.authority_epoch <= 0
        or not context.relay_instance_id or not _fresh(writer.observed_at, now, policy)
        or writer.relay_instance_id != context.relay_instance_id
        or type(writer.generation) is not int or writer.generation <= 0
    ):
        return result("UNKNOWN", "WRITER_OR_TARGET_FACTS_INVALID")
    provider_fresh = _fresh(broadcast.observed_at, now, policy) and broadcast.broadcaster_id == context.broadcaster_id
    if broadcast.broadcaster_id != context.broadcaster_id:
        return result("UNKNOWN", "BROADCASTER_ID_MISMATCH")
    if state.began_at is not None and (now.utc - state.began_at).total_seconds() >= policy.renewal_timeout_seconds:
        return result("UNKNOWN", "TWITCH_RENEWAL_DEADLINE_EXCEEDED", phase=Phase.UNKNOWN)

    if state.phase == Phase.TRACKING:
        session = state.session
        if provider_fresh and broadcast.status == BroadcastStatus.LIVE:
            candidate = broadcast.session
            if not isinstance(candidate, Session) or not _aware(candidate.started_at) or not _id(candidate.stream_id):
                return result("UNKNOWN", "BROADCAST_SESSION_INVALID")
            if session is not None and candidate != session and (
                candidate.stream_id == session.stream_id or candidate.started_at <= session.started_at
            ):
                return result("UNKNOWN", "BROADCAST_SESSION_REGRESSED_OR_INCONSISTENT")
            session = candidate
        if session is None or not _aware(session.started_at):
            return result("UNKNOWN", "BROADCAST_START_UNCONFIRMED")
        age = (now.utc - session.started_at).total_seconds()
        if age < 0:
            return result("UNKNOWN", "BROADCAST_START_IN_FUTURE")
        if not _writer_progress(writer, policy):
            return result("UNKNOWN", "TWITCH_WRITER_PROGRESS_UNCONFIRMED", session=session)
        scheduled_at = next_daily_renewal_at(session.started_at, policy)
        age_guard_at = session.started_at + timedelta(seconds=policy.renew_after_seconds)
        due_at = min(scheduled_at, age_guard_at)
        if now.utc < due_at and validation_authorized is not True:
            return result("TRACKING", "TWITCH_SESSION_BEFORE_JST_RENEWAL", session=session)
        reason = ("TWITCH_AUTHORIZED_VALIDATION_DUE" if validation_authorized is True else
                  "TWITCH_DAILY_JST_RENEWAL_DUE" if scheduled_at <= age_guard_at else "TWITCH_SESSION_AGE_GUARD_DUE")
        binding = Binding(context.broadcaster_id, context.route_generation, context.authority_epoch,
                          context.relay_instance_id, now.boot_id, writer.generation)
        payload = [*binding.__dict__.values(), session.stream_id, session.started_at.astimezone(UTC).isoformat()]
        operation_id = "twitch-renew-" + hashlib.sha256(json.dumps(payload).encode()).hexdigest()
        command = Command(Action.STOP_TWITCH, operation_id + ":stop", binding)
        return result("STOP_REQUESTED", reason, command=command,
                      phase=Phase.WAIT_STOP, session=session, binding=binding,
                      operation_id=operation_id, began_at=now.utc)

    if state.binding is None or state.session is None or not state.operation_id or state.began_at is None:
        return result("UNKNOWN", "RENEWAL_STATE_CORRUPT", phase=Phase.UNKNOWN)
    expected_action = Action.STOP_TWITCH if state.phase == Phase.WAIT_STOP else Action.START_TWITCH
    expected_id = state.operation_id + (":stop" if expected_action == Action.STOP_TWITCH else ":start")
    issued_at = state.began_at if state.phase == Phase.WAIT_STOP else state.start_requested_at
    matches = state.phase in {Phase.WAIT_STOP, Phase.WAIT_START} and outcome is not None and (
        outcome.action == expected_action and outcome.request_id == expected_id
        and outcome.binding == state.binding and _fresh(outcome.observed_at, now, policy)
        and _aware(issued_at) and outcome.observed_at >= issued_at
    )
    if matches and outcome.status == "UNKNOWN":
        return result("UNKNOWN", "TWITCH_COMMAND_OUTCOME_UNKNOWN", phase=Phase.UNKNOWN)
    old_generation = state.binding.writer_generation

    if state.phase == Phase.WAIT_STOP:
        if not matches or outcome.status != "STOPPED":
            return result("WAITING", "TWITCH_STOP_NOT_CONFIRMED")
        if writer.status != WriterStatus.STOPPED or writer.generation != old_generation or writer.observed_at < outcome.observed_at:
            return result("WAITING", "TWITCH_OLD_WRITER_STILL_UNCONFIRMED")
        return result("WAITING", "TWITCH_WRITER_STOPPED_WAIT_PROVIDER_OFFLINE",
                      phase=Phase.WAIT_OFFLINE, stopped_clock=now)
    if state.phase == Phase.WAIT_OFFLINE:
        if state.stopped_clock is None:
            return result("UNKNOWN", "RENEWAL_STATE_CORRUPT", phase=Phase.UNKNOWN)
        if writer.status != WriterStatus.STOPPED or writer.generation != old_generation:
            return result("UNKNOWN", "TWITCH_WRITER_REAPPEARED", phase=Phase.UNKNOWN)
        wait = now.elapsed_seconds - state.stopped_clock.elapsed_seconds
        if wait < policy.offline_wait_seconds:
            return result("WAITING", "TWITCH_OFFLINE_HOLD")
        if not provider_fresh or broadcast.status != BroadcastStatus.OFFLINE or broadcast.observed_at < state.stopped_clock.utc:
            return result("WAITING", "TWITCH_PROVIDER_OFFLINE_UNCONFIRMED")
        command = Command(Action.START_TWITCH, state.operation_id + ":start", state.binding)
        return result("START_REQUESTED", "TWITCH_PROVIDER_OFFLINE_CONFIRMED", command=command,
                      phase=Phase.WAIT_START, start_requested_at=now.utc)
    if state.phase == Phase.WAIT_START:
        if not matches or outcome.status != "STARTED":
            return result("WAITING", "TWITCH_START_NOT_CONFIRMED")
        if writer.generation != old_generation + 1 or not _writer_progress(writer, policy) or writer.observed_at < outcome.observed_at:
            return result("WAITING", "TWITCH_NEW_WRITER_PROGRESS_UNCONFIRMED")
        return result("WAITING", "TWITCH_WRITER_STARTED_VERIFY_BROADCAST", phase=Phase.VERIFYING)
    if state.phase == Phase.VERIFYING:
        new = broadcast.session
        if (
            writer.generation != old_generation + 1 or not _writer_progress(writer, policy)
            or not provider_fresh or broadcast.status != BroadcastStatus.LIVE or new is None
            or not _aware(new.started_at) or state.start_requested_at is None
            or not _id(new.stream_id)
            or new.stream_id == state.session.stream_id or new.started_at <= state.session.started_at
            or new.started_at < state.start_requested_at - timedelta(seconds=policy.clock_skew_seconds)
            or new.started_at > now.utc
        ):
            return result("WAITING", "TWITCH_NEW_BROADCAST_UNCONFIRMED")
        return result("RENEWED", "TWITCH_NEW_BROADCAST_VERIFIED", phase=Phase.TRACKING,
                      session=new, binding=None, last_completed_operation=state.operation_id,
                      operation_id="", began_at=None, stopped_clock=None, start_requested_at=None)
    return result("UNKNOWN", "RENEWAL_STATE_CORRUPT", phase=Phase.UNKNOWN)
