from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable
from unittest.mock import patch

from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.commands.wall_clock_loop import next_run_epoch
from stream_monitoring_v4.domains.reducer import reduce_domain
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.storage import factory


UTC = timezone.utc


def load_fixture(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != "monitoring_v4.time_boundary_replay.v1":
        raise ValueError("unsupported time-boundary fixture schema")
    return payload


def epoch(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo != UTC:
        raise ValueError(f"fixture timestamp must use UTC Z form: {value!r}")
    return int(parsed.timestamp())


@dataclass(frozen=True)
class CompressedEvent:
    kind: str
    original_offset_sec: int
    compressed_offset_sec: Fraction


def compress_events(
    events: Iterable[dict[str, Any]], *, factor: int
) -> tuple[CompressedEvent, ...]:
    if factor <= 0:
        raise ValueError("compression factor must be positive")
    materialized = list(events)
    if not materialized:
        return ()
    origin = epoch(str(materialized[0]["at"]))
    result = tuple(
        CompressedEvent(
            kind=str(item["kind"]),
            original_offset_sec=epoch(str(item["at"])) - origin,
            compressed_offset_sec=Fraction(epoch(str(item["at"])) - origin, factor),
        )
        for item in materialized
    )
    if any(item.original_offset_sec < 0 for item in result):
        raise ValueError("fixture events must not precede the first event")
    return result


def startup_budget_sequence(
    *, retry_timeout_sec: int, post_retry_wait_sec: int, probe_budget_sec: int
) -> dict[str, Any]:
    ready_at = retry_timeout_sec + post_retry_wait_sec
    if ready_at < probe_budget_sec:
        outcome = "margin"
    elif ready_at == probe_budget_sec:
        outcome = "equal_boundary_race"
    else:
        outcome = "probe_precedes_ready"
    return {
        "actions": [
            {"at_sec": 0, "action": "process_start"},
            {"at_sec": retry_timeout_sec, "action": "db_retry_boundary"},
            {"at_sec": ready_at, "action": "application_ready"},
            {"at_sec": probe_budget_sec, "action": "nominal_probe_deadline"},
        ],
        "ready_at_sec": ready_at,
        "probe_budget_sec": probe_budget_sec,
        "margin_sec": probe_budget_sec - ready_at,
        "outcome": outcome,
    }


class VirtualMonotonicClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, duration: float) -> None:
        if duration < 0:
            raise AssertionError("SUT requested a negative sleep")
        self.sleeps.append(duration)
        self.now += duration


@dataclass(frozen=True)
class RetryReplay:
    timeout_sec: int
    dependency_ready_at_sec: int
    succeeded: bool
    completed_at_sec: float
    integrity_checks: tuple[float, ...]
    virtual_sleeps: tuple[float, ...]

    def sequence(self) -> dict[str, Any]:
        return {
            "timeout_sec": self.timeout_sec,
            "dependency_ready_at_sec": self.dependency_ready_at_sec,
            "succeeded": self.succeeded,
            "completed_at_sec": self.completed_at_sec,
            "integrity_checks": self.integrity_checks,
            "virtual_sleeps": self.virtual_sleeps,
        }


def replay_integrity_retry(
    *, timeout_sec: int, dependency_ready_at_sec: int, retry_interval_sec: float = 1.0
) -> RetryReplay:
    clock = VirtualMonotonicClock()
    checks: list[float] = []

    class Repository:
        def integrity_check(self) -> str:
            checks.append(clock.now)
            return "ok" if clock.now >= dependency_ready_at_sec else "failed"

    succeeded = True
    with patch.object(factory.time, "monotonic", side_effect=clock.monotonic), patch.object(
        factory.time, "sleep", side_effect=clock.sleep
    ):
        try:
            factory.wait_for_integrity(
                Repository(),
                timeout_sec=timeout_sec,
                retry_interval_sec=retry_interval_sec,
            )
        except RuntimeError:
            succeeded = False
    return RetryReplay(
        timeout_sec=timeout_sec,
        dependency_ready_at_sec=dependency_ready_at_sec,
        succeeded=succeeded,
        completed_at_sec=clock.now,
        integrity_checks=tuple(checks),
        virtual_sleeps=tuple(clock.sleeps),
    )


def replay_video_resolver_ttl(*, age_sec: int, freshness_limit_sec: int = 90) -> dict[str, Any]:
    base_ts = epoch("2026-08-14T14:38:50Z")
    item = ObservationEnvelope.create(
        domain="youtube_lifecycle",
        source="youtube_video_resolver",
        source_event_id="test-only-boundary-transform",
        source_generation="time-boundary-harness-r1",
        evidence_role="current_authoritative",
        status="good",
        reason_code="resolver_good",
        observed_at=utc_text(base_ts),
        received_at=utc_text(base_ts),
        freshness_limit_sec=freshness_limit_sec,
        producer_revision="test-only-time-boundary-r1",
        payload={
            "classification": "contract_boundary_transform",
            "historical_maximum_observed_gap_sec": 85,
        },
    )
    current = reduce_domain(
        [item], DEFAULT_POLICIES["youtube_lifecycle"], now_ts=base_ts + age_sec
    )
    return {
        "age_sec": age_sec,
        "source_ttl_sec": DEFAULT_POLICIES["youtube_lifecycle"].rule(
            "youtube_video_resolver"
        ).ttl_sec,
        "envelope_freshness_limit_sec": freshness_limit_sec,
        "state": current.state,
        "reason_codes": current.reason_codes,
        "ignored": current.payload["ignored"],
    }


def parse_daily_cron(schedule: str) -> tuple[int, int]:
    fields = schedule.split()
    if len(fields) != 5 or fields[2:] != ["*", "*", "*"]:
        raise ValueError(f"only exact daily UTC cron is supported: {schedule!r}")
    minute, hour = (int(fields[0]), int(fields[1]))
    if not 0 <= minute <= 59 or not 0 <= hour <= 23:
        raise ValueError(f"invalid daily UTC cron: {schedule!r}")
    return hour, minute


def manifest_startup_budget(path: Path) -> dict[str, int]:
    text = path.read_text(encoding="utf-8")
    retry_match = re.search(
        r"--startup-db-timeout-sec\s*\n\s*-\s*[\"']?(\d+)[\"']?", text
    )
    probe_match = re.search(
        r"startupProbe:.*?periodSeconds:\s*(\d+).*?failureThreshold:\s*(\d+)",
        text,
        flags=re.DOTALL,
    )
    if retry_match is None or probe_match is None:
        raise ValueError(f"startup retry/probe contract not found in {path}")
    retry_timeout = int(retry_match.group(1))
    period = int(probe_match.group(1))
    failures = int(probe_match.group(2))
    return {
        "retry_timeout_sec": retry_timeout,
        "probe_period_sec": period,
        "probe_failure_threshold": failures,
        "nominal_startup_probe_budget_sec": period * failures,
    }


def next_daily_epoch(now: float, *, schedule: str) -> int:
    hour, minute = parse_daily_cron(schedule)
    current = datetime.fromtimestamp(now, tz=UTC)
    candidate = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate.timestamp() <= now:
        candidate += timedelta(days=1)
    return int(candidate.timestamp())


def replay_periodic_boundary(
    *, scheduled_epoch: int, phase_sec: int, kind: str, schedule: str | None = None,
    second: int | None = None, minute_modulo: int | None = None
) -> dict[str, Any]:
    now = scheduled_epoch + phase_sec
    if kind == "daily_cron":
        if schedule is None:
            raise ValueError("daily_cron replay requires schedule")
        next_epoch = next_daily_epoch(now, schedule=schedule)
    elif kind == "minute_modulo":
        if second is None or minute_modulo is None:
            raise ValueError("minute_modulo replay requires second and minute_modulo")
        next_epoch = next_run_epoch(now, second=second, minute_modulo=minute_modulo)
    else:
        raise ValueError(f"unsupported periodic kind: {kind!r}")
    return {
        "scheduled_epoch": scheduled_epoch,
        "phase_sec": phase_sec,
        "now_epoch": now,
        "next_epoch": next_epoch,
        "strictly_future": next_epoch > now,
    }


def readable_sequence(name: str, payload: dict[str, Any]) -> str:
    return f"{name}: {json.dumps(payload, ensure_ascii=False, sort_keys=True, default=list)}"
