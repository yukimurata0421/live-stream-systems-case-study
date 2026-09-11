#!/usr/bin/env python3
"""Bounded, test-only delivery two-source authority exploration.

The oracle deliberately contains the explicit delivery policy facts rather than
calling or mirroring the production reducer.  It is limited to the two
equal-priority current-authoritative sources audited for this slice.
"""
from __future__ import annotations

import argparse
import json
import random
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.time import unix_ts, utc_text
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.reducer import REDUCER_REVISION
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES, SOURCE_POLICY_REVISION
from stream_monitoring_v4.incidents.policy import DEFAULT_INCIDENT_POLICIES, INCIDENT_POLICY_REVISION
from stream_monitoring_v4.incidents.service import IncidentService
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.storage.repository import MonitoringRepository
try:
    from tests.stateful_runtime import (
        DEFAULT_WALL_TIME_SEC,
        MAX_ARTIFACT_BYTES,
        MAX_ROWS,
        ExplorationConfig,
        StatefulActionRuntime,
        StatefulAttributionGap,
        StatefulHarnessFailure,
        SUTInvariantViolation,
        WriteAheadActionLog,
        bounded_explore,
        require_artifact_size,
        source_identity,
        verified_cleanup,
    )
except ModuleNotFoundError:  # Direct script execution puts tests/ on sys.path.
    from stateful_runtime import (  # type: ignore[no-redef]
        DEFAULT_WALL_TIME_SEC,
        MAX_ARTIFACT_BYTES,
        MAX_ROWS,
        ExplorationConfig,
        StatefulActionRuntime,
        StatefulAttributionGap,
        StatefulHarnessFailure,
        SUTInvariantViolation,
        WriteAheadActionLog,
        bounded_explore,
        require_artifact_size,
        source_identity,
        verified_cleanup,
    )


ORACLE_REVISION = "delivery-two-source-authority-oracle-r1"
HISTORICAL_WINDOW_ID = "HR-20260813-RUNTIME-ROLLOUT"
HISTORICAL_ANCHOR_TS = unix_ts("2026-08-12T16:48:32Z")


@dataclass(frozen=True)
class SourceFact:
    source: str
    priority: int
    ttl_sec: int


SOURCES = {
    "a": SourceFact("runtime_delivery_watchdog", 100, 180),
    "b": SourceFact("rtmps_tcp", 100, 60),
}


@dataclass(frozen=True)
class Evidence:
    source_key: str
    state: str
    observed_ts: int
    received_ts: int
    identity: str
    provenance: str
    freshness_limit_sec: int

    @property
    def source(self) -> str:
        return SOURCES[self.source_key].source

    @property
    def effective_ttl(self) -> int:
        return min(SOURCES[self.source_key].ttl_sec, self.freshness_limit_sec)

    def envelope(self) -> ObservationEnvelope:
        return ObservationEnvelope.create(
            domain="delivery",
            source=self.source,
            source_event_id=self.identity,
            source_generation=f"synthetic-generation-{self.source_key}",
            evidence_role="current_authoritative",
            status=self.state,
            reason_code=f"synthetic_{self.state}_evidence",
            observed_at=utc_text(self.observed_ts),
            received_at=utc_text(self.received_ts),
            freshness_limit_sec=self.freshness_limit_sec,
            producer_revision="stateful-delivery-authority-r1",
            payload={"synthetic": self.provenance == "synthetic"},
        )

    @property
    def observation_id(self) -> str:
        return self.envelope().observation_id

    @property
    def latest_key(self) -> tuple[int, int, str]:
        return (self.observed_ts, self.received_ts, self.observation_id)


@dataclass(frozen=True)
class ExpectedCanonical:
    state: str
    reason: str
    observed_ts: int
    valid_until_ts: int
    selected_sources: tuple[str, ...]


class DeliveryAuthorityOracle:
    """Independent policy oracle for this fixed two-source subset only."""

    def canonical(self, evidence: list[Evidence], *, now_ts: int) -> ExpectedCanonical:
        latest: dict[str, Evidence] = {}
        for item in evidence:
            previous = latest.get(item.source_key)
            if previous is None or item.latest_key > previous.latest_key:
                latest[item.source_key] = item
        fresh = [
            item
            for item in latest.values()
            if item.observed_ts <= now_ts and now_ts - item.observed_ts <= item.effective_ttl
        ]
        if not fresh:
            return ExpectedCanonical("unknown", "missing_current_evidence", now_ts, now_ts, ())
        states = {item.state for item in fresh}
        if states == {"good"}:
            state, reason = "good", "current_good_evidence"
        elif states == {"bad"}:
            state, reason = "bad", "current_bad_evidence"
        else:
            state, reason = "unknown", "source_disagreement"
        return ExpectedCanonical(
            state=state,
            reason=reason,
            observed_ts=max(item.observed_ts for item in fresh),
            valid_until_ts=min(item.observed_ts + item.effective_ttl for item in fresh),
            selected_sources=tuple(sorted(item.source for item in fresh)),
        )


def validate_oracle_policy_alignment() -> None:
    """Fail closed if the explicit policy facts require a fresh oracle audit."""
    delivery = DEFAULT_POLICIES["delivery"]
    if delivery.revision != SOURCE_POLICY_REVISION:
        raise StatefulHarnessFailure("unexpected source policy revision")
    for fact in SOURCES.values():
        rule = delivery.rule(fact.source)
        if rule is None or not rule.current_authority:
            raise StatefulHarnessFailure(f"delivery source is not current authority: {fact.source}")
        if rule.priority != fact.priority or rule.ttl_sec != fact.ttl_sec:
            raise StatefulHarnessFailure(f"oracle policy fact drifted: {fact.source}")


def _current_public(item: Any) -> dict[str, Any]:
    selected = item.payload.get("selected") if isinstance(item.payload, dict) else []
    return {
        "state": item.state,
        "reason": item.reason_codes[0] if item.reason_codes else "",
        "observed_ts": unix_ts(item.observed_at),
        "valid_until_ts": unix_ts(item.valid_until),
        "selected_sources": tuple(sorted(raw["source"] for raw in selected)),
    }


def _failure_metadata(trace: list[dict[str, Any]]) -> dict[str, str]:
    """Classify a failed independent-oracle assertion without changing its oracle."""
    record = trace[-1]
    expected = record["expected"]
    actual = record["actual"]
    if expected["reason"] == "source_disagreement":
        return {
            "broken_invariant": "I4_equal_priority_disagreement_fail_closed",
            "root_cause_hypothesis": "a semantically newer equal-timestamp source-disagreement snapshot was not persisted",
        }
    if (
        expected["selected_sources"] == ("runtime_delivery_watchdog",)
        and "rtmps_tcp" in actual["selected_sources"]
    ):
        return {
            "broken_invariant": "I2_stale_source_exclusion",
            "root_cause_hypothesis": "an expired selected source remained persisted after a fresh source became authoritative",
        }
    return {
        "broken_invariant": "delivery_two_source_authority_oracle_mismatch",
        "root_cause_hypothesis": "unclassified delivery two-source authority mismatch",
    }


class DeliveryStateMachine:
    """One isolated repository and one bounded generated action sequence."""

    def __init__(
        self,
        *,
        seed: int,
        max_steps: int,
        example_index: int = 0,
        action_log_path: Path | None = None,
    ) -> None:
        validate_oracle_policy_alignment()
        self.seed = seed
        self.max_steps = max_steps
        self.example_index = example_index
        self.run_id = f"delivery-{seed}-{example_index}"
        self.root = Path(tempfile.mkdtemp(prefix="stream-v4-stateful-delivery-"))
        self.database = self.root / "delivery.sqlite3"
        self.repository = MonitoringRepository(self.database)
        self.repository.initialize(applied_at=utc_text(HISTORICAL_ANCHOR_TS))
        self.oracle = DeliveryAuthorityOracle()
        self.now_ts = HISTORICAL_ANCHOR_TS
        self.evidence: list[Evidence] = []
        self.trace: list[dict[str, Any]] = []
        self._next_identity = 0
        self._last_semantic: tuple[Any, ...] | None = None
        self._transition_count = 0
        self._intent_count = 0
        self.repository_generation = 0
        self.action_log_path = action_log_path or self.root / "actions.jsonl"
        self.action_log = WriteAheadActionLog(self.action_log_path)
        self.action_runtime = StatefulActionRuntime(
            run_id=self.run_id,
            seed=self.seed,
            example_index=self.example_index,
            capture_state=self._measurement_state,
            action_log=self.action_log,
        )
        self.action_events = self.action_runtime.events
        self.expected_effects = 0
        anchor = Evidence(
            "a",
            "good",
            self.now_ts,
            self.now_ts,
            "historical-delivery-good",
            "historical_observed",
            180,
        )
        self._run_action(
            "historical_anchor",
            anchor,
            lambda: self._append(anchor, action="historical_anchor"),
        )

    def close(self) -> dict[str, Any]:
        return verified_cleanup(self.root, delete=shutil.rmtree)

    def database_state(self) -> dict[str, Any]:
        with self.repository.connection(read_only=True) as connection:
            observations = [
                dict(row)
                for row in connection.execute(
                    "SELECT source, status, observed_at, received_at, source_event_id, source_generation, observation_id FROM observations ORDER BY source, observation_id"
                ).fetchall()
            ]
            current = connection.execute(
                "SELECT state, observed_at, reduced_at, valid_until, snapshot_id FROM domain_current WHERE domain='delivery'"
            ).fetchone()
        measured = self._measurement_state()
        return {
            "observations": observations,
            "domain_current": dict(current) if current is not None else None,
            "transitions": measured["transition_count"],
            "intents": measured["intent_count"],
            "transition_ids": measured["transition_ids"],
            "intent_ids": measured["intent_ids"],
            "episode_ids": measured["episode_ids"],
        }

    def _episode_state(self) -> list[dict[str, Any]]:
        with self.repository.connection(read_only=True) as connection:
            rows = connection.execute(
                """SELECT episode_id, status, severity, opened_at, last_bad_at,
                          closed_at, last_transition_at, bad_samples, unknown_samples
                   FROM incident_episodes
                   WHERE domain='delivery'
                   ORDER BY opened_ts, episode_id"""
            ).fetchall()
        return [dict(row) for row in rows]

    def _measurement_state(self) -> dict[str, Any]:
        current = self.repository.current("delivery")
        transitions = self.repository.transitions(domain="delivery")
        intents = self.repository.intents()
        episodes = self._episode_state()
        active = [episode for episode in episodes if episode["status"] == "active"]
        return {
            "canonical": None if current is None else _current_public(current),
            "current_snapshot_id": None if current is None else current.snapshot_id,
            "domain_current_snapshot_id": None if current is None else current.snapshot_id,
            "transition_ids": sorted(item.transition_id for item in transitions),
            "intent_ids": sorted(item.intent_id for item in intents),
            "episode_ids": sorted(item["episode_id"] for item in episodes),
            "transition_records": {
                item.transition_id: {
                    "transition_id": item.transition_id,
                    "episode_id": item.episode_id,
                    "phase": item.phase,
                    "current_snapshot_id": item.current_snapshot_id,
                }
                for item in transitions
            },
            "intent_records": {
                item.intent_id: {
                    "intent_id": item.intent_id,
                    "transition_id": item.transition_id,
                    "episode_id": item.episode_id,
                    "phase": item.phase,
                }
                for item in intents
            },
            "episode_state": episodes,
            "last_bad_at": active[0]["last_bad_at"] if len(active) == 1 else None,
            "transition_count": len(transitions),
            "intent_count": len(intents),
            "repository_generation": self.repository_generation,
        }

    @staticmethod
    def _action_input(item: Evidence | None) -> dict[str, Any]:
        if item is None:
            return {
                "source": None,
                "evidence_classification": "none",
                "observation_identity": None,
                "source_event_identity": None,
                "source_generation": None,
                "state": None,
                "reason": None,
                "observed_at": None,
                "received_at": None,
                "freshness_limit_sec": None,
            }
        envelope = item.envelope()
        return {
            "source": item.source,
            "evidence_classification": item.provenance,
            "observation_identity": envelope.observation_id,
            "source_event_identity": item.identity,
            "source_generation": envelope.source_generation,
            "state": item.state,
            "reason": envelope.reason_code,
            "observed_at": utc_text(item.observed_ts),
            "received_at": utc_text(item.received_ts),
            "freshness_limit_sec": item.freshness_limit_sec,
        }

    def _run_action(self, action_type: str, item: Evidence | None, mutation: Any) -> Any:
        return self.action_runtime.run(
            action_type=action_type,
            logical_now=utc_text(self.now_ts),
            action_input=self._action_input(item),
            mutation=mutation,
            trace_provider=lambda: self.trace[-1] if self.trace else None,
        )

    def action_trace_summary(self) -> dict[str, Any]:
        return self.action_runtime.summary()

    def _identity(self, prefix: str) -> str:
        self._next_identity += 1
        return f"{prefix}-{self.seed}-{self._next_identity}"

    def _append(self, item: Evidence, *, action: str) -> bool:
        accepted = self.repository.append_observation(item.envelope())
        if accepted:
            self.evidence.append(item)
        self._sync(action=action, accepted=accepted, item=item)
        return accepted

    def _counts(self) -> tuple[int, int]:
        return len(self.repository.transitions(domain="delivery")), len(self.repository.intents())

    def _sync(self, *, action: str, accepted: bool | None = None, item: Evidence | None = None) -> None:
        current = CurrentReducerService(self.repository, DEFAULT_POLICIES).reduce(("delivery",), now_ts=self.now_ts)[0]
        IncidentService(self.repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy()).process(current, now_at=utc_text(self.now_ts))
        expected = self.oracle.canonical(self.evidence, now_ts=self.now_ts)
        actual = _current_public(current)
        required = (expected.state, expected.reason, expected.observed_ts, expected.valid_until_ts, expected.selected_sources)
        observed = (actual["state"], actual["reason"], actual["observed_ts"], actual["valid_until_ts"], actual["selected_sources"])
        post = self._measurement_state()
        transition_count, intent_count = post["transition_count"], post["intent_count"]
        attribution = self.action_runtime.attribution(post)
        record = {
            "action": action,
            "logical_now": utc_text(self.now_ts),
            "accepted": accepted,
            "evidence": None if item is None else {
                "source": item.source,
                "state": item.state,
                "observed_at": utc_text(item.observed_ts),
                "received_at": utc_text(item.received_ts),
                "provenance": item.provenance,
            },
            "expected": {"state": expected.state, "reason": expected.reason, "selected_sources": expected.selected_sources},
            "actual": actual,
            "transitions": transition_count,
            "intents": intent_count,
            "attribution": attribution,
        }
        self.trace.append(record)
        if not attribution["attribution_complete"]:
            raise StatefulAttributionGap(
                "stateful attribution gap: " + ", ".join(attribution["attribution_errors"])
            )
        if observed != required:
            raise SUTInvariantViolation(f"oracle mismatch expected={required!r} actual={observed!r}")
        semantic = required
        if self._last_semantic == semantic and (transition_count, intent_count) != (self._transition_count, self._intent_count):
            raise SUTInvariantViolation("canonical semantics unchanged but transition or intent count increased")
        if expected.state != "good":
            if any(item["phase"] == "recovered" for item in attribution["created_transitions"]):
                raise SUTInvariantViolation("unsupported recovery transition")
            self.expected_effects += 1
        self._last_semantic = semantic
        self._transition_count, self._intent_count = transition_count, intent_count
        if len(self.trace) > self.max_steps + 1:
            raise StatefulHarnessFailure("action trace bound exceeded")
        with self.repository.connection(read_only=True) as connection:
            rows = int(connection.execute("SELECT count(*) FROM observations").fetchone()[0])
        if rows > MAX_ROWS:
            raise StatefulHarnessFailure("SQLite row bound exceeded")

    def ingest(self, source_key: str, state: str) -> None:
        item = Evidence(source_key, state, self.now_ts, self.now_ts, self._identity(f"ingest-{source_key}"), "synthetic", SOURCES[source_key].ttl_sec)
        self._run_action(
            f"ingest_source_{source_key}",
            item,
            lambda: self._append(item, action=f"ingest_source_{source_key}"),
        )
        self.now_ts += 1

    def replay_old(self, source_key: str) -> None:
        candidates = [item for item in self.evidence if item.source_key == source_key]
        if not candidates:
            self._run_action(
                f"replay_old_{source_key}_noop",
                None,
                lambda: self._sync(action=f"replay_old_{source_key}_noop"),
            )
            return
        original = min(candidates, key=lambda item: item.observed_ts)
        item = Evidence(source_key, original.state, original.observed_ts, self.now_ts, self._identity(f"replay-{source_key}"), "synthetic", original.freshness_limit_sec)
        self._run_action(
            f"replay_old_{source_key}",
            item,
            lambda: self._append(item, action=f"replay_old_{source_key}"),
        )

    def duplicate_last(self) -> None:
        if not self.evidence:
            self._run_action("duplicate_last_noop", None, lambda: self._sync(action="duplicate_last_noop"))
            return
        item = self.evidence[-1]
        self._run_action("duplicate_last", item, lambda: self._append(item, action="duplicate_last"))

    def delay_receive(self) -> None:
        if not self.evidence:
            self._run_action("delay_receive_noop", None, lambda: self._sync(action="delay_receive_noop"))
            return
        original = min(self.evidence, key=lambda item: item.observed_ts)
        item = Evidence(original.source_key, original.state, original.observed_ts, self.now_ts, self._identity(f"delay-{original.source_key}"), "synthetic", original.freshness_limit_sec)
        self._run_action("delay_receive", item, lambda: self._append(item, action="delay_receive"))

    def advance_clock(self, offset: int) -> None:
        def mutation() -> None:
            self.now_ts = max(self.now_ts, self.now_ts + max(0, offset))
            self._sync(action=f"advance_clock_{offset}")

        self._run_action(f"advance_clock_{offset}", None, mutation)

    def restart_component(self) -> None:
        self._run_action("restart_component", None, lambda: self._sync(action="restart_component"))

    def reopen_repository(self) -> None:
        def mutation() -> None:
            self.repository = MonitoringRepository(self.database)
            self.repository_generation += 1
            self._sync(action="reopen_repository")

        self._run_action("reopen_repository", None, mutation)

    def inject_action_exception_for_test(self) -> None:
        item = Evidence(
            "a",
            "bad",
            self.now_ts,
            self.now_ts,
            self._identity("test-injected"),
            "synthetic",
            SOURCES["a"].ttl_sec,
        )

        def mutation() -> None:
            raise StatefulHarnessFailure("test-only injected action exception")

        self._run_action("test_injected_exception", item, mutation)

    def choose_and_apply(self, rng: random.Random) -> None:
        choices = (
            lambda: self.ingest("a", rng.choice(("good", "bad"))),
            lambda: self.ingest("b", rng.choice(("good", "bad"))),
            lambda: self.replay_old("a"),
            lambda: self.replay_old("b"),
            self.duplicate_last,
            self.delay_receive,
            lambda: self.advance_clock(rng.choice((1, 59, 60, 179, 180, 181))),
            self.restart_component,
            self.reopen_repository,
        )
        rng.choice(choices)()


def _source_identity() -> dict[str, str]:
    return source_identity(Path(__file__).resolve().parents[1])


def explore(
    *,
    seed: int,
    examples: int,
    max_steps: int,
    wall_time_sec: int = DEFAULT_WALL_TIME_SEC,
    action_log_path: Path | None = None,
) -> dict[str, Any]:
    config = ExplorationConfig(seed, examples, max_steps, wall_time_sec)
    return bounded_explore(
        config=config,
        machine_factory=lambda machine_seed, example: DeliveryStateMachine(
            seed=machine_seed,
            max_steps=max_steps,
            example_index=example,
            action_log_path=action_log_path,
        ),
        artifact_builder=lambda started, totals, runs, result, log_path: _artifact(
            seed,
            examples,
            max_steps,
            started,
            totals,
            runs,
            result,
            action_log_path=log_path,
        ),
        action_log_path=action_log_path,
        sut_failure_context=lambda machine, _exc: {
            "db_state": machine.database_state(),
            **_failure_metadata(machine.trace),
        },
    )


def reproduce_equal_timestamp_conflict() -> dict[str, Any]:
    """Fixed two-step regression fixture for the first discovered SUT failure."""
    started = time.monotonic()
    seed = 909_100_004
    machine = DeliveryStateMachine(seed=seed, max_steps=1)
    try:
        machine.ingest("b", "bad")
    except SUTInvariantViolation as exc:
        artifact = _artifact(
            seed,
            1,
            1,
            started,
            {"action_count": 1, "expected_perturbation_effects": 1, "oracle_gaps": 0, "sut_failures": 1, "harness_failures": 0, "unknown": 0},
            [{"example": 0, "steps": 1, "trace": machine.trace}],
            {"classification": "sut_monitoring_failure", "reason": str(exc), "minimized_trace": machine.trace, "db_state": machine.database_state(), "broken_invariant": "I4_equal_priority_disagreement_fail_closed", "root_cause_hypothesis": "domain_current equal observed_at/reduced_at snapshot_id tie-break can retain an earlier good canonical instead of persisting source_disagreement"},
        )
        artifact["reproduction_command"] = "PYTHONPATH=src python3 tests/stateful_delivery_authority_harness.py --reproduce-equal-timestamp --artifact OUTPUT_ARTIFACT.json"
        return artifact
    else:
        artifact = _artifact(
            seed,
            1,
            1,
            started,
            {"action_count": 1, "expected_perturbation_effects": machine.expected_effects, "oracle_gaps": 0, "sut_failures": 0, "harness_failures": 0, "unknown": 0},
            [{"example": 0, "steps": 1, "trace": machine.trace}],
            {"classification": "pass", "fixed_regression": "I4_equal_priority_disagreement_fail_closed", "minimized_trace": machine.trace, "db_state": machine.database_state()},
        )
        artifact["reproduction_command"] = "PYTHONPATH=src python3 tests/stateful_delivery_authority_harness.py --reproduce-equal-timestamp --artifact OUTPUT_ARTIFACT.json"
        return artifact
    finally:
        machine.close()


def _artifact(
    seed: int,
    examples: int,
    max_steps: int,
    started: float,
    totals: dict[str, int],
    runs: list[dict[str, Any]],
    result: dict[str, Any],
    *,
    action_log_path: Path | None = None,
) -> dict[str, Any]:
    artifact = {
        "artifact_schema": "monitoring_v4.delivery_stateful_authority.v1",
        "source_identity": _source_identity(),
        "historical_anchor": {"window_id": HISTORICAL_WINDOW_ID, "source": "runtime_delivery_watchdog", "state": "good", "observed_at": utc_text(HISTORICAL_ANCHOR_TS), "provenance": "historical_observed"},
        "oracle": {"revision": ORACLE_REVISION, "source_policy_revision": SOURCE_POLICY_REVISION, "reducer_revision": REDUCER_REVISION, "incident_policy_revision": INCIDENT_POLICY_REVISION, "scope": [fact.source for fact in SOURCES.values()], "generation_semantics": "out_of_scope"},
        "bounds": {"seed": seed, "examples": examples, "max_steps": max_steps, "max_rows": MAX_ROWS, "max_artifact_bytes": MAX_ARTIFACT_BYTES, "wall_time_sec": DEFAULT_WALL_TIME_SEC},
        "totals": totals,
        "result": result,
        "runs": runs,
        "write_ahead_action_log": WriteAheadActionLog.metadata(action_log_path),
        "reproduction_command": f"PYTHONPATH=src python3 tests/stateful_delivery_authority_harness.py --seed {seed} --examples {examples} --max-steps {max_steps} --artifact OUTPUT_ARTIFACT.json",
        "duration_sec": round(time.monotonic() - started, 6),
    }
    require_artifact_size(artifact)
    return artifact


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--examples", type=int)
    parser.add_argument("--max-steps", type=int)
    parser.add_argument("--reproduce-equal-timestamp", action="store_true")
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.reproduce_equal_timestamp:
        artifact = reproduce_equal_timestamp_conflict()
    else:
        if args.seed is None or args.examples is None or args.max_steps is None:
            parser.error("--seed, --examples and --max-steps are required for exploration")
        action_log_path = args.artifact.with_name(f"{args.artifact.stem}.actions.jsonl")
        artifact = explore(
            seed=args.seed,
            examples=args.examples,
            max_steps=args.max_steps,
            action_log_path=action_log_path,
        )
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(artifact, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return 0 if artifact["result"]["classification"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
