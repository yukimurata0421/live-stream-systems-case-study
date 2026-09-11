#!/usr/bin/env python3
"""Bounded youtube_lifecycle authority/diagnostic/supporting exploration."""
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


ORACLE_REVISION = "youtube-lifecycle-role-boundary-oracle-r1"
SYNTHETIC_ANCHOR_TS = unix_ts("2026-08-21T00:00:00Z")
SLICE_SOURCES = {
    "authoritative": ("watchdog", "resolver"),
    "diagnostic": ("watchdog", "resolver", "diagnostic"),
    "supporting": ("watchdog", "resolver", "diagnostic", "supporting"),
}


@dataclass(frozen=True)
class SourceFact:
    source: str
    role: str
    evidence_role: str
    priority: int
    ttl_sec: int
    current_authority: bool


SOURCES = {
    "watchdog": SourceFact(
        "youtube_watchdog", "authoritative", "current_authoritative", 100, 180, True
    ),
    "resolver": SourceFact(
        "youtube_video_resolver", "authoritative", "current_authoritative", 80, 90, True
    ),
    "diagnostic": SourceFact(
        "youtube_api_direct_lifecycle", "diagnostic", "current_authoritative", 90, 300, False
    ),
    "supporting": SourceFact(
        "youtube_public", "supporting", "supporting", 30, 300, False
    ),
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
    def fact(self) -> SourceFact:
        return SOURCES[self.source_key]

    @property
    def source(self) -> str:
        return self.fact.source

    @property
    def effective_ttl(self) -> int:
        return min(self.fact.ttl_sec, self.freshness_limit_sec)

    @property
    def latest_key(self) -> tuple[int, int, str]:
        return (self.observed_ts, self.received_ts, self.observation_id)

    @property
    def observation_id(self) -> str:
        return self.envelope().observation_id

    def envelope(self) -> ObservationEnvelope:
        return ObservationEnvelope.create(
            domain="youtube_lifecycle",
            source=self.source,
            source_event_id=self.identity,
            source_generation=f"synthetic-generation-{self.source_key}",
            evidence_role=self.fact.evidence_role,
            status=self.state,
            reason_code=f"synthetic_{self.state}_evidence",
            observed_at=utc_text(self.observed_ts),
            received_at=utc_text(self.received_ts),
            freshness_limit_sec=self.freshness_limit_sec,
            producer_revision="stateful-youtube-lifecycle-r1",
            payload={
                "synthetic": True,
                "provenance": self.provenance,
                "modeled_role": self.fact.role,
            },
        )


@dataclass(frozen=True)
class ExpectedCanonical:
    state: str
    reason: str
    observed_ts: int
    valid_until_ts: int
    selected_sources: tuple[str, ...]


class YouTubeLifecycleOracle:
    """Independent oracle for the audited role and authoritative-priority facts."""

    def canonical(self, evidence: list[Evidence], *, now_ts: int) -> ExpectedCanonical:
        latest: dict[str, Evidence] = {}
        for item in evidence:
            previous = latest.get(item.source_key)
            if previous is None or item.latest_key > previous.latest_key:
                latest[item.source_key] = item
        fresh_authoritative = [
            item
            for item in latest.values()
            if item.fact.role == "authoritative"
            and item.observed_ts <= now_ts
            and now_ts - item.observed_ts <= item.effective_ttl
        ]
        if not fresh_authoritative:
            return ExpectedCanonical(
                "unknown", "missing_current_evidence", now_ts, now_ts, ()
            )
        top_priority = max(item.fact.priority for item in fresh_authoritative)
        selected = [
            item for item in fresh_authoritative if item.fact.priority == top_priority
        ]
        if len(selected) != 1:
            raise StatefulHarnessFailure(
                "youtube_lifecycle oracle scope expects distinct authoritative priorities"
            )
        item = selected[0]
        return ExpectedCanonical(
            state=item.state,
            reason=f"current_{item.state}_evidence",
            observed_ts=item.observed_ts,
            valid_until_ts=item.observed_ts + item.effective_ttl,
            selected_sources=(item.source,),
        )


def validate_oracle_policy_alignment() -> dict[str, dict[str, Any]]:
    policy = DEFAULT_POLICIES["youtube_lifecycle"]
    if policy.revision != SOURCE_POLICY_REVISION:
        raise StatefulHarnessFailure("unexpected source policy revision")
    result: dict[str, dict[str, Any]] = {}
    for key, fact in SOURCES.items():
        rule = policy.rule(fact.source)
        if rule is None:
            raise StatefulHarnessFailure(f"youtube source missing from policy: {fact.source}")
        if rule.priority != fact.priority or rule.ttl_sec != fact.ttl_sec:
            raise StatefulHarnessFailure(f"youtube policy fact drifted: {fact.source}")
        if fact.evidence_role not in rule.roles:
            raise StatefulHarnessFailure(f"youtube source role drifted: {fact.source}")
        if fact.role == "authoritative":
            effective_authority = rule.current_authority and fact.evidence_role in policy.allowed_roles
        elif fact.role == "diagnostic":
            effective_authority = rule.current_authority
        else:
            effective_authority = (
                rule.current_authority and fact.evidence_role in policy.allowed_roles
            )
        if effective_authority != fact.current_authority:
            raise StatefulHarnessFailure(f"youtube effective authority drifted: {fact.source}")
        result[key] = {
            "source": fact.source,
            "modeled_role": fact.role,
            "evidence_role": fact.evidence_role,
            "priority": fact.priority,
            "ttl_sec": fact.ttl_sec,
            "configured_current_authority": rule.current_authority,
            "effective_canonical_authority": effective_authority,
            "allowed_by_domain_role": fact.evidence_role in policy.allowed_roles,
            "diagnostic_kind": rule.diagnostic_kind,
        }
    return result


def _current_public(item: Any) -> dict[str, Any]:
    payload = item.payload if isinstance(item.payload, dict) else {}
    selected = payload.get("selected") or []
    diagnostics = payload.get("diagnostics") or []
    ignored = payload.get("ignored") or []
    return {
        "state": item.state,
        "reason": item.reason_codes[0] if item.reason_codes else "",
        "observed_ts": unix_ts(item.observed_at),
        "valid_until_ts": unix_ts(item.valid_until),
        "selected_sources": tuple(sorted(raw["source"] for raw in selected)),
        "diagnostic_sources": tuple(sorted(raw["source"] for raw in diagnostics)),
        "ignored": tuple(
            sorted((raw["source"], raw["reason"]) for raw in ignored)
        ),
    }


class YouTubeLifecycleStateMachine:
    def __init__(
        self,
        *,
        slice_name: str,
        seed: int,
        max_steps: int,
        example_index: int = 0,
        action_log_path: Path | None = None,
    ) -> None:
        self.policy_identity = validate_oracle_policy_alignment()
        if slice_name not in SLICE_SOURCES:
            raise StatefulHarnessFailure(f"unsupported slice: {slice_name}")
        self.slice_name = slice_name
        self.enabled_sources = SLICE_SOURCES[slice_name]
        self.seed = seed
        self.max_steps = max_steps
        self.example_index = example_index
        self.run_id = f"youtube-lifecycle-{slice_name}-{seed}-{example_index}"
        self.root = Path(tempfile.mkdtemp(prefix="stream-v4-stateful-youtube-lifecycle-"))
        self.database = self.root / "youtube_lifecycle.sqlite3"
        self.repository = MonitoringRepository(self.database)
        self.repository.initialize(applied_at=utc_text(SYNTHETIC_ANCHOR_TS))
        self.oracle = YouTubeLifecycleOracle()
        self.now_ts = SYNTHETIC_ANCHOR_TS
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
            capture_state=self.capture_state,
            action_log=self.action_log,
        )
        self.action_events = self.action_runtime.events
        self.expected_effects = 0
        anchor = Evidence(
            "watchdog",
            "good",
            self.now_ts,
            self.now_ts,
            "synthetic-youtube-lifecycle-good",
            "synthetic_initialization",
            SOURCES["watchdog"].ttl_sec,
        )
        self._run_action(
            "synthetic_initial_anchor",
            anchor,
            lambda: self._append(anchor, action="synthetic_initial_anchor"),
        )

    def close(self) -> dict[str, Any]:
        return verified_cleanup(self.root, delete=shutil.rmtree)

    def _episode_state(self) -> list[dict[str, Any]]:
        with self.repository.connection(read_only=True) as connection:
            rows = connection.execute(
                """SELECT episode_id, status, severity, opened_at, last_bad_at,
                          closed_at, last_transition_at, bad_samples, unknown_samples
                   FROM incident_episodes
                   WHERE domain='youtube_lifecycle'
                   ORDER BY opened_ts, episode_id"""
            ).fetchall()
        return [dict(row) for row in rows]

    def capture_state(self) -> dict[str, Any]:
        current = self.repository.current("youtube_lifecycle")
        transitions = self.repository.transitions(domain="youtube_lifecycle")
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
                "modeled_role": None,
                "evidence_role": None,
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
            "modeled_role": item.fact.role,
            "evidence_role": item.fact.evidence_role,
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

    def _sync(
        self,
        *,
        action: str,
        accepted: bool | None = None,
        item: Evidence | None = None,
    ) -> None:
        current = CurrentReducerService(self.repository, DEFAULT_POLICIES).reduce(
            ("youtube_lifecycle",), now_ts=self.now_ts
        )[0]
        IncidentService(
            self.repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy()
        ).process(current, now_at=utc_text(self.now_ts))
        expected = self.oracle.canonical(self.evidence, now_ts=self.now_ts)
        actual = _current_public(current)
        required = (
            expected.state,
            expected.reason,
            expected.observed_ts,
            expected.valid_until_ts,
            expected.selected_sources,
        )
        observed = (
            actual["state"],
            actual["reason"],
            actual["observed_ts"],
            actual["valid_until_ts"],
            actual["selected_sources"],
        )
        post = self.capture_state()
        attribution = self.action_runtime.attribution(post)
        record = {
            "action": action,
            "logical_now": utc_text(self.now_ts),
            "slice": self.slice_name,
            "accepted": accepted,
            "evidence": None
            if item is None
            else {
                "source": item.source,
                "modeled_role": item.fact.role,
                "state": item.state,
                "observed_at": utc_text(item.observed_ts),
                "received_at": utc_text(item.received_ts),
                "provenance": item.provenance,
            },
            "expected": {
                "state": expected.state,
                "reason": expected.reason,
                "selected_sources": expected.selected_sources,
            },
            "actual": actual,
            "transitions": post["transition_count"],
            "intents": post["intent_count"],
            "attribution": attribution,
        }
        self.trace.append(record)
        if not attribution["attribution_complete"]:
            raise StatefulAttributionGap(
                "stateful attribution gap: "
                + ", ".join(attribution["attribution_errors"])
            )
        if observed != required:
            raise SUTInvariantViolation(
                f"oracle mismatch expected={required!r} actual={observed!r}"
            )
        if any(
            source in actual["selected_sources"]
            for source in (
                SOURCES["diagnostic"].source,
                SOURCES["supporting"].source,
            )
        ):
            raise SUTInvariantViolation("non-authoritative role selected as canonical")
        semantic = required
        if self._last_semantic == semantic and (
            post["transition_count"],
            post["intent_count"],
        ) != (self._transition_count, self._intent_count):
            raise SUTInvariantViolation(
                "canonical semantics unchanged but transition or intent count increased"
            )
        if (
            any(
                transition["phase"] == "recovered"
                for transition in attribution["created_transitions"]
            )
            and expected.state != "good"
        ):
            raise SUTInvariantViolation("recovered transition without authoritative good")
        if expected.state != "good":
            self.expected_effects += 1
        self._last_semantic = semantic
        self._transition_count = post["transition_count"]
        self._intent_count = post["intent_count"]
        if len(self.trace) > self.max_steps + 1:
            raise StatefulHarnessFailure("action trace bound exceeded")
        with self.repository.connection(read_only=True) as connection:
            rows = int(
                connection.execute("SELECT count(*) FROM observations").fetchone()[0]
            )
        if rows > MAX_ROWS:
            raise StatefulHarnessFailure("SQLite row bound exceeded")

    def ingest(self, source_key: str, state: str) -> None:
        if source_key not in self.enabled_sources:
            raise StatefulHarnessFailure(f"source disabled for slice: {source_key}")
        item = Evidence(
            source_key,
            state,
            self.now_ts,
            self.now_ts,
            self._identity(f"ingest-{source_key}"),
            "synthetic",
            SOURCES[source_key].ttl_sec,
        )
        self._run_action(
            f"ingest_{source_key}_{state}",
            item,
            lambda: self._append(item, action=f"ingest_{source_key}_{state}"),
        )
        self.now_ts += 1

    def replay_old(self, source_key: str) -> None:
        if source_key not in self.enabled_sources:
            raise StatefulHarnessFailure(f"source disabled for slice: {source_key}")
        candidates = [item for item in self.evidence if item.source_key == source_key]
        if not candidates:
            self._run_action(
                f"replay_old_{source_key}_noop",
                None,
                lambda: self._sync(action=f"replay_old_{source_key}_noop"),
            )
            return
        original = min(candidates, key=lambda item: item.observed_ts)
        item = Evidence(
            source_key,
            original.state,
            original.observed_ts,
            self.now_ts,
            self._identity(f"replay-{source_key}"),
            "synthetic",
            original.freshness_limit_sec,
        )
        self._run_action(
            f"replay_old_{source_key}",
            item,
            lambda: self._append(item, action=f"replay_old_{source_key}"),
        )

    def duplicate_last(self) -> None:
        item = self.evidence[-1]
        self._run_action(
            "duplicate_last", item, lambda: self._append(item, action="duplicate_last")
        )

    def delay_receive(self) -> None:
        original = min(self.evidence, key=lambda item: item.observed_ts)
        item = Evidence(
            original.source_key,
            original.state,
            original.observed_ts,
            self.now_ts,
            self._identity(f"delay-{original.source_key}"),
            "synthetic",
            original.freshness_limit_sec,
        )
        self._run_action(
            "delay_receive", item, lambda: self._append(item, action="delay_receive")
        )

    def advance_clock(self, offset: int) -> None:
        def mutation() -> None:
            self.now_ts += max(0, offset)
            self._sync(action=f"advance_clock_{offset}")

        self._run_action(f"advance_clock_{offset}", None, mutation)

    def restart_component(self) -> None:
        self._run_action(
            "restart_component",
            None,
            lambda: self._sync(action="restart_component"),
        )

    def reopen_repository(self) -> None:
        def mutation() -> None:
            self.repository = MonitoringRepository(self.database)
            self.repository_generation += 1
            self._sync(action="reopen_repository")

        self._run_action("reopen_repository", None, mutation)

    def choose_and_apply(self, rng: random.Random) -> None:
        choices: list[Any] = [
            self.duplicate_last,
            self.delay_receive,
            lambda: self.advance_clock(rng.choice((1, 89, 90, 91, 179, 180, 181))),
            self.restart_component,
            self.reopen_repository,
        ]
        for source_key in self.enabled_sources:
            choices.extend(
                (
                    lambda key=source_key: self.ingest(
                        key, rng.choice(("good", "bad"))
                    ),
                    lambda key=source_key: self.replay_old(key),
                )
            )
        rng.choice(choices)()


def _artifact(
    *,
    slice_name: str,
    seed: int,
    examples: int,
    max_steps: int,
    started: float,
    totals: dict[str, int],
    runs: list[dict[str, Any]],
    result: dict[str, Any],
    action_log_path: Path | None,
) -> dict[str, Any]:
    artifact = {
        "artifact_schema": "monitoring_v4.youtube_lifecycle_stateful_authority.v1",
        "source_identity": source_identity(Path(__file__).resolve().parents[1]),
        "slice": {
            "name": slice_name,
            "enabled_source_keys": SLICE_SOURCES[slice_name],
            "sources": [
                {
                    "source": SOURCES[key].source,
                    "modeled_role": SOURCES[key].role,
                    "evidence_role": SOURCES[key].evidence_role,
                    "priority": SOURCES[key].priority,
                    "ttl_sec": SOURCES[key].ttl_sec,
                    "effective_canonical_authority": SOURCES[key].current_authority,
                }
                for key in SLICE_SOURCES[slice_name]
            ],
        },
        "synthetic_anchor": {
            "source": "youtube_watchdog",
            "state": "good",
            "observed_at": utc_text(SYNTHETIC_ANCHOR_TS),
            "provenance": "synthetic_initialization",
        },
        "oracle": {
            "revision": ORACLE_REVISION,
            "source_policy_revision": SOURCE_POLICY_REVISION,
            "reducer_revision": REDUCER_REVISION,
            "incident_policy_revision": INCIDENT_POLICY_REVISION,
            "selection_order": [
                "role eligibility",
                "freshness",
                "authoritative priority",
                "canonical state",
            ],
        },
        "bounds": {
            "seed": seed,
            "examples": examples,
            "max_steps": max_steps,
            "max_rows": MAX_ROWS,
            "max_artifact_bytes": MAX_ARTIFACT_BYTES,
            "wall_time_sec": DEFAULT_WALL_TIME_SEC,
        },
        "totals": totals,
        "result": result,
        "runs": runs,
        "write_ahead_action_log": WriteAheadActionLog.metadata(action_log_path),
        "reproduction_command": (
            "PYTHONPATH=src python3 "
            "tests/stateful_youtube_lifecycle_authority_harness.py "
            f"--slice {slice_name} --seed {seed} --examples {examples} "
            f"--max-steps {max_steps} --artifact OUTPUT_ARTIFACT.json"
        ),
        "duration_sec": round(time.monotonic() - started, 6),
    }
    require_artifact_size(artifact)
    return artifact


def explore(
    *,
    slice_name: str,
    seed: int,
    examples: int,
    max_steps: int,
    wall_time_sec: int = DEFAULT_WALL_TIME_SEC,
    action_log_path: Path | None = None,
) -> dict[str, Any]:
    config = ExplorationConfig(seed, examples, max_steps, wall_time_sec)
    return bounded_explore(
        config=config,
        machine_factory=lambda machine_seed, example: YouTubeLifecycleStateMachine(
            slice_name=slice_name,
            seed=machine_seed,
            max_steps=max_steps,
            example_index=example,
            action_log_path=action_log_path,
        ),
        artifact_builder=lambda started, totals, runs, result, log_path: _artifact(
            slice_name=slice_name,
            seed=seed,
            examples=examples,
            max_steps=max_steps,
            started=started,
            totals=totals,
            runs=runs,
            result=result,
            action_log_path=log_path,
        ),
        action_log_path=action_log_path,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--slice", required=True, choices=tuple(SLICE_SOURCES)
    )
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--examples", required=True, type=int)
    parser.add_argument("--max-steps", required=True, type=int)
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args(argv)
    action_log_path = args.artifact.with_name(f"{args.artifact.stem}.actions.jsonl")
    artifact = explore(
        slice_name=args.slice,
        seed=args.seed,
        examples=args.examples,
        max_steps=args.max_steps,
        action_log_path=action_log_path,
    )
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(
        json.dumps(artifact, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return 0 if artifact["result"]["classification"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
