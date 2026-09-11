#!/usr/bin/env python3
"""Adapter-realistic YouTube lifecycle status/incident exploration.

This is a separate r2 slice.  It imports the frozen v1 YouTube domain adapter
mechanics without changing the v1 harness or its artifacts.  The additional
scope is deliberately limited to source behavior visible in the current
adapters: current_correlated resolver/API evidence, authoritative ``unknown``,
diagnostic disagreement context, and the incident candidate/repeat cadence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.time import utc_text
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
    from tests.stateful_youtube_lifecycle_authority_harness import (
        ExpectedCanonical,
        SYNTHETIC_ANCHOR_TS,
        YouTubeLifecycleStateMachine,
        _current_public,
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
    from stateful_youtube_lifecycle_authority_harness import (  # type: ignore[no-redef]
        ExpectedCanonical,
        SYNTHETIC_ANCHOR_TS,
        YouTubeLifecycleStateMachine,
        _current_public,
    )


ORACLE_REVISION = "youtube-lifecycle-adapter-incident-oracle-r2"
SLICE_NAME = "adapter_status_incident"


@dataclass(frozen=True)
class AdapterSourceFact:
    source: str
    modeled_role: str
    evidence_role: str
    priority: int
    ttl_sec: int
    current_authority: bool
    producer_id: str
    diagnostic_kind: str

    @property
    def role(self) -> str:
        """Compatibility name used by the frozen v1 action serializer."""
        return self.modeled_role


SOURCES = {
    "watchdog": AdapterSourceFact(
        "youtube_watchdog",
        "authoritative",
        "current_authoritative",
        100,
        180,
        True,
        "youtube_watchdog",
        "correlated_non_authoritative",
    ),
    "resolver": AdapterSourceFact(
        "youtube_video_resolver",
        "authoritative",
        "current_correlated",
        80,
        90,
        True,
        "youtube_video_resolver",
        "correlated_non_authoritative",
    ),
    "diagnostic": AdapterSourceFact(
        "youtube_api_direct_lifecycle",
        "diagnostic",
        "current_correlated",
        90,
        300,
        False,
        "youtube_watchdog",
        "same_producer_api_probe",
    ),
}


@dataclass(frozen=True)
class AdapterEvidence:
    source_key: str
    state: str
    observed_ts: int
    received_ts: int
    identity: str
    provenance: str
    freshness_limit_sec: int

    @property
    def fact(self) -> AdapterSourceFact:
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
            source_generation=f"synthetic-adapter-generation-{self.source_key}",
            evidence_role=self.fact.evidence_role,
            status=self.state,
            reason_code=f"synthetic_{self.state}_evidence",
            observed_at=utc_text(self.observed_ts),
            received_at=utc_text(self.received_ts),
            freshness_limit_sec=self.freshness_limit_sec,
            producer_revision="stateful-youtube-lifecycle-expanded-r2",
            payload={
                "synthetic": True,
                "provenance": self.provenance,
                "modeled_role": self.fact.modeled_role,
            },
        )


@dataclass(frozen=True)
class ExpectedContext:
    canonical: ExpectedCanonical
    diagnostic_sources: tuple[str, ...]
    ignored: tuple[tuple[str, str], ...]
    same_producer_api_disagreement: bool


class ExpandedYouTubeLifecycleOracle:
    """Independent r2 oracle derived from current source/incident contracts."""

    @staticmethod
    def _latest(evidence: list[Any]) -> dict[str, Any]:
        latest: dict[str, Any] = {}
        for item in evidence:
            previous = latest.get(item.source_key)
            if previous is None or item.latest_key > previous.latest_key:
                latest[item.source_key] = item
        return latest

    def context(self, evidence: list[Any], *, now_ts: int) -> ExpectedContext:
        latest = self._latest(evidence)
        eligible: list[Any] = []
        diagnostics: list[Any] = []
        ignored: list[tuple[str, str]] = []
        for item in latest.values():
            fact = SOURCES[item.source_key]
            if item.fact.evidence_role != fact.evidence_role:
                raise StatefulHarnessFailure(
                    f"adapter evidence role drifted for {item.source_key}"
                )
            if item.observed_ts > now_ts:
                ignored.append((fact.source, "future_timestamp"))
                continue
            if now_ts - item.observed_ts > min(fact.ttl_sec, item.freshness_limit_sec):
                ignored.append((fact.source, "stale"))
                continue
            if not fact.current_authority:
                diagnostics.append(item)
                ignored.append((fact.source, "correlated_not_current_authority"))
                continue
            eligible.append(item)

        if not eligible:
            canonical = ExpectedCanonical(
                "unknown", "missing_current_evidence", now_ts, now_ts, ()
            )
            selected: list[Any] = []
        else:
            top = max(SOURCES[item.source_key].priority for item in eligible)
            selected = [
                item for item in eligible if SOURCES[item.source_key].priority == top
            ]
            if len(selected) != 1:
                raise StatefulHarnessFailure(
                    "expanded oracle expects distinct authoritative priorities"
                )
            item = selected[0]
            if item.state == "good":
                state, reason = "good", "current_good_evidence"
            elif item.state == "bad":
                state, reason = "bad", "current_bad_evidence"
            elif item.state == "unknown":
                state, reason = "unknown", "current_evidence_unknown"
            else:
                raise StatefulHarnessFailure(
                    f"status outside expanded deterministic scope: {item.state}"
                )
            canonical = ExpectedCanonical(
                state,
                reason,
                item.observed_ts,
                item.observed_ts + min(
                    SOURCES[item.source_key].ttl_sec, item.freshness_limit_sec
                ),
                (SOURCES[item.source_key].source,),
            )

        selected_by_producer = {
            SOURCES[item.source_key].producer_id: item.state for item in selected
        }
        disagreement = any(
            SOURCES[item.source_key].diagnostic_kind == "same_producer_api_probe"
            and SOURCES[item.source_key].producer_id in selected_by_producer
            and item.state != selected_by_producer[SOURCES[item.source_key].producer_id]
            for item in diagnostics
        )
        return ExpectedContext(
            canonical=canonical,
            diagnostic_sources=tuple(sorted(item.source for item in diagnostics)),
            ignored=tuple(sorted(ignored)),
            same_producer_api_disagreement=disagreement,
        )

    def canonical(self, evidence: list[Any], *, now_ts: int) -> ExpectedCanonical:
        return self.context(evidence, now_ts=now_ts).canonical


def validate_adapter_policy_alignment() -> dict[str, dict[str, Any]]:
    policy = DEFAULT_POLICIES["youtube_lifecycle"]
    if policy.revision != SOURCE_POLICY_REVISION:
        raise StatefulHarnessFailure("unexpected source policy revision")
    result: dict[str, dict[str, Any]] = {}
    for key, fact in SOURCES.items():
        rule = policy.rule(fact.source)
        if rule is None:
            raise StatefulHarnessFailure(f"missing source policy: {fact.source}")
        actual = (
            rule.priority,
            rule.ttl_sec,
            fact.evidence_role in rule.roles,
            rule.current_authority,
            rule.producer_key,
            rule.diagnostic_kind,
        )
        expected = (
            fact.priority,
            fact.ttl_sec,
            True,
            fact.current_authority,
            fact.producer_id,
            fact.diagnostic_kind,
        )
        if actual != expected:
            raise StatefulHarnessFailure(
                f"adapter/source policy alignment drifted: {fact.source}"
            )
        if fact.current_authority and fact.evidence_role not in policy.allowed_roles:
            raise StatefulHarnessFailure(
                f"authoritative adapter role is not domain-current eligible: {fact.source}"
            )
        result[key] = {
            "source": fact.source,
            "modeled_role": fact.modeled_role,
            "adapter_evidence_role": fact.evidence_role,
            "priority": fact.priority,
            "ttl_sec": fact.ttl_sec,
            "current_authority": fact.current_authority,
            "producer_id": fact.producer_id,
            "diagnostic_kind": fact.diagnostic_kind,
        }
    return result


# action, positional args, expected phase, candidate samples, active episode
SCENARIOS: dict[str, tuple[tuple[str, tuple[Any, ...], str | None, int | str, bool], ...]] = {
    "authoritative_unknown_gate": (
        ("advance", (1,), None, "absent", False),
        ("ingest", ("watchdog", "unknown"), None, 1, False),
        ("advance", (149,), None, 1, False),
        ("ingest", ("watchdog", "unknown"), None, 2, False),
        ("advance", (149,), None, 2, False),
        ("ingest", ("watchdog", "unknown"), "detected", "absent", True),
        ("ingest", ("diagnostic", "good"), None, "absent", True),
        ("ingest", ("watchdog", "good"), "recovered", "absent", False),
    ),
    "missing_current_diagnostic_gate": (
        ("advance", (181,), None, 1, False),
        ("ingest", ("diagnostic", "good"), None, 2, False),
        ("advance", (298,), None, 3, False),
        ("advance", (1,), "detected", "absent", True),
        ("ingest", ("diagnostic", "bad"), None, "absent", True),
        ("ingest", ("resolver", "good"), "recovered", "absent", False),
    ),
    "diagnostic_disagreement": (
        ("ingest", ("diagnostic", "bad"), None, "absent", False),
        ("ingest", ("diagnostic", "good"), None, "absent", False),
        ("advance", (180,), None, 1, False),
        ("ingest", ("resolver", "good"), None, "absent", False),
        ("ingest", ("watchdog", "bad"), "detected", "absent", True),
        ("ingest", ("diagnostic", "good"), None, "absent", True),
        ("ingest", ("watchdog", "good"), "recovered", "absent", False),
    ),
    "diagnostic_due_repeat": (
        ("advance", (1,), None, "absent", False),
        ("ingest", ("watchdog", "bad"), "detected", "absent", True),
        ("advance", (499,), None, "absent", True),
        ("ingest", ("watchdog", "bad"), None, "absent", True),
        ("advance", (99,), None, "absent", True),
        ("ingest", ("diagnostic", "good"), "repeat", "absent", True),
        ("ingest", ("watchdog", "good"), "recovered", "absent", False),
    ),
    "candidate_restart_reopen": (
        ("advance", (181,), None, 1, False),
        ("reopen", (), None, 1, False),
        ("restart", (), None, 1, False),
        ("ingest", ("diagnostic", "unknown"), None, 2, False),
        ("advance", (300,), "detected", "absent", True),
        ("reopen", (), None, "absent", True),
        ("ingest", ("resolver", "good"), "recovered", "absent", False),
    ),
}


class ExpandedYouTubeLifecycleStateMachine(YouTubeLifecycleStateMachine):
    """Directed stateful scenarios with cadence-aware incident assertions."""

    def __init__(
        self,
        *,
        seed: int,
        max_steps: int,
        example_index: int = 0,
        scenario_name: str | None = None,
        action_log_path: Path | None = None,
    ) -> None:
        self.policy_identity = validate_adapter_policy_alignment()
        self.slice_name = SLICE_NAME
        self.enabled_sources = tuple(SOURCES)
        self.seed = seed
        self.max_steps = max_steps
        self.example_index = example_index
        names = tuple(SCENARIOS)
        self.scenario_name = scenario_name or names[example_index % len(names)]
        if self.scenario_name not in SCENARIOS:
            raise StatefulHarnessFailure(f"unsupported expanded scenario: {self.scenario_name}")
        self.run_id = f"youtube-lifecycle-expanded-{seed}-{example_index}"
        self.root = Path(tempfile.mkdtemp(prefix="stream-v4-stateful-youtube-expanded-"))
        self.database = self.root / "youtube_lifecycle.sqlite3"
        self.repository = MonitoringRepository(self.database)
        self.repository.initialize(applied_at=utc_text(SYNTHETIC_ANCHOR_TS))
        self.oracle = ExpandedYouTubeLifecycleOracle()
        self.now_ts = SYNTHETIC_ANCHOR_TS
        self.evidence: list[Any] = []
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
        self.plan_index = 0
        self._expected_phase: str | None = None
        self._candidate_expectation: int | str = "any"
        self._active_expectation: bool | None = None
        anchor = AdapterEvidence(
            "watchdog",
            "good",
            self.now_ts,
            self.now_ts,
            "synthetic-youtube-lifecycle-expanded-good",
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

    def _candidate(self) -> dict[str, Any] | None:
        with self.repository.connection(read_only=True) as connection:
            return self.repository.candidate("youtube_lifecycle", connection=connection)

    def _sync(
        self,
        *,
        action: str,
        accepted: bool | None = None,
        item: Any | None = None,
    ) -> None:
        current = CurrentReducerService(self.repository, DEFAULT_POLICIES).reduce(
            ("youtube_lifecycle",), now_ts=self.now_ts
        )[0]
        IncidentService(
            self.repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy()
        ).process(current, now_at=utc_text(self.now_ts))
        expected_context = self.oracle.context(self.evidence, now_ts=self.now_ts)
        expected = expected_context.canonical
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
        phases = tuple(item["phase"] for item in attribution["created_transitions"])
        expected_phases = () if self._expected_phase is None else (self._expected_phase,)
        payload = current.payload
        record = {
            "action": action,
            "logical_now": utc_text(self.now_ts),
            "slice": self.slice_name,
            "scenario": self.scenario_name,
            "scenario_step": self.plan_index,
            "scenario_complete": self.plan_index == len(SCENARIOS[self.scenario_name]),
            "accepted": accepted,
            "evidence": None
            if item is None
            else {
                "source": item.source,
                "modeled_role": item.fact.modeled_role,
                "evidence_role": item.fact.evidence_role,
                "state": item.state,
                "observed_at": utc_text(item.observed_ts),
                "received_at": utc_text(item.received_ts),
                "provenance": item.provenance,
            },
            "expected": {
                "state": expected.state,
                "reason": expected.reason,
                "selected_sources": expected.selected_sources,
                "diagnostic_sources": expected_context.diagnostic_sources,
                "same_producer_api_disagreement": (
                    expected_context.same_producer_api_disagreement
                ),
                "transition_phases": expected_phases,
            },
            "actual": {
                **actual,
                "same_producer_api_disagreement": bool(
                    payload.get("same_producer_api_disagreement")
                ),
            },
            "candidate": self._candidate(),
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
                f"expanded oracle mismatch expected={required!r} actual={observed!r}"
            )
        if actual["diagnostic_sources"] != expected_context.diagnostic_sources:
            raise SUTInvariantViolation(
                "diagnostic source context differs from independent role oracle"
            )
        if bool(payload.get("same_producer_api_disagreement")) != (
            expected_context.same_producer_api_disagreement
        ):
            raise SUTInvariantViolation("same-producer API disagreement flag mismatch")
        if actual["ignored"] != expected_context.ignored:
            raise SUTInvariantViolation(
                f"ignored evidence mismatch expected={expected_context.ignored!r} "
                f"actual={actual['ignored']!r}"
            )
        if phases != expected_phases:
            raise SUTInvariantViolation(
                f"incident phase mismatch expected={expected_phases!r} actual={phases!r}"
            )
        candidate = record["candidate"]
        if self._candidate_expectation == "absent" and candidate is not None:
            raise SUTInvariantViolation("incident candidate unexpectedly present")
        if isinstance(self._candidate_expectation, int):
            if candidate is None or candidate["samples"] != self._candidate_expectation:
                raise SUTInvariantViolation(
                    "incident candidate sample count differs from directed oracle"
                )
        active = [episode for episode in post["episode_state"] if episode["status"] == "active"]
        if self._active_expectation is not None and bool(active) != self._active_expectation:
            raise SUTInvariantViolation(
                f"active episode mismatch expected={self._active_expectation} actual={bool(active)}"
            )
        semantic = required
        if self._last_semantic == semantic and (
            post["transition_count"], post["intent_count"]
        ) != (self._transition_count, self._intent_count):
            if self._expected_phase != "repeat":
                raise SUTInvariantViolation(
                    "canonical semantics unchanged outside an independently due repeat"
                )
        if expected.state != "good":
            self.expected_effects += 1
        self._last_semantic = semantic
        self._transition_count = post["transition_count"]
        self._intent_count = post["intent_count"]
        if len(self.trace) > self.max_steps + 1:
            raise StatefulHarnessFailure("expanded action trace bound exceeded")
        with self.repository.connection(read_only=True) as connection:
            rows = int(connection.execute("SELECT count(*) FROM observations").fetchone()[0])
        if rows > MAX_ROWS:
            raise StatefulHarnessFailure("SQLite row bound exceeded")

    def ingest(self, source_key: str, state: str) -> None:
        if source_key not in SOURCES:
            raise StatefulHarnessFailure(f"source outside expanded slice: {source_key}")
        if state not in {"good", "bad", "unknown"}:
            raise StatefulHarnessFailure(f"status outside expanded slice: {state}")
        item = AdapterEvidence(
            source_key,
            state,
            self.now_ts,
            self.now_ts,
            self._identity(f"expanded-{source_key}"),
            "synthetic_adapter_realistic",
            SOURCES[source_key].ttl_sec,
        )
        self._run_action(
            f"ingest_{source_key}_{state}",
            item,
            lambda: self._append(item, action=f"ingest_{source_key}_{state}"),
        )
        self.now_ts += 1

    def choose_and_apply(self, rng: random.Random) -> None:
        del rng
        plan = SCENARIOS[self.scenario_name]
        if self.plan_index >= len(plan):
            self._expected_phase = None
            self._candidate_expectation = "any"
            self._active_expectation = None
            self.restart_component()
            return
        action, args, phase, candidate, active = plan[self.plan_index]
        self.plan_index += 1
        self._expected_phase = phase
        self._candidate_expectation = candidate
        self._active_expectation = active
        if action == "advance":
            self.advance_clock(*args)
        elif action == "ingest":
            self.ingest(*args)
        elif action == "reopen":
            self.reopen_repository()
        elif action == "restart":
            self.restart_component()
        else:
            raise StatefulHarnessFailure(f"unknown expanded action: {action}")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _coverage(runs: list[dict[str, Any]]) -> dict[str, Any]:
    scenarios = {name: {"examples": 0, "completed": 0} for name in SCENARIOS}
    phases: dict[str, int] = {"detected": 0, "repeat": 0, "recovered": 0}
    canonical_reasons: dict[str, int] = {}
    disagreement_true = 0
    for run in runs:
        trace = run.get("trace") or []
        if not trace:
            continue
        name = str(trace[-1].get("scenario"))
        if name in scenarios:
            scenarios[name]["examples"] += 1
            if any(item.get("scenario_complete") for item in trace):
                scenarios[name]["completed"] += 1
        for item in trace:
            reason = str(item["actual"]["reason"])
            canonical_reasons[reason] = canonical_reasons.get(reason, 0) + 1
            if item["actual"].get("same_producer_api_disagreement") is True:
                disagreement_true += 1
            for transition in item["attribution"]["created_transitions"]:
                phase = transition["phase"]
                phases[phase] = phases.get(phase, 0) + 1
    return {
        "scenarios": scenarios,
        "transition_phases": phases,
        "canonical_reasons": canonical_reasons,
        "same_producer_api_disagreement_true": disagreement_true,
    }


def _artifact(
    *,
    seed: int,
    examples: int,
    max_steps: int,
    started: float,
    totals: dict[str, int],
    runs: list[dict[str, Any]],
    result: dict[str, Any],
    action_log_path: Path | None,
) -> dict[str, Any]:
    repo = Path(__file__).resolve().parents[1]
    implementation = {
        "common_runtime_sha256": _sha256(repo / "tests/stateful_runtime.py"),
        "frozen_v1_harness_sha256": _sha256(
            repo / "tests/stateful_youtube_lifecycle_authority_harness.py"
        ),
        "expanded_harness_sha256": _sha256(Path(__file__)),
    }
    artifact = {
        "artifact_schema": "monitoring_v4.youtube_lifecycle_expanded_stateful.v1",
        "source_identity": source_identity(repo),
        "implementation_identity": implementation,
        "slice": {
            "name": SLICE_NAME,
            "scope": [
                "adapter-realistic evidence roles",
                "authoritative unknown status",
                "diagnostic context/disagreement",
                "unknown candidate threshold",
                "incident detected/repeat/recovered",
                "restart/reopen candidate persistence",
            ],
            "excluded": [
                "youtube_public expansion: no current producer found",
                "policy/schema/API mutation",
                "real notification and production state",
            ],
            "sources": list(validate_adapter_policy_alignment().values()),
            "scenarios": list(SCENARIOS),
        },
        "oracle": {
            "revision": ORACLE_REVISION,
            "source_policy_revision": SOURCE_POLICY_REVISION,
            "reducer_revision": REDUCER_REVISION,
            "incident_policy_revision": INCIDENT_POLICY_REVISION,
            "unknown_gate": {"min_samples": 2, "min_duration_sec": 300},
            "repeat_sec": 600,
        },
        "bounds": {
            "seed": seed,
            "examples": examples,
            "max_steps": max_steps,
            "max_rows": MAX_ROWS,
            "max_artifact_bytes": MAX_ARTIFACT_BYTES,
            "wall_time_sec": DEFAULT_WALL_TIME_SEC,
        },
        "coverage": _coverage(runs),
        "totals": totals,
        "result": result,
        "runs": runs,
        "write_ahead_action_log": WriteAheadActionLog.metadata(action_log_path),
        "reproduction_command": (
            "PYTHONPATH=src python3 "
            "tests/stateful_youtube_lifecycle_expanded_harness.py "
            f"--seed {seed} --examples {examples} --max-steps {max_steps} "
            "--artifact OUTPUT_ARTIFACT.json"
        ),
        "duration_sec": round(time.monotonic() - started, 6),
    }
    require_artifact_size(artifact)
    return artifact


def explore(
    *,
    seed: int,
    examples: int,
    max_steps: int,
    wall_time_sec: int = DEFAULT_WALL_TIME_SEC,
    action_log_path: Path | None = None,
) -> dict[str, Any]:
    config = ExplorationConfig(seed, examples, max_steps, wall_time_sec)
    artifact = bounded_explore(
        config=config,
        machine_factory=lambda machine_seed, example: ExpandedYouTubeLifecycleStateMachine(
            seed=machine_seed,
            max_steps=max_steps,
            example_index=example,
            action_log_path=action_log_path,
        ),
        artifact_builder=lambda started, totals, runs, result, log_path: _artifact(
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
    if examples >= 50 and artifact["result"]["classification"] == "pass":
        missing = [
            name
            for name, counts in artifact["coverage"]["scenarios"].items()
            if counts["completed"] == 0
        ]
        required_phases = artifact["coverage"]["transition_phases"]
        if missing or any(required_phases.get(phase, 0) == 0 for phase in ("detected", "repeat", "recovered")):
            artifact["totals"]["harness_failures"] += 1
            artifact["result"] = {
                "classification": "stateful_harness_failure",
                "reason": f"expanded coverage incomplete scenarios={missing!r}",
            }
    require_artifact_size(artifact)
    return artifact


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--examples", required=True, type=int)
    parser.add_argument("--max-steps", required=True, type=int)
    parser.add_argument("--artifact", required=True, type=Path)
    args = parser.parse_args(argv)
    action_log_path = args.artifact.with_name(f"{args.artifact.stem}.actions.jsonl")
    artifact = explore(
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
