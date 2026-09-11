#!/usr/bin/env python3
"""Bounded stateful exploration for viewer_external supporting-current sources.

The current viewer_external policy is an explicit domain exception: both
configured sources use the ``supporting`` evidence role, while both retain
current authority and the domain allows only that role.  This independent
oracle covers that actual contract without inventing authoritative-role input.
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
        SUTInvariantViolation,
        StatefulActionRuntime,
        StatefulHarnessFailure,
        WriteAheadActionLog,
        bounded_explore,
        require_artifact_size,
        source_identity,
        verified_cleanup,
    )
except ModuleNotFoundError:  # Direct execution puts tests/ on sys.path.
    from stateful_runtime import (  # type: ignore[no-redef]
        DEFAULT_WALL_TIME_SEC,
        MAX_ARTIFACT_BYTES,
        MAX_ROWS,
        ExplorationConfig,
        SUTInvariantViolation,
        StatefulActionRuntime,
        StatefulHarnessFailure,
        WriteAheadActionLog,
        bounded_explore,
        require_artifact_size,
        source_identity,
        verified_cleanup,
    )


ORACLE_REVISION = "viewer-external-supporting-current-oracle-r1"
SYNTHETIC_ANCHOR_TS = unix_ts("2026-08-21T00:00:00Z")


@dataclass(frozen=True)
class SourceFact:
    key: str
    source: str
    role: str
    priority: int
    ttl_sec: int
    current_authority: bool = True


SOURCES = {
    "viewer": SourceFact("viewer", "viewer_synthetic", "supporting", 100, 900),
    "external": SourceFact("external", "external_blackbox", "supporting", 80, 900),
}


@dataclass(frozen=True)
class Evidence:
    source_key: str
    status: str
    observed_ts: int
    received_ts: int
    source_event_id: str
    provenance: str
    freshness_limit_sec: int = 900

    @property
    def fact(self) -> SourceFact:
        return SOURCES[self.source_key]

    @property
    def observation_id(self) -> str:
        return self.envelope().observation_id

    def envelope(self) -> ObservationEnvelope:
        return ObservationEnvelope.create(
            domain="viewer_external",
            source=self.fact.source,
            source_event_id=self.source_event_id,
            source_generation="synthetic-viewer-external-v1",
            evidence_role=self.fact.role,
            status=self.status,
            reason_code=f"{self.fact.source}_{self.status}",
            observed_at=utc_text(self.observed_ts),
            received_at=utc_text(self.received_ts),
            freshness_limit_sec=self.freshness_limit_sec,
            producer_revision="stateful-viewer-external-harness-r1",
            payload={"provenance": self.provenance},
        )


@dataclass(frozen=True)
class ExpectedCanonical:
    state: str
    reason: str
    observed_ts: int
    valid_until_ts: int
    selected_sources: tuple[str, ...]


class ViewerExternalOracle:
    """Independent selection oracle for the audited supporting-current pair."""

    def canonical(self, evidence: list[Evidence], *, now_ts: int) -> ExpectedCanonical:
        latest: dict[str, Evidence] = {}
        for item in evidence:
            previous = latest.get(item.fact.source)
            identity = (item.observed_ts, item.received_ts, item.observation_id)
            previous_identity = None if previous is None else (
                previous.observed_ts,
                previous.received_ts,
                previous.observation_id,
            )
            if previous_identity is None or identity > previous_identity:
                latest[item.fact.source] = item
        eligible = [
            item
            for item in latest.values()
            if item.fact.current_authority
            and item.fact.role == "supporting"
            and item.observed_ts <= now_ts
            and now_ts - item.observed_ts <= min(item.fact.ttl_sec, item.freshness_limit_sec)
        ]
        if not eligible:
            return ExpectedCanonical("unknown", "missing_current_evidence", now_ts, now_ts, ())
        priority = max(item.fact.priority for item in eligible)
        selected = [item for item in eligible if item.fact.priority == priority]
        statuses = {item.status for item in selected}
        if "bad" in statuses and "good" in statuses:
            state, reason = "unknown", "source_disagreement"
        elif "bad" in statuses:
            state, reason = "bad", "current_bad_evidence"
        elif "good" in statuses:
            state, reason = "good", "current_good_evidence"
        else:
            state, reason = "unknown", "current_evidence_unknown"
        return ExpectedCanonical(
            state,
            reason,
            max(item.observed_ts for item in selected),
            min(
                item.observed_ts + min(item.fact.ttl_sec, item.freshness_limit_sec)
                for item in selected
            ),
            tuple(sorted(item.fact.source for item in selected)),
        )


class ViewerExternalIncidentOracle:
    """Small independent incident model for the included viewer_external scope."""

    def __init__(self) -> None:
        self.processed_snapshots: set[str] = set()
        self.active = False
        self.candidate: dict[str, Any] | None = None
        self.next_due_ts: int | None = None

    @staticmethod
    def _threshold(state: str) -> tuple[int, int]:
        if state == "bad":
            return 1, 0
        if state == "unknown":
            return 2, 300
        raise StatefulHarnessFailure(f"incident oracle received non-actionable state: {state}")

    @staticmethod
    def _repeat(state: str) -> int:
        return 300 if state == "bad" else 900

    def transition_phase(
        self,
        canonical: ExpectedCanonical,
        *,
        snapshot_id: str,
        now_ts: int,
    ) -> str | None:
        if snapshot_id in self.processed_snapshots:
            return None
        self.processed_snapshots.add(snapshot_id)
        if canonical.state == "good":
            self.candidate = None
            if not self.active:
                return None
            self.active = False
            self.next_due_ts = None
            return "recovered"
        if not self.active:
            key = (canonical.state, canonical.reason)
            if self.candidate is None or self.candidate["key"] != key:
                self.candidate = {
                    "key": key,
                    "first_seen_ts": canonical.observed_ts,
                    "samples": 1,
                }
            else:
                self.candidate["samples"] += 1
            min_samples, min_duration = self._threshold(canonical.state)
            elapsed = max(0, now_ts - int(self.candidate["first_seen_ts"]))
            if self.candidate["samples"] < min_samples or elapsed < min_duration:
                return None
            self.active = True
            self.candidate = None
            self.next_due_ts = now_ts + self._repeat(canonical.state)
            return "detected"
        repeat = self._repeat(canonical.state)
        if self.next_due_ts is None:
            raise StatefulHarnessFailure("active incident oracle has no repeat deadline")
        self.next_due_ts = min(self.next_due_ts, now_ts + repeat)
        if now_ts < self.next_due_ts:
            return None
        while self.next_due_ts <= now_ts:
            self.next_due_ts += repeat
        return "repeat"


def validate_oracle_policy_alignment() -> dict[str, Any]:
    policy = DEFAULT_POLICIES["viewer_external"]
    if tuple(policy.allowed_roles) != ("supporting",):
        raise StatefulHarnessFailure("viewer_external allowed-role contract drifted")
    for fact in SOURCES.values():
        rule = policy.rule(fact.source)
        if rule is None:
            raise StatefulHarnessFailure(f"viewer_external source disappeared: {fact.source}")
        actual = (tuple(rule.roles), rule.priority, rule.ttl_sec, rule.current_authority)
        expected = ((fact.role,), fact.priority, fact.ttl_sec, fact.current_authority)
        if actual != expected:
            raise StatefulHarnessFailure(f"viewer_external policy fact drifted: {fact.source}")
    return {
        "domain": policy.domain,
        "allowed_roles": list(policy.allowed_roles),
        "sources": [fact.source for fact in SOURCES.values()],
    }


def _current_public(item: Any) -> dict[str, Any]:
    selected = item.payload.get("selected", [])
    return {
        "state": item.state,
        "reason": item.reason_codes[0] if item.reason_codes else "",
        "observed_ts": unix_ts(item.observed_at),
        "valid_until_ts": unix_ts(item.valid_until),
        "selected_sources": tuple(sorted(raw["source"] for raw in selected)),
    }


class ViewerExternalStateMachine:
    def __init__(
        self,
        *,
        seed: int,
        max_steps: int,
        example_index: int = 0,
        action_log_path: Path | None = None,
    ) -> None:
        self.policy_identity = validate_oracle_policy_alignment()
        self.seed = seed
        self.max_steps = max_steps
        self.example_index = example_index
        self.run_id = f"viewer-external-{seed}-{example_index}"
        self.root = Path(tempfile.mkdtemp(prefix="stream-v4-stateful-viewer-external-"))
        self.database = self.root / "viewer_external.sqlite3"
        self.repository = MonitoringRepository(self.database)
        self.repository.initialize(applied_at=utc_text(SYNTHETIC_ANCHOR_TS))
        self.oracle = ViewerExternalOracle()
        self.incident_oracle = ViewerExternalIncidentOracle()
        self.now_ts = SYNTHETIC_ANCHOR_TS
        self.evidence: list[Evidence] = []
        self.trace: list[dict[str, Any]] = []
        self._next_identity = 0
        self._last_semantic: tuple[Any, ...] | None = None
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
            "viewer",
            "good",
            self.now_ts,
            self.now_ts,
            "synthetic-viewer-good",
            "synthetic_initialization",
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
                          closed_at, last_transition_at, bad_samples, unknown_samples,
                          next_notification_at
                   FROM incident_episodes
                   WHERE domain='viewer_external'
                   ORDER BY opened_ts, episode_id"""
            ).fetchall()
        return [dict(row) for row in rows]

    def _measurement_state(self) -> dict[str, Any]:
        current = self.repository.current("viewer_external")
        transitions = self.repository.transitions(domain="viewer_external")
        intents = self.repository.intents()
        episodes = self._episode_state()
        active = [episode for episode in episodes if episode["status"] == "active"]
        return {
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
            "last_bad_at": active[0]["last_bad_at"] if active else None,
            "canonical": None if current is None else _current_public(current),
            "transition_count": len(transitions),
            "intent_count": len(intents),
            "repository_generation": self.repository_generation,
        }

    @staticmethod
    def _action_input(item: Evidence | None) -> dict[str, Any]:
        if item is None:
            return {
                "evidence_classification": "synthetic",
                "observation_identity": None,
                "source": None,
                "input_status": None,
                "input_observed_at": None,
                "input_received_at": None,
            }
        return {
            "evidence_classification": item.provenance,
            "observation_identity": item.observation_id,
            "source": item.fact.source,
            "input_status": item.status,
            "input_observed_at": utc_text(item.observed_ts),
            "input_received_at": utc_text(item.received_ts),
        }

    def _run_action(
        self,
        action_type: str,
        item: Evidence | None,
        mutation: Any,
    ) -> Any:
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
            ("viewer_external",),
            now_ts=self.now_ts,
        )[0]
        expected = self.oracle.canonical(self.evidence, now_ts=self.now_ts)
        required = {
            "state": expected.state,
            "reason": expected.reason,
            "observed_ts": expected.observed_ts,
            "valid_until_ts": expected.valid_until_ts,
            "selected_sources": expected.selected_sources,
        }
        observed = _current_public(current)
        if observed != required:
            raise SUTInvariantViolation(
                f"viewer_external oracle mismatch expected={required!r} actual={observed!r}"
            )
        expected_phase = self.incident_oracle.transition_phase(
            expected,
            snapshot_id=current.snapshot_id,
            now_ts=self.now_ts,
        )
        process = IncidentService(
            self.repository,
            DEFAULT_INCIDENT_POLICIES,
            RoutePolicy(),
        ).process(current, now_at=utc_text(self.now_ts))
        actual_phase = None if process.transition is None else process.transition.phase
        if actual_phase != expected_phase:
            raise SUTInvariantViolation(
                "viewer_external incident oracle mismatch "
                f"expected_phase={expected_phase!r} actual_phase={actual_phase!r}"
            )
        semantic = (
            required["state"],
            required["reason"],
            required["selected_sources"],
            expected_phase,
        )
        if semantic != self._last_semantic:
            self.expected_effects += 1
        self._last_semantic = semantic
        self.trace.append(
            {
                "action": action,
                "now_at": utc_text(self.now_ts),
                "accepted": accepted,
                "input_source": None if item is None else item.fact.source,
                "input_status": None if item is None else item.status,
                "expected": {**required, "incident_phase": expected_phase},
                "actual": {**observed, "incident_phase": actual_phase},
            }
        )
        if len(self.trace) > self.max_steps + 1:
            raise StatefulHarnessFailure("action trace bound exceeded")
        with self.repository.connection(read_only=True) as connection:
            observation_rows = int(
                connection.execute("SELECT count(*) FROM observations").fetchone()[0]
            )
        if observation_rows > MAX_ROWS:
            raise StatefulHarnessFailure("SQLite row bound exceeded")

    def ingest(self, source_key: str, status: str) -> None:
        item = Evidence(
            source_key,
            status,
            self.now_ts,
            self.now_ts,
            self._identity(f"{source_key}-{status}"),
            "synthetic",
        )
        self._run_action("ingest", item, lambda: self._append(item, action="ingest"))

    def replay_old(self, source_key: str) -> None:
        candidates = [item for item in self.evidence if item.source_key == source_key]
        if not candidates:
            self.ingest(source_key, "good")
            return
        previous = min(candidates, key=lambda item: (item.observed_ts, item.received_ts))
        replay = Evidence(
            source_key,
            previous.status,
            previous.observed_ts - 1,
            self.now_ts,
            self._identity(f"old-{source_key}"),
            "synthetic_old_replay",
        )
        self._run_action("replay_old", replay, lambda: self._append(replay, action="replay_old"))

    def duplicate_last(self) -> None:
        item = self.evidence[-1]
        self._run_action("duplicate_last", item, lambda: self._append(item, action="duplicate_last"))

    def delay_receive(self) -> None:
        previous = self.evidence[-1]
        delayed = Evidence(
            previous.source_key,
            previous.status,
            previous.observed_ts,
            self.now_ts + 1,
            self._identity("delayed"),
            "synthetic_delayed_receipt",
        )
        self._run_action("delay_receive", delayed, lambda: self._append(delayed, action="delay_receive"))

    def advance_clock(self, delta: int) -> None:
        def mutation() -> None:
            self.now_ts += delta
            self._sync(action="advance_clock")

        self._run_action("advance_clock", None, mutation)

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
        rng.choice(
            (
                lambda: self.ingest("viewer", rng.choice(("good", "bad"))),
                lambda: self.ingest("external", rng.choice(("good", "bad"))),
                lambda: self.replay_old("viewer"),
                lambda: self.replay_old("external"),
                self.duplicate_last,
                self.delay_receive,
                lambda: self.advance_clock(rng.choice((1, 299, 300, 899, 900, 901))),
                self.restart_component,
                self.reopen_repository,
            )
        )()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(
    seed: int,
    examples: int,
    max_steps: int,
    started: float,
    totals: dict[str, int],
    runs: list[dict[str, Any]],
    result: dict[str, Any],
    action_log_path: Path | None,
) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    artifact = {
        "artifact_schema": "monitoring_v4.viewer_external_stateful_supporting_current.v1",
        "source_identity": source_identity(project_root),
        "implementation_identity": {
            "common_runtime_sha256": _file_sha256(Path(__file__).with_name("stateful_runtime.py")),
            "harness_and_oracle_sha256": _file_sha256(Path(__file__)),
        },
        "oracle": {
            "revision": ORACLE_REVISION,
            "source_policy_revision": SOURCE_POLICY_REVISION,
            "reducer_revision": REDUCER_REVISION,
            "incident_policy_revision": INCIDENT_POLICY_REVISION,
            "scope": [fact.source for fact in SOURCES.values()],
            "role": "supporting_current_domain_exception",
            "explicit_source_unknown_status": "out_of_scope",
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
            "PYTHONPATH=src python3 tests/stateful_viewer_external_authority_harness.py "
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
    return bounded_explore(
        config=config,
        machine_factory=lambda machine_seed, example: ViewerExternalStateMachine(
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
            log_path,
        ),
        action_log_path=action_log_path,
    )


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
        json.dumps(artifact, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0 if artifact["result"]["classification"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
