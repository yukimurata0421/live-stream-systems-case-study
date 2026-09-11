"""Test-only runtime mechanics shared by bounded domain stateful adapters.

This module owns execution and evidence plumbing only.  It deliberately has no
source, role, priority, freshness, canonical, incident, or recovery semantics.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol


MAX_ROWS = 128
MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
MAX_ACTION_LOG_BYTES = 8 * 1024 * 1024
DEFAULT_WALL_TIME_SEC = 600


class StatefulHarnessFailure(RuntimeError):
    pass


class TerminalEvidencePersistenceFailure(StatefulHarnessFailure):
    """A started action could not be given durable terminal evidence."""

    def __init__(self, message: str, *, evidence: dict[str, Any]) -> None:
        super().__init__(message)
        self.evidence = evidence


class ActionLogAppendFailure(StatefulHarnessFailure):
    """An append failed, with an explicit statement about partial persistence."""

    def __init__(self, message: str, *, event_persisted: bool | None) -> None:
        super().__init__(message)
        self.event_persisted = event_persisted


class StatefulAttributionGap(StatefulHarnessFailure):
    """A persisted effect cannot be attributed from pre/post identities."""


class StatefulOracleGap(StatefulHarnessFailure):
    """The domain policy does not independently determine an expectation."""


class StatefulUnknownAmbiguous(StatefulHarnessFailure):
    """Required input evidence is incomplete or ambiguous."""


class SUTInvariantViolation(AssertionError):
    pass


class WriteAheadActionLog:
    """Append-only, fsync-backed stateful input/output evidence."""

    def __init__(self, path: Path, *, max_bytes: int = MAX_ACTION_LOG_BYTES) -> None:
        self.path = path
        self.max_bytes = max_bytes
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.size = path.stat().st_size if path.exists() else 0

    def append(self, event: dict[str, Any]) -> None:
        encoded = (json.dumps(event, sort_keys=True) + "\n").encode("utf-8")
        if self.size + len(encoded) > self.max_bytes:
            raise StatefulHarnessFailure("write-ahead action log size bound exceeded")
        previous_size = self.size
        try:
            with self.path.open("ab") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException as exc:
            rollback_error: BaseException | None = None
            try:
                if self.path.exists():
                    with self.path.open("r+b") as handle:
                        handle.truncate(previous_size)
                        handle.flush()
                        os.fsync(handle.fileno())
                rollback_verified = (
                    (not self.path.exists() and previous_size == 0)
                    or (self.path.exists() and self.path.stat().st_size == previous_size)
                )
            except BaseException as cleanup_exc:
                rollback_error = cleanup_exc
                rollback_verified = False
            suffix = ""
            if rollback_error is not None:
                suffix = f"; rollback failed: {type(rollback_error).__name__}: {rollback_error}"
            raise ActionLogAppendFailure(
                f"action log append failed: {type(exc).__name__}: {exc}{suffix}",
                event_persisted=False if rollback_verified else None,
            ) from exc
        self.size += len(encoded)

    @staticmethod
    def metadata(path: Path | None, *, max_bytes: int = MAX_ACTION_LOG_BYTES) -> dict[str, Any] | None:
        if path is None or not path.exists():
            return None
        return {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size_bytes": path.stat().st_size,
            "max_size_bytes": max_bytes,
        }


def source_identity(root: Path) -> dict[str, str]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        check=True,
    ).stdout.strip()
    raw = subprocess.run(
        ["git", "diff", "--binary", "--"],
        cwd=root,
        stdout=subprocess.PIPE,
        check=True,
    ).stdout
    return {
        "git_head": head,
        "tracked_dirty_diff_sha256": hashlib.sha256(raw).hexdigest(),
    }


def require_artifact_size(artifact: dict[str, Any], *, max_bytes: int = MAX_ARTIFACT_BYTES) -> None:
    if len(json.dumps(artifact, sort_keys=True).encode()) > max_bytes:
        raise StatefulHarnessFailure("artifact size bound exceeded")


def verified_cleanup(
    target: Path,
    *,
    delete: Callable[[Path], None] = shutil.rmtree,
) -> dict[str, Any]:
    """Delete a temporary resource and prove its final absence."""
    existed_before = target.exists()
    delete_attempted = existed_before
    delete_exception: dict[str, str] | None = None
    if delete_attempted:
        try:
            delete(target)
        except BaseException as exc:
            delete_exception = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
    exists_after = target.exists()
    return {
        "cleanup_target": str(target),
        "existed_before": existed_before,
        "delete_attempted": delete_attempted,
        "delete_exception": delete_exception,
        "exists_after": exists_after,
        "cleanup_verified": delete_exception is None and not exists_after,
    }


def action_state_fields(state: dict[str, Any], *, prefix: str) -> dict[str, Any]:
    """Serialize only the common identity/canonical measurement contract."""
    return {
        f"{prefix}_current_snapshot_id": state["current_snapshot_id"],
        f"{prefix}_domain_current_snapshot_id": state["domain_current_snapshot_id"],
        f"{prefix}_transition_ids": state["transition_ids"],
        f"{prefix}_intent_ids": state["intent_ids"],
        f"{prefix}_episode_ids": state["episode_ids"],
        f"{prefix}_episode_state": state["episode_state"],
        f"{prefix}_last_bad_at": state["last_bad_at"],
        f"{prefix}_canonical": state["canonical"],
        f"{prefix}_transition_count": state["transition_count"],
        f"{prefix}_intent_count": state["intent_count"],
    }


def identity_attribution(pre: dict[str, Any], post: dict[str, Any]) -> dict[str, Any]:
    """Attribute effects only through pre/post set differences and references."""
    transition_ids = sorted(set(post["transition_ids"]) - set(pre["transition_ids"]))
    intent_ids = sorted(set(post["intent_ids"]) - set(pre["intent_ids"]))
    episode_ids = sorted(set(post["episode_ids"]) - set(pre["episode_ids"]))
    transitions = [post["transition_records"][ident] for ident in transition_ids]
    intents = [post["intent_records"][ident] for ident in intent_ids]
    errors: list[str] = []
    post_episode_ids = set(post["episode_ids"])
    created_transition_by_id = {item["transition_id"]: item for item in transitions}
    for transition in transitions:
        if transition["episode_id"] not in post_episode_ids:
            errors.append(f"transition_episode_not_post_state:{transition['transition_id']}")
        if transition["current_snapshot_id"] != post["domain_current_snapshot_id"]:
            errors.append(f"transition_snapshot_not_post_current:{transition['transition_id']}")
    for intent in intents:
        transition = created_transition_by_id.get(intent["transition_id"])
        if transition is None:
            errors.append(f"intent_transition_not_created_by_action:{intent['intent_id']}")
        elif transition["episode_id"] != intent["episode_id"]:
            errors.append(f"intent_episode_mismatch:{intent['intent_id']}")
    return {
        "created_transition_ids": transition_ids,
        "created_intent_ids": intent_ids,
        "created_episode_ids": episode_ids,
        "created_transitions": transitions,
        "created_intents": intents,
        "attribution_complete": not errors,
        "attribution_errors": errors,
    }


class StatefulActionRuntime:
    """Write-ahead action transaction with best-effort failure post-state."""

    def __init__(
        self,
        *,
        run_id: str,
        seed: int,
        example_index: int,
        capture_state: Callable[[], dict[str, Any]],
        action_log: WriteAheadActionLog,
    ) -> None:
        self.run_id = run_id
        self.seed = seed
        self.example_index = example_index
        self.capture_state = capture_state
        self.action_log = action_log
        self.action_index = 0
        self.events: list[dict[str, Any]] = []
        self.pre_state: dict[str, Any] | None = None
        self.terminal_persistence_failures: list[dict[str, Any]] = []

    def attribution(self, post: dict[str, Any]) -> dict[str, Any]:
        if self.pre_state is None:
            raise StatefulAttributionGap("no active pre-state for attribution")
        return identity_attribution(self.pre_state, post)

    def _start(
        self,
        *,
        action_type: str,
        logical_now: str,
        action_input: dict[str, Any],
    ) -> dict[str, Any]:
        state = self.capture_state()
        self.pre_state = state
        record = {
            "status": "ACTION_STARTED",
            "run_id": self.run_id,
            "seed": self.seed,
            "example_index": self.example_index,
            "action_index": self.action_index,
            "action_type": action_type,
            "logical_now": logical_now,
            **action_input,
            **action_state_fields(state, prefix="pre"),
            "repository_generation": state["repository_generation"],
        }
        self.action_log.append(record)
        self.events.append(record)
        self.action_index += 1
        return record

    def _completion_record(
        self,
        *,
        started: dict[str, Any],
        result: Any,
        trace: dict[str, Any] | None,
        state: dict[str, Any],
        attribution: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "status": "ACTION_COMPLETED",
            "run_id": self.run_id,
            "seed": self.seed,
            "example_index": self.example_index,
            "action_index": started["action_index"],
            "action_type": started["action_type"],
            "append_result": result if isinstance(result, bool) else None,
            **action_state_fields(state, prefix="post"),
            **attribution,
            "oracle_expected": None if trace is None else trace["expected"],
            "actual": None if trace is None else trace["actual"],
            "classification": "expected_behavior",
        }

    def _persist_terminal(self, record: dict[str, Any]) -> None:
        action_index = record["action_index"]
        if any(
            event["action_index"] == action_index
            and event["status"] in {"ACTION_COMPLETED", "ACTION_FAILED"}
            for event in self.events
        ):
            raise StatefulHarnessFailure(f"double terminal event refused for action {action_index}")
        self.action_log.append(record)
        self.events.append(record)

    @staticmethod
    def _classification(exc: BaseException) -> str:
        if isinstance(exc, StatefulAttributionGap):
            return "stateful_attribution_gap"
        if isinstance(exc, StatefulOracleGap):
            return "oracle_gap"
        if isinstance(exc, StatefulUnknownAmbiguous):
            return "unknown_ambiguous"
        if isinstance(exc, SUTInvariantViolation):
            return "sut_monitoring_failure"
        return "stateful_harness_failure"

    def _record_terminal_persistence_failure(
        self,
        *,
        started: dict[str, Any],
        failure_stage: str,
        exc: BaseException,
        terminal_status: str,
    ) -> TerminalEvidencePersistenceFailure:
        evidence = {
            "classification": "stateful_harness_failure",
            "failure_type": "terminal_evidence_persistence_failure",
            "failure_stage": failure_stage,
            "terminal_status": terminal_status,
            "run_id": self.run_id,
            "seed": self.seed,
            "example_index": self.example_index,
            "action_index": started["action_index"],
            "action_type": started["action_type"],
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
            "event_persisted": getattr(exc, "event_persisted", None),
        }
        self.terminal_persistence_failures.append(evidence)
        self.pre_state = None
        return TerminalEvidencePersistenceFailure(
            "terminal evidence persistence failure: "
            f"{terminal_status} at {failure_stage}: {type(exc).__name__}: {exc}",
            evidence=evidence,
        )

    def _fail(
        self,
        *,
        started: dict[str, Any],
        exc: BaseException,
        failure_stage: str,
        mutation_started: bool,
        mutation_completed: bool,
        post_state: dict[str, Any] | None,
        attribution: dict[str, Any] | None,
    ) -> None:
        collection_errors: list[str] = []
        state = post_state
        if state is None:
            try:
                state = self.capture_state()
            except BaseException as collection_exc:
                collection_errors.append(
                    f"post_state:{type(collection_exc).__name__}: {collection_exc}"
                )
        fields = {} if state is None else action_state_fields(state, prefix="post")
        attributed = attribution
        if attributed is None and state is not None:
            try:
                attributed = self.attribution(state)
            except BaseException as collection_exc:
                collection_errors.append(
                    f"attribution:{type(collection_exc).__name__}: {collection_exc}"
                )
        attribution_fields = {} if attributed is None else attributed
        record = {
            "status": "ACTION_FAILED",
            "run_id": self.run_id,
            "seed": self.seed,
            "example_index": self.example_index,
            "action_index": started["action_index"],
            "action_type": started["action_type"],
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
            "failure_stage": failure_stage,
            "mutation_started": mutation_started,
            "mutation_completed": mutation_completed,
            "post_state_available": state is not None,
            "attribution_available": attributed is not None,
            "best_effort_post_state": fields or None,
            "classification": self._classification(exc),
            **fields,
            **attribution_fields,
            "created_transition_ids": (
                None if attributed is None else attributed.get("created_transition_ids")
            ),
            "created_intent_ids": (
                None if attributed is None else attributed.get("created_intent_ids")
            ),
            "created_episode_ids": (
                None if attributed is None else attributed.get("created_episode_ids")
            ),
            "post_state_collection_errors": collection_errors,
        }
        try:
            self._persist_terminal(record)
        except BaseException as persistence_exc:
            raise self._record_terminal_persistence_failure(
                started=started,
                failure_stage=failure_stage,
                exc=persistence_exc,
                terminal_status="ACTION_FAILED",
            ) from persistence_exc
        self.pre_state = None

    def run(
        self,
        *,
        action_type: str,
        logical_now: str,
        action_input: dict[str, Any],
        mutation: Callable[[], Any],
        trace_provider: Callable[[], dict[str, Any] | None],
    ) -> Any:
        started = self._start(
            action_type=action_type,
            logical_now=logical_now,
            action_input=action_input,
        )
        failure_stage = "mutation"
        mutation_started = False
        mutation_completed = False
        post_state: dict[str, Any] | None = None
        attributed: dict[str, Any] | None = None
        try:
            mutation_started = True
            result = mutation()
            mutation_completed = True
            failure_stage = "post_state_capture"
            post_state = self.capture_state()
            failure_stage = "attribution"
            attributed = self.attribution(post_state)
            failure_stage = "completion_build"
            trace = trace_provider()
            record = self._completion_record(
                started=started,
                result=result,
                trace=trace,
                state=post_state,
                attribution=attributed,
            )
            failure_stage = "completion_persist"
            self._persist_terminal(record)
        except BaseException as exc:
            if (
                failure_stage == "completion_persist"
                and getattr(exc, "event_persisted", None) is not False
            ):
                raise self._record_terminal_persistence_failure(
                    started=started,
                    failure_stage=failure_stage,
                    exc=exc,
                    terminal_status="ACTION_COMPLETED",
                ) from exc
            self._fail(
                started=started,
                exc=exc,
                failure_stage=failure_stage,
                mutation_started=mutation_started,
                mutation_completed=mutation_completed,
                post_state=post_state,
                attribution=attributed,
            )
            raise
        self.pre_state = None
        return result

    def summary(self) -> dict[str, Any]:
        started = [event for event in self.events if event["status"] == "ACTION_STARTED"]
        completed = [event for event in self.events if event["status"] == "ACTION_COMPLETED"]
        failed = [event for event in self.events if event["status"] == "ACTION_FAILED"]
        terminal = completed + failed
        terminal_counts: dict[int, int] = {}
        for event in terminal:
            terminal_counts[event["action_index"]] = terminal_counts.get(event["action_index"], 0) + 1
        orphan = [
            event for event in started if terminal_counts.get(event["action_index"], 0) == 0
        ]
        double = [
            event for event in started if terminal_counts.get(event["action_index"], 0) > 1
        ]
        return {
            "started_actions": len(started),
            "completed_actions": len(completed),
            "failed_actions": len(failed),
            "terminal_actions": len(terminal),
            "orphan_started_actions": len(orphan),
            "double_terminal_actions": len(double),
            "terminal_evidence_persistence_failures": len(self.terminal_persistence_failures),
            "terminal_integrity_verified": not orphan and not double,
            "incomplete_actions": len(orphan),
            "last_started_action": started[-1] if started else None,
            "last_completed_action": completed[-1] if completed else None,
            "last_failed_action": failed[-1] if failed else None,
            "incomplete_action_candidates": orphan,
            "orphan_started_action_candidates": orphan,
            "double_terminal_action_candidates": double,
            "terminal_persistence_failure_evidence": self.terminal_persistence_failures,
        }


class DomainStatefulAdapter(Protocol):
    trace: list[dict[str, Any]]
    expected_effects: int

    def choose_and_apply(self, rng: random.Random) -> None: ...

    def action_trace_summary(self) -> dict[str, Any]: ...

    def close(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ExplorationConfig:
    seed: int
    examples: int
    max_steps: int
    wall_time_sec: int = DEFAULT_WALL_TIME_SEC

    def validate(self) -> None:
        if self.examples <= 0 or self.max_steps <= 0:
            raise StatefulHarnessFailure("examples and max_steps must be positive")
        if self.wall_time_sec <= 0:
            raise StatefulHarnessFailure("wall_time_sec must be positive")


ArtifactBuilder = Callable[
    [float, dict[str, int], list[dict[str, Any]], dict[str, Any], Path | None],
    dict[str, Any],
]
FailureContext = Callable[[DomainStatefulAdapter, BaseException], dict[str, Any]]


def bounded_explore(
    *,
    config: ExplorationConfig,
    machine_factory: Callable[[int, int], DomainStatefulAdapter],
    artifact_builder: ArtifactBuilder,
    action_log_path: Path | None,
    initial_totals: dict[str, int] | None = None,
    sut_failure_context: FailureContext | None = None,
) -> dict[str, Any]:
    """Run deterministic bounded examples and preserve failure/cleanup evidence."""
    config.validate()
    started = time.monotonic()
    rng = random.Random(config.seed)
    runs: list[dict[str, Any]] = []
    totals = {
        "action_count": 0,
        "expected_perturbation_effects": 0,
        "sut_failures": 0,
        "harness_failures": 0,
        "attribution_gaps": 0,
        "oracle_gaps": 0,
        "unknown": 0,
        "cleanup_failures": 0,
        "cleanup_verified": 0,
        "started_actions": 0,
        "completed_actions": 0,
        "failed_actions": 0,
        "terminal_actions": 0,
        "orphan_started_actions": 0,
        "double_terminal_actions": 0,
        "terminal_integrity_failures": 0,
        "terminal_evidence_persistence_failures": 0,
        **(initial_totals or {}),
    }
    failure_result: dict[str, Any] | None = None
    for example in range(config.examples):
        if time.monotonic() - started > config.wall_time_sec:
            totals["harness_failures"] += 1
            failure_result = {
                "classification": "stateful_harness_failure",
                "reason": "wall_time_limit",
            }
            break
        machine = machine_factory(config.seed * 100_000 + example, example)
        run_record: dict[str, Any] = {"example": example}
        try:
            steps = rng.randint(1, config.max_steps)
            for _ in range(steps):
                machine.choose_and_apply(rng)
            totals["action_count"] += steps
            totals["expected_perturbation_effects"] += machine.expected_effects
            run_record.update(
                {
                    "steps": steps,
                    "trace": machine.trace,
                    "action_trace": machine.action_trace_summary(),
                }
            )
        except StatefulAttributionGap as exc:
            totals["attribution_gaps"] += 1
            run_record.update({"trace": machine.trace, "action_trace": machine.action_trace_summary()})
            failure_result = {
                "classification": "stateful_attribution_gap",
                "reason": str(exc),
                "action_trace": machine.action_trace_summary(),
            }
        except StatefulOracleGap as exc:
            totals["oracle_gaps"] += 1
            run_record.update({"trace": machine.trace, "action_trace": machine.action_trace_summary()})
            failure_result = {
                "classification": "oracle_gap",
                "reason": str(exc),
                "action_trace": machine.action_trace_summary(),
            }
        except StatefulUnknownAmbiguous as exc:
            totals["unknown"] += 1
            run_record.update({"trace": machine.trace, "action_trace": machine.action_trace_summary()})
            failure_result = {
                "classification": "unknown_ambiguous",
                "reason": str(exc),
                "action_trace": machine.action_trace_summary(),
            }
        except SUTInvariantViolation as exc:
            totals["sut_failures"] += 1
            run_record.update({"trace": machine.trace, "action_trace": machine.action_trace_summary()})
            extra = {} if sut_failure_context is None else sut_failure_context(machine, exc)
            failure_result = {
                "classification": "sut_monitoring_failure",
                "reason": str(exc),
                "minimized_trace": machine.trace,
                "action_trace": machine.action_trace_summary(),
                **extra,
            }
        except TerminalEvidencePersistenceFailure as exc:
            totals["harness_failures"] += 1
            run_record.update({"trace": machine.trace, "action_trace": machine.action_trace_summary()})
            failure_result = {
                "classification": "stateful_harness_failure",
                "failure_type": "terminal_evidence_persistence_failure",
                "reason": str(exc),
                "terminal_persistence_evidence": exc.evidence,
                "action_trace": machine.action_trace_summary(),
            }
        except BaseException as exc:
            totals["harness_failures"] += 1
            run_record.update({"trace": machine.trace, "action_trace": machine.action_trace_summary()})
            failure_result = {
                "classification": "stateful_harness_failure",
                "reason": f"{type(exc).__name__}: {exc}",
                "action_trace": machine.action_trace_summary(),
            }
        finally:
            try:
                cleanup_evidence = machine.close()
                run_record["cleanup_evidence"] = cleanup_evidence
                if cleanup_evidence["cleanup_verified"]:
                    totals["cleanup_verified"] += 1
                    run_record["cleanup_result"] = "verified"
                else:
                    totals["cleanup_failures"] += 1
                    totals["harness_failures"] += 1
                    run_record["cleanup_result"] = "failed:cleanup_not_verified"
                    failure_result = {
                        "classification": "stateful_harness_failure",
                        "failure_type": "cleanup_verification_failure",
                        "reason": run_record["cleanup_result"],
                        "cleanup_evidence": cleanup_evidence,
                        "action_trace": machine.action_trace_summary(),
                    }
            except BaseException as cleanup_exc:
                totals["cleanup_failures"] += 1
                totals["harness_failures"] += 1
                run_record["cleanup_result"] = f"failed:{type(cleanup_exc).__name__}:{cleanup_exc}"
                failure_result = {
                    "classification": "stateful_harness_failure",
                    "reason": run_record["cleanup_result"],
                    "action_trace": machine.action_trace_summary(),
                }
            action_trace = machine.action_trace_summary()
            run_record["action_trace"] = action_trace
            for key in (
                "started_actions",
                "completed_actions",
                "failed_actions",
                "terminal_actions",
                "orphan_started_actions",
                "double_terminal_actions",
                "terminal_evidence_persistence_failures",
            ):
                totals[key] += int(action_trace.get(key, 0))
            integrity_errors: list[str] = []
            action_started = int(action_trace.get("started_actions", 0))
            action_completed = int(action_trace.get("completed_actions", 0))
            action_failed = int(action_trace.get("failed_actions", 0))
            action_terminal = int(action_trace.get("terminal_actions", 0))
            action_orphan = int(action_trace.get("orphan_started_actions", 0))
            action_double = int(action_trace.get("double_terminal_actions", 0))
            if action_started != action_completed + action_failed:
                integrity_errors.append("started_not_equal_completed_plus_failed")
            if action_terminal != action_completed + action_failed:
                integrity_errors.append("terminal_not_equal_completed_plus_failed")
            if action_orphan:
                integrity_errors.append("orphan_started_actions_nonzero")
            if action_double:
                integrity_errors.append("double_terminal_actions_nonzero")
            run_record["terminal_integrity"] = {
                "verified": not integrity_errors,
                "errors": integrity_errors,
            }
            if integrity_errors:
                totals["terminal_integrity_failures"] += 1
                if failure_result is None:
                    totals["harness_failures"] += 1
                    failure_result = {
                        "classification": "stateful_harness_failure",
                        "failure_type": "terminal_integrity_failure",
                        "reason": ",".join(integrity_errors),
                        "action_trace": action_trace,
                    }
            runs.append(run_record)
        if failure_result is not None:
            break
    result = failure_result or {"classification": "pass"}
    return artifact_builder(started, totals, runs, result, action_log_path)
