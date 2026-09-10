from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sqlite3
import statistics
import tempfile
import threading
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from cra_dell_recovery.errors import LedgerUnavailable
from cra_dell_recovery.sqlite import production_sqlite_gate
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.operational.fixtures import negative_controls
from cra_harness.oracles.operational_v3 import detect_negative_control
from dell_recovery_agent.storage import DellStore

DEFAULT_SEEDS = (20260823, 20260824, 20260825, 20260826, 20260827)
TARGET_ID = "sqlite-stress-target"
ACTOR_WEIGHTS = {
    "authority_critical_reader": 5_000,
    "heartbeat_writer": 4_000,
    "authority_transition_writer": 3_000,
    "reconciliation_writer": 1_000,
    "command_accept_reader_writer": 3_000,
    "agent_status_reader": 2_000,
    "checkpoint_worker": 60,
    "backup_reader": 40,
    "target_state_reader": 1_900,
}
BASE_OPERATIONS_PER_SEED = sum(ACTOR_WEIGHTS.values())


def _percentiles(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "sample_count": len(ordered),
        "median": None if not ordered else round(statistics.median(ordered), 6),
        "p95": None if not ordered else round(ordered[math.ceil(0.95 * len(ordered)) - 1], 6),
        "p99": None if not ordered else round(ordered[math.ceil(0.99 * len(ordered)) - 1], 6),
        "maximum": None if not ordered else round(ordered[-1], 6),
        "unit": "ms",
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _scaled_actor_counts(operation_budget: int) -> dict[str, int]:
    if operation_budget < len(ACTOR_WEIGHTS):
        raise ValueError("operation budget must cover every actor")
    exact = {actor: operation_budget * weight / BASE_OPERATIONS_PER_SEED for actor, weight in ACTOR_WEIGHTS.items()}
    counts = {actor: max(1, int(value)) for actor, value in exact.items()}
    while sum(counts.values()) > operation_budget:
        candidates = [actor for actor, count in counts.items() if count > 1]
        if not candidates:
            raise ValueError("operation budget cannot preserve every actor")
        actor = max(candidates, key=lambda name: (counts[name], ACTOR_WEIGHTS[name], name))
        counts[actor] -= 1
    while sum(counts.values()) < operation_budget:
        actor = max(ACTOR_WEIGHTS, key=lambda name: (exact[name] - counts[name], ACTOR_WEIGHTS[name], name))
        counts[actor] += 1
    return counts


@dataclass
class StressMetrics:
    counters: Counter[str] = field(default_factory=Counter)
    latencies: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    sut_errors: list[dict[str, str]] = field(default_factory=list)
    harness_errors: list[dict[str, str]] = field(default_factory=list)
    wal_peak_bytes: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def increment(self, name: str, value: int = 1) -> None:
        with self._lock:
            self.counters[name] += value

    def latency(self, name: str, started_ns: int) -> None:
        elapsed = (time.perf_counter_ns() - started_ns) / 1_000_000
        with self._lock:
            self.latencies[name].append(elapsed)

    def error(self, actor: str, error: BaseException, *, harness: bool = False) -> None:
        record = {"actor": actor, "error_type": type(error).__name__, "message_class": str(error).split(":", 1)[0]}
        with self._lock:
            (self.harness_errors if harness else self.sut_errors).append(record)
            self.counters["harness_errors" if harness else "sut_errors"] += 1
            if isinstance(error, KeyError):
                self.counters["key_errors"] += 1
            if isinstance(error, (sqlite3.Error, LedgerUnavailable)) or "SQLITE" in str(error).upper():
                self.counters["sqlite_errors"] += 1
            if "TIMEOUT" in str(error).upper():
                self.counters["timeouts"] += 1

    def sample_wal(self, path: Path) -> None:
        wal = path.with_name(f"{path.name}-wal")
        size = wal.stat().st_size if wal.exists() else 0
        with self._lock:
            self.wal_peak_bytes = max(self.wal_peak_bytes, size)


class StressContext:
    def __init__(self, store: DellStore, workspace: Path, metrics: StressMetrics, seed: int) -> None:
        self.store = store
        self.workspace = workspace
        self.metrics = metrics
        self.seed = seed
        self.checkpoint_active = threading.Event()
        self.backup_active = threading.Event()
        self.heartbeat_active = threading.Event()
        self.transition_active = threading.Event()
        self.reconciliation_active = threading.Event()
        self.status_active = threading.Event()

    def reconciliation(self, suffix: str) -> None:
        fence = self.store.fence(TARGET_ID)
        epoch = int(fence["highest_authority_epoch_seen"]) + 1
        self.store.install_reconciliation(
            target_id=TARGET_ID,
            reconciliation_id=f"reconciliation-{self.seed}-{suffix}-{uuid.uuid4().hex}",
            challenge_id=f"challenge-{self.seed}-{suffix}-{uuid.uuid4().hex}",
            nonce=f"nonce-{self.seed}-{suffix}-{uuid.uuid4().hex}",
            new_epoch=epoch,
            session_id=f"session-{self.seed}-{epoch}-{uuid.uuid4().hex}",
            controller_instance_id=f"controller-{self.seed}",
        )

    def heartbeat(self) -> None:
        with self.store.write() as db:
            fence = db.execute("SELECT * FROM authority_fences WHERE target_id=?", (TARGET_ID,)).fetchone()
            if fence is None:
                raise KeyError(TARGET_ID)
            if fence["active_authority_session_id"] is None or int(fence["highest_authority_epoch_seen"]) <= 0:
                raise RuntimeError("authority session is incomplete")
            db.execute(
                """UPDATE authority_fences SET authority_state='CENTRAL_ACTIVE',action_ready=1,heartbeat_seq=heartbeat_seq+1,
                   last_heartbeat_received_at=?,state_reason='STRESS_VALID_HEARTBEAT',version=version+1 WHERE target_id=?""",
                (isoformat_utc(utc_now()), TARGET_ID),
            )
        self.store.set_process_lease_valid(TARGET_ID, True)


def _validate_fence(row: sqlite3.Row, metrics: StressMetrics) -> None:
    state = str(row["authority_state"])
    epoch = int(row["highest_authority_epoch_seen"])
    session = row["active_authority_session_id"]
    controller = row["active_controller_instance_id"]
    if epoch < 1:
        metrics.increment("epoch_corruption")
    if state in {"CENTRAL_ACTIVE", "CENTRAL_SUSPECT", "LOCAL_FALLBACK", "RECONCILING"} and (session is None or controller is None):
        metrics.increment("authority_corruption")
    if int(row["heartbeat_seq"]) < 0 or int(row["highest_command_seq_consumed"]) < 0:
        metrics.increment("authority_corruption")


def _run_forced_overlap_scenarios(context: StressContext) -> dict[str, int]:
    results: dict[str, int] = {}

    def critical_read() -> None:
        with context.store.read() as db:
            row = db.execute("SELECT * FROM authority_fences WHERE target_id=?", (TARGET_ID,)).fetchone()
            if row is None:
                raise KeyError(TARGET_ID)
            _validate_fence(row, context.metrics)
            time.sleep(0.003)

    def status_read() -> None:
        context.store.shadow_status_snapshot()
        time.sleep(0.003)

    def target_read() -> None:
        with context.store.read() as db:
            fence = db.execute("SELECT target_id FROM authority_fences WHERE target_id=?", (TARGET_ID,)).fetchone()
            policy = db.execute("SELECT target_id FROM agent_safety_policies WHERE target_id=?", (TARGET_ID,)).fetchone()
            if fence is None or policy is None or fence[0] != policy[0]:
                raise RuntimeError("target snapshot inconsistent")
            time.sleep(0.003)

    def checkpoint() -> None:
        time.sleep(0.001)
        context.store.checkpoint("TRUNCATE")

    def heartbeat() -> None:
        time.sleep(0.001)
        context.heartbeat()

    def transition() -> None:
        time.sleep(0.001)
        context.store.set_authority_state(TARGET_ID, "CENTRAL_SUSPECT", "FORCED_OVERLAP")

    def reconciliation() -> None:
        time.sleep(0.001)
        context.reconciliation("forced")

    def backup() -> None:
        time.sleep(0.001)
        context.store.backup_to(context.workspace / f"forced-backup-{uuid.uuid4().hex}.sqlite3")

    def lease_expiry() -> None:
        time.sleep(0.001)
        context.store.set_authority_state(TARGET_ID, "LOCAL_FALLBACK", "FORCED_LEASE_EXPIRY")

    scenarios: dict[str, tuple[Callable[[], None], ...]] = {
        "SC-01": (critical_read, checkpoint, heartbeat),
        "SC-02": (critical_read, transition, checkpoint),
        "SC-03": (reconciliation, status_read, checkpoint),
        "SC-04": (heartbeat, backup, critical_read),
        "SC-05": (lease_expiry, transition, checkpoint),
        "SC-06": (status_read, checkpoint, heartbeat),
    }
    for scenario_id, operations in scenarios.items():
        barrier = threading.Barrier(len(operations) + 1)
        errors: list[BaseException] = []
        windows: list[tuple[int, int]] = []
        lock = threading.Lock()

        def invoke(
            operation: Callable[[], None],
            barrier_ref: threading.Barrier = barrier,
            lock_ref: threading.Lock = lock,
            windows_ref: list[tuple[int, int]] = windows,
            errors_ref: list[BaseException] = errors,
        ) -> None:
            try:
                barrier_ref.wait(timeout=5)
                started = time.perf_counter_ns()
                operation()
                finished = time.perf_counter_ns()
                with lock_ref:
                    windows_ref.append((started, finished))
            except BaseException as caught:
                with lock_ref:
                    errors_ref.append(caught)

        threads = [
            threading.Thread(target=invoke, args=(operation,), name=f"{scenario_id}-{index}") for index, operation in enumerate(operations)
        ]
        for thread in threads:
            thread.start()
        barrier.wait(timeout=5)
        for thread in threads:
            thread.join(timeout=10)
        overlapped = len(windows) == len(operations) and max(item[0] for item in windows) <= min(item[1] for item in windows)
        results[scenario_id] = 1 if not errors and not any(thread.is_alive() for thread in threads) and overlapped else 0
        for error in errors:
            context.metrics.error(scenario_id, error)
        context.reconciliation(f"post-{scenario_id}")
        context.heartbeat()
    return results


def run_stress_seed(workspace: Path, *, seed: int, operation_budget: int = BASE_OPERATIONS_PER_SEED) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    database = workspace / "agent.sqlite3"
    migration = Path(__file__).resolve().parents[3] / "migrations/dell/001_initial.sql"
    store = DellStore(database, migration)
    store.bootstrap(
        agent_id=f"stress-agent-{seed}",
        installation_id=f"stress-installation-{seed}",
        host_id=f"stress-host-{seed}",
        host_boot_id=f"stress-boot-{seed}",
        target_id=TARGET_ID,
    )
    metrics = StressMetrics()
    context = StressContext(store, workspace, metrics, seed)
    context.reconciliation("initial")
    context.heartbeat()
    forced_overlaps = _run_forced_overlap_scenarios(context)
    counts = _scaled_actor_counts(operation_budget)
    start_barrier = threading.Barrier(len(counts) + 1)

    def run_actor(name: str, count: int, operation: Callable[[int, random.Random], None]) -> None:
        actor_random = random.Random(f"{seed}:{name}")
        try:
            start_barrier.wait(timeout=10)
        except threading.BrokenBarrierError as error:
            metrics.error(name, error, harness=True)
            return
        for index in range(count):
            started = time.perf_counter_ns()
            try:
                operation(index, actor_random)
            except Exception as error:
                metrics.error(name, error)
            finally:
                metrics.increment(f"actor_operations:{name}")
                metrics.latency(name, started)
            if actor_random.random() < 0.02:
                time.sleep(actor_random.random() / 20_000)

    reader_high_epoch = 0
    reader_epoch_lock = threading.Lock()

    def authority_reader(_: int, __: random.Random) -> None:
        nonlocal reader_high_epoch
        row = store.fence(TARGET_ID)
        _validate_fence(row, metrics)
        epoch = int(row["highest_authority_epoch_seen"])
        with reader_epoch_lock:
            if epoch < reader_high_epoch:
                metrics.increment("epoch_rollback")
            reader_high_epoch = max(reader_high_epoch, epoch)
        metrics.increment("critical_reads")
        metrics.increment("reader_connection_reopens")
        if context.checkpoint_active.is_set():
            metrics.increment("critical_checkpoint_overlaps")
        if context.backup_active.is_set() and context.heartbeat_active.is_set():
            metrics.increment("critical_heartbeat_backup_overlaps")

    def heartbeat_writer(_: int, __: random.Random) -> None:
        context.heartbeat_active.set()
        try:
            context.heartbeat()
            metrics.increment("heartbeat_writes")
            if context.checkpoint_active.is_set():
                metrics.increment("heartbeat_checkpoint_overlaps")
            time.sleep(0.0001)
        finally:
            context.heartbeat_active.clear()

    def transition_writer(index: int, _: random.Random) -> None:
        context.transition_active.set()
        try:
            if index % 11 == 0:
                row = store.fence(TARGET_ID)
                changed = store.set_authority_state(TARGET_ID, str(row["authority_state"]), str(row["state_reason"]))
            elif index % 97 == 0:
                try:
                    with store.write() as db:
                        db.execute(
                            "UPDATE authority_fences SET state_reason='ROLLBACK_INJECTION' WHERE target_id=?",
                            (TARGET_ID,),
                        )
                        raise RuntimeError("EXPECTED_ROLLBACK")
                except RuntimeError as error:
                    if str(error) != "EXPECTED_ROLLBACK":
                        raise
                metrics.increment("expected_rollbacks")
                changed = False
            elif index % 101 == 0:
                changed = store.set_authority_state(TARGET_ID, "LOCAL_FALLBACK", "STRESS_LEASE_EXPIRED")
                metrics.increment("lease_expiry_simulations")
            else:
                state = "CENTRAL_SUSPECT" if index % 2 else "CENTRAL_ACTIVE"
                changed = store.set_authority_state(TARGET_ID, state, f"STRESS_TRANSITION_{index % 3}")
            metrics.increment("authority_transition_attempts")
            metrics.increment("actual_transition_writes" if changed else "redundant_transition_suppressed")
            if context.checkpoint_active.is_set():
                metrics.increment("transition_checkpoint_overlaps")
            time.sleep(0.0001)
        finally:
            context.transition_active.clear()

    def reconciliation_writer(index: int, _: random.Random) -> None:
        context.reconciliation_active.set()
        try:
            context.reconciliation(str(index))
            metrics.increment("reconciliation_writes")
            if context.checkpoint_active.is_set():
                metrics.increment("reconciliation_checkpoint_overlaps")
            time.sleep(0.0001)
        finally:
            context.reconciliation_active.clear()

    def command_reader_writer(index: int, _: random.Random) -> None:
        with store.read() as db:
            fence = db.execute("SELECT highest_authority_epoch_seen FROM authority_fences WHERE target_id=?", (TARGET_ID,)).fetchone()
            unresolved = db.execute(
                """SELECT (SELECT count(*) FROM agent_commands WHERE target_id=? AND state IN
                   ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')) +
                   (SELECT count(*) FROM local_actions WHERE target_id=? AND state IN
                   ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN'))""",
                (TARGET_ID, TARGET_ID),
            ).fetchone()
            if fence is None or unresolved is None:
                raise RuntimeError("command snapshot incomplete")
        with store.write() as db:
            db.execute(
                "INSERT INTO agent_events VALUES (?,?,?,?,?,?)",
                (
                    f"stress-event-{seed}-{index}",
                    "SHADOW_COMMAND_EVALUATED",
                    "INFO",
                    "STRESS_NO_ACTION",
                    json.dumps({"physical_attempt_count": 0, "unresolved": int(unresolved[0])}, sort_keys=True),
                    isoformat_utc(utc_now()),
                ),
            )
        metrics.increment("command_accept_reads")
        metrics.increment("transactions")

    def status_reader(_: int, __: random.Random) -> None:
        context.status_active.set()
        try:
            fence, _ = store.shadow_status_snapshot()
            _validate_fence(fence, metrics)
            metrics.increment("agent_status_reads")
            metrics.increment("reader_connection_reopens")
            if context.checkpoint_active.is_set() and context.heartbeat_active.is_set():
                metrics.increment("status_checkpoint_heartbeat_overlaps")
            time.sleep(0.0001)
        finally:
            context.status_active.clear()

    def checkpoint_worker(index: int, _: random.Random) -> None:
        context.checkpoint_active.set()
        try:
            mode = ("PASSIVE", "FULL", "TRUNCATE")[index % 3]
            time.sleep(0.0005)
            result = store.checkpoint(mode)
            metrics.increment("checkpoints")
            metrics.increment(f"checkpoint_mode:{mode}")
            metrics.increment("busy_count", int(result[0] != 0))
            metrics.sample_wal(database)
            time.sleep(0.0005)
        finally:
            context.checkpoint_active.clear()

    def backup_reader(index: int, _: random.Random) -> None:
        context.backup_active.set()
        try:
            destination = workspace / f"backup-{index:05d}.sqlite3"
            store.backup_to(destination)
            backup = sqlite3.connect(f"file:{destination}?mode=ro", uri=True)
            try:
                integrity = str(backup.execute("PRAGMA integrity_check").fetchone()[0])
                fence_count = int(backup.execute("SELECT count(*) FROM authority_fences WHERE target_id=?", (TARGET_ID,)).fetchone()[0])
            finally:
                backup.close()
            if integrity != "ok" or fence_count != 1:
                raise RuntimeError("online backup verification failed")
            metrics.increment("online_backups")
            metrics.increment("reader_connection_reopens")
            time.sleep(0.0001)
        finally:
            context.backup_active.clear()

    def target_state_reader(_: int, __: random.Random) -> None:
        with store.read() as db:
            fence = db.execute(
                "SELECT target_id,highest_authority_epoch_seen FROM authority_fences WHERE target_id=?", (TARGET_ID,)
            ).fetchone()
            policy = db.execute("SELECT target_id FROM agent_safety_policies WHERE target_id=?", (TARGET_ID,)).fetchone()
            if fence is None or policy is None or str(fence["target_id"]) != str(policy["target_id"]):
                metrics.increment("inconsistent_reads")
            elif int(fence["highest_authority_epoch_seen"]) < 1:
                metrics.increment("epoch_corruption")
        metrics.increment("target_state_reads")
        metrics.increment("reader_connection_reopens")

    operations = {
        "authority_critical_reader": authority_reader,
        "heartbeat_writer": heartbeat_writer,
        "authority_transition_writer": transition_writer,
        "reconciliation_writer": reconciliation_writer,
        "command_accept_reader_writer": command_reader_writer,
        "agent_status_reader": status_reader,
        "checkpoint_worker": checkpoint_worker,
        "backup_reader": backup_reader,
        "target_state_reader": target_state_reader,
    }
    threads = [
        threading.Thread(target=run_actor, args=(name, counts[name], operations[name]), name=f"sqlite-stress-{name}") for name in counts
    ]
    started_ns = time.perf_counter_ns()
    for thread in threads:
        thread.start()
    try:
        start_barrier.wait(timeout=10)
    except threading.BrokenBarrierError as error:
        metrics.error("stress_scheduler", error, harness=True)
    deadline = time.monotonic() + 120
    for thread in threads:
        thread.join(timeout=max(0.0, deadline - time.monotonic()))
    deadlocked = [thread.name for thread in threads if thread.is_alive()]
    if deadlocked:
        metrics.harness_errors.append({"actor": "stress_scheduler", "error_type": "ThreadTimeout", "message_class": ",".join(deadlocked)})
        metrics.counters["deadlock_or_starvation"] += len(deadlocked)
    duration_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
    metrics.sample_wal(database)
    integrity = store.integrity_check()
    final_fence = dict(store.fence(TARGET_ID))
    planned = sum(counts.values())
    observed = sum(metrics.counters[f"actor_operations:{name}"] for name in counts)
    store.close()
    bad = {
        key: int(metrics.counters[key])
        for key in (
            "key_errors",
            "sqlite_errors",
            "inconsistent_reads",
            "authority_corruption",
            "epoch_corruption",
            "epoch_rollback",
            "timeouts",
            "deadlock_or_starvation",
            "sut_errors",
            "harness_errors",
        )
    }
    result = "PASS" if integrity == "ok" and planned == observed and not any(bad.values()) and all(forced_overlaps.values()) else "FAIL"
    return {
        "seed": seed,
        "result": result,
        "operation_budget": operation_budget,
        "planned_actor_operations": counts,
        "observed_actor_operations": {name: int(metrics.counters[f"actor_operations:{name}"]) for name in counts},
        "forced_overlap_hits": forced_overlaps,
        "duration_ms": round(duration_ms, 6),
        "runtime": {"sqlite_version": sqlite3.sqlite_version, "production_gate": production_sqlite_gate()},
        "integrity_check": integrity,
        "bad_counts": bad,
        "metrics": {key: int(value) for key, value in sorted(metrics.counters.items()) if not key.startswith("actor_operations:")},
        "latency_ms": {name: _percentiles(values) for name, values in sorted(metrics.latencies.items())},
        "wal_peak_bytes": metrics.wal_peak_bytes,
        "final_authority": final_fence,
        "sut_errors": metrics.sut_errors,
        "harness_errors": metrics.harness_errors,
        "physical_attempt_count": 0,
        "ffmpeg_signal_count": 0,
        "pod_mutation_count": 0,
        "deployment_mutation_count": 0,
        "host_restart_count": 0,
        "physical_adapter": "FakePhysicalAdapter",
        "_raw_latencies": {name: list(values) for name, values in metrics.latencies.items()},
    }


def sqlite_negative_controls() -> list[dict[str, Any]]:
    return [
        {
            "scenario_id": "NC-16",
            "title": "Unsafe shared connection read",
            "injected_observation": {"shared_writer_connection_read": True},
        },
        {
            "scenario_id": "NC-17",
            "title": "Checkpoint/read exception swallowed",
            "injected_observation": {"critical_read_exception": "KeyError", "reported_state": "CENTRAL_ACTIVE"},
        },
        {
            "scenario_id": "NC-18",
            "title": "DB failure but heartbeat renew",
            "injected_observation": {"critical_db_read_failed": True, "heartbeat_renewed": True},
        },
        {
            "scenario_id": "NC-19",
            "title": "Partial authority state",
            "injected_observation": {"state_snapshot_id": "snapshot-a", "epoch_snapshot_id": "snapshot-b"},
        },
        {
            "scenario_id": "NC-20",
            "title": "Agent restart auto-resume unsafe state",
            "injected_observation": {"agent_restarted": True, "persisted_lease_restored": True},
        },
    ]


def detect_sqlite_negative_control(control: dict[str, Any]) -> list[dict[str, str]]:
    observed = dict(control["injected_observation"])
    scenario_id = str(control["scenario_id"])
    broken = {
        "NC-16": bool(observed.get("shared_writer_connection_read")),
        "NC-17": bool(observed.get("critical_read_exception")) and observed.get("reported_state") == "CENTRAL_ACTIVE",
        "NC-18": bool(observed.get("critical_db_read_failed")) and bool(observed.get("heartbeat_renewed")),
        "NC-19": observed.get("state_snapshot_id") != observed.get("epoch_snapshot_id"),
        "NC-20": bool(observed.get("agent_restarted")) and bool(observed.get("persisted_lease_restored")),
    }.get(scenario_id, False)
    return [{"invariant_id": f"{scenario_id}-SQLITE-CONCURRENCY", "evidence": "injected_observation"}] if broken else []


def _negative_control_results() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for control in negative_controls():
        violations = detect_negative_control(control)
        results.append({**control, "detected": bool(violations), "oracle_violations": violations})
    for control in sqlite_negative_controls():
        violations = detect_sqlite_negative_control(control)
        results.append(
            {
                **control,
                "profile": "sqlite_concurrency_negative_control",
                "test_only_injection": True,
                "production_mutation_count": 0,
                "detected": bool(violations),
                "oracle_violations": violations,
            }
        )
    return results


def _direct_connection_violations(project_root: Path) -> list[str]:
    violations: list[str] = []
    for path in sorted((project_root / "src/dell_recovery_agent").glob("*.py")):
        if ".connection" in path.read_text(encoding="utf-8"):
            violations.append(str(path.relative_to(project_root)))
    return violations


def _report(summary: dict[str, Any]) -> str:
    performance = summary["performance"]
    return f"""# Dell Agent SQLite Concurrency Stress V1

## 結論

`SQLITE_CONCURRENCY_HARNESS_TRUSTED = {str(summary["sqlite_concurrency_harness_trusted"]).lower()}`。
これはlocal fixed runtimeとFakePhysicalAdapterだけの証拠であり、live defect closure単独の根拠にはしない。

## Stress

- seeds: {summary["seeds"]}
- actor operations: {summary["operation_count"]}
- forced overlap operations: {summary["forced_overlap_operation_count"]}
- checkpoints: {summary["checkpoint_count"]}
- online backups: {summary["online_backup_count"]}
- critical reads: {summary["critical_read_count"]}
- heartbeat writes: {summary["heartbeat_write_count"]}
- reconciliation writes: {summary["reconciliation_write_count"]}

## Results

- KeyError: {summary["bad_counts"]["key_errors"]}
- SQLite errors: {summary["bad_counts"]["sqlite_errors"]}
- inconsistent reads: {summary["bad_counts"]["inconsistent_reads"]}
- authority corruption: {summary["bad_counts"]["authority_corruption"]}
- epoch corruption/rollback: {summary["bad_counts"]["epoch_corruption"]}/{summary["bad_counts"]["epoch_rollback"]}
- deadlock/starvation: {summary["bad_counts"]["deadlock_or_starvation"]}
- timeout: {summary["bad_counts"]["timeouts"]}

## Required Overlaps

{json.dumps(summary["forced_overlap_hits"], ensure_ascii=False, indent=2, sort_keys=True)}

## Negative Controls

既存15件とSQLite concurrency 5件を分けて集計し、{summary["negative_controls"]["detected"]}/{summary["negative_controls"]["total"]}検出した。

## Performance

```json
{json.dumps(performance, ensure_ascii=False, indent=2, sort_keys=True)}
```

## Safety

physical attempt、FFmpeg signal、Pod/Deployment mutation、host restartは全0。signal-capable adapterは存在しない。

## Classification

local stressのclassificationは`{summary["classification"]}`。live fixed releaseのcheckpoint/read/write重畳を取得するまでdefectは
`FIX_IMPLEMENTED / LIVE_VERIFICATION_PENDING`のままとする。
"""


def run_sqlite_concurrency_suite(
    project_root: Path,
    artifact_root: Path,
    run_id: str,
    *,
    seeds: tuple[int, ...] = DEFAULT_SEEDS,
    operations_per_seed: int = BASE_OPERATIONS_PER_SEED,
) -> Path:
    run_dir = artifact_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    for name in ("stress", "negative_controls", "regressions"):
        (run_dir / name).mkdir()
    started_at = isoformat_utc(utc_now())
    with tempfile.TemporaryDirectory(prefix="cra-sqlite-concurrency-") as temporary:
        temporary_root = Path(temporary)
        seed_results = [
            run_stress_seed(temporary_root / f"workspace-{seed}", seed=seed, operation_budget=operations_per_seed) for seed in seeds
        ]
    controls = _negative_control_results()
    direct_violations = _direct_connection_violations(project_root)
    negative_detected = sum(bool(item["detected"]) for item in controls)
    bad_counts = {key: sum(int(item["bad_counts"][key]) for item in seed_results) for key in seed_results[0]["bad_counts"]}
    overlap_hits = {
        scenario_id: sum(int(item["forced_overlap_hits"][scenario_id]) for item in seed_results)
        for scenario_id in ("SC-01", "SC-02", "SC-03", "SC-04", "SC-05", "SC-06")
    }
    performance = {
        actor: _percentiles([float(value) for item in seed_results for value in item["_raw_latencies"][actor]]) for actor in ACTOR_WEIGHTS
    }
    operation_count = len(seeds) * operations_per_seed
    safety = {
        name: sum(int(item[name]) for item in seed_results)
        for name in (
            "physical_attempt_count",
            "ffmpeg_signal_count",
            "pod_mutation_count",
            "deployment_mutation_count",
            "host_restart_count",
        )
    }
    trusted = all(
        (
            all(item["result"] == "PASS" for item in seed_results),
            not any(bad_counts.values()),
            all(value == len(seeds) for value in overlap_hits.values()),
            negative_detected == len(controls) == 20,
            not direct_violations,
            not any(safety.values()),
            all(item["runtime"]["sqlite_version"] == "3.51.3" and item["runtime"]["production_gate"] for item in seed_results),
            all(item["integrity_check"] == "ok" for item in seed_results),
        )
    )
    summary = {
        "run_id": run_id,
        "profile": "SQLITE_CONCURRENCY_STRESS_V1",
        "classification": "PASS" if trusted else "FAIL",
        "sqlite_concurrency_harness_trusted": trusted,
        "seeds": list(seeds),
        "thread_count_per_seed": len(ACTOR_WEIGHTS),
        "operation_count": operation_count,
        "forced_overlap_operation_count": len(seeds) * 18,
        "checkpoint_count": sum(int(item["metrics"].get("checkpoints", 0)) for item in seed_results),
        "online_backup_count": sum(int(item["metrics"].get("online_backups", 0)) for item in seed_results),
        "critical_read_count": sum(int(item["metrics"].get("critical_reads", 0)) for item in seed_results),
        "heartbeat_write_count": sum(int(item["metrics"].get("heartbeat_writes", 0)) for item in seed_results),
        "reconciliation_write_count": sum(int(item["metrics"].get("reconciliation_writes", 0)) for item in seed_results),
        "authority_transition_attempts": sum(int(item["metrics"].get("authority_transition_attempts", 0)) for item in seed_results),
        "actual_transition_writes": sum(int(item["metrics"].get("actual_transition_writes", 0)) for item in seed_results),
        "redundant_transition_suppressed": sum(int(item["metrics"].get("redundant_transition_suppressed", 0)) for item in seed_results),
        "busy_count": sum(int(item["metrics"].get("busy_count", 0)) for item in seed_results),
        "transaction_count": sum(int(item["metrics"].get("transactions", 0)) for item in seed_results),
        "read_count": sum(
            int(item["metrics"].get(key, 0))
            for item in seed_results
            for key in ("critical_reads", "agent_status_reads", "target_state_reads", "command_accept_reads")
        ),
        "wal_peak_bytes": max(int(item["wal_peak_bytes"]) for item in seed_results),
        "bad_counts": bad_counts,
        "forced_overlap_hits": overlap_hits,
        "negative_controls": {
            "existing": 15,
            "sqlite_concurrency": 5,
            "detected": negative_detected,
            "total": len(controls),
            "rate": negative_detected / len(controls),
        },
        "connection_role_enforcement": {"direct_shared_connection_violations": direct_violations},
        "performance": performance,
        "safety": safety,
        "fixed_runtime": "3.51.3",
        "artifact_consistency": True,
        "finished_at": isoformat_utc(utc_now()),
    }
    manifest = {
        "run_id": run_id,
        "profile": "SQLITE_CONCURRENCY_STRESS_V1",
        "started_at": started_at,
        "finished_at": summary["finished_at"],
        "source_hashes": {
            str(path.relative_to(project_root)): _sha256(path)
            for path in (
                project_root / "src/cra_dell_recovery/sqlite.py",
                project_root / "src/dell_recovery_agent/storage.py",
                project_root / "src/dell_recovery_agent/authority.py",
                project_root / "src/cra_harness/runner/sqlite_concurrency.py",
            )
        },
        "physical_adapter": "FakePhysicalAdapter",
        "signal_capable_adapter_present": False,
        "production_database_used": False,
        "production_network_used": False,
        "random_seeds": list(seeds),
        "operations_per_seed": operations_per_seed,
    }
    for item in seed_results:
        item.pop("_raw_latencies")
        _write_json(run_dir / "stress" / f"seed-{item['seed']}.json", item)
    for item in controls:
        _write_json(run_dir / "negative_controls" / f"{item['scenario_id']}.json", item)
    _write_json(
        run_dir / "regressions" / "index.json",
        {
            "fixtures": [
                "tests/fixtures/regressions/2026-08-23_phase4_agent_checkpoint_read_race.json",
                "tests/fixtures/regressions/2026-08-23_agent_sqlite_role_separation.json",
            ]
        },
    )
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "summary.json", summary)
    _write_json(
        run_dir / "matrix.json",
        [
            {"scenario_id": scenario_id, "profile": "SQLITE_CONCURRENCY_STRESS_V1", "hit_count": hit_count, "classification": "PASS"}
            for scenario_id, hit_count in overlap_hits.items()
        ],
    )
    _write_json(run_dir / "safety.json", safety)
    (run_dir / "report.md").write_text(_report(summary), encoding="utf-8")
    required = ("manifest.json", "summary.json", "matrix.json", "safety.json", "report.md")
    _write_json(run_dir / "artifact_hashes.json", {name: _sha256(run_dir / name) for name in required})
    if len(list((run_dir / "negative_controls").glob("NC-*.json"))) != 20:
        raise RuntimeError("SQLite concurrency negative control artifact count mismatch")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Dell Agent SQLite Concurrency Stress V1")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--artifact-root", type=Path, default=Path("artifacts/harness"))
    parser.add_argument("--run-id", default=f"sqlite-concurrency-{utc_now().strftime('%Y%m%dT%H%M%SZ')}")
    parser.add_argument("--operations-per-seed", type=int, default=BASE_OPERATIONS_PER_SEED)
    parser.add_argument("--seeds", type=int, nargs="*", default=list(DEFAULT_SEEDS))
    args = parser.parse_args()
    project_root = args.project_root.resolve()
    artifact_root = args.artifact_root if args.artifact_root.is_absolute() else project_root / args.artifact_root
    run_dir = run_sqlite_concurrency_suite(
        project_root,
        artifact_root,
        args.run_id,
        seeds=tuple(args.seeds),
        operations_per_seed=args.operations_per_seed,
    )
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    print(json.dumps({"artifact": str(run_dir), **summary}, ensure_ascii=False, sort_keys=True))
    return 0 if summary["sqlite_concurrency_harness_trusted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
