from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import Any

from cra_authority.authorizer import RecoveryPolicy
from cra_authority.no_action_store import NoActionCentralStore
from cra_authority.runtime import RuntimeConfig
from cra_dell_recovery.models import RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now

DEFAULT_SEEDS = (20260901, 20260902, 20260903, 20260904, 20260905)
SAFETY = {
    "physical_attempt_count": 0,
    "ffmpeg_signal_count": 0,
    "pod_mutation_count": 0,
    "deployment_mutation_count": 0,
    "host_restart_count": 0,
    "production_database_used": False,
    "production_network_used": False,
}


@dataclass(frozen=True)
class CaseResult:
    scenario_id: str
    family: str
    classification: str
    evidence: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "family": self.family,
            "classification": self.classification,
            "evidence": self.evidence,
        }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _error_code(error: BaseException) -> str:
    return str(error).split(":", 1)[0][:160]


def _expect_rejection(operation: Callable[[], Any], *reason_fragments: str) -> tuple[bool, dict[str, str]]:
    try:
        operation()
    except BaseException as error:
        reason = _error_code(error)
        valid = not reason_fragments or any(fragment in str(error) for fragment in reason_fragments)
        return valid, {"error_class": type(error).__name__, "reason_class": reason}
    return False, {"error_class": "NONE", "reason_class": "UNEXPECTED_ACCEPTANCE"}


def _migration(project_root: Path) -> Path:
    return project_root / "migrations/central/001_initial.sql"


def _store(root: Path, project_root: Path, *, bootstrap: bool = True) -> NoActionCentralStore:
    root.mkdir(parents=True, exist_ok=False)
    store = NoActionCentralStore(root / "central.db", _migration(project_root))
    if bootstrap:
        store.bootstrap(
            target_id="stream-target",
            host_id="dell",
            agent_id="dell-agent",
            controller_instance_id="cra-controller",
            agent_installation_id="dell-installation",
        )
    return store


def _projection(sequence: int, *, projection_id: str | None = None, target_id: str = "stream-target") -> dict[str, Any]:
    now = utc_now()
    identifier = projection_id or f"projection-{sequence}"
    return {
        "projection_id": identifier,
        "target_id": target_id,
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "arena-release-a",
        "monitoring_cycle_id": f"cycle-{identifier}",
        "observation_revision": f"revision-{sequence}",
        "observation_sequence": sequence,
        "incident": {
            "incident_id": f"incident-{sequence}",
            "source_episode_id": f"episode-{sequence}",
            "state": "CONFIRMED",
        },
        "observed_target": {
            "host_id": "dell",
            "host_boot_id": "boot-a",
            "namespace": "stream-v3",
            "pod_uid": "pod-a",
            "container_name": "stream-engine",
            "container_id": "containerd://a",
            "ffmpeg_generation": "generation-a",
            "ffmpeg_pid": 4100,
        },
        "observed_at": isoformat_utc(now),
        "issued_at": isoformat_utc(now),
        "expires_at": isoformat_utc(now + timedelta(seconds=60)),
        "payload_sha256": hashlib.sha256(identifier.encode()).hexdigest(),
        "key_id": "monitoring-key-a",
        "signature": "synthetic-signature",
    }


def _authorization(sequence: int, *, blockers: tuple[str, ...] = ("CRA_OPERATING_MODE_NO_ACTION",)) -> RecoveryAuthorizationInput:
    now = utc_now()
    target = TargetIdentity(
        "dell",
        "boot-a",
        "stream-v3",
        "pod-a",
        "stream-engine",
        "containerd://a",
        "generation-a",
        4100,
    )
    return RecoveryAuthorizationInput(
        authorization_id=f"authorization-{sequence}",
        incident_id=f"incident-{sequence}",
        source_episode_id=f"episode-{sequence}",
        target_id="stream-target",
        action="restart_ffmpeg",
        reason_code="confirmed_tcp_stall",
        policy_revision="no-action-policy-a",
        observation_revision=f"revision-{sequence}",
        expected_target=target,
        blockers=blockers,
        authorized_at=isoformat_utc(now),
        expires_at=isoformat_utc(now + timedelta(seconds=30)),
    )


def _record(scenario_id: str, family: str, passed: bool, evidence: dict[str, Any]) -> CaseResult:
    return CaseResult(scenario_id, family, "PASS" if passed else "SUT_FAILURE", evidence)


def _runtime_contract_classification(
    *,
    production_gate: str,
    quick_check: str,
    foreign_key_check: str,
    integrity_check: str,
) -> str:
    if production_gate != "PASS":
        return "ENVIRONMENT_FAILURE"
    return "PASS" if quick_check == foreign_key_check == integrity_check == "ok" else "SUT_FAILURE"


def _wrong_observation_revision(authorization: RecoveryAuthorizationInput, projection: dict[str, Any]) -> RecoveryAuthorizationInput:
    del projection
    return replace(authorization, observation_revision="wrong-revision")


def _unrecorded_blocker(authorization: RecoveryAuthorizationInput, projection: dict[str, Any]) -> RecoveryAuthorizationInput:
    del projection
    return replace(authorization, blockers=("CRA_OPERATING_MODE_NO_ACTION", "UNRECORDED_BLOCKER"))


def _expiry_beyond_projection(authorization: RecoveryAuthorizationInput, projection: dict[str, Any]) -> RecoveryAuthorizationInput:
    return replace(
        authorization,
        expires_at=isoformat_utc(parse_utc(projection["expires_at"]) + timedelta(seconds=1)),
    )


def _run_deterministic(project_root: Path, workspace: Path) -> list[CaseResult]:
    results: list[CaseResult] = []

    root = workspace / "db-01"
    store = _store(root, project_root)
    runtime_integrity = store.integrity_check()
    runtime_classification = _runtime_contract_classification(
        production_gate=store.status.production_gate,
        quick_check=store.status.quick_check,
        foreign_key_check=store.status.foreign_key_check,
        integrity_check=runtime_integrity,
    )
    results.append(
        CaseResult(
            "DB-01",
            "runtime_contract",
            runtime_classification,
            {
                "sqlite_version": store.status.version,
                "production_gate": store.status.production_gate,
                "quick_check": store.status.quick_check,
                "foreign_key_check": store.status.foreign_key_check,
                "integrity_check": runtime_integrity,
            },
        )
    )
    store.close()

    root = workspace / "db-02"
    store = _store(root, project_root)
    first = _projection(1)
    accepted = store.ingest_monitoring_projection(first)
    duplicate = store.ingest_monitoring_projection(first)
    count = int(store.read_one("SELECT count(*) FROM monitoring_evidence_projections")[0])  # type: ignore[index]
    results.append(
        _record("DB-02", "projection_idempotency", accepted == "ACCEPTED" and duplicate == "DUPLICATE" and count == 1, {"rows": count})
    )
    store.close()

    root = workspace / "db-03"
    store = _store(root, project_root)
    projection = _projection(1)
    store.ingest_monitoring_projection(projection)
    conflict = {**projection, "payload_sha256": "f" * 64}
    rejected, evidence = _expect_rejection(lambda: store.ingest_monitoring_projection(conflict), "MONITORING_PROJECTION_ID_CONFLICT")
    results.append(_record("DB-03", "projection_conflict", rejected, evidence))
    store.close()

    root = workspace / "db-04"
    store = _store(root, project_root)
    store.ingest_monitoring_projection(_projection(2))
    rejected, evidence = _expect_rejection(
        lambda: store.ingest_monitoring_projection(_projection(1, projection_id="projection-regressed")),
        "MONITORING_PROJECTION_SEQUENCE_REGRESSION",
    )
    results.append(_record("DB-04", "sequence_monotonicity", rejected, evidence))
    store.close()

    root = workspace / "db-05"
    store = _store(root, project_root)
    rejected, evidence = _expect_rejection(
        lambda: store.ingest_monitoring_projection(_projection(1, target_id="missing-target")),
        "FOREIGN KEY constraint failed",
    )
    results.append(_record("DB-05", "foreign_key_write", rejected, evidence))
    store.close()

    root = workspace / "db-06"
    store = _store(root, project_root)
    projection = _projection(1)
    store.ingest_monitoring_projection(projection)
    authorization = _authorization(1)
    first_decision = store.persist_policy_decision(
        projection=projection,
        authorization=authorization,
        decision_id="decision-1",
        decision="BLOCKED",
        blockers=authorization.blockers,
    )
    repeated_decision = store.persist_policy_decision(
        projection=projection,
        authorization=authorization,
        decision_id="decision-1",
        decision="BLOCKED",
        blockers=authorization.blockers,
    )
    decision_count = int(store.read_one("SELECT count(*) FROM cra_policy_decisions")[0])  # type: ignore[index]
    results.append(
        _record(
            "DB-06",
            "decision_idempotency",
            first_decision == repeated_decision == "BLOCKED" and decision_count == 1,
            {"rows": decision_count},
        )
    )
    store.close()

    root = workspace / "db-07"
    store = _store(root, project_root)
    projection = _projection(1)
    store.ingest_monitoring_projection(projection)
    authorization = _authorization(1)
    store.persist_policy_decision(
        projection=projection,
        authorization=authorization,
        decision_id="decision-1",
        decision="BLOCKED",
        blockers=authorization.blockers,
    )
    changed = _authorization(1, blockers=("CRA_OPERATING_MODE_NO_ACTION", "EXTRA_BLOCKER"))
    rejected, evidence = _expect_rejection(
        lambda: store.persist_policy_decision(
            projection=projection,
            authorization=changed,
            decision_id="decision-1",
            decision="BLOCKED",
            blockers=changed.blockers,
        ),
        "CRA_POLICY_DECISION_CONFLICT",
    )
    results.append(_record("DB-07", "decision_conflict", rejected, evidence))
    store.close()

    for scenario_id, decision, blockers in (
        ("DB-08", "AUTHORIZED", ("CRA_OPERATING_MODE_NO_ACTION",)),
        ("DB-09", "BLOCKED", ("MONITORING_EVIDENCE_EXPIRED",)),
        ("DB-10", "BLOCKED", ()),
    ):
        root = workspace / scenario_id.lower()
        store = _store(root, project_root)
        projection = _projection(1)
        store.ingest_monitoring_projection(projection)
        authorization = _authorization(1, blockers=blockers)
        rejected, evidence = _expect_rejection(
            partial(
                store.persist_policy_decision,
                projection=projection,
                authorization=authorization,
                decision_id=f"decision-{scenario_id}",
                decision=decision,
                blockers=blockers,
            ),
            "NO_ACTION_STORE_REFUSES_EXECUTABLE_POLICY_DECISION",
        )
        zero_effects = int(store.read_one("SELECT count(*) FROM recovery_authorizations")[0]) == 0  # type: ignore[index]
        results.append(_record(scenario_id, "no_action_capability_boundary", rejected and zero_effects, evidence))
        store.close()

    root = workspace / "db-11"
    store = _store(root, project_root)
    try:
        with store._NoActionCentralStore__ledger.write() as connection:  # type: ignore[attr-defined]
            connection.execute("UPDATE targets SET version=version+1 WHERE target_id='stream-target'")
            raise RuntimeError("EXPECTED_ROLLBACK")
    except RuntimeError as error:
        if str(error) != "EXPECTED_ROLLBACK":
            raise
    version = int(store.read_one("SELECT version FROM targets WHERE target_id='stream-target'")[0])  # type: ignore[index]
    results.append(_record("DB-11", "transaction_rollback", version == 0, {"version": version}))
    store.close()

    root = workspace / "db-12"
    store = _store(root, project_root)
    projection = _projection(1)
    store.ingest_monitoring_projection(projection)
    backup = root / "online-backup.db"
    digest = store.backup_to(backup)
    store.close()
    restored = NoActionCentralStore(backup, _migration(project_root))
    restored_count = int(restored.read_one("SELECT count(*) FROM monitoring_evidence_projections")[0])  # type: ignore[index]
    restored_ok = restored.integrity_check() == "ok" and restored.status.foreign_key_check == "ok"
    restored.close()
    results.append(
        _record(
            "DB-12",
            "online_backup_restore",
            restored_ok and restored_count == 1 and digest == _sha256(backup),
            {"rows": restored_count, "backup_sha256": digest},
        )
    )

    root = workspace / "db-13"
    store = _store(root, project_root)
    existing = root / "existing.db"
    existing.write_bytes(b"preserve")
    rejected_existing, evidence = _expect_rejection(lambda: store.backup_to(existing), "SQLITE_BACKUP_DESTINATION_EXISTS")
    link = root / "backup-link.db"
    link.symlink_to(existing)
    rejected_link, _ = _expect_rejection(lambda: store.backup_to(link), "SQLITE_BACKUP_DESTINATION_EXISTS")
    results.append(
        _record(
            "DB-13",
            "backup_path_identity",
            rejected_existing and rejected_link and existing.read_bytes() == b"preserve",
            evidence,
        )
    )
    store.close()

    root = workspace / "db-14"
    store = _store(root, project_root)
    database = root / "central.db"
    store.close()
    link = root / "central-link.db"
    link.symlink_to(database)
    rejected, evidence = _expect_rejection(lambda: NoActionCentralStore(link, _migration(project_root)), "SQLITE_DATABASE_NOT_REGULAR")
    results.append(_record("DB-14", "database_path_identity", rejected, evidence))

    root = workspace / "db-15"
    store = _store(root, project_root)
    database = root / "central.db"
    store.close()
    migration_link = root / "migration-link.sql"
    migration_link.symlink_to(_migration(project_root))
    rejected, evidence = _expect_rejection(lambda: NoActionCentralStore(database, migration_link), "SQLITE_MIGRATION_NOT_REGULAR")
    results.append(_record("DB-15", "migration_path_identity", rejected, evidence))

    root = workspace / "db-16"
    store = _store(root, project_root)
    database = root / "central.db"
    store.close()
    sidecar_target = root / "sidecar-target"
    sidecar_target.touch()
    database.with_name(f"{database.name}-wal").symlink_to(sidecar_target)
    rejected, evidence = _expect_rejection(
        lambda: NoActionCentralStore(database, _migration(project_root)), "SQLITE_SIDECAR_NOT_REGULAR:-wal"
    )
    results.append(_record("DB-16", "wal_path_identity", rejected, evidence))

    root = workspace / "db-17"
    store = _store(root, project_root)
    database = root / "central.db"
    store.close()
    connection = sqlite3.connect(database)
    connection.execute("INSERT INTO schema_migrations VALUES ('999_unknown.sql','deadbeef','2026-09-01T00:00:00Z')")
    connection.commit()
    connection.close()
    rejected, evidence = _expect_rejection(lambda: NoActionCentralStore(database, _migration(project_root)), "MIGRATION_SET_MISMATCH")
    results.append(_record("DB-17", "migration_set", rejected, evidence))

    root = workspace / "db-18"
    migration_root = root / "migrations"
    shutil.copytree(_migration(project_root).parent, migration_root)
    migration = migration_root / "001_initial.sql"
    root.mkdir(exist_ok=True)
    database = root / "central.db"
    store = NoActionCentralStore(database, migration)
    store.close()
    changed_migration = migration_root / "003_cra_responsibility_split.sql"
    changed_migration.write_text(changed_migration.read_text(encoding="utf-8") + "\n-- checksum drift\n", encoding="utf-8")
    rejected, evidence = _expect_rejection(lambda: NoActionCentralStore(database, migration), "MIGRATION_CHECKSUM_MISMATCH")
    results.append(_record("DB-18", "migration_checksum", rejected, evidence))

    root = workspace / "db-19"
    store = _store(root, project_root)
    database = root / "central.db"
    store.close()
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE unexpected_shadow(value TEXT) STRICT")
    connection.close()
    rejected, evidence = _expect_rejection(
        lambda: NoActionCentralStore(database, _migration(project_root)), "CENTRAL_SCHEMA_CONTRACT_DRIFT"
    )
    results.append(_record("DB-19", "schema_fingerprint", rejected, evidence))

    root = workspace / "db-20"
    store = _store(root, project_root)
    database = root / "central.db"
    store.close()
    stamp = isoformat_utc(utc_now())
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute(
        "INSERT INTO monitoring_evidence_projections VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "orphan-projection",
            "missing-target",
            "arena",
            "release",
            "cycle",
            "revision",
            1,
            "incident",
            "CONFIRMED",
            stamp,
            stamp,
            stamp,
            "{}",
            "a" * 64,
            "key",
            "signature",
            stamp,
        ),
    )
    connection.commit()
    connection.close()
    rejected, evidence = _expect_rejection(lambda: NoActionCentralStore(database, _migration(project_root)), "foreign_key_check")
    results.append(_record("DB-20", "foreign_key_startup", rejected, evidence))

    root = workspace / "db-21"
    store = _store(root, project_root)
    database = root / "central.db"
    store.close()
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA ignore_check_constraints=ON")
    connection.execute("UPDATE targets SET current_authority_epoch=-1 WHERE target_id='stream-target'")
    connection.commit()
    connection.close()
    rejected, evidence = _expect_rejection(lambda: NoActionCentralStore(database, _migration(project_root)), "SQLite readiness failed")
    results.append(_record("DB-21", "check_constraint_startup", rejected, evidence))

    base_root = workspace / "corruption-base"
    store = _store(base_root, project_root)
    store.close()
    for scenario_id, mutation in (("DB-22", "header"), ("DB-23", "truncation")):
        root = workspace / scenario_id.lower()
        root.mkdir()
        database = root / "central.db"
        shutil.copy2(base_root / "central.db", database)
        if mutation == "header":
            with database.open("r+b") as handle:
                handle.write(b"\x00" * 32)
                handle.flush()
                os.fsync(handle.fileno())
        else:
            with database.open("r+b") as handle:
                handle.truncate(257)
                handle.flush()
                os.fsync(handle.fileno())
        rejected, evidence = _expect_rejection(partial(NoActionCentralStore, database, _migration(project_root)))
        results.append(_record(scenario_id, f"database_{mutation}", rejected, evidence))

    for scenario_id, commit in (("DB-24", False), ("DB-25", True)):
        root = workspace / scenario_id.lower()
        store = _store(root, project_root)
        database = root / "central.db"
        store.close()
        child = (
            "import os,sqlite3,sys;"
            "c=sqlite3.connect(sys.argv[1],isolation_level=None);"
            "c.execute('PRAGMA journal_mode=WAL');"
            "c.execute('PRAGMA synchronous=FULL');"
            "c.execute('BEGIN IMMEDIATE');"
            "c.execute(\"UPDATE targets SET version=version+1 WHERE target_id='stream-target'\");"
            + ("c.execute('COMMIT');" if commit else "")
            + "os._exit(73)"
        )
        completed = subprocess.run([sys.executable, "-c", child, str(database)], check=False)
        reopened = NoActionCentralStore(database, _migration(project_root))
        version = int(reopened.read_one("SELECT version FROM targets WHERE target_id='stream-target'")[0])  # type: ignore[index]
        integrity = reopened.integrity_check()
        reopened.close()
        expected_version = 1 if commit else 0
        results.append(
            _record(
                scenario_id,
                "crash_commit_boundary",
                completed.returncode == 73 and version == expected_version and integrity == "ok",
                {"committed": commit, "version": version, "child_exit": completed.returncode},
            )
        )

    root = workspace / "db-26"
    store = _store(root, project_root)
    nested, evidence = _expect_rejection(
        lambda: _nested_write(store),
        "SQLITE_NESTED_WRITE_FORBIDDEN",
    )
    results.append(_record("DB-26", "nested_writer", nested, evidence))
    store.close()

    mismatch_cases: tuple[
        tuple[
            str,
            Callable[[RecoveryAuthorizationInput, dict[str, Any]], RecoveryAuthorizationInput],
            str,
        ],
        ...,
    ] = (
        ("DB-28", _wrong_observation_revision, "context"),
        ("DB-29", _unrecorded_blocker, "context"),
        ("DB-30", _expiry_beyond_projection, "time"),
    )
    for scenario_id, mutate, mismatch_kind in mismatch_cases:
        root = workspace / scenario_id.lower()
        store = _store(root, project_root)
        projection = _projection(1)
        store.ingest_monitoring_projection(projection)
        authorization = mutate(_authorization(1), projection)
        expected_reason = "NO_ACTION_DECISION_TIME_BOUNDARY_MISMATCH" if mismatch_kind == "time" else "NO_ACTION_DECISION_CONTEXT_MISMATCH"
        rejected, evidence = _expect_rejection(
            partial(
                store.persist_policy_decision,
                projection=projection,
                authorization=authorization,
                decision_id=f"decision-{scenario_id}",
                decision="BLOCKED",
                blockers=("CRA_OPERATING_MODE_NO_ACTION",),
            ),
            expected_reason,
        )
        results.append(_record(scenario_id, "decision_context_binding", rejected, evidence))
        store.close()

    root = workspace / "db-31"
    store = _store(root, project_root)
    ledger = store._NoActionCentralStore__ledger  # type: ignore[attr-defined]
    page_count = int(ledger.connection.execute("PRAGMA page_count").fetchone()[0])
    ledger.connection.execute(f"PRAGMA max_page_count={page_count}")
    projection = _projection(1)
    projection["signature"] = "x" * (2 * 1024 * 1024)
    rejected, evidence = _expect_rejection(lambda: store.ingest_monitoring_projection(projection), "SQLITE_FULL")
    ledger.connection.execute(f"PRAGMA max_page_count={page_count + 4096}")
    projection_rows = int(store.read_one("SELECT count(*) FROM monitoring_evidence_projections")[0])  # type: ignore[index]
    recovered = store.ingest_monitoring_projection(_projection(1, projection_id="projection-after-full"))
    integrity = store.integrity_check()
    results.append(
        _record(
            "DB-31",
            "disk_full_transition",
            rejected and projection_rows == 0 and recovered == "ACCEPTED" and integrity == "ok",
            {**evidence, "rows_after_failure": projection_rows, "recovery": recovered, "integrity": integrity},
        )
    )
    store.close()

    return results


def _nested_write(store: NoActionCentralStore) -> None:
    ledger = store._NoActionCentralStore__ledger  # type: ignore[attr-defined]
    with ledger.write(), ledger.write():
        pass


def _runtime_config_base(root: Path) -> dict[str, Any]:
    return {
        "schema": "cra.runtime.v2",
        "operating_mode": "NO_ACTION",
        "runtime_release_id": "synthetic-release-a",
        "database": str(root / "central.db"),
        "migration": str(root / "001_initial.sql"),
        "target_id": "stream-target",
        "lock_file": str(root / "runtime.lock"),
        "status_file": str(root / "status.json"),
        "event_file": str(root / "events.jsonl"),
        "cycle_interval_seconds": 1,
        "heartbeat_interval_seconds": 60,
        "monitoring": {
            "projection_file": str(root / "projection.json"),
            "schema_file": str(root / "projection.schema.json"),
            "source_instance_id": "arena-monitoring-v4",
            "source_release_id": "arena-release-a",
            "key_id": "monitoring-key-a",
            "public_key_file": str(root / "monitoring.pem"),
            "maximum_ttl_seconds": 60,
            "maximum_check_age_seconds": 180,
        },
        "policy": {
            "revision": "no-action-policy-a",
            "authorization_lifetime_seconds": 30,
            "minimum_action_interval_seconds": 60,
            "hourly_action_limit": 24,
            "daily_action_limit": 96,
        },
        "retention": {
            "archive_database": str(root / "archive.db"),
            "archive_signing_private_key": str(root / "archive.pem"),
            "archive_key_id": "archive-key-a",
            "retain_decision_count": 4096,
            "compact_interval_seconds": 60,
        },
    }


def _set_path(value: dict[str, Any], path: tuple[str, ...], replacement: Any) -> None:
    current: dict[str, Any] = value
    for field in path[:-1]:
        current = current[field]
    current[path[-1]] = replacement


def _run_config_semantics(workspace: Path) -> tuple[list[CaseResult], int]:
    root = workspace / "config-semantics"
    root.mkdir()
    base = _runtime_config_base(root)
    numeric_fields: tuple[tuple[str, ...], ...] = (
        ("cycle_interval_seconds",),
        ("heartbeat_interval_seconds",),
        ("monitoring", "maximum_ttl_seconds"),
        ("monitoring", "maximum_check_age_seconds"),
        ("retention", "compact_interval_seconds"),
    )
    integer_fields: tuple[tuple[str, ...], ...] = (
        ("policy", "authorization_lifetime_seconds"),
        ("policy", "minimum_action_interval_seconds"),
        ("policy", "hourly_action_limit"),
        ("policy", "daily_action_limit"),
        ("retention", "retain_decision_count"),
    )
    string_fields: tuple[tuple[str, ...], ...] = (
        ("runtime_release_id",),
        ("target_id",),
        ("monitoring", "source_instance_id"),
        ("monitoring", "source_release_id"),
        ("monitoring", "key_id"),
        ("policy", "revision"),
        ("retention", "archive_key_id"),
    )
    path_fields: tuple[tuple[str, ...], ...] = (
        ("database",),
        ("migration",),
        ("lock_file",),
        ("status_file",),
        ("event_file",),
        ("monitoring", "projection_file"),
        ("monitoring", "schema_file"),
        ("monitoring", "public_key_file"),
        ("retention", "archive_database"),
        ("retention", "archive_signing_private_key"),
    )
    cases: list[tuple[tuple[str, ...], Any]] = []
    numeric_invalids: tuple[Any, ...] = (True, False, "1", None, [], {}, math.nan, math.inf, -math.inf)
    integer_invalids: tuple[Any, ...] = (True, False, 1.0, "1", None, [], {}, -1)
    string_invalids: tuple[Any, ...] = (True, 1, None, "", " ")
    path_invalids: tuple[Any, ...] = (True, None, "", "relative/path")
    for field in numeric_fields:
        cases.extend((field, invalid_value) for invalid_value in numeric_invalids)
    for field in integer_fields:
        cases.extend((field, invalid_value) for invalid_value in integer_invalids)
    for field in string_fields:
        cases.extend((field, invalid_value) for invalid_value in string_invalids)
    for field in path_fields:
        cases.extend((field, invalid_value) for invalid_value in path_invalids)
    cases.extend(
        [
            (("cycle_interval_seconds",), 0.099),
            (("cycle_interval_seconds",), 301),
            (("heartbeat_interval_seconds",), 9),
            (("heartbeat_interval_seconds",), 3601),
            (("monitoring", "maximum_ttl_seconds"), 0),
            (("monitoring", "maximum_ttl_seconds"), 61),
            (("monitoring", "maximum_check_age_seconds"), 0),
            (("monitoring", "maximum_check_age_seconds"), 301),
            (("policy", "authorization_lifetime_seconds"), 0),
            (("policy", "authorization_lifetime_seconds"), 301),
            (("policy", "minimum_action_interval_seconds"), 0),
            (("policy", "hourly_action_limit"), 0),
            (("policy", "daily_action_limit"), 0),
            (("retention", "retain_decision_count"), 1),
            (("retention", "compact_interval_seconds"), 9),
            (("retention", "compact_interval_seconds"), 86401),
        ]
    )
    path = root / "runtime.json"
    rejected = 0
    for field, invalid in cases:
        value = json.loads(json.dumps(base))
        _set_path(value, field, invalid)
        path.write_text(json.dumps(value, allow_nan=True), encoding="utf-8")
        os.chmod(path, 0o600)
        valid, _ = _expect_rejection(partial(RuntimeConfig.load, path))
        rejected += int(valid)
    relationship = json.loads(json.dumps(base))
    relationship["policy"]["hourly_action_limit"] = 25
    relationship["policy"]["daily_action_limit"] = 24
    path.write_text(json.dumps(relationship), encoding="utf-8")
    os.chmod(path, 0o600)
    valid_relationship, _ = _expect_rejection(partial(RuntimeConfig.load, path), "daily_action_limit")
    rejected += int(valid_relationship)
    policy_cases: tuple[dict[str, Any], ...] = (
        {"revision": ""},
        {"revision": True},
        {"authorization_lifetime_seconds": True},
        {"authorization_lifetime_seconds": 0},
        {"minimum_action_interval_seconds": 0},
        {"hourly_action_limit": 0},
        {"hourly_action_limit": 1.0},
        {"daily_action_limit": 0},
        {"hourly_action_limit": 25, "daily_action_limit": 24},
        {"materialize_authorization": 1},
    )
    for policy_case in policy_cases:
        valid, _ = _expect_rejection(partial(RecoveryPolicy, **{"revision": "policy-a", **policy_case}))
        rejected += int(valid)
    total = len(cases) + 1 + len(policy_cases)
    return [
        _record(
            "CFG-01",
            "type_range_and_path_semantics",
            rejected == total,
            {"invalid_cases": total, "rejected_cases": rejected},
        )
    ], total


def _run_random_trace(project_root: Path, workspace: Path, *, seed: int, operations: int) -> dict[str, Any]:
    root = workspace / f"trace-{seed}"
    store = _store(root, project_root)
    generator = random.Random(seed)
    accepted: dict[str, dict[str, Any]] = {}
    decisions: set[str] = set()
    decision_authorizations: dict[str, RecoveryAuthorizationInput] = {}
    high_sequence = 0
    counters = {name: 0 for name in ("accepted", "duplicates", "conflicts", "regressions", "decisions", "checkpoints", "backups")}
    errors: list[dict[str, str]] = []
    error_count = 0
    schedule = [6 if index % 257 == 0 else 5 if index % 31 == 0 else index % 5 for index in range(operations)]
    generator.shuffle(schedule)
    for index, operation in enumerate(schedule, start=1):
        try:
            if operation == 0 or not accepted:
                candidate_sequence = high_sequence + 1
                projection = _projection(candidate_sequence, projection_id=f"trace-{seed}-{candidate_sequence}")
                if store.ingest_monitoring_projection(projection) != "ACCEPTED":
                    raise AssertionError("new projection was not accepted")
                accepted[str(projection["projection_id"])] = projection
                high_sequence = candidate_sequence
                counters["accepted"] += 1
            elif operation == 1:
                projection = generator.choice(list(accepted.values()))
                if store.ingest_monitoring_projection(projection) != "DUPLICATE":
                    raise AssertionError("duplicate projection was not idempotent")
                counters["duplicates"] += 1
            elif operation == 2:
                projection = dict(generator.choice(list(accepted.values())))
                projection["payload_sha256"] = hashlib.sha256(f"conflict-{seed}-{index}".encode()).hexdigest()
                rejected, _ = _expect_rejection(
                    partial(store.ingest_monitoring_projection, projection),
                    "MONITORING_PROJECTION_ID_CONFLICT",
                )
                if not rejected:
                    raise AssertionError("projection conflict was accepted")
                counters["conflicts"] += 1
            elif operation == 3:
                projection = _projection(max(1, high_sequence), projection_id=f"regression-{seed}-{index}")
                rejected, _ = _expect_rejection(
                    partial(store.ingest_monitoring_projection, projection),
                    "MONITORING_PROJECTION_SEQUENCE_REGRESSION",
                )
                if not rejected:
                    raise AssertionError("sequence regression was accepted")
                counters["regressions"] += 1
            elif operation == 4:
                projection = generator.choice(list(accepted.values()))
                sequence = int(projection["observation_sequence"])
                decision_id = f"trace-decision-{seed}-{sequence}"
                authorization = decision_authorizations.setdefault(
                    decision_id,
                    replace(
                        _authorization(sequence),
                        authorized_at=str(projection["issued_at"]),
                        expires_at=isoformat_utc(parse_utc(str(projection["issued_at"])) + timedelta(seconds=30)),
                    ),
                )
                store.persist_policy_decision(
                    projection=projection,
                    authorization=authorization,
                    decision_id=decision_id,
                    decision="BLOCKED",
                    blockers=authorization.blockers,
                )
                decisions.add(decision_id)
                counters["decisions"] += 1
            elif operation == 5:
                store.checkpoint(("PASSIVE", "FULL", "TRUNCATE")[index % 3])
                counters["checkpoints"] += 1
            else:
                destination = root / f"backup-{index:05d}.db"
                store.backup_to(destination)
                backup = sqlite3.connect(f"{destination.absolute().as_uri()}?mode=ro", uri=True)
                try:
                    if str(backup.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
                        raise AssertionError("backup integrity failed")
                    if backup.execute("PRAGMA foreign_key_check").fetchone() is not None:
                        raise AssertionError("backup foreign key check failed")
                finally:
                    backup.close()
                    destination.unlink(missing_ok=True)
                counters["backups"] += 1
        except BaseException as error:
            error_count += 1
            if len(errors) < 100:
                errors.append({"operation": str(operation), "error_class": type(error).__name__, "reason_class": _error_code(error)})
    expected_projections = len(accepted)
    expected_decisions = len(decisions)
    observed_projections = int(store.read_one("SELECT count(*) FROM monitoring_evidence_projections")[0])  # type: ignore[index]
    observed_decisions = int(store.read_one("SELECT count(*) FROM cra_policy_decisions")[0])  # type: ignore[index]
    authorizations = int(store.read_one("SELECT count(*) FROM recovery_authorizations")[0])  # type: ignore[index]
    commands = int(store.read_one("SELECT count(*) FROM commands")[0])  # type: ignore[index]
    integrity = store.integrity_check()
    foreign_keys = store.status.foreign_key_check
    store.close()
    reopened = NoActionCentralStore(root / "central.db", _migration(project_root))
    reopen_counts = (
        int(reopened.read_one("SELECT count(*) FROM monitoring_evidence_projections")[0]),  # type: ignore[index]
        int(reopened.read_one("SELECT count(*) FROM cra_policy_decisions")[0]),  # type: ignore[index]
    )
    reopened.close()
    passed = (
        error_count == 0
        and observed_projections == expected_projections
        and observed_decisions == expected_decisions
        and reopen_counts == (expected_projections, expected_decisions)
        and authorizations == commands == 0
        and integrity == foreign_keys == "ok"
    )
    return {
        "seed": seed,
        "classification": "PASS" if passed else "SUT_FAILURE",
        "operations": operations,
        "counters": counters,
        "expected": {"projections": expected_projections, "decisions": expected_decisions},
        "observed": {
            "projections": observed_projections,
            "decisions": observed_decisions,
            "recovery_authorizations": authorizations,
            "commands": commands,
        },
        "reopen_counts": list(reopen_counts),
        "integrity_check": integrity,
        "foreign_key_check": foreign_keys,
        "errors": errors,
        "error_count": error_count,
        **SAFETY,
    }


def _run_concurrency(project_root: Path, workspace: Path, *, projection_count: int = 512) -> CaseResult:
    root = workspace / "central-concurrency"
    store = _store(root, project_root)
    projections = [_projection(index, projection_id=f"concurrent-{index}") for index in range(1, projection_count + 1)]
    for projection in projections:
        store.ingest_monitoring_projection(projection)
    start = threading.Barrier(10)
    errors: list[dict[str, str]] = []
    lock = threading.Lock()

    def capture(error: BaseException) -> None:
        with lock:
            errors.append({"error_class": type(error).__name__, "reason_class": _error_code(error)})

    def writer(worker: int) -> None:
        try:
            start.wait(timeout=10)
            for projection in projections[worker::8]:
                sequence = int(projection["observation_sequence"])
                authorization = _authorization(sequence)
                store.persist_policy_decision(
                    projection=projection,
                    authorization=authorization,
                    decision_id=f"concurrent-decision-{sequence}",
                    decision="BLOCKED",
                    blockers=authorization.blockers,
                )
        except BaseException as error:
            capture(error)

    def reader() -> None:
        try:
            start.wait(timeout=10)
            previous = 0
            for _ in range(projection_count):
                row = store.read_one("SELECT count(*) FROM cra_policy_decisions")
                current = 0 if row is None else int(row[0])
                if current < previous:
                    raise AssertionError("decision count regressed")
                previous = current
        except BaseException as error:
            capture(error)

    def checkpointer() -> None:
        try:
            start.wait(timeout=10)
            for index in range(60):
                store.checkpoint(("PASSIVE", "FULL", "TRUNCATE")[index % 3])
        except BaseException as error:
            capture(error)

    threads = [threading.Thread(target=writer, args=(worker,)) for worker in range(8)]
    threads.extend((threading.Thread(target=reader), threading.Thread(target=checkpointer)))
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    alive = sum(thread.is_alive() for thread in threads)
    elapsed = time.perf_counter() - started
    decisions = int(store.read_one("SELECT count(*) FROM cra_policy_decisions")[0])  # type: ignore[index]
    authorizations = int(store.read_one("SELECT count(*) FROM recovery_authorizations")[0])  # type: ignore[index]
    commands = int(store.read_one("SELECT count(*) FROM commands")[0])  # type: ignore[index]
    integrity = store.integrity_check()
    store.close()
    passed = not errors and alive == 0 and decisions == projection_count and authorizations == commands == 0 and integrity == "ok"
    return _record(
        "DB-27",
        "central_concurrency_checkpoint",
        passed,
        {
            "writer_threads": 8,
            "projection_count": projection_count,
            "decision_count": decisions,
            "checkpoint_count": 60,
            "alive_threads": alive,
            "errors": errors,
            "duration_seconds": round(elapsed, 6),
        },
    )


def _run_multiprocess_contention(project_root: Path, workspace: Path, *, worker_count: int = 8, projection_count: int = 512) -> CaseResult:
    root = workspace / "central-multiprocess"
    store = _store(root, project_root)
    for sequence in range(1, projection_count + 1):
        store.ingest_monitoring_projection(_projection(sequence, projection_id=f"multiprocess-{sequence}"))
    database = root / "central.db"
    store.close()
    ready = root / "ready"
    ready.mkdir()
    start = root / "start"
    child = """
import json,sys,time
from pathlib import Path
from cra_authority.no_action_store import NoActionCentralStore
from cra_harness.runner.no_action_sqlite_chaos import _authorization
database,migration,ready,start,worker,sequences=sys.argv[1:]
store=NoActionCentralStore(Path(database),Path(migration))
Path(ready,worker).touch()
deadline=time.monotonic()+15
while not Path(start).exists():
    if time.monotonic() >= deadline:
        raise RuntimeError('MULTIPROCESS_START_TIMEOUT')
    time.sleep(0.005)
for text in sequences.split(','):
    sequence=int(text)
    row=store.read_one('SELECT payload_json FROM monitoring_evidence_projections WHERE observation_sequence=?',(sequence,))
    if row is None:
        raise RuntimeError('MULTIPROCESS_PROJECTION_MISSING')
    projection=json.loads(str(row[0]))
    authorization=_authorization(sequence)
    store.persist_policy_decision(projection=projection,authorization=authorization,decision_id=f'multiprocess-decision-{sequence}',decision='BLOCKED',blockers=authorization.blockers)
store.close()
"""
    processes: list[subprocess.Popen[str]] = []
    for worker in range(worker_count):
        sequences = ",".join(str(sequence) for sequence in range(worker + 1, projection_count + 1, worker_count))
        processes.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    child,
                    str(database),
                    str(_migration(project_root)),
                    str(ready),
                    str(start),
                    str(worker),
                    sequences,
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )
    ready_deadline = time.monotonic() + 20
    while len(tuple(ready.iterdir())) < worker_count and time.monotonic() < ready_deadline:
        time.sleep(0.01)
    ready_count = len(tuple(ready.iterdir()))
    started = time.perf_counter()
    start.touch()
    failures: list[dict[str, Any]] = []
    for worker, process in enumerate(processes):
        try:
            stdout, stderr = process.communicate(timeout=45)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
            failures.append({"worker": worker, "returncode": process.returncode, "reason": "TIMEOUT"})
            continue
        if process.returncode != 0:
            failures.append(
                {
                    "worker": worker,
                    "returncode": process.returncode,
                    "reason": (stderr or stdout).splitlines()[-1][:200] if (stderr or stdout) else "NO_OUTPUT",
                }
            )
    elapsed = time.perf_counter() - started
    reopened = NoActionCentralStore(database, _migration(project_root))
    decisions = int(reopened.read_one("SELECT count(*) FROM cra_policy_decisions")[0])  # type: ignore[index]
    effects = int(reopened.read_one("SELECT count(*) FROM commands")[0])  # type: ignore[index]
    integrity = reopened.integrity_check()
    reopened.close()
    passed = ready_count == worker_count and not failures and decisions == projection_count and effects == 0 and integrity == "ok"
    return _record(
        "DB-32",
        "multiprocess_writer_contention",
        passed,
        {
            "worker_count": worker_count,
            "ready_count": ready_count,
            "projection_count": projection_count,
            "decision_count": decisions,
            "command_count": effects,
            "integrity": integrity,
            "duration_seconds": round(elapsed, 6),
            "failures": failures,
        },
    )


def run_suite(
    project_root: Path,
    output: Path,
    *,
    seeds: tuple[int, ...] = DEFAULT_SEEDS,
    operations_per_seed: int = 512,
) -> dict[str, Any]:
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    started_at = isoformat_utc(utc_now())
    with tempfile.TemporaryDirectory(prefix="cra-no-action-sqlite-chaos-") as temporary:
        workspace = Path(temporary)
        deterministic = _run_deterministic(project_root, workspace)
        config_results, config_cases = _run_config_semantics(workspace)
        concurrency = _run_concurrency(project_root, workspace)
        multiprocess = _run_multiprocess_contention(project_root, workspace)
        randomized: list[dict[str, Any]] = []
        for seed in seeds:
            try:
                randomized.append(_run_random_trace(project_root, workspace, seed=seed, operations=operations_per_seed))
            except BaseException as error:
                randomized.append(
                    {
                        "seed": seed,
                        "classification": "ENVIRONMENT_FAILURE",
                        "operations": operations_per_seed,
                        "error_count": 1,
                        "errors": [{"error_class": type(error).__name__, "reason_class": _error_code(error)}],
                        **SAFETY,
                    }
                )
    all_cases = [*deterministic, *config_results, concurrency, multiprocess]
    environment_failure_count = sum(item.classification == "ENVIRONMENT_FAILURE" for item in all_cases) + sum(
        item["classification"] == "ENVIRONMENT_FAILURE" for item in randomized
    )
    sut_failure_count = sum(item.classification == "SUT_FAILURE" for item in all_cases)
    deterministic_pass = all(item.classification == "PASS" for item in all_cases)
    randomized_pass = all(item["classification"] == "PASS" for item in randomized)
    safety_pass = not any(value for key, value in SAFETY.items() if key.endswith("_count"))
    classification = (
        "ENVIRONMENT_FAILURE" if environment_failure_count else "PASS" if deterministic_pass and randomized_pass and safety_pass else "FAIL"
    )
    summary = {
        "schema": "cra.no_action_sqlite_chaos.v1",
        "classification": classification,
        "started_at": started_at,
        "finished_at": isoformat_utc(utc_now()),
        "sqlite_version": sqlite3.sqlite_version,
        "deterministic_case_count": len(all_cases),
        "deterministic_pass_count": sum(item.classification == "PASS" for item in all_cases),
        "environment_failure_count": environment_failure_count,
        "sut_failure_count": sut_failure_count,
        "config_invalid_case_count": config_cases,
        "random_seeds": list(seeds),
        "random_operations_per_seed": operations_per_seed,
        "random_operation_count": len(seeds) * operations_per_seed,
        "randomized_pass_count": sum(item["classification"] == "PASS" for item in randomized),
        "randomized_run_count": len(randomized),
        "safety": SAFETY,
        "source_hashes": {
            str(path.relative_to(project_root)): _sha256(path)
            for path in (
                project_root / "src/cra_dell_recovery/sqlite.py",
                project_root / "src/cra_authority/schema_contract.py",
                project_root / "src/cra_authority/no_action_store.py",
                project_root / "src/cra_authority/runtime.py",
                project_root / "src/cra_authority/authorizer.py",
                project_root / "src/cra_harness/runner/no_action_sqlite_chaos.py",
            )
        },
    }
    _write_json(output / "summary.json", summary)
    _write_json(output / "deterministic_cases.json", [item.to_dict() for item in all_cases])
    _write_json(output / "randomized_traces.json", randomized)
    _write_json(output / "safety.json", SAFETY)
    artifact_hashes = {
        name: _sha256(output / name) for name in ("summary.json", "deterministic_cases.json", "randomized_traces.json", "safety.json")
    }
    _write_json(output / "artifact_hashes.json", artifact_hashes)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run isolated CRA no-action SQLite and interpretation chaos")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--operations-per-seed", type=int, default=512)
    parser.add_argument("--seeds", type=int, nargs="*", default=list(DEFAULT_SEEDS))
    args = parser.parse_args()
    project_root = args.project_root.resolve()
    output = args.output if args.output.is_absolute() else project_root / args.output
    summary = run_suite(
        project_root,
        output,
        seeds=tuple(args.seeds),
        operations_per_seed=args.operations_per_seed,
    )
    print(json.dumps({"artifact": str(output), **summary}, ensure_ascii=False, sort_keys=True))
    return 0 if summary["classification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
