from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from cra_dell_recovery.recovery_history import empty_policy
from runtime_boundary import recovery_publisher as module
from runtime_boundary.ledger import EffectLedger
from runtime_boundary.observation import RuntimeObservationPublisher
from runtime_boundary.recovery_evidence import source_hash


@pytest.fixture
def publisher(tmp_path: Path) -> Any:
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="owner", initial_producer_generation=1)
    now = datetime.now(UTC)
    target = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-id",
        "namespace": "stream-v3",
        "pod_uid": "pod-id",
        "container_name": "stream-engine",
        "container_id": "containerd://test-child",
        "ffmpeg_pid": 7654321,  # Host PID is not the child's local PID.
        "ffmpeg_generation": "ffmpeg-" + hashlib.sha256(b"pod-id:containerd://test-child:7654321:99").hexdigest()[:32],
    }
    target_path = tmp_path / "target.json"
    target_path.write_text(
        json.dumps(
            {
                "schema": "cra_dell_recovery.target_snapshot.v1",
                "status": "VALID",
                "target_identity": target,
                "observed_at": now.isoformat(),
                "valid_until": (now + timedelta(seconds=30)).isoformat(),
            }
        )
    )
    target_path.chmod(0o644)
    proc = tmp_path / "proc"
    (proc / "sys/kernel/random").mkdir(parents=True)
    (proc / "sys/kernel/random/boot_id").write_text("boot-id\n")
    (proc / "123").mkdir()
    (proc / "123/stat").write_text(f"123 (ffmpeg) S {os.getpid()} " + "0 " * 17 + "99 0\n")
    (proc / "123/environ").write_bytes(
        b"FR_FFMPEG_FORCE_KILL_ENABLED=1\0FFMPEG_RW_TIMEOUT_ENABLED=1\0SECRET_NOT_FOR_EXPORT=redacted-fixture\0"
    )
    (proc / "123/cmdline").write_bytes(b"\0".join([b"ffmpeg", b"-rw_timeout", b"15000000", b""]))
    process = {
        "local_ffmpeg_pid": 123,
        "ffmpeg_running": True,
        "managed_child_cardinality": "1",
        "managed_child_pids": [123],
        "pod_uid": "pod-id",
        "ffmpeg_generation": "native-123-generation",
    }
    output_directory = tmp_path / "exports"
    output_directory.mkdir()
    output_directory.chmod(0o755)
    config = {
        "schema": module.CONFIG_SCHEMA,
        "host_id": "dell-stream-runtime",
        "target_host_id": "dell-yuki",
        "release_id": "candidate-owner",
        "source_commit": "a" * 40,
        "stream_id": "stream-test",
        "allowed_producers": ["owner"],
        "effect_history_policy": empty_policy(),
        "output_file": str(output_directory / "recovery.json"),
    }
    value = module.RecoveryEvidencePublisher(
        ledger=ledger,
        config=config,
        target_path=target_path,
        process_supplier=lambda: process,
        proc_root=proc,
        owner_uid=os.getuid(),
    )
    try:
        yield value, process, target
    finally:
        ledger.close()


def test_export_binds_local_child_to_host_target_and_does_not_change_writer(publisher: Any) -> None:
    owner, _, target = publisher
    before = owner.ledger.connection.total_changes
    payload = owner.publish()
    assert payload["host_id"] == "dell-stream-runtime"
    assert payload["target_sha256"] == source_hash(target)
    assert payload["activation"] == {"bounded_termination_enabled": True, "rw_timeout_enabled": True}
    assert payload["effects"]["physical_attempt_count"] == payload["effects"]["unresolved_scope_count"] == 0
    assert owner.ledger.connection.execute("PRAGMA query_only").fetchone()[0] == 0
    assert owner.ledger.connection.total_changes == before
    assert json.loads(owner.path.read_text()) == payload
    assert owner.path.stat().st_mode & 0o777 == 0o644
    assert "SECRET_NOT_FOR_EXPORT" not in owner.path.read_text()
    assert not list(owner.path.parent.glob("*.tmp"))


def test_target_owner_is_explicit_and_does_not_change_export_owner(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, process, _ = publisher
    seen = []
    original = module.owned_json

    def target_read(path: Path, *, owner_uid: int = 0) -> dict[str, Any]:
        seen.append(owner_uid)
        assert path == owner.target_path
        return original(path, owner_uid=os.getuid())

    candidate = module.RecoveryEvidencePublisher(
        ledger=owner.ledger,
        config={**owner.config, "target_snapshot_owner_uid": 992},
        target_path=owner.target_path,
        process_supplier=lambda: process,
        proc_root=owner.proc_root,
        owner_uid=os.getuid(),
    )
    monkeypatch.setattr(module, "owned_json", target_read)
    candidate.publish()
    assert seen and set(seen) == {992}
    assert candidate.path.stat().st_uid == os.getuid()


@pytest.mark.parametrize("uid", [True, -1, "992", None])
def test_invalid_target_owner_is_rejected(publisher: Any, uid: Any) -> None:
    owner, process, _ = publisher
    with pytest.raises(ValueError, match="CONFIG_INVALID"):
        module.RecoveryEvidencePublisher(
            ledger=owner.ledger,
            config={**owner.config, "target_snapshot_owner_uid": uid},
            target_path=owner.target_path,
            process_supplier=lambda: process,
        )


@pytest.mark.parametrize(
    "change", ["host", "boot", "future", "stale", "generation", "pod", "pid-reuse", "zombie", "not-child", "two-children"]
)
def test_identity_and_freshness_failure_preserves_previous_export(publisher: Any, change: str) -> None:
    owner, process, _ = publisher
    owner.publish()
    original = owner.path.read_bytes()
    raw = json.loads(owner.target_path.read_text())
    if change == "host":
        raw["target_identity"]["host_id"] = "unbound-host"
    elif change == "boot":
        raw["target_identity"]["host_boot_id"] = "other-boot"
    elif change == "future":
        raw["observed_at"] = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    elif change == "stale":
        raw["valid_until"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    elif change == "generation":
        raw["target_identity"]["ffmpeg_generation"] = "other-generation"
    elif change == "pod":
        process["pod_uid"] = "other-pod"
    elif change == "two-children":
        process["managed_child_cardinality"] = "2+"
    else:
        path = owner.proc_root / "123/stat"
        text = path.read_text()
        if change == "pid-reuse":
            text = text.replace("99 0", "100 0")
        elif change == "zombie":
            text = text.replace(") S ", ") Z ")
        else:
            text = text.replace(f") S {os.getpid()} ", ") S 1 ")
        path.write_text(text)
    owner.target_path.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        owner.publish()
    assert owner.path.read_bytes() == original


def test_target_transition_during_db_read_is_not_published(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, process, _ = publisher
    real = module.owner_evidence

    def export(db: sqlite3.Connection, **kwargs: Any) -> Any:
        result = real(db, **kwargs)
        process["ffmpeg_generation"] = "new-native-generation"
        return result

    monkeypatch.setattr(module, "owner_evidence", export)
    with pytest.raises(ValueError, match="TARGET_CHANGED"):
        owner.publish()
    assert not owner.path.exists()


def test_db_failure_does_not_retime_cached_export(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, _, _ = publisher
    owner.publish()
    original = owner.path.read_bytes()

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise sqlite3.OperationalError("synthetic database unavailability")

    monkeypatch.setattr(module.sqlite3, "connect", fail)
    owner.publish_if_due()
    assert owner.last_error_code == "RECOVERY_OWNER_EXPORT_UNAVAILABLE"
    assert owner.path.read_bytes() == original


def test_readonly_transaction_and_sql_deadline_are_enforced(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, _, _ = publisher
    owner.publish()
    original = owner.path.read_bytes()

    def excessive_query(db: sqlite3.Connection, **kwargs: Any) -> Any:
        assert db.in_transaction
        assert db.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("DELETE FROM runtime_authority")
        db.execute(
            "WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM numbers WHERE x<1000000000) SELECT sum(x) FROM numbers"
        ).fetchone()
        pytest.fail("read-only export SQL deadline was not enforced")

    monkeypatch.setattr(module, "QUERY_SECONDS", 0.02)
    monkeypatch.setattr(module, "owner_evidence", excessive_query)
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        owner.publish()
    assert owner.path.read_bytes() == original
    assert owner.ledger.connection.execute("SELECT count(*) FROM runtime_authority").fetchone()[0] == 1


def test_publication_failure_keeps_previous_complete_json(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, _, _ = publisher
    owner.publish()
    original = owner.path.read_bytes()

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise OSError("isolated replace failure")

    monkeypatch.setattr(module.os, "replace", fail)
    owner.publish_if_due()
    assert owner.path.read_bytes() == original
    assert not list(owner.path.parent.glob(".*.tmp"))


def test_owner_will_not_export_into_database_directory(publisher: Any) -> None:
    owner, _, _ = publisher
    value = {**owner.config, "output_file": str(owner.ledger.path) + "-wal"}
    with pytest.raises(ValueError, match="PATH_COLLISION"):
        module.RecoveryEvidencePublisher(
            ledger=owner.ledger,
            config=value,
            target_path=owner.target_path,
            process_supplier=owner.process_supplier,
            owner_uid=os.getuid(),
        )


def test_export_is_rate_limited_with_no_catchup_burst(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, _, _ = publisher
    calls: list[bool] = []
    clock = [10.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(owner, "publish", lambda: (calls.append(True), {})[1])
    owner.publish_if_due()
    owner.publish_if_due()
    clock[0] += 4.999
    owner.publish_if_due()
    assert len(calls) == 1
    clock[0] += 100
    owner.publish_if_due()
    owner.publish_if_due()
    assert len(calls) == 2


@pytest.mark.parametrize("change", ["parent-mode", "source-mode", "source-symlink", "output-symlink"])
def test_unsafe_paths_rejected(publisher: Any, change: str) -> None:
    owner, _, _ = publisher
    if change == "parent-mode":
        owner.path.parent.chmod(0o775)
    elif change == "source-mode":
        owner.target_path.chmod(0o666)
    elif change == "source-symlink":
        original = owner.target_path.with_suffix(".original")
        owner.target_path.rename(original)
        owner.target_path.symlink_to(original)
    else:
        owner.path.symlink_to(owner.target_path)
    with pytest.raises((ValueError, OSError)):
        owner.publish()


def test_existing_observation_cycle_runs_optional_callback_and_isolates_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from runtime_boundary import observation

    calls: list[bool] = []

    def failing_export() -> None:
        calls.append(True)
        raise OSError("isolated export failure")

    monkeypatch.setattr(observation, "_tcp_metrics", lambda *_: {})
    owner = RuntimeObservationPublisher(
        path=tmp_path / "observation.json",
        projection_path=tmp_path / "missing-projection",
        target_snapshot_path=tmp_path / "missing-target",
        process_supplier=lambda: {},
        recovery_evidence_cycle=failing_export,
    )
    assert owner.publish()["sequence"] == 1
    assert owner.publish()["sequence"] == 2
    assert len(calls) == 2
    assert json.loads(owner.path.read_text())["sequence"] == 2
