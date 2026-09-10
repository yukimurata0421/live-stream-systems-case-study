"""Postmortem-derived read/check/use and cleanup interleavings.

Only fixture-owned files/DBs and explicitly created local children are touched.
Assertions describe externally observable evidence, not private call counts.
"""

from __future__ import annotations

import copy
import errno
import hashlib
import json
import os
import random
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from cra_dell_recovery.recovery_history import empty_policy
from cra_no_action_soak import recovery_soak
from cra_no_action_soak.recovery_facts import runtime_evidence_fact, sign_facts, source_hash, validate_lifecycle
from runtime_boundary import recovery_publisher as module
from tests.harness.integration.test_owner_lifecycle_evidence import (
    FixtureRegistry,
    binding,
    child,
    configure,
    registry_entry,
    wait_for_proc_command,
)
from tests.harness.unit.test_recovery_soak import START, packet
from tests.harness.unit.test_recovery_soak import setup as setup
from tests.runtime_boundary.test_recovery_publisher import publisher as publisher


def helper(owner: Any, *, present: bool) -> None:
    root = owner.proc_root / "124"
    tasks = owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children"
    if present:
        root.mkdir(exist_ok=True)
        (root / "stat").write_text(f"124 (probe-helper) S {os.getpid()} " + "0 " * 17 + "100 0\n")
        (root / "cmdline").write_bytes(b"probe-helper\0")
        tasks.write_text("123 124")
    else:
        for name in ("stat", "cmdline"):
            (root / name).unlink(missing_ok=True)
        if root.exists():
            root.rmdir()
        tasks.write_text("123")


def running(owner: Any) -> dict[str, Any]:
    result = owner.publish()
    assert result["lifecycle"]["state"] == "RUNNING", result
    assert result["target_identity"] is not None
    assert all(v is True for v in result["activation"].values())
    assert result["effects"]["integrity"] == "ok"
    assert result["effects"]["physical_attempt_count"] == 0
    return result


@pytest.mark.parametrize("at", ["child_stat", "child_command", "child_stat_final", "child_set_final", "task_list_final", "thread_children"])
def test_normal_cleanup_interleaving_keeps_fresh_exact_child(publisher: Any, monkeypatch: pytest.MonkeyPatch, at: str) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    before = running(owner)
    helper(owner, present=True)
    fired = False
    original_stat, original_open, original_iterdir = owner._proc_stat, Path.open, Path.iterdir
    tasks = owner.proc_root / str(os.getpid()) / "task"
    extra = tasks / "999999"
    if at == "thread_children":
        extra.mkdir()
        (extra / "children").write_text("")

    def mutate() -> None:
        nonlocal fired
        fired = True
        if at == "task_list_final":
            extra.mkdir()
            (extra / "children").write_text("")
        elif at == "thread_children":
            (extra / "children").unlink()
            extra.rmdir()
        else:
            helper(owner, present=False)

    def stat(pid: int) -> Any:
        if not fired and pid == 124 and owner._read_stage == at:
            mutate()
        return original_stat(pid)

    def opened(path: Path, *args: Any, **kwargs: Any) -> Any:
        if (
            not fired
            and owner._read_stage == at
            and (
                (at == "child_command" and path == owner.proc_root / "124/cmdline")
                or (at == "thread_children" and path == extra / "children")
            )
        ):
            mutate()
        return original_open(path, *args, **kwargs)

    def iterdir(path: Path) -> Any:
        if not fired and path == tasks and owner._read_stage in {"task_list_final", "child_set_final"}:
            mutate()
        return original_iterdir(path)

    # child_set_final begins by re-reading thread children.
    def scan_read(path: Path, *args: Any, **kwargs: Any) -> Any:
        if not fired and at == "child_set_final" and owner._read_stage == "child_stat_final" and path == owner.proc_root / "124/stat":
            result = original_open(path, *args, **kwargs)
            mutate()
            return result
        return opened(path, *args, **kwargs)

    monkeypatch.setattr(owner, "_proc_stat", stat)
    monkeypatch.setattr(Path, "open", scan_read)
    monkeypatch.setattr(Path, "iterdir", iterdir)
    after = running(owner)
    assert fired
    assert after["target_identity"] == before["target_identity"]
    assert after["observed_at"] > before["observed_at"]
    diag = after["lifecycle"]["read_diagnostics"]
    assert diag["proc_scan_retry_count"] >= 1
    assert diag["events"]
    validate_lifecycle(after["lifecycle"], now=datetime.now(UTC), boot_id="boot-id")


def test_registered_child_reaped_between_enumeration_and_prune_is_retried(publisher: Any, record_property: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    before = running(owner)
    helper(owner, present=True)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary_exe = owner.proc_root / "124/exe"
    auxiliary_exe.write_bytes(b"auxiliary")

    class ReapedDuringSnapshot(FixtureRegistry):
        fired = False

        def snapshot_locked(self) -> dict[int, dict[str, Any]]:
            result = super().snapshot_locked()
            if not self.fired:
                self.fired = True
                auxiliary_exe.unlink()
                helper(owner, present=False)
            return result

    registry = ReapedDuringSnapshot(
        {
            123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
            124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
        }
    )
    owner.child_registry = registry
    after = owner.publish()
    diagnostics = after["lifecycle"]["read_diagnostics"]
    observed = {
        "triggered": registry.fired,
        "state": after["lifecycle"]["state"],
        "same_target": after["target_identity"] == before["target_identity"],
        "retries": diagnostics["proc_scan_retry_count"],
        "registered_disappearance": any(event["code"] == "RECOVERY_OWNER_REGISTERED_CHILD_DISAPPEARED" for event in diagnostics["events"]),
    }
    record_property("cra_evidence", json.dumps(observed))
    assert observed == {
        "triggered": True,
        "state": "RUNNING",
        "same_target": True,
        "retries": 1,
        "registered_disappearance": True,
    }


@pytest.mark.parametrize(
    "fault",
    [
        "permission",
        "io",
        "missing-managed",
        "managed-exit",
        "managed-pid-reuse",
        "owner-reuse",
        "second-managed",
        "auxiliary-exec",
        "continuous-churn",
        "deadline",
    ],
)
def test_retry_never_certifies_unknown_or_changed_identity(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, fault: str, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    helper(owner, present=True)
    original = owner._proc_stat
    fired = False

    def stat(pid: int) -> Any:
        nonlocal fired
        if (
            pid == 124
            and not fired
            and (
                fault != "auxiliary-exec"
                or (owner._read_stage == "child_stat_final" and owner._read_diagnostics["proc_scan_attempt_count"] == 2)
            )
        ):
            fired = True
            if fault in {"permission", "io"}:
                raise OSError(errno.EACCES if fault == "permission" else errno.EIO, "SECRET_MUST_NOT_LEAK")
            if fault == "missing-managed":
                (owner.proc_root / "123/stat").unlink()
            elif fault == "managed-exit":
                child(owner, process, absent=True)
            elif fault == "managed-pid-reuse":
                child(owner, process, absent=False, ticks=100)
            elif fault == "owner-reuse":
                path = owner.proc_root / str(os.getpid()) / "stat"
                path.write_text(path.read_text().replace("40 0", "41 0"))
            elif fault in {"second-managed", "auxiliary-exec"}:
                (owner.proc_root / "124/cmdline").write_bytes(b"ffmpeg\0rtmps://test.invalid/second\0")
                return original(pid)
            helper(owner, present=False)
        return original(pid)

    monkeypatch.setattr(owner, "_proc_stat", stat)
    if fault in {"continuous-churn", "deadline"}:
        original_scan = owner._scan_children

        def scan() -> list[int]:
            nonlocal fired
            fired = False
            helper(owner, present=True)
            return original_scan()

        monkeypatch.setattr(owner, "_scan_children", scan)
    if fault == "deadline":
        monkeypatch.setattr(module, "PROC_SCAN_SECONDS", 0.0)
    owner._next_cycle = 0
    owner.publish_if_due()
    result = json.loads(owner.path.read_text())
    record_property(
        "cra_evidence",
        json.dumps(
            {
                "triggered": fired,
                "state": result["lifecycle"]["state"],
                "target": result["target_identity"],
                "attempts": result["lifecycle"]["read_diagnostics"]["proc_scan_attempt_count"],
                "diagnostic_count": len(result["lifecycle"]["read_diagnostics"]["events"]),
            }
        ),
    )
    assert fired
    assert result["lifecycle"]["state"] != "RUNNING", result
    assert result["target_identity"] is None
    assert all(v is None for v in result["activation"].values())
    assert result["effects"]["integrity"] == "ok"
    assert result["lifecycle"]["read_diagnostics"]["events"]
    assert "SECRET_MUST_NOT_LEAK" not in owner.path.read_text()
    validate_lifecycle(result["lifecycle"], now=datetime.now(UTC), boot_id="boot-id")
    if result["lifecycle"]["state"] == "UNKNOWN":
        assert owner.last_error_code == "RECOVERY_OWNER_LIFECYCLE_UNKNOWN"
    if fault in {"permission", "io", "missing-managed"}:
        assert result["lifecycle"]["read_diagnostics"]["proc_scan_retry_count"] == 0
    if fault == "deadline":
        # Two independent contexts per snapshot; neither may retry after expiry.
        assert result["lifecycle"]["read_diagnostics"]["proc_scan_attempt_count"] <= 2


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "extra",
        "type",
        "schema",
        "diagnostic-schema",
        "stage",
        "code",
        "errno",
        "attempts",
        "retries",
        "events",
        "process-fields",
        "process-pid",
        "process-relation",
        "process-digest",
        "secret",
    ],
)
def test_diagnostics_are_strictly_admitted(publisher: Any, mutation: str, record_property: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    life = copy.deepcopy(running(owner)["lifecycle"])
    diag = life["read_diagnostics"]
    event = {"stage": "child_stat", "code": "PERMISSION_DENIED", "errno": 13, "process": None}
    diag["events"] = [event]
    if mutation == "missing":
        del life["read_diagnostics"]
    elif mutation == "extra":
        diag["extra"] = 1
    elif mutation == "type":
        life["read_diagnostics"] = []
    elif mutation == "schema":
        life["schema"] = []
    elif mutation == "diagnostic-schema":
        diag["schema"] = "runtime.owner_read_diagnostics.v1"
    elif mutation == "stage":
        event["stage"] = "secret-path"
    elif mutation == "code":
        event["code"] = []
    elif mutation == "errno":
        event["errno"] = True
    elif mutation == "attempts":
        diag["proc_scan_attempt_count"] = 7
    elif mutation == "retries":
        diag["proc_scan_retry_count"] = 5
    elif mutation == "events":
        diag["events"] *= 17
    elif mutation.startswith("process-"):
        event["process"] = {
            "pid": 124,
            "state": "S",
            "parent_pid": os.getpid(),
            "start_ticks": 100,
            "command_bytes": 0,
            "anchor_relation": "AUXILIARY",
            "executable_relation": "DIFFERENT",
            "anchor_target_sha256": "a" * 64,
            "owner_state_relation": "MATCHED",
        }
        if mutation == "process-fields":
            event["process"]["command"] = "SECRET_MUST_NOT_LEAK"
        elif mutation == "process-pid":
            event["process"]["pid"] = True
        elif mutation == "process-relation":
            event["process"]["anchor_relation"] = "TRUST_ME"
        else:
            event["process"]["anchor_target_sha256"] = "not-a-digest"
    else:
        event["exception"] = "SECRET_MUST_NOT_LEAK"
    with pytest.raises(ValueError) as caught:
        validate_lifecycle(life, now=datetime.now(UTC), boot_id="boot-id")
    record_property("cra_evidence", json.dumps({"exception_type": caught.type.__name__}))


def test_historical_lifecycle_v1_remains_readable(publisher: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    life = running(owner)["lifecycle"]
    life["schema"] = "runtime.child_lifecycle.v1"
    del life["read_diagnostics"]
    assert validate_lifecycle(life, now=datetime.now(UTC), boot_id="boot-id") == life


@pytest.mark.parametrize("seed", [0, 1, 7, 19, 31, 42, 99, 20260907])
def test_seeded_normal_and_fault_interleavings(publisher: Any, monkeypatch: pytest.MonkeyPatch, seed: int) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    randomizer = random.Random(seed)
    original = owner._proc_stat
    changes = owner.ledger.connection.total_changes
    for _ in range(32):
        helper(owner, present=True)
        fault = randomizer.choice(["cleanup", "stable", "denied", "activation-off"])
        point = randomizer.choice(["child_stat", "child_stat_final"])
        fired = False

        def stat(pid: int, *, point: str = point, fault: str = fault) -> Any:
            nonlocal fired
            if pid == 124 and not fired and owner._read_stage == point:
                fired = True
                if fault == "cleanup":
                    helper(owner, present=False)
                elif fault == "denied":
                    raise PermissionError(errno.EACCES, "test-owned")
            return original(pid)

        (owner.proc_root / "123/environ").write_bytes(
            b"FR_FFMPEG_FORCE_KILL_ENABLED=1\0FFMPEG_RW_TIMEOUT_ENABLED=" + (b"0" if fault == "activation-off" else b"1") + b"\0"
        )
        monkeypatch.setattr(owner, "_proc_stat", stat)
        result = owner.publish()
        assert result["lifecycle"]["state"] == ("UNKNOWN" if fault == "denied" else "RUNNING"), (seed, fault, result)
        if fault == "activation-off":
            assert result["activation"]["rw_timeout_enabled"] is False
        if fault == "denied":
            assert all(v is None for v in result["activation"].values())
        assert result["effects"]["physical_attempt_count"] == 0
        assert owner.ledger.connection.total_changes == changes
        validate_lifecycle(result["lifecycle"], now=datetime.now(UTC), boot_id="boot-id")


@pytest.mark.parametrize("kind", ["helper-exit", "thread-exit"])
def test_real_kernel_normal_cleanup_has_no_evidence_gap(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any, kind: str
) -> None:
    owner, process, target = publisher
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v3"
    managed = subprocess.Popen(
        ["ffmpeg", "-c", "import sys; sys.stdin.read(1)", "-rw_timeout", "15000000", "rtmps://test.invalid/unused"],
        executable=sys.executable,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FFMPEG_RW_TIMEOUT_ENABLED": "1"},
    )
    auxiliary = None
    release = threading.Event()
    thread = None
    try:
        wait_for_proc_command(managed)
        owner.proc_root = Path("/proc")
        _, _, ticks = owner._proc_stat(managed.pid)
        target.update(ffmpeg_pid=managed.pid, host_boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
        target["ffmpeg_generation"] = (
            "ffmpeg-" + hashlib.sha256(f"{target['pod_uid']}:{target['container_id']}:{managed.pid}:{ticks}".encode()).hexdigest()[:32]
        )
        raw = json.loads(owner.target_path.read_text())
        raw["target_identity"] = target
        owner.target_path.write_text(json.dumps(raw))
        process.update(local_ffmpeg_pid=managed.pid, managed_child_pids=[managed.pid], runtime_lifecycle_state="FFMPEG_RUNNING")
        before = running(owner)
        original_stat, original_iterdir = owner._proc_stat, Path.iterdir
        tasks = Path("/proc") / str(os.getpid()) / "task"
        durations = []
        for _ in range(16):
            fired = False
            release.clear()
            if kind == "helper-exit":
                auxiliary = subprocess.Popen(
                    ["/bin/cat"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                wait_for_proc_command(auxiliary)
            else:
                thread = threading.Thread(target=lambda: release.wait(timeout=3))
                thread.start()

            def stat(pid: int, *, auxiliary: Any = auxiliary) -> Any:
                nonlocal fired
                if not fired and auxiliary is not None and pid == auxiliary.pid:
                    fired = True
                    assert auxiliary.stdin is not None
                    auxiliary.stdin.close()
                    auxiliary.wait(timeout=2)
                return original_stat(pid)

            def iterdir(path: Path, *, thread: Any = thread) -> Any:
                nonlocal fired
                if not fired and thread is not None and path == tasks and owner._read_stage == "task_list_final":
                    fired = True
                    release.set()
                    thread.join(timeout=2)
                    assert not thread.is_alive()
                    # Python join may finish before the kernel removes the task.
                    # Prove the injected task-list change before expecting a retry.
                    task_path = tasks / str(thread.native_id)
                    deadline = time.monotonic() + 0.05
                    while task_path.exists() and time.monotonic() < deadline:
                        time.sleep(0.0005)
                    assert not task_path.exists(), "test-owned thread remains in procfs"
                return original_iterdir(path)

            monkeypatch.setattr(owner, "_proc_stat", stat)
            monkeypatch.setattr(Path, "iterdir", iterdir)
            started = time.monotonic()
            after = running(owner)
            durations.append(time.monotonic() - started)
            assert fired and after["lifecycle"]["read_diagnostics"]["proc_scan_retry_count"] >= 1
            assert after["target_identity"] == before["target_identity"]
            assert managed.poll() is None
        record_property("native_cycles", len(durations))
        record_property("maximum_cycle_seconds", max(durations))
        record_property("cra_evidence", json.dumps({"native_cycles": len(durations), "maximum_cycle_seconds": max(durations)}))
        assert max(durations) < module.QUERY_SECONDS
    finally:
        release.set()
        if thread is not None:
            thread.join(timeout=2)
        for proc in (auxiliary, managed):
            if proc is not None:
                if proc.stdin is not None and not proc.stdin.closed:
                    proc.stdin.close()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()  # Only this test's child; never a discovered PID.
                    proc.wait(timeout=2)


@pytest.mark.parametrize("failure", ["disk-full", "permission", "sqlite"])
def test_failed_export_logs_bounded_diagnostic_without_secrets(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, failure: str
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    previous = owner.path.read_bytes()
    if failure == "sqlite":
        import sqlite3

        def fail_query(*args: Any, **kwargs: Any) -> Any:
            raise sqlite3.OperationalError("SECRET_SQL_MUST_NOT_LEAK")

        monkeypatch.setattr(module, "owner_evidence", fail_query)
    else:

        def fail_write(*args: Any, **kwargs: Any) -> None:
            raise OSError(errno.ENOSPC if failure == "disk-full" else errno.EACCES, "SECRET_PATH_MUST_NOT_LEAK")

        monkeypatch.setattr(module, "write_export", fail_write)
    owner._next_cycle = 0
    owner.publish_if_due()
    assert owner.path.read_bytes() == previous
    assert owner.last_error_code == "RECOVERY_OWNER_EXPORT_UNAVAILABLE"
    assert "RECOVERY_OWNER_EXPORT_UNAVAILABLE" in caplog.text
    assert ("SQL_READ_FAILED" if failure == "sqlite" else "export_write") in caplog.text
    assert "SECRET_" not in caplog.text


@pytest.mark.parametrize(
    "fault", ["cleanup", "zombie-cleanup", "live-empty", "permission", "transport-null", "invalid-diagnostic", "tampered-signature"]
)
def test_signed_cleanup_and_unknown_retained_through_collector(
    publisher: Any, setup: Any, monkeypatch: pytest.MonkeyPatch, fault: str, record_property: Any
) -> None:
    owner, process, _ = publisher
    config, signers = setup
    configure(owner, process)
    config.value.update(require_independent_effect_evidence=True, require_owner_lifecycle_evidence=True)
    owner.config.update(host_id=config.bindings["dell"].host_id, stream_id=config.bindings["dell"].stream_id)
    clock = [START]

    class Clock:
        @staticmethod
        def now(tz: Any = None) -> datetime:
            return clock[0]

    monkeypatch.setattr(module, "datetime", Clock)
    original = owner._proc_stat
    for seconds in range(0, 181, 15):
        clock[0] = START + timedelta(seconds=seconds)
        target = json.loads(owner.target_path.read_text())
        target.update(observed_at=clock[0].isoformat(), valid_until=(clock[0] + timedelta(seconds=45)).isoformat())
        owner.target_path.write_text(json.dumps(target))
        if seconds == 45:
            helper(owner, present=True)
            fired = False

            def stat(pid: int) -> Any:
                nonlocal fired
                if pid == 124 and not fired:
                    fired = True
                    if fault == "permission":
                        raise PermissionError(errno.EACCES, "test-owned")
                    if fault in {"zombie-cleanup", "live-empty"}:
                        before = original(pid)
                        (owner.proc_root / "124/cmdline").write_bytes(b"")
                        if fault == "zombie-cleanup":
                            stat_path = owner.proc_root / "124/stat"
                            stat_path.write_text(stat_path.read_text().replace(") S ", ") Z "))
                        return before
                    helper(owner, present=False)
                return original(pid)

            monkeypatch.setattr(owner, "_proc_stat", stat)
        else:
            monkeypatch.setattr(owner, "_proc_stat", original)
            if seconds == 60 and fault in {"zombie-cleanup", "live-empty"}:
                helper(owner, present=False)
        raw = owner.publish()
        effects, activation = runtime_evidence_fact(
            raw,
            target={},
            binding=config.bindings["dell"],
            runtime_binding=binding(owner),
            now=clock[0],
            history_policy=empty_policy(),
            physical_boot_id="boot-id",
        )
        inputs = {role: packet(config, signers, role, seconds) for role in config.bindings}
        facts = copy.deepcopy(inputs["dell"]["facts"])
        facts.update(effects=effects, activation=activation)
        facts["transport"].update(
            target=raw["target_sha256"],
            bytes_acked=1000 + seconds * 100 if raw["target_identity"] else None,
            lifecycle=copy.deepcopy(raw["lifecycle"]),
        )
        if seconds == 45 and fault == "transport-null":
            facts["transport"] = None
        if seconds == 45 and fault == "invalid-diagnostic":
            facts["transport"]["lifecycle"]["read_diagnostics"]["proc_scan_retry_count"] = True
        inputs["dell"] = sign_facts(
            binding=config.bindings["dell"],
            signer=signers["dell"],
            host_boot_id="boot-id",
            producer_id="owner-fixture",
            sequence=seconds + 1,
            now=clock[0],
            valid_until=clock[0] + timedelta(seconds=45),
            facts=facts,
            source_hashes={"runtime_evidence": source_hash(raw)},
        )
        if seconds == 45 and fault == "tampered-signature":
            inputs["dell"]["facts"]["transport"]["lifecycle"]["read_diagnostics"]["events"] = []
        for role, value in inputs.items():
            inbox = Path(config.value["hosts"][role]["inbox_file"])
            inbox.write_text(json.dumps(value))
            inbox.chmod(0o644)
        recovery_soak.collect(config, now=clock[0])
    rows = [json.loads(line) for line in Path(config.value["evidence_file"]).read_text().splitlines()]
    result = recovery_soak.evaluate(rows, config=config, now=clock[0])
    operator = result["operator_status"]
    record_property(
        "cra_evidence",
        json.dumps(
            {
                "physical_delta": result["physical_attempt_delta"],
                "eligible": result["eligible"],
                "unknown_count": len(result["unknown_reasons"]),
                "live_health": result["live_health"],
                "operator_sut_state": operator["current_sut_state"],
                "operator_failure_domain": operator["failure_domain"],
                "operator_soak_impact": operator["soak_impact"],
            }
        ),
    )
    assert "UNKNOWN" not in json.dumps(operator, sort_keys=True)
    assert result["physical_attempt_delta"] == 0
    assert result["oracle_errors"] == []
    assert result["live_health"] == "READY"
    assert result["eligible"] is False
    assert result["recovery"]["episode_count"] == 0
    if fault in {"cleanup", "zombie-cleanup"}:
        assert result["harness_classification"] == "PASS", result
        assert result["unknown_reasons"] == result["blockers"] == []
        assert result["owner_read_retry_sample_count"] == 1
        assert result["owner_read_diagnostic_sample_count"] == 1
        assert result["owner_lifecycle_sample_counts"]["RUNNING"] == len(rows)
        assert operator["current_sut_state"] == "VERIFIED_HEALTHY"
        assert operator["failure_domain"] == "NONE"
        assert operator["soak_impact"] == "CERTIFICATION_PENDING"
    else:
        assert result["harness_classification"] != "PASS", result
        assert result["unknown_reasons"]
        assert operator["current_sut_state"] == "NOT_OBSERVABLE"
        assert operator["failure_domain"] == "OBSERVATION_SOURCE"
        assert operator["soak_impact"] == "CERTIFICATION_PAUSED"
    if fault in {"permission", "live-empty"}:
        assert result["activation_unknown_sample_count"] == 0  # Historical target-bound metric.
        assert result["activation_unavailable_sample_count"] == 1
        assert result["owner_lifecycle_sample_counts"]["UNKNOWN"] == 1
        assert result["owner_read_diagnostic_sample_count"] == 1


def test_auxiliary_exit_does_not_invalidate_running_child(publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any) -> None:
    """Same behavioral probe also runs against the unmodified deployed source."""
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    helper(owner, present=True)
    original = owner._proc_stat
    fired = False

    def stat(pid: int) -> Any:
        nonlocal fired
        if pid == 124 and not fired:
            fired = True
            helper(owner, present=False)
        return original(pid)

    monkeypatch.setattr(owner, "_proc_stat", stat)
    after = running(owner)
    record_property(
        "cra_evidence",
        json.dumps(
            {
                "triggered": fired,
                "state": after["lifecycle"]["state"],
                "retries": after["lifecycle"]["read_diagnostics"]["proc_scan_retry_count"],
                "physical_attempts": after["effects"]["physical_attempt_count"],
            }
        ),
    )
    assert fired
    assert after["lifecycle"]["state"] == "RUNNING"


@pytest.mark.parametrize("context", [1, 2])
@pytest.mark.parametrize("stage", ["child_command", "child_command_final"])
@pytest.mark.parametrize("victim", ["auxiliary", "managed"])
def test_real_kernel_exit_after_cmdline_open(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any, context: int, stage: str, victim: str
) -> None:
    """Retire a test-owned process after open; the kernel, not a mock, raises ESRCH."""
    owner, process, target = publisher
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v3"
    managed = subprocess.Popen(
        ["ffmpeg", "-c", "import sys; sys.stdin.read(1)", "-rw_timeout", "15000000", "rtmps://test.invalid/unused"],
        executable=sys.executable,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FFMPEG_RW_TIMEOUT_ENABLED": "1"},
    )
    auxiliary = None
    errors: list[int | None] = []
    durations = []
    original_open = Path.open
    try:
        wait_for_proc_command(managed)
        owner.proc_root = Path("/proc")
        _, _, ticks = owner._proc_stat(managed.pid)
        target.update(ffmpeg_pid=managed.pid, host_boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
        target["ffmpeg_generation"] = (
            "ffmpeg-" + hashlib.sha256(f"{target['pod_uid']}:{target['container_id']}:{managed.pid}:{ticks}".encode()).hexdigest()[:32]
        )
        raw = json.loads(owner.target_path.read_text())
        raw["target_identity"] = target
        owner.target_path.write_text(json.dumps(raw))
        process.update(local_ffmpeg_pid=managed.pid, managed_child_pids=[managed.pid], runtime_lifecycle_state="FFMPEG_RUNNING")
        before = running(owner)
        for _ in range(8 if victim == "auxiliary" else 1):
            fired = False
            if victim == "auxiliary":
                auxiliary = subprocess.Popen(["/bin/cat"], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                wait_for_proc_command(auxiliary)
            retired = auxiliary if victim == "auxiliary" else managed
            assert retired is not None

            class OpenedProcFile:
                def __init__(self, source: Any, retired: Any) -> None:
                    self.source = source
                    self.retired = retired

                def __enter__(self) -> Any:
                    return self

                def __exit__(self, *args: Any) -> None:
                    self.source.close()

                def read(self, size: int) -> bytes:
                    assert self.retired.stdin is not None
                    self.retired.stdin.close()
                    self.retired.wait(timeout=2)
                    try:
                        return self.source.read(size)
                    except OSError as error:
                        errors.append(error.errno)
                        raise

            def opened(path: Path, *args: Any, retired: Any = retired, **kwargs: Any) -> Any:
                nonlocal fired
                source = original_open(path, *args, **kwargs)
                if (
                    not fired
                    and path == Path("/proc") / str(retired.pid) / "cmdline"
                    and owner._read_stage == stage
                    and owner._read_diagnostics["proc_scan_attempt_count"] == context
                ):
                    fired = True
                    return OpenedProcFile(source, retired)
                return source

            monkeypatch.setattr(Path, "open", opened)
            started = time.monotonic()
            after = owner.publish()
            durations.append(time.monotonic() - started)
            diag = after["lifecycle"]["read_diagnostics"]
            observed = {
                "triggered": fired,
                "kernel_errno": errors[-1] if errors else None,
                "state": after["lifecycle"]["state"],
                "target": after["target_identity"],
                "same_target": after["target_identity"] == before["target_identity"],
                "retries": diag["proc_scan_retry_count"],
                "physical_attempts": after["effects"]["physical_attempt_count"],
                "native_cycles": len(durations),
                "maximum_cycle_seconds": max(durations),
            }
            assert fired and errors[-1] == errno.ESRCH, observed
            assert observed["physical_attempts"] == 0 and after["effects"]["integrity"] == "ok"
            assert any(event["errno"] == errno.ESRCH for event in diag["events"])
            if victim == "auxiliary":
                assert observed["state"] == "RUNNING", observed
                assert observed["same_target"] and observed["retries"] >= 1
                assert after["observed_at"] > before["observed_at"]
                assert all(value is True for value in after["activation"].values())
                assert managed.poll() is None
            else:
                assert observed["state"] != "RUNNING", observed
                assert observed["target"] is None and observed["retries"] == 0
                assert all(value is None for value in after["activation"].values())
            validate_lifecycle(after["lifecycle"], now=datetime.now(UTC), boot_id=target["host_boot_id"])
        record_property("cra_evidence", json.dumps(observed))
        assert max(durations) < module.QUERY_SECONDS
    finally:
        for proc in (auxiliary, managed):
            if proc is not None:
                if proc.stdin is not None and not proc.stdin.closed:
                    proc.stdin.close()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()  # Only a child created above.
                    proc.wait(timeout=2)


@pytest.mark.parametrize("stage", ["child_command", "child_command_final"])
@pytest.mark.parametrize("fault", ["permission", "io", "no-anchor", "managed-exit", "owner-reuse"])
def test_esrch_retry_requires_live_anchor(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any, stage: str, fault: str
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    helper(owner, present=True)
    if fault == "no-anchor":
        owner._child_anchor = None
    original_open = Path.open
    fired = False

    def opened(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        if not fired and path == owner.proc_root / "124/cmdline" and owner._read_stage == stage:
            fired = True
            if fault == "managed-exit":
                child(owner, process, absent=True)
            elif fault == "owner-reuse":
                owner_stat = owner.proc_root / str(os.getpid()) / "stat"
                owner_stat.write_text(owner_stat.read_text().replace("40 0", "41 0"))
            helper(owner, present=False)
            raise OSError({"permission": errno.EACCES, "io": errno.EIO}.get(fault, errno.ESRCH), "test-owned")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", opened)
    after = owner.publish()
    observed = {
        "triggered": fired,
        "state": after["lifecycle"]["state"],
        "target": after["target_identity"],
        "retries": after["lifecycle"]["read_diagnostics"]["proc_scan_retry_count"],
        "physical_attempts": after["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert fired and observed["state"] != "RUNNING", observed
    assert observed["target"] is None and observed["retries"] == observed["physical_attempts"] == 0
    assert all(value is None for value in after["activation"].values())
