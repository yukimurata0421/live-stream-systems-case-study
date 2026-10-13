"""Owner kernel identity -> original signature -> collector -> formal replay.

All kernel files, database rows, keys and clocks belong to the test fixture.
"""

from __future__ import annotations

import copy
import errno
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from cra_dell_recovery.recovery_history import empty_policy
from cra_no_action_soak import recovery_observer, recovery_soak
from cra_no_action_soak.recovery_facts import owner_target_snapshot, runtime_evidence_fact, sign_facts, source_hash
from runtime_boundary import recovery_publisher as owner_module
from runtime_boundary.child_registry import ChildLifecycleRegistry
from tests.harness.unit.test_recovery_soak import START, packet
from tests.harness.unit.test_recovery_soak import setup as setup
from tests.runtime_boundary.test_recovery_publisher import publisher as publisher


@pytest.mark.parametrize("fault", ["none", "digest", "pipe", "socket", "pid", "parent", "executable", "owner", "unbound", "unregistered"])
def test_pipe_encoder_requires_all_independent_bindings(publisher: Any, fault: str, record_property: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    producer = owner.proc_root / "124"
    (producer / "fd").mkdir(parents=True)
    (producer / "net").mkdir()
    executable = producer / "exe"
    executable.write_bytes(b"encoder executable")
    (producer / "stat").write_text(f"124 (ffmpeg) S {os.getpid()} " + "0 " * 17 + "100 0\n")
    command = b"ffmpeg\0-f\0mpegts\0pipe:1\0"
    (producer / "cmdline").write_bytes(command)
    fifo = owner.proc_root / "encoder-fifo"
    os.mkfifo(fifo, 0o600)
    (producer / "fd/1").symlink_to(fifo)
    for table in ("tcp", "tcp6", "udp", "udp6", "raw", "raw6"):
        (producer / "net" / table).write_text("header\n")
    registry = ChildLifecycleRegistry(proc_root=owner.proc_root)
    encoder = registry_entry(owner, 124, role="ENCODER_PIPE", ticks=100, executable=executable)
    encoder.update(encoder_command_sha256=hashlib.sha256(command).hexdigest(), encoder_pipe_inode=fifo.stat().st_ino)
    registry._entries = {123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe), 124: encoder}
    owner.child_registry = registry
    process["local_encoder_pid"] = 124
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    if fault == "digest":
        (producer / "cmdline").write_bytes(command + b"changed\0")
    elif fault == "pipe":
        encoder["encoder_pipe_inode"] += 1
    elif fault == "socket":
        (producer / "fd/9").symlink_to("socket:[900]")
        (producer / "net/tcp").write_text("header\n0 0 0 0 0 0 0 0 0 900\n")
    elif fault == "pid":
        encoder["start_ticks"] += 1
    elif fault == "parent":
        encoder["parent_pid"] += 1
    elif fault == "executable":
        encoder["executable_inode"] += 1
    elif fault == "owner":
        encoder["owner_start_ticks"] += 1
    elif fault == "unbound":
        process["local_encoder_pid"] = 125
    elif fault == "unregistered":
        registry._entries.pop(124)
    result = owner.publish()
    observed = {"state": result["lifecycle"]["state"], "target_present": result["target_identity"] is not None,
                "physical_attempts": result["effects"]["physical_attempt_count"]}
    record_property("cra_evidence", json.dumps(observed))
    assert observed == {"state": "RUNNING" if fault == "none" else "UNKNOWN",
                        "target_present": fault == "none", "physical_attempts": 0}


def configure(owner: Any, process: dict[str, Any]) -> None:
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v3"
    root = owner.proc_root / str(os.getpid())
    root.mkdir(exist_ok=True)
    (root / "stat").write_text(f"{os.getpid()} (engine) S 1 " + "0 " * 17 + "40 0\n")
    (root / "task" / str(os.getpid())).mkdir(parents=True)
    (root / "task" / str(os.getpid()) / "children").write_text("123")
    process["runtime_lifecycle_state"] = "FFMPEG_RUNNING"
    with (owner.proc_root / "123/cmdline").open("ab") as output:
        output.write(b"rtmps://test.invalid/live\0")


def child(owner: Any, process: dict[str, Any], *, absent: bool, ticks: int = 99) -> None:
    (owner.proc_root / "123/stat").write_text(f"123 (ffmpeg) {'Z' if absent else 'S'} {os.getpid()} " + "0 " * 17 + f"{ticks} 0\n")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("" if absent else "123")
    process.update(
        local_ffmpeg_pid=0 if absent else 123,
        ffmpeg_running=not absent,
        managed_child_pids=[] if absent else [123],
        managed_child_cardinality="0" if absent else "1",
        runtime_lifecycle_state="RESTART_DELAY" if absent else "FFMPEG_RUNNING",
    )


def binding(owner: Any) -> dict[str, str]:
    return {k: owner.config[k] for k in ("release_id", "source_commit", "target_host_id", "evidence_schema")}


def wait_for_proc_command(process: subprocess.Popen[Any], *, timeout: float = 2.0) -> bytes:
    """Prove that a test-owned child completed exec before observing procfs."""
    deadline = time.monotonic() + timeout
    command_path = Path("/proc") / str(process.pid) / "cmdline"
    while True:
        assert process.poll() is None, "test-owned child exited before procfs command became readable"
        try:
            command = command_path.read_bytes()
        except FileNotFoundError:
            command = b""
        if command:
            return command
        assert time.monotonic() < deadline, "test-owned child command did not become readable"
        time.sleep(0.001)


class FixtureRegistry:
    def __init__(self, entries: dict[int, dict[str, Any]]) -> None:
        self.entries = entries

    @contextmanager
    def guard(self) -> Any:
        yield

    def snapshot_locked(self) -> dict[int, dict[str, Any]]:
        return copy.deepcopy(self.entries)


def registry_entry(owner: Any, pid: int, *, role: str, ticks: int, executable: Path) -> dict[str, Any]:
    meta = executable.stat()
    return {
        "pid": pid,
        "parent_pid": os.getpid(),
        "start_ticks": ticks,
        "owner_pid": os.getpid(),
        "owner_start_ticks": 40,
        "role": role,
        "executable_device": meta.st_dev,
        "executable_inode": meta.st_ino,
        "identity_source": "PROC_EXE",
        "registered_post_exec": True,
    }


def bind_real_managed_child(owner: Any, process: dict[str, Any], target: dict[str, Any], managed: Any) -> None:
    """Bind one test-owned kernel child to the owner without touching a live process."""
    wait_for_proc_command(managed)
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v3"
    owner.proc_root = Path("/proc")
    _, _, ticks = owner._proc_stat(managed.pid)
    target.update(
        ffmpeg_pid=managed.pid,
        host_boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
    )
    target["ffmpeg_generation"] = (
        "ffmpeg-"
        + hashlib.sha256(
            f"{target['pod_uid']}:{target['container_id']}:{managed.pid}:{ticks}".encode()
        ).hexdigest()[:32]
    )
    raw = json.loads(owner.target_path.read_text())
    raw["target_identity"] = target
    owner.target_path.write_text(json.dumps(raw))
    process.update(
        local_ffmpeg_pid=managed.pid,
        ffmpeg_running=True,
        managed_child_pids=[managed.pid],
        managed_child_cardinality="1",
        runtime_lifecycle_state="FFMPEG_RUNNING",
    )


@pytest.mark.parametrize("runtime", [None, [], "invalid", True, 1])
def test_observer_rejects_invalid_runtime_binding_type(setup: Any, tmp_path: Path, runtime: Any) -> None:
    config, _ = setup
    value = {
        "schema": recovery_observer.CONFIG_SCHEMA,
        "binding": config.bindings["dell"].__dict__,
        "sources": {name: str(tmp_path / name) for name in recovery_observer.SOURCE_NAMES["dell"]},
        "runtime_binding": runtime,
        "effect_history_policy": empty_policy(),
        **{name: str(tmp_path / name) for name in ["state_file", "output_file", "signing_private_key_file", "host_contract_file"]},
    }
    with pytest.raises(ValueError):
        recovery_observer.validate_config(value)
    assert not Path(value["state_file"]).exists()


def test_live_child_revalidation_does_not_cache_activation_or_depend_on_async_target(publisher: Any, record_property: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    changes = owner.ledger.connection.total_changes
    before = owner.publish()
    assert before["lifecycle"]["state"] == "RUNNING"
    owner.target_path.unlink()
    after = owner.publish()
    assert after["target_identity"] == before["target_identity"]
    assert after["observed_at"] > before["observed_at"]
    assert all(after["activation"].values())
    (owner.proc_root / "123/environ").write_bytes(b"FR_FFMPEG_FORCE_KILL_ENABLED=0\0FFMPEG_RW_TIMEOUT_ENABLED=0\0")
    disabled = owner.publish()
    record_property("cra_evidence", json.dumps({
        **disabled["activation"], "ledger_changes": owner.ledger.connection.total_changes - changes,
    }))
    assert disabled["lifecycle"]["state"] == "RUNNING"
    assert disabled["activation"] == {"bounded_termination_enabled": False, "rw_timeout_enabled": False}
    assert owner.ledger.connection.total_changes == changes


def test_real_kernel_child_activation_is_reread_after_async_target_loss(
    publisher: Any, record_property: Any
) -> None:
    owner, process, target = publisher
    mutator = r'''import ctypes,sys
libc=ctypes.CDLL(None)
environment=ctypes.POINTER(ctypes.c_void_p).in_dll(libc,"environ")
print("READY",flush=True)
sys.stdin.buffer.read(1)
index=0
while environment[index]:
    value=ctypes.string_at(environment[index])
    if value in (b"FR_FFMPEG_FORCE_KILL_ENABLED=1",b"FFMPEG_RW_TIMEOUT_ENABLED=1"):
        ctypes.memmove(environment[index]+len(value)-1,b"0",1)
    index+=1
print("CHANGED",flush=True)
sys.stdin.buffer.read(1)
'''
    managed = subprocess.Popen(
        ["ffmpeg", "-c", mutator, "-rw_timeout", "15000000", "rtmps://test.invalid/unused"],
        executable=sys.executable,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env={"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FFMPEG_RW_TIMEOUT_ENABLED": "1"},
    )
    try:
        assert managed.stdout is not None
        assert managed.stdout.readline() == b"READY\n"
        bind_real_managed_child(owner, process, target, managed)
        changes = owner.ledger.connection.total_changes
        before = owner.publish()
        assert before["lifecycle"]["state"] == "RUNNING"
        assert all(before["activation"].values())
        owner.target_path.unlink()
        assert managed.stdin is not None
        managed.stdin.write(b"0")
        managed.stdin.flush()
        assert managed.stdout.readline() == b"CHANGED\n"
        deadline = time.monotonic() + 2
        environment = Path(f"/proc/{managed.pid}/environ")
        while b"ENABLED=0" not in environment.read_bytes():
            assert time.monotonic() < deadline
            time.sleep(0.001)

        disabled = owner.publish()
        observed = {
            **disabled["activation"],
            "ledger_changes": owner.ledger.connection.total_changes - changes,
            "physical_attempts": disabled["effects"]["physical_attempt_count"],
            "same_target": disabled["target_identity"] == before["target_identity"],
            "state": disabled["lifecycle"]["state"],
        }
        record_property("cra_evidence", json.dumps(observed))
        assert observed == {
            "bounded_termination_enabled": False,
            "rw_timeout_enabled": False,
            "ledger_changes": 0,
            "physical_attempts": 0,
            "same_target": True,
            "state": "RUNNING",
        }
    finally:
        if managed.stdin is not None and not managed.stdin.closed:
            managed.stdin.close()
        try:
            managed.wait(timeout=2)
        except subprocess.TimeoutExpired:
            managed.kill()
            managed.wait(timeout=2)


@pytest.mark.parametrize("auxiliary_state", ["R", "Z"])
def test_registered_empty_cmdline_auxiliary_does_not_hide_exact_delivery_child(
    publisher: Any, auxiliary_state: str, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary = owner.proc_root / "124"
    auxiliary.mkdir()
    auxiliary_exe = auxiliary / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    (auxiliary / "stat").write_text(
        f"124 (helper) {auxiliary_state} {os.getpid()} " + "0 " * 17 + "100 0\n"
    )
    (auxiliary / "cmdline").write_bytes(b"")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    owner.child_registry = FixtureRegistry(
        {
            123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
            124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
        }
    )

    result = owner.publish()
    record_property(
        "cra_evidence",
        json.dumps(
            {
                "state": result["lifecycle"]["state"],
                "target_present": result["target_identity"] is not None,
                "physical_attempts": result["effects"]["physical_attempt_count"],
            }
        ),
    )

    assert result["lifecycle"]["state"] == "RUNNING"
    assert result["lifecycle"]["reason"] == "EXACT_CHILD"
    assert result["target_identity"] is not None


@pytest.mark.parametrize("fault", ["unregistered", "pid-reuse", "exec-to-delivery", "delivery-unbound"])
def test_registry_ambiguity_remains_unknown(publisher: Any, fault: str, record_property: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary = owner.proc_root / "124"
    auxiliary.mkdir()
    auxiliary_exe = auxiliary / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    (auxiliary / "stat").write_text(f"124 (helper) S {os.getpid()} " + "0 " * 17 + "100 0\n")
    (auxiliary / "cmdline").write_bytes(b"")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    entries = {
        123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
        124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
    }
    if fault == "unregistered":
        del entries[124]
    elif fault == "pid-reuse":
        entries[124]["start_ticks"] = 99
    elif fault == "exec-to-delivery":
        auxiliary_exe.unlink()
        os.link(managed_exe, auxiliary_exe)
    else:
        entries[124]["role"] = "DELIVERY_CANDIDATE"
    owner.child_registry = FixtureRegistry(entries)

    result = owner.publish()
    record_property(
        "cra_evidence",
        json.dumps(
            {
                "state": result["lifecycle"]["state"],
                "target": result["target_identity"],
                "physical_attempts": result["effects"]["physical_attempt_count"],
                "diagnostic_count": len(result["lifecycle"]["read_diagnostics"]["events"]),
            }
        ),
    )

    assert result["lifecycle"]["state"] == "UNKNOWN"
    assert result["target_identity"] is None
    assert result["activation"] == {"bounded_termination_enabled": None, "rw_timeout_enabled": None}


def test_registered_auxiliary_launcher_exec_drift_remains_non_delivery(
    publisher: Any, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary = owner.proc_root / "124"
    auxiliary.mkdir()
    auxiliary_exe = auxiliary / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    (auxiliary / "stat").write_text(f"124 (helper) S {os.getpid()} " + "0 " * 17 + "100 0\n")
    (auxiliary / "cmdline").write_bytes(b"")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    entries = {
        123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
        124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
    }
    entries[124]["executable_inode"] += 1
    owner.child_registry = FixtureRegistry(entries)

    result = owner.publish()
    record_property(
        "cra_evidence",
        json.dumps(
            {
                "state": result["lifecycle"]["state"],
                "target_present": result["target_identity"] is not None,
                "physical_attempts": result["effects"]["physical_attempt_count"],
            }
        ),
    )

    assert result["lifecycle"]["state"] == "RUNNING"
    assert result["lifecycle"]["reason"] == "EXACT_CHILD"
    assert result["target_identity"] is not None


def test_registered_auxiliary_scheduler_state_churn_keeps_exact_delivery_child(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    """R/S scheduling changes are not process or executable identity drift."""
    owner, process, _ = publisher
    configure(owner, process)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary = owner.proc_root / "124"
    auxiliary.mkdir()
    auxiliary_exe = auxiliary / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    (auxiliary / "stat").write_text(f"124 (helper) S {os.getpid()} " + "0 " * 17 + "100 0\n")
    (auxiliary / "cmdline").write_bytes(b"")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    owner.child_registry = FixtureRegistry(
        {
            123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
            124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
        }
    )
    original = owner._proc_stat
    auxiliary_reads = 0

    def scheduled(pid: int) -> tuple[str, int, int]:
        nonlocal auxiliary_reads
        state, parent, ticks = original(pid)
        if pid == 124:
            auxiliary_reads += 1
            state = "R" if auxiliary_reads % 2 else "S"
        return state, parent, ticks

    monkeypatch.setattr(owner, "_proc_stat", scheduled)
    result = owner.publish()
    diagnostics = result["lifecycle"]["read_diagnostics"]
    observed = {
        "scheduler_state_changed": auxiliary_reads >= 4,
        "state": result["lifecycle"]["state"],
        "target_present": result["target_identity"] is not None,
        "retries": diagnostics["proc_scan_retry_count"],
        "physical_attempts": result["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))

    assert observed == {
        "scheduler_state_changed": True,
        "state": "RUNNING",
        "target_present": True,
        "retries": 0,
        "physical_attempts": 0,
    }


@pytest.mark.parametrize(
    "missing_errno",
    [pytest.param(errno.ENOENT, id="enoent"), pytest.param(errno.ESRCH, id="esrch")],
)
def test_registered_auxiliary_executable_exit_is_retried_with_original_errno(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, missing_errno: int, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    before = owner.publish()
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    helper_root = owner.proc_root / "124"
    helper_root.mkdir()
    auxiliary_exe = helper_root / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    (helper_root / "stat").write_text(f"124 (helper) R {os.getpid()} " + "0 " * 17 + "100 0\n")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    owner.child_registry = FixtureRegistry(
        {
            123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
            124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
        }
    )
    original = owner._executable_identity
    fired = False

    def exit_before_executable_read(pid: int) -> tuple[int, int]:
        nonlocal fired
        if pid == 124 and not fired:
            fired = True
            auxiliary_exe.unlink()
            (helper_root / "stat").unlink()
            helper_root.rmdir()
            (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123")
            error_type = FileNotFoundError if missing_errno == errno.ENOENT else ProcessLookupError
            raise error_type(missing_errno, "test-owned executable disappeared")
        return original(pid)

    monkeypatch.setattr(owner, "_executable_identity", exit_before_executable_read)
    result = owner.publish()
    diagnostics = result["lifecycle"]["read_diagnostics"]
    observed = {
        "triggered": fired,
        "state": result["lifecycle"]["state"],
        "same_target": result["target_identity"] == before["target_identity"],
        "retries": diagnostics["proc_scan_retry_count"],
        "errno_preserved": any(event["errno"] == missing_errno for event in diagnostics["events"]),
        "registered_disappearance": any(
            event["code"] == "RECOVERY_OWNER_REGISTERED_CHILD_DISAPPEARED"
            for event in diagnostics["events"]
        ),
        "physical_attempts": result["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert observed == {
        "triggered": True,
        "state": "RUNNING",
        "same_target": True,
        "retries": 1,
        "errno_preserved": True,
        "registered_disappearance": True,
        "physical_attempts": 0,
    }


@pytest.mark.parametrize(
    ("read_errno", "expected_code"),
    [
        pytest.param(errno.ENOENT, "FILE_DISAPPEARED", id="enoent-live"),
        pytest.param(errno.EACCES, "PERMISSION_DENIED", id="permission"),
        pytest.param(errno.EIO, "OS_READ_FAILED", id="io"),
    ],
)
def test_registered_auxiliary_executable_failure_without_confirmed_exit_remains_unknown(
    publisher: Any,
    monkeypatch: pytest.MonkeyPatch,
    read_errno: int,
    expected_code: str,
    record_property: Any,
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    owner.publish()
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary = owner.proc_root / "124"
    auxiliary.mkdir()
    auxiliary_exe = auxiliary / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    (auxiliary / "stat").write_text(f"124 (helper) R {os.getpid()} " + "0 " * 17 + "100 0\n")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    owner.child_registry = FixtureRegistry(
        {
            123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
            124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
        }
    )
    original = owner._executable_identity
    fired = False

    def fail_once(pid: int) -> tuple[int, int]:
        nonlocal fired
        if pid == 124 and not fired:
            fired = True
            raise OSError(read_errno, "test-owned executable read failure")
        return original(pid)

    monkeypatch.setattr(owner, "_executable_identity", fail_once)
    result = owner.publish()
    diagnostics = result["lifecycle"]["read_diagnostics"]
    observed = {
        "triggered": fired,
        "state": result["lifecycle"]["state"],
        "target": result["target_identity"],
        "retries": diagnostics["proc_scan_retry_count"],
        "original_error": any(
            event["code"] == expected_code and event["errno"] == read_errno
            for event in diagnostics["events"]
        ),
        "physical_attempts": result["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert observed == {
        "triggered": True,
        "state": "UNKNOWN",
        "target": None,
        "retries": 0,
        "original_error": True,
        "physical_attempts": 0,
    }
    assert result["activation"] == {"bounded_termination_enabled": None, "rw_timeout_enabled": None}


@pytest.mark.parametrize("drift", ["auxiliary-pid-reuse", "managed-generation"])
def test_registered_auxiliary_executable_exit_retry_rejects_identity_drift(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, drift: str, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    owner.publish()
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary = owner.proc_root / "124"
    auxiliary.mkdir()
    auxiliary_exe = auxiliary / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    auxiliary_stat = auxiliary / "stat"
    auxiliary_stat.write_text(f"124 (helper) R {os.getpid()} " + "0 " * 17 + "100 0\n")
    children = owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children"
    children.write_text("123 124")
    owner.child_registry = FixtureRegistry(
        {
            123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
            124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
        }
    )
    original = owner._executable_identity
    fired = False

    def drift_during_exit(pid: int) -> tuple[int, int]:
        nonlocal fired
        if pid == 124 and not fired:
            fired = True
            auxiliary_exe.unlink()
            if drift == "auxiliary-pid-reuse":
                auxiliary_stat.write_text(f"124 (reused) R {os.getpid()} " + "0 " * 17 + "101 0\n")
            else:
                auxiliary_stat.unlink()
                auxiliary.rmdir()
                children.write_text("123")
                process["ffmpeg_generation"] = "changed-during-retry"
            raise FileNotFoundError(errno.ENOENT, "test-owned executable disappeared")
        return original(pid)

    monkeypatch.setattr(owner, "_executable_identity", drift_during_exit)
    result = owner.publish()
    diagnostics = result["lifecycle"]["read_diagnostics"]
    observed = {
        "triggered": fired,
        "state": result["lifecycle"]["state"],
        "target": result["target_identity"],
        "retries": diagnostics["proc_scan_retry_count"],
        "physical_attempts": result["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert observed == {
        "triggered": True,
        "state": "UNKNOWN",
        "target": None,
        "retries": 0,
        "physical_attempts": 0,
    }


def test_registered_auxiliary_executable_churn_remains_unknown(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    auxiliary = owner.proc_root / "124"
    auxiliary.mkdir()
    auxiliary_exe = auxiliary / "exe"
    auxiliary_exe.write_bytes(b"auxiliary")
    (auxiliary / "stat").write_text(f"124 (helper) S {os.getpid()} " + "0 " * 17 + "100 0\n")
    (auxiliary / "cmdline").write_bytes(b"")
    (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    owner.child_registry = FixtureRegistry(
        {
            123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe),
            124: registry_entry(owner, 124, role="AUXILIARY", ticks=100, executable=auxiliary_exe),
        }
    )
    original = owner._executable_identity
    auxiliary_reads = 0

    def changing(pid: int) -> tuple[int, int]:
        nonlocal auxiliary_reads
        identity = original(pid)
        if pid == 124:
            auxiliary_reads += 1
            return identity[0], identity[1] + auxiliary_reads % 2
        return identity

    monkeypatch.setattr(owner, "_executable_identity", changing)
    result = owner.publish()
    diagnostics = result["lifecycle"]["read_diagnostics"]
    observed = {
        "executable_changed": auxiliary_reads >= 4,
        "state": result["lifecycle"]["state"],
        "target": result["target_identity"],
        "diagnostic_count": sum(
            event["code"] == "RECOVERY_OWNER_AUXILIARY_EXECUTABLE_CHANGED"
            for event in diagnostics["events"]
        ),
        "physical_attempts": result["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))

    assert observed["executable_changed"] is True
    assert observed["state"] == "UNKNOWN"
    assert observed["target"] is None
    assert observed["diagnostic_count"] >= 1
    assert observed["physical_attempts"] == 0


def test_real_kernel_auxiliary_stop_state_does_not_invalidate_managed_child(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    """Only test-owned children are signalled; no live or discovered PID is used."""
    owner, process, target = publisher
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v3"
    managed = subprocess.Popen(
        ["ffmpeg", "-c", "import sys; sys.stdin.read(1)", "rtmps://test.invalid/unused"],
        executable=sys.executable,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FFMPEG_RW_TIMEOUT_ENABLED": "1"},
    )
    auxiliary = subprocess.Popen(
        ["/bin/sleep", "10"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    stopped = False
    try:
        wait_for_proc_command(managed)
        wait_for_proc_command(auxiliary)
        owner.proc_root = Path("/proc")
        _, _, managed_ticks = owner._proc_stat(managed.pid)
        _, _, auxiliary_ticks = owner._proc_stat(auxiliary.pid)
        _, _, owner_ticks = owner._proc_stat(os.getpid())
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        target.update(ffmpeg_pid=managed.pid, host_boot_id=boot_id)
        target["ffmpeg_generation"] = (
            "ffmpeg-"
            + hashlib.sha256(
                f"{target['pod_uid']}:{target['container_id']}:{managed.pid}:{managed_ticks}".encode()
            ).hexdigest()[:32]
        )
        raw = json.loads(owner.target_path.read_text())
        raw["target_identity"] = target
        owner.target_path.write_text(json.dumps(raw))
        process.update(
            local_ffmpeg_pid=managed.pid,
            managed_child_pids=[managed.pid],
            runtime_lifecycle_state="FFMPEG_RUNNING",
        )
        managed_entry = registry_entry(
            owner,
            managed.pid,
            role="DELIVERY_CANDIDATE",
            ticks=managed_ticks,
            executable=Path(f"/proc/{managed.pid}/exe"),
        )
        auxiliary_entry = registry_entry(
            owner,
            auxiliary.pid,
            role="AUXILIARY",
            ticks=auxiliary_ticks,
            executable=Path(f"/proc/{auxiliary.pid}/exe"),
        )
        for entry in (managed_entry, auxiliary_entry):
            entry["owner_start_ticks"] = owner_ticks
        owner.child_registry = FixtureRegistry(
            {managed.pid: managed_entry, auxiliary.pid: auxiliary_entry}
        )
        original = owner._executable_identity
        transition_triggered = False

        def stop_during_identity_read(pid: int) -> tuple[int, int]:
            nonlocal transition_triggered, stopped
            identity = original(pid)
            if pid == auxiliary.pid and not transition_triggered:
                transition_triggered = True
                os.kill(auxiliary.pid, signal.SIGSTOP)
                deadline = time.monotonic() + 2
                while owner._proc_stat(auxiliary.pid)[0] not in {"T", "t"}:
                    assert time.monotonic() < deadline
                    time.sleep(0.001)
                stopped = True
            return identity

        monkeypatch.setattr(owner, "_executable_identity", stop_during_identity_read)
        result = owner.publish()
        diagnostics = result["lifecycle"]["read_diagnostics"]
        observed = {
            "scheduler_state_changed": transition_triggered and stopped,
            "state": result["lifecycle"]["state"],
            "target_present": result["target_identity"] is not None,
            "retries": diagnostics["proc_scan_retry_count"],
            "physical_attempts": result["effects"]["physical_attempt_count"],
        }
        record_property("cra_evidence", json.dumps(observed))
        assert observed == {
            "scheduler_state_changed": True,
            "state": "RUNNING",
            "target_present": True,
            "retries": 0,
            "physical_attempts": 0,
        }, json.dumps(result, sort_keys=True)
    finally:
        if stopped and auxiliary.poll() is None:
            os.kill(auxiliary.pid, signal.SIGCONT)
        for child_process in (auxiliary, managed):
            if child_process.stdin is not None and not child_process.stdin.closed:
                child_process.stdin.close()
            try:
                child_process.wait(timeout=0.05)
            except subprocess.TimeoutExpired:
                child_process.terminate()
                child_process.wait(timeout=3)


def test_real_kernel_auxiliary_exit_during_first_executable_read_is_retried(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    """Exercise the production ordering with only test-owned kernel children."""
    owner, process, target = publisher
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v3"
    managed = subprocess.Popen(
        ["ffmpeg", "-c", "import sys; sys.stdin.read(1)", "rtmps://test.invalid/unused"],
        executable=sys.executable,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FFMPEG_RW_TIMEOUT_ENABLED": "1"},
    )
    auxiliary = subprocess.Popen(
        ["/bin/cat"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_proc_command(managed)
        wait_for_proc_command(auxiliary)
        owner.proc_root = Path("/proc")
        _, _, managed_ticks = owner._proc_stat(managed.pid)
        _, _, auxiliary_ticks = owner._proc_stat(auxiliary.pid)
        _, _, owner_ticks = owner._proc_stat(os.getpid())
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        target.update(ffmpeg_pid=managed.pid, host_boot_id=boot_id)
        target["ffmpeg_generation"] = (
            "ffmpeg-"
            + hashlib.sha256(
                f"{target['pod_uid']}:{target['container_id']}:{managed.pid}:{managed_ticks}".encode()
            ).hexdigest()[:32]
        )
        raw = json.loads(owner.target_path.read_text())
        raw["target_identity"] = target
        owner.target_path.write_text(json.dumps(raw))
        process.update(
            local_ffmpeg_pid=managed.pid,
            managed_child_pids=[managed.pid],
            runtime_lifecycle_state="FFMPEG_RUNNING",
        )
        managed_entry = registry_entry(
            owner,
            managed.pid,
            role="DELIVERY_CANDIDATE",
            ticks=managed_ticks,
            executable=Path(f"/proc/{managed.pid}/exe"),
        )
        auxiliary_entry = registry_entry(
            owner,
            auxiliary.pid,
            role="AUXILIARY",
            ticks=auxiliary_ticks,
            executable=Path(f"/proc/{auxiliary.pid}/exe"),
        )
        for entry in (managed_entry, auxiliary_entry):
            entry["owner_start_ticks"] = owner_ticks
        owner.child_registry = FixtureRegistry({managed.pid: managed_entry, auxiliary.pid: auxiliary_entry})
        before = owner.publish()
        original = owner._executable_identity
        fired = False

        def exit_during_executable_read(pid: int) -> tuple[int, int]:
            nonlocal fired
            if pid == auxiliary.pid and not fired:
                fired = True
                assert auxiliary.stdin is not None
                auxiliary.stdin.close()
                auxiliary.wait(timeout=2)
            return original(pid)

        monkeypatch.setattr(owner, "_executable_identity", exit_during_executable_read)
        started = time.monotonic()
        result = owner.publish()
        elapsed = time.monotonic() - started
        diagnostics = result["lifecycle"]["read_diagnostics"]
        observed = {
            "triggered": fired,
            "state": result["lifecycle"]["state"],
            "same_target": result["target_identity"] == before["target_identity"],
            "retries": diagnostics["proc_scan_retry_count"],
            "file_disappeared": any(
                event["code"] == "FILE_DISAPPEARED" and event["errno"] in {errno.ENOENT, errno.ESRCH}
                for event in diagnostics["events"]
            ),
            "physical_attempts": result["effects"]["physical_attempt_count"],
            "elapsed_seconds": elapsed,
        }
        record_property("cra_evidence", json.dumps(observed))
        assert observed["triggered"] is True
        assert observed["state"] == "RUNNING"
        assert observed["same_target"] is True
        assert observed["retries"] >= 1
        assert observed["file_disappeared"] is True
        assert observed["physical_attempts"] == 0
        assert elapsed < owner_module.QUERY_SECONDS
    finally:
        for child_process in (auxiliary, managed):
            if child_process.stdin is not None and not child_process.stdin.closed:
                child_process.stdin.close()
            try:
                child_process.wait(timeout=0.05)
            except subprocess.TimeoutExpired:
                child_process.terminate()
                child_process.wait(timeout=3)


def test_managed_executable_read_failure_remains_unknown(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    managed_exe = owner.proc_root / "123/exe"
    managed_exe.write_bytes(b"managed")
    owner.child_registry = FixtureRegistry(
        {123: registry_entry(owner, 123, role="DELIVERY_CANDIDATE", ticks=99, executable=managed_exe)}
    )
    original = owner._executable_identity

    def deny_managed(pid: int) -> tuple[int, int]:
        if pid == 123:
            raise PermissionError(13, "test-owned denial")
        return original(pid)

    monkeypatch.setattr(owner, "_executable_identity", deny_managed)
    result = owner.publish()
    diagnostics = result["lifecycle"]["read_diagnostics"]["events"]
    record_property(
        "cra_evidence",
        json.dumps(
            {
                "state": result["lifecycle"]["state"],
                "target": result["target_identity"],
                "permission_diagnostic": any(event["code"] == "PERMISSION_DENIED" for event in diagnostics),
                "physical_attempts": result["effects"]["physical_attempt_count"],
            }
        ),
    )

    assert result["lifecycle"]["state"] == "UNKNOWN"
    assert result["target_identity"] is None
    assert any(event["code"] == "PERMISSION_DENIED" for event in diagnostics)
    assert result["effects"]["physical_attempt_count"] == 0


@pytest.mark.parametrize(
    "fault",
    [
        "unreadable-child",
        "unreadable-children",
        "fake-absence",
        "owner-reuse",
        "pod-drift",
        "boot-drift",
        "stopped-child",
        "second-child",
        "unknown-cardinality",
        "invalid-generation",
    ],
)
def test_unknown_is_not_proof_of_absence_or_cached_health(publisher: Any, fault: str) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    owner.publish()
    if fault == "unreadable-child":
        (owner.proc_root / "123/stat").unlink()
    elif fault == "unreadable-children":
        (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").unlink()
    elif fault == "fake-absence":
        child(owner, process, absent=True)
        (owner.proc_root / "123/stat").write_text(f"123 (ffmpeg) S {os.getpid()} " + "0 " * 17 + "99 0\n")
    elif fault == "owner-reuse":
        p = owner.proc_root / str(os.getpid()) / "stat"
        p.write_text(p.read_text().replace("40 0", "41 0"))
    elif fault == "pod-drift":
        process["pod_uid"] = "different-pod"
    elif fault == "boot-drift":
        (owner.proc_root / "sys/kernel/random/boot_id").write_text("different-boot")
    elif fault == "stopped-child":
        p = owner.proc_root / "123/stat"
        p.write_text(p.read_text().replace(") S ", ") T "))
    elif fault == "unknown-cardinality":
        process["managed_child_cardinality"] = "UNKNOWN"
    elif fault == "invalid-generation":
        process["ffmpeg_generation"] = None
    else:
        p = owner.proc_root / "124"
        p.mkdir()
        (p / "stat").write_text(f"124 (ffmpeg) S {os.getpid()} " + "0 " * 17 + "100 0\n")
        (p / "cmdline").write_bytes(b"ffmpeg\0rtmps://test.invalid/second\0")
        (owner.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("123 124")
    result = owner.publish()
    assert result["lifecycle"]["state"] == "UNKNOWN"
    assert result["target_identity"] is None
    assert all(v is None for v in result["activation"].values())
    assert result["effects"]["integrity"] == "ok"


def test_read_error_then_good_context_does_not_invent_child_transition(publisher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    owner.publish()
    real = owner._strict_children
    reads = 0

    def interrupted() -> list[int]:
        nonlocal reads
        reads += 1
        if reads == 1:
            raise OSError("test-owned read failure")
        return real()

    monkeypatch.setattr(owner, "_strict_children", interrupted)
    raw = owner.publish()
    assert raw["lifecycle"]["state"] == "UNKNOWN"
    assert raw["target_sha256"] is None


def test_confirmed_absence_and_unmatched_replacement_are_explicit(publisher: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    owner.publish()
    child(owner, process, absent=True)
    absent = owner.publish()
    assert absent["lifecycle"]["state"] == "ABSENT"
    child(owner, process, absent=False, ticks=100)
    replacement = owner.publish()
    assert replacement["lifecycle"]["state"] == "TRANSITION"
    assert replacement["lifecycle"]["child_start_ticks"] == 100
    assert replacement["target_sha256"] is None


def test_cold_owner_cannot_claim_absence_without_runtime_anchor(publisher: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    child(owner, process, absent=True)
    life = owner.publish()["lifecycle"]
    assert life["state"] == "UNKNOWN"
    assert {event["code"] for event in life["read_diagnostics"]["events"]} == {"RECOVERY_OWNER_CHILD_NOT_ANCHORED"}


def test_activation_read_failure_keeps_unknown_without_reusing_previous_true(publisher: Any) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    assert all(owner.publish()["activation"].values())
    (owner.proc_root / "123/environ").unlink()
    result = owner.publish()
    assert result["lifecycle"]["state"] == "RUNNING"
    assert all(value is None for value in result["activation"].values())


@pytest.mark.parametrize("fault", ["exit", "read-gap"])
def test_owner_snapshot_transition_preserves_only_proven_claims(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, fault: str, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    owner.publish()
    real = owner_module.owner_evidence

    def inject(db: Any, **kwargs: Any) -> Any:
        result = real(db, **kwargs)
        if fault == "exit":
            child(owner, process, absent=True)
        else:
            (owner.proc_root / "123/stat").unlink()
        return result

    monkeypatch.setattr(owner_module, "owner_evidence", inject)
    result = owner.publish()
    record_property("cra_evidence", json.dumps({"state": result["lifecycle"]["state"], "target": result["target_identity"]}))
    assert result["lifecycle"]["state"] == ("ABSENT" if fault == "exit" else "UNKNOWN")
    assert result["target_identity"] is None
    assert all(value is None for value in result["activation"].values())
    assert result["effects"]["integrity"] == "ok"


def test_real_kernel_child_exit_after_ledger_snapshot_is_finally_observed(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    owner, process, target = publisher
    managed = subprocess.Popen(
        ["ffmpeg", "-c", "import sys; sys.stdin.read(1)", "rtmps://test.invalid/unused"],
        executable=sys.executable,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FFMPEG_RW_TIMEOUT_ENABLED": "1"},
    )
    try:
        bind_real_managed_child(owner, process, target, managed)
        before = owner.publish()
        assert before["lifecycle"]["state"] == "RUNNING"
        real = owner_module.owner_evidence
        triggered = False

        def exit_after_ledger(db: Any, **kwargs: Any) -> Any:
            nonlocal triggered
            result = real(db, **kwargs)
            assert managed.stdin is not None
            managed.stdin.close()
            managed.wait(timeout=2)
            process.update(
                local_ffmpeg_pid=0,
                ffmpeg_running=False,
                managed_child_pids=[],
                managed_child_cardinality="0",
                runtime_lifecycle_state="RESTART_DELAY",
            )
            triggered = True
            return result

        monkeypatch.setattr(owner_module, "owner_evidence", exit_after_ledger)
        result = owner.publish()
        observed = {
            "triggered": triggered,
            "state": result["lifecycle"]["state"],
            "target": result["target_identity"],
            "activation_unknown": all(value is None for value in result["activation"].values()),
            "integrity": result["effects"]["integrity"],
            "physical_attempts": result["effects"]["physical_attempt_count"],
        }
        record_property("cra_evidence", json.dumps(observed))
        assert observed == {
            "triggered": True,
            "state": "ABSENT",
            "target": None,
            "activation_unknown": True,
            "integrity": "ok",
            "physical_attempts": 0,
        }
    finally:
        if managed.stdin is not None and not managed.stdin.closed:
            managed.stdin.close()
        if managed.poll() is None:
            managed.kill()
        managed.wait(timeout=2)


@pytest.mark.parametrize("time_offset", [None, -0.001, 0.502])
def test_final_child_observation_does_not_retime_ledger_snapshot(
    publisher: Any, setup: Any, monkeypatch: pytest.MonkeyPatch, time_offset: float | None
) -> None:
    owner, process, _ = publisher
    config, _ = setup
    configure(owner, process)
    owner.config.update(host_id=config.bindings["dell"].host_id, stream_id=config.bindings["dell"].stream_id)
    clock = [datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=1)]

    class Clock:
        @staticmethod
        def now(tz: Any = None) -> datetime:
            return clock[0]

    monkeypatch.setattr(owner_module, "datetime", Clock)
    owner.publish()
    ledger_at = clock[0]
    real = owner_module.owner_evidence

    def exit_after_ledger(db: Any, **kwargs: Any) -> Any:
        result = real(db, **kwargs)
        clock[0] += timedelta(seconds=0.2)
        child(owner, process, absent=True)
        return result

    monkeypatch.setattr(owner_module, "owner_evidence", exit_after_ledger)
    raw = owner.publish()
    assert datetime.fromisoformat(raw["observed_at"].replace("Z", "+00:00")) == ledger_at
    assert datetime.fromisoformat(raw["lifecycle"]["observed_at"]) == ledger_at + timedelta(seconds=0.2)
    assert raw["lifecycle"]["state"] == "ABSENT"
    if time_offset is not None:
        raw["lifecycle"]["observed_at"] = (ledger_at + timedelta(seconds=time_offset)).isoformat()
    kwargs = dict(
        target={},
        binding=config.bindings["dell"],
        runtime_binding=binding(owner),
        now=ledger_at + timedelta(seconds=1),
        history_policy=empty_policy(),
        physical_boot_id="boot-id",
    )
    if time_offset is None:
        effects, _ = runtime_evidence_fact(raw, **kwargs)
        assert effects["integrity"] == "ok"
    else:
        with pytest.raises(ValueError, match="OWNER_LIFECYCLE_TARGET_BINDING_INVALID"):
            runtime_evidence_fact(raw, **kwargs)


def test_actual_test_owned_child_exit_is_bound_to_real_proc_identity(publisher: Any) -> None:
    owner, process, target = publisher
    owner.config["evidence_schema"] = "runtime.recovery_evidence.v3"
    proc = subprocess.Popen(["ffmpeg", "-c", "import time; time.sleep(10)", "rtmps://test.invalid/unused"], executable=sys.executable)
    try:
        wait_for_proc_command(proc)
        owner.proc_root = Path("/proc")
        _, _, ticks = owner._proc_stat(proc.pid)
        target.update(ffmpeg_pid=proc.pid, host_boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
        target["ffmpeg_generation"] = (
            "ffmpeg-" + hashlib.sha256(f"{target['pod_uid']}:{target['container_id']}:{proc.pid}:{ticks}".encode()).hexdigest()[:32]
        )
        raw = json.loads(owner.target_path.read_text())
        raw["target_identity"] = target
        owner.target_path.write_text(json.dumps(raw))
        process.update(local_ffmpeg_pid=proc.pid, managed_child_pids=[proc.pid], runtime_lifecycle_state="FFMPEG_RUNNING")
        running = owner.publish()
        assert running["lifecycle"]["state"] == "RUNNING"
        proc.terminate()
        proc.wait(timeout=3)
        process.update(
            local_ffmpeg_pid=0,
            managed_child_pids=[],
            managed_child_cardinality="0",
            ffmpeg_running=False,
            runtime_lifecycle_state="RESTART_DELAY",
        )
        absent = owner.publish()
        assert absent["lifecycle"]["state"] == "ABSENT"
        assert absent["lifecycle"]["anchor_target"] == target
        assert absent["effects"]["physical_attempt_count"] == 0
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=3)


def test_observer_v3_uses_coherent_owner_identity_and_preserves_lifecycle(
    publisher: Any, setup: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner, process, _ = publisher
    config, _ = setup
    configure(owner, process)
    owner.config.update(host_id=config.bindings["dell"].host_id, stream_id=config.bindings["dell"].stream_id)
    raw = owner.publish()
    parent = owner.path.parent
    sources = {"runtime_evidence": str(owner.path), "network": str(parent / "network.json"), "transport": str(parent / "transport.json")}
    Path(sources["network"]).write_text(
        json.dumps(
            {
                "ts_utc": raw["observed_at"],
                "probes": [{"name": name, "ok": True} for name in ["cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6"]],
            }
        )
    )
    Path(sources["transport"]).write_text(
        json.dumps({"ts_utc": raw["observed_at"], "ffmpeg_pid": raw["target_identity"]["ffmpeg_pid"], "metrics": {"bytes_acked": 100}})
    )
    for path in sources.values():
        Path(path).chmod(0o644)
    # Fixture files are user-owned; production still requires root ownership.
    reader = recovery_observer.read_regular_bytes

    def fixture_reader(path: Path, **kwargs: Any) -> bytes:
        kwargs["expected_owner_uid"] = os.getuid()
        return reader(path, **kwargs)

    monkeypatch.setattr(recovery_observer, "read_regular_bytes", fixture_reader)
    value = {
        "schema": recovery_observer.CONFIG_SCHEMA,
        "binding": config.bindings["dell"].__dict__,
        "sources": sources,
        "runtime_binding": binding(owner),
        "effect_history_policy": empty_policy(),
        **{name: str(parent / name) for name in ["state_file", "output_file", "signing_private_key_file", "host_contract_file"]},
    }
    recovery_observer.validate_config(value)
    facts, hashes = recovery_observer.build_facts(value, now=datetime.now(UTC), host_boot_id="boot-id")
    assert facts["transport"]["target"] == raw["target_sha256"]
    assert facts["transport"]["lifecycle"] == raw["lifecycle"]
    assert facts["activation"] == raw["activation"]
    assert hashes["runtime_evidence"] == source_hash(raw)
    assert "target" not in hashes


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "unknown-onset",
        "missing-lifecycle",
        "bad-signature",
        "false-activation",
        "missing-ledger",
        "wrong-boot",
        "stale-lifecycle",
        "long-absence",
    ],
)
def test_signed_owner_lifecycle_collector_and_oracle(publisher: Any, setup: Any, monkeypatch: pytest.MonkeyPatch, fault: str) -> None:
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

    monkeypatch.setattr(owner_module, "datetime", Clock)
    end = 705 if fault == "long-absence" else 180
    for seconds in range(0, end + 1, 15):
        now = START + timedelta(seconds=seconds)
        clock[0] = now
        absent = seconds >= 45 and (seconds <= 75 or fault == "long-absence")
        child(owner, process, absent=absent, ticks=99 if seconds < 90 or absent else 100)
        target = json.loads(owner.target_path.read_text())
        if seconds >= 90 and not absent:
            target["target_identity"]["ffmpeg_generation"] = (
                "ffmpeg-" + hashlib.sha256(b"pod-id:containerd://test-child:7654321:100").hexdigest()[:32]
            )
        target.update(observed_at=now.isoformat(), valid_until=(now + timedelta(seconds=45)).isoformat())
        owner.target_path.write_text(json.dumps(target))
        raw = owner.publish()
        assert raw["lifecycle"]["state"] == ("ABSENT" if absent else "RUNNING"), raw
        effects, activation = runtime_evidence_fact(
            raw,
            target={},
            binding=config.bindings["dell"],
            runtime_binding=binding(owner),
            now=now,
            history_policy=empty_policy(),
            physical_boot_id="boot-id",
        )
        # This test signs the output of the real owner admission path. Physical
        # boot and child identity are retained, never borrowed from live data.
        inputs = {role: packet(config, signers, role, seconds) for role in config.bindings}
        facts = copy.deepcopy(inputs["dell"]["facts"])
        facts.update(effects=effects, activation=activation)
        facts["transport"].update(
            target=raw["target_sha256"], bytes_acked=None if absent else 1000 + seconds * 100, lifecycle=copy.deepcopy(raw["lifecycle"])
        )
        if absent:
            inputs["arena"] = packet(config, signers, "arena", seconds, platform={"state": "UNKNOWN"})
        if seconds == 45:
            if fault == "unknown-onset":
                facts["transport"]["lifecycle"].update(state="UNKNOWN", reason="READ_UNAVAILABLE", anchor_target=None)
            elif fault == "missing-lifecycle":
                del facts["transport"]["lifecycle"]
            elif fault == "false-activation":
                facts["activation"]["rw_timeout_enabled"] = False
                # Retain a target to make this explicit disabled setting part
                # of the activation gate, independently from absence handling.
                facts["transport"]["target"] = source_hash(raw["lifecycle"]["anchor_target"])
            elif fault == "missing-ledger":
                facts["effects"] = {}
            elif fault == "wrong-boot":
                facts["transport"]["lifecycle"]["anchor_target"]["host_boot_id"] = "foreign"
            elif fault == "stale-lifecycle":
                facts["transport"]["lifecycle"]["observed_at"] = (now - timedelta(seconds=46)).isoformat()
        inputs["dell"] = sign_facts(
            binding=config.bindings["dell"],
            signer=signers["dell"],
            host_boot_id="boot-id",
            producer_id="owner-fixture",
            sequence=seconds + 1,
            now=now,
            valid_until=now + timedelta(seconds=45),
            facts=facts,
            source_hashes={"runtime_evidence": source_hash(raw)},
        )
        if seconds == 45 and fault == "bad-signature":
            inputs["dell"]["signature"] = "invalid"
        for role, value in inputs.items():
            inbox = Path(config.value["hosts"][role]["inbox_file"])
            inbox.write_text(json.dumps(value))
            inbox.chmod(0o644)
        recovery_soak.collect(config, now=now)
    rows = [json.loads(line) for line in Path(config.value["evidence_file"]).read_text().splitlines()]
    result = recovery_soak.evaluate(rows, config=config, now=clock[0])
    assert result["physical_attempt_delta"] == 0, result
    assert result["oracle_errors"] == []
    if fault == "none":
        assert result["harness_classification"] == "PASS", result
        assert result["unknown_reasons"] == result["blockers"] == []
        assert result["recovery"]["episode_count"] == result["recovery"]["recovered_episode_count"] == 1
        assert result["recovery"]["recent_episodes"][0]["trigger"] == "OWNER_CHILD_UNAVAILABLE"
        assert result["recovery"]["verified_network_recovered_episode_count"] == 0
        assert result["eligible"] is False
        assert result["live_health"] == "READY"
    else:
        assert result["harness_classification"] != "PASS", result
        assert result["eligible"] is False
    assert all(
        row["inputs"]["cra"]["facts"]["safety"]["action_table_counts"]
        == dict.fromkeys(row["inputs"]["cra"]["facts"]["safety"]["action_table_counts"], 0)
        for row in rows
    )


@pytest.mark.parametrize(
    "field,value", [("state", []), ("owner_pid", True), ("child_start_ticks", False), ("anchor_target", {}), ("observed_at", "invalid")]
)
def test_owner_adapter_rejects_malformed_lifecycle(publisher: Any, setup: Any, field: str, value: Any) -> None:
    owner, process, _ = publisher
    config, _ = setup
    configure(owner, process)
    owner.config.update(host_id=config.bindings["dell"].host_id, stream_id=config.bindings["dell"].stream_id)
    raw = owner.publish()
    raw["lifecycle"][field] = value
    with pytest.raises((ValueError, TypeError)):
        runtime_evidence_fact(
            raw,
            target=owner_target_snapshot(raw),
            binding=config.bindings["dell"],
            runtime_binding=binding(owner),
            now=datetime.now(UTC),
            history_policy=empty_policy(),
            physical_boot_id="boot-id",
        )


def test_pipe_metadata_does_not_invent_a_sender_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, record_property: Any) -> None:
    from runtime_boundary import observation
    monkeypatch.setattr(observation, "_tcp_metrics", lambda *_: {})
    owner = observation.RuntimeObservationPublisher(path=tmp_path/'observation.json', projection_path=tmp_path/'missing-projection',
        target_snapshot_path=tmp_path/'missing-target',
        process_supplier=lambda:{"local_encoder_pid":123,"delivery_route":"PRIMARY","delivery_route_generation":2})
    value=owner.publish()
    observed={"target":value["target_identity"],"sender_running":value["ffmpeg_running"],
              "encoder_pid":value["local_encoder_pid"],"route_generation":value["delivery_route_generation"]}
    record_property("cra_evidence",json.dumps(observed))
    assert observed=={"target":None,"sender_running":False,"encoder_pid":123,"route_generation":2}
