"""Unreaped child exit and empty cmdline boundaries, using only owned children."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from cra_no_action_soak.recovery_facts import validate_lifecycle
from runtime_boundary import recovery_publisher as module
from tests.harness.integration.test_owner_lifecycle_evidence import child, configure, wait_for_proc_command
from tests.harness.integration.test_owner_observation_chaos import helper, running
from tests.runtime_boundary.test_recovery_publisher import publisher as publisher


def _executable_fixture(owner: Any, *, same: bool = False) -> None:
    managed = owner.proc_root / "123/exe"
    auxiliary = owner.proc_root / "124/exe"
    managed.write_bytes(b"managed-ffmpeg-executable")
    auxiliary.unlink(missing_ok=True)
    if same:
        os.link(managed, auxiliary)
    else:
        auxiliary.write_bytes(b"auxiliary-executable")


def test_live_empty_auxiliary_command_is_retried_only_with_independent_executable_and_owner_state(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    before = running(owner)
    ledger_changes = owner.ledger.connection.total_changes
    helper(owner, present=True)
    _executable_fixture(owner)
    original_open = Path.open
    fired = False

    def opened(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        if not fired and path == owner.proc_root / "124/cmdline" and owner._read_stage == "child_command":
            fired = True
            path.write_bytes(b"probe-helper\0")
            return io.BytesIO(b"")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", opened)
    after = owner.publish()
    diagnostics = after["lifecycle"]["read_diagnostics"]
    events = [event for event in diagnostics["events"] if event["code"] == "RECOVERY_OWNER_AUXILIARY_COMMAND_UNAVAILABLE"]
    observed = {
        "triggered": fired,
        "state": after["lifecycle"]["state"],
        "same_target": after["target_identity"] == before["target_identity"],
        "retries": diagnostics["proc_scan_retry_count"],
        "capsule": events[0]["process"] if events else None,
        "physical_attempts": after["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert fired and observed["state"] == "RUNNING", observed
    assert observed["same_target"] and observed["retries"] >= 1
    assert observed["physical_attempts"] == 0 and owner.ledger.connection.total_changes == ledger_changes
    assert observed["capsule"] == {
        "pid": 124,
        "state": "S",
        "parent_pid": os.getpid(),
        "start_ticks": 100,
        "command_bytes": 0,
        "anchor_relation": "AUXILIARY",
        "executable_relation": "DIFFERENT",
        "anchor_target_sha256": before["target_sha256"],
        "owner_state_relation": "MATCHED",
    }
    assert "probe-helper" not in json.dumps(observed["capsule"])
    validate_lifecycle(after["lifecycle"], now=datetime.now(UTC), boot_id="boot-id")


@pytest.mark.parametrize("fault", ["same-executable", "owner-state-unproven", "executable-unavailable"])
def test_live_empty_command_corroboration_never_bypasses_unproven_identity(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any, fault: str
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    helper(owner, present=True)
    if fault != "executable-unavailable":
        _executable_fixture(owner, same=fault == "same-executable")
    if fault == "owner-state-unproven":
        process["managed_child_cardinality"] = "UNKNOWN"
    fired = False
    original_open = Path.open

    def opened(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        if not fired and path == owner.proc_root / "124/cmdline" and owner._read_stage == "child_command":
            fired = True
            if fault == "same-executable":
                path.write_bytes(b"probe-helper\0")
            return io.BytesIO(b"")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", opened)
    after = owner.publish()
    diagnostics = after["lifecycle"]["read_diagnostics"]
    invalid = [event for event in diagnostics["events"] if event["code"] == "RECOVERY_OWNER_CHILD_COMMAND_INVALID"]
    observed = {
        "triggered": fired,
        "state": after["lifecycle"]["state"],
        "target": after["target_identity"],
        "retries": diagnostics["proc_scan_retry_count"],
        "capsule": invalid[0]["process"] if invalid else None,
        "physical_attempts": after["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert fired and observed["state"] == "UNKNOWN", observed
    assert observed["target"] is None and observed["retries"] == 0
    assert observed["physical_attempts"] == 0
    assert observed["capsule"] is not None and observed["capsule"]["command_bytes"] == 0
    assert all(value is None for value in after["activation"].values())
    validate_lifecycle(after["lifecycle"], now=datetime.now(UTC), boot_id="boot-id")


@pytest.mark.parametrize("context", [1, 2])
@pytest.mark.parametrize("stage", ["child_command", "child_command_final"])
@pytest.mark.parametrize("victim", ["auxiliary", "managed"])
def test_real_kernel_unreaped_exit_returns_empty_cmdline(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any, context: int, stage: str, victim: str
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
    original_open = Path.open
    lengths: list[int] = []
    durations: list[float] = []
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
        changes = owner.ledger.connection.total_changes
        for _ in range(8 if victim == "auxiliary" else 1):
            fired = False
            if victim == "auxiliary":
                auxiliary = subprocess.Popen(["/bin/cat"], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                wait_for_proc_command(auxiliary)
            retired = auxiliary if victim == "auxiliary" else managed
            assert retired is not None

            class ProcFile:
                def __init__(self, source: Any, retired: Any) -> None:
                    self.source = source
                    self.retired = retired

                def __enter__(self) -> Any:
                    return self

                def __exit__(self, *args: Any) -> None:
                    self.source.close()

                def read(self, size: int) -> bytes:
                    retired = self.retired
                    assert retired is not None and retired.stdin is not None
                    retired.stdin.close()
                    # WNOWAIT preserves the zombie and its procfs entries.
                    # No fabricated bytes and no reaping before the kernel read.
                    until = time.monotonic() + 2
                    while os.waitid(os.P_PID, retired.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None:
                        assert time.monotonic() < until, "owned child exit timeout"
                        time.sleep(0.001)
                    assert owner._proc_stat(retired.pid)[0] == "Z"
                    value = self.source.read(size)
                    lengths.append(len(value))
                    return value

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
                    return ProcFile(source, retired)
                return source

            monkeypatch.setattr(Path, "open", opened)
            started = time.monotonic()
            after = owner.publish()
            durations.append(time.monotonic() - started)
            observed = {
                "triggered": fired,
                "kernel_read_bytes": lengths[-1] if lengths else None,
                "state": after["lifecycle"]["state"],
                "same_target": after["target_identity"] == before["target_identity"],
                "target": after["target_identity"],
                "retries": after["lifecycle"]["read_diagnostics"]["proc_scan_retry_count"],
                "physical_attempts": after["effects"]["physical_attempt_count"],
                "native_cycles": len(durations),
                "maximum_cycle_seconds": max(durations),
            }
            assert fired and observed["kernel_read_bytes"] == 0, observed
            assert observed["physical_attempts"] == 0 and owner.ledger.connection.total_changes == changes
            if victim == "auxiliary":
                assert observed["state"] == "RUNNING", observed
                assert observed["same_target"] and observed["retries"] >= 1
                assert all(value is True for value in after["activation"].values())
                assert after["observed_at"] > before["observed_at"] and managed.poll() is None
            else:
                assert observed["state"] != "RUNNING", observed
                assert observed["target"] is None and observed["retries"] == 0
                assert all(value is None for value in after["activation"].values())
            validate_lifecycle(after["lifecycle"], now=datetime.now(UTC), boot_id=target["host_boot_id"])
            retired.wait(timeout=2)
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
                    proc.kill()  # Only this test's explicitly created child.
                    proc.wait(timeout=2)


@pytest.mark.parametrize("context", [1, 2])
@pytest.mark.parametrize("stage", ["child_command", "child_command_final"])
@pytest.mark.parametrize(
    "fault", ["live-empty", "oversize", "no-anchor", "pid-reuse", "reparent", "managed-exit", "owner-reuse", "permission", "io"]
)
def test_empty_cmdline_never_bypasses_identity_or_read_failure(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any, context: int, stage: str, fault: str
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    helper(owner, present=True)
    if fault == "no-anchor":
        owner._child_anchor = None
    fired = False
    original_open, original_stat = Path.open, owner._proc_stat

    def opened(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        if (
            not fired
            and path == owner.proc_root / "124/cmdline"
            and owner._read_stage == stage
            and owner._read_diagnostics["proc_scan_attempt_count"] == context
        ):
            fired = True
            if fault == "no-anchor":
                owner._child_anchor = None
            if fault == "oversize":
                return io.BytesIO(b"x" * (128 * 1024 + 1))
            if fault != "live-empty":
                stat = owner.proc_root / "124/stat"
                stat.write_text(
                    f"124 (probe-helper) Z {1 if fault == 'reparent' else os.getpid()} "
                    + "0 " * 17
                    + ("101 0\n" if fault == "pid-reuse" else "100 0\n")
                )
            if fault == "managed-exit":
                child(owner, process, absent=True)
            if fault == "owner-reuse":
                stat = owner.proc_root / str(os.getpid()) / "stat"
                stat.write_text(stat.read_text().replace("40 0", "41 0"))
            return io.BytesIO(b"")
        return original_open(path, *args, **kwargs)

    def stat(pid: int) -> Any:
        if fired and pid == 124 and fault in {"permission", "io"}:
            raise OSError(13 if fault == "permission" else 5, "SECRET_MUST_NOT_LEAK")
        return original_stat(pid)

    monkeypatch.setattr(Path, "open", opened)
    monkeypatch.setattr(owner, "_proc_stat", stat)
    after = owner.publish()
    diag = after["lifecycle"]["read_diagnostics"]
    observed = {
        "triggered": fired,
        "state": after["lifecycle"]["state"],
        "target": after["target_identity"],
        "retries": diag["proc_scan_retry_count"],
        "physical_attempts": after["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert fired and observed["state"] != "RUNNING", observed
    assert observed["target"] is None and observed["physical_attempts"] == 0
    assert all(value is None for value in after["activation"].values())
    assert diag["events"] and "SECRET_MUST_NOT_LEAK" not in owner.path.read_text()
    if fault in {"live-empty", "oversize", "permission", "io"}:
        assert observed["retries"] == 0
    validate_lifecycle(after["lifecycle"], now=datetime.now(UTC), boot_id="boot-id")


@pytest.mark.parametrize("fault", ["continuous-exit", "deadline", "activation-off"])
def test_empty_cmdline_reobservation_remains_bounded_and_fresh(
    publisher: Any, monkeypatch: pytest.MonkeyPatch, record_property: Any, fault: str
) -> None:
    owner, process, _ = publisher
    configure(owner, process)
    running(owner)
    helper(owner, present=True)
    original_scan, original_open = owner._scan_children, Path.open
    fired = False

    def scan() -> list[int]:
        nonlocal fired
        if fault != "activation-off":
            helper(owner, present=True)
            fired = False
        return original_scan()

    def opened(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal fired
        if not fired and path == owner.proc_root / "124/cmdline" and owner._read_stage == "child_command":
            fired = True
            stat = owner.proc_root / "124/stat"
            stat.write_text(stat.read_text().replace(") S ", ") Z "))
            (owner.proc_root / "124/cmdline").write_bytes(b"")
            if fault == "activation-off":
                (owner.proc_root / "123/environ").write_bytes(b"FR_FFMPEG_FORCE_KILL_ENABLED=1\0FFMPEG_RW_TIMEOUT_ENABLED=0\0")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(owner, "_scan_children", scan)
    monkeypatch.setattr(Path, "open", opened)
    if fault == "deadline":
        monkeypatch.setattr(module, "PROC_SCAN_SECONDS", 0.0)
    after = owner.publish()
    diag = after["lifecycle"]["read_diagnostics"]
    observed = {
        "triggered": fired,
        "state": after["lifecycle"]["state"],
        "attempts": diag["proc_scan_attempt_count"],
        "retries": diag["proc_scan_retry_count"],
        "rw_timeout": after["activation"]["rw_timeout_enabled"],
        "physical_attempts": after["effects"]["physical_attempt_count"],
    }
    record_property("cra_evidence", json.dumps(observed))
    assert fired and observed["physical_attempts"] == 0
    assert diag["proc_scan_attempt_count"] <= 6 and diag["proc_scan_retry_count"] <= 4
    if fault == "activation-off":
        assert observed["state"] == "RUNNING" and observed["rw_timeout"] is False
    else:
        assert observed["state"] == "UNKNOWN" and after["target_identity"] is None
    if fault == "deadline":
        assert observed["attempts"] <= 2 and observed["retries"] == 0
    validate_lifecycle(after["lifecycle"], now=datetime.now(UTC), boot_id="boot-id")
