from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from runtime_boundary.child_registry import ChildLifecycleRegistry


def wait_for_state(pid: int, expected: str, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            raw = Path(f"/proc/{pid}/stat").read_text()
        except FileNotFoundError:
            time.sleep(0.001)
            continue
        if raw[raw.rfind(")") + 2 :].split()[0] == expected:
            return
        time.sleep(0.001)
    raise AssertionError(f"pid {pid} did not reach {expected}")


def test_registry_records_post_exec_identity_without_argv_or_environment(record_property: Any) -> None:
    registry = ChildLifecycleRegistry()
    registry.install()
    process = None
    try:
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(5)", "secret-stream-key"],
            env={**os.environ, "SECRET_STREAM_KEY": "must-not-be-stored"},
        )
        with registry.guard():
            entry = registry.snapshot_locked()[process.pid]
        assert entry["registered_post_exec"] is True
        assert entry["parent_pid"] == os.getpid()
        assert entry["role"] == "AUXILIARY"
        serialized = repr(entry)
        assert "secret-stream-key" not in serialized
        assert "SECRET_STREAM_KEY" not in serialized
        assert "must-not-be-stored" not in serialized
        record_property(
            "cra_evidence",
            json.dumps({"registered": True, "credential_field_count": 0, "parent_matches": entry["parent_pid"] == os.getpid()}),
        )
    finally:
        if process is not None:
            process.terminate()
            process.wait(timeout=3)
        registry.close()


def test_registry_retains_registered_zombie_until_reaped() -> None:
    registry = ChildLifecycleRegistry()
    registry.install()
    process = None
    try:
        process = subprocess.Popen([sys.executable, "-c", "pass"])
        wait_for_state(process.pid, "Z")
        with registry.guard():
            entry = registry.snapshot_locked()[process.pid]
        assert entry["start_ticks"] > 0
        assert entry["registered_post_exec"] is True
    finally:
        if process is not None:
            process.wait(timeout=3)
        registry.close()


def test_registry_returns_reaped_identity_once_then_prunes_it(record_property: Any) -> None:
    registry = ChildLifecycleRegistry()
    registry.install()
    process = None
    try:
        process = subprocess.Popen([sys.executable, "-c", "pass"])
        pid = process.pid
        process.wait(timeout=3)
        with registry.guard():
            during_race = registry.snapshot_locked()
            after_prune = registry.snapshot_locked()
        observed = {
            "registered_identity_retained": pid in during_race,
            "pruned_after_snapshot": pid not in after_prune,
        }
        record_property("cra_evidence", json.dumps(observed))
        assert observed == {"registered_identity_retained": True, "pruned_after_snapshot": True}
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            process.wait(timeout=3)
        registry.close()


def test_registry_keeps_pid_identity_across_auxiliary_exec(record_property: Any) -> None:
    registry = ChildLifecycleRegistry()
    registry.install()
    process = None
    try:
        process = subprocess.Popen(["/bin/sh", "-c", "sleep 0.05; exec sleep 5"])
        with registry.guard():
            entry = registry.snapshot_locked()[process.pid]
        deadline = time.monotonic() + 2
        current = None
        while time.monotonic() < deadline:
            meta = Path(f"/proc/{process.pid}/exe").stat()
            current = (meta.st_dev, meta.st_ino)
            if current != (entry["executable_device"], entry["executable_inode"]):
                break
            time.sleep(0.01)
        assert current is not None
        assert current != (entry["executable_device"], entry["executable_inode"])
        with registry.guard():
            retained = registry.snapshot_locked()[process.pid]
        assert retained["start_ticks"] == entry["start_ticks"]
        record_property(
            "cra_evidence",
            json.dumps({"exec_changed": True, "pid_identity_retained": True, "credential_field_count": 0}),
        )
    finally:
        if process is not None:
            process.terminate()
            process.wait(timeout=3)
        registry.close()


def test_unregistered_fork_is_not_misclassified_as_owner_spawn() -> None:
    registry = ChildLifecycleRegistry()
    registry.install()
    pid = os.fork()
    if pid == 0:
        time.sleep(5)
        os._exit(0)
    try:
        with registry.guard():
            assert pid not in registry.snapshot_locked()
    finally:
        os.kill(pid, 15)
        os.waitpid(pid, 0)
        registry.close()


def test_observation_guard_serializes_popen_return_and_registration() -> None:
    registry = ChildLifecycleRegistry()
    registry.install()
    entered = threading.Event()
    result: list[subprocess.Popen[bytes]] = []

    def spawn() -> None:
        entered.set()
        result.append(subprocess.Popen([sys.executable, "-c", "pass"]))

    try:
        with registry.guard():
            thread = threading.Thread(target=spawn)
            thread.start()
            assert entered.wait(timeout=1)
            time.sleep(0.02)
            assert result == []
        thread.join(timeout=3)
        assert not thread.is_alive()
        result[0].wait(timeout=3)
    finally:
        for process in result:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=3)
        registry.close()


def test_close_restores_original_popen() -> None:
    original = subprocess.Popen
    registry = ChildLifecycleRegistry()
    registry.install()
    assert subprocess.Popen is not original
    registry.close()
    assert subprocess.Popen is original
