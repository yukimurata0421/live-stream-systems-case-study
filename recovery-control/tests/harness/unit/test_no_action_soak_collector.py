from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[3]


def _collector() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "collect_cra_no_action_soak",
        ROOT / "tools/collect_cra_no_action_soak.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_collector_imports_without_site_packages() -> None:
    environment = {"PYTHONPATH": str(ROOT / "src"), "PATH": os.environ["PATH"]}
    command = (
        "import runpy; "
        f"runpy.run_path({str(ROOT / 'tools/collect_cra_no_action_soak.py')!r}); "
        "import cra_no_action_soak.gate, cra_no_action_soak.sample"
    )
    subprocess.run([sys.executable, "-S", "-c", command], check=True, env=environment)


def test_run_accepts_explicit_inactive_systemd_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    result = subprocess.CompletedProcess(["ssh"], 3, stdout="inactive\n", stderr="")
    monkeypatch.setattr(collector.subprocess, "run", lambda *args, **kwargs: result)

    assert collector._run("arena-server", "systemctl is-active example.timer", allowed_returncodes=frozenset({0, 3})) == "inactive"


def test_formal_local_mode_forbids_remote_shell_and_skips_sudo(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="local\n", stderr="")

    monkeypatch.setenv("CRA_SOAK_LOCAL_DIRECT", "1")
    monkeypatch.setenv("CRA_SOAK_REMOTE_EXECUTION", "forbidden")
    monkeypatch.setattr(collector.subprocess, "run", run)

    assert collector._run(None, "printf local") == "local"
    assert calls == [["sh", "-c", "printf local"]]
    with pytest.raises(ValueError, match="SOAK_COLLECTOR_REMOTE_EXECUTION_FORBIDDEN"):
        collector._run("arena-server", "true")
    assert len(calls) == 1


def test_local_database_summary_uses_read_only_python_sqlite(tmp_path: Path) -> None:
    collector = _collector()
    database = tmp_path / "central.db"
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        connection.execute("CREATE TABLE recovery_authorizations (authorization_id TEXT PRIMARY KEY)")
        connection.execute("CREATE TABLE commands (command_id TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO recovery_authorizations VALUES ('authorization-1')")
        connection.commit()
    finally:
        connection.close()

    summary, integrity, journal_mode = collector._local_db_summary(
        database,
        "cra-no-action-0123456789ab",
        "2026-09-02T16:00:00.000Z",
    )

    assert summary == {
        "schema": "cra.no_action_db_summary.v1",
        "runtime_release_id": "cra-no-action-0123456789ab",
        "authorization_row_count": 1,
        "command_row_count": 0,
        "observed_at": "2026-09-02T16:00:00.000Z",
    }
    assert integrity == "ok"
    assert journal_mode == "wal"


def test_local_host_status_waits_only_for_expired_snapshot_refresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = _collector()
    path = tmp_path / "host-status.json"
    path.write_text("{}\n", encoding="utf-8")
    first = datetime(2026, 9, 2, 16, 30, tzinfo=UTC)
    second = first + timedelta(milliseconds=500)
    clock_values = iter((first, second))
    waits: list[float] = []

    class Contract:
        attempts = 0

        def decode(self, value: object, *, now: datetime) -> dict[str, object]:
            assert value == {"sequence": 2}
            self.attempts += 1
            if self.attempts == 1:
                raise ValueError("HOST_STATUS_EXPIRED")
            return {"accepted_at": now}

    monkeypatch.setattr("cra_no_action_soak.host_status.read_object", lambda _path: {"sequence": 2})
    result, accepted_at = collector._decode_local_host_status(
        Contract(),
        path,
        clock=lambda: next(clock_values),
        wait=waits.append,
    )

    assert result == {"accepted_at": second}
    assert accepted_at == second
    assert waits == [0.5]


def test_local_host_status_does_not_retry_contract_or_trust_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = _collector()
    path = tmp_path / "host-status.json"
    path.write_text("{}\n", encoding="utf-8")
    waits: list[float] = []

    class Contract:
        def decode(self, _value: object, *, now: datetime) -> object:
            raise ValueError("HOST_STATUS_SIGNATURE_INVALID")

    monkeypatch.setattr("cra_no_action_soak.host_status.read_object", lambda _path: {})
    with pytest.raises(ValueError, match="HOST_STATUS_SIGNATURE_INVALID"):
        collector._decode_local_host_status(Contract(), path, wait=waits.append)

    assert waits == []


def test_local_collector_unit_identity_is_bound_to_immutable_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = _collector()
    monkeypatch.setenv("CRA_IMMUTABLE_RELEASE_ID", "cra-no-action-0123456789ab")
    assert collector._collector_release_id() == "cra-no-action-0123456789ab"

    monkeypatch.setenv("CRA_IMMUTABLE_RELEASE_ID", "arena-cra-projection-0123456789ab")
    with pytest.raises(ValueError, match="SOAK_COLLECTOR_RUNTIME_RELEASE_INVALID"):
        collector._collector_release_id()


def test_configuration_digest_forces_a_canonical_locale(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    calls: list[str] = []

    def capture(_host: str | None, script: str) -> str:
        calls.append(script)
        return "1" * 64 + "  -"

    monkeypatch.setattr(collector, "_run", capture)

    assert collector._config_set_sha256("arena-server", "/config") == "1" * 64
    assert "LC_ALL=C sort -z" in calls[0]


def test_failed_invocation_counter_uses_systemd_failure_message_id(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    calls: list[str] = []

    def capture(_host: str | None, script: str) -> str:
        calls.append(script)
        return "2"

    monkeypatch.setattr(collector, "_run", capture)

    assert collector._unit_failure_count("arena-server", "example.service", "2026-08-30T19:10:00Z") == 2
    assert "MESSAGE_ID=d9b373ed55a64feb8242e02dbe79a49c" in calls[0]
    assert "example.service" in calls[0]


def test_persistent_invocation_identity_is_not_substituted_with_nrestarts(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    calls: list[str] = []

    def capture(_host: str | None, script: str) -> str:
        calls.append(script)
        return "a" * 32

    monkeypatch.setattr(collector, "_run", capture)

    assert collector._show_invocation_id("cra-01", "cra-runtime.service") == "a" * 32
    assert "InvocationID" in calls[0]
    assert "NRestarts" not in calls[0]


def test_persistent_invocation_identity_rejects_empty_inactive_unit(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    monkeypatch.setattr(collector, "_run", lambda *_args, **_kwargs: "")

    with pytest.raises(ValueError, match="SOAK_COLLECTOR_SYSTEMD_INVOCATION_ID_INVALID"):
        collector._show_invocation_id("cra-01", "missing.service")


def test_collector_recomputes_current_gate_after_durable_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = _collector()
    sample_path = tmp_path / "samples.jsonl"
    gate_path = tmp_path / "gate-current.json"
    releases = {"arena": "arena-a", "cra": "cra-a", "dell": "dell-a"}
    monkeypatch.setattr(
        collector,
        "evaluate_no_action_soak",
        lambda samples, *, expected_releases: {
            "sample_count": len(samples),
            "blockers": ["SOAK_SAMPLE_COUNT_INSUFFICIENT"],
            "expected_releases": expected_releases,
        },
    )

    report = collector._append_sample_and_write_gate(sample_path, gate_path, {"sample": 1}, releases)

    assert report["sample_count"] == 1
    assert report["blockers"] == ["SOAK_SAMPLE_COUNT_INSUFFICIENT"]
    assert json.loads(gate_path.read_text(encoding="utf-8")) == report


def test_collector_writes_terminal_marker_only_for_terminal_epoch_failure(tmp_path: Path) -> None:
    collector = _collector()
    terminal_path = tmp_path / "terminal-failure.json"
    detected_at = datetime(2026, 9, 1, 7, 30, tzinfo=UTC)

    assert (
        collector._write_terminal_gate(
            terminal_path,
            {"status": "NOT_YET_ELIGIBLE", "terminal_failure": False},
            detected_at=detected_at,
        )
        is False
    )
    assert not terminal_path.exists()

    gate = {
        "status": "FAIL",
        "terminal_failure": True,
        "terminal_blockers": ["SOAK_PERSISTENT_SERVICE_INVOCATION_DRIFT:CRA_RUNTIME"],
    }
    assert collector._write_terminal_gate(terminal_path, gate, detected_at=detected_at) is True
    marker = json.loads(terminal_path.read_text(encoding="utf-8"))
    assert marker == {
        "schema": "cra.no_action_soak_terminal_failure.v1",
        "detected_at": "2026-09-01T07:30:00.000Z",
        "gate": gate,
    }
    later = {**gate, "terminal_blockers": ["LATER_FAILURE"]}
    assert collector._write_terminal_gate(terminal_path, later, detected_at=detected_at + timedelta(seconds=30)) is False
    assert json.loads(terminal_path.read_text(encoding="utf-8")) == marker


def test_collector_serializes_append_and_gate_to_prevent_stale_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = _collector()
    sample_path = tmp_path / "samples.jsonl"
    gate_path = tmp_path / "gate-current.json"
    releases = {"arena": "arena-a", "cra": "cra-a", "dell": "dell-a"}
    first_evaluation_entered = threading.Event()
    second_evaluation_entered = threading.Event()
    release_first = threading.Event()
    errors: list[BaseException] = []

    def evaluate(samples: list[dict[str, object]], *, expected_releases: dict[str, str]) -> dict[str, object]:
        if len(samples) == 1:
            first_evaluation_entered.set()
            assert release_first.wait(timeout=2)
        elif len(samples) == 2:
            second_evaluation_entered.set()
        return {"sample_count": len(samples), "expected_releases": expected_releases}

    def append(value: int) -> None:
        try:
            collector._append_sample_and_write_gate(sample_path, gate_path, {"sample": value}, releases)
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(collector, "evaluate_no_action_soak", evaluate)
    first = threading.Thread(target=append, args=(1,))
    second = threading.Thread(target=append, args=(2,))
    first.start()
    assert first_evaluation_entered.wait(timeout=2)
    second.start()
    assert not second_evaluation_entered.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert second_evaluation_entered.is_set()
    assert json.loads(gate_path.read_text(encoding="utf-8"))["sample_count"] == 2


def test_last_sample_reuses_bounded_nofollow_evidence_reader(tmp_path: Path) -> None:
    collector = _collector()
    target = tmp_path / "target.jsonl"
    target.write_text('{"sample":1}\n', encoding="utf-8")
    symlink = tmp_path / "samples.jsonl"
    symlink.symlink_to(target)

    with pytest.raises(OSError):
        collector._last_sample(symlink)


def test_resource_capture_rejects_missing_persistent_process(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    monkeypatch.setattr(collector, "_show_integer", lambda host, unit, field: 0)

    with pytest.raises(ValueError, match="SOAK_COLLECTOR_RESOURCE_PROCESS_MISSING"):
        collector._resources("arena-server", ["persistent.service"])


def test_collect_measures_only_long_lived_resource_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = _collector()
    now = datetime(2026, 8, 30, 19, 10, tzinfo=UTC)
    releases = {
        "arena": "arena-cra-projection-0123456789ab",
        "cra": "cra-no-action-0123456789ab",
        "dell": "dell-observation-0123456789ab",
    }
    calls: list[tuple[str | None, list[str]]] = []

    class ResourceCallsCaptured(Exception):
        pass

    def capture_resources(host: str | None, units: list[str]) -> tuple[int, int]:
        calls.append((host, list(units)))
        if len(calls) == 3:
            raise ResourceCallsCaptured
        return 1, 1

    monkeypatch.setattr(
        collector,
        "_json",
        lambda host, path: {
            "status": "VALID",
            "valid_until": (now + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
            "target_identity": {"host_id": "dell-target"},
        },
    )
    monkeypatch.setattr(
        collector,
        "_db_summary",
        lambda host, release, observed_at: ({}, "ok", "wal"),
    )
    monkeypatch.setattr(collector, "_resources", capture_resources)

    with pytest.raises(ResourceCallsCaptured):
        collector.collect(
            {
                "arena_host": "arena-server",
                "cra_host": "cra-01",
                "legacy_arena_recovery_unit": "stream-v3-remote-recovery.timer",
                "releases": releases,
                "rehearsal_evidence_sha256": {},
                "epoch_started_at": "2026-08-30T19:10:00Z",
            },
            now=now,
        )

    assert calls == [
        (
            "arena-server",
            ["monitoring-v4-cra-projection-server@arena-cra-projection-0123456789ab.service"],
        ),
        ("cra-01", ["cra-runtime-no-action@cra-no-action-0123456789ab.service"]),
        (None, ["dell-observation-server@dell-observation-0123456789ab.service"]),
    ]
