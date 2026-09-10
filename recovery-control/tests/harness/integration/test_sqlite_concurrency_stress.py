from __future__ import annotations

from pathlib import Path

from cra_harness.runner.live_sqlite_probe import run_live_probe
from cra_harness.runner.sqlite_concurrency import (
    _direct_connection_violations,
    _negative_control_results,
    run_stress_seed,
)
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.storage import DellStore

ROOT = Path(__file__).resolve().parents[3]


def test_sqlite_concurrency_negative_controls_are_all_detected() -> None:
    controls = _negative_control_results()
    assert [item["scenario_id"] for item in controls[-5:]] == ["NC-16", "NC-17", "NC-18", "NC-19", "NC-20"]
    assert len(controls) == 20
    assert all(item["detected"] for item in controls)


def test_dell_runtime_has_no_direct_shared_writer_connection_reads() -> None:
    assert _direct_connection_violations(ROOT) == []


def test_sqlite_concurrency_stress_smoke(tmp_path: Path) -> None:
    result = run_stress_seed(tmp_path, seed=20260823, operation_budget=900)
    assert result["result"] == "PASS"
    assert all(result["forced_overlap_hits"].values())
    assert not any(result["bad_counts"].values())
    assert result["integrity_check"] == "ok"
    assert result["physical_attempt_count"] == 0


def test_live_probe_requires_real_checkpoint_read_overlaps(tmp_path: Path) -> None:
    database = tmp_path / "agent.sqlite3"
    store = DellStore(database, ROOT / "migrations/dell/001_initial.sql")
    store.bootstrap(
        agent_id="probe-agent",
        installation_id="probe-installation",
        host_id="probe-host",
        host_boot_id="probe-boot",
        target_id="probe-target",
    )
    store.install_reconciliation(
        target_id="probe-target",
        reconciliation_id="probe-reconciliation",
        challenge_id="probe-challenge",
        nonce="probe-nonce",
        new_epoch=1,
        session_id="probe-session",
        controller_instance_id="probe-controller",
    )
    store.close()
    summary = run_live_probe(database, tmp_path / "evidence", target_id="probe-target", cycles=3, reader_hold_seconds=0.01)
    assert summary["result"] == "PASS"
    assert summary["checkpoint_read_overlap_hits"] == 3
    assert summary["checkpoint_modes"] == {"PASSIVE": 1, "FULL": 1, "TRUNCATE": 1}


def test_critical_db_uncertainty_fails_closed_without_lease_renewal(environment, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    lease = AgentAuthorityLease(environment.dell, environment.agent_codec)
    environment.dell.set_process_lease_valid("stream-target", True)

    def missing(_: str) -> None:
        raise KeyError("simulated missing authority row")

    monkeypatch.setattr(environment.dell, "fence", missing)
    assert lease.tick("stream-target") == "SAFE_BLOCKED"
    assert environment.dell.process_lease_valid("stream-target") is False
