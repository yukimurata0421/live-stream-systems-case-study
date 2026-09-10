from __future__ import annotations

import sqlite3

from cra_dell_recovery.sqlite import production_sqlite_gate


def test_production_sqlite_version_gate_fails_below_minimum() -> None:
    assert production_sqlite_gate("3.46.1") is False


def test_production_sqlite_version_gate_passes_at_minimum() -> None:
    assert production_sqlite_gate("3.51.3") is True


def test_backport_proof_can_satisfy_gate() -> None:
    assert production_sqlite_gate("3.46.1", backport_proven=True) is True


def test_runtime_version_is_exposed(environment: object) -> None:
    central = environment.central  # type: ignore[attr-defined]
    assert central.status.version == sqlite3.sqlite_version
    assert central.status.production_gate in {"PASS", "FAIL"}
