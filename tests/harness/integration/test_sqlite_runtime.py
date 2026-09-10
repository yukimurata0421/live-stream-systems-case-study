from __future__ import annotations

import sqlite3
from pathlib import Path

from cra_harness.controls.sqlite_runtime import run_sqlite_probe


def test_sqlite_durability_contract(tmp_path: Path) -> None:
    result = run_sqlite_probe(tmp_path)
    assert result.functional_gate_passed


def test_sqlite_runtime_reports_loaded_library(tmp_path: Path) -> None:
    result = run_sqlite_probe(tmp_path)
    assert result.runtime_version == sqlite3.sqlite_version
    assert result.loaded_library is not None
    assert "libsqlite3.so" in result.loaded_library


def test_sqlite_online_backup_and_restore_are_independent(tmp_path: Path) -> None:
    result = run_sqlite_probe(tmp_path)
    assert (result.backup_rows, result.restored_rows) == (3, 3)
    assert (result.backup_integrity, result.restore_integrity) == ("ok", "ok")


def test_sqlite_checkpoint_truncates_wal(tmp_path: Path) -> None:
    result = run_sqlite_probe(tmp_path)
    assert result.wal_size_before_checkpoint_bytes > 0
    assert result.checkpoint_result[0] == 0
    assert result.wal_size_after_checkpoint_bytes == 0
