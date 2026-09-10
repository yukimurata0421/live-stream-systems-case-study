from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from cra_dell_recovery.errors import LedgerUnavailable
from cra_dell_recovery.sqlite import SQLiteLedger

UNRESOLVED_COMMAND_STATES = frozenset(
    {
        "COMMITTED",
        "OUTBOX_PENDING",
        "SENT",
        "ACCEPTED",
        "EXECUTION_STARTED",
        "EFFECT_OBSERVED",
        "EFFECT_FAILED",
        "OUTCOME_UNKNOWN",
        "VERIFYING",
        "VERIFICATION_UNKNOWN",
    }
)


def _normalized_schema(rows: Any) -> dict[tuple[str, str], tuple[str, str]]:
    return {(str(row[0]), str(row[1])): (str(row[2]), " ".join(str(row[3]).split())) for row in rows if row[3] is not None}


def _expected_schema(migration: Path) -> dict[tuple[str, str], tuple[str, str]]:
    candidates = sorted(migration.parent.glob("[0-9][0-9][0-9]_*.sql"))
    if migration not in candidates:
        candidates.insert(0, migration)
    reference = sqlite3.connect(":memory:", isolation_level=None)
    try:
        reference.execute("PRAGMA foreign_keys=ON")
        reference.execute("PRAGMA trusted_schema=OFF")
        reference.executescript(migration.read_text(encoding="utf-8"))
        reference.execute(
            """CREATE TABLE schema_migrations (
                   migration_name TEXT PRIMARY KEY,
                   sha256 TEXT NOT NULL,
                   applied_at TEXT NOT NULL
               ) STRICT"""
        )
        for candidate in candidates:
            if candidate != migration:
                reference.executescript(candidate.read_text(encoding="utf-8"))
        return _normalized_schema(
            reference.execute(
                """SELECT type,name,tbl_name,sql FROM sqlite_schema
                   WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL
                   ORDER BY type,name"""
            )
        )
    finally:
        reference.close()


def verify_central_schema_contract(ledger: SQLiteLedger) -> None:
    row = ledger.read_one("SELECT sql FROM sqlite_schema WHERE type='index' AND name='one_unresolved_command_per_target'")
    if row is None or row[0] is None:
        raise LedgerUnavailable("CENTRAL_UNRESOLVED_INDEX_MISSING")
    sql = str(row[0])
    where_index = sql.upper().find("WHERE")
    if where_index < 0:
        raise LedgerUnavailable("CENTRAL_UNRESOLVED_INDEX_WHERE_MISSING")
    where = sql[where_index:]
    actual = frozenset(re.findall(r"'([A-Z_]+)'", where))
    if actual != UNRESOLVED_COMMAND_STATES:
        raise LedgerUnavailable("CENTRAL_UNRESOLVED_STATE_CONTRACT_DRIFT")
    logical = ledger.read_one("SELECT sql FROM sqlite_schema WHERE type='index' AND name='one_central_effect_per_logical_generation'")
    if logical is None or logical[0] is None:
        raise LedgerUnavailable("CENTRAL_LOGICAL_GENERATION_INDEX_MISSING")
    normalized = " ".join(str(logical[0]).upper().split())
    if (
        "CREATE UNIQUE INDEX" not in normalized
        or "LOGICAL_GENERATION_SCOPE_ID" not in normalized
        or "WHERE LOGICAL_GENERATION_SCOPE_ID IS NOT NULL" not in normalized
    ):
        raise LedgerUnavailable("CENTRAL_LOGICAL_GENERATION_INDEX_DRIFT")
    actual_schema = _normalized_schema(
        ledger.read_all(
            """SELECT type,name,tbl_name,sql FROM sqlite_schema
               WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL
               ORDER BY type,name"""
        )
    )
    if actual_schema != _expected_schema(ledger.migration):
        raise LedgerUnavailable("CENTRAL_SCHEMA_CONTRACT_DRIFT")
