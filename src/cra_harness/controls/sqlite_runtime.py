from __future__ import annotations

import sqlite3
import stat
import time
from contextlib import closing, suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

FIXED_SQLITE_VERSION = "3.51.3"
FIXED_SQLITE_LIBRARY = Path(".runtime/sqlite-3.51.3/install/lib/libsqlite3.so.3.51.3")


@dataclass(frozen=True)
class FixedSQLiteRuntimeIdentity:
    required_version: str
    runtime_version: str
    expected_library: str
    loaded_library: str | None
    expected_library_regular: bool
    version_matches: bool
    library_matches: bool

    @property
    def passed(self) -> bool:
        return self.expected_library_regular and self.version_matches and self.library_matches

    @property
    def classification(self) -> str:
        return "PASS" if self.passed else "ENVIRONMENT_FAILURE"

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "classification": self.classification, "passed": self.passed}


def evaluate_fixed_sqlite_runtime(
    project_root: Path,
    *,
    runtime_version: str,
    loaded_library: str | None,
) -> FixedSQLiteRuntimeIdentity:
    expected_entry = project_root.resolve() / FIXED_SQLITE_LIBRARY
    try:
        expected_regular = stat.S_ISREG(expected_entry.lstat().st_mode)
    except OSError:
        expected_regular = False
    expected = expected_entry.resolve()
    loaded_resolved: str | None = None
    if loaded_library is not None:
        try:
            loaded_resolved = str(Path(loaded_library).resolve(strict=True))
        except OSError:
            loaded_resolved = None
    return FixedSQLiteRuntimeIdentity(
        required_version=FIXED_SQLITE_VERSION,
        runtime_version=runtime_version,
        expected_library=str(expected),
        loaded_library=loaded_resolved,
        expected_library_regular=expected_regular,
        version_matches=runtime_version == FIXED_SQLITE_VERSION,
        library_matches=expected_regular and loaded_resolved == str(expected),
    )


def inspect_fixed_sqlite_runtime(project_root: Path) -> FixedSQLiteRuntimeIdentity:
    return evaluate_fixed_sqlite_runtime(
        project_root,
        runtime_version=sqlite3.sqlite_version,
        loaded_library=loaded_sqlite_library(),
    )


def fixed_sqlite_failure_message(identity: FixedSQLiteRuntimeIdentity) -> str:
    return (
        "CRA_FULL_REGRESSION_ENVIRONMENT_FAILURE: "
        f"required SQLite={identity.required_version} from {identity.expected_library}; "
        f"actual SQLite={identity.runtime_version}, loaded_library={identity.loaded_library or 'NOT_FOUND'}. "
        "Run tools/run_full_regression.sh instead of invoking the full pytest suite directly."
    )


def is_full_regression_selection(project_root: Path, selection_args: list[str] | tuple[str, ...]) -> bool:
    if not selection_args:
        return True
    if len(selection_args) != 1 or "::" in selection_args[0]:
        return False
    try:
        selected = Path(selection_args[0])
        if not selected.is_absolute():
            selected = project_root / selected
        return selected.resolve() == (project_root.resolve() / "tests")
    except OSError:
        return False


@dataclass(frozen=True)
class SQLiteProbeResult:
    runtime_version: str
    source_id: str
    loaded_library: str | None
    journal_mode: str
    synchronous: int
    multi_connection_visible_rows: int
    writer_serialization_error: str
    wal_size_before_checkpoint_bytes: int
    wal_size_after_checkpoint_bytes: int
    checkpoint_result: tuple[int, int, int]
    backup_rows: int
    restored_rows: int
    backup_integrity: str
    restore_integrity: str
    io_failure_error: str
    io_failure_error_code: int | None
    timings_ms: dict[str, float]

    @property
    def functional_gate_passed(self) -> bool:
        return all(
            (
                self.journal_mode == "wal",
                self.synchronous == 2,
                self.multi_connection_visible_rows == 3,
                self.writer_serialization_error == "database is locked",
                self.wal_size_before_checkpoint_bytes > 0,
                self.wal_size_after_checkpoint_bytes == 0,
                self.checkpoint_result[0] == 0,
                self.backup_rows == 3,
                self.restored_rows == 3,
                self.backup_integrity == "ok",
                self.restore_integrity == "ok",
                self.io_failure_error == "attempt to write a readonly database",
                self.io_failure_error_code == sqlite3.SQLITE_READONLY,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "functional_gate_passed": self.functional_gate_passed}


def loaded_sqlite_library() -> str | None:
    maps = Path("/proc/self/maps")
    if not maps.exists():
        return None
    candidates = {
        fields[-1] for line in maps.read_text(encoding="utf-8").splitlines() if "libsqlite3.so" in line and (fields := line.split())
    }
    return sorted(candidates)[0] if candidates else None


def _elapsed_ms(started_ns: int) -> float:
    return round((time.perf_counter_ns() - started_ns) / 1_000_000, 6)


def run_sqlite_probe(root: Path) -> SQLiteProbeResult:
    root.mkdir(parents=True, exist_ok=True)
    database = root / "central.sqlite3"
    backup = root / "online-backup.sqlite3"
    restored = root / "restored.sqlite3"
    for path in (database, backup, restored):
        path.unlink(missing_ok=True)

    timings: dict[str, float] = {}
    writer = sqlite3.connect(database, isolation_level=None, timeout=0.0)
    reader = sqlite3.connect(database, isolation_level=None, timeout=0.0)
    contender = sqlite3.connect(database, isolation_level=None, timeout=0.0)
    try:
        started = time.perf_counter_ns()
        journal_mode = str(writer.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        writer.execute("PRAGMA synchronous=FULL")
        synchronous = int(writer.execute("PRAGMA synchronous").fetchone()[0])
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        writer.execute("BEGIN IMMEDIATE")
        writer.executemany("INSERT INTO events(value) VALUES (?)", [("one",), ("two",), ("three",)])
        writer.execute("COMMIT")
        timings["command_db_transaction"] = _elapsed_ms(started)

        started = time.perf_counter_ns()
        visible_rows = int(reader.execute("SELECT count(*) FROM events").fetchone()[0])
        timings["multi_connection_read"] = _elapsed_ms(started)
        wal_path = Path(f"{database}-wal")
        wal_size_before = wal_path.stat().st_size

        started = time.perf_counter_ns()
        writer.execute("BEGIN IMMEDIATE")
        try:
            contender.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as error:
            serialization_error = str(error)
        else:  # pragma: no cover - an invariant violation, kept explicit for the probe report.
            serialization_error = "NO_ERROR"
            contender.execute("ROLLBACK")
        writer.execute("ROLLBACK")
        contender.execute("BEGIN IMMEDIATE")
        contender.execute("ROLLBACK")
        timings["writer_serialization"] = _elapsed_ms(started)

        started = time.perf_counter_ns()
        with closing(sqlite3.connect(backup)) as destination:
            writer.backup(destination)
        timings["online_backup"] = _elapsed_ms(started)

        with closing(sqlite3.connect(backup)) as backup_reader:
            backup_rows = int(backup_reader.execute("SELECT count(*) FROM events").fetchone()[0])
            backup_integrity = str(backup_reader.execute("PRAGMA integrity_check").fetchone()[0])
            with closing(sqlite3.connect(restored)) as restore_destination:
                backup_reader.backup(restore_destination)

        with closing(sqlite3.connect(restored)) as restored_reader:
            restored_rows = int(restored_reader.execute("SELECT count(*) FROM events").fetchone()[0])
            restore_integrity = str(restored_reader.execute("PRAGMA integrity_check").fetchone()[0])

        reader.close()
        started = time.perf_counter_ns()
        checkpoint_row = writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        timings["checkpoint"] = _elapsed_ms(started)
        checkpoint_result = (int(checkpoint_row[0]), int(checkpoint_row[1]), int(checkpoint_row[2]))
        wal_size_after = wal_path.stat().st_size if wal_path.exists() else 0
        source_id = str(writer.execute("SELECT sqlite_source_id()").fetchone()[0])
    finally:
        writer.close()
        with suppress(sqlite3.Error):
            reader.close()
        contender.close()

    readonly = sqlite3.connect(f"file:{database}?mode=ro", uri=True, isolation_level=None)
    try:
        try:
            readonly.execute("BEGIN IMMEDIATE")
            readonly.execute("INSERT INTO events(value) VALUES ('forbidden')")
        except sqlite3.OperationalError as error:
            io_failure_error = str(error)
            io_failure_error_code = getattr(error, "sqlite_errorcode", None)
            with suppress(sqlite3.Error):
                readonly.execute("ROLLBACK")
        else:  # pragma: no cover - an invariant violation, kept explicit for the probe report.
            io_failure_error = "NO_ERROR"
            io_failure_error_code = None
    finally:
        readonly.close()

    return SQLiteProbeResult(
        runtime_version=sqlite3.sqlite_version,
        source_id=source_id,
        loaded_library=loaded_sqlite_library(),
        journal_mode=journal_mode,
        synchronous=synchronous,
        multi_connection_visible_rows=visible_rows,
        writer_serialization_error=serialization_error,
        wal_size_before_checkpoint_bytes=wal_size_before,
        wal_size_after_checkpoint_bytes=wal_size_after,
        checkpoint_result=checkpoint_result,
        backup_rows=backup_rows,
        restored_rows=restored_rows,
        backup_integrity=backup_integrity,
        restore_integrity=restore_integrity,
        io_failure_error=io_failure_error,
        io_failure_error_code=io_failure_error_code,
        timings_ms=timings,
    )
