from __future__ import annotations

import os
import shutil
import sqlite3
import stat
import tempfile
from contextlib import closing
from pathlib import Path


class SQLiteMaintenanceRepositoryMixin:
    """SQLite-only backup, restore, health, and forensic-copy operations."""

    def backup(self, target: Path) -> None:
        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.resolve() == self.path.resolve():
            raise ValueError("backup target must differ from source database")
        if os.path.lexists(target):
            raise FileExistsError(f"backup target already exists: {target}")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            with self.connection(read_only=True) as source:
                with closing(sqlite3.connect(temporary)) as destination:
                    source.backup(destination)
                    result = destination.execute("PRAGMA integrity_check").fetchone()
                    if result is None or str(result[0]) != "ok":
                        raise RuntimeError("backup database failed integrity_check")
            self._publish_database_file(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    @classmethod
    def restore(cls, backup: Path, target: Path, *, applied_at: str):
        backup = Path(backup)
        target = Path(target)
        if os.path.lexists(target):
            raise FileExistsError(f"restore target already exists: {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            backup,
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        descriptor_stat = os.fstat(descriptor)
        if not stat.S_ISREG(descriptor_stat.st_mode):
            os.close(descriptor)
            raise ValueError("restore source must be a regular file")
        temporary_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
        )
        os.close(temporary_descriptor)
        temporary = Path(temporary_name)
        try:
            source_uri = (
                f"{Path(f'/proc/self/fd/{descriptor}').as_uri()}"
                "?mode=ro&immutable=1"
            )
            with closing(sqlite3.connect(source_uri, uri=True)) as source:
                with closing(sqlite3.connect(temporary)) as destination:
                    source.backup(destination)
            restored = cls(temporary)
            restored.initialize(applied_at=applied_at)
            if restored.integrity_check() != "ok":
                raise RuntimeError("restored database failed integrity_check")
            cls._publish_database_file(temporary, target)
            return cls(target)
        finally:
            os.close(descriptor)
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _publish_database_file(source: Path, target: Path) -> None:
        descriptor = os.open(
            source,
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("database publication source must be a regular file")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.link(source, target, follow_symlinks=False)
        directory_descriptor = os.open(
            target.parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)

    def integrity_check(self) -> str:
        with self.connection(read_only=True) as connection:
            return str(connection.execute("PRAGMA integrity_check").fetchone()[0])

    def ping(self) -> bool:
        try:
            with self.connection(read_only=True) as connection:
                row = connection.execute("SELECT 1 AS healthy").fetchone()
            return bool(row and int(row["healthy"]) == 1)
        except Exception:
            return False

    def copy_database_files_for_forensics(self, target_dir: Path) -> list[Path]:
        target_dir.mkdir(parents=True, exist_ok=True)
        copied: list[Path] = []
        for suffix in ("", "-wal", "-shm"):
            source = Path(str(self.path) + suffix)
            if source.exists():
                target = target_dir / source.name
                if target.exists():
                    raise FileExistsError(
                        f"forensics target already exists: {target}"
                    )
                shutil.copy2(source, target)
                copied.append(target)
        return copied
