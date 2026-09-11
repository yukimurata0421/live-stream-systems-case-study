#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Create a consistent read-only SQLite import snapshot")
    result.add_argument("--source", type=Path, required=True)
    result.add_argument("--target", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    source = args.source.expanduser().resolve()
    raw_target = args.target.expanduser()
    if raw_target.is_symlink():
        raise ValueError("target must not be a symlink")
    target = raw_target.parent.resolve() / raw_target.name
    if source == target:
        raise ValueError("source and target must differ")
    if not source.is_file():
        raise FileNotFoundError(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with closing(sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True)) as input_db:
            with closing(sqlite3.connect(temporary)) as output_db, output_db:
                input_db.backup(output_db)
        with closing(sqlite3.connect(temporary)) as output_db:
            with output_db:
                journal_mode = str(output_db.execute("PRAGMA journal_mode=DELETE").fetchone()[0])
                integrity = str(output_db.execute("PRAGMA integrity_check").fetchone()[0])
                schema_version = int(output_db.execute("PRAGMA user_version").fetchone()[0])
        if journal_mode.lower() != "delete" or integrity != "ok":
            raise RuntimeError(
                f"import snapshot validation failed: journal={journal_mode} integrity={integrity}"
            )
        temporary.chmod(0o600)
        os.replace(temporary, target)
        directory_fd = os.open(
            target.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()
    print(
        json.dumps(
            {
                "schema": "monitoring_v4.sqlite_import_snapshot.v1",
                "source": str(source),
                "target": str(target),
                "bytes": target.stat().st_size,
                "journal_mode": "delete",
                "integrity": "ok",
                "schema_version": schema_version,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
