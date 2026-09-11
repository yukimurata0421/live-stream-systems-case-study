from __future__ import annotations

from pathlib import Path


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def validate_isolated_database_path(
    database: Path,
    *,
    source_state_root: Path,
    migration_source_repository: Path,
) -> Path:
    """Reject DB placement that could mix v4 writes into the v3 source tree."""

    database = Path(database).expanduser().resolve()
    state_root = Path(source_state_root).expanduser().resolve()
    source_repository = Path(migration_source_repository).expanduser().resolve()
    if database == state_root or _is_within(database, state_root):
        raise ValueError("isolated v4 database must not be placed in the read-only source state root")
    if database == source_repository or _is_within(database, source_repository):
        raise ValueError("isolated v4 database must not be placed in the stream_v3 migration source repository")
    return database


def validate_isolated_output_path(
    output: Path,
    *,
    source_state_root: Path,
    migration_source_repository: Path,
) -> Path:
    """Apply the same no-write boundary to non-database shadow artifacts."""

    return validate_isolated_database_path(
        output,
        source_state_root=source_state_root,
        migration_source_repository=migration_source_repository,
    )
