from __future__ import annotations

from dataclasses import dataclass
import re


_TABLE = re.compile(
    r"CREATE TABLE IF NOT EXISTS\s+([a-z_]+)\s*\((.*?)\);",
    re.IGNORECASE | re.DOTALL,
)
_INDEX = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX IF NOT EXISTS\s+([a-z_]+)",
    re.IGNORECASE,
)
_ADD_COLUMN = re.compile(
    r"ALTER TABLE\s+([a-z_]+)\s+ADD COLUMN(?: IF NOT EXISTS)?\s+\"?([a-z_]+)\"?",
    re.IGNORECASE,
)
_NON_COLUMN_PREFIXES = {"PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "CONSTRAINT"}


@dataclass(frozen=True)
class SchemaSurface:
    tables: tuple[tuple[str, tuple[str, ...]], ...]
    indexes: tuple[str, ...]

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(name for name, _columns in self.tables)


def _comma_separated(definition: str) -> tuple[str, ...]:
    result: list[str] = []
    start = 0
    depth = 0
    quote = ""
    for index, character in enumerate(definition):
        if quote:
            if character == quote:
                quote = ""
            continue
        if character in {"'", '"'}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            result.append(definition[start:index])
            start = index + 1
    result.append(definition[start:])
    return tuple(result)


def schema_surface(ddl: str) -> SchemaSurface:
    tables: dict[str, tuple[str, ...]] = {}
    for match in _TABLE.finditer(ddl):
        columns: list[str] = []
        for raw in _comma_separated(match.group(2)):
            definition = raw.strip()
            if not definition:
                continue
            name = definition.split(None, 1)[0].strip('"')
            # Table constraints may be written either as ``UNIQUE (...)`` or
            # ``UNIQUE(...)``.  The latter must not be mistaken for a column
            # when deriving the expected live schema surface.
            leading_keyword = re.split(r"[\s(]", definition, maxsplit=1)[0]
            if leading_keyword.upper() not in _NON_COLUMN_PREFIXES:
                columns.append(name)
        tables[match.group(1)] = tuple(columns)
    for table, column in _ADD_COLUMN.findall(ddl):
        existing = tables.get(table)
        if existing is not None and column not in existing:
            tables[table] = (*existing, column)
    return SchemaSurface(
        tables=tuple(sorted(tables.items())),
        indexes=tuple(sorted(set(_INDEX.findall(ddl)))),
    )


def postgres_schema_surface(connection: object) -> SchemaSurface:
    columns: dict[str, list[str]] = {}
    rows = connection.execute(  # type: ignore[attr-defined]
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema='public' ORDER BY table_name, ordinal_position"
    ).fetchall()
    for row in rows:
        table = str(row[0])
        columns.setdefault(table, []).append(str(row[1]))
    indexes = connection.execute(  # type: ignore[attr-defined]
        "SELECT indexname FROM pg_indexes "
        "WHERE schemaname='public' AND indexname LIKE '%\\_idx' ESCAPE '\\' "
        "ORDER BY indexname"
    ).fetchall()
    return SchemaSurface(
        tables=tuple((table, tuple(names)) for table, names in sorted(columns.items())),
        indexes=tuple(sorted(str(row[0]) for row in indexes)),
    )


LIVE_SNAPSHOT_REFERENCES = (
    ("domain_current", ("snapshot_id", "domain"), "current_snapshots"),
    ("incident_candidates", ("snapshot_id", "domain"), "current_snapshots"),
    ("incident_transitions", ("current_snapshot_id", "domain"), "current_snapshots"),
)

SQLITE_LIVE_REFERENCE_TRIGGERS = (
    "domain_current_snapshot_insert_guard",
    "domain_current_snapshot_update_guard",
    "incident_candidate_snapshot_insert_guard",
    "incident_candidate_snapshot_update_guard",
    "incident_transition_snapshot_insert_guard",
    "incident_transition_snapshot_update_guard",
    "current_snapshot_delete_guard",
    "current_snapshot_identity_update_guard",
)

POSTGRES_LIVE_REFERENCE_CONSTRAINTS = (
    "domain_current_snapshot_reference",
    "incident_candidates_snapshot_reference",
    "incident_transitions_snapshot_reference",
)
