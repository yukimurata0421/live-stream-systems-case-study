from __future__ import annotations

import json
import os

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.storage.postgres import PostgresMonitoringRepository
from stream_monitoring_v4.storage.role_contract import table_grant_statements


ROLE_PASSWORD_ENV = {
    "v4_migrator": "STREAM_V4_MIGRATOR_DB_PASSWORD",
    "v4_core_rw": "STREAM_V4_CORE_DB_PASSWORD",
    "v4_exporter_ro": "STREAM_V4_EXPORTER_DB_PASSWORD",
    "v4_reporter_ro": "STREAM_V4_REPORTER_DB_PASSWORD",
    "v4_backup_ro": "STREAM_V4_BACKUP_DB_PASSWORD",
    "v4_notifier_rw": "STREAM_V4_NOTIFIER_DB_PASSWORD",
    "v4_maintenance_rw": "STREAM_V4_MAINTENANCE_DB_PASSWORD",
}


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def _connection_kwargs(*, user: str | None = None, password: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {
        "host": _required("PGHOST"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": _required("PGDATABASE"),
        "user": user or _required("PGUSER"),
        "password": password if password is not None else _required("PGPASSWORD"),
        "connect_timeout": 5,
        "application_name": "stream-monitoring-v4-migration",
    }
    return result


def _ensure_roles(admin: object, passwords: dict[str, str]) -> None:
    from psycopg import sql

    for role, password in passwords.items():
        exists = admin.execute(
            "SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)
        ).fetchone()
        if exists:
            statement = sql.SQL(
                "ALTER ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD {}"
            ).format(sql.Identifier(role), sql.Literal(password))
        else:
            statement = sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD {}"
            ).format(sql.Identifier(role), sql.Literal(password))
        admin.execute(statement)


def _grant_schema(admin: object, *, database: str) -> None:
    from psycopg import sql

    roles = tuple(ROLE_PASSWORD_ENV)
    admin.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    admin.execute("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC")
    for role in roles:
        admin.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database), sql.Identifier(role)
            )
        )
        admin.execute(
            sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role))
        )
        admin.execute(
            sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {}").format(
                sql.Identifier(role)
            )
        )
    admin.execute("GRANT CREATE ON SCHEMA public TO v4_migrator")
    admin.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO v4_migrator")
    for statement in table_grant_statements():
        admin.execute(statement)
    admin.execute(
        sql.SQL("GRANT TEMPORARY ON DATABASE {} TO v4_maintenance_rw").format(
            sql.Identifier(database)
        )
    )
    for role in (
        "v4_core_rw",
        "v4_exporter_ro",
        "v4_reporter_ro",
        "v4_backup_ro",
        "v4_notifier_rw",
        "v4_maintenance_rw",
    ):
        admin.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE v4_migrator IN SCHEMA public "
                "REVOKE ALL ON TABLES FROM {}"
            ).format(sql.Identifier(role))
        )
    # Future schema changes must explicitly update the role/table matrix above.
    # Only the backup role intentionally receives new-table SELECT by default.
    admin.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE v4_migrator IN SCHEMA public "
        "GRANT SELECT ON TABLES TO v4_backup_ro"
    )


def main() -> int:
    try:
        import psycopg
        from psycopg.conninfo import make_conninfo
    except ImportError as exc:  # pragma: no cover - container integration path
        raise RuntimeError("postgres migration requires psycopg[binary]") from exc

    passwords = {role: _required(env_name) for role, env_name in ROLE_PASSWORD_ENV.items()}
    admin = psycopg.connect(**_connection_kwargs(), autocommit=False)
    try:
        with admin.transaction():
            _ensure_roles(admin, passwords)
            admin.execute("GRANT USAGE ON SCHEMA public TO v4_migrator")
            admin.execute("GRANT CREATE ON SCHEMA public TO v4_migrator")
    finally:
        admin.close()

    migrator_kwargs = _connection_kwargs(
        user="v4_migrator",
        password=passwords["v4_migrator"],
    )
    repository = PostgresMonitoringRepository(
        make_conninfo(**migrator_kwargs),
        application_name="stream-monitoring-v4-migration",
        statement_timeout_ms=300_000,
        lock_timeout_ms=30_000,
    )
    now_at = utc_text(int(__import__("time").time()))
    repository.initialize(applied_at=now_at)

    admin = psycopg.connect(**_connection_kwargs(), autocommit=False)
    try:
        with admin.transaction():
            _grant_schema(admin, database=_required("PGDATABASE"))
            rows = admin.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
    finally:
        admin.close()
    print(
        json.dumps(
            {
                "schema": "monitoring_v4.postgres_migration.v1",
                "applied_at": now_at,
                "schema_versions": [int(row[0]) for row in rows],
                "roles": sorted(passwords),
                "notification_delivery_enabled": False,
                "runtime_mutation_enabled": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
