from __future__ import annotations

import io
import json
import os
import sys
import unittest
from contextlib import redirect_stdout
from types import ModuleType
from unittest.mock import Mock, patch

from stream_monitoring_v4.commands import postgres_migrate


class _Result:
    def fetchall(self) -> list[tuple[int]]:
        return [(1,), (2,), (3,), (4,), (5,), (6,)]


class _Transaction:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection

    def __enter__(self) -> None:
        self.connection.in_transaction = True
        self.connection.transaction_entries += 1

    def __exit__(self, exc_type: object, _exc: object, _traceback: object) -> bool:
        self.connection.in_transaction = False
        if exc_type is None:
            self.connection.commits += 1
        else:
            self.connection.rollbacks += 1
        return False


class _Connection:
    def __init__(self) -> None:
        self.in_transaction = False
        self.transaction_entries = 0
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    def execute(self, _sql: str) -> _Result:
        if not self.in_transaction:
            raise AssertionError("migration SQL executed outside a transaction")
        return _Result()

    def close(self) -> None:
        self.closed = True


class PostgresMigrateCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.environment = {
            "PGHOST": "postgres",
            "PGPORT": "5432",
            "PGDATABASE": "stream_v4_integration",
            "PGUSER": "postgres",
            "PGPASSWORD": "admin-test-password",
        }
        for environment_name in postgres_migrate.ROLE_PASSWORD_ENV.values():
            self.environment[environment_name] = f"{environment_name.lower()}-test"

    @staticmethod
    def _fake_driver(connect: Mock) -> dict[str, ModuleType]:
        psycopg = ModuleType("psycopg")
        psycopg.connect = connect  # type: ignore[attr-defined]
        conninfo = ModuleType("psycopg.conninfo")
        conninfo.make_conninfo = Mock(return_value="migrator-conninfo")  # type: ignore[attr-defined]
        psycopg.conninfo = conninfo  # type: ignore[attr-defined]
        return {"psycopg": psycopg, "psycopg.conninfo": conninfo}

    def test_role_and_grant_mutations_are_separate_atomic_transactions(self) -> None:
        role_admin = _Connection()
        grant_admin = _Connection()
        repository = Mock()
        connect = Mock(side_effect=[role_admin, grant_admin])

        def ensure_roles(admin: _Connection, _passwords: dict[str, str]) -> None:
            self.assertIs(admin, role_admin)
            self.assertTrue(admin.in_transaction)

        def grant_schema(admin: _Connection, *, database: str) -> None:
            self.assertIs(admin, grant_admin)
            self.assertTrue(admin.in_transaction)
            self.assertEqual(database, "stream_v4_integration")

        output = io.StringIO()
        with patch.dict(os.environ, self.environment, clear=False), patch.dict(
            sys.modules, self._fake_driver(connect)
        ), patch.object(
            postgres_migrate, "_ensure_roles", side_effect=ensure_roles
        ), patch.object(
            postgres_migrate, "_grant_schema", side_effect=grant_schema
        ), patch.object(
            postgres_migrate, "PostgresMonitoringRepository", return_value=repository
        ), redirect_stdout(output):
            self.assertEqual(postgres_migrate.main(), 0)

        self.assertEqual(
            [call.kwargs["autocommit"] for call in connect.call_args_list],
            [False, False],
        )
        self.assertEqual(role_admin.transaction_entries, 1)
        self.assertEqual(role_admin.commits, 1)
        self.assertEqual(grant_admin.transaction_entries, 1)
        self.assertEqual(grant_admin.commits, 1)
        self.assertTrue(role_admin.closed)
        self.assertTrue(grant_admin.closed)
        repository.initialize.assert_called_once()
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["schema_versions"], [1, 2, 3, 4, 5, 6])

    def test_role_mutation_failure_rolls_back_before_schema_migration(self) -> None:
        role_admin = _Connection()
        repository = Mock()
        connect = Mock(return_value=role_admin)

        def fail_inside_transaction(admin: _Connection, _passwords: dict[str, str]) -> None:
            self.assertTrue(admin.in_transaction)
            raise RuntimeError("injected role failure")

        with patch.dict(os.environ, self.environment, clear=False), patch.dict(
            sys.modules, self._fake_driver(connect)
        ), patch.object(
            postgres_migrate, "_ensure_roles", side_effect=fail_inside_transaction
        ), patch.object(
            postgres_migrate, "PostgresMonitoringRepository", return_value=repository
        ):
            with self.assertRaisesRegex(RuntimeError, "injected role failure"):
                postgres_migrate.main()

        self.assertEqual(role_admin.rollbacks, 1)
        self.assertEqual(role_admin.commits, 0)
        self.assertTrue(role_admin.closed)
        repository.initialize.assert_not_called()


if __name__ == "__main__":
    unittest.main()
