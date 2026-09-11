from __future__ import annotations

import json
import io
import os
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path

from ops.scripts import monitoring_v4_prepare_sqlite_import as prepare_sqlite_import
from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.compatibility.publication import (
    reconcile_public_safe_publication,
)
from stream_monitoring_v4.compatibility.public_safe import (
    PUBLIC_SCHEMA,
    public_safe_bytes,
)
from stream_monitoring_v4.commands.retention import DAY, _run_retention
from stream_monitoring_v4.commands.sqlite_to_postgres import _run_import
from stream_monitoring_v4.exporter.metrics import render_metrics
from stream_monitoring_v4.storage.postgres import (
    PostgresConnection,
    PostgresMonitoringRepository,
)
from stream_monitoring_v4.storage.repository import MonitoringRepository
from stream_monitoring_v4.storage.publications import PUBLIC_SAFE_ARTIFACT_KEY
from stream_monitoring_v4.storage.postgres_schema import (
    DDL_V1,
    DDL_V2,
    DDL_V3,
    DDL_V4,
    DDL_V5,
    DDL_V6,
    SCHEMA_VERSION,
    _statements,
    migrate,
)
from stream_monitoring_v4.storage.role_contract import ROLE_TABLE_PRIVILEGES
from stream_monitoring_v4.storage.schema_parity import (
    postgres_schema_surface,
    schema_surface,
)

from tests.helpers import BASE_TS, observation


ADMIN_DSN_ENV = "STREAM_V4_TEST_POSTGRES_ADMIN_DSN"
CORE_DSN_ENV = "STREAM_V4_TEST_POSTGRES_CORE_DSN"
EXPORTER_DSN_ENV = "STREAM_V4_TEST_POSTGRES_EXPORTER_DSN"
MAINTENANCE_DSN_ENV = "STREAM_V4_TEST_POSTGRES_MAINTENANCE_DSN"


@unittest.skipUnless(
    all(
        os.environ.get(name)
        for name in (ADMIN_DSN_ENV, CORE_DSN_ENV, EXPORTER_DSN_ENV, MAINTENANCE_DSN_ENV)
    ),
    "disposable PostgreSQL integration DSNs are not configured",
)
class LivePostgresIntegrationTests(unittest.TestCase):
    """Destructive checks restricted to a database named ``*_integration``."""

    @classmethod
    def setUpClass(cls) -> None:
        import psycopg

        cls.psycopg = psycopg
        cls.admin_dsn = os.environ[ADMIN_DSN_ENV]
        with psycopg.connect(cls.admin_dsn, autocommit=True) as connection:
            database = str(connection.execute("SELECT current_database()").fetchone()[0])
        if not database.endswith("_integration"):
            raise RuntimeError(
                "refusing destructive PostgreSQL checks outside a *_integration database"
            )

    def setUp(self) -> None:
        self._cleanup()

    def tearDown(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        with self.psycopg.connect(self.admin_dsn, autocommit=True) as connection:
            connection.execute(
                "DELETE FROM public_artifact_publications "
                "WHERE cycle_id LIKE 'integration-%'"
            )
            connection.execute(
                "DELETE FROM shadow_cycles WHERE cycle_id LIKE 'integration-%'"
            )
            connection.execute("DELETE FROM incident_transitions WHERE transition_id LIKE 'integration-%'")
            connection.execute("DELETE FROM incident_candidates WHERE domain LIKE 'integration-%'")
            connection.execute("DELETE FROM incident_episodes WHERE episode_id LIKE 'integration-%'")
            connection.execute("DELETE FROM domain_current WHERE domain LIKE 'integration-%'")
            connection.execute("DELETE FROM current_snapshots WHERE snapshot_id LIKE 'integration-%'")
            connection.execute("DELETE FROM observations WHERE observation_id LIKE 'integration-%'")

    def test_public_artifact_intent_reconciles_on_postgres(self) -> None:
        repository = PostgresMonitoringRepository(
            os.environ[CORE_DSN_ENV],
            application_name="stream-v4-integration-publication",
            pool_min_size=0,
            pool_max_size=1,
        )
        cycle_id = "integration-publication-cycle"
        created_at = utc_text(200 * DAY)
        payload = {
            "schema": PUBLIC_SCHEMA,
            "generated_at": created_at,
            "scope": "isolated_non_public_compatibility_shadow",
            "interpretation": (
                "Measurements only; this artifact is not an incident or runtime control input."
            ),
            "items": [],
        }
        try:
            with repository.transaction() as connection:
                repository.append_shadow_cycle(
                    cycle_id=cycle_id,
                    started_at=created_at,
                    completed_at=created_at,
                    build_revision="integration",
                    source_revision="integration",
                    observer={},
                    current_states={},
                    parity={},
                    notification_intent_count=0,
                    connection=connection,
                )
                staged = repository.stage_artifact_publication(
                    artifact_key=PUBLIC_SAFE_ARTIFACT_KEY,
                    cycle_id=cycle_id,
                    created_at=created_at,
                    payload=payload,
                    connection=connection,
                )
            with tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "public-safe-shadow.json"
                result = reconcile_public_safe_publication(
                    repository,
                    output,
                    now_ts=200 * DAY + 1,
                )
                self.assertIsNotNone(result)
                self.assertEqual(result.publication_id, staged.publication_id)
                self.assertEqual(output.read_bytes(), public_safe_bytes(payload))
            publication = repository.artifact_publications(
                PUBLIC_SAFE_ARTIFACT_KEY
            )[-1]
            self.assertEqual(publication.state, "published")
            self.assertEqual(publication.attempt_count, 1)
        finally:
            repository.close()

    def test_v2_schema_migrates_in_place_to_v6_and_is_idempotent(self) -> None:
        from psycopg.rows import dict_row

        schema = "monitoring_v4_upgrade_integration"
        applied_at = utc_text(BASE_TS)
        with self.psycopg.connect(
            self.admin_dsn,
            autocommit=True,
            row_factory=dict_row,
        ) as raw:
            raw.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            raw.execute(f'CREATE SCHEMA "{schema}"')
            raw.execute(f'SET search_path TO "{schema}"')
            connection = PostgresConnection(raw)
            try:
                for script in (DDL_V1, DDL_V2):
                    for statement in _statements(script):
                        connection.execute(statement)
                for version in (1, 2):
                    connection.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (version, applied_at),
                    )
                connection.execute(
                    """INSERT INTO shadow_cycles(
                        cycle_id, started_at, started_ts, completed_at, completed_ts,
                        build_revision, observer_json, current_states_json, parity_json,
                        notification_intent_count, real_delivery_enabled,
                        runtime_mutation_enabled
                    ) VALUES (?, ?, ?, ?, ?, ?, '{}', '{}', '{}', 0, 0, 0)""",
                    (
                        "integration-upgrade-cycle",
                        applied_at,
                        BASE_TS,
                        applied_at,
                        BASE_TS,
                        "integration-v2",
                    ),
                )

                with raw.transaction():
                    migrate(connection, applied_at=applied_at)
                with raw.transaction():
                    migrate(connection, applied_at=applied_at)

                versions = [
                    int(row["version"])
                    for row in raw.execute(
                        "SELECT version FROM schema_migrations ORDER BY version"
                    ).fetchall()
                ]
                retained = raw.execute(
                    "SELECT source_revision FROM shadow_cycles WHERE cycle_id=%s",
                    ("integration-upgrade-cycle",),
                ).fetchone()["source_revision"]
                publication_table = raw.execute(
                    "SELECT to_regclass(%s) AS name",
                    (f"{schema}.public_artifact_publications",),
                ).fetchone()["name"]
                self.assertEqual(versions, list(range(1, SCHEMA_VERSION + 1)))
                self.assertEqual(retained, "unknown-source-revision")
                self.assertEqual(
                    publication_table,
                    "public_artifact_publications",
                )
            finally:
                raw.execute("SET search_path TO public")
                raw.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    def test_role_matrix_exporter_render_and_core_single_writer_lock(self) -> None:
        expected_surface = schema_surface(
            DDL_V1 + DDL_V2 + DDL_V3 + DDL_V4 + DDL_V5 + DDL_V6
        )
        with self.psycopg.connect(self.admin_dsn, autocommit=True) as admin:
            self.assertEqual(postgres_schema_surface(admin), expected_surface)
            all_tables = frozenset(expected_surface.table_names)
            for role, contract in ROLE_TABLE_PRIVILEGES.items():
                for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                    expected = contract.tables_for(privilege, all_tables=all_tables)
                    for table in all_tables:
                        row = admin.execute(
                            "SELECT has_table_privilege(%s, %s, %s)",
                            (role, f"public.{table}", privilege),
                        ).fetchone()
                        with self.subTest(role=role, privilege=privilege, table=table):
                            self.assertEqual(bool(row[0]), table in expected)

        with self.psycopg.connect(
            os.environ[EXPORTER_DSN_ENV], autocommit=True
        ) as exporter:
            exporter.execute("SELECT COUNT(*) FROM observations").fetchone()
            with self.assertRaises(self.psycopg.errors.InsufficientPrivilege):
                exporter.execute("DELETE FROM observations")

        exporter_repository = PostgresMonitoringRepository(
            os.environ[EXPORTER_DSN_ENV],
            application_name="stream-v4-integration-exporter",
            pool_min_size=0,
            pool_max_size=1,
        )
        try:
            with exporter_repository.connection(read_only=True) as connection:
                self.assertEqual(
                    connection.execute("SHOW statement_timeout").fetchone()[
                        "statement_timeout"
                    ],
                    "30s",
                )
                self.assertEqual(
                    connection.execute("SHOW lock_timeout").fetchone()["lock_timeout"],
                    "5s",
                )
            rendered = render_metrics(
                exporter_repository,
                now_ts=200 * DAY,
                build_revision="integration",
            )
        finally:
            exporter_repository.close()
        self.assertIn("stream_v3_monitoring_v4_build_info", rendered)

        first = PostgresMonitoringRepository(
            os.environ[CORE_DSN_ENV], pool_min_size=0, pool_max_size=1
        )
        second = PostgresMonitoringRepository(
            os.environ[CORE_DSN_ENV], pool_min_size=0, pool_max_size=1
        )
        try:
            with first.cycle_guard("integration-core") as first_acquired:
                with second.cycle_guard("integration-core") as second_acquired:
                    self.assertTrue(first_acquired)
                    self.assertFalse(second_acquired)
            with second.cycle_guard("integration-core") as acquired_after_release:
                self.assertTrue(acquired_after_release)
        finally:
            first.close()
            second.close()

    def test_database_rejects_dangling_live_snapshot_reference(self) -> None:
        with self.psycopg.connect(self.admin_dsn, autocommit=True) as connection:
            with self.assertRaises(self.psycopg.errors.ForeignKeyViolation):
                connection.execute(
                    """INSERT INTO incident_candidates(
                        domain, state, first_seen_at, first_seen_ts, last_seen_at,
                        last_seen_ts, samples, snapshot_id, reason_codes_json
                    ) VALUES ('integration-missing', 'bad',
                              '1970-01-01T00:00:00Z', 0,
                              '1970-01-01T00:00:00Z', 0, 1,
                              'integration-snapshot-missing', '[]')"""
                )

    def test_retention_preserves_observations_referenced_by_live_evidence(self) -> None:
        old_at = "1970-01-01T00:00:00Z"
        observations = (
            "integration-observation-current",
            "integration-observation-candidate",
            "integration-observation-transition",
            "integration-observation-unreferenced",
        )
        with self.psycopg.connect(self.admin_dsn, autocommit=True) as connection:
            for observation_id in observations:
                connection.execute(
                    """INSERT INTO observations(
                        observation_id, schema_name, domain, source, source_event_id,
                        source_generation, evidence_role, status, reason_code,
                        observed_at, observed_ts, received_at, received_ts,
                        freshness_limit_sec, producer_revision, payload_sha256, payload_json
                    ) VALUES (
                        %s, 'monitoring_v4.observation.v1', 'integration-retention',
                        'integration', %s, 'integration-generation',
                        'current_authoritative', 'bad', 'integration_bad',
                        %s, 0, %s, 0, 60, 'integration', 'digest', '{}'
                    )""",
                    (observation_id, observation_id, old_at, old_at),
                )

            snapshots = (
                (
                    "integration-snapshot-current",
                    "integration-current",
                    "integration-observation-current",
                ),
                (
                    "integration-snapshot-candidate",
                    "integration-candidate",
                    "integration-observation-candidate",
                ),
                (
                    "integration-snapshot-transition",
                    "integration-transition",
                    "integration-observation-transition",
                ),
            )
            for snapshot_id, domain, observation_id in snapshots:
                connection.execute(
                    """INSERT INTO current_snapshots(
                        snapshot_id, domain, state, observed_at, observed_ts,
                        reduced_at, reduced_ts, valid_until, valid_until_ts,
                        policy_revision, reducer_revision, reason_codes_json,
                        source_observation_ids_json, payload_json
                    ) VALUES (%s, %s, 'bad', %s, 0, %s, 0, %s, 0,
                              'integration', 'integration', '[\"integration_bad\"]', %s, '{}')""",
                    (snapshot_id, domain, old_at, old_at, old_at, json.dumps([observation_id])),
                )

            connection.execute(
                """INSERT INTO domain_current(
                    domain, snapshot_id, state, observed_at, observed_ts,
                    reduced_at, reduced_ts, valid_until, valid_until_ts,
                    policy_revision, reducer_revision, reason_codes_json,
                    source_observation_ids_json, payload_json
                ) SELECT domain, snapshot_id, state, observed_at, observed_ts,
                         reduced_at, reduced_ts, valid_until, valid_until_ts,
                         policy_revision, reducer_revision, reason_codes_json,
                         source_observation_ids_json, payload_json
                  FROM current_snapshots
                  WHERE snapshot_id='integration-snapshot-current'"""
            )
            connection.execute(
                """INSERT INTO incident_candidates(
                    domain, state, first_seen_at, first_seen_ts, last_seen_at,
                    last_seen_ts, samples, snapshot_id, reason_codes_json
                ) VALUES ('integration-candidate', 'bad', %s, 0, %s, 0, 1,
                          'integration-snapshot-candidate', '[\"integration_bad\"]')""",
                (old_at, old_at),
            )
            connection.execute(
                """INSERT INTO incident_episodes(
                    episode_id, domain, status, severity, opened_at, opened_ts,
                    last_bad_at, last_bad_ts, closed_at, closed_ts, bad_samples,
                    unknown_samples, last_transition_at, last_transition_ts,
                    next_notification_at, next_notification_ts, policy_revision,
                    summary, reason_codes_json
                ) VALUES ('integration-episode', 'integration-transition', 'closed',
                          'warning', %s, 0, %s, 0, %s, 1, 1, 0, %s, 0,
                          %s, 0, 'integration', 'integration', '[\"integration_bad\"]')""",
                (old_at, old_at, old_at, old_at, old_at),
            )
            connection.execute(
                """INSERT INTO incident_transitions(
                    transition_id, episode_id, domain, phase, severity, occurred_at,
                    occurred_ts, current_snapshot_id, summary, reason_codes_json
                ) VALUES ('integration-transition', 'integration-episode',
                          'integration-transition', 'detected', 'warning', %s, 0,
                          'integration-snapshot-transition', 'integration',
                          '[\"integration_bad\"]')""",
                (old_at,),
            )

        maintenance = PostgresMonitoringRepository(
            os.environ[MAINTENANCE_DSN_ENV],
            application_name="stream-v4-integration-retention",
            pool_min_size=0,
            pool_max_size=1,
        )
        try:
            acquired, deleted = _run_retention(
                maintenance,
                now_ts=200 * DAY,
                batch_size=100,
            )
        finally:
            maintenance.close()
        self.assertTrue(acquired)
        self.assertEqual(deleted["observations"], 1)

        with self.psycopg.connect(self.admin_dsn, autocommit=True) as connection:
            remaining = {
                row[0]
                for row in connection.execute(
                    "SELECT observation_id FROM observations "
                    "WHERE observation_id LIKE 'integration-%'"
                ).fetchall()
            }
        self.assertEqual(
            remaining,
            {
                "integration-observation-current",
                "integration-observation-candidate",
                "integration-observation-transition",
            },
        )

    def test_full_sqlite_import_is_idempotent_accepts_earlier_receipt_and_rejects_conflict(
        self,
    ) -> None:
        item = observation(
            observed_ts=BASE_TS - 20,
            received_ts=BASE_TS + 10,
            event="integration-sqlite-import",
            payload={"sample": "integration-sqlite-import"},
        )
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_path = root / "source.sqlite3"
            prepared_path = root / "prepared.sqlite3"
            source = MonitoringRepository(source_path)
            source.initialize(applied_at=utc_text(BASE_TS))
            source.append_observation(item)
            postgres = PostgresMonitoringRepository(
                self.admin_dsn,
                application_name="stream-v4-integration-sqlite-import",
                pool_min_size=0,
                pool_max_size=1,
            )
            try:
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(
                        prepare_sqlite_import.main(
                            ["--source", str(source_path), "--target", str(prepared_path)]
                        ),
                        0,
                    )
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(
                        _run_import(Namespace(output=None), prepared_path, postgres),
                        0,
                    )
                first = json.loads(output.getvalue())
                self.assertEqual(first["tables"]["observations"]["changed_rows"], 1)

                output = io.StringIO()
                with redirect_stdout(output):
                    _run_import(Namespace(output=None), prepared_path, postgres)
                repeated = json.loads(output.getvalue())
                self.assertEqual(
                    repeated["tables"]["observations"]["changed_rows"],
                    0,
                )

                with source.transaction() as connection:
                    connection.execute(
                        "UPDATE observations SET received_at=?, received_ts=? "
                        "WHERE observation_id=?",
                        (utc_text(BASE_TS), BASE_TS, item.observation_id),
                    )
                with redirect_stdout(io.StringIO()):
                    prepare_sqlite_import.main(
                        ["--source", str(source_path), "--target", str(prepared_path)]
                    )
                output = io.StringIO()
                with redirect_stdout(output):
                    _run_import(Namespace(output=None), prepared_path, postgres)
                earlier = json.loads(output.getvalue())
                self.assertEqual(
                    earlier["tables"]["observations"]["changed_rows"],
                    1,
                )
                self.assertEqual(
                    earlier["accepted_temporal_identity_differences"]["observations"],
                    0,
                )
                with self.psycopg.connect(self.admin_dsn, autocommit=True) as admin:
                    received_ts = admin.execute(
                        "SELECT received_ts FROM observations WHERE observation_id=%s",
                        (item.observation_id,),
                    ).fetchone()[0]
                self.assertEqual(int(received_ts), BASE_TS)

                with source.transaction() as connection:
                    connection.execute(
                        "UPDATE observations SET received_at=?, received_ts=? "
                        "WHERE observation_id=?",
                        (utc_text(BASE_TS + 10), BASE_TS + 10, item.observation_id),
                    )
                with redirect_stdout(io.StringIO()):
                    prepare_sqlite_import.main(
                        ["--source", str(source_path), "--target", str(prepared_path)]
                    )
                output = io.StringIO()
                with redirect_stdout(output):
                    _run_import(Namespace(output=None), prepared_path, postgres)
                later = json.loads(output.getvalue())
                self.assertEqual(later["tables"]["observations"]["changed_rows"], 0)
                self.assertEqual(
                    later["accepted_temporal_identity_differences"]["observations"],
                    1,
                )
                with self.psycopg.connect(self.admin_dsn, autocommit=True) as admin:
                    retained_ts = admin.execute(
                        "SELECT received_ts FROM observations WHERE observation_id=%s",
                        (item.observation_id,),
                    ).fetchone()[0]
                self.assertEqual(int(retained_ts), BASE_TS)

                with source.transaction() as connection:
                    connection.execute(
                        "UPDATE observations SET payload_json=? WHERE observation_id=?",
                        ('{"tampered":true}', item.observation_id),
                    )
                with redirect_stdout(io.StringIO()):
                    prepare_sqlite_import.main(
                        ["--source", str(source_path), "--target", str(prepared_path)]
                    )
                with self.assertRaisesRegex(RuntimeError, "verification mismatch"):
                    with redirect_stdout(io.StringIO()):
                        _run_import(Namespace(output=None), prepared_path, postgres)
                with self.psycopg.connect(self.admin_dsn, autocommit=True) as admin:
                    retained_payload = admin.execute(
                        "SELECT payload_json FROM observations WHERE observation_id=%s",
                        (item.observation_id,),
                    ).fetchone()[0]
                self.assertEqual(
                    json.loads(retained_payload),
                    {"sample": "integration-sqlite-import"},
                )
            finally:
                postgres.close()
                with self.psycopg.connect(self.admin_dsn, autocommit=True) as admin:
                    admin.execute(
                        "DELETE FROM observations WHERE observation_id=%s",
                        (item.observation_id,),
                    )
