from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import warnings
from pathlib import Path

from stream_monitoring_v4.commands.wall_clock_loop import next_run_epoch
from stream_monitoring_v4.commands import shadow_once
from stream_monitoring_v4.commands.sqlite_to_postgres import (
    TEMPORAL_IDENTITY_POLICIES,
    _earliest_temporal_insert,
    _iter_rows,
    _observation_insert,
    _snapshot,
)
from stream_monitoring_v4.storage.postgres import (
    PostgresMonitoringRepository,
    postgres_advisory_lock_key,
    postgres_sql,
)
from stream_monitoring_v4.storage.ports import (
    CurrentReducerRepository,
    ExporterRepository,
    IncidentRepository,
    NotificationDispatchRepository,
    ObserverRepository,
    ReliabilityProjectionRepository,
    RuntimeLifecycleRepository,
)
from stream_monitoring_v4.storage.repository import MonitoringRepository
from stream_monitoring_v4.storage.factory import wait_for_integrity
from stream_monitoring_v4.storage.postgres_schema import (
    DDL_V1,
    DDL_V2,
    DDL_V3,
    DDL_V4,
    DDL_V5,
    DDL_V6,
)
from stream_monitoring_v4.storage.role_contract import (
    ROLE_TABLE_PRIVILEGES,
    table_grant_statements,
)
from stream_monitoring_v4.storage.schema_parity import (
    POSTGRES_LIVE_REFERENCE_CONSTRAINTS,
    SQLITE_LIVE_REFERENCE_TRIGGERS,
    schema_surface,
)
from stream_monitoring_v4.storage.schema import (
    DDL_V1 as SQLITE_DDL_V1,
    DDL_V2 as SQLITE_DDL_V2,
    DDL_V3 as SQLITE_DDL_V3,
    DDL_V4 as SQLITE_DDL_V4,
    DDL_V5 as SQLITE_DDL_V5,
    DDL_V6 as SQLITE_DDL_V6,
    SCHEMA_VERSION as SQLITE_SCHEMA_VERSION,
)
from stream_monitoring_v4.storage.postgres_schema import SCHEMA_VERSION as POSTGRES_SCHEMA_VERSION
from ops.scripts.monitoring_v4_source_identity import content_identity, included_paths


ROOT = Path(__file__).resolve().parents[1]


class PostgresCompatibilityTests(unittest.TestCase):
    def test_failed_session_configuration_is_discarded_from_the_pool(self) -> None:
        class Raw:
            def __init__(self) -> None:
                self.closed = False

            def execute(self, _sql, _params=()):
                raise RuntimeError("session configuration failed")

            def close(self) -> None:
                self.closed = True

        class Pool:
            def __init__(self, raw) -> None:
                self.raw = raw
                self.returned = []

            def getconn(self, *, timeout):
                self.timeout = timeout
                return self.raw

            def putconn(self, raw) -> None:
                self.returned.append(raw)

        raw = Raw()
        pool = Pool(raw)
        repository = PostgresMonitoringRepository(
            connect_timeout_sec=3,
            pool_min_size=0,
            pool_max_size=1,
        )
        repository._pool = pool
        with self.assertRaisesRegex(RuntimeError, "configuration failed"):
            repository.connect()
        self.assertTrue(raw.closed)
        self.assertEqual(pool.returned, [raw])

    def test_service_repository_ports_are_backend_neutral_and_role_scoped(self) -> None:
        sqlite_repository = MonitoringRepository(Path("/not-opened/monitoring.sqlite3"))
        postgres_repository = object.__new__(PostgresMonitoringRepository)
        ports = (
            CurrentReducerRepository,
            ExporterRepository,
            IncidentRepository,
            NotificationDispatchRepository,
            ObserverRepository,
            ReliabilityProjectionRepository,
            RuntimeLifecycleRepository,
        )
        for port in ports:
            with self.subTest(backend="sqlite", port=port.__name__):
                self.assertIsInstance(sqlite_repository, port)
            with self.subTest(backend="postgresql", port=port.__name__):
                self.assertIsInstance(postgres_repository, port)

        self.assertFalse(hasattr(ExporterRepository, "connection"))
        self.assertFalse(hasattr(ExporterRepository, "save_current"))
        self.assertFalse(hasattr(ObserverRepository, "append_intent"))
        self.assertFalse(hasattr(ReliabilityProjectionRepository, "save_episode"))
        self.assertFalse(hasattr(RuntimeLifecycleRepository, "save_candidate"))

    def test_postgres_is_not_a_sqlite_repository_and_rejects_restore_before_io(self) -> None:
        self.assertFalse(issubclass(PostgresMonitoringRepository, MonitoringRepository))
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "sqlite-backup.db"
            target = root / "postgres-target.db"
            source.write_bytes(b"not-a-database")
            with self.assertRaisesRegex(NotImplementedError, "pg_restore"):
                PostgresMonitoringRepository.restore(
                    source,
                    target,
                    applied_at="2026-08-11T00:00:00Z",
                )
            self.assertFalse(target.exists())

    def test_sqlite_import_streams_rows_in_stable_key_order(self) -> None:
        connection = sqlite3.connect(":memory:")
        try:
            connection.execute(
                "CREATE TABLE observations(observation_id TEXT PRIMARY KEY, payload TEXT)"
            )
            connection.executemany(
                "INSERT INTO observations VALUES (?, ?)",
                [("c", "3"), ("a", "1"), ("b", "2")],
            )
            rows = list(
                _iter_rows(
                    connection,
                    "observations",
                    ("observation_id", "payload"),
                    batch_size=1,
                )
            )
        finally:
            connection.close()
        self.assertEqual(rows, [("a", "1"), ("b", "2"), ("c", "3")])

    def test_sqlite_import_rejects_non_positive_batch_size(self) -> None:
        connection = sqlite3.connect(":memory:")
        try:
            with self.assertRaisesRegex(ValueError, "batch_size"):
                list(_iter_rows(connection, "observations", (), batch_size=0))
        finally:
            connection.close()

    def test_live_postgres_cycle_requires_real_clock_and_single_writer_lease(self) -> None:
        fixed = shadow_once.parser().parse_args(
            [
                "--postgres",
                "--state-root",
                "/tmp/source",
                "--now-ts",
                "1",
                "--lease-name",
                "test-core",
            ]
        )
        with self.assertRaisesRegex(ValueError, "do not accept --now-ts"):
            shadow_once.run_parsed_once(fixed, object(), None)

        unleased = shadow_once.parser().parse_args(
            ["--postgres", "--state-root", "/tmp/source"]
        )
        with self.assertRaisesRegex(ValueError, "require --lease-name"):
            shadow_once.run_parsed_once(unleased, object(), None)

    def test_startup_integrity_retry_absorbs_bounded_database_unavailability(self) -> None:
        class Repository:
            def __init__(self) -> None:
                self.calls = 0

            def integrity_check(self) -> str:
                self.calls += 1
                return "ok" if self.calls == 3 else "failed"

        repository = Repository()
        wait_for_integrity(repository, timeout_sec=1, retry_interval_sec=0.01)
        self.assertEqual(repository.calls, 3)

    def test_repository_sql_translation_is_narrow_and_idempotent(self) -> None:
        translated = postgres_sql(
            "INSERT OR IGNORE INTO observations(a, b) VALUES (?, ?)"
        )
        self.assertEqual(
            translated,
            "INSERT INTO observations(a, b) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        )
        self.assertEqual(
            postgres_sql("SELECT * FROM observations WHERE domain=?"),
            "SELECT * FROM observations WHERE domain=%s",
        )

    def test_live_cycle_guard_is_session_scoped_and_deterministic(self) -> None:
        class Cursor:
            def __init__(self, row):
                self.row = row

            def fetchone(self):
                return self.row

        class Connection:
            def __init__(self, acquired: bool, *, released: bool = True) -> None:
                self.acquired = acquired
                self.released = released
                self.calls = []
                self.closed = False
                self.discarded = False

            def execute(self, sql, params=()):
                self.calls.append((sql, params))
                return Cursor({"acquired": self.acquired, "released": self.released})

            def close(self):
                self.closed = True

            def discard(self):
                self.discarded = True
                self.closed = True

        self.assertEqual(
            postgres_advisory_lock_key("monitoring-v4-core"),
            postgres_advisory_lock_key("monitoring-v4-core"),
        )
        self.assertNotEqual(
            postgres_advisory_lock_key("monitoring-v4-core"),
            postgres_advisory_lock_key("another-writer"),
        )

        repository = object.__new__(PostgresMonitoringRepository)
        connection = Connection(True)
        repository.connect = lambda: connection
        with repository.cycle_guard("monitoring-v4-core") as acquired:
            self.assertTrue(acquired)
            self.assertFalse(connection.closed)
        self.assertTrue(connection.closed)
        self.assertIn("pg_try_advisory_lock", connection.calls[0][0])
        self.assertIn("pg_advisory_unlock", connection.calls[-1][0])
        self.assertEqual(connection.calls[0][1], connection.calls[-1][1])

        held = Connection(False)
        repository.connect = lambda: held
        with repository.cycle_guard("monitoring-v4-core") as acquired:
            self.assertFalse(acquired)
        self.assertTrue(held.closed)
        self.assertEqual(len(held.calls), 1)

        ambiguous_unlock = Connection(True, released=False)
        repository.connect = lambda: ambiguous_unlock
        with repository.cycle_guard("monitoring-v4-core") as acquired:
            self.assertTrue(acquired)
        self.assertTrue(ambiguous_unlock.discarded)

    def test_sqlite_merge_keeps_earliest_receipt_only_for_same_observation_semantics(self) -> None:
        sql = _observation_insert(
            (
                "observation_id",
                "domain",
                "status",
                "received_at",
                "received_ts",
                "payload_sha256",
            )
        )
        self.assertIn('ON CONFLICT("observation_id") DO UPDATE', sql)
        self.assertIn('"payload_sha256" IS NOT DISTINCT FROM excluded."payload_sha256"', sql)
        self.assertIn('excluded."received_ts" < "observations"."received_ts"', sql)

    def test_parallel_reader_auxiliary_times_have_explicit_merge_policies(self) -> None:
        self.assertEqual(
            TEMPORAL_IDENTITY_POLICIES,
            {
                "observations": ("received_at", "received_ts"),
                "current_snapshots": ("reduced_at", "reduced_ts"),
                "incident_processed_currents": ("processed_at", "processed_ts"),
                "sli_projections": ("evaluated_at", "evaluated_ts"),
            },
        )
        sql = _earliest_temporal_insert(
            "current_snapshots",
            ("snapshot_id", "domain", "reduced_at", "reduced_ts", "payload_json"),
            clock_text="reduced_at",
            clock_ts="reduced_ts",
        )
        self.assertIn('ON CONFLICT("snapshot_id") DO UPDATE', sql)
        self.assertIn('"domain" IS NOT DISTINCT FROM excluded."domain"', sql)
        self.assertIn('excluded."reduced_ts" < "current_snapshots"."reduced_ts"', sql)

    def test_postgres_schema_keeps_timestamp_range_and_has_no_sqlite_pragma(self) -> None:
        schema = DDL_V1 + DDL_V2 + DDL_V3 + DDL_V4 + DDL_V5 + DDL_V6
        self.assertIn("observed_ts BIGINT", schema)
        self.assertIn("window_end_ts BIGINT", schema)
        self.assertNotIn("PRAGMA", schema)
        self.assertIn("ADD COLUMN IF NOT EXISTS source_revision", schema)

    def test_sqlite_and_postgres_schema_table_sets_and_versions_match(self) -> None:
        sqlite_ddl = (
            SQLITE_DDL_V1
            + SQLITE_DDL_V2
            + SQLITE_DDL_V3
            + SQLITE_DDL_V4
            + SQLITE_DDL_V5
            + SQLITE_DDL_V6
        )
        postgres_ddl = DDL_V1 + DDL_V2 + DDL_V3 + DDL_V4 + DDL_V5 + DDL_V6
        self.assertEqual(SQLITE_SCHEMA_VERSION, POSTGRES_SCHEMA_VERSION)
        self.assertEqual(schema_surface(sqlite_ddl), schema_surface(postgres_ddl))
        for trigger in SQLITE_LIVE_REFERENCE_TRIGGERS:
            self.assertIn(trigger, sqlite_ddl)
        for constraint in POSTGRES_LIVE_REFERENCE_CONSTRAINTS:
            self.assertIn(constraint, postgres_ddl)

    def test_schema_surface_does_not_treat_compact_constraints_as_columns(self) -> None:
        surface = schema_surface(
            """CREATE TABLE IF NOT EXISTS example (
                item_id TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                UNIQUE(item_id, value),
                CHECK(value <> '')
            );
            ALTER TABLE example ADD COLUMN IF NOT EXISTS source_revision TEXT;
            """
        )
        self.assertEqual(
            surface.tables,
            (("example", ("item_id", "value", "source_revision")),),
        )

    def test_wall_clock_scheduler_uses_exact_second_and_minute_modulo(self) -> None:
        now = datetime(2026, 8, 11, 23, 59, 51, tzinfo=timezone.utc).timestamp()
        next_epoch = next_run_epoch(now, second=50, minute_modulo=5)
        self.assertEqual(
            datetime.fromtimestamp(next_epoch, tz=timezone.utc),
            datetime(2026, 8, 12, 0, 0, 50, tzinfo=timezone.utc),
        )


class K3sManifestTests(unittest.TestCase):
    def test_initial_memory_calibration_keeps_limits_and_versions_node_reservation(self) -> None:
        core = (ROOT / "deploy/k3s/core.yaml").read_text(encoding="utf-8")
        reporter = (ROOT / "deploy/k3s/reporter.yaml").read_text(encoding="utf-8")
        reservation = (
            ROOT / "deploy/host/kubelet/50-arena-memory-reservation.conf"
        ).read_text(encoding="utf-8")

        self.assertIn("memory: 160Mi", core)
        self.assertIn('memory: 384Mi', core)
        self.assertIn("memory: 96Mi", reporter)
        self.assertIn("memory: 192Mi", reporter)
        self.assertIn("apiVersion: kubelet.config.k8s.io/v1beta1", reservation)
        self.assertIn("kind: KubeletConfiguration", reservation)
        self.assertIn("systemReserved:\n  memory: 2Gi", reservation)
        self.assertIn("kubeReserved:\n  memory: 1Gi", reservation)
        self.assertNotIn("evictionHard", reservation)
        self.assertNotIn("enforceNodeAllocatable", reservation)

    def test_images_are_pinned_and_stateful_storage_is_retained(self) -> None:
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        runtime_lock = (
            ROOT / "requirements/runtime-py313-linux-amd64.lock"
        ).read_text(encoding="utf-8")
        postgres = (ROOT / "deploy/k3s/postgres.yaml").read_text(encoding="utf-8")
        storage = (ROOT / "deploy/k3s/storageclass.yaml").read_text(encoding="utf-8")
        self.assertIn("python@sha256:", dockerfile)
        self.assertIn("--require-hashes", dockerfile)
        self.assertIn("--no-deps", dockerfile)
        self.assertIn("chmod -R a=rX /opt/stream-v4/src", dockerfile)
        self.assertNotIn("'.[postgres]'", dockerfile)
        self.assertEqual(runtime_lock.count("--hash=sha256:"), 4)
        self.assertIn("psycopg==3.3.4", runtime_lock)
        self.assertIn("psycopg-pool==3.3.1", runtime_lock)
        self.assertIn("postgres@sha256:", postgres)
        sentinel_contracts = (
            ROOT / "src/stream_monitoring_v4/sentinel/contracts.py"
        ).read_text(encoding="utf-8")
        self.assertIn("POSTGRES_IMAGE =", sentinel_contracts)
        self.assertIn(
            re.search(r"postgres@sha256:[0-9a-f]{64}", postgres).group(0),
            sentinel_contracts,
        )
        self.assertIn("reclaimPolicy: Retain", storage)
        self.assertIn("storage: 20Gi", postgres)

    def test_backup_is_verified_and_mirrored_to_an_independent_disk(self) -> None:
        backup = (ROOT / "deploy/k3s/backup.yaml").read_text(encoding="utf-8")
        restore = (ROOT / "deploy/k3s/restore-smoke-job.yaml").read_text(encoding="utf-8")
        sentinel_checks = (
            ROOT / "src/stream_monitoring_v4/sentinel/checks.py"
        ).read_text(encoding="utf-8")
        integrity = (
            ROOT / "src/stream_monitoring_v4/runtime/backup_integrity.py"
        ).read_text(encoding="utf-8")
        self.assertIn("pg_restore --list", backup)
        self.assertIn("sha256sum", backup)
        self.assertIn("/var/backups/stream-monitoring-v4", backup)
        self.assertIn('test "$(stat -c %d /backup)" !=', backup)
        self.assertIn("activeDeadlineSeconds: 1800", backup)
        self.assertIn('sync -f "${temporary}"', backup)
        self.assertLess(
            backup.index('ln "${checksum_temporary}" "${final}.sha256"'),
            backup.index('ln "${temporary}" "${final}"'),
        )
        self.assertLess(
            backup.index(
                'ln "${independent_checksum_temporary}" "${independent_final}.sha256"'
            ),
            backup.index('ln "${independent_temporary}" "${independent_final}"'),
        )
        self.assertNotIn('mv "${temporary}" "${final}"', backup)
        self.assertIn("stream_monitoring_v4.commands.restore_select", restore)
        self.assertIn("--backup-max-age-sec", restore)
        self.assertNotIn("ls -1t", restore)
        self.assertIn("image: __APP_IMAGE__", restore)
        self.assertIn("runAsUser: 70\n                # Attestations", restore)
        self.assertIn("runAsGroup: 1000", restore)
        self.assertIn("kind: CronJob", restore)
        self.assertIn('schedule: "37 18 * * *"', restore)
        self.assertIn("initdb --pgdata", restore)
        self.assertIn("/independent-backup", restore)
        self.assertIn("postgresql_restore_verification.v1", restore)
        self.assertNotIn("PGPASSWORD", restore)
        self.assertNotIn("envFrom:", restore)
        self.assertIn('"backup_copies_match": facts.backup_copies_match', sentinel_checks)
        self.assertIn("restore_verification_acceptable", sentinel_checks)
        self.assertIn("backup_checksum_mismatch", integrity)

    def test_core_is_single_writer_and_no_authority_bearing_notifier_is_deployed(self) -> None:
        core = (ROOT / "deploy/k3s/core.yaml").read_text(encoding="utf-8")
        projector = (ROOT / "deploy/k3s/input-projector.yaml").read_text(encoding="utf-8")
        kustomization = (ROOT / "deploy/k3s/kustomization.yaml").read_text(encoding="utf-8")
        all_manifests = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "deploy/k3s").glob("*.yaml")
        )
        self.assertIn("replicas: 1", core)
        self.assertIn("type: Recreate", core)
        self.assertIn("--lease-name", core)
        self.assertIn("--input-projection-lock", core)
        self.assertIn("validate_safe_input_generation", (
            ROOT / "src/stream_monitoring_v4/commands/shadow_once.py"
        ).read_text(encoding="utf-8"))
        self.assertIn("/source-state/.projection.lock", core)
        self.assertIn("stream_monitoring_v4.commands.core_runner", core)
        self.assertIn("/tmp/core-heartbeat", core)
        self.assertNotIn("migration-source", core)
        self.assertNotIn("projects/stream_v3", core)
        self.assertIn("source-safe", core)
        self.assertIn("/srv/stream-v3/.state/arena-monitor", projector)
        self.assertIn("/tmp/projector-heartbeat", projector)
        self.assertIn("/tmp/projector-ready", projector)
        self.assertNotIn("envFrom:", projector)
        self.assertNotIn("database-roles", projector)
        self.assertIn("input-projector.yaml", kustomization)
        self.assertNotIn("notifier.yaml", kustomization)
        self.assertNotIn("discord", all_manifests.lower())
        self.assertNotIn("slack", all_manifests.lower())
        self.assertNotIn("raspberry", all_manifests.lower())

    def test_youtube_api_credential_is_confined_to_one_read_only_collector(self) -> None:
        collector = (ROOT / "deploy/k3s/youtube-api-collector.yaml").read_text(
            encoding="utf-8"
        )
        core = (ROOT / "deploy/k3s/core.yaml").read_text(encoding="utf-8")
        policy = (ROOT / "deploy/k3s/network-policy.yaml").read_text(
            encoding="utf-8"
        )
        kustomization = (ROOT / "deploy/k3s/kustomization.yaml").read_text(
            encoding="utf-8"
        )
        other_manifests = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "deploy/k3s").glob("*.yaml")
            if path.name != "youtube-api-collector.yaml"
        )
        self.assertIn("youtube-api-collector.yaml", kustomization)
        self.assertIn("replicas: 1", collector)
        self.assertIn("type: Recreate", collector)
        self.assertIn("automountServiceAccountToken: false", collector)
        self.assertIn("readOnlyRootFilesystem: true", collector)
        self.assertIn('drop: ["ALL"]', collector)
        self.assertIn("secretName: youtube-api-oauth", collector)
        self.assertEqual(collector.count("secretName: youtube-api-oauth"), 1)
        self.assertNotIn("database-roles", collector)
        self.assertNotIn("postgres-admin", collector)
        self.assertNotIn("envFrom:", collector)
        self.assertNotIn("WEBHOOK", collector)
        self.assertNotIn("runtime-mutation", collector.lower())
        self.assertNotIn("systemctl", collector.lower())
        self.assertNotIn("kubectl", collector.lower())
        self.assertNotIn("secretName: youtube-api-oauth", other_manifests)
        self.assertIn("--youtube-api-state-file", core)
        self.assertIn("mountPath: /api-source\n              readOnly: true", core)
        self.assertIn("name: allow-youtube-api-collector-egress", policy)
        self.assertIn("app.kubernetes.io/component: youtube-api-collector", policy)
        self.assertIn("cidr: 0.0.0.0/0", policy)
        self.assertIn("port: 443", policy)

    def test_database_roles_are_table_scoped_and_reporter_is_separate(self) -> None:
        migrate = (ROOT / "src/stream_monitoring_v4/commands/postgres_migrate.py").read_text(
            encoding="utf-8"
        )
        reporter = (ROOT / "deploy/k3s/reporter.yaml").read_text(encoding="utf-8")
        bootstrap = (ROOT / "ops/scripts/monitoring_v4_bootstrap_db_secrets.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('"v4_reporter_ro": "STREAM_V4_REPORTER_DB_PASSWORD"', migrate)
        self.assertNotIn(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO v4_core_rw",
            migrate,
        )
        self.assertFalse(ROLE_TABLE_PRIVILEGES["v4_core_rw"].select_all)
        self.assertFalse(ROLE_TABLE_PRIVILEGES["v4_exporter_ro"].select_all)
        self.assertEqual(
            ROLE_TABLE_PRIVILEGES["v4_reporter_ro"].select,
            {"schema_migrations", "shadow_cycles"},
        )
        self.assertIn(
            "GRANT SELECT ON schema_migrations, shadow_cycles TO v4_reporter_ro",
            table_grant_statements(),
        )
        self.assertEqual(ROLE_TABLE_PRIVILEGES["v4_reporter_ro"].insert, frozenset())
        self.assertEqual(ROLE_TABLE_PRIVILEGES["v4_exporter_ro"].delete, frozenset())
        self.assertIn("value: v4_reporter_ro", reporter)
        self.assertIn("/tmp/reporter-heartbeat", reporter)
        self.assertIn('"reporter-password"', bootstrap)
        self.assertIn('"maintenance-password"', bootstrap)
        self.assertIn('"v4_maintenance_rw": "STREAM_V4_MAINTENANCE_DB_PASSWORD"', migrate)
        self.assertTrue(ROLE_TABLE_PRIVILEGES["v4_maintenance_rw"].delete)
        self.assertIn("GRANT TEMPORARY ON DATABASE", migrate)

    def test_retention_is_backup_gated_scoped_and_separately_scheduled(self) -> None:
        manifest = (ROOT / "deploy/k3s/maintenance.yaml").read_text(encoding="utf-8")
        backup = (ROOT / "deploy/k3s/backup.yaml").read_text(encoding="utf-8")
        backup_retention = (ROOT / "deploy/k3s/backup-retention.yaml").read_text(
            encoding="utf-8"
        )
        kustomization = (ROOT / "deploy/k3s/kustomization.yaml").read_text(encoding="utf-8")
        policy = (ROOT / "deploy/k3s/network-policy.yaml").read_text(encoding="utf-8")
        self.assertIn('schedule: "17 19 * * *"', manifest)
        self.assertIn("v4_maintenance_rw", manifest)
        self.assertIn("--independent-backup-dir", manifest)
        self.assertIn("--restore-verification-dir", manifest)
        self.assertIn("readOnly: true", manifest)
        self.assertIn("maintenance.yaml", kustomization)
        self.assertIn("restore-smoke-job.yaml", kustomization)
        self.assertIn("backup-retention.yaml", kustomization)
        self.assertIn('"maintenance"', policy)
        self.assertNotIn("-mtime +35 -delete", backup)
        self.assertNotIn("-mtime +90 -delete", backup)
        self.assertIn("stream_monitoring_v4.commands.backup_retention", backup_retention)
        self.assertIn('schedule: "27 19 * * *"', backup_retention)
        self.assertIn("--restore-verification-dir", backup_retention)
        self.assertNotIn("PGPASSWORD", backup_retention)
        self.assertNotIn("envFrom:", backup_retention)

    def test_database_and_exporter_health_avoid_probe_connection_churn(self) -> None:
        postgres = (ROOT / "deploy/k3s/postgres.yaml").read_text(encoding="utf-8")
        core = (ROOT / "deploy/k3s/core.yaml").read_text(encoding="utf-8")
        exporter_manifest = (ROOT / "deploy/k3s/exporter.yaml").read_text(encoding="utf-8")
        exporter = (ROOT / "src/stream_monitoring_v4/commands/exporter.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("periodSeconds: 30", postgres)
        self.assertIn("readiness-30s-no-query-liveness-v1", postgres)
        self.assertNotIn("livenessProbe:", postgres)
        self.assertNotIn("log_connections=on", postgres)
        self.assertNotIn("log_disconnections=on", postgres)
        self.assertIn("/tmp/core-heartbeat", core)
        self.assertIn("livenessProbe:", core)
        self.assertIn("--startup-db-timeout-sec", core)
        self.assertIn('            - "180"', core)
        self.assertIn("failureThreshold: 60", core)
        self.assertIn("path: /livez", exporter_manifest)
        self.assertIn("--startup-db-timeout-sec", exporter_manifest)
        self.assertIn("startupProbe:", exporter_manifest)
        self.assertIn("failureThreshold: 48", exporter_manifest)
        self.assertIn("repository.ping()", exporter)
        self.assertIn("_health_safely(repository, build_revision)", exporter)
        self.assertIn("_render_safely(repository, build_revision)", exporter)
        self.assertNotIn("repository.integrity_check()", exporter)

    def test_network_is_default_deny_and_sentinel_cannot_restart_cluster(self) -> None:
        policy = (ROOT / "deploy/k3s/network-policy.yaml").read_text(encoding="utf-8")
        sentinel = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((ROOT / "src/stream_monitoring_v4/sentinel").glob("*.py"))
        )
        sentinel_unit = (ROOT / "ops/systemd/stream-monitoring-v4-k3s-sentinel.service").read_text(
            encoding="utf-8"
        )
        self.assertIn("name: default-deny", policy)
        self.assertIn('"is-active", "k3s"', sentinel)
        self.assertNotIn('"restart", "k3s"', sentinel)
        self.assertIn('"automatic_k3s_restart_enabled": False', sentinel)
        self.assertIn("counts[component] == required", sentinel)
        self.assertIn('"app_images_match_release": facts.app_images_match_release', sentinel)
        self.assertIn('"database_images_match_release": facts.database_images_match_release', sentinel)
        self.assertIn('"report_build_matches_release": facts.report_build_matches_release', sentinel)
        self.assertIn("active_mutating_auxiliary_pods", sentinel)
        self.assertIn("auxiliary_image_identity_matches_release", sentinel)
        self.assertIn('text(data, "build-revision")', sentinel)
        self.assertIn("ProtectSystem=strict", sentinel_unit)
        self.assertIn("ProtectHome=read-only", sentinel_unit)
        self.assertIn("Group=streamv4", sentinel_unit)
        self.assertIn(
            "ReadWritePaths=/var/lib/stream-monitoring-v4/.state/sentinel",
            sentinel_unit,
        )
        self.assertIn("CapabilityBoundingSet=\n", sentinel_unit)

    def test_source_identity_is_stable_40_hex(self) -> None:
        command = [sys.executable, str(ROOT / "ops/scripts/monitoring_v4_source_identity.py")]
        first = subprocess.check_output(command, text=True).strip()
        second = subprocess.check_output(command, text=True).strip()
        self.assertEqual(first, second)
        self.assertRegex(first, r"^[0-9a-f]{40}$")

    def test_source_identity_covers_deployment_and_sentinel_behavior(self) -> None:
        relatives = {path.relative_to(ROOT).as_posix() for path in included_paths(ROOT)}
        self.assertIn("deploy/k3s/core.yaml", relatives)
        self.assertIn("deploy/k3s/input-projector.yaml", relatives)
        self.assertIn("requirements/runtime-py313-linux-amd64.lock", relatives)
        self.assertIn("ops/scripts/monitoring_v4_k3s_sentinel.py", relatives)
        self.assertIn(
            "ops/scripts/monitoring_v4_bootstrap_youtube_api_secret.py",
            relatives,
        )
        self.assertIn("ops/systemd/stream-monitoring-v4-k3s-sentinel.service", relatives)
        with tempfile.TemporaryDirectory() as td:
            copied_root = Path(td)
            for source in included_paths(ROOT):
                target = copied_root / source.relative_to(ROOT)
                target.parent.mkdir(parents=True, exist_ok=True)
                # The immutable arena release stores inputs as 0444.  Copy content
                # without preserving that mode so this behavioral probe can mutate
                # only its temporary fixture.
                shutil.copyfile(source, target)
            before = content_identity(copied_root)
            core = copied_root / "deploy/k3s/core.yaml"
            core.write_text(core.read_text(encoding="utf-8") + "\n# identity probe\n", encoding="utf-8")
            self.assertNotEqual(before, content_identity(copied_root))

    def test_import_snapshot_is_checkpointed_for_read_only_container_mount(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "source.sqlite3"
            target = root / "target.sqlite3"
            with closing(sqlite3.connect(source)) as connection:
                self.assertEqual(
                    connection.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower(),
                    "wal",
                )
                connection.execute("CREATE TABLE evidence(value TEXT PRIMARY KEY)")
                connection.execute("INSERT INTO evidence(value) VALUES ('preserved')")
                connection.commit()
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "ops/scripts/monitoring_v4_prepare_sqlite_import.py"),
                    "--source",
                    str(source),
                    "--target",
                    str(target),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            with closing(sqlite3.connect(f"file:{target}?mode=ro", uri=True)) as connection:
                self.assertEqual(connection.execute("SELECT value FROM evidence").fetchone()[0], "preserved")
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0].lower(), "delete")
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)

            victim = root / "victim.sqlite3"
            victim.write_bytes(b"must remain unchanged")
            link = root / "symlink-target.sqlite3"
            link.symlink_to(victim)
            refused = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "ops/scripts/monitoring_v4_prepare_sqlite_import.py"),
                    "--source",
                    str(source),
                    "--target",
                    str(link),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(refused.returncode, 0)
            self.assertEqual(victim.read_bytes(), b"must remain unchanged")

    def test_sqlite_snapshot_closes_every_connection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "source.sqlite3"
            target = root / "target.sqlite3"
            with closing(sqlite3.connect(source)) as connection:
                connection.execute("CREATE TABLE evidence(value TEXT PRIMARY KEY)")
                connection.execute("INSERT INTO evidence(value) VALUES ('preserved')")
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.commit()

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", ResourceWarning)
                _snapshot(source, target)

            self.assertFalse(
                [item for item in caught if issubclass(item.category, ResourceWarning)],
                caught,
            )


if __name__ == "__main__":
    unittest.main()
