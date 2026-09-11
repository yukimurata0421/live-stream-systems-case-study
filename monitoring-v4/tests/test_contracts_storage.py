from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.incident import IncidentEpisode
from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.recovery import RecoveryAuthorization, RuntimeControlCommand
from stream_contracts.monitoring_v4.sli import SLIProjection
from stream_contracts.monitoring_v4.time import ContractTimeError
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.runtime.coverage import source_coverage
from stream_monitoring_v4.storage.repository import MonitoringRepository
from stream_monitoring_v4.storage.schema import DDL_V1, DDL_V2, DDL_V3, DDL_V4

from tests.helpers import BASE_TS, current, observation


APPLIED_AT = "2026-08-11T12:00:00Z"


class ContractTests(unittest.TestCase):
    def test_observation_id_is_stable_and_json_payload_is_copied(self) -> None:
        payload = {"b": 2, "a": [1]}
        first = observation(payload=payload)
        payload["a"].append(9)
        second = observation(payload={"a": [1], "b": 2})
        self.assertEqual(first.observation_id, second.observation_id)
        self.assertEqual(first.payload, {"a": [1], "b": 2})

    def test_missing_naive_non_utc_and_future_timestamps_are_rejected(self) -> None:
        base = dict(
            domain="delivery",
            source="runtime_delivery_watchdog",
            source_event_id="event-1",
            source_generation="generation-1",
            evidence_role="current_authoritative",
            status="good",
            reason_code="sample_good",
            freshness_limit_sec=180,
            producer_revision="test-r1",
            payload={},
        )
        for observed in ("", "2026-08-11T12:00:00", "2026-08-11T21:00:00+09:00"):
            with self.subTest(observed=observed):
                with self.assertRaises(ContractTimeError):
                    ObservationEnvelope.create(
                        **base,
                        observed_at=observed,
                        received_at="2026-08-11T12:00:01Z",
                    )
        with self.assertRaisesRegex(ContractTimeError, "must not precede"):
            ObservationEnvelope.create(
                **base,
                observed_at="2026-08-11T12:00:02Z",
                received_at="2026-08-11T12:00:01Z",
            )

    def test_domain_current_cannot_expire_before_evaluation(self) -> None:
        with self.assertRaisesRegex(ContractTimeError, "valid_until"):
            DomainCurrent.create(
                domain="delivery",
                state="unknown",
                reason_codes=("stale",),
                source_observation_ids=(),
                observed_at="2026-08-11T12:00:00Z",
                reduced_at="2026-08-11T12:01:00Z",
                valid_until="2026-08-11T12:00:30Z",
                policy_revision="test-r3",
                reducer_revision="test-r3",
                payload={},
            )

    def test_recovery_target_is_typed_but_does_not_execute(self) -> None:
        episode_id = stable_id("inc", "delivery", "episode")
        auth = RecoveryAuthorization.create(
            episode_id=episode_id,
            action="restart_workload",
            target="deployment/stream-v3-runtime",
            authorized_at="2026-08-11T12:00:00Z",
            expires_at="2026-08-11T12:05:00Z",
            policy_revision="test-r1",
        )
        command = RuntimeControlCommand.create(
            authorization_id=auth.authorization_id,
            action=auth.action,
            target=auth.target,
            issued_at=auth.authorized_at,
            expires_at=auth.expires_at,
            idempotency_key="episode/action/1",
            target_generation="pod-123",
        )
        self.assertEqual(command.target, "deployment/stream-v3-runtime")
        with self.assertRaisesRegex(ValueError, "kind/name"):
            RecoveryAuthorization.create(
                episode_id=episode_id,
                action="restart_workload",
                target="stream-v3-runtime",
                authorized_at="2026-08-11T12:00:00Z",
                expires_at="2026-08-11T12:05:00Z",
                policy_revision="test-r1",
            )

    def test_sli_projection_cannot_become_current_or_automatic_recovery_authority(self) -> None:
        evidence_id = stable_id("evd", "fixture")
        item = SLIProjection.create(
            objective_id="youtube_input_quality",
            window="rolling_7d",
            assessment_scope="formal",
            is_official_window=True,
            observed=100,
            eligible=100,
            bad=0,
            missing=0,
            coverage_pct=100,
            source_freshness_pct=100,
            source_disagreement=False,
            compliance_status="met",
            measurement_unknown_reasons=(),
            window_start="2026-08-04T12:00:00Z",
            window_end="2026-08-11T12:00:00Z",
            evaluated_at="2026-08-11T12:00:00Z",
            policy_revision="test-r6",
            evidence_ids=(evidence_id,),
            payload={"sli_pct": 100.0},
        )
        self.assertTrue(item.no_automatic_recovery)
        self.assertEqual(SLIProjection.from_dict(item.to_dict()), item)
        with self.assertRaisesRegex(ValueError, "non-formal"):
            SLIProjection.create(
                objective_id="youtube_input_quality",
                window="rolling_1h",
                assessment_scope="fast",
                is_official_window=False,
                observed=None,
                eligible=None,
                bad=None,
                missing=None,
                coverage_pct=100,
                source_freshness_pct=100,
                source_disagreement=False,
                compliance_status="met",
                measurement_unknown_reasons=(),
                window_start="2026-08-11T11:00:00Z",
                window_end="2026-08-11T12:00:00Z",
                evaluated_at="2026-08-11T12:00:00Z",
                policy_revision="test-r6",
                evidence_ids=(evidence_id,),
                payload={},
            )

    def test_schema_files_have_unique_versioned_ids(self) -> None:
        schema_dir = Path(__file__).resolve().parents[1] / "src" / "stream_contracts" / "monitoring_v4" / "schema"
        paths = sorted(schema_dir.glob("*.json"))
        payloads = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
        identifiers = [item["$id"] for item in payloads]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        for path, identifier in zip(paths, identifiers, strict=True):
            version = path.stem.rsplit(".", 1)[-1]
            with self.subTest(schema=path.name):
                self.assertRegex(version, r"^v[1-9][0-9]*$")
                self.assertTrue(
                    identifier.endswith(f".{version}"),
                    f"{path.name}: $id {identifier!r} does not use filename version {version!r}",
                )

    def test_contract_decoders_reject_coercion_missing_and_extra_fields(self) -> None:
        source = observation().to_dict()
        malformed_observations = []
        for field, replacement in (
            ("freshness_limit_sec", True),
            ("payload", []),
            ("producer_revision", 7),
        ):
            item = dict(source)
            item[field] = replacement
            malformed_observations.append(item)
        missing = dict(source)
        del missing["reason_code"]
        malformed_observations.append(missing)
        extra = dict(source)
        extra["unexpected"] = "silently-ignored-before"
        malformed_observations.append(extra)
        for item in malformed_observations:
            with self.subTest(observation=item):
                with self.assertRaises(ValueError):
                    ObservationEnvelope.from_dict(item)

        projection = SLIProjection.create(
            objective_id="youtube_input_quality",
            window="rolling_7d",
            assessment_scope="formal",
            is_official_window=True,
            observed=100,
            eligible=100,
            bad=0,
            missing=0,
            coverage_pct=100,
            source_freshness_pct=100,
            source_disagreement=False,
            compliance_status="met",
            measurement_unknown_reasons=(),
            window_start="2026-08-04T12:00:00Z",
            window_end="2026-08-11T12:00:00Z",
            evaluated_at="2026-08-11T12:00:00Z",
            policy_revision="test-r6",
            evidence_ids=(stable_id("evd", "strict-fixture"),),
            payload={"sli_pct": 100.0},
        ).to_dict()
        for field, replacement in (
            ("is_official_window", "true"),
            ("source_disagreement", 0),
            ("observed", "100"),
            ("measurement_unknown_reasons", "none"),
        ):
            item = dict(projection)
            item[field] = replacement
            with self.subTest(sli_field=field):
                with self.assertRaises(ValueError):
                    SLIProjection.from_dict(item)

        episode = IncidentEpisode.open(
            domain="delivery",
            severity="warning",
            opened_at="2026-08-11T12:00:00Z",
            summary="strict contract fixture",
            reason_codes=("sample_bad",),
            current_snapshot_id=current(state="bad").snapshot_id,
            current_state="bad",
            next_notification_at="2026-08-11T12:10:00Z",
            policy_revision="test-r1",
        ).to_dict()
        episode["bad_samples"] = 1.5
        with self.assertRaises(ValueError):
            IncidentEpisode.from_dict(episode)


class StorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repository = MonitoringRepository(self.root / "monitoring.sqlite3", busy_timeout_ms=20)
        self.repository.initialize(applied_at=APPLIED_AT)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_wal_foreign_keys_integrity_and_schema_version(self) -> None:
        with self.repository.connection(read_only=True) as connection:
            self.assertEqual(str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(), "wal")
            self.assertEqual(int(connection.execute("PRAGMA foreign_keys").fetchone()[0]), 1)
            self.assertEqual(int(connection.execute("PRAGMA user_version").fetchone()[0]), 6)
        self.assertEqual(self.repository.integrity_check(), "ok")
        self.assertTrue(self.repository.ping())

    def test_duplicate_observation_is_not_inserted_twice(self) -> None:
        item = observation()
        self.assertTrue(self.repository.append_observation(item))
        self.assertFalse(self.repository.append_observation(item))
        self.assertEqual(self.repository.latest_observations("delivery"), [item])

    def test_older_replay_cannot_replace_newer_domain_current(self) -> None:
        newer = current(state="good", observed_ts=BASE_TS + 600, marker="newer")
        older = current(state="bad", observed_ts=BASE_TS, marker="older")
        self.assertTrue(self.repository.save_current(newer))
        self.assertFalse(self.repository.save_current(older))

        selected = self.repository.current("delivery")
        self.assertIsNotNone(selected)
        self.assertEqual(selected.snapshot_id, newer.snapshot_id)
        self.assertEqual(selected.state, "good")
        with self.repository.connection(read_only=True) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM current_snapshots WHERE domain='delivery'"
            ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_equal_timestamp_semantic_replacement_becomes_domain_current(self) -> None:
        initial = current(
            state="good",
            observed_ts=BASE_TS,
            reduced_ts=BASE_TS,
            marker="first",
        )
        disagreement = current(
            state="unknown",
            observed_ts=BASE_TS,
            reduced_ts=BASE_TS,
            reason="source_disagreement",
            marker="candidate",
        )
        self.assertLess(disagreement.snapshot_id, initial.snapshot_id)
        self.assertTrue(self.repository.save_current(initial))
        self.assertTrue(self.repository.save_current(disagreement))

        selected = self.repository.current("delivery")
        self.assertIsNotNone(selected)
        self.assertEqual(selected.snapshot_id, disagreement.snapshot_id)
        self.assertEqual(selected.state, "unknown")
        self.assertEqual(selected.reason_codes, ("source_disagreement",))

    def test_expired_current_is_replaced_by_older_fresh_fallback(self) -> None:
        expired_high_priority = current(
            state="bad",
            observed_ts=BASE_TS + 120,
            reduced_ts=BASE_TS + 120,
            marker="expired-high-priority",
        )
        fresh_fallback = current(
            state="good",
            observed_ts=BASE_TS,
            reduced_ts=BASE_TS + 300,
            marker="fresh-fallback",
        )
        self.assertTrue(self.repository.save_current(expired_high_priority))
        self.assertTrue(self.repository.save_current(fresh_fallback))

        selected = self.repository.current("delivery")
        self.assertIsNotNone(selected)
        self.assertEqual(selected.snapshot_id, fresh_fallback.snapshot_id)
        self.assertEqual(selected.state, "good")
        self.assertEqual(selected.observed_at, fresh_fallback.observed_at)

    def test_live_snapshot_references_reject_dangling_child_and_parent_rows(self) -> None:
        item = current(state="bad")
        self.repository.save_current(item)
        with self.repository.connection() as connection:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "snapshot reference mismatch"):
                connection.execute(
                    """INSERT INTO incident_candidates(
                        domain, state, first_seen_at, first_seen_ts, last_seen_at,
                        last_seen_ts, samples, snapshot_id, reason_codes_json
                    ) VALUES ('delivery', 'bad', ?, ?, ?, ?, 1, 'cur:missing', '[]')""",
                    (APPLIED_AT, BASE_TS, APPLIED_AT, BASE_TS),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "referenced by live evidence"):
                connection.execute(
                    "DELETE FROM current_snapshots WHERE snapshot_id=?",
                    (item.snapshot_id,),
                )

    def test_v5_migration_refuses_preexisting_dangling_live_snapshot_reference(self) -> None:
        path = self.root / "broken-v4.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.executescript(DDL_V1)
            connection.executescript(DDL_V2)
            connection.executescript(DDL_V3)
            connection.executescript(DDL_V4)
            connection.executemany(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                ((1, APPLIED_AT), (2, APPLIED_AT), (3, APPLIED_AT), (4, APPLIED_AT)),
            )
            connection.execute(
                """INSERT INTO domain_current(
                    domain, snapshot_id, state, observed_at, observed_ts, reduced_at,
                    reduced_ts, valid_until, valid_until_ts, policy_revision,
                    reducer_revision, reason_codes_json, source_observation_ids_json,
                    payload_json
                ) VALUES ('delivery', 'cur:missing', 'bad', ?, ?, ?, ?, ?, ?,
                          'test', 'test', '[]', '[]', '{}')""",
                (APPLIED_AT, BASE_TS, APPLIED_AT, BASE_TS, APPLIED_AT, BASE_TS),
            )
            connection.execute("PRAGMA user_version=4")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(RuntimeError, "live snapshot reference integrity"):
            MonitoringRepository(path).initialize(applied_at=APPLIED_AT)

    def test_reducer_returns_canonical_current_after_clock_rollback(self) -> None:
        future = observation(
            status="good",
            observed_ts=BASE_TS + 600,
            received_ts=BASE_TS + 600,
            event="future-good",
        )
        self.repository.append_observation(future)
        reducer = CurrentReducerService(self.repository, DEFAULT_POLICIES)
        first = reducer.reduce(("delivery",), now_ts=BASE_TS + 600)[0]
        self.assertEqual(first.state, "good")

        rolled_back = reducer.reduce(("delivery",), now_ts=BASE_TS)[0]
        self.assertEqual(rolled_back.snapshot_id, first.snapshot_id)
        self.assertEqual(rolled_back.state, "good")
        self.assertEqual(self.repository.current("delivery").snapshot_id, first.snapshot_id)
        with self.repository.connection(read_only=True) as connection:
            health = connection.execute(
                "SELECT checked_ts FROM component_health WHERE component='current_reducer'"
            ).fetchone()
        self.assertEqual(int(health["checked_ts"]), BASE_TS + 600)

    def test_transaction_rolls_back_without_silent_partial_write(self) -> None:
        item = observation()
        with self.assertRaisesRegex(RuntimeError, "injected"):
            with self.repository.transaction() as connection:
                self.repository.append_observation(item, connection=connection)
                raise RuntimeError("injected")
        self.assertEqual(self.repository.latest_observations("delivery"), [])

    def test_locked_database_raises_instead_of_dropping(self) -> None:
        locker = self.repository.connect()
        try:
            locker.execute("BEGIN IMMEDIATE")
            with self.assertRaises(sqlite3.OperationalError):
                self.repository.append_observation(observation())
        finally:
            locker.rollback()
            locker.close()
        self.assertEqual(self.repository.latest_observations("delivery"), [])

    def test_newer_schema_is_rejected(self) -> None:
        path = self.root / "future.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.execute("PRAGMA user_version=99")
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(RuntimeError, "newer than supported"):
            MonitoringRepository(path).initialize(applied_at=APPLIED_AT)

    def test_v1_database_migrates_forward_without_recreating_baseline_tables(self) -> None:
        path = self.root / "v1.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.executescript(DDL_V1)
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (1, ?)",
                (APPLIED_AT,),
            )
            connection.execute("PRAGMA user_version=1")
            connection.commit()
        finally:
            connection.close()
        repository = MonitoringRepository(path)
        repository.initialize(applied_at=APPLIED_AT)
        with repository.connection(read_only=True) as migrated:
            self.assertEqual(int(migrated.execute("PRAGMA user_version").fetchone()[0]), 6)
            names = {
                row[0]
                for row in migrated.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertIn("observations", names)
        self.assertIn("sli_projections", names)
        self.assertIn("shadow_cycles", names)
        with repository.connection(read_only=True) as migrated:
            columns = {
                row[1] for row in migrated.execute("PRAGMA table_info(shadow_cycles)").fetchall()
            }
            migrations = [
                row[0]
                for row in migrated.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
            ]
        self.assertIn("source_revision", columns)
        self.assertEqual(migrations, [1, 2, 3, 4, 5, 6])

    def test_v2_shadow_cycle_migrates_with_explicit_unknown_source_revision(self) -> None:
        path = self.root / "v2.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.executescript(DDL_V1)
            connection.executescript(DDL_V2)
            connection.executemany(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                ((1, APPLIED_AT), (2, APPLIED_AT)),
            )
            connection.execute(
                """INSERT INTO shadow_cycles(
                    cycle_id, started_at, started_ts, completed_at, completed_ts,
                    build_revision, observer_json, current_states_json, parity_json,
                    notification_intent_count, real_delivery_enabled, runtime_mutation_enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)""",
                (
                    "cyc:legacy-v2",
                    APPLIED_AT,
                    BASE_TS,
                    APPLIED_AT,
                    BASE_TS,
                    "legacy-build",
                    "{}",
                    "{}",
                    "{}",
                ),
            )
            connection.execute("PRAGMA user_version=2")
            connection.commit()
        finally:
            connection.close()

        repository = MonitoringRepository(path)
        repository.initialize(applied_at=APPLIED_AT)
        rows = repository.shadow_cycle_rows()
        self.assertEqual(rows[0]["source_revision"], "unknown-source-revision")
        with repository.connection(read_only=True) as migrated:
            self.assertEqual(int(migrated.execute("PRAGMA user_version").fetchone()[0]), 6)

    def test_v3_notification_backlog_migrates_as_nondeliverable_shadow(self) -> None:
        path = self.root / "v3-outbox.sqlite3"
        connection = sqlite3.connect(path)
        intent_id = stable_id("ntf", "legacy-shadow")
        try:
            connection.executescript(DDL_V1)
            connection.executescript(DDL_V2)
            connection.executescript(DDL_V3)
            connection.executemany(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                ((1, APPLIED_AT), (2, APPLIED_AT), (3, APPLIED_AT)),
            )
            connection.execute(
                """INSERT INTO notification_intents(
                    intent_id, transition_id, episode_id, route, phase, severity,
                    created_at, created_ts, not_before, not_before_ts, subject,
                    content, dedupe_key, route_policy_revision, template_revision
                ) VALUES (?, ?, ?, 'discord', 'detected', 'warning', ?, ?, ?, ?,
                    'legacy', 'legacy', 'legacy-dedupe', 'legacy-route', 'legacy-template')""",
                (
                    intent_id,
                    stable_id("trn", "legacy-shadow"),
                    stable_id("inc", "legacy-shadow"),
                    APPLIED_AT,
                    BASE_TS,
                    APPLIED_AT,
                    BASE_TS,
                ),
            )
            connection.execute("PRAGMA user_version=3")
            connection.commit()
        finally:
            connection.close()

        repository = MonitoringRepository(path)
        repository.initialize(applied_at=APPLIED_AT)
        metadata = repository.intent_delivery_metadata(intent_id)
        self.assertEqual(metadata["mode"], "shadow")
        self.assertEqual(metadata["eligible"], 0)
        self.assertEqual(metadata["eligibility_reason"], "pre_cutover_quarantine")

    def test_backup_and_non_destructive_restore(self) -> None:
        item = observation()
        self.repository.append_observation(item)
        backup = self.root / "backup.sqlite3"
        restored_path = self.root / "restored.sqlite3"
        self.repository.backup(backup)
        restored = MonitoringRepository.restore(backup, restored_path, applied_at=APPLIED_AT)
        self.assertEqual(restored.integrity_check(), "ok")
        self.assertEqual(restored.latest_observations("delivery"), [item])
        with self.assertRaises(FileExistsError):
            MonitoringRepository.restore(backup, restored_path, applied_at=APPLIED_AT)
        with self.assertRaises(FileExistsError):
            self.repository.backup(backup)

    def test_restore_handles_uri_characters_and_never_publishes_a_corrupt_copy(self) -> None:
        item = observation()
        self.repository.append_observation(item)
        special = self.root / "backup #1.sqlite3"
        restored_path = self.root / "restored #1.sqlite3"
        self.repository.backup(special)
        restored = MonitoringRepository.restore(special, restored_path, applied_at=APPLIED_AT)
        self.assertEqual(restored.latest_observations("delivery"), [item])

        corrupt = self.root / "corrupt #1.sqlite3"
        corrupt.write_bytes(b"not a sqlite database")
        unpublished = self.root / "must-not-exist.sqlite3"
        with self.assertRaises(sqlite3.DatabaseError):
            MonitoringRepository.restore(corrupt, unpublished, applied_at=APPLIED_AT)
        self.assertFalse(os.path.lexists(unpublished))

    def test_backup_and_restore_refuse_broken_symlink_targets(self) -> None:
        backup_target = self.root / "backup.sqlite3"
        restore_target = self.root / "restore.sqlite3"
        backup_target.symlink_to(self.root / "missing-backup-target")
        restore_target.symlink_to(self.root / "missing-restore-target")
        with self.assertRaises(FileExistsError):
            self.repository.backup(backup_target)

        source = self.root / "source.sqlite3"
        self.repository.backup(source)
        with self.assertRaises(FileExistsError):
            MonitoringRepository.restore(source, restore_target, applied_at=APPLIED_AT)

    def test_seven_day_coverage_counts_buckets_not_duplicates(self) -> None:
        cadence = 300
        expected = 7 * 24 * 60 * 60 // cadence
        items = [
            observation(observed_ts=BASE_TS + index * cadence, event=f"sample-{index}")
            for index in range(expected)
            if index != 100
        ]
        items.append(items[0])
        report = source_coverage(
            items,
            source="runtime_delivery_watchdog",
            window_start_ts=BASE_TS,
            window_end_ts=BASE_TS + 7 * 24 * 60 * 60,
            cadence_sec=cadence,
        )
        self.assertEqual(report.expected, 2016)
        self.assertEqual(report.observed, 2015)
        self.assertEqual(report.missing, 1)
        self.assertEqual(report.duplicate_or_extra, 1)


if __name__ == "__main__":
    unittest.main()
