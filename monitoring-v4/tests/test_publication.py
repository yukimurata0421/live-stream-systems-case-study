from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.compatibility.publication import (
    reconcile_public_safe_publication,
)
from stream_monitoring_v4.compatibility.public_safe import (
    PUBLIC_SCHEMA,
    public_safe_bytes,
    write_public_safe_atomic,
)
from stream_monitoring_v4.storage.publications import PUBLIC_SAFE_ARTIFACT_KEY
from stream_monitoring_v4.storage.repository import MonitoringRepository
from stream_monitoring_v4.runtime.shadow_cycle import (
    ShadowCycleRequest,
    execute_shadow_cycle,
)

from tests.helpers import BASE_TS


class ArtifactPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repository = MonitoringRepository(self.root / "monitoring.sqlite3")
        self.repository.initialize(applied_at=utc_text(BASE_TS))
        self.output = self.root / "artifacts" / "public-safe-shadow.json"

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _payload(generated_ts: int) -> dict[str, object]:
        return {
            "schema": PUBLIC_SCHEMA,
            "generated_at": utc_text(generated_ts),
            "scope": "isolated_non_public_compatibility_shadow",
            "interpretation": (
                "Measurements only; this artifact is not an incident or runtime control input."
            ),
            "items": [],
        }

    def _cycle(self, suffix: str, ts: int) -> str:
        cycle_id = stable_id("cyc", suffix)
        self.repository.append_shadow_cycle(
            cycle_id=cycle_id,
            started_at=utc_text(ts),
            completed_at=utc_text(ts),
            build_revision="a" * 40,
            source_revision="source-test",
            observer={},
            current_states={},
            parity={},
            notification_intent_count=0,
        )
        return cycle_id

    def _stage(self, suffix: str, ts: int):
        cycle_id = self._cycle(suffix, ts)
        return self.repository.stage_artifact_publication(
            artifact_key=PUBLIC_SAFE_ARTIFACT_KEY,
            cycle_id=cycle_id,
            created_at=utc_text(ts),
            payload=self._payload(ts),
        )

    def test_committed_intent_recovers_crash_before_file_publication(self) -> None:
        staged = self._stage("before-file", BASE_TS)
        self.assertFalse(self.output.exists())
        self.assertEqual(staged.state, "pending")

        result = reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 1,
        )

        self.assertIsNotNone(result)
        self.assertTrue(result.changed)
        self.assertEqual(self.output.read_bytes(), public_safe_bytes(staged.payload))
        published = self.repository.artifact_publications(PUBLIC_SAFE_ARTIFACT_KEY)
        self.assertEqual([item.state for item in published], ["published"])
        self.assertEqual(published[0].attempt_count, 1)

    def test_pending_intent_is_repaired_before_a_new_source_cycle_can_fail(self) -> None:
        staged = self._stage("repair-before-source", BASE_TS)
        request = ShadowCycleRequest(
            state_root=self.root / "source-state",
            source_repository=self.root / "migration-source",
            database=self.repository.path,
            public_shadow_output=self.output,
            build_revision="a" * 40,
            source_revision="source-test",
            fixed_now_ts=BASE_TS + 1,
            summary_only=True,
        )
        with patch(
            "stream_monitoring_v4.runtime.shadow_cycle._run_pipeline",
            side_effect=RuntimeError("new source cycle failed"),
        ), self.assertRaisesRegex(RuntimeError, "source cycle failed"):
            execute_shadow_cycle(self.repository, request)

        self.assertEqual(self.output.read_bytes(), public_safe_bytes(staged.payload))
        publication = self.repository.artifact_publications(
            PUBLIC_SAFE_ARTIFACT_KEY
        )[0]
        self.assertEqual(publication.state, "published")
        self.assertEqual(publication.attempt_count, 1)

    def test_exact_file_recovers_crash_before_database_ack_without_rewrite(self) -> None:
        staged = self._stage("before-ack", BASE_TS)
        write_public_safe_atomic(self.output, staged.payload)
        before_mtime = self.output.stat().st_mtime_ns

        result = reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 1,
        )

        self.assertTrue(result.changed)
        self.assertEqual(self.output.stat().st_mtime_ns, before_mtime)
        publication = self.repository.artifact_publications(PUBLIC_SAFE_ARTIFACT_KEY)[0]
        self.assertEqual(publication.state, "published")
        self.assertEqual(publication.attempt_count, 1)

    def test_newer_intent_supersedes_pending_payload_before_reconciliation(self) -> None:
        older = self._stage("older", BASE_TS)
        newer = self._stage("newer", BASE_TS + 60)
        publications = self.repository.artifact_publications(PUBLIC_SAFE_ARTIFACT_KEY)
        self.assertEqual([item.state for item in publications], ["superseded", "pending"])

        result = reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 61,
        )

        self.assertEqual(result.publication_id, newer.publication_id)
        self.assertNotEqual(result.publication_id, older.publication_id)
        self.assertEqual(self.output.read_bytes(), public_safe_bytes(newer.payload))

    def test_pending_intent_wins_after_wall_clock_rollback(self) -> None:
        published = self._stage("future-published", BASE_TS + 600)
        reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 601,
        )
        pending = self._stage("clock-rolled-back", BASE_TS)

        selected = self.repository.reconcilable_artifact_publication(
            PUBLIC_SAFE_ARTIFACT_KEY
        )
        self.assertIsNotNone(selected)
        self.assertEqual(selected.publication_id, pending.publication_id)

        result = reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 1,
        )
        self.assertEqual(result.publication_id, pending.publication_id)
        self.assertNotEqual(result.publication_id, published.publication_id)
        self.assertEqual(self.output.read_bytes(), public_safe_bytes(pending.payload))

    def test_missing_published_file_is_repaired_from_the_ledger(self) -> None:
        staged = self._stage("repair", BASE_TS)
        reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 1,
        )
        self.output.unlink()

        result = reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 2,
        )

        self.assertTrue(result.changed)
        self.assertEqual(self.output.read_bytes(), public_safe_bytes(staged.payload))
        publication = self.repository.artifact_publications(PUBLIC_SAFE_ARTIFACT_KEY)[0]
        self.assertEqual(publication.state, "published")
        self.assertEqual(publication.attempt_count, 2)

    def test_failed_filesystem_attempt_remains_pending_and_retryable(self) -> None:
        staged = self._stage("retry", BASE_TS)
        blocking_parent = self.root / "not-a-directory"
        blocking_parent.write_text("blocked", encoding="utf-8")
        blocked_output = blocking_parent / "artifact.json"

        with self.assertRaises(OSError):
            reconcile_public_safe_publication(
                self.repository,
                blocked_output,
                now_ts=BASE_TS + 1,
            )
        failed = self.repository.artifact_publications(PUBLIC_SAFE_ARTIFACT_KEY)[0]
        self.assertEqual(failed.state, "pending")
        self.assertEqual(failed.attempt_count, 1)
        self.assertTrue(failed.last_error)

        result = reconcile_public_safe_publication(
            self.repository,
            self.output,
            now_ts=BASE_TS + 2,
        )
        self.assertEqual(result.publication_id, staged.publication_id)
        self.assertEqual(
            self.repository.artifact_publications(PUBLIC_SAFE_ARTIFACT_KEY)[0].state,
            "published",
        )

    def test_same_cycle_cannot_be_reused_for_different_payload(self) -> None:
        staged = self._stage("collision", BASE_TS)
        changed = self._payload(BASE_TS + 1)
        with self.assertRaisesRegex(RuntimeError, "identity collision"):
            self.repository.stage_artifact_publication(
                artifact_key=PUBLIC_SAFE_ARTIFACT_KEY,
                cycle_id=staged.cycle_id,
                created_at=utc_text(BASE_TS),
                payload=changed,
            )
        self.assertEqual(
            len(self.repository.artifact_publications(PUBLIC_SAFE_ARTIFACT_KEY)),
            1,
        )


if __name__ == "__main__":
    unittest.main()
