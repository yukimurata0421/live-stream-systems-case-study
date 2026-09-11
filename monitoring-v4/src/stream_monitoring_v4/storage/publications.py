from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from stream_contracts.monitoring_v4.ids import (
    canonical_json,
    payload_sha256,
    stable_id,
)
from stream_contracts.monitoring_v4.time import unix_ts


PUBLIC_SAFE_ARTIFACT_KEY = "public-safe-shadow"


@dataclass(frozen=True)
class ArtifactPublication:
    publication_id: str
    artifact_key: str
    cycle_id: str
    state: str
    created_at: str
    payload_sha256: str
    payload: dict[str, Any]
    attempt_count: int
    last_attempt_at: str
    published_at: str
    last_error: str


class ArtifactPublicationRepositoryMixin:
    """Durable intent ledger for filesystem artifacts produced by the core writer."""

    def stage_artifact_publication(
        self,
        *,
        artifact_key: str,
        cycle_id: str,
        created_at: str,
        payload: Mapping[str, Any],
        connection: Any | None = None,
    ) -> ArtifactPublication:
        if not artifact_key or len(artifact_key) > 80:
            raise ValueError("artifact_key must contain 1-80 characters")
        created_ts = unix_ts(created_at)
        payload_value = json.loads(canonical_json(payload))
        digest = payload_sha256(payload_value)
        publication_id = stable_id(
            "pub",
            artifact_key,
            cycle_id,
            digest,
        )
        if connection is None:
            with self.transaction() as owned_connection:
                return self.stage_artifact_publication(
                    artifact_key=artifact_key,
                    cycle_id=cycle_id,
                    created_at=created_at,
                    payload=payload_value,
                    connection=owned_connection,
                )
        existing = connection.execute(
            "SELECT * FROM public_artifact_publications "
            "WHERE artifact_key=? AND cycle_id=?",
            (artifact_key, cycle_id),
        ).fetchone()
        if existing is not None:
            result = self._artifact_publication(existing)
            if (
                result.publication_id != publication_id
                or result.payload_sha256 != digest
                or result.payload != payload_value
            ):
                raise RuntimeError(
                    "artifact publication identity collision for existing cycle"
                )
            return result
        connection.execute(
            """UPDATE public_artifact_publications
            SET state='superseded', last_error=?
            WHERE artifact_key=? AND state='pending'""",
            (f"superseded_by:{publication_id}", artifact_key),
        )
        connection.execute(
            """INSERT INTO public_artifact_publications(
                publication_id, artifact_key, cycle_id, state,
                created_at, created_ts, payload_sha256, payload_json,
                attempt_count, last_attempt_at, last_attempt_ts,
                published_at, published_ts, last_error
            ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, 0, '', 0, '', 0, '')""",
            (
                publication_id,
                artifact_key,
                cycle_id,
                created_at,
                created_ts,
                digest,
                canonical_json(payload_value),
            ),
        )
        row = connection.execute(
            "SELECT * FROM public_artifact_publications WHERE publication_id=?",
            (publication_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("staged artifact publication disappeared")
        return self._artifact_publication(row)

    def reconcilable_artifact_publication(
        self,
        artifact_key: str,
    ) -> ArtifactPublication | None:
        with self.connection(read_only=True) as connection:
            row = connection.execute(
                """SELECT * FROM public_artifact_publications
                WHERE artifact_key=? AND state IN ('pending', 'published')
                ORDER BY CASE WHEN state='pending' THEN 0 ELSE 1 END,
                         created_ts DESC, publication_id DESC LIMIT 1""",
                (artifact_key,),
            ).fetchone()
        if row is None:
            return None
        return self._artifact_publication(row)

    def mark_artifact_publication_published(
        self,
        publication_id: str,
        *,
        expected_payload_sha256: str,
        published_at: str,
    ) -> bool:
        published_ts = unix_ts(published_at)
        with self.transaction() as connection:
            cursor = connection.execute(
                """UPDATE public_artifact_publications
                SET state='published', attempt_count=attempt_count+1,
                    last_attempt_at=?, last_attempt_ts=?,
                    published_at=?, published_ts=?, last_error=''
                WHERE publication_id=? AND state='pending' AND payload_sha256=?""",
                (
                    published_at,
                    published_ts,
                    published_at,
                    published_ts,
                    publication_id,
                    expected_payload_sha256,
                ),
            )
            if cursor.rowcount == 1:
                return True
            row = connection.execute(
                "SELECT state, payload_sha256 FROM public_artifact_publications "
                "WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            if row is not None and row["state"] == "published":
                repair = connection.execute(
                    """UPDATE public_artifact_publications
                    SET attempt_count=attempt_count+1,
                        last_attempt_at=?, last_attempt_ts=?, last_error=''
                    WHERE publication_id=? AND state='published'
                      AND payload_sha256=?""",
                    (
                        published_at,
                        published_ts,
                        publication_id,
                        expected_payload_sha256,
                    ),
                )
                if repair.rowcount == 1:
                    return False
            raise RuntimeError("artifact publication lost its pending fence")

    def record_artifact_publication_failure(
        self,
        publication_id: str,
        *,
        attempted_at: str,
        error: str,
    ) -> bool:
        attempted_ts = unix_ts(attempted_at)
        with self.transaction() as connection:
            cursor = connection.execute(
                """UPDATE public_artifact_publications
                SET attempt_count=attempt_count+1,
                    last_attempt_at=?, last_attempt_ts=?, last_error=?
                WHERE publication_id=? AND state IN ('pending', 'published')""",
                (
                    attempted_at,
                    attempted_ts,
                    error.strip()[:200] or "publication_failed",
                    publication_id,
                ),
            )
            return cursor.rowcount == 1

    def artifact_publications(self, artifact_key: str) -> list[ArtifactPublication]:
        with self.connection(read_only=True) as connection:
            rows = connection.execute(
                """SELECT * FROM public_artifact_publications
                WHERE artifact_key=? ORDER BY created_ts, publication_id""",
                (artifact_key,),
            ).fetchall()
        return [self._artifact_publication(row) for row in rows]

    @staticmethod
    def _artifact_publication(row: Any) -> ArtifactPublication:
        payload = json.loads(row["payload_json"])
        if not isinstance(payload, dict):
            raise RuntimeError("artifact publication payload is not an object")
        digest = payload_sha256(payload)
        if digest != row["payload_sha256"]:
            raise RuntimeError("artifact publication payload digest mismatch")
        return ArtifactPublication(
            publication_id=str(row["publication_id"]),
            artifact_key=str(row["artifact_key"]),
            cycle_id=str(row["cycle_id"]),
            state=str(row["state"]),
            created_at=str(row["created_at"]),
            payload_sha256=digest,
            payload=payload,
            attempt_count=int(row["attempt_count"]),
            last_attempt_at=str(row["last_attempt_at"]),
            published_at=str(row["published_at"]),
            last_error=str(row["last_error"]),
        )
