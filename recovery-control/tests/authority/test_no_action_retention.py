from __future__ import annotations

import base64
import json
import os
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.no_action_store import NoActionCentralStore
from cra_authority.retention import SignedNoActionArchive
from cra_dell_recovery.models import RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.time import isoformat_utc

ROOT = Path(__file__).resolve().parents[2]


def _projection(index: int, now: datetime) -> dict[str, object]:
    return {
        "projection_id": f"projection-{index}",
        "target_id": "stream-target",
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "arena-release-a",
        "monitoring_cycle_id": f"cycle-{index}",
        "observation_revision": f"revision-{index}",
        "observation_sequence": index,
        "incident": {
            "incident_id": f"incident-{index}",
            "source_episode_id": f"episode-{index}",
            "state": "CONFIRMED",
        },
        "observed_target": {
            "host_id": "dell",
            "host_boot_id": "boot-a",
            "namespace": "stream-v3",
            "pod_uid": "pod-a",
            "container_name": "stream-engine",
            "container_id": "containerd://a",
            "ffmpeg_generation": "generation-a",
            "ffmpeg_pid": 4100,
        },
        "observed_at": isoformat_utc(now),
        "issued_at": isoformat_utc(now),
        "expires_at": isoformat_utc(now + timedelta(seconds=60)),
        "payload_sha256": f"{index:064x}",
        "key_id": "monitoring-key-a",
        "signature": "signed",
    }


def test_no_action_decision_is_bound_to_projection_context_and_expiry(tmp_path: Path) -> None:
    store = NoActionCentralStore(tmp_path / "central.db", ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="dell-installation",
    )
    now = datetime(2026, 8, 30, 1, 0, tzinfo=UTC)
    projection = _projection(1, now)
    assert store.ingest_monitoring_projection(projection) == "ACCEPTED"
    target = TargetIdentity("dell", "boot-a", "stream-v3", "pod-a", "stream-engine", "containerd://a", "generation-a", 4100)
    authorization = RecoveryAuthorizationInput(
        authorization_id="authorization-1",
        incident_id="incident-1",
        source_episode_id="episode-1",
        target_id="stream-target",
        action="restart_ffmpeg",
        reason_code="confirmed_tcp_stall",
        policy_revision="policy-a",
        observation_revision="revision-1",
        expected_target=target,
        blockers=("CRA_OPERATING_MODE_NO_ACTION",),
        authorized_at=isoformat_utc(now + timedelta(seconds=1)),
        expires_at=isoformat_utc(now + timedelta(seconds=31)),
    )
    try:
        with pytest.raises(ValueError, match="NO_ACTION_DECISION_CONTEXT_MISMATCH"):
            store.persist_policy_decision(
                projection=projection,
                authorization=replace(authorization, observation_revision="wrong-revision"),
                decision_id="decision-context-mismatch",
                decision="BLOCKED",
                blockers=authorization.blockers,
            )
        with pytest.raises(ValueError, match="NO_ACTION_DECISION_TIME_BOUNDARY_MISMATCH"):
            store.persist_policy_decision(
                projection=projection,
                authorization=replace(authorization, expires_at=isoformat_utc(now + timedelta(seconds=61))),
                decision_id="decision-expiry-mismatch",
                decision="BLOCKED",
                blockers=authorization.blockers,
            )
        assert store.read_one("SELECT count(*) FROM cra_policy_decisions")[0] == 0
    finally:
        store.close()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("candidate_reason_code", "different_candidate"),
        ("decision_reason_code", "different_decision"),
    ],
)
def test_each_policy_reason_dimension_is_bound_to_decision_digest(tmp_path: Path, field: str, value: str) -> None:
    store = NoActionCentralStore(tmp_path / "central.db", ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="dell-installation",
    )
    now = datetime(2026, 8, 30, 1, 0, tzinfo=UTC)
    projection = _projection(1, now)
    store.ingest_monitoring_projection(projection)
    authorization = RecoveryAuthorizationInput(
        authorization_id="authorization-1",
        incident_id="incident-1",
        source_episode_id="episode-1",
        target_id="stream-target",
        action="restart_ffmpeg",
        reason_code="confirmed_tcp_stall",
        policy_revision="policy-a",
        observation_revision="revision-1",
        expected_target=TargetIdentity("dell", "boot-a", "stream-v3", "pod-a", "stream-engine", "containerd://a", "generation-a", 4100),
        blockers=("CRA_OPERATING_MODE_NO_ACTION",),
        authorized_at=isoformat_utc(now + timedelta(seconds=1)),
        expires_at=isoformat_utc(now + timedelta(seconds=31)),
    )
    original = {
        "candidate_reason_code": "confirmed_tcp_stall",
        "decision_reason_code": "CRA_OPERATING_MODE_NO_ACTION",
    }
    store.persist_policy_decision(
        projection=projection,
        authorization=authorization,
        decision_id="decision-reason-binding",
        decision="BLOCKED",
        blockers=authorization.blockers,
        **original,
    )
    changed = {**original, field: value}
    try:
        with pytest.raises(Exception, match="CRA_POLICY_DECISION_CONFLICT"):
            store.persist_policy_decision(
                projection=projection,
                authorization=authorization,
                decision_id="decision-reason-binding",
                decision="BLOCKED",
                blockers=authorization.blockers,
                **changed,
            )
        assert store.read_one("SELECT count(*) FROM cra_policy_decisions")[0] == 1
    finally:
        store.close()


def test_signed_archive_compaction_keeps_hot_tail_and_preserves_verifiable_chain(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.generate()
    store = NoActionCentralStore(tmp_path / "central.db", ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="dell-installation",
    )
    now = datetime(2026, 8, 30, 1, 0, tzinfo=UTC)
    target = TargetIdentity(
        "dell",
        "boot-a",
        "stream-v3",
        "pod-a",
        "stream-engine",
        "containerd://a",
        "generation-a",
        4100,
    )
    for index in range(1, 6):
        projection = _projection(index, now + timedelta(seconds=index))
        assert store.ingest_monitoring_projection(projection) == "ACCEPTED"
        authorization = RecoveryAuthorizationInput(
            authorization_id=f"authorization-{index}",
            incident_id=f"incident-{index}",
            source_episode_id=f"episode-{index}",
            target_id="stream-target",
            action="restart_ffmpeg",
            reason_code="confirmed_tcp_stall",
            policy_revision="policy-a",
            observation_revision=f"revision-{index}",
            expected_target=target,
            blockers=("CRA_OPERATING_MODE_NO_ACTION",),
            authorized_at=isoformat_utc(now + timedelta(seconds=index)),
            expires_at=isoformat_utc(now + timedelta(minutes=1, seconds=index)),
        )
        store.persist_policy_decision(
            projection=projection,
            authorization=authorization,
            decision_id=f"decision-{index}",
            decision="NO_ACTION",
            blockers=("CRA_OPERATING_MODE_NO_ACTION",),
        )

    archive = SignedNoActionArchive(tmp_path / "archive.db", key_id="archive-key-a", private_key=private)
    try:
        assert store.compact_no_action_evidence(archive, retain_decision_count=2) == 3
        hot_decisions = store.read_one("SELECT count(*) FROM cra_policy_decisions")
        hot_projections = store.read_one("SELECT count(*) FROM monitoring_evidence_projections")
        assert hot_decisions is not None and hot_decisions[0] == 2
        assert hot_projections is not None and hot_projections[0] == 2
        rows = archive.connection.execute("SELECT * FROM archive_records ORDER BY rowid").fetchall()
        assert len(rows) == 3
        assert [row["record_id"] for row in rows] == [
            "no-action-decision:decision-1",
            "no-action-decision:decision-2",
            "no-action-decision:decision-3",
        ]
        previous = "0" * 64
        for row in rows:
            assert row["previous_digest"] == previous
            private.public_key().verify(base64.b64decode(row["signature"]), bytes.fromhex(row["record_digest"]))
            previous = str(row["record_digest"])
        assert archive.checkpoint_completed_days(now=datetime(2026, 8, 31, tzinfo=UTC)) in {0, 1}
        checkpoint = archive.connection.execute("SELECT * FROM daily_checkpoints").fetchone()
        assert checkpoint is not None
        private.public_key().verify(base64.b64decode(checkpoint["signature"]), bytes.fromhex(checkpoint["checkpoint_digest"]))

        # Crash-safe replay sees already archived records and does not duplicate them.
        assert store.compact_no_action_evidence(archive, retain_decision_count=2) == 0
        assert archive.status()["archive_record_count"] == 3
    finally:
        archive.close()
        store.close()


def test_compaction_preserves_source_high_water_when_wall_clock_moves_back(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.generate()
    store = NoActionCentralStore(tmp_path / "central.db", ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="dell-installation",
    )
    target = TargetIdentity("dell", "boot-a", "stream-v3", "pod-a", "stream-engine", "containerd://a", "generation-a", 4100)
    now = datetime(2026, 8, 30, 1, 0, tzinfo=UTC)
    for index in range(1, 6):
        projection = _projection(index, now)
        store.ingest_monitoring_projection(projection)
        authorization = RecoveryAuthorizationInput(
            authorization_id=f"clock-auth-{index}",
            incident_id=f"incident-{index}",
            source_episode_id=f"episode-{index}",
            target_id="stream-target",
            action="restart_ffmpeg",
            reason_code="confirmed_tcp_stall",
            policy_revision="policy-a",
            observation_revision=f"revision-{index}",
            expected_target=target,
            blockers=("CRA_OPERATING_MODE_NO_ACTION",),
            authorized_at=isoformat_utc(now),
            expires_at=isoformat_utc(now + timedelta(seconds=30)),
        )
        store.persist_policy_decision(
            projection=projection,
            authorization=authorization,
            decision_id=f"clock-decision-{index}",
            decision="NO_ACTION",
            blockers=authorization.blockers,
        )
        # Simulate a backward wall clock: newest sequence receives oldest wall timestamp.
        with store._NoActionCentralStore__ledger.write() as database:  # type: ignore[attr-defined]
            database.execute(
                "UPDATE cra_policy_decisions SET decided_at=? WHERE decision_id=?",
                (isoformat_utc(now - timedelta(minutes=index)), f"clock-decision-{index}"),
            )
    archive = SignedNoActionArchive(tmp_path / "archive.db", key_id="archive-key-a", private_key=private)
    try:
        assert store.compact_no_action_evidence(archive, retain_decision_count=2) == 2
        high = store.read_one("SELECT max(observation_sequence) FROM monitoring_evidence_projections")
        current = store.read_one("SELECT count(*) FROM cra_policy_decisions WHERE projection_id='projection-5'")
        assert high is not None and high[0] == 5
        assert current is not None and current[0] == 1
        with pytest.raises(Exception, match="MONITORING_PROJECTION_SEQUENCE_REGRESSION"):
            store.ingest_monitoring_projection(_projection(4, now))
    finally:
        archive.close()
        store.close()


def test_archive_private_key_is_never_stored_in_archive_database(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.generate()
    pem = private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    key_path = tmp_path / "archive-private.pem"
    key_path.write_bytes(pem)
    os.chmod(key_path, 0o600)
    archive = SignedNoActionArchive(tmp_path / "archive.db", key_id="archive-key-a", private_key=private)
    archive.append(
        record_id="record-a",
        record_type="TEST",
        source_created_at="2026-08-30T00:00:00Z",
        payload={"safe": True},
    )
    archive.close()
    assert pem not in (tmp_path / "archive.db").read_bytes()
    connection = sqlite3.connect(tmp_path / "archive.db")
    try:
        assert json.loads(connection.execute("SELECT payload_json FROM archive_records").fetchone()[0]) == {"safe": True}
    finally:
        connection.close()


def test_compaction_reports_only_deleted_rows_when_verifier_pins_projection(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.generate()
    store = NoActionCentralStore(tmp_path / "central.db", ROOT / "migrations/central/001_initial.sql")
    store.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="dell-installation",
    )
    now = datetime(2026, 8, 30, 2, 0, tzinfo=UTC)
    target = TargetIdentity("dell", "boot-a", "stream-v3", "pod-a", "stream-engine", "containerd://a", "generation-a", 4100)
    for index in range(1, 6):
        projection = _projection(index, now + timedelta(seconds=index))
        store.ingest_monitoring_projection(projection)
        authorization = RecoveryAuthorizationInput(
            authorization_id=f"authorization-pinned-{index}",
            incident_id=f"incident-{index}",
            source_episode_id=f"episode-{index}",
            target_id="stream-target",
            action="restart_ffmpeg",
            reason_code="confirmed_tcp_stall",
            policy_revision="policy-a",
            observation_revision=f"revision-{index}",
            expected_target=target,
            blockers=("CRA_OPERATING_MODE_NO_ACTION",),
            authorized_at=isoformat_utc(now + timedelta(seconds=index)),
            expires_at=isoformat_utc(now + timedelta(minutes=1, seconds=index)),
        )
        store.persist_policy_decision(
            projection=projection,
            authorization=authorization,
            decision_id=f"decision-{index}",
            decision="NO_ACTION",
            blockers=("CRA_OPERATING_MODE_NO_ACTION",),
        )
    # Retention may archive this decision, but must keep its projection hot
    # while a final-verification record references it.
    connection = sqlite3.connect(tmp_path / "central.db")
    try:
        connection.execute(
            "INSERT INTO cra_verifier_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "verifier-pins-oldest",
                None,
                "local-action-pins-oldest",
                "scope-pins-oldest",
                "projection-1",
                "projection-5",
                "UNKNOWN",
                "[]",
                "{}",
                isoformat_utc(now + timedelta(minutes=2)),
                "digest-pins-oldest",
            ),
        )
        connection.commit()
    finally:
        connection.close()
    archive = SignedNoActionArchive(tmp_path / "archive.db", key_id="archive-key-a", private_key=private)
    try:
        assert store.compact_no_action_evidence(archive, retain_decision_count=2) == 2
        hot_decisions = store.read_one("SELECT count(*) FROM cra_policy_decisions")
        hot_projections = store.read_one("SELECT count(*) FROM monitoring_evidence_projections")
        assert hot_decisions is not None and hot_decisions[0] == 3
        assert hot_projections is not None and hot_projections[0] == 3
        assert archive.status()["archive_record_count"] == 3
        assert store.compact_no_action_evidence(archive, retain_decision_count=2) == 0
    finally:
        archive.close()
        store.close()
