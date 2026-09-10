from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.storage import CentralStore
from cra_dell_recovery.canonical import KeyRing, SignedMessageCodec, Signer
from cra_dell_recovery.models import RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.schema import SchemaRegistry
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.controls.sqlite_runtime import (
    fixed_sqlite_failure_message,
    inspect_fixed_sqlite_runtime,
    is_full_regression_selection,
)
from dell_recovery_agent.execution import AgentService, FakePhysicalAdapter, FakeTargetObserver
from dell_recovery_agent.storage import DellStore

ROOT = Path(__file__).resolve().parents[1]


def pytest_sessionstart(session: pytest.Session) -> None:
    if not is_full_regression_selection(ROOT, session.config.args):
        return
    identity = inspect_fixed_sqlite_runtime(ROOT)
    if not identity.passed:
        raise pytest.UsageError(fixed_sqlite_failure_message(identity))


@dataclass
class TestEnvironment:
    central: CentralStore
    dell: DellStore
    central_codec: SignedMessageCodec
    agent_codec: SignedMessageCodec
    target: TargetIdentity
    adapter: FakePhysicalAdapter

    def authorization(
        self,
        suffix: str = "1",
        *,
        blockers: tuple[str, ...] = (),
        target: TargetIdentity | None = None,
    ) -> RecoveryAuthorizationInput:
        now = utc_now()
        return RecoveryAuthorizationInput(
            authorization_id=f"auth-{suffix}",
            incident_id=f"incident-{suffix}",
            source_episode_id=f"episode-{suffix}",
            target_id="stream-target",
            action="restart_ffmpeg",
            reason_code="confirmed_tcp_stall",
            policy_revision="shadow-policy-v1",
            observation_revision=f"observation-{suffix}",
            expected_target=target or self.target,
            blockers=blockers,
            authorized_at=isoformat_utc(now),
            expires_at=isoformat_utc(now + timedelta(minutes=2)),
        )

    def command(self, suffix: str = "1", *, target: TargetIdentity | None = None) -> dict[str, object]:
        authorization = self.authorization(suffix, target=target)
        self.central.add_authorization(authorization)
        return self.central.create_command(
            authorization.authorization_id,
            self.central_codec,
            command_id=f"command-{suffix}",
        )

    def service(self, *observations: TargetIdentity | None) -> AgentService:
        return AgentService(
            self.dell,
            self.agent_codec,
            FakeTargetObserver(*(observations or (self.target, self.target))),
            self.adapter,
        )


@pytest.fixture
def registry() -> SchemaRegistry:
    return SchemaRegistry(ROOT / "contracts/cra_dell_recovery/v1")


@pytest.fixture
def codecs(registry: SchemaRegistry) -> tuple[SignedMessageCodec, SignedMessageCodec]:
    central_private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    agent_private = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    central = SignedMessageCodec(
        registry,
        Signer("cra-test-key", central_private),
        KeyRing({"agent-test-key": agent_private.public_key()}),
    )
    agent = SignedMessageCodec(
        registry,
        Signer("agent-test-key", agent_private),
        KeyRing({"cra-test-key": central_private.public_key()}),
    )
    return central, agent


@pytest.fixture
def environment(tmp_path: Path, codecs: tuple[SignedMessageCodec, SignedMessageCodec]) -> Iterator[TestEnvironment]:
    central = CentralStore(tmp_path / "central/central.db", ROOT / "migrations/central/001_initial.sql")
    central.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="agent-installation-1",
    )
    dell = DellStore(tmp_path / "dell/dell.db", ROOT / "migrations/dell/001_initial.sql")
    dell.bootstrap(
        agent_id="dell-agent",
        installation_id="agent-installation-1",
        host_id="dell",
        host_boot_id="boot-a",
        target_id="stream-target",
    )
    dell.install_reconciliation(
        target_id="stream-target",
        reconciliation_id="initial-reconciliation",
        challenge_id="initial-challenge",
        nonce="initial-nonce-value-with-at-least-32-characters",
        new_epoch=1,
        session_id="session-1",
        controller_instance_id="cra-controller",
    )
    # The fixture represents a completed reconciliation followed by one valid authority heartbeat.
    dell.connection.execute(
        """UPDATE authority_fences SET authority_state='CENTRAL_ACTIVE',action_ready=1,
           state_reason='VALID_TEST_HEARTBEAT'"""
    )
    dell.set_process_lease_valid("stream-target", True)
    target = TargetIdentity(
        host_id="dell",
        host_boot_id="boot-a",
        namespace="default",
        pod_uid="pod-a",
        container_name="stream-engine",
        container_id="container-a",
        ffmpeg_generation="run-a:0:4100",
        ffmpeg_pid=4100,
    )
    environment = TestEnvironment(
        central=central,
        dell=dell,
        central_codec=codecs[0],
        agent_codec=codecs[1],
        target=target,
        adapter=FakePhysicalAdapter(),
    )
    try:
        yield environment
    finally:
        dell.close()
        central.close()
