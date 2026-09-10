from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.storage import CentralStore
from cra_dell_recovery.canonical import KeyRing, SignedMessageCodec, Signer
from cra_dell_recovery.models import RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.schema import SchemaRegistry
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.execution import AgentService, FakePhysicalAdapter, FakeTargetObserver
from dell_recovery_agent.storage import DellStore


@dataclass
class HarnessEnvironment:
    root: Path
    central: CentralStore
    dell: DellStore
    central_codec: SignedMessageCodec
    agent_codec: SignedMessageCodec
    target: TargetIdentity
    adapter: FakePhysicalAdapter

    @classmethod
    def create(cls, root: Path, project_root: Path) -> HarnessEnvironment:
        registry = SchemaRegistry(project_root / "contracts/cra_dell_recovery/v1")
        central_private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
        agent_private = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
        central_codec = SignedMessageCodec(
            registry,
            Signer("cra-harness-key", central_private),
            KeyRing({"agent-harness-key": agent_private.public_key()}),
        )
        agent_codec = SignedMessageCodec(
            registry,
            Signer("agent-harness-key", agent_private),
            KeyRing({"cra-harness-key": central_private.public_key()}),
        )
        central = CentralStore(
            root / "central/central.db",
            project_root / "migrations/central/001_initial.sql",
            allow_legacy_test_verification=True,
        )
        central.bootstrap(
            target_id="stream-target",
            host_id="dell",
            agent_id="dell-agent",
            controller_instance_id="cra-controller",
            agent_installation_id="agent-installation-1",
        )
        dell = DellStore(root / "dell/dell.db", project_root / "migrations/dell/001_initial.sql")
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
        dell.connection.execute(
            """UPDATE authority_fences SET authority_state='CENTRAL_ACTIVE',action_ready=1,
               state_reason='VALID_HARNESS_HEARTBEAT'"""
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
        return cls(root, central, dell, central_codec, agent_codec, target, FakePhysicalAdapter())

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

    def close(self) -> None:
        self.central.close()
        self.dell.close()
