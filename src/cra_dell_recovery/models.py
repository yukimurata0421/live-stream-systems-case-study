from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class TargetIdentity:
    host_id: str
    host_boot_id: str
    namespace: str
    pod_uid: str
    container_name: str
    container_id: str
    ffmpeg_generation: str
    ffmpeg_pid: int

    def __post_init__(self) -> None:
        for name in (
            "host_id",
            "host_boot_id",
            "namespace",
            "pod_uid",
            "container_name",
            "container_id",
            "ffmpeg_generation",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        if self.ffmpeg_pid <= 1:
            raise ValueError("ffmpeg_pid must be greater than 1")
        if self.container_name != "stream-engine":
            raise ValueError("container_name must be stream-engine")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TargetIdentity:
        return cls(
            host_id=str(value.get("host_id") or ""),
            host_boot_id=str(value.get("host_boot_id") or ""),
            namespace=str(value.get("namespace") or ""),
            pod_uid=str(value.get("pod_uid") or ""),
            container_name=str(value.get("container_name") or ""),
            container_id=str(value.get("container_id") or ""),
            ffmpeg_generation=str(value.get("ffmpeg_generation") or ""),
            ffmpeg_pid=int(value.get("ffmpeg_pid") or 0),
        )


def is_expected_ffmpeg_successor(before: TargetIdentity, after: TargetIdentity) -> bool:
    """Return true only for an FFmpeg successor inside the exact same runtime scope."""

    return (
        before.host_id == after.host_id
        and before.host_boot_id == after.host_boot_id
        and before.namespace == after.namespace
        and before.pod_uid == after.pod_uid
        and before.container_name == after.container_name
        and before.container_id == after.container_id
        and before.ffmpeg_generation != after.ffmpeg_generation
        and before.ffmpeg_pid != after.ffmpeg_pid
    )


@dataclass(frozen=True)
class RecoveryAuthorizationInput:
    authorization_id: str
    incident_id: str
    source_episode_id: str
    target_id: str
    action: str
    reason_code: str
    policy_revision: str
    observation_revision: str
    expected_target: TargetIdentity
    blockers: tuple[str, ...]
    authorized_at: str
    expires_at: str

    @property
    def executable(self) -> bool:
        return self.action == "restart_ffmpeg" and self.reason_code == "confirmed_tcp_stall" and not self.blockers


@dataclass(frozen=True)
class MonitoringReadiness:
    input_fresh: bool
    decision_service_ready: bool
    observed_at: str
    reason: str = ""

    @property
    def authority_ready(self) -> bool:
        return self.input_fresh and self.decision_service_ready


@dataclass(frozen=True)
class RecoveryVerificationInput:
    verification_id: str
    command_id: str
    monitoring_cycle_id: str
    verdict: str
    observed_target: TargetIdentity
    checks: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    observed_at: str


@dataclass(frozen=True)
class LocalRecoveryCandidate:
    local_action_id: str
    target_id: str
    action: str
    reason_code: str
    target_identity: TargetIdentity
    evidence: dict[str, Any]
    observed_at: str
