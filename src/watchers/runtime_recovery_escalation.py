from __future__ import annotations

import argparse
import os
import signal
import subprocess
import threading
import uuid
from pathlib import Path
from typing import Any

from runtime_boundary import (
    ESCALATE_RUNTIME_RECOVERY,
    EffectExecutorServer,
    EffectLedger,
    EffectRejected,
    RuntimeSnapshotReader,
)
from runtime_boundary.observation import atomic_write

from maintenance_audit import audit_maintenance_decision


def utc_text() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class RuntimeRecoveryEscalationAdapter:
    """Preserve the legacy runtime-wide recovery scope outside the FFmpeg executor."""

    def __init__(self) -> None:
        if os.environ.get("MAINTENANCE_ENFORCEMENT_ENABLED", "0").lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("P2_ENFORCEMENT_NOT_AUTHORIZED")
        self.snapshot_path = Path(os.environ["FR_TARGET_SNAPSHOT_FILE"])
        self.observation_path = Path(
            os.environ.get(
                "FR_RUNTIME_RECOVERY_OBSERVATION_FILE",
                "/run/stream-v3-control/runtime-recovery-observation.json",
            )
        )
        self.instance_id = f"runtime-recovery-adapter-{uuid.uuid4()}"
        self.stop_event = threading.Event()
        self.sequence = 0
        self.ledger = EffectLedger(
            Path(os.environ.get("FR_EFFECT_LEDGER_FILE", "/state/runtime/fast_recovery_effects.sqlite3")),
            initial_producer_id=os.environ.get("FR_EFFECT_INITIAL_PRODUCER_ID", "legacy-in-pod"),
            initial_producer_generation=max(
                1, int(os.environ.get("FR_EFFECT_INITIAL_PRODUCER_GENERATION", "1") or 1)
            ),
            allow_initialize=False,
        )
        self.namespace = os.environ.get("STREAM_K8S_NAMESPACE", "stream-v3")
        self.kubectl_bin = os.environ.get("STREAM_KUBECTL_BIN", "kubectl")
        self.runtime_target = os.environ.get("FR_RUNTIME_RECOVERY_TARGET", "deployment/stream-v3-runtime")
        if self.namespace != "stream-v3" or self.runtime_target != "deployment/stream-v3-runtime":
            raise RuntimeError("RUNTIME_RECOVERY_TARGET_NOT_EXACT")
        controller_uid = max(1, int(os.environ.get("FR_EFFECT_CONTROLLER_UID", "992") or 992))
        socket_gid = max(1, int(os.environ.get("FR_EFFECT_SOCKET_GID", "983") or 983))
        self.server = EffectExecutorServer(
            socket_path=Path(
                os.environ.get("FR_RUNTIME_RECOVERY_SOCKET", "/run/stream-v3-control/runtime-recovery.sock")
            ),
            ledger=self.ledger,
            allowed_peer_uids={0, controller_uid},
            current_target=self.current_target,
            perform_effect=self.perform_effect,
            before_effect=self.audit_effect_boundary,
            allowed_intents={ESCALATE_RUNTIME_RECOVERY},
            socket_gid=socket_gid,
        )

    def current_target(self) -> dict[str, object]:
        decision = RuntimeSnapshotReader(self.snapshot_path).read()
        if not decision.available or decision.runtime_identity is None:
            return {}
        expected_pod_uid = os.environ.get("STREAM_V3_POD_UID", "")
        if expected_pod_uid and decision.runtime_identity["pod_uid"] != expected_pod_uid:
            return {}
        return {
            "runtime_identity": decision.runtime_identity,
            "runtime_snapshot_id": decision.snapshot_id,
            "executor_instance_id": self.instance_id,
        }

    def audit_effect_boundary(self, request: Any) -> None:
        audit_maintenance_decision(
            path_id="MP-03",
            phase="EFFECT_BOUNDARY",
            operation="escalate_runtime_recovery",
            path_role="NORMAL_MUTATOR",
            process_service="legacy-runtime-recovery-adapter",
            resource_identity=f"runtime/pod/{request.runtime_identity['pod_uid']}",
            correlation_id=request.correlation_id,
            in_flight_evidence={
                "status": "CONFIRMED",
                "count": self.ledger.unresolved_count(),
                "source": "shared durable runtime Effect Ledger",
            },
            generation_evidence={
                "status": "CONFIRMED",
                "producer_id": request.producer_id,
                "producer_generation": request.producer_generation,
                "runtime_generation": request.runtime_identity["runtime_generation"],
            },
            p2_disabled_evaluation=True,
            native_operation_id=request.request_id,
            native_operation_generation=request.runtime_identity["runtime_generation"],
            actual_production_decision="LEGACY_RUNTIME_RECOVERY_EFFECT_CALL_PROCEEDS",
        )

    def perform_effect(self, request: Any) -> dict[str, object]:
        if request.intent_type != ESCALATE_RUNTIME_RECOVERY:
            raise EffectRejected("INTENT_NOT_OWNED_BY_ESCALATION_ADAPTER")
        completed = subprocess.run(
            [self.kubectl_bin, "-n", self.namespace, "rollout", "restart", self.runtime_target],
            check=False,
            capture_output=True,
            text=True,
            timeout=10.0,
        )
        if completed.returncode != 0:
            raise EffectRejected("RUNTIME_RECOVERY_DISPATCH_FAILED")
        return {
            "physical_effect_count": 1,
            "effect": "KUBERNETES_ROLLOUT_RESTART",
            "target": self.runtime_target,
        }

    def publish_observation(self) -> dict[str, object]:
        self.sequence += 1
        current = self.current_target()
        authority = self.ledger.authority()
        observation = {
            "schema_version": "runtime.ffmpeg_observation.v1",
            "observation_id": f"runtime-recovery-observation-{uuid.uuid4()}",
            "observed_at": utc_text(),
            "producer_id": "legacy-runtime-recovery-adapter",
            "executor_instance_id": self.instance_id,
            "producer_instance_id": self.instance_id,
            "sequence": self.sequence,
            "runtime_identity": current.get("runtime_identity"),
            "runtime_snapshot_id": current.get("runtime_snapshot_id", ""),
            "runtime_snapshot_status": "VALID" if current else "UNKNOWN",
            "active_producer_id": str(authority.get("producer_id") or ""),
            "active_producer_generation": int(authority.get("producer_generation") or 0),
            "authority_version": int(authority.get("authority_version") or 0),
            "in_flight_count": self.ledger.unresolved_count(),
            "maintenance_evidence_status": "UNKNOWN",
            "projection_id": "",
            "projection_sequence": 0,
            "production_behavior_modified": False,
        }
        atomic_write(self.observation_path, observation)
        return observation

    def run(self) -> int:
        self.server.start()
        try:
            while not self.stop_event.is_set():
                self.publish_observation()
                self.stop_event.wait(1.0)
            return 0
        finally:
            self.server.close()
            self.ledger.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="typed legacy runtime-recovery escalation adapter")
    parser.parse_args()
    adapter = RuntimeRecoveryEscalationAdapter()
    signal.signal(signal.SIGTERM, lambda *_args: adapter.stop_event.set())
    signal.signal(signal.SIGINT, lambda *_args: adapter.stop_event.set())
    return adapter.run()


if __name__ == "__main__":
    raise SystemExit(main())
