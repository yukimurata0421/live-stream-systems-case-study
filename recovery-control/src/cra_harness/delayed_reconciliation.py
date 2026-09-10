from __future__ import annotations

import os
import random
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger, OutcomeUnknown

MUTATIONS = (
    "VALID",
    "OWNER_DIGEST_TAMPER",
    "ZERO_TRANSPORT_BYTES",
    "POD_IDENTITY_DRIFT",
    "SAME_FFMPEG_PID",
    "STALE_EVIDENCE",
    "EXTRA_EVIDENCE_FIELD",
)


def _target(token: str, *, pid: int, generation: str) -> dict[str, object]:
    return {
        "host_id": "dell-harness",
        "host_boot_id": f"boot-{token}",
        "namespace": "stream-v3",
        "pod_uid": f"pod-{token}",
        "container_name": "stream-engine",
        "container_id": f"containerd://{token}",
        "ffmpeg_generation": generation,
        "ffmpeg_pid": pid,
    }


def _request(token: str, target: dict[str, object]) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": f"request-{token}",
        "producer_id": "harness-controller",
        "producer_generation": 1,
        "operation": "restart_ffmpeg",
        "reason": "isolated delayed-exit harness",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "target_identity": target,
        "expected_ffmpeg_generation": f"native-{token}",
        "idempotency_key": f"request-{token}",
        "correlation_id": f"request-{token}",
        "target_snapshot_id": f"snapshot-{token}",
        "runtime_observation_id": f"observation-before-{token}",
        "expected_executor_instance_id": f"executor-{token}",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": f"projection-{token}",
        "projection_sequence": 1,
    }


def _evidence(before: dict[str, object], after: dict[str, object], token: str) -> dict[str, object]:
    return {
        "schema_version": "runtime.delayed_exit_reconciliation_evidence.v1",
        "oracle": "DELAYED_FFMPEG_EXIT_AND_HEALTHY_SUCCESSOR",
        "observed_at": datetime.now(UTC).isoformat(),
        "physical_effect_count": 1,
        "automatic_retry_count": 0,
        "before_target": dict(before),
        "observed_target": dict(after),
        "runtime_observation_id": f"observation-after-{token}",
        "transport": {"bytes_sent": 4096, "network_down": False, "tcp_probe_ok": True},
    }


def run_delayed_reconciliation_campaign(*, case_count: int = 192, seed: int = 20260901) -> dict[str, Any]:
    if isinstance(case_count, bool) or not 7 <= case_count <= 4096:
        raise ValueError("DELAYED_RECONCILIATION_CASE_COUNT_INVALID")
    rng = random.Random(seed)
    scheduled = list(MUTATIONS)
    scheduled.extend(rng.choice(MUTATIONS) for _ in range(case_count - len(scheduled)))
    rng.shuffle(scheduled)
    failures: list[dict[str, str | int]] = []
    detected = {name: 0 for name in MUTATIONS if name != "VALID"}
    valid_count = 0
    replay_count = 0
    with tempfile.TemporaryDirectory(prefix="cra-delayed-reconcile-") as temporary:
        root = Path(temporary)
        for index, mutation in enumerate(scheduled):
            token = f"{seed}-{index}"
            case_root = root / str(index)
            case_root.mkdir()
            before = _target(token, pid=4100, generation=f"generation-before-{token}")
            successor = _target(token, pid=4200, generation=f"generation-after-{token}")
            current: dict[str, object] = {
                "target_identity": before,
                "ffmpeg_generation": f"native-{token}",
                "executor_instance_id": f"executor-{token}",
            }

            def current_target(current_state: dict[str, object] = current) -> dict[str, object]:
                return current_state

            ledger = EffectLedger(
                case_root / "ledger.sqlite3",
                initial_producer_id="harness-controller",
                initial_producer_generation=1,
            )
            server = EffectExecutorServer(
                socket_path=case_root / "effect.sock",
                ledger=ledger,
                allowed_peer_uids={os.getuid()},
                current_target=current_target,
                perform_effect=lambda _item: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")),
            )
            server.start()
            client = EffectClient(case_root / "effect.sock")
            try:
                original = client.execute(_request(token, before))
                query = client.unresolved()
                scope = query.get("unresolved_scopes", [{}])[0]
                current["target_identity"] = successor
                value: dict[str, Any] = {
                    "schema_version": "runtime.effect_reconciliation_request.v1",
                    "reconciliation_id": f"reconcile-{token}",
                    "effect_scope_id": scope.get("effect_scope_id"),
                    "owner_request_id": scope.get("owner_request_id"),
                    "owner_request_digest": scope.get("owner_request_digest"),
                    "resolution": "EFFECT_OBSERVED",
                    "evidence": _evidence(before, successor, token),
                }
                if mutation == "OWNER_DIGEST_TAMPER":
                    value["owner_request_digest"] = "0" * 64
                elif mutation == "ZERO_TRANSPORT_BYTES":
                    value["evidence"]["transport"]["bytes_sent"] = 0
                elif mutation == "POD_IDENTITY_DRIFT":
                    value["evidence"]["observed_target"]["pod_uid"] = "pod-drift"
                elif mutation == "SAME_FFMPEG_PID":
                    same_pid = {**successor, "ffmpeg_pid": before["ffmpeg_pid"]}
                    current["target_identity"] = same_pid
                    value["evidence"]["observed_target"] = same_pid
                elif mutation == "STALE_EVIDENCE":
                    value["evidence"]["observed_at"] = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
                elif mutation == "EXTRA_EVIDENCE_FIELD":
                    value["evidence"]["unexpected"] = True
                response = client.reconcile(value)
                reconciliation_rows = int(ledger.connection.execute("SELECT count(*) FROM effect_reconciliations").fetchone()[0])
                raw = ledger.request(f"request-{token}")
                invariant = (
                    original.get("state") == "OUTCOME_UNKNOWN"
                    and query.get("unresolved_count") == 1
                    and raw is not None
                    and raw["state"] == "OUTCOME_UNKNOWN"
                )
                if mutation == "VALID":
                    exact_replay = client.reconcile(value)
                    passed = (
                        invariant
                        and response.get("ok") is True
                        and ledger.unresolved_count() == 0
                        and reconciliation_rows == 1
                        and exact_replay.get("replay") is True
                        and int(ledger.connection.execute("SELECT count(*) FROM effect_reconciliations").fetchone()[0]) == 1
                    )
                    valid_count += 1
                    replay_count += int(exact_replay.get("replay") is True)
                else:
                    passed = invariant and response.get("ok") is False and ledger.unresolved_count() == 1 and reconciliation_rows == 0
                    detected[mutation] += int(passed)
                if not passed:
                    failures.append(
                        {
                            "index": index,
                            "mutation": mutation,
                            "response_reason": str(response.get("reason")),
                        }
                    )
            finally:
                server.close()
                ledger.close()
    return {
        "schema": "cra.delayed_reconciliation_chaos_report.v1",
        "seed": seed,
        "case_count": case_count,
        "valid_case_count": valid_count,
        "idempotent_replay_count": replay_count,
        "negative_control_detected": detected,
        "failure_count": len(failures),
        "failures": failures[:20],
        "physical_effect_adapter_success_count": 0,
        "production_target_touched": False,
        "pass": not failures and all(count > 0 for count in detected.values()),
    }
