from __future__ import annotations

import argparse
import json
import os
import random
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger, EffectRequest, OutcomeUnknown

MUTATIONS = (
    "VALID",
    "SAME_LIFECYCLE",
    "HOST_DOMAIN_MISMATCH",
    "NAMESPACE_DOMAIN_MISMATCH",
    "CONTAINER_NAME_DOMAIN_MISMATCH",
    "CURRENT_TARGET_MISMATCH",
    "STALE_EVIDENCE",
    "OWNER_DIGEST_TAMPER",
    "ZERO_TRANSPORT_BYTES",
    "PHYSICAL_OUTCOME_FORGED",
    "EXTRA_EVIDENCE_FIELD",
)


def _target(token: str, *, pod: str, container: str, generation: str, pid: int) -> dict[str, object]:
    return {
        "host_id": "dell-harness",
        "host_boot_id": f"boot-{token}",
        "namespace": "stream-v3",
        "pod_uid": pod,
        "container_name": "stream-engine",
        "container_id": container,
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
        "reason": "retired-target chaos",
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


def _evidence(before: dict[str, object], observed: dict[str, object], token: str) -> dict[str, object]:
    return {
        "schema_version": "runtime.retired_target_reconciliation_evidence.v1",
        "oracle": "EXACT_TARGET_RETIRED_AND_HEALTHY_REPLACEMENT",
        "observed_at": datetime.now(UTC).isoformat(),
        "physical_attempt_count": 1,
        "physical_effect_outcome": "UNKNOWN",
        "automatic_retry_count": 0,
        "before_target": before,
        "observed_target": observed,
        "runtime_observation_id": f"observation-after-{token}",
        "transport": {"bytes_sent": 4096, "network_down": False, "tcp_probe_ok": True},
    }


def run_target_retirement_chaos(*, case_count: int = 512, seed: int = 20260901) -> dict[str, Any]:
    if isinstance(case_count, bool) or not isinstance(case_count, int) or not len(MUTATIONS) <= case_count <= 4096:
        raise ValueError("TARGET_RETIREMENT_CASE_COUNT_INVALID")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("TARGET_RETIREMENT_SEED_INVALID")
    rng = random.Random(seed)
    scheduled = list(MUTATIONS)
    scheduled.extend(rng.choice(MUTATIONS) for _ in range(case_count - len(scheduled)))
    rng.shuffle(scheduled)
    failures: list[dict[str, str | int]] = []
    detected = {name: 0 for name in MUTATIONS if name != "VALID"}
    valid_count = 0
    replay_count = 0
    with tempfile.TemporaryDirectory(prefix="cra-target-retirement-") as temporary:
        root = Path(temporary)
        for index, mutation in enumerate(scheduled):
            token = f"{seed}-{index}"
            case_root = root / str(index)
            case_root.mkdir()
            before = _target(
                token,
                pod=f"pod-before-{token}",
                container=f"containerd://before-{token}",
                generation=f"generation-before-{token}",
                pid=4100,
            )
            replacement = _target(
                token,
                pod=f"pod-after-{token}",
                container=f"containerd://after-{token}",
                generation=f"generation-after-{token}",
                pid=4200,
            )
            current: dict[str, object] = {
                "target_identity": replacement,
                "ffmpeg_generation": f"native-after-{token}",
                "executor_instance_id": f"executor-{token}",
            }
            physical_adapter_calls = 0

            def perform(_request: EffectRequest) -> dict[str, object]:
                nonlocal physical_adapter_calls
                physical_adapter_calls += 1
                raise OutcomeUnknown("delayed exit")

            def current_target(current: dict[str, object] = current) -> dict[str, object]:
                return current

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
                perform_effect=perform,
            )
            current["target_identity"] = before
            current["ffmpeg_generation"] = f"native-{token}"
            server.start()
            client = EffectClient(case_root / "effect.sock")
            try:
                original = client.execute(_request(token, before))
                unresolved = client.unresolved()["unresolved_scopes"][0]
                current["target_identity"] = replacement
                current["ffmpeg_generation"] = f"native-after-{token}"
                observed = dict(replacement)
                value: dict[str, Any] = {
                    "schema_version": "runtime.effect_reconciliation_request.v1",
                    "reconciliation_id": f"retire-{token}",
                    "effect_scope_id": unresolved["effect_scope_id"],
                    "owner_request_id": unresolved["owner_request_id"],
                    "owner_request_digest": unresolved["owner_request_digest"],
                    "resolution": "TARGET_RETIRED",
                    "evidence": _evidence(before, observed, token),
                }
                if mutation == "SAME_LIFECYCLE":
                    same = {**before, "ffmpeg_generation": f"generation-successor-{token}", "ffmpeg_pid": 4300}
                    current["target_identity"] = same
                    value["evidence"]["observed_target"] = same
                elif mutation == "HOST_DOMAIN_MISMATCH":
                    observed["host_id"] = "other-host"
                    current["target_identity"] = observed
                elif mutation == "NAMESPACE_DOMAIN_MISMATCH":
                    observed["namespace"] = "other-namespace"
                    current["target_identity"] = observed
                elif mutation == "CONTAINER_NAME_DOMAIN_MISMATCH":
                    observed["container_name"] = "other-container"
                    current["target_identity"] = observed
                elif mutation == "CURRENT_TARGET_MISMATCH":
                    value["evidence"]["observed_target"] = {**observed, "pod_uid": "forged-pod"}
                elif mutation == "STALE_EVIDENCE":
                    value["evidence"]["observed_at"] = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
                elif mutation == "OWNER_DIGEST_TAMPER":
                    value["owner_request_digest"] = "0" * 64
                elif mutation == "ZERO_TRANSPORT_BYTES":
                    value["evidence"]["transport"]["bytes_sent"] = 0
                elif mutation == "PHYSICAL_OUTCOME_FORGED":
                    value["evidence"]["physical_effect_outcome"] = "EFFECT_OBSERVED"
                elif mutation == "EXTRA_EVIDENCE_FIELD":
                    value["evidence"]["unexpected"] = True
                response = client.reconcile(value)
                rows = int(ledger.connection.execute("SELECT count(*) FROM effect_scope_retirements").fetchone()[0])
                invariant = original.get("state") == "OUTCOME_UNKNOWN" and physical_adapter_calls == 1
                if mutation == "VALID":
                    exact_replay = client.reconcile(value)
                    passed = (
                        invariant
                        and response.get("ok") is True
                        and response.get("state") == "RETIRED_TARGET_OUTCOME_UNKNOWN"
                        and ledger.unresolved_count() == 0
                        and ledger.raw_unresolved_count() == 1
                        and rows == 1
                        and exact_replay.get("replay") is True
                    )
                    valid_count += 1
                    replay_count += int(exact_replay.get("replay") is True)
                else:
                    passed = invariant and response.get("ok") is False and ledger.unresolved_count() == 1 and rows == 0
                    detected[mutation] += int(passed)
                if not passed:
                    failures.append({"index": index, "mutation": mutation, "response_reason": str(response.get("reason"))})
            finally:
                server.close()
                ledger.close()
    return {
        "schema": "cra.target_retirement_chaos_report.v1",
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run exact-target retirement reconciliation chaos")
    parser.add_argument("--cases", type=int, default=512)
    parser.add_argument("--seed", type=int, default=20260901)
    args = parser.parse_args()
    report = run_target_retirement_chaos(case_count=args.cases, seed=args.seed)
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    if not report["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
