from __future__ import annotations

import argparse
import json
import os
import signal
import ssl
import threading
import time
import uuid
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_authority.reconciliation import CentralReconciler
from cra_authority.storage import CentralStore
from cra_authority.transport import SignedHTTPSClient
from cra_dell_recovery.canonical import KeyRing, SignedMessageCodec, Signer
from cra_dell_recovery.models import MonitoringReadiness, RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.schema import SchemaRegistry
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now
from maintenance_shadow.snapshot import atomic_write_json, read_json


def _private(path: Path) -> Ed25519PrivateKey:
    value = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(value, Ed25519PrivateKey):
        raise ValueError("CRA signing key must be Ed25519")
    return value


def _public(path: Path) -> Ed25519PublicKey:
    value = serialization.load_pem_public_key(path.read_bytes())
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("agent verification key must be Ed25519")
    return value


def _append(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _authority_projection(store: CentralStore, target_id: str) -> dict[str, Any]:
    target = store.authority_snapshot(target_id)
    session = store.connection.execute(
        """SELECT authority_session_id,authority_epoch,state,last_heartbeat_sent_at,last_heartbeat_acked_at
           FROM authority_sessions WHERE target_id=? AND state='ACTIVE'
           ORDER BY authority_epoch DESC LIMIT 1""",
        (target_id,),
    ).fetchone()
    return {
        "schema_version": "maintenance.authority_projection.v1",
        "target_id": target_id,
        "authority_state": str(target["authority_state"]),
        "authority_epoch": int(target["current_authority_epoch"]),
        "authority_session_id": str(session["authority_session_id"] if session is not None else ""),
        "session_state": str(session["state"] if session is not None else "MISSING"),
        "last_heartbeat_sent_at": session["last_heartbeat_sent_at"] if session is not None else None,
        "last_heartbeat_acked_at": session["last_heartbeat_acked_at"] if session is not None else None,
        "observed_at": isoformat_utc(utc_now()),
        "production_behavior_modified": False,
    }


def _publish_local_projections(
    store: CentralStore,
    config: dict[str, Any],
    target_snapshot: dict[str, Any] | None,
) -> None:
    if target_snapshot is not None and config.get("target_snapshot_cache"):
        atomic_write_json(Path(config["target_snapshot_cache"]), target_snapshot)
    if config.get("authority_projection"):
        atomic_write_json(
            Path(config["authority_projection"]),
            _authority_projection(store, str(config["target_id"])),
        )


def _distribute_maintenance_snapshot(
    client: SignedHTTPSClient,
    config: dict[str, Any],
    output_dir: Path,
) -> None:
    if not config.get("maintenance_snapshot_source"):
        return
    snapshot = read_json(Path(config["maintenance_snapshot_source"]))
    if snapshot is None:
        _append(
            output_dir / "maintenance_snapshot_distribution.jsonl",
            {
                "observed_at": isoformat_utc(utc_now()),
                "result": "SOURCE_UNAVAILABLE",
                "physical_effect_count": 0,
                "production_behavior_modified": False,
            },
        )
        return
    try:
        result = client.post_shadow_maintenance_snapshot(snapshot)
    except Exception as exc:
        result = {"result": "TRANSPORT_ERROR", "error": type(exc).__name__}
    _append(
        output_dir / "maintenance_snapshot_distribution.jsonl",
        {
            **result,
            "observed_at": isoformat_utc(utc_now()),
            "source_snapshot_id": str(snapshot.get("snapshot_id") or ""),
            "physical_effect_count": 0,
            "production_behavior_modified": False,
        },
    )


def _initial_reconcile_with_retry(
    reconciler: _Reconciler,
    target_id: str,
    output_dir: Path,
    stopped: threading.Event,
    *,
    retry_seconds: float,
) -> int | None:
    while not stopped.is_set():
        try:
            return reconciler.reconcile(target_id)
        except Exception as exc:
            _append(
                output_dir / "agent_state.jsonl",
                {
                    "event": "STARTUP_RECONCILIATION_RETRY",
                    "error": type(exc).__name__,
                    "observed_at": isoformat_utc(utc_now()),
                    "physical_effect_count": 0,
                    "production_behavior_modified": False,
                },
            )
            stopped.wait(max(0.1, retry_seconds))
    return None


class _Reconciler(Protocol):
    def reconcile(self, target_id: str) -> int: ...


class AuthorityReconciliationLoop:
    """Reclaim authority only from durable states that require reconciliation."""

    def __init__(
        self,
        reconciler: _Reconciler,
        target_id: str,
        output_dir: Path,
        *,
        retry_seconds: float = 15.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.reconciler = reconciler
        self.target_id = target_id
        self.output_dir = output_dir
        self.retry_seconds = retry_seconds
        self.monotonic = monotonic
        self.last_attempt_at: float | None = None

    def observe(self, response: dict[str, Any]) -> int | None:
        observed_state = response.get("authority_state")
        if observed_state not in {
            "AGENT_STARTUP_RECONCILING",
            "LOCAL_FALLBACK",
            "LOCAL_AUTHORITY_ACTIVE",
        }:
            return None
        now = self.monotonic()
        if self.last_attempt_at is not None and now - self.last_attempt_at < self.retry_seconds:
            return None
        self.last_attempt_at = now
        epoch = self.reconciler.reconcile(self.target_id)
        _append(
            self.output_dir / "agent_state.jsonl",
            {
                "event": "RECONCILED",
                "authority_epoch": epoch,
                "observed_at": isoformat_utc(utc_now()),
                "trigger": f"READY_AUTHORITY_RECONCILIATION_REQUIRED:{observed_state}",
            },
        )
        return epoch


def _monitoring_readiness(config: dict[str, Any]) -> MonitoringReadiness:
    now = utc_now()
    try:
        sentinel_path = Path(config["monitoring_sentinel"])
        report_path = Path(config["monitoring_report"])
        sentinel = json.loads(sentinel_path.read_text(encoding="utf-8"))
        report = json.loads(report_path.read_text(encoding="utf-8"))
        sentinel_at = parse_utc(str(sentinel["checked_at"]))
        parse_utc(str(report["generated_at"]))
        assessed_report_at = parse_utc(str(sentinel["report"]["generated_at"]))
        sentinel_max_age = float(config.get("monitoring_sentinel_max_age_seconds", 180.0))
        sentinel_report = dict(sentinel.get("report") or {})
        fresh = (
            0 <= (now - sentinel_at).total_seconds() <= sentinel_max_age
            and assessed_report_at <= now
            and sentinel_report.get("fresh") is True
            and sentinel_report.get("future") is False
            and sentinel_report.get("readable") is True
        )
        safe = (
            sentinel.get("status") == "good"
            and sentinel.get("detection_only") is True
            and sentinel.get("automatic_runtime_mutation_enabled") is False
            and sentinel.get("automatic_k3s_restart_enabled") is False
            and sentinel_report.get("latest_cycle_clean") is True
            and sentinel_report.get("parity_clean") is True
            and sentinel_report.get("projection_clean") is True
            and sentinel_report.get("unsafe_runtime_mutation") is False
            and sentinel_report.get("unsafe_real_delivery") is False
        )
        override_path = Path(config["monitoring_override"])
        if override_path.exists():
            override = json.loads(override_path.read_text(encoding="utf-8"))
            if override.get("force_stale") is True and parse_utc(str(override["expires_at"])) > now:
                return MonitoringReadiness(False, True, isoformat_utc(now), "PHASE4_ISOLATED_STALE_OVERRIDE")
        observed = min(sentinel_at, assessed_report_at)
        reason = "REAL_MONITORING_V4_READONLY" if fresh and safe else "MONITORING_V4_FORMAL_READINESS_FALSE"
        return MonitoringReadiness(fresh, safe, isoformat_utc(observed), reason)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return MonitoringReadiness(False, False, isoformat_utc(now), "MONITORING_EVIDENCE_UNAVAILABLE")


def _diagnostic_command(
    store: CentralStore,
    codec: SignedMessageCodec,
    client: SignedHTTPSClient,
    config: dict[str, Any],
    output_dir: Path,
) -> None:
    if not config.get("diagnostic_shadow_command_once", False) or store.command_count() != 0:
        return
    target_response = client.get_shadow_target()
    snapshot = target_response.get("snapshot")
    if target_response.get("available") is not True or not isinstance(snapshot, dict):
        _append(output_dir / "commands.jsonl", {"kind": "DIAGNOSTIC_SKIPPED", "reason": target_response.get("reason_code")})
        return
    expected = TargetIdentity.from_dict(dict(snapshot["target_identity"]))
    now = utc_now()
    suffix = uuid.uuid4().hex
    authorization = RecoveryAuthorizationInput(
        authorization_id=f"phase4-auth-{suffix}",
        incident_id=f"phase4-incident-{suffix}",
        source_episode_id=f"phase4-diagnostic-{suffix}",
        target_id=str(config["target_id"]),
        action="restart_ffmpeg",
        reason_code="confirmed_tcp_stall",
        policy_revision="shadow-policy-v1",
        observation_revision=str(snapshot["source_revision"]),
        expected_target=expected,
        blockers=(),
        authorized_at=isoformat_utc(now),
        expires_at=isoformat_utc(now + timedelta(seconds=30)),
    )
    store.add_authorization(authorization)
    command = store.create_command(authorization.authorization_id, codec)
    start = time.monotonic_ns()
    response = client.post_shadow_command(command)
    latency_ms = (time.monotonic_ns() - start) / 1_000_000
    _append(
        output_dir / "commands.jsonl",
        {
            "kind": "PHASE4_DIAGNOSTIC_SHADOW_COMMAND",
            "command_id": command["command_id"],
            "authority_epoch": command["authority_epoch"],
            "command_seq": command["command_seq"],
            "expected_target": expected.to_dict(),
            "issued_at": command["issued_at"],
            "expires_at": command["expires_at"],
            "protocol_latency_ms": latency_ms,
        },
    )
    _append(output_dir / "receipts.jsonl", {**response, "protocol_latency_ms": latency_ms})


def main() -> None:
    parser = argparse.ArgumentParser(description="CRA Phase 4 real no-action shadow loop")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = dict(json.loads(args.config.read_text(encoding="utf-8")))
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    store = CentralStore(Path(config["database"]), Path(config["migration"]))
    if store.connection.execute("SELECT count(*) FROM control_plane_identity").fetchone()[0] == 0:
        store.bootstrap(
            target_id=str(config["target_id"]),
            host_id=str(config["host_id"]),
            agent_id=str(config["agent_id"]),
            controller_instance_id=str(config["controller_instance_id"]),
            agent_installation_id=str(config["agent_installation_id"]),
        )
    codec = SignedMessageCodec(
        SchemaRegistry(Path(config["contract_dir"])),
        Signer(str(config["cra_key_id"]), _private(Path(config["cra_private_key"]))),
        KeyRing({str(config["agent_key_id"]): _public(Path(config["agent_public_key"]))}),
    )
    tls = ssl.create_default_context(cafile=str(config["server_ca_certificate"]))
    tls.minimum_version = ssl.TLSVersion.TLSv1_3
    tls.check_hostname = True
    tls.load_cert_chain(str(config["tls_certificate"]), str(config["tls_private_key"]))
    client = SignedHTTPSClient(str(config["agent_base_url"]), tls, timeout=float(config.get("timeout_seconds", 2.0)))
    reconciler = CentralReconciler(store, codec, client)
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    epoch = _initial_reconcile_with_retry(
        reconciler,
        str(config["target_id"]),
        output_dir,
        stopped,
        retry_seconds=float(config.get("startup_reconciliation_retry_seconds", 2.0)),
    )
    if epoch is None:
        store.connection.close()
        return
    _append(output_dir / "agent_state.jsonl", {"event": "RECONCILED", "authority_epoch": epoch, "observed_at": isoformat_utc(utc_now())})
    publisher = AuthorityHeartbeatPublisher(
        store,
        codec,
        interval_seconds=float(config.get("heartbeat_interval_seconds", 2.0)),
        lease_ttl_seconds=float(config.get("lease_ttl_seconds", 15.0)),
    )
    interval = float(config.get("heartbeat_interval_seconds", 2.0))
    authority_reconciliation = AuthorityReconciliationLoop(
        reconciler,
        str(config["target_id"]),
        output_dir,
        retry_seconds=float(config.get("reconciliation_retry_seconds", 15.0)),
    )
    diagnostic_pending = True
    while not stopped.is_set():
        cycle_started = time.monotonic_ns()
        readiness = _monitoring_readiness(config)
        heartbeat = publisher.build(str(config["target_id"]), readiness)
        event: dict[str, Any] = {
            "cycle_observed_at": isoformat_utc(utc_now()),
            "monitoring_ready": readiness.authority_ready,
            "monitoring_reason": readiness.reason,
            "sent": heartbeat is not None,
        }
        if heartbeat is not None:
            try:
                started = time.monotonic_ns()
                response = client.post_shadow_heartbeat(heartbeat)
                event.update(
                    {
                        "heartbeat_id": heartbeat["heartbeat_id"],
                        "heartbeat_seq": heartbeat["heartbeat_seq"],
                        "authority_epoch": heartbeat["authority_epoch"],
                        "authority_session_id": heartbeat["authority_session_id"],
                        "issued_at": heartbeat["issued_at"],
                        "received_authority_state": response.get("authority_state"),
                        "processing_roundtrip_ms": (time.monotonic_ns() - started) / 1_000_000,
                        "physical_attempt_count": response.get("physical_attempt_count"),
                    }
                )
                reconciled_epoch = authority_reconciliation.observe(response)
                if reconciled_epoch is not None:
                    event["reconciled_authority_epoch"] = reconciled_epoch
                if diagnostic_pending:
                    _diagnostic_command(store, codec, client, config, output_dir)
                    diagnostic_pending = False
            except Exception as exc:  # bounded loop records transport class without secrets
                event.update({"transport_error": type(exc).__name__})
        event["cycle_duration_ms"] = (time.monotonic_ns() - cycle_started) / 1_000_000
        _append(output_dir / "heartbeat.jsonl", event)
        try:
            status = client.get_shadow_status()
            _append(output_dir / "agent_state.jsonl", {**status, "observed_at": isoformat_utc(utc_now())})
            if readiness.authority_ready:
                try:
                    authority_reconciliation.observe(status)
                except Exception as exc:
                    _append(
                        output_dir / "agent_state.jsonl",
                        {
                            "error": type(exc).__name__,
                            "event": "RECONCILIATION_RETRY_FAILED",
                            "observed_at": isoformat_utc(utc_now()),
                        },
                    )
            target = client.get_shadow_target()
            snapshot = target.get("snapshot")
            if isinstance(snapshot, dict):
                _append(output_dir / "target_snapshots.jsonl", snapshot)
            _publish_local_projections(store, config, dict(snapshot) if isinstance(snapshot, dict) else None)
        except Exception as exc:
            _append(output_dir / "agent_state.jsonl", {"error": type(exc).__name__, "observed_at": isoformat_utc(utc_now())})
        _distribute_maintenance_snapshot(client, config, output_dir)
        elapsed = (time.monotonic_ns() - cycle_started) / 1_000_000_000
        stopped.wait(max(0.05, interval - elapsed))


if __name__ == "__main__":
    main()
