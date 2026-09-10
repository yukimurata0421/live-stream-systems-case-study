from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator, FormatChecker

from cra_dell_recovery.canonical import Signer, canonical_json
from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.observation import SCHEMA
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc

CONFIG_SCHEMA = "cra_dell_recovery.observation_publisher_config.v1"
TARGET_SCHEMA = "cra_dell_recovery.target_snapshot.v1"


def _read_json(path: Path, *, maximum_bytes: int) -> dict[str, Any]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("DELL_OBSERVATION_INPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("DELL_OBSERVATION_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("DELL_OBSERVATION_INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("DELL_OBSERVATION_INPUT_TOO_LARGE")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("DELL_OBSERVATION_INPUT_NOT_OBJECT")
    return dict(value)


def _validator(path: Path) -> Draft202012Validator:
    return Draft202012Validator(
        json.loads(path.read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    )


def _private_key(path: Path) -> Ed25519PrivateKey:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("DELL_OBSERVATION_SIGNING_KEY_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ValueError("DELL_OBSERVATION_SIGNING_KEY_PERMISSIONS_UNSAFE")
        if metadata.st_size > 64 * 1024:
            raise ValueError("DELL_OBSERVATION_SIGNING_KEY_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(64 * 1024 + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > 64 * 1024:
        raise ValueError("DELL_OBSERVATION_SIGNING_KEY_TOO_LARGE")
    value = serialization.load_pem_private_key(raw, password=None)
    if not isinstance(value, Ed25519PrivateKey):
        raise ValueError("DELL_OBSERVATION_SIGNING_KEY_NOT_ED25519")
    return value


def _required_mapping(value: object, code: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(code)
    return dict(value)


def _integer(value: object, code: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(code)
    try:
        result = int(value)
    except ValueError as error:
        raise ValueError(code) from error
    if result < minimum:
        raise ValueError(code)
    return result


def _number(value: object, code: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(code)
    try:
        result = float(value)
    except ValueError as error:
        raise ValueError(code) from error
    if not math.isfinite(result) or result < minimum:
        raise ValueError(code)
    return result


def _optional_number(value: object, code: str, *, minimum: float = 0.0) -> float | None:
    """Validate a numeric observation while preserving an explicit unknown."""

    if value is None:
        return None
    return _number(value, code, minimum=minimum)


@dataclass(frozen=True)
class DellObservationPublisherConfig:
    value: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: Path) -> DellObservationPublisherConfig:
        value = _read_json(path, maximum_bytes=64 * 1024)
        required = {
            "schema",
            "publisher_release_id",
            "source_instance_id",
            "source_release_id",
            "expected_target",
            "target_snapshot_file",
            "transport_snapshot_file",
            "controller_state_file",
            "maintenance_snapshot_file",
            "target_snapshot_schema_file",
            "observation_schema_file",
            "signing_private_key_file",
            "key_id",
            "maximum_transport_age_seconds",
            "maximum_state_age_seconds",
            "maximum_bundle_ttl_seconds",
            "stall_confirm_threshold",
            "stall_lastsnd_ms",
            "stall_notsent_bytes",
            "stall_unacked",
            "healthy_lastsnd_ms",
            "minimum_upload_progress_bytes",
            "minimum_healthy_send_mbps",
            "startup_gate_uptime_seconds",
        }
        if set(value) != required:
            raise ValueError("DELL_OBSERVATION_CONFIG_FIELDS_NOT_EXACT")
        if value["schema"] != CONFIG_SCHEMA:
            raise ValueError("DELL_OBSERVATION_CONFIG_SCHEMA_UNSUPPORTED")
        for name in ("publisher_release_id", "source_instance_id", "source_release_id", "key_id"):
            if not str(value[name]).strip():
                raise ValueError(f"DELL_OBSERVATION_CONFIG_{name.upper()}_MISSING")
        require_runtime_release(value["publisher_release_id"], "DELL_OBSERVATION_RUNTIME_RELEASE_MISMATCH")
        require_runtime_release(value["source_release_id"], "DELL_OBSERVATION_SOURCE_RELEASE_MISMATCH")
        expected = _required_mapping(value["expected_target"], "DELL_OBSERVATION_EXPECTED_TARGET_INVALID")
        if set(expected) != {"host_id", "namespace", "container_name"}:
            raise ValueError("DELL_OBSERVATION_EXPECTED_TARGET_FIELDS_NOT_EXACT")
        if not str(expected["host_id"]).strip() or not str(expected["namespace"]).strip() or expected["container_name"] != "stream-engine":
            raise ValueError("DELL_OBSERVATION_EXPECTED_TARGET_INVALID")
        for name, lower, upper in (
            ("maximum_transport_age_seconds", 1.0, 60.0),
            ("maximum_state_age_seconds", 1.0, 60.0),
            ("maximum_bundle_ttl_seconds", 1.0, 30.0),
        ):
            if not lower <= float(value[name]) <= upper:
                raise ValueError(f"DELL_OBSERVATION_CONFIG_{name.upper()}_OUT_OF_RANGE")
        for name, lower, upper in (
            ("stall_confirm_threshold", 1, 20),
            ("stall_lastsnd_ms", 1000, 120_000),
            ("stall_notsent_bytes", 1, 128 * 1024 * 1024),
            ("stall_unacked", 1, 1_000_000),
            ("healthy_lastsnd_ms", 1, 60_000),
            ("minimum_upload_progress_bytes", 1, 128 * 1024 * 1024),
            ("startup_gate_uptime_seconds", 1, 600),
        ):
            if not lower <= int(value[name]) <= upper:
                raise ValueError(f"DELL_OBSERVATION_CONFIG_{name.upper()}_OUT_OF_RANGE")
        if not 0.001 <= float(value["minimum_healthy_send_mbps"]) <= 100_000:
            raise ValueError("DELL_OBSERVATION_CONFIG_MINIMUM_HEALTHY_SEND_MBPS_OUT_OF_RANGE")
        return cls(value, path)


class DellObservationPublisher:
    """Produces signed target/transport facts and owns no command or effect API."""

    def __init__(self, config: DellObservationPublisherConfig) -> None:
        self.config = config
        value = config.value
        self.target_validator = _validator(Path(value["target_snapshot_schema_file"]))
        self.observation_validator = _validator(Path(value["observation_schema_file"]))
        self.signer = Signer(str(value["key_id"]), _private_key(Path(value["signing_private_key_file"])))

    def _target(self, now: datetime) -> tuple[dict[str, Any], TargetIdentity, datetime, datetime]:
        value = self.config.value
        snapshot = _read_json(Path(value["target_snapshot_file"]), maximum_bytes=256 * 1024)
        self.target_validator.validate(snapshot)
        if snapshot.get("schema") != TARGET_SCHEMA or snapshot.get("status") != "VALID" or snapshot.get("runtime_status") != "VALID":
            raise ValueError("DELL_OBSERVATION_TARGET_NOT_VALID")
        if snapshot.get("runtime_container_ready") is not True:
            raise ValueError("DELL_OBSERVATION_RUNTIME_NOT_READY")
        target = TargetIdentity.from_dict(_required_mapping(snapshot["target_identity"], "DELL_OBSERVATION_TARGET_MISSING"))
        expected = _required_mapping(value["expected_target"], "DELL_OBSERVATION_EXPECTED_TARGET_INVALID")
        if (
            target.host_id != expected["host_id"]
            or target.namespace != expected["namespace"]
            or target.container_name != expected["container_name"]
        ):
            raise ValueError("DELL_OBSERVATION_TARGET_IDENTITY_MISMATCH")
        runtime = _required_mapping(snapshot["runtime_identity"], "DELL_OBSERVATION_RUNTIME_IDENTITY_MISSING")
        if (
            runtime.get("host_id") != target.host_id
            or runtime.get("host_boot_id") != target.host_boot_id
            or runtime.get("namespace") != target.namespace
            or runtime.get("pod_uid") != target.pod_uid
            or runtime.get("stream_engine_container_name") != target.container_name
            or runtime.get("stream_engine_container_id") != target.container_id
        ):
            raise ValueError("DELL_OBSERVATION_RUNTIME_TARGET_MISMATCH")
        observed = parse_utc(str(snapshot["observed_at"]))
        valid_until = parse_utc(str(snapshot["valid_until"]))
        if observed > now or valid_until <= now:
            raise ValueError("DELL_OBSERVATION_TARGET_NOT_FRESH")
        return snapshot, target, observed, valid_until

    def _transport(
        self,
        target: TargetIdentity,
        now: datetime,
    ) -> tuple[dict[str, Any], datetime, datetime, dict[str, Any], dict[str, Any]]:
        value = self.config.value
        raw = _read_json(Path(value["transport_snapshot_file"]), maximum_bytes=256 * 1024)
        exact = {"ts_utc", "controller_id", "ffmpeg_pid", "ffmpeg_uptime_sec", "metrics", "network"}
        if set(raw) != exact:
            raise ValueError("DELL_OBSERVATION_TRANSPORT_FIELDS_NOT_EXACT")
        observed = parse_utc(str(raw["ts_utc"]))
        if observed > now:
            raise ValueError("DELL_OBSERVATION_TRANSPORT_IN_FUTURE")
        valid_until = observed + timedelta(seconds=float(value["maximum_transport_age_seconds"]))
        if valid_until <= now:
            raise ValueError("DELL_OBSERVATION_TRANSPORT_STALE")
        if _integer(raw["ffmpeg_pid"], "DELL_OBSERVATION_TRANSPORT_PID_INVALID", minimum=2) != target.ffmpeg_pid:
            raise ValueError("DELL_OBSERVATION_TRANSPORT_TARGET_MISMATCH")
        metrics = _required_mapping(raw["metrics"], "DELL_OBSERVATION_TRANSPORT_METRICS_MISSING")
        network = _required_mapping(raw["network"], "DELL_OBSERVATION_NETWORK_MISSING")
        expected_metrics = {
            "bytes_sent_delta",
            "bytes_sent",
            "bytes_acked",
            "bytes_elapsed_sec",
            "send_mbps",
            "send_q",
            "lastsnd_ms",
            "notsent",
            "unacked",
            "rto_ms",
            "network_down",
            "remote_warning",
            "low_upload_pressure",
        }
        expected_network = {"gateway_ok", "public_ok_count", "dns_ok", "tcp_probe_ok", "network_down"}
        if set(metrics) != expected_metrics or set(network) != expected_network:
            raise ValueError("DELL_OBSERVATION_TRANSPORT_PAYLOAD_FIELDS_NOT_EXACT")
        for name in (
            "bytes_sent_delta",
            "bytes_sent",
            "bytes_acked",
            "bytes_elapsed_sec",
            "send_q",
            "lastsnd_ms",
            "notsent",
            "unacked",
            "rto_ms",
        ):
            metrics[name] = _integer(metrics[name], f"DELL_OBSERVATION_METRIC_{name.upper()}_INVALID")
        metrics["send_mbps"] = _optional_number(metrics["send_mbps"], "DELL_OBSERVATION_METRIC_SEND_MBPS_INVALID")
        for name in ("network_down", "remote_warning", "low_upload_pressure"):
            if not isinstance(metrics[name], bool):
                raise ValueError(f"DELL_OBSERVATION_METRIC_{name.upper()}_INVALID")
        network["public_ok_count"] = _integer(
            network["public_ok_count"],
            "DELL_OBSERVATION_NETWORK_PUBLIC_OK_COUNT_INVALID",
        )
        for name in ("gateway_ok", "dns_ok", "tcp_probe_ok", "network_down"):
            if not isinstance(network[name], bool):
                raise ValueError(f"DELL_OBSERVATION_NETWORK_{name.upper()}_INVALID")
        projected = {
            "observed_at": isoformat_utc(observed),
            "controller_id": str(raw["controller_id"]),
            "ffmpeg_pid": target.ffmpeg_pid,
            "ffmpeg_uptime_sec": _integer(raw["ffmpeg_uptime_sec"], "DELL_OBSERVATION_FFMPEG_UPTIME_INVALID"),
            "metrics": metrics,
            "network": network,
        }
        return projected, observed, valid_until, metrics, network

    def _recovery(
        self,
        target: TargetIdentity,
        now: datetime,
    ) -> tuple[dict[str, Any], datetime, datetime]:
        value = self.config.value
        state = _read_json(Path(value["controller_state_file"]), maximum_bytes=2 * 1024 * 1024)
        observed_epoch = _number(state.get("observed_ts"), "DELL_OBSERVATION_STATE_TIMESTAMP_INVALID")
        observed = datetime.fromtimestamp(observed_epoch, UTC)
        if observed > now:
            raise ValueError("DELL_OBSERVATION_STATE_IN_FUTURE")
        valid_until = observed + timedelta(seconds=float(value["maximum_state_age_seconds"]))
        if valid_until <= now:
            raise ValueError("DELL_OBSERVATION_STATE_STALE")
        if _integer(state.get("last_pid"), "DELL_OBSERVATION_STATE_PID_INVALID", minimum=2) != target.ffmpeg_pid:
            raise ValueError("DELL_OBSERVATION_STATE_TARGET_MISMATCH")
        result = {
            "observed_at": isoformat_utc(observed),
            "stall_streak": _integer(state.get("stall_streak"), "DELL_OBSERVATION_STALL_STREAK_INVALID"),
            "stall_confirm_threshold": int(value["stall_confirm_threshold"]),
            "net_fail_streak": _integer(state.get("net_fail_streak"), "DELL_OBSERVATION_NET_FAIL_STREAK_INVALID"),
        }
        return result, observed, valid_until

    def _maintenance(
        self,
        target: TargetIdentity,
        now: datetime,
    ) -> tuple[dict[str, str], datetime, datetime]:
        value = self.config.value
        projection = _read_json(Path(value["maintenance_snapshot_file"]), maximum_bytes=512 * 1024)
        if projection.get("schema_version") != "maintenance.snapshot_projection.v1":
            raise ValueError("DELL_OBSERVATION_MAINTENANCE_SCHEMA_INVALID")
        payload = _required_mapping(projection.get("payload"), "DELL_OBSERVATION_MAINTENANCE_PAYLOAD_MISSING")
        observed = parse_utc(str(payload.get("observed_at") or projection.get("source_observed_at") or ""))
        valid_until = min(
            parse_utc(str(projection["fresh_until"])),
            parse_utc(str(projection["source_fresh_until"])),
            parse_utc(str(payload["fresh_until"])),
        )
        if observed > now or valid_until <= now:
            raise ValueError("DELL_OBSERVATION_MAINTENANCE_NOT_FRESH")
        source_target = TargetIdentity.from_dict(
            _required_mapping(payload.get("source_target_identity"), "DELL_OBSERVATION_MAINTENANCE_TARGET_MISSING")
        )
        projected_target = TargetIdentity.from_dict(
            _required_mapping(payload.get("target_identity"), "DELL_OBSERVATION_MAINTENANCE_TARGET_MISSING")
        )
        target_matches = source_target == target and projected_target == target
        proof = _required_mapping(payload.get("proof"), "DELL_OBSERVATION_MAINTENANCE_PROOF_MISSING")
        positive_inactive = (
            payload.get("available") is True
            and payload.get("maintenance_state") == "INACTIVE"
            and payload.get("transaction_state") == "INACTIVE"
            and proof.get("integrity_ok") is True
            and proof.get("positive_inactive_proof") is True
            and proof.get("startup_reconciled") is True
            and proof.get("transaction_uncertain") is False
            and _integer(proof.get("unresolved_transaction_count"), "DELL_OBSERVATION_MAINTENANCE_PROOF_INVALID") == 0
            and target_matches
        )
        if positive_inactive:
            status = "FALSE"
        elif payload.get("maintenance_state") not in (None, "", "INACTIVE") and target_matches:
            status = "TRUE"
        else:
            status = "UNKNOWN"
        evidence_ref = f"dell/maintenance/{projection['projection_id']}"
        return {"status": status, "observed_at": isoformat_utc(observed), "evidence_ref": evidence_ref}, observed, valid_until

    def build(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        value = self.config.value
        target_snapshot, target, target_observed, target_valid = self._target(current)
        transport, transport_observed, transport_valid, metrics, network = self._transport(target, current)
        recovery, recovery_observed, recovery_valid = self._recovery(target, current)
        maintenance, maintenance_observed, maintenance_valid = self._maintenance(target, current)
        network_consistent = metrics["network_down"] is network["network_down"]
        network_status = ("TRUE" if network["network_down"] else "FALSE") if network_consistent else "UNKNOWN"
        raw_stall = (
            metrics["bytes_sent_delta"] == 0
            and metrics["lastsnd_ms"] >= int(value["stall_lastsnd_ms"])
            and (metrics["notsent"] >= int(value["stall_notsent_bytes"]) or metrics["unacked"] >= int(value["stall_unacked"]))
        )
        stall_confirmed = raw_stall and recovery["stall_streak"] >= recovery["stall_confirm_threshold"]
        tcp_flow_healthy = (
            not network["network_down"]
            and network["tcp_probe_ok"]
            and metrics["bytes_sent_delta"] >= int(value["minimum_upload_progress_bytes"])
            and metrics["lastsnd_ms"] <= int(value["healthy_lastsnd_ms"])
        )
        upload_progress_healthy = (
            metrics["send_mbps"] is not None
            and metrics["bytes_sent_delta"] >= int(value["minimum_upload_progress_bytes"])
            and metrics["send_mbps"] >= float(value["minimum_healthy_send_mbps"])
            and metrics["low_upload_pressure"] is False
        )
        startup_gate = transport["ffmpeg_uptime_sec"] >= int(value["startup_gate_uptime_seconds"])
        checks = {
            "tcp_stall": {
                "status": "CONFIRMED" if stall_confirmed else "FALSE",
                "observed_at": isoformat_utc(max(transport_observed, recovery_observed)),
                "evidence_ref": f"dell/transport/{target_snapshot['snapshot_id']}/tcp-stall",
            },
            "network_down": {
                "status": network_status,
                "observed_at": isoformat_utc(transport_observed),
                "evidence_ref": f"dell/transport/{target_snapshot['snapshot_id']}/network",
            },
            "ffmpeg_present": {
                "status": "TRUE",
                "observed_at": isoformat_utc(target_observed),
                "evidence_ref": f"dell/target/{target_snapshot['snapshot_id']}/ffmpeg",
            },
            "target_stable": {
                "status": "TRUE",
                "observed_at": isoformat_utc(max(target_observed, transport_observed, recovery_observed)),
                "evidence_ref": f"dell/target/{target_snapshot['snapshot_id']}/stable",
            },
            "maintenance": maintenance,
            "stream_engine_ready": {
                "status": "TRUE",
                "observed_at": isoformat_utc(target_observed),
                "evidence_ref": f"dell/target/{target_snapshot['snapshot_id']}/runtime-ready",
            },
            "tcp_flow_healthy": {
                "status": "TRUE" if tcp_flow_healthy else "FALSE",
                "observed_at": isoformat_utc(transport_observed),
                "evidence_ref": f"dell/transport/{target_snapshot['snapshot_id']}/tcp-flow",
            },
            "upload_progress_healthy": {
                "status": "UNKNOWN" if metrics["send_mbps"] is None else ("TRUE" if upload_progress_healthy else "FALSE"),
                "observed_at": isoformat_utc(transport_observed),
                "evidence_ref": f"dell/transport/{target_snapshot['snapshot_id']}/upload-progress",
            },
            "startup_gate": {
                "status": "TRUE" if startup_gate else "FALSE",
                "observed_at": isoformat_utc(transport_observed),
                "evidence_ref": f"dell/target/{target_snapshot['snapshot_id']}/startup-gate",
            },
        }
        transport_ref = f"dell/transport/{target_snapshot['snapshot_id']}"
        measurements = [
            {"name": "rtmps_bytes_delta", "value": metrics["bytes_sent_delta"], "unit": "bytes", "evidence_ref": transport_ref},
            {"name": "rtmps_lastsnd_ms", "value": metrics["lastsnd_ms"], "unit": "milliseconds", "evidence_ref": transport_ref},
            {"name": "rtmps_notsent_bytes", "value": metrics["notsent"], "unit": "bytes", "evidence_ref": transport_ref},
            {"name": "rtmps_unacked_segments", "value": metrics["unacked"], "unit": "segments", "evidence_ref": transport_ref},
            {"name": "tcp_stall_streak", "value": recovery["stall_streak"], "unit": "samples", "evidence_ref": transport_ref},
            {"name": "network_down_streak", "value": recovery["net_fail_streak"], "unit": "samples", "evidence_ref": transport_ref},
        ]
        if metrics["send_mbps"] is not None:
            measurements.insert(
                1,
                {
                    "name": "rtmps_send_mbps",
                    "value": metrics["send_mbps"],
                    "unit": "megabits_per_second",
                    "evidence_ref": transport_ref,
                },
            )
        observed = max(target_observed, transport_observed, recovery_observed, maintenance_observed)
        valid_until = min(
            target_valid,
            transport_valid,
            recovery_valid,
            maintenance_valid,
            observed + timedelta(seconds=float(value["maximum_bundle_ttl_seconds"])),
        )
        if observed > current or valid_until <= current:
            raise ValueError("DELL_OBSERVATION_BUNDLE_HAS_NO_VALIDITY_WINDOW")
        source = {
            "target_snapshot": target_snapshot,
            "transport": transport,
            "recovery": recovery,
            "maintenance": maintenance,
            "checks": checks,
            "measurements": measurements,
        }
        revision = hashlib.sha256(canonical_json(source)).hexdigest()
        # The evidence timestamp can remain unchanged while another raw input at the
        # same timestamp changes. Use issuance time for transport ordering so two
        # different signed payloads never claim the same inbox sequence.
        sequence = int(current.timestamp() * 1_000_000)
        bundle: dict[str, Any] = {
            "schema": SCHEMA,
            "source_instance_id": value["source_instance_id"],
            "source_release_id": value["source_release_id"],
            "observation_id": f"dell-observation-{revision[:40]}",
            "observation_revision": revision,
            "observation_sequence": sequence,
            "observed_at": isoformat_utc(observed),
            "valid_until": isoformat_utc(valid_until),
            "target_snapshot": target_snapshot,
            "transport": transport,
            "recovery": recovery,
            "maintenance": maintenance,
            "checks": checks,
            "measurements": measurements,
            "evidence_refs": sorted(
                {
                    f"dell/target/{target_snapshot['snapshot_id']}",
                    transport_ref,
                    maintenance["evidence_ref"],
                }
            ),
            "key_id": self.signer.key_id,
        }
        signed = self.signer.sign(bundle)
        self.observation_validator.validate(signed)
        return signed
