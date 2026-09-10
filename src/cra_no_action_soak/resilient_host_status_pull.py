from __future__ import annotations

import argparse
import json
import math
import random
import ssl
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_dell_recovery.bounded_http import fetch_bounded_json
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.recovery_health import RecoveryHealthStore
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc
from cra_dell_recovery.transport_resilience import FailureClassification, classify_failure

from .host_status import atomic_write_json, read_object
from .host_status_pull import _client_context, _deadline_clock, _open_url, _public_key
from .resilient_host_status import STATUS_PATH, ResilientHostStatusContract, evidence_valid_until

CONFIG_SCHEMA = "cra.resilient_host_status_pull.v2"
ADMISSION_SCHEMA = "cra.resilient_host_status_admission.v1"
COMPONENTS = frozenset({"arena_dell_resilient_status_pull", "cra_arena_resilient_status_pull"})


class PullConfig:
    def __init__(self, value: dict[str, Any], path: Path) -> None:
        self.value = value
        self.path = path

    @classmethod
    def load(cls, path: Path) -> PullConfig:
        value = read_object(path, maximum_bytes=128 * 1024)
        required = {
            "schema",
            "component",
            "puller_release_id",
            "endpoint_url",
            "tls_certificate",
            "tls_private_key",
            "server_ca_certificate",
            "resilient_host_status_schema_file",
            "source_key_id",
            "source_public_key_file",
            "source_role",
            "source_host_id",
            "source_release_id",
            "upstream_key_id",
            "upstream_public_key_file",
            "upstream_role",
            "upstream_host_id",
            "upstream_release_id",
            "maximum_report_lease_seconds",
            "maximum_clock_skew_seconds",
            "minimum_remaining_lease_seconds",
            "timeout_seconds",
            "transient_retry_delays_seconds",
            "maximum_retry_elapsed_seconds",
            "output_file",
            "admission_state_file",
            "status_file",
            "recovery_state_file",
            "recovery_event_file",
        }
        if set(value) != required or value.get("schema") != CONFIG_SCHEMA:
            raise ValueError("RESILIENT_STATUS_PULL_CONFIG_FIELDS_INVALID")
        expected_source_roles = {
            "arena_dell_resilient_status_pull": "dell",
            "cra_arena_resilient_status_pull": "arena",
        }
        component = value.get("component")
        if not isinstance(component, str) or component not in COMPONENTS or value.get("source_role") != expected_source_roles[component]:
            raise ValueError("RESILIENT_STATUS_PULL_COMPONENT_INVALID")
        for name in ("puller_release_id", "source_key_id", "source_host_id", "source_release_id"):
            if not isinstance(value.get(name), str) or not str(value[name]).strip():
                raise ValueError(f"RESILIENT_STATUS_PULL_{name.upper()}_INVALID")
        require_runtime_release(str(value["puller_release_id"]), "RESILIENT_STATUS_PULL_RUNTIME_RELEASE_MISMATCH")
        endpoint = urllib.parse.urlparse(str(value.get("endpoint_url")))
        if endpoint.scheme != "https" or endpoint.path != STATUS_PATH or endpoint.query or endpoint.fragment or not endpoint.hostname:
            raise ValueError("RESILIENT_STATUS_PULL_ENDPOINT_INVALID")
        for name in (
            "tls_certificate",
            "tls_private_key",
            "server_ca_certificate",
            "resilient_host_status_schema_file",
            "source_public_key_file",
            "output_file",
            "admission_state_file",
            "status_file",
            "recovery_state_file",
            "recovery_event_file",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).startswith("/"):
                raise ValueError(f"RESILIENT_STATUS_PULL_{name.upper()}_INVALID")
        upstream_names = (
            "upstream_key_id",
            "upstream_public_key_file",
            "upstream_role",
            "upstream_host_id",
            "upstream_release_id",
        )
        if value["source_role"] == "arena":
            for name in upstream_names:
                if not isinstance(value.get(name), str) or not str(value[name]).strip():
                    raise ValueError(f"RESILIENT_STATUS_PULL_{name.upper()}_INVALID")
            if value["upstream_role"] != "dell" or not str(value["upstream_public_key_file"]).startswith("/"):
                raise ValueError("RESILIENT_STATUS_PULL_UPSTREAM_IDENTITY_INVALID")
        elif any(value.get(name) is not None for name in upstream_names):
            raise ValueError("RESILIENT_STATUS_PULL_DELL_UPSTREAM_FORBIDDEN")
        for name, lower, upper in (
            ("maximum_report_lease_seconds", 15, 300),
            ("maximum_clock_skew_seconds", 0, 30),
            ("minimum_remaining_lease_seconds", 0, 120),
            ("timeout_seconds", 0.1, 10),
            ("maximum_retry_elapsed_seconds", 0.2, 30),
        ):
            raw = value[name]
            if (
                isinstance(raw, bool)
                or not isinstance(raw, (int, float))
                or not math.isfinite(float(raw))
                or not lower <= float(raw) <= upper
            ):
                raise ValueError(f"RESILIENT_STATUS_PULL_{name.upper()}_INVALID")
        if float(value["minimum_remaining_lease_seconds"]) >= float(value["maximum_report_lease_seconds"]):
            raise ValueError("RESILIENT_STATUS_PULL_REMAINING_LEASE_INVALID")
        retry = value["transient_retry_delays_seconds"]
        if (
            not isinstance(retry, list)
            or not 1 <= len(retry) <= 8
            or any(
                isinstance(delay, bool)
                or not isinstance(delay, (int, float))
                or not math.isfinite(float(delay))
                or not 0.01 <= float(delay) <= 2
                for delay in retry
            )
            or sum(float(delay) for delay in retry) > 5
        ):
            raise ValueError("RESILIENT_STATUS_PULL_RETRY_DELAYS_INVALID")
        return cls(value, path)


class ResilientStatusPuller:
    def __init__(
        self,
        *,
        endpoint_url: str,
        ssl_context: ssl.SSLContext,
        contract: ResilientHostStatusContract,
        output_file: Path,
        admission_state_file: Path,
        timeout_seconds: float,
        transient_retry_delays_seconds: tuple[float, ...],
        maximum_retry_elapsed_seconds: float,
        minimum_remaining_lease_seconds: float,
        maximum_bytes: int = 2 * 1024 * 1024,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = _deadline_clock,
        wait: Callable[[float], None] = time.sleep,
        jitter: Callable[[float], float] = lambda cap: random.SystemRandom().uniform(0.0, cap),
        open_url: Callable[..., Any] | None = None,
        failure_observer: Callable[[FailureClassification, int, bool], None] | None = None,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.ssl_context = ssl_context
        self.contract = contract
        self.output_file = output_file
        self.admission_state_file = admission_state_file
        self.timeout_seconds = timeout_seconds
        self.transient_retry_delays_seconds = transient_retry_delays_seconds
        self.maximum_retry_elapsed_seconds = maximum_retry_elapsed_seconds
        self.minimum_remaining_lease_seconds = minimum_remaining_lease_seconds
        self.maximum_bytes = maximum_bytes
        self.clock = clock
        self.monotonic = monotonic
        self.wait = wait
        self.jitter = jitter
        self.open_url = open_url or _open_url
        self.failure_observer = failure_observer

    def _admit(self, value: dict[str, Any]) -> str:
        current = {
            "schema": ADMISSION_SCHEMA,
            "source_role": value["role"],
            "source_host_id": value["host_id"],
            "source_release_id": value["release_id"],
            "producer_instance_id": value["producer_instance_id"],
            "host_boot_id": value["host_boot_id"],
            "producer_sequence": value["producer_sequence"],
            "payload_sha256": value["payload_sha256"],
        }
        if not self.admission_state_file.exists():
            if self.output_file.exists():
                raise ValueError("RESILIENT_STATUS_ADMISSION_STATE_MISSING")
            atomic_write_json(self.output_file, value)
            atomic_write_json(self.admission_state_file, current)
            return "UPDATED"
        previous = read_object(self.admission_state_file, maximum_bytes=64 * 1024)
        if set(previous) != set(current) or previous.get("schema") != ADMISSION_SCHEMA:
            raise ValueError("RESILIENT_STATUS_ADMISSION_STATE_INVALID")
        for name in ("source_role", "source_host_id", "source_release_id"):
            if previous[name] != current[name]:
                raise ValueError("RESILIENT_STATUS_ADMISSION_SOURCE_MISMATCH")
        if previous["producer_instance_id"] == current["producer_instance_id"]:
            if previous["host_boot_id"] != current["host_boot_id"]:
                raise ValueError("RESILIENT_STATUS_ADMISSION_INSTANCE_BOOT_CONFLICT")
            if int(current["producer_sequence"]) < int(previous["producer_sequence"]):
                raise ValueError("RESILIENT_STATUS_SEQUENCE_REGRESSION")
            if int(current["producer_sequence"]) == int(previous["producer_sequence"]):
                if current["payload_sha256"] != previous["payload_sha256"]:
                    raise ValueError("RESILIENT_STATUS_SEQUENCE_CONFLICT")
                return "UNCHANGED"
        elif previous["host_boot_id"] == current["host_boot_id"]:
            raise ValueError("RESILIENT_STATUS_PRODUCER_INSTANCE_CONFLICT")
        atomic_write_json(self.output_file, value)
        atomic_write_json(self.admission_state_file, current)
        return "UPDATED"

    def pull(self, *, now: datetime | None = None) -> dict[str, Any]:
        request = urllib.request.Request(self.endpoint_url, headers={"Accept": "application/json"}, method="GET")
        raw, attempts = fetch_bounded_json(
            request,
            context=self.ssl_context,
            open_url=self.open_url,
            timeout_seconds=self.timeout_seconds,
            transient_retry_delays_seconds=self.transient_retry_delays_seconds,
            maximum_retry_elapsed_seconds=self.maximum_retry_elapsed_seconds,
            maximum_bytes=self.maximum_bytes,
            monotonic=self.monotonic,
            wait=self.wait,
            jitter=self.jitter,
            error_prefix="RESILIENT_STATUS",
            retryable=lambda error: classify_failure(error).retryable,
            failure_observer=self.failure_observer,
        )
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("RESILIENT_STATUS_PULL_RESPONSE_NOT_OBJECT")
        current = (now or self.clock()).astimezone(UTC)
        verified = self.contract.decode(value, now=current)
        remaining = (parse_utc(str(verified["report_lease_until"])) - current).total_seconds()
        evidence_remaining = (evidence_valid_until(verified) - current).total_seconds()
        if evidence_remaining < self.minimum_remaining_lease_seconds:
            raise ValueError("RESILIENT_STATUS_PULL_INSUFFICIENT_EVIDENCE_VALIDITY")
        disposition = self._admit(verified)
        return {
            "host_status": verified,
            "disposition": disposition,
            "pulled_at": isoformat_utc(current),
            "transport_attempt_count": attempts,
            "transport_retry_count": attempts - 1,
            "source_remaining_lease_seconds": round(remaining, 3),
            "source_remaining_evidence_seconds": round(evidence_remaining, 3),
        }


def _contract(config: PullConfig) -> ResilientHostStatusContract:
    value = config.value
    upstream = None
    if value["source_role"] == "arena":
        upstream = ResilientHostStatusContract(
            Path(value["resilient_host_status_schema_file"]),
            KeyRing({str(value["upstream_key_id"]): _public_key(Path(value["upstream_public_key_file"]))}),
            key_id=str(value["upstream_key_id"]),
            expected_role=str(value["upstream_role"]),
            expected_host_id=str(value["upstream_host_id"]),
            expected_release_id=str(value["upstream_release_id"]),
            maximum_report_lease_seconds=float(value["maximum_report_lease_seconds"]),
            maximum_clock_skew_seconds=float(value["maximum_clock_skew_seconds"]),
        )
    return ResilientHostStatusContract(
        Path(value["resilient_host_status_schema_file"]),
        KeyRing({str(value["source_key_id"]): _public_key(Path(value["source_public_key_file"]))}),
        key_id=str(value["source_key_id"]),
        expected_role=str(value["source_role"]),
        expected_host_id=str(value["source_host_id"]),
        expected_release_id=str(value["source_release_id"]),
        maximum_report_lease_seconds=float(value["maximum_report_lease_seconds"]),
        maximum_clock_skew_seconds=float(value["maximum_clock_skew_seconds"]),
        upstream_contract=upstream,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull and admit signed resilient status v3")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = PullConfig.load(args.config)
    value = config.value
    health = RecoveryHealthStore(
        state_file=Path(value["recovery_state_file"]),
        event_file=Path(value["recovery_event_file"]),
        component=str(value["component"]),
        release_id=str(value["puller_release_id"]),
    )
    try:
        context = _client_context(
            Path(value["tls_certificate"]),
            Path(value["tls_private_key"]),
            Path(value["server_ca_certificate"]),
        )
        contract = _contract(config)
        if args.check_config:
            print(json.dumps({"config": "VALID", "component": value["component"], "schema": CONFIG_SCHEMA}, sort_keys=True))
            return

        def observe_failure(classification: FailureClassification, attempt: int, exhausted: bool) -> None:
            health.record_failure(classification, attempt=attempt, exhausted=exhausted)

        result = ResilientStatusPuller(
            endpoint_url=str(value["endpoint_url"]),
            ssl_context=context,
            contract=contract,
            output_file=Path(value["output_file"]),
            admission_state_file=Path(value["admission_state_file"]),
            timeout_seconds=float(value["timeout_seconds"]),
            transient_retry_delays_seconds=tuple(float(item) for item in value["transient_retry_delays_seconds"]),
            maximum_retry_elapsed_seconds=float(value["maximum_retry_elapsed_seconds"]),
            minimum_remaining_lease_seconds=float(value["minimum_remaining_lease_seconds"]),
            failure_observer=observe_failure,
        ).pull()
        health.record_success(attempt_count=int(result["transport_attempt_count"]))
        status = {
            "schema": "cra.resilient_host_status_pull_status.v2",
            "component": value["component"],
            "status": "READY",
            "source_state": result["host_status"]["state"],
            "source_release_id": value["source_release_id"],
            "source_producer_instance_id": result["host_status"]["producer_instance_id"],
            "source_producer_sequence": result["host_status"]["producer_sequence"],
            "disposition": result["disposition"],
            "observed_at": result["pulled_at"],
            "transport_attempt_count": result["transport_attempt_count"],
            "transport_retry_count": result["transport_retry_count"],
            "source_remaining_lease_seconds": result["source_remaining_lease_seconds"],
            "source_remaining_evidence_seconds": result["source_remaining_evidence_seconds"],
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        atomic_write_json(Path(value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True))
    except Exception as error:
        if health.snapshot()["current_state"] == "READY":
            health.record_failure(classify_failure(error), attempt=1, exhausted=True)
        status = {
            "schema": "cra.resilient_host_status_pull_status.v2",
            "component": value["component"],
            "status": "SAFE_BLOCKED",
            "source_release_id": value["source_release_id"],
            "error_class": type(error).__name__,
            "observed_at": isoformat_utc(datetime.now(UTC)),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        atomic_write_json(Path(value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
