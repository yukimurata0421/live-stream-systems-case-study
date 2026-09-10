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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from cra_dell_recovery.bounded_http import MAXIMUM_TRANSPORT_ATTEMPTS, fetch_bounded_json
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.recovery_health import RecoveryHealthStore
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_dell_recovery.tls_credentials import validate_ca_certificate, validate_leaf_certificate
from cra_dell_recovery.transport_resilience import FailureClassification, classify_failure

from .host_status import STATUS_PATH, HostStatusBundle, HostStatusContract, atomic_write_json, read_object, read_regular_bytes

CONFIG_SCHEMA = "cra.no_action_host_status_pull.v1"
COMPONENTS = frozenset({"arena_dell_soak_status_pull", "cra_arena_soak_status_pull"})


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        fp.close()
        raise ValueError("HOST_STATUS_REDIRECT_FORBIDDEN")


def _open_url(request: urllib.request.Request, *, context: ssl.SSLContext, timeout: float) -> Any:
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), _NoRedirect())
    return opener.open(request, timeout=timeout)


def _deadline_clock() -> float:
    clock_boottime = getattr(time, "CLOCK_BOOTTIME", None)
    if clock_boottime is not None:
        return time.clock_gettime(clock_boottime)
    return time.monotonic()


def _public_key(path: Path) -> Ed25519PublicKey:
    value = serialization.load_pem_public_key(read_regular_bytes(path, maximum_bytes=64 * 1024))
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("HOST_STATUS_PULL_KEY_NOT_ED25519")
    return value


def _client_context(certificate: Path, private_key: Path, server_ca: Path) -> ssl.SSLContext:
    validate_leaf_certificate(certificate, "HOST_STATUS_PULL_TLS_CERTIFICATE", usage="client")
    validate_ca_certificate(server_ca, "HOST_STATUS_PULL_SERVER_CA")
    read_regular_bytes(certificate, maximum_bytes=64 * 1024)
    read_regular_bytes(private_key, maximum_bytes=64 * 1024, secret=True)
    read_regular_bytes(server_ca, maximum_bytes=64 * 1024)
    context = ssl.create_default_context(cafile=server_ca)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(certificate, private_key)
    return context


@dataclass(frozen=True)
class PullConfig:
    value: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: Path) -> PullConfig:
        value = read_object(path, maximum_bytes=64 * 1024)
        required = {
            "schema",
            "component",
            "puller_release_id",
            "endpoint_url",
            "tls_certificate",
            "tls_private_key",
            "server_ca_certificate",
            "host_status_schema_file",
            "source_key_id",
            "source_public_key_file",
            "source_role",
            "source_host_id",
            "source_release_id",
            "maximum_ttl_seconds",
            "timeout_seconds",
            "transient_retry_delays_seconds",
            "maximum_retry_elapsed_seconds",
            "output_file",
            "status_file",
            "recovery_state_file",
            "recovery_event_file",
        }
        optional = {"minimum_remaining_validity_seconds"}
        if set(value) not in {frozenset(required), frozenset(required | optional)} or value.get("schema") != CONFIG_SCHEMA:
            raise ValueError("HOST_STATUS_PULL_CONFIG_FIELDS_INVALID")
        if value.get("component") not in COMPONENTS:
            raise ValueError("HOST_STATUS_PULL_COMPONENT_INVALID")
        if value.get("source_role") not in {"dell", "arena"}:
            raise ValueError("HOST_STATUS_PULL_SOURCE_ROLE_INVALID")
        for name in (
            "puller_release_id",
            "source_key_id",
            "source_host_id",
            "source_release_id",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).strip():
                raise ValueError(f"HOST_STATUS_PULL_{name.upper()}_INVALID")
        require_runtime_release(str(value["puller_release_id"]), "HOST_STATUS_PULL_RUNTIME_RELEASE_MISMATCH")
        parsed = urllib.parse.urlparse(str(value.get("endpoint_url")))
        if parsed.scheme != "https" or parsed.path != STATUS_PATH or parsed.query or parsed.fragment or not parsed.hostname:
            raise ValueError("HOST_STATUS_PULL_ENDPOINT_INVALID")
        for name in (
            "tls_certificate",
            "tls_private_key",
            "server_ca_certificate",
            "host_status_schema_file",
            "source_public_key_file",
            "output_file",
            "status_file",
            "recovery_state_file",
            "recovery_event_file",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).startswith("/"):
                raise ValueError(f"HOST_STATUS_PULL_{name.upper()}_INVALID")
        timeout = value["timeout_seconds"]
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0.1 <= float(timeout) <= 10:
            raise ValueError("HOST_STATUS_PULL_TIMEOUT_OUT_OF_RANGE")
        ttl = value["maximum_ttl_seconds"]
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or not 5 <= float(ttl) <= 60:
            raise ValueError("HOST_STATUS_PULL_TTL_OUT_OF_RANGE")
        minimum_remaining = value.get("minimum_remaining_validity_seconds", 0)
        if (
            isinstance(minimum_remaining, bool)
            or not isinstance(minimum_remaining, (int, float))
            or not math.isfinite(float(minimum_remaining))
            or not 0 <= float(minimum_remaining) < float(ttl)
        ):
            raise ValueError("HOST_STATUS_PULL_MINIMUM_VALIDITY_OUT_OF_RANGE")
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
            raise ValueError("HOST_STATUS_PULL_RETRY_DELAYS_INVALID")
        elapsed = value["maximum_retry_elapsed_seconds"]
        if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or not 0.2 <= float(elapsed) <= 30:
            raise ValueError("HOST_STATUS_PULL_RETRY_ELAPSED_INVALID")
        return cls(value, path)


class HostStatusPuller:
    def __init__(
        self,
        *,
        endpoint_url: str,
        ssl_context: ssl.SSLContext,
        contract: HostStatusContract,
        output_file: Path,
        timeout_seconds: float,
        transient_retry_delays_seconds: tuple[float, ...],
        maximum_retry_elapsed_seconds: float,
        minimum_remaining_validity_seconds: float = 0.0,
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
        self.timeout_seconds = timeout_seconds
        self.transient_retry_delays_seconds = transient_retry_delays_seconds
        self.maximum_retry_elapsed_seconds = maximum_retry_elapsed_seconds
        self.minimum_remaining_validity_seconds = minimum_remaining_validity_seconds
        self.maximum_bytes = maximum_bytes
        self.clock = clock
        self.monotonic = monotonic
        self.wait = wait
        self.jitter = jitter
        self.open_url = open_url or _open_url
        self.failure_observer = failure_observer

    def pull(self, *, now: datetime | None = None) -> dict[str, Any]:
        request = urllib.request.Request(self.endpoint_url, headers={"Accept": "application/json"}, method="GET")
        invocation_deadline = self.monotonic() + self.maximum_retry_elapsed_seconds
        attempts = 0
        validity_retries = 0

        def observe_failure(classification: FailureClassification, attempt: int, exhausted: bool) -> None:
            if self.failure_observer is not None:
                self.failure_observer(classification, attempts + attempt, exhausted)

        while True:
            remaining = invocation_deadline - self.monotonic()
            if remaining <= 0 or attempts >= MAXIMUM_TRANSPORT_ATTEMPTS:
                raise ValueError("HOST_STATUS_PULL_VALIDITY_RETRY_BUDGET_EXHAUSTED")

            raw, request_attempts = fetch_bounded_json(
                request,
                context=self.ssl_context,
                open_url=self.open_url,
                timeout_seconds=self.timeout_seconds,
                transient_retry_delays_seconds=self.transient_retry_delays_seconds,
                maximum_retry_elapsed_seconds=remaining,
                maximum_bytes=self.maximum_bytes,
                monotonic=self.monotonic,
                wait=self.wait,
                jitter=self.jitter,
                error_prefix="HOST_STATUS",
                retryable=lambda error: classify_failure(error).retryable,
                failure_observer=observe_failure,
            )
            attempts += request_attempts
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("HOST_STATUS_PULL_RESPONSE_NOT_OBJECT")
            current = (now or self.clock()).astimezone(UTC)
            fetched: HostStatusBundle | None
            try:
                fetched = self.contract.decode(value, now=current)
            except ValueError as error:
                if str(error) != "HOST_STATUS_EXPIRED" or self.minimum_remaining_validity_seconds <= 0:
                    raise
                fetched = None
            remaining_validity = -1.0 if fetched is None else (fetched.valid_until - current).total_seconds()
            if fetched is not None and remaining_validity >= self.minimum_remaining_validity_seconds:
                break
            remaining = invocation_deadline - self.monotonic()
            retry_cap = self.transient_retry_delays_seconds[min(validity_retries, len(self.transient_retry_delays_seconds) - 1)]
            if retry_cap >= remaining or attempts >= MAXIMUM_TRANSPORT_ATTEMPTS:
                raise ValueError("HOST_STATUS_PULL_INSUFFICIENT_VALIDITY")
            validity_retries += 1
            self.wait(retry_cap)

        assert fetched is not None
        disposition = "UPDATED"
        if self.output_file.exists():
            previous = self.contract.verify_integrity(read_object(self.output_file, maximum_bytes=self.maximum_bytes))
            if fetched.sequence < previous.sequence:
                raise ValueError("HOST_STATUS_PULL_SEQUENCE_REGRESSION")
            if fetched.sequence == previous.sequence:
                if fetched.value["payload_sha256"] != previous.value["payload_sha256"]:
                    raise ValueError("HOST_STATUS_PULL_SEQUENCE_CONFLICT")
                disposition = "UNCHANGED"
        if disposition == "UPDATED":
            atomic_write_json(self.output_file, fetched.value)
        return {
            "host_status": fetched.value,
            "disposition": disposition,
            "pulled_at": isoformat_utc(current),
            "transport_attempt_count": attempts,
            "transport_retry_count": attempts - 1,
            "validity_retry_count": validity_retries,
            "source_remaining_validity_seconds": remaining_validity,
        }


def _contract(config: PullConfig) -> HostStatusContract:
    value = config.value
    return HostStatusContract(
        Path(value["host_status_schema_file"]),
        KeyRing({str(value["source_key_id"]): _public_key(Path(value["source_public_key_file"]))}),
        key_id=str(value["source_key_id"]),
        expected_role=str(value["source_role"]),
        expected_host_id=str(value["source_host_id"]),
        expected_release_id=str(value["source_release_id"]),
        maximum_ttl_seconds=float(value["maximum_ttl_seconds"]),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull and verify one signed NO_ACTION host status")
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
            print(json.dumps({"config": "VALID", "component": value["component"], "schema": value["schema"]}, sort_keys=True))
            return

        def observe_failure(classification: FailureClassification, attempt: int, exhausted: bool) -> None:
            health.record_failure(classification, attempt=attempt, exhausted=exhausted)

        result = HostStatusPuller(
            endpoint_url=str(value["endpoint_url"]),
            ssl_context=context,
            contract=contract,
            output_file=Path(value["output_file"]),
            timeout_seconds=float(value["timeout_seconds"]),
            transient_retry_delays_seconds=tuple(float(item) for item in value["transient_retry_delays_seconds"]),
            maximum_retry_elapsed_seconds=float(value["maximum_retry_elapsed_seconds"]),
            minimum_remaining_validity_seconds=float(value.get("minimum_remaining_validity_seconds", 0)),
            failure_observer=observe_failure,
        ).pull()
        health.record_success(attempt_count=int(result["transport_attempt_count"]))
        status = {
            "schema": "cra.no_action_host_status_pull_status.v1",
            "component": value["component"],
            "status": "READY",
            "puller_release_id": value["puller_release_id"],
            "source_release_id": value["source_release_id"],
            "source_sequence": result["host_status"]["sequence"],
            "disposition": result["disposition"],
            "observed_at": result["pulled_at"],
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "transport_attempt_count": result["transport_attempt_count"],
            "transport_retry_count": result["transport_retry_count"],
            "validity_retry_count": result["validity_retry_count"],
            "source_remaining_validity_seconds": round(float(result["source_remaining_validity_seconds"]), 3),
        }
        atomic_write_json(Path(value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True))
    except Exception as error:
        if health.snapshot()["current_state"] == "READY":
            health.record_failure(classify_failure(error), attempt=1, exhausted=True)
        status = {
            "schema": "cra.no_action_host_status_pull_status.v1",
            "component": value["component"],
            "status": "SAFE_BLOCKED",
            "puller_release_id": value["puller_release_id"],
            "source_release_id": value["source_release_id"],
            "error_class": type(error).__name__,
            "observed_at": isoformat_utc(utc_now()),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        atomic_write_json(Path(value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
