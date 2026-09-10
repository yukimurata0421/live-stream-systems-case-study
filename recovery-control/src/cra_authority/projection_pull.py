from __future__ import annotations

import argparse
import json
import math
import os
import random
import ssl
import stat
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

from cra_authority.json_input import load_object
from cra_authority.monitoring_evidence import MonitoringEvidenceContract
from cra_dell_recovery.bounded_http import fetch_bounded_json
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.recovery_health import RecoveryHealthStore
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_dell_recovery.tls_credentials import validate_ca_certificate, validate_leaf_certificate
from cra_dell_recovery.transport_resilience import FailureClassification, classify_failure, retryable_transport_error

CONFIG_SCHEMA = "cra.monitoring_projection_pull.v3"
LEGACY_CONFIG_SCHEMA = "cra.monitoring_projection_pull.v2"
PROJECTION_PATH = "/v1/monitoring-evidence/latest"


def _deadline_clock() -> float:
    """Return elapsed time including host suspend whenever Linux provides it."""

    clock_boottime = getattr(time, "CLOCK_BOOTTIME", None)
    if clock_boottime is not None:
        return time.clock_gettime(clock_boottime)
    return time.monotonic()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        fp.close()
        raise ValueError("MONITORING_PROJECTION_REDIRECT_FORBIDDEN")


def _open_projection_url(request: urllib.request.Request, *, context: ssl.SSLContext, timeout: float) -> Any:
    # Refuse before issuing the redirected request, including HTTPS downgrade.
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), _NoRedirect())
    return opener.open(request, timeout=timeout)


def _read_regular_bytes(path: Path, *, maximum_bytes: int, sensitive: bool) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("PROJECTION_PULL_INPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & (0o077 if sensitive else 0o022):
            raise ValueError("PROJECTION_PULL_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("PROJECTION_PULL_INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("PROJECTION_PULL_INPUT_TOO_LARGE")
    return raw


def _read_json(path: Path, *, maximum_bytes: int) -> dict[str, Any]:
    raw = _read_regular_bytes(path, maximum_bytes=maximum_bytes, sensitive=False)
    return load_object(raw, maximum_bytes=maximum_bytes)


def _validate_tls_path(path: Path, code: str, *, sensitive: bool) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise ValueError(f"{code}_UNAVAILABLE") from error
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{code}_NOT_REGULAR_FILE")
    if stat.S_IMODE(metadata.st_mode) & (0o077 if sensitive else 0o022):
        raise ValueError(f"{code}_PERMISSIONS_UNSAFE")


def _public_key(path: Path) -> Ed25519PublicKey:
    raw = _read_regular_bytes(path, maximum_bytes=64 * 1024, sensitive=False)
    value = serialization.load_pem_public_key(raw)
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("MONITORING_VERIFICATION_KEY_NOT_ED25519")
    return value


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


@dataclass(frozen=True)
class ProjectionPullConfig:
    value: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: Path) -> ProjectionPullConfig:
        value = _read_json(path, maximum_bytes=64 * 1024)
        required = {
            "schema",
            "puller_release_id",
            "endpoint_url",
            "tls_certificate",
            "tls_private_key",
            "server_ca_certificate",
            "projection_schema_file",
            "monitoring_key_id",
            "monitoring_public_key_file",
            "source_instance_id",
            "source_release_id",
            "maximum_ttl_seconds",
            "maximum_check_age_seconds",
            "timeout_seconds",
            "transient_retry_delays_seconds",
            "projection_file",
            "status_file",
        }
        if value.get("schema") == CONFIG_SCHEMA:
            required |= {
                "maximum_retry_elapsed_seconds",
                "recovery_state_file",
                "recovery_event_file",
            }
        if set(value) != required:
            raise ValueError("PROJECTION_PULL_CONFIG_FIELDS_NOT_EXACT")
        if value["schema"] not in {CONFIG_SCHEMA, LEGACY_CONFIG_SCHEMA}:
            raise ValueError("PROJECTION_PULL_CONFIG_SCHEMA_UNSUPPORTED")
        parsed = urllib.parse.urlparse(str(value["endpoint_url"]))
        if parsed.scheme != "https" or parsed.path != PROJECTION_PATH or parsed.query or parsed.fragment:
            raise ValueError("PROJECTION_PULL_ENDPOINT_INVALID")
        if not parsed.hostname:
            raise ValueError("PROJECTION_PULL_ENDPOINT_HOST_MISSING")
        if not 0.1 <= float(value["timeout_seconds"]) <= 10.0:
            raise ValueError("PROJECTION_PULL_TIMEOUT_OUT_OF_RANGE")
        retry_delays = value["transient_retry_delays_seconds"]
        if (
            not isinstance(retry_delays, list)
            or not 1 <= len(retry_delays) <= 8
            or any(
                isinstance(delay, bool) or not isinstance(delay, (int, float)) or not 0.01 <= float(delay) <= 2.0 for delay in retry_delays
            )
            or sum(float(delay) for delay in retry_delays) > 3.0
        ):
            raise ValueError("PROJECTION_PULL_RETRY_DELAYS_INVALID")
        if value["schema"] == CONFIG_SCHEMA:
            maximum_elapsed = value["maximum_retry_elapsed_seconds"]
            if (
                isinstance(maximum_elapsed, bool)
                or not isinstance(maximum_elapsed, (int, float))
                or not 0.2 <= float(maximum_elapsed) <= 30.0
            ):
                raise ValueError("PROJECTION_PULL_RETRY_ELAPSED_INVALID")
        if not 1.0 <= float(value["maximum_ttl_seconds"]) <= 60.0:
            raise ValueError("PROJECTION_PULL_TTL_OUT_OF_RANGE")
        if not 1.0 <= float(value["maximum_check_age_seconds"]) <= 300.0:
            raise ValueError("PROJECTION_PULL_CHECK_AGE_OUT_OF_RANGE")
        for name in ("puller_release_id", "monitoring_key_id", "source_instance_id", "source_release_id"):
            if not str(value[name]).strip():
                raise ValueError(f"PROJECTION_PULL_{name.upper()}_MISSING")
        require_runtime_release(value["puller_release_id"], "PROJECTION_PULL_RUNTIME_RELEASE_MISMATCH")
        return cls(value, path)


def _client_context(certificate: Path, private_key: Path, server_ca: Path) -> ssl.SSLContext:
    _validate_tls_path(certificate, "PROJECTION_PULL_TLS_CERTIFICATE", sensitive=False)
    _validate_tls_path(private_key, "PROJECTION_PULL_TLS_PRIVATE_KEY", sensitive=True)
    _validate_tls_path(server_ca, "PROJECTION_PULL_SERVER_CA", sensitive=False)
    validate_leaf_certificate(certificate, "PROJECTION_PULL_TLS_CERTIFICATE", usage="client")
    validate_ca_certificate(server_ca, "PROJECTION_PULL_SERVER_CA")
    context = ssl.create_default_context(cafile=server_ca)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(certificate, private_key)
    return context


class ProjectionPuller:
    """mTLS pull, signature gate and atomic inbox update; no control capability."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        ssl_context: ssl.SSLContext,
        contract: MonitoringEvidenceContract,
        projection_file: Path,
        timeout_seconds: float = 2.0,
        transient_retry_delays_seconds: tuple[float, ...] = (0.1, 0.2, 0.4, 0.8),
        maximum_bytes: int = 2 * 1024 * 1024,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        wait: Callable[[float], None] = time.sleep,
        open_url: Callable[..., Any] | None = None,
        monotonic: Callable[[], float] = _deadline_clock,
        maximum_retry_elapsed_seconds: float = 15.0,
        jitter: Callable[[float], float] = lambda cap: random.SystemRandom().uniform(0.0, cap),
        failure_observer: Callable[[FailureClassification, int, bool], None] | None = None,
    ) -> None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not 0.1 <= timeout_seconds <= 10:
            raise ValueError("PROJECTION_PULL_TIMEOUT_OUT_OF_RANGE")
        if (
            len(transient_retry_delays_seconds) > 8
            or any(
                isinstance(d, bool) or not isinstance(d, (int, float)) or not math.isfinite(d) or not 0.01 <= d <= 2
                for d in transient_retry_delays_seconds
            )
            or sum(transient_retry_delays_seconds) > 3
        ):
            raise ValueError("PROJECTION_PULL_RETRY_DELAYS_INVALID")
        if isinstance(maximum_bytes, bool) or not isinstance(maximum_bytes, int) or not 1 <= maximum_bytes <= 2 * 1024 * 1024:
            raise ValueError("PROJECTION_PULL_MAXIMUM_BYTES_INVALID")
        if (
            isinstance(maximum_retry_elapsed_seconds, bool)
            or not isinstance(maximum_retry_elapsed_seconds, (int, float))
            or not math.isfinite(maximum_retry_elapsed_seconds)
            or not 0.2 <= maximum_retry_elapsed_seconds <= 30
        ):
            raise ValueError("PROJECTION_PULL_RETRY_ELAPSED_INVALID")
        self.endpoint_url = endpoint_url
        self.ssl_context = ssl_context
        self.contract = contract
        self.projection_file = projection_file
        self.timeout_seconds = timeout_seconds
        self.transient_retry_delays_seconds = transient_retry_delays_seconds
        self.maximum_bytes = maximum_bytes
        self.clock = clock
        self.wait = wait
        self.open_url = open_url or _open_projection_url
        self.monotonic = monotonic
        self.maximum_retry_elapsed_seconds = maximum_retry_elapsed_seconds
        self.jitter = jitter
        self.failure_observer = failure_observer

    @staticmethod
    def _transient_transport_error(error: BaseException) -> bool:
        return retryable_transport_error(error)

    def _fetch_body(self, request: urllib.request.Request) -> tuple[bytes, int]:
        return fetch_bounded_json(
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
            error_prefix="MONITORING_PROJECTION",
            retryable=self._transient_transport_error,
            failure_observer=self.failure_observer,
        )

    def pull(self, *, now: datetime | None = None) -> dict[str, Any]:
        request = urllib.request.Request(
            self.endpoint_url,
            headers={"Accept": "application/json"},
            method="GET",
        )
        body, transport_attempt_count = self._fetch_body(request)
        raw = load_object(body, maximum_bytes=self.maximum_bytes)
        # The response can consume a meaningful share of the short raw
        # evidence lifetime. Never admit a projection using request-start
        # time after it has already expired in the CRA failure domain.
        current = (now or self.clock()).astimezone(UTC)
        fetched = self.contract.decode(raw, now=current)
        disposition = "UPDATED"
        if self.projection_file.exists():
            previous_raw = _read_json(self.projection_file, maximum_bytes=self.maximum_bytes)
            previous = self.contract.verify_integrity(previous_raw)
            if fetched.sequence < previous.sequence:
                raise ValueError("MONITORING_PROJECTION_INBOX_SEQUENCE_REGRESSION")
            if fetched.sequence == previous.sequence:
                if fetched.value["payload_sha256"] != previous.value["payload_sha256"]:
                    raise ValueError("MONITORING_PROJECTION_INBOX_SEQUENCE_CONFLICT")
                disposition = "UNCHANGED"
        if disposition == "UPDATED":
            _atomic_json(self.projection_file, fetched.value)
        return {
            "projection": fetched.value,
            "disposition": disposition,
            "pulled_at": isoformat_utc(current),
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "transport_attempt_count": transport_attempt_count,
            "transport_retry_count": transport_attempt_count - 1,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="mTLS puller for signed Monitoring v4 CRA projections")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = ProjectionPullConfig.load(args.config)
    value = config.value
    health: RecoveryHealthStore | None = None
    if value["schema"] == CONFIG_SCHEMA:
        health = RecoveryHealthStore(
            state_file=Path(value["recovery_state_file"]),
            event_file=Path(value["recovery_event_file"]),
            component="cra_projection_pull",
            release_id=str(value["puller_release_id"]),
        )
    try:
        context = _client_context(
            Path(value["tls_certificate"]),
            Path(value["tls_private_key"]),
            Path(value["server_ca_certificate"]),
        )
        contract = MonitoringEvidenceContract(
            Path(value["projection_schema_file"]),
            KeyRing({str(value["monitoring_key_id"]): _public_key(Path(value["monitoring_public_key_file"]))}),
            allowed_sources={str(value["source_instance_id"]): str(value["source_release_id"])},
            maximum_ttl_seconds=float(value["maximum_ttl_seconds"]),
            maximum_check_age_seconds=float(value["maximum_check_age_seconds"]),
        )
        if args.check_config:
            print(json.dumps({"config": "VALID", "schema": value["schema"]}, sort_keys=True))
            return

        def observe_failure(classification: FailureClassification, attempt: int, exhausted: bool) -> None:
            if health is not None:
                health.record_failure(classification, attempt=attempt, exhausted=exhausted)

        result = ProjectionPuller(
            endpoint_url=str(value["endpoint_url"]),
            ssl_context=context,
            contract=contract,
            projection_file=Path(value["projection_file"]),
            timeout_seconds=float(value["timeout_seconds"]),
            transient_retry_delays_seconds=tuple(float(item) for item in value["transient_retry_delays_seconds"]),
            maximum_retry_elapsed_seconds=float(value.get("maximum_retry_elapsed_seconds", 15.0)),
            failure_observer=observe_failure,
        ).pull()
        if health is not None:
            health.record_success(attempt_count=int(result["transport_attempt_count"]))
        status = {
            "schema": "cra.monitoring_projection_pull_status.v1",
            "status": "READY",
            "puller_release_id": value["puller_release_id"],
            "projection_id": result["projection"]["projection_id"],
            "observation_sequence": result["projection"]["observation_sequence"],
            "disposition": result["disposition"],
            "observed_at": result["pulled_at"],
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "transport_attempt_count": result["transport_attempt_count"],
            "transport_retry_count": result["transport_retry_count"],
        }
        _atomic_json(Path(value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True))
    except Exception as error:
        if health is not None and health.snapshot()["current_state"] == "READY":
            health.record_failure(classify_failure(error), attempt=1, exhausted=True)
        status = {
            "schema": "cra.monitoring_projection_pull_status.v1",
            "status": "SAFE_BLOCKED",
            "puller_release_id": value["puller_release_id"],
            "error_class": type(error).__name__,
            "observed_at": isoformat_utc(utc_now()),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        _atomic_json(Path(value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
