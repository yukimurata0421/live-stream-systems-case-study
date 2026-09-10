from __future__ import annotations

import email.message
import json
import os
import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing, Signer
from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak.host_status import (
    HostStatusContract,
    atomic_write_json,
    read_server_resource_status,
    served_host_status_bytes,
    write_server_resource_status,
)
from cra_no_action_soak.host_status_pull import HostStatusPuller

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / "contracts/cra_no_action_soak/host_status.v1.schema.json"


def _signed_dell_status(
    signer: Signer,
    *,
    sequence: int = 1,
    observed_at: datetime | None = None,
    validity_seconds: float = 30,
    release_id: str = "dell-observation-0123456789ab",
) -> dict[str, Any]:
    observed = observed_at or datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    return signer.sign(
        {
            "schema": "cra.no_action_host_status.v1",
            "role": "dell",
            "host_id": "dell-stream-runtime",
            "release_id": release_id,
            "sequence": sequence,
            "observed_at": isoformat_utc(observed),
            "valid_until": isoformat_utc(observed + timedelta(seconds=validity_seconds)),
            "host_boot_id": "11111111-1111-4111-8111-111111111111",
            "identity": {
                "release_manifest_sha256": "1" * 64,
                "runtime_manifest_sha256": "2" * 64,
                "configuration_set_sha256": "3" * 64,
                "maintenance_restart_policy_sha256": "4" * 64,
            },
            "resources": {
                "disk_free_bytes": 10_000_000_000,
                "resident_memory_bytes": 10_000_000,
                "open_fd_count": 10,
                "resource_limit_breach_count": 0,
            },
            "services": {
                "dell_observation_server": {
                    "unit": "dell-observation-server@dell-observation-0123456789ab.service",
                    "active_state": "active",
                    "sub_state": "running",
                    "restart_count": 0,
                    "invocation_id": "a" * 32,
                }
            },
            "service_failed_invocation_count": {"dell_observation_server": 0},
            "credential_minimum_remaining_seconds": {"dell_target_observer": 86_400},
            "payload": {
                "observation": {
                    "schema": "cra_dell_recovery.observation_bundle.v1",
                    "source_release_id": release_id,
                    "observation_id": "dell-observation-1",
                    "observation_revision": "5" * 64,
                    "observation_sequence": sequence,
                    "observed_at": isoformat_utc(observed),
                    "valid_until": isoformat_utc(observed + timedelta(seconds=validity_seconds)),
                    "payload_sha256": "6" * 64,
                },
                "target_snapshot_valid": True,
                "target_identity_sha256": "7" * 64,
                "control_capability_count": 0,
                "physical_effect_count": 0,
                "effect_counters": {
                    "effect_request_count": 0,
                    "effect_boundary_count": 0,
                    "effect_scope_count": 0,
                    "raw_outcome_unknown_count": 0,
                    "unresolved_scope_count": 0,
                    "reconciliation_count": 0,
                    "duplicate_scope_count": 0,
                    "oldest_unresolved_age_seconds": 0,
                },
            },
            "key_id": signer.key_id,
        }
    )


def _contract(private: Ed25519PrivateKey) -> HostStatusContract:
    return HostStatusContract(
        SCHEMA,
        KeyRing({"dell-host-status-key": private.public_key()}),
        key_id="dell-host-status-key",
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
        maximum_ttl_seconds=30,
    )


def test_host_status_contract_verifies_signature_identity_and_freshness() -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signer = Signer("dell-host-status-key", private)
    observed = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    value = _signed_dell_status(signer, observed_at=observed)
    contract = _contract(private)

    assert contract.decode(value, now=observed + timedelta(seconds=1)).sequence == 1
    with pytest.raises(ValueError, match="HOST_STATUS_EXPIRED"):
        contract.decode(value, now=observed + timedelta(seconds=30))
    with pytest.raises(ValueError, match="HOST_STATUS_RELEASE_ID_MISMATCH"):
        wrong = _signed_dell_status(signer, release_id="dell-observation-fedcba987654", observed_at=observed)
        contract.decode(wrong, now=observed + timedelta(seconds=1))
    tampered = dict(value)
    tampered["sequence"] = 2
    with pytest.raises(Exception, match="payload digest mismatch"):
        contract.verify_integrity(tampered)


def test_served_status_is_fail_closed_on_expiry_without_requiring_server_signing_key(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    observed = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    value = _signed_dell_status(Signer("dell-host-status-key", private), observed_at=observed)
    path = tmp_path / "status.json"
    atomic_write_json(path, value)

    assert (
        json.loads(
            served_host_status_bytes(
                path,
                expected_role="dell",
                expected_host_id="dell-stream-runtime",
                expected_release_id="dell-observation-0123456789ab",
                maximum_bytes=2 * 1024 * 1024,
                now=observed + timedelta(seconds=1),
            )
        )
        == value
    )
    with pytest.raises(ValueError, match="HOST_STATUS_FILE_EXPIRED"):
        served_host_status_bytes(
            path,
            expected_role="dell",
            expected_host_id="dell-stream-runtime",
            expected_release_id="dell-observation-0123456789ab",
            maximum_bytes=2 * 1024 * 1024,
            now=observed + timedelta(seconds=30),
        )


def test_server_self_resource_status_is_exact_release_bound_and_fresh(tmp_path: Path) -> None:
    observed = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    path = tmp_path / "server-resource-status.json"
    previous_umask = os.umask(0o077)
    try:
        value = write_server_resource_status(
            path,
            role="dell",
            host_id="dell-stream-runtime",
            release_id="dell-observation-0123456789ab",
            now=observed,
        )
    finally:
        os.umask(previous_umask)

    assert path.stat().st_mode & 0o777 == 0o640
    assert value["resident_memory_bytes"] > 0
    assert value["open_fd_count"] > 0
    assert read_server_resource_status(
        path,
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
        maximum_age_seconds=30,
        now=observed + timedelta(seconds=1),
    ) == (value["resident_memory_bytes"], value["open_fd_count"])
    assert read_server_resource_status(
        path,
        expected_role="dell",
        expected_host_id="dell-stream-runtime",
        expected_release_id="dell-observation-0123456789ab",
        maximum_age_seconds=30,
        now=observed - timedelta(milliseconds=250),
    ) == (value["resident_memory_bytes"], value["open_fd_count"])
    with pytest.raises(ValueError, match="SERVER_RESOURCE_STATUS_FROM_FUTURE"):
        read_server_resource_status(
            path,
            expected_role="dell",
            expected_host_id="dell-stream-runtime",
            expected_release_id="dell-observation-0123456789ab",
            maximum_age_seconds=30,
            now=observed - timedelta(seconds=2),
        )
    with pytest.raises(ValueError, match="SERVER_RESOURCE_STATUS_STALE"):
        read_server_resource_status(
            path,
            expected_role="dell",
            expected_host_id="dell-stream-runtime",
            expected_release_id="dell-observation-0123456789ab",
            maximum_age_seconds=30,
            now=observed + timedelta(seconds=31),
        )


class _Response:
    def __init__(self, value: dict[str, Any]) -> None:
        self.body = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
        headers = email.message.Message()
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(self.body))
        self.headers = headers

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, maximum: int) -> bytes:
        assert maximum > len(self.body)
        return self.body


class _Clock:
    def __init__(self, wall: datetime) -> None:
        self.wall = wall
        self.elapsed = 0.0
        self.waits: list[float] = []

    def now(self) -> datetime:
        return self.wall + timedelta(seconds=self.elapsed)

    def monotonic(self) -> float:
        return self.elapsed

    def wait(self, delay: float) -> None:
        self.waits.append(delay)
        self.elapsed += delay


def test_host_status_puller_enforces_monotonic_sequence_and_conflict(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signer = Signer("dell-host-status-key", private)
    observed = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    current = [_signed_dell_status(signer, sequence=2, observed_at=observed)]
    puller = HostStatusPuller(
        endpoint_url="https://dell.invalid/v1/no-action-soak-status/latest",
        ssl_context=ssl.create_default_context(),
        contract=_contract(private),
        output_file=tmp_path / "host-status.json",
        timeout_seconds=1,
        transient_retry_delays_seconds=(0.1,),
        maximum_retry_elapsed_seconds=2,
        monotonic=lambda: 0.0,
        wait=lambda _delay: None,
        jitter=lambda cap: cap,
        open_url=lambda *_args, **_kwargs: _Response(current[0]),
    )

    assert puller.pull(now=observed + timedelta(seconds=1))["disposition"] == "UPDATED"
    assert puller.pull(now=observed + timedelta(seconds=1))["disposition"] == "UNCHANGED"
    conflict = dict(current[0])
    conflict["resources"] = {**dict(conflict["resources"]), "disk_free_bytes": 9_000_000_000}
    current[0] = signer.sign(conflict)
    with pytest.raises(ValueError, match="HOST_STATUS_PULL_SEQUENCE_CONFLICT"):
        puller.pull(now=observed + timedelta(seconds=1))
    current[0] = _signed_dell_status(signer, sequence=1, observed_at=observed)
    with pytest.raises(ValueError, match="HOST_STATUS_PULL_SEQUENCE_REGRESSION"):
        puller.pull(now=observed + timedelta(seconds=1))


def test_host_status_puller_refetches_until_source_has_downstream_validity_headroom(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signer = Signer("dell-host-status-key", private)
    current = datetime(2026, 9, 2, 12, 0, 20, tzinfo=UTC)
    clock = _Clock(current)
    responses = iter(
        (
            _signed_dell_status(signer, sequence=1, observed_at=current - timedelta(seconds=20)),
            _signed_dell_status(signer, sequence=2, observed_at=current),
        )
    )
    puller = HostStatusPuller(
        endpoint_url="https://dell.invalid/v1/no-action-soak-status/latest",
        ssl_context=ssl.create_default_context(),
        contract=_contract(private),
        output_file=tmp_path / "host-status.json",
        timeout_seconds=1,
        transient_retry_delays_seconds=(1,),
        maximum_retry_elapsed_seconds=5,
        minimum_remaining_validity_seconds=12,
        clock=clock.now,
        monotonic=clock.monotonic,
        wait=clock.wait,
        jitter=lambda cap: cap,
        open_url=lambda *_args, **_kwargs: _Response(next(responses)),
    )

    result = puller.pull()

    assert result["host_status"]["sequence"] == 2
    assert result["transport_attempt_count"] == 2
    assert result["transport_retry_count"] == 1
    assert result["validity_retry_count"] == 1
    assert result["source_remaining_validity_seconds"] == pytest.approx(29)
    assert clock.waits == [1]
    assert json.loads((tmp_path / "host-status.json").read_text(encoding="utf-8"))["sequence"] == 2


def test_host_status_puller_fails_closed_without_persisting_insufficient_validity(tmp_path: Path) -> None:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signer = Signer("dell-host-status-key", private)
    current = datetime(2026, 9, 2, 12, 0, 20, tzinfo=UTC)
    clock = _Clock(current)
    sequence = 0

    def response(*_args: object, **_kwargs: object) -> _Response:
        nonlocal sequence
        sequence += 1
        return _Response(
            _signed_dell_status(
                signer,
                sequence=sequence,
                observed_at=clock.now() - timedelta(seconds=20),
            )
        )

    output = tmp_path / "host-status.json"
    puller = HostStatusPuller(
        endpoint_url="https://dell.invalid/v1/no-action-soak-status/latest",
        ssl_context=ssl.create_default_context(),
        contract=_contract(private),
        output_file=output,
        timeout_seconds=1,
        transient_retry_delays_seconds=(1,),
        maximum_retry_elapsed_seconds=1.5,
        minimum_remaining_validity_seconds=12,
        clock=clock.now,
        monotonic=clock.monotonic,
        wait=clock.wait,
        jitter=lambda cap: cap,
        open_url=response,
    )

    with pytest.raises(ValueError, match="HOST_STATUS_PULL_INSUFFICIENT_VALIDITY"):
        puller.pull()

    assert sequence == 2
    assert clock.waits == [1]
    assert not output.exists()
