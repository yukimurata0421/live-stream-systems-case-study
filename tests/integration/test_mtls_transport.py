from __future__ import annotations

import datetime
import ipaddress
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_authority.monitoring_evidence import MonitoringEvidenceContract
from cra_authority.projection_pull import ProjectionPuller
from cra_authority.reconciliation import CentralReconciler
from cra_authority.transport import SignedHTTPSClient
from cra_dell_recovery.canonical import KeyRing, Signer
from cra_dell_recovery.models import MonitoringReadiness
from cra_dell_recovery.observation import OBSERVATION_PATH, DellObservationContract
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_no_action_soak.host_status import STATUS_PATH
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.maintenance_snapshot import ShadowMaintenanceSnapshotCache
from dell_recovery_agent.observation_server import DellObservationHTTPSServer
from dell_recovery_agent.server import ShadowAgentHTTPServer
from monitoring_projection.dell_observation_pull import DellObservationPuller
from monitoring_projection.server import PROJECTION_PATH, ProjectionHTTPSServer

ROOT = Path(__file__).resolve().parents[2]


def issue_certificate(
    *,
    common_name: str,
    ca_certificate: x509.Certificate,
    ca_key: rsa.RSAPrivateKey,
    usage: ExtendedKeyUsageOID,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca_certificate.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return key, certificate


def write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )


def write_certificate(path: Path, certificate: x509.Certificate) -> None:
    path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))


def mtls_contexts(tmp_path: Path) -> tuple[ssl.SSLContext, ssl.SSLContext, ssl.SSLContext]:
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "CRA Phase 3 runtime test CA")])
    now = datetime.datetime.now(datetime.UTC)
    ca_certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    server_key, server_certificate = issue_certificate(
        common_name="fake-dell-agent",
        ca_certificate=ca_certificate,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.SERVER_AUTH,
    )
    client_key, client_certificate = issue_certificate(
        common_name="fake-cra",
        ca_certificate=ca_certificate,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )
    ca_path = tmp_path / "runtime-test-ca.pem"
    server_key_path = tmp_path / "runtime-server-key.pem"
    server_cert_path = tmp_path / "runtime-server.pem"
    client_key_path = tmp_path / "runtime-client-key.pem"
    client_cert_path = tmp_path / "runtime-client.pem"
    write_certificate(ca_path, ca_certificate)
    write_key(server_key_path, server_key)
    write_certificate(server_cert_path, server_certificate)
    write_key(client_key_path, client_key)
    write_certificate(client_cert_path, client_certificate)

    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.minimum_version = ssl.TLSVersion.TLSv1_3
    server_context.load_cert_chain(server_cert_path, server_key_path)
    server_context.load_verify_locations(cafile=ca_path)
    server_context.verify_mode = ssl.CERT_REQUIRED

    client_context = ssl.create_default_context(cafile=ca_path)
    client_context.minimum_version = ssl.TLSVersion.TLSv1_3
    client_context.load_cert_chain(client_cert_path, client_key_path)

    missing_client_certificate = ssl.create_default_context(cafile=ca_path)
    missing_client_certificate.minimum_version = ssl.TLSVersion.TLSv1_3
    return server_context, client_context, missing_client_certificate


def test_signed_command_over_mutual_tls_vertical_slice(environment: object, tmp_path: Path) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    server_context, client_context, _ = mtls_contexts(tmp_path)
    with ShadowAgentHTTPServer(service, server_context) as server:
        client = SignedHTTPSClient(server.base_url, client_context)
        receipt = client.post_command(command)
        status = client.get_status(str(command["command_id"]))
    environment.central_codec.decode(receipt)  # type: ignore[attr-defined]
    environment.central_codec.decode(status)  # type: ignore[attr-defined]
    assert receipt["disposition"] == "ACCEPTED"
    assert status["attempt_count"] == 1
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_mtls_endpoint_rejects_client_without_certificate(environment: object, tmp_path: Path) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    server_context, _, no_client_certificate = mtls_contexts(tmp_path)
    with ShadowAgentHTTPServer(service, server_context) as server:
        client = SignedHTTPSClient(server.base_url, no_client_certificate)
        with pytest.raises((urllib.error.URLError, ssl.SSLError, ConnectionError)):
            client.post_command(command)
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def _monitoring_projection(signer: Signer, target: object, *, sequence: int, reason: str = "tcp_stall") -> dict[str, object]:
    now = utc_now()
    observed = now - datetime.timedelta(seconds=1)
    value = {
        "schema": "monitoring_v4.evidence_projection.v1",
        "projection_id": f"projection-{sequence}-{reason}",
        "target_id": "stream-target",
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "monitoring-release-a",
        "monitoring_cycle_id": f"cycle-{sequence}",
        "observation_revision": f"revision-{sequence}",
        "observation_sequence": sequence,
        "observed_at": isoformat_utc(observed),
        "issued_at": isoformat_utc(now),
        "expires_at": isoformat_utc(now + datetime.timedelta(seconds=45)),
        "readiness": {
            "input_fresh": True,
            "parity_clean": True,
            "projection_clean": True,
            "source_ready": True,
        },
        "incident": {
            "incident_id": "incident-a",
            "source_episode_id": "episode-a",
            "domain": "network_transport",
            "state": "CONFIRMED",
            "reason_codes": [reason],
        },
        "observed_target": target.to_dict(),  # type: ignore[attr-defined]
        "checks": {
            "tcp_stall": {
                "status": "CONFIRMED",
                "observed_at": isoformat_utc(observed),
                "evidence_ref": f"arena/check/{sequence}",
            }
        },
        "measurements": [],
        "evidence_refs": [f"arena/cycle/{sequence}"],
        "key_id": signer.key_id,
    }
    return signer.sign(value)


def test_monitoring_projection_mtls_pull_verifies_signature_and_monotonic_inbox(
    environment: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("monitoring_projection.server.SERVER_RESOURCE_REFRESH_SECONDS", 0.01)
    monitoring_private = ed25519.Ed25519PrivateKey.from_private_bytes(bytes(range(81, 113)))
    signer = Signer("monitoring-key-a", monitoring_private)
    contract = MonitoringEvidenceContract(
        ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        KeyRing({"monitoring-key-a": monitoring_private.public_key()}),
        allowed_sources={"arena-monitoring-v4": "monitoring-release-a"},
    )
    served = tmp_path / "served-projection.json"
    served.write_text(json.dumps(_monitoring_projection(signer, environment.target, sequence=1)), encoding="utf-8")  # type: ignore[attr-defined]
    os.chmod(served, 0o600)
    inbox = tmp_path / "cra-inbox.json"
    resource_status = tmp_path / "server-resource-status.json"
    server_context, client_context, no_client_certificate = mtls_contexts(tmp_path)
    with ProjectionHTTPSServer(
        served,
        server_context,
        host_status_file=tmp_path / "host-status.json",
        host_status_host_id="arena-monitoring-facts",
        host_status_release_id="arena-cra-projection-release-a",
        server_resource_status_file=resource_status,
    ) as server:
        initial_resource_time = json.loads(resource_status.read_text(encoding="utf-8"))["observed_at"]
        deadline = time.monotonic() + 1
        while json.loads(resource_status.read_text(encoding="utf-8"))["observed_at"] == initial_resource_time:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        with pytest.raises(urllib.error.HTTPError) as unavailable_status:
            urllib.request.urlopen(f"{server.base_url}{STATUS_PATH}", context=client_context, timeout=2)
        try:
            assert unavailable_status.value.code == HTTPStatus.SERVICE_UNAVAILABLE
            assert unavailable_status.value.headers["Retry-After"] == "1"
        finally:
            unavailable_status.value.close()

        puller = ProjectionPuller(
            endpoint_url=f"{server.base_url}{PROJECTION_PATH}",
            ssl_context=client_context,
            contract=contract,
            projection_file=inbox,
        )
        first = puller.pull()
        assert first["disposition"] == "UPDATED"
        resources = json.loads(resource_status.read_text(encoding="utf-8"))
        assert resources["role"] == "arena"
        assert resources["release_id"] == "arena-cra-projection-release-a"
        assert resources["resident_memory_bytes"] > 0
        assert resources["open_fd_count"] > 0
        assert resource_status.stat().st_mode & 0o777 == 0o640
        assert puller.pull()["disposition"] == "UNCHANGED"
        served.write_text(json.dumps(_monitoring_projection(signer, environment.target, sequence=2)), encoding="utf-8")  # type: ignore[attr-defined]
        os.chmod(served, 0o600)
        assert puller.pull()["disposition"] == "UPDATED"
        served.write_text(json.dumps(_monitoring_projection(signer, environment.target, sequence=1)), encoding="utf-8")  # type: ignore[attr-defined]
        os.chmod(served, 0o600)
        with pytest.raises(ValueError, match="MONITORING_PROJECTION_INBOX_SEQUENCE_REGRESSION"):
            puller.pull()
        unauthenticated = ProjectionPuller(
            endpoint_url=f"{server.base_url}{PROJECTION_PATH}",
            ssl_context=no_client_certificate,
            contract=contract,
            projection_file=tmp_path / "unauthenticated.json",
        )
        with pytest.raises((urllib.error.URLError, ssl.SSLError, ConnectionError)):
            unauthenticated.pull()

    assert json.loads(inbox.read_text(encoding="utf-8"))["observation_sequence"] == 2
    assert not hasattr(puller, "create_command")
    assert not hasattr(puller, "perform_effect")


def test_projection_server_rotates_tls_context_without_restarting_listener(tmp_path: Path) -> None:
    first_directory = tmp_path / "first-credentials"
    second_directory = tmp_path / "second-credentials"
    first_directory.mkdir()
    second_directory.mkdir()
    first_server, first_client, _ = mtls_contexts(first_directory)
    second_server, second_client, _ = mtls_contexts(second_directory)
    served = tmp_path / "served-projection.json"
    served.write_text(json.dumps({"schema": "monitoring_v4.evidence_projection.v1"}), encoding="utf-8")
    os.chmod(served, 0o600)

    with ProjectionHTTPSServer(served, first_server) as server:
        endpoint = f"{server.base_url}{PROJECTION_PATH}"
        with urllib.request.urlopen(endpoint, context=first_client, timeout=2) as response:
            assert response.status == HTTPStatus.OK
        assert server.reload_tls(second_server) == 2
        assert server.tls_generation == 2
        with pytest.raises((urllib.error.URLError, ssl.SSLError, ConnectionError)):
            urllib.request.urlopen(endpoint, context=first_client, timeout=2)
        with urllib.request.urlopen(endpoint, context=second_client, timeout=2) as response:
            assert response.status == HTTPStatus.OK


def test_phase4_shadow_endpoint_returns_only_would_decision(environment: object, tmp_path: Path) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    server_context, client_context, _ = mtls_contexts(tmp_path)
    with ShadowAgentHTTPServer(service, server_context, shadow_only=True) as server:
        result = SignedHTTPSClient(server.base_url, client_context).post_shadow_command(command)
    assert result["shadow_decision"] == "WOULD_ACCEPT"
    assert result["physical_adapter"] == "FAKE_COUNTER_ONLY"
    assert set(result) == {
        "command_id",
        "authority_epoch",
        "command_seq",
        "shadow_decision",
        "reason_code",
        "agent_observed_at",
        "physical_adapter",
        "physical_attempt_count",
    }
    assert result["physical_attempt_count"] == 0
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_phase4_shadow_endpoint_receives_readiness_heartbeat_without_physical_action(environment: object, tmp_path: Path) -> None:
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    publisher = AuthorityHeartbeatPublisher(environment.central, environment.central_codec)  # type: ignore[attr-defined]
    heartbeat = publisher.build(
        "stream-target",
        MonitoringReadiness(True, True, isoformat_utc(utc_now()), "phase4-shadow-test"),
    )
    assert heartbeat is not None
    lease = AgentAuthorityLease(environment.dell, environment.agent_codec)  # type: ignore[attr-defined]
    server_context, client_context, _ = mtls_contexts(tmp_path)
    with ShadowAgentHTTPServer(service, server_context, shadow_only=True, authority_lease=lease) as server:
        result = SignedHTTPSClient(server.base_url, client_context).post_shadow_heartbeat(heartbeat)
    assert result == {
        "authority_state": "CENTRAL_ACTIVE",
        "physical_adapter": "FAKE_COUNTER_ONLY",
        "physical_attempt_count": 0,
    }


def test_shadow_endpoint_accepts_only_no_effect_maintenance_snapshot(environment: object, tmp_path: Path) -> None:
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    server_context, client_context, _ = mtls_contexts(tmp_path)
    cache_path = tmp_path / "maintenance-snapshot.json"
    now = datetime.datetime.now(datetime.UTC)
    snapshot = {
        "schema_version": "maintenance.audit_state_snapshot.v2",
        "snapshot_id": "snapshot-distribution-1",
        "producer_id": "arena-maintenance-shadow",
        "producer_instance_id": "instance-1",
        "observed_at": now.isoformat().replace("+00:00", "Z"),
        "fresh_until": (now + datetime.timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
        "physical_effect_count": 0,
        "production_behavior_modified": False,
    }
    with ShadowAgentHTTPServer(
        service,
        server_context,
        shadow_only=True,
        maintenance_snapshot_cache=ShadowMaintenanceSnapshotCache(cache_path),
    ) as server:
        result = SignedHTTPSClient(server.base_url, client_context).post_shadow_maintenance_snapshot(snapshot)
    assert result["accepted"] is True
    assert result["physical_effect_count"] == 0
    assert cache_path.exists()
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_shadow_reconciliation_arms_grace_for_first_new_epoch_heartbeat(environment: object, tmp_path: Path) -> None:
    clock = [100.0]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    publisher = AuthorityHeartbeatPublisher(environment.central, environment.central_codec)  # type: ignore[attr-defined]
    ready = MonitoringReadiness(True, True, isoformat_utc(utc_now()), "phase4-shadow-test")
    lease = AgentAuthorityLease(  # type: ignore[attr-defined]
        environment.dell,
        environment.agent_codec,
        monotonic=lambda: clock[0],
    )
    server_context, client_context, _ = mtls_contexts(tmp_path)
    with ShadowAgentHTTPServer(
        service,
        server_context,
        shadow_only=True,
        authority_lease=lease,
    ) as server:
        client = SignedHTTPSClient(server.base_url, client_context)
        first = publisher.build("stream-target", ready)
        assert first is not None
        assert client.post_shadow_heartbeat(first)["authority_state"] == "CENTRAL_ACTIVE"
        clock[0] += 16.0
        assert lease.tick("stream-target") == "LOCAL_FALLBACK"

        assert CentralReconciler(environment.central, environment.central_codec, client).reconcile("stream-target") == 2  # type: ignore[attr-defined]
        assert lease.tick("stream-target") == "CENTRAL_SUSPECT"
        second = publisher.build("stream-target", ready)
        assert second is not None
        assert client.post_shadow_heartbeat(second)["authority_state"] == "CENTRAL_ACTIVE"

    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def _signed_dell_observation(
    signer: Signer,
    *,
    sequence: int,
    notsent: int = 2_000_000,
) -> dict[str, Any]:
    now = utc_now()
    observed = now - datetime.timedelta(seconds=1)
    valid_until = now + datetime.timedelta(seconds=15)
    target = {
        "host_id": "dell-stream-runtime",
        "host_boot_id": "boot-observation-a",
        "namespace": "stream-v3",
        "pod_uid": "pod-observation-a",
        "container_name": "stream-engine",
        "container_id": "containerd://stream-engine-observation-a",
        "ffmpeg_generation": "ffmpeg-observation-a",
        "ffmpeg_pid": 4100,
    }
    target_snapshot = {
        "schema": "cra_dell_recovery.target_snapshot.v1",
        "snapshot_id": "target-observation-a",
        "observed_at": isoformat_utc(observed),
        "valid_until": isoformat_utc(valid_until),
        "source_revision": "a" * 64,
        "read_started_at": isoformat_utc(observed - datetime.timedelta(milliseconds=10)),
        "read_finished_at": isoformat_utc(observed),
        "status": "VALID",
        "reason_code": "SNAPSHOT_CONSISTENT",
        "target_identity": target,
        "runtime_snapshot_id": "runtime-observation-a",
        "runtime_status": "VALID",
        "runtime_reason_code": "RUNTIME_SNAPSHOT_CONSISTENT",
        "runtime_identity": {
            "host_id": target["host_id"],
            "host_boot_id": target["host_boot_id"],
            "namespace": target["namespace"],
            "pod_uid": target["pod_uid"],
            "stream_engine_container_name": target["container_name"],
            "stream_engine_container_id": target["container_id"],
            "runtime_generation": "runtime-generation-observation-a",
        },
        "runtime_container_ready": True,
    }

    def check(name: str, status: str) -> dict[str, str]:
        return {
            "status": status,
            "observed_at": isoformat_utc(observed),
            "evidence_ref": f"dell/observation/{sequence}/{name}",
        }

    unsigned = {
        "schema": "cra_dell_recovery.observation_bundle.v1",
        "source_instance_id": "dell-stream-runtime-observer",
        "source_release_id": "dell-observer-release-a",
        "observation_id": f"dell-observation-{sequence}-{notsent}",
        "observation_revision": f"{sequence:064x}",
        "observation_sequence": sequence,
        "observed_at": isoformat_utc(observed),
        "valid_until": isoformat_utc(valid_until),
        "target_snapshot": target_snapshot,
        "transport": {
            "observed_at": isoformat_utc(observed),
            "controller_id": "controller-observation-a",
            "ffmpeg_pid": 4100,
            "ffmpeg_uptime_sec": 900,
            "metrics": {
                "bytes_sent_delta": 0,
                "bytes_sent": 20_000_000,
                "bytes_acked": 10_000_000,
                "bytes_elapsed_sec": 5,
                "send_mbps": 0.0,
                "send_q": 2_000_000,
                "lastsnd_ms": 7_000,
                "notsent": notsent,
                "unacked": 32,
                "rto_ms": 1_000,
                "network_down": False,
                "remote_warning": True,
                "low_upload_pressure": True,
            },
            "network": {
                "gateway_ok": True,
                "public_ok_count": 2,
                "dns_ok": True,
                "tcp_probe_ok": True,
                "network_down": False,
            },
        },
        "recovery": {
            "observed_at": isoformat_utc(observed),
            "stall_streak": 4,
            "stall_confirm_threshold": 3,
            "net_fail_streak": 0,
        },
        "maintenance": check("maintenance", "FALSE"),
        "checks": {
            "tcp_stall": check("tcp_stall", "CONFIRMED"),
            "network_down": check("network_down", "FALSE"),
            "ffmpeg_present": check("ffmpeg_present", "TRUE"),
            "target_stable": check("target_stable", "TRUE"),
            "maintenance": check("maintenance", "FALSE"),
            "stream_engine_ready": check("stream_engine_ready", "TRUE"),
            "tcp_flow_healthy": check("tcp_flow_healthy", "FALSE"),
            "upload_progress_healthy": check("upload_progress_healthy", "FALSE"),
            "startup_gate": check("startup_gate", "TRUE"),
        },
        "measurements": [
            {
                "name": "rtmps_notsent_bytes",
                "value": notsent,
                "unit": "bytes",
                "evidence_ref": f"dell/observation/{sequence}/transport",
            }
        ],
        "evidence_refs": [
            f"dell/observation/{sequence}/target",
            f"dell/observation/{sequence}/transport",
            f"dell/observation/{sequence}/maintenance",
        ],
        "key_id": signer.key_id,
    }
    return signer.sign(unsigned)


class _StaticDellObservationPublisher:
    def __init__(self, value: dict[str, Any]) -> None:
        self.value = value
        self.build_count = 0

    def build(self) -> dict[str, Any]:
        self.build_count += 1
        return self.value


def test_dell_observation_get_only_mtls_signature_and_monotonic_inbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("dell_recovery_agent.observation_server.SERVER_RESOURCE_REFRESH_SECONDS", 0.01)
    private = ed25519.Ed25519PrivateKey.from_private_bytes(bytes(range(101, 133)))
    signer = Signer("dell-observation-key-a", private)
    publisher = _StaticDellObservationPublisher(_signed_dell_observation(signer, sequence=7))
    contract = DellObservationContract(
        ROOT / "contracts/cra_dell_recovery/v1/observation_bundle.schema.json",
        KeyRing({signer.key_id: private.public_key()}),
        allowed_sources={"dell-stream-runtime-observer": "dell-observer-release-a"},
        expected_host_id="dell-stream-runtime",
        expected_namespace="stream-v3",
    )
    inbox = tmp_path / "dell-observation-inbox.json"
    persisted = tmp_path / "dell-observation-current.json"
    resource_status = tmp_path / "server-resource-status.json"
    status_file = tmp_path / "host-status.json"
    status = {
        "schema": "cra.no_action_host_status.v1",
        "role": "dell",
        "host_id": "dell-stream-runtime",
        "release_id": "dell-observer-release-a",
        "observed_at": isoformat_utc(datetime.datetime.now(datetime.UTC)),
        "valid_until": isoformat_utc(datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1)),
    }
    status_file.write_text(json.dumps(status), encoding="utf-8")
    status_file.chmod(0o600)
    server_context, client_context, no_client_certificate = mtls_contexts(tmp_path)
    with DellObservationHTTPSServer(  # type: ignore[arg-type]
        publisher,
        server_context,
        published_observation_file=persisted,
        host_status_file=status_file,
        host_status_host_id="dell-stream-runtime",
        host_status_release_id="dell-observer-release-a",
        server_resource_status_file=resource_status,
    ) as server:
        endpoint = f"{server.base_url}{OBSERVATION_PATH}"
        initial_resource_time = json.loads(resource_status.read_text(encoding="utf-8"))["observed_at"]
        deadline = time.monotonic() + 1
        while json.loads(resource_status.read_text(encoding="utf-8"))["observed_at"] == initial_resource_time:
            assert time.monotonic() < deadline
            time.sleep(0.01)

        def receive_clock() -> datetime.datetime:
            assert publisher.build_count > 0
            return datetime.datetime.now(datetime.UTC)

        puller = DellObservationPuller(
            endpoint_url=endpoint,
            ssl_context=client_context,
            contract=contract,
            observation_file=inbox,
            clock=receive_clock,
        )
        assert puller.pull()["disposition"] == "UPDATED"
        assert puller.pull()["disposition"] == "UNCHANGED"
        assert json.loads(persisted.read_text(encoding="utf-8"))["observation_sequence"] == 7
        assert persisted.stat().st_mode & 0o777 == 0o640
        resources = json.loads(resource_status.read_text(encoding="utf-8"))
        assert resources["role"] == "dell"
        assert resources["resident_memory_bytes"] > 0
        assert resources["open_fd_count"] > 0
        assert resource_status.stat().st_mode & 0o777 == 0o640
        with urllib.request.urlopen(f"{server.base_url}{STATUS_PATH}", context=client_context, timeout=2) as response:
            assert json.load(response) == status

        status["valid_until"] = isoformat_utc(datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=1))
        status_file.write_text(json.dumps(status), encoding="utf-8")
        status_file.chmod(0o600)
        with pytest.raises(urllib.error.HTTPError) as unavailable_status:
            urllib.request.urlopen(f"{server.base_url}{STATUS_PATH}", context=client_context, timeout=2)
        try:
            assert unavailable_status.value.code == HTTPStatus.SERVICE_UNAVAILABLE
            assert unavailable_status.value.headers["Retry-After"] == "1"
        finally:
            unavailable_status.value.close()

        monkeypatch.setattr(
            "dell_recovery_agent.observation_server.atomic_write_json",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("injected soak persistence failure")),
        )

        request = urllib.request.Request(endpoint, data=b"{}", method="POST")
        with pytest.raises(urllib.error.HTTPError) as rejected:
            urllib.request.urlopen(request, context=client_context, timeout=2)
        try:
            assert rejected.value.code == 405
        finally:
            rejected.value.close()

        with pytest.raises((urllib.error.URLError, ssl.SSLError, ConnectionError)):
            urllib.request.urlopen(endpoint, context=no_client_certificate, timeout=2)

        publisher.value = _signed_dell_observation(signer, sequence=6)
        with pytest.raises(ValueError, match="DELL_OBSERVATION_INBOX_SEQUENCE_REGRESSION"):
            puller.pull()
        publisher.value = _signed_dell_observation(signer, sequence=7, notsent=2_000_001)
        with pytest.raises(ValueError, match="DELL_OBSERVATION_INBOX_SEQUENCE_CONFLICT"):
            puller.pull()

    saved = json.loads(inbox.read_text(encoding="utf-8"))
    assert saved["observation_sequence"] == 7
    assert publisher.build_count == 4
