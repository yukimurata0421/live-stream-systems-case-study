from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier

from cra_dell_recovery.tls_credentials import validate_ca_certificate, validate_leaf_certificate


def _write(path: Path, certificate: x509.Certificate) -> Path:
    path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    path.chmod(0o644)
    return path


def _ca(now: datetime, *, include_key_usage: bool = True) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test CA")])
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
    )
    if include_key_usage:
        builder = builder.add_extension(
            x509.KeyUsage(
                digital_signature=False,
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
    return key, builder.sign(key, hashes.SHA256())


def _leaf(
    now: datetime,
    ca_key: rsa.RSAPrivateKey,
    ca_certificate: x509.Certificate,
    usage: ObjectIdentifier,
) -> x509.Certificate:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test leaf")]))
        .issuer_name(ca_certificate.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
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
        .sign(ca_key, hashes.SHA256())
    )


def test_validates_ca_and_endpoint_roles(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    ca_key, ca_certificate = _ca(now)
    ca_path = _write(tmp_path / "ca.pem", ca_certificate)
    server_path = _write(tmp_path / "server.pem", _leaf(now, ca_key, ca_certificate, ExtendedKeyUsageOID.SERVER_AUTH))
    client_path = _write(tmp_path / "client.pem", _leaf(now, ca_key, ca_certificate, ExtendedKeyUsageOID.CLIENT_AUTH))

    validate_ca_certificate(ca_path, "TEST_CA", now=now)
    validate_leaf_certificate(server_path, "TEST_SERVER", usage="server", now=now)
    validate_leaf_certificate(client_path, "TEST_CLIENT", usage="client", now=now)


def test_rejects_ca_without_key_cert_sign_before_handshake(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    _, ca_certificate = _ca(now, include_key_usage=False)
    ca_path = _write(tmp_path / "legacy-ca.pem", ca_certificate)

    with pytest.raises(ValueError, match="TEST_CA_KEY_USAGE_INVALID"):
        validate_ca_certificate(ca_path, "TEST_CA", now=now)


def test_rejects_wrong_leaf_extended_usage(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    ca_key, ca_certificate = _ca(now)
    client_path = _write(tmp_path / "client.pem", _leaf(now, ca_key, ca_certificate, ExtendedKeyUsageOID.CLIENT_AUTH))

    with pytest.raises(ValueError, match="TEST_SERVER_EXTENDED_KEY_USAGE_INVALID"):
        validate_leaf_certificate(client_path, "TEST_SERVER", usage="server", now=now)
