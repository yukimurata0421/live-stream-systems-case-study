from __future__ import annotations

import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID

CertificateUsage = Literal["client", "server"]


def _certificate(path: Path, code: str) -> x509.Certificate:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{code}_NOT_REGULAR_FILE")
        if metadata.st_size > 64 * 1024:
            raise ValueError(f"{code}_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(64 * 1024 + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > 64 * 1024:
        raise ValueError(f"{code}_TOO_LARGE")
    try:
        return x509.load_pem_x509_certificate(raw)
    except ValueError as error:
        raise ValueError(f"{code}_PEM_INVALID") from error


def _validate_time(certificate: x509.Certificate, code: str, *, now: datetime | None) -> None:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if current < certificate.not_valid_before_utc:
        raise ValueError(f"{code}_NOT_YET_VALID")
    if current >= certificate.not_valid_after_utc:
        raise ValueError(f"{code}_EXPIRED")


def validate_ca_certificate(path: Path, code: str, *, now: datetime | None = None) -> None:
    certificate = _certificate(path, code)
    try:
        basic_constraints = certificate.extensions.get_extension_for_class(x509.BasicConstraints)
    except x509.ExtensionNotFound as error:
        raise ValueError(f"{code}_BASIC_CONSTRAINTS_INVALID") from error
    if not basic_constraints.critical or not basic_constraints.value.ca:
        raise ValueError(f"{code}_BASIC_CONSTRAINTS_INVALID")
    try:
        key_usage = certificate.extensions.get_extension_for_class(x509.KeyUsage)
    except x509.ExtensionNotFound as error:
        raise ValueError(f"{code}_KEY_USAGE_INVALID") from error
    if not key_usage.critical or not key_usage.value.key_cert_sign or not key_usage.value.crl_sign:
        raise ValueError(f"{code}_KEY_USAGE_INVALID")
    _validate_time(certificate, code, now=now)


def validate_leaf_certificate(
    path: Path,
    code: str,
    *,
    usage: CertificateUsage,
    now: datetime | None = None,
) -> None:
    certificate = _certificate(path, code)
    try:
        basic_constraints = certificate.extensions.get_extension_for_class(x509.BasicConstraints)
    except x509.ExtensionNotFound as error:
        raise ValueError(f"{code}_BASIC_CONSTRAINTS_INVALID") from error
    if not basic_constraints.critical or basic_constraints.value.ca:
        raise ValueError(f"{code}_BASIC_CONSTRAINTS_INVALID")
    try:
        key_usage = certificate.extensions.get_extension_for_class(x509.KeyUsage)
    except x509.ExtensionNotFound as error:
        raise ValueError(f"{code}_KEY_USAGE_INVALID") from error
    if not key_usage.critical or not key_usage.value.digital_signature or key_usage.value.key_cert_sign:
        raise ValueError(f"{code}_KEY_USAGE_INVALID")
    try:
        extended_key_usage = certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
    except x509.ExtensionNotFound as error:
        raise ValueError(f"{code}_EXTENDED_KEY_USAGE_INVALID") from error
    required_usage = ExtendedKeyUsageOID.CLIENT_AUTH if usage == "client" else ExtendedKeyUsageOID.SERVER_AUTH
    if required_usage not in extended_key_usage.value:
        raise ValueError(f"{code}_EXTENDED_KEY_USAGE_INVALID")
    _validate_time(certificate, code, now=now)
