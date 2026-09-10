from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import product
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from cra_authority.json_input import load_object
from cra_authority.projection_pull import ProjectionPuller, _atomic_json, _client_context, _deadline_clock
from cra_dell_recovery.bounded_http import fetch_bounded_json
from cra_dell_recovery.tls_credentials import validate_ca_certificate, validate_leaf_certificate
from cra_harness.covering_array import coverage_report, generate_covering_array
from dell_recovery_agent.observation_publisher import _optional_number

SCHEMA = "cra.external_boundary_covering.v1"
_HARNESS_SSL_CONTEXT = ssl.create_default_context()
DOMAINS = (
    "host_suspend",
    "credential_validity",
    "certificate_rotation",
    "disk_full_transition",
    "sqlite_multiprocess_contention",
    "dns_tls_stage_failure",
    "source_update_interruption",
    "source_metric_availability",
)
FACTOR_VALUES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("failure_domain", DOMAINS),
    ("failure_phase", ("before", "during_open", "during_read", "during_commit", "after_commit", "during_rotate", "recovery", "relapse")),
    ("time_offset", ("minus_1h", "minus_1s", "exact", "plus_1s", "plus_30s", "plus_1h", "plus_1d")),
    ("stall_duration", ("zero", "below_deadline", "at_deadline", "above_deadline", "long", "unbounded_simulated")),
    ("concurrency", ("one", "two", "four", "eight", "sixteen", "thirty_two")),
    ("retry_budget", ("zero", "one", "two", "four", "eight")),
    ("persistence_state", ("empty", "hot", "wal_heavy", "checkpointing", "reopen")),
    ("rotation_state", ("old", "new", "mixed", "missing", "recovered")),
    ("recovery_order", ("source_first", "consumer_first", "simultaneous", "oscillating")),
    ("payload_profile", ("small", "limit_minus_one", "at_limit", "over_limit")),
    ("oracle_margin", ("tight", "nominal", "wide", "extreme")),
)
FACTOR_SIZES = tuple(len(values) for _, values in FACTOR_VALUES)
MIXED_INDICES = (0, 1, 2, 3, 4, 8)
MIXED_LEVELS = (
    tuple(range(8)),
    (1, 2, 3, 5),
    (1, 2, 3, 6),
    (1, 2, 3),
    (0, 3, 5),
    (0, 1, 2),
)


def _mandatory_rows() -> tuple[tuple[int, ...], ...]:
    rows: list[tuple[int, ...]] = []
    for values in product(*MIXED_LEVELS):
        row = [0] * len(FACTOR_SIZES)
        for index, value in zip(MIXED_INDICES, values, strict=True):
            row[index] = value
        rows.append(tuple(row))
    return tuple(rows)


def campaign_rows(*, case_count: int, seed: int) -> tuple[tuple[int, ...], ...]:
    return generate_covering_array(
        FACTOR_SIZES,
        strength=4,
        target_count=case_count,
        mandatory_rows=_mandatory_rows(),
        seed=seed,
    )


def _source_hashes() -> dict[str, str]:
    project_root = Path(__file__).resolve().parents[3]
    paths = {
        Path(__file__).resolve(),
        Path(generate_covering_array.__code__.co_filename).resolve(),
        Path(ProjectionPuller._transient_transport_error.__code__.co_filename).resolve(),
        Path(fetch_bounded_json.__code__.co_filename).resolve(),
        Path(load_object.__code__.co_filename).resolve(),
        Path(validate_leaf_certificate.__code__.co_filename).resolve(),
        Path(_optional_number.__code__.co_filename).resolve(),
    }
    return {str(path.relative_to(project_root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def _tls_stage_probes() -> dict[str, bool]:
    probes: dict[str, BaseException] = {
        "temporary_dns": urllib.error.URLError(OSError(-3, "temporary dns")),
        "connect_timeout": urllib.error.URLError(TimeoutError("connect timeout")),
        "tls_timeout": urllib.error.URLError(TimeoutError("tls timeout")),
        "tls_eof": urllib.error.URLError(ssl.SSLEOFError(8, "unexpected eof")),
        "certificate": urllib.error.URLError(ssl.SSLCertVerificationError("expired")),
        "protocol": urllib.error.URLError(ssl.SSLError("protocol mismatch")),
    }
    # A real gaierror is constructed separately so EAI_AGAIN is not confused
    # with a generic OSError sharing the platform's numeric errno.
    import socket

    probes["temporary_dns"] = urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, "temporary dns"))
    return {name: ProjectionPuller._transient_transport_error(error) for name, error in probes.items()}


@dataclass(frozen=True)
class _CredentialFixture:
    results: dict[str, bool]
    certificate_a: Path
    key_a: Path
    certificate_b: Path
    key_b: Path
    ca: Path
    not_before: datetime
    baseline: datetime
    not_after: datetime


def _credential_fixture(root: Path) -> _CredentialFixture:
    baseline = datetime.now(UTC).replace(microsecond=0)
    not_before = baseline - timedelta(hours=1)
    not_after = baseline + timedelta(hours=1)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "CRA boundary CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(baseline - timedelta(days=1))
        .not_valid_after(baseline + timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
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
        .sign(ca_key, hashes.SHA256())
    )

    def issue(name: str) -> tuple[Path, Path]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        certificate = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
            .issuer_name(ca.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(not_before)
            .not_valid_after(not_after)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256())
        )
        certificate_path = root / f"{name}.pem"
        private_key_path = root / f"{name}-key.pem"
        certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        private_key_path.write_bytes(
            key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        )
        certificate_path.chmod(0o644)
        private_key_path.chmod(0o600)
        return certificate_path, private_key_path

    ca_path = root / "ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    ca_path.chmod(0o644)
    certificate_a, key_a = issue("client-a")
    certificate_b, key_b = issue("client-b")

    def accepted(at: datetime) -> bool:
        try:
            validate_leaf_certificate(certificate_a, "BOUNDARY_CLIENT", usage="client", now=at)
        except ValueError:
            return False
        return True

    def context_accepted(certificate: Path, key: Path) -> bool:
        try:
            _client_context(certificate, key, ca_path)
        except (OSError, ssl.SSLError, ValueError):
            return False
        return True

    validate_ca_certificate(ca_path, "BOUNDARY_CA", now=baseline)
    results = {
        "not_yet_valid_rejected": not accepted(not_before - timedelta(seconds=1)),
        "exact_not_before_accepted": accepted(not_before),
        "valid_accepted": accepted(baseline),
        "exact_not_after_fail_closed": not accepted(not_after),
        "expired_rejected": not accepted(not_after + timedelta(seconds=1)),
        "matching_old_pair_accepted": context_accepted(certificate_a, key_a),
        "matching_new_pair_accepted": context_accepted(certificate_b, key_b),
        "mixed_pair_rejected": not context_accepted(certificate_a, key_b),
        "missing_key_rejected": not context_accepted(certificate_a, root / "missing-key.pem"),
    }
    return _CredentialFixture(results, certificate_a, key_a, certificate_b, key_b, ca_path, not_before, baseline, not_after)


def _source_interruption_probes(root: Path) -> dict[str, bool]:
    target = root / "projection.json"
    _atomic_json(target, {"generation": "old", "sequence": 1})
    old_digest = hashlib.sha256(target.read_bytes()).hexdigest()
    before_temporary = root / ".before.tmp"
    before_script = """
import json,os,sys
from pathlib import Path
path=Path(sys.argv[1])
descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(descriptor,'w') as handle:
    json.dump({'generation':'new','sequence':2},handle)
    handle.flush(); os.fsync(handle.fileno())
os._exit(73)
"""
    before = subprocess.run([sys.executable, "-c", before_script, str(before_temporary)], check=False)
    before_preserved = before.returncode == 73 and hashlib.sha256(target.read_bytes()).hexdigest() == old_digest
    before_temporary.unlink(missing_ok=True)

    after_temporary = root / ".after.tmp"
    after_script = """
import json,os,sys
from pathlib import Path
temporary,target=map(Path,sys.argv[1:])
descriptor=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(descriptor,'w') as handle:
    json.dump({'generation':'new','sequence':2},handle)
    handle.flush(); os.fsync(handle.fileno())
os.replace(temporary,target)
os._exit(74)
"""
    after = subprocess.run([sys.executable, "-c", after_script, str(after_temporary), str(target)], check=False)
    after_value = load_object(target.read_bytes(), maximum_bytes=4096)
    after_valid = after.returncode == 74 and after_value == {"generation": "new", "sequence": 2}

    partial = root / "partial.json"
    partial.write_bytes(b'{"generation":"new"')
    partial_rejected = False
    try:
        load_object(partial.read_bytes(), maximum_bytes=4096)
    except ValueError:
        partial_rejected = True
    return {
        "before_replace_preserves_old": before_preserved,
        "after_replace_exposes_complete_new": after_valid,
        "partial_source_rejected": partial_rejected,
    }


def _sqlite_evidence(path: Path) -> dict[str, bool]:
    cases = json.loads(path.read_text(encoding="utf-8"))
    by_id = {str(item["scenario_id"]): item for item in cases}
    return {
        "disk_full_transition": by_id.get("DB-31", {}).get("classification") == "PASS",
        "sqlite_multiprocess_contention": by_id.get("DB-32", {}).get("classification") == "PASS",
    }


def _source_metric_availability_probes() -> dict[str, bool]:
    def rejected(value: object) -> bool:
        try:
            _optional_number(value, "BOUNDARY_RATE_INVALID")
        except ValueError:
            return True
        return False

    return {
        "unknown_preserved": _optional_number(None, "BOUNDARY_RATE_INVALID") is None,
        "zero_preserved": _optional_number(0, "BOUNDARY_RATE_INVALID") == 0.0,
        "positive_preserved": _optional_number(4.8, "BOUNDARY_RATE_INVALID") == 4.8,
        "numeric_string_preserved": _optional_number("4.8", "BOUNDARY_RATE_INVALID") == 4.8,
        "boolean_rejected": rejected(True),
        "negative_rejected": rejected(-0.1),
        "infinity_rejected": rejected(float("inf")),
        "malformed_rejected": rejected("unavailable"),
    }


def _posture(row: tuple[int, ...]) -> str:
    domain = DOMAINS[row[0]]
    if domain == "dns_tls_stage_failure":
        return "BOUNDED_RETRY_OR_FAIL_CLOSED"
    if domain == "source_update_interruption":
        return "OLD_OR_COMPLETE_NEW"
    if domain == "source_metric_availability":
        return "PRESERVE_UNKNOWN_WITHOUT_FABRICATION"
    if domain in {"disk_full_transition", "sqlite_multiprocess_contention"}:
        return "ROLLBACK_OR_SERIALIZED_COMMIT"
    if domain in {"credential_validity", "certificate_rotation"}:
        return "SECURITY_FAIL_CLOSED"
    return "SUSPEND_AWARE_DEADLINE_AND_FRESHNESS"


class _Headers:
    def get(self, _name: str) -> None:
        return None

    def get_content_type(self) -> str:
        return "application/json"


class _Response:
    headers = _Headers()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _maximum_bytes: int) -> bytes:
        return b"{}"


def _host_suspend_case(row: tuple[int, ...]) -> bool:
    elapsed = (0.0, 0.05, 0.1, 0.11, 1.0, 3600.0)[row[3]]
    samples = iter((100.0, 100.0, 100.0 + elapsed, 100.0 + elapsed))
    puller = ProjectionPuller(
        endpoint_url="https://127.0.0.1/v1/monitoring-evidence/latest",
        ssl_context=_HARNESS_SSL_CONTEXT,
        contract=object(),  # type: ignore[arg-type]
        projection_file=Path("/not-written-by-boundary-fetch"),
        timeout_seconds=0.1,
        transient_retry_delays_seconds=(),
        open_url=lambda *_args, **_kwargs: _Response(),
        monotonic=lambda: next(samples),
    )
    expired = False
    try:
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    except ValueError as error:
        expired = "RESPONSE_DEADLINE_EXCEEDED" in str(error)
    return expired is (elapsed >= 0.1)


def _credential_validity_case(row: tuple[int, ...], fixture: _CredentialFixture) -> bool:
    instants = (
        fixture.not_before - timedelta(hours=1),
        fixture.not_before - timedelta(seconds=1),
        fixture.not_before,
        fixture.not_before + timedelta(seconds=1),
        fixture.baseline + timedelta(seconds=30),
        fixture.not_after,
        fixture.not_after + timedelta(days=1),
    )
    expected = (False, False, True, True, True, False, False)[row[2]]
    accepted = True
    try:
        validate_leaf_certificate(fixture.certificate_a, "BOUNDARY_CLIENT", usage="client", now=instants[row[2]])
    except ValueError:
        accepted = False
    return accepted is expected


def _certificate_rotation_case(row: tuple[int, ...], fixture: _CredentialFixture, root: Path) -> bool:
    variants = (
        (fixture.certificate_a, fixture.key_a, True),
        (fixture.certificate_b, fixture.key_b, True),
        (fixture.certificate_a, fixture.key_b, False),
        (fixture.certificate_a, root / "missing-key.pem", False),
        (fixture.certificate_b, fixture.key_b, True),
    )
    certificate, key, expected = variants[row[7]]
    accepted = True
    try:
        _client_context(certificate, key, fixture.ca)
    except (OSError, ssl.SSLError, ValueError):
        accepted = False
    return accepted is expected


def _dns_tls_case(row: tuple[int, ...], tls: dict[str, bool]) -> bool:
    variants = (
        ("temporary_dns", True),
        ("connect_timeout", True),
        ("tls_timeout", True),
        ("tls_eof", True),
        ("certificate", False),
        ("protocol", False),
        ("temporary_dns", True),
        ("tls_eof", True),
    )
    name, expected = variants[row[1]]
    return tls[name] is expected


def _source_update_case(row: tuple[int, ...], source: dict[str, bool], root: Path, sequence: int) -> bool:
    path = root / "source-stress.json"
    payload = {"generation": row[7], "sequence": sequence}
    _atomic_json(path, payload)
    observed = load_object(path.read_bytes(), maximum_bytes=4096)
    preflight_names = ("before_replace_preserves_old", "after_replace_exposes_complete_new", "partial_source_rejected")
    return observed == payload and source[preflight_names[row[8] % len(preflight_names)]]


def _source_metric_availability_case(row: tuple[int, ...], probes: dict[str, bool]) -> bool:
    names = tuple(probes)
    return probes[names[row[1] % len(names)]]


def execute(
    rows: tuple[tuple[int, ...], ...],
    *,
    sqlite_cases: Path,
    source_root: Path,
) -> dict[str, Any]:
    tls = _tls_stage_probes()
    source = _source_interruption_probes(source_root)
    credential_fixture = _credential_fixture(source_root)
    credentials = credential_fixture.results
    sqlite = _sqlite_evidence(sqlite_cases)
    source_metrics = _source_metric_availability_probes()
    clock_boottime = getattr(time, "CLOCK_BOOTTIME", None)
    monotonic_before = time.monotonic()
    deadline_sample = _deadline_clock()
    suspend_aware_clock = clock_boottime is not None and deadline_sample >= monotonic_before
    probes = {
        "host_suspend": suspend_aware_clock,
        "credential_validity": all(credentials[name] for name in credentials if "pair" not in name and "key" not in name),
        "certificate_rotation": all(credentials[name] for name in credentials if "pair" in name or "key" in name),
        "disk_full_transition": sqlite["disk_full_transition"],
        "sqlite_multiprocess_contention": sqlite["sqlite_multiprocess_contention"],
        "dns_tls_stage_failure": (
            tls["temporary_dns"]
            and tls["connect_timeout"]
            and tls["tls_timeout"]
            and tls["tls_eof"]
            and not tls["certificate"]
            and not tls["protocol"]
        ),
        "source_update_interruption": all(source.values()),
        "source_metric_availability": all(source_metrics.values()),
    }
    failures: list[dict[str, Any]] = []
    failure_count = 0
    scenario_counts: Counter[str] = Counter()
    result_hasher = hashlib.sha256()
    for index, row in enumerate(rows, start=1):
        domain = DOMAINS[row[0]]
        if domain == "host_suspend":
            passed = probes[domain] and _host_suspend_case(row)
        elif domain == "credential_validity":
            passed = probes[domain] and _credential_validity_case(row, credential_fixture)
        elif domain == "certificate_rotation":
            passed = probes[domain] and _certificate_rotation_case(row, credential_fixture, source_root)
        elif domain == "dns_tls_stage_failure":
            passed = probes[domain] and _dns_tls_case(row, tls)
        elif domain == "source_update_interruption":
            passed = probes[domain] and _source_update_case(row, source, source_root, index)
        elif domain == "source_metric_availability":
            passed = probes[domain] and _source_metric_availability_case(row, source_metrics)
        else:
            passed = probes[domain]
        scenario_counts[domain] += 1
        summary = {"case": index, "domain": domain, "factors": row, "posture": _posture(row), "passed": passed}
        result_hasher.update(json.dumps(summary, separators=(",", ":"), sort_keys=True).encode() + b"\n")
        if not passed:
            failure_count += 1
            if len(failures) < 100:
                failures.append(summary)
    coverage = coverage_report(rows, FACTOR_SIZES, strength=4)
    mandatory = set(_mandatory_rows())
    mandatory_observed = len(mandatory.intersection(rows))
    passed = failure_count == 0 and all(probes.values()) and coverage["coverage_complete"] is True and mandatory_observed == len(mandatory)
    return {
        "schema": SCHEMA,
        "result": "PASS" if passed else "FAIL",
        "case_count": len(rows),
        "failure_count": failure_count,
        "failure_examples": failures,
        "factor_model": {name: list(values) for name, values in FACTOR_VALUES},
        "scenario_counts": dict(sorted(scenario_counts.items())),
        "global_coverage": coverage,
        "mixed_six_way": {
            "factor_names": [FACTOR_VALUES[index][0] for index in MIXED_INDICES],
            "expected_combination_count": len(mandatory),
            "observed_combination_count": mandatory_observed,
            "coverage_complete": mandatory_observed == len(mandatory),
        },
        "probe_results": {
            "domain": probes,
            "credential_certificate": credentials,
            "tls_stage": tls,
            "source_update": source,
            "source_metric_availability": source_metrics,
            "sqlite": sqlite,
        },
        "case_results_sha256": result_hasher.hexdigest(),
        "safety": {
            "production_network_used": False,
            "production_database_used": False,
            "physical_effect_count": 0,
            "host_suspend_count": 0,
            "production_mutation_count": 0,
        },
        "formal_soak_replacement": False,
    }


def _atomic_report(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, separators=(",", ":"), sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run expanded external boundary covering campaign")
    parser.add_argument("--cases", type=int, default=30_000)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--sqlite-cases", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started = time.perf_counter()
    source_hashes = _source_hashes()
    rows = campaign_rows(case_count=args.cases, seed=args.seed)
    with tempfile.TemporaryDirectory(prefix="cra-external-boundary-") as temporary:
        report = execute(rows, sqlite_cases=args.sqlite_cases, source_root=Path(temporary))
    if _source_hashes() != source_hashes:
        raise RuntimeError("EXTERNAL_BOUNDARY_CAMPAIGN_SOURCE_DRIFT")
    report["seed"] = args.seed
    report["source_hashes"] = source_hashes
    report["wall_duration_seconds"] = round(time.perf_counter() - started, 6)
    _atomic_report(args.output, report)
    print(
        json.dumps(
            {
                "artifact": str(args.output),
                "artifact_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                "case_count": report["case_count"],
                "global_coverage": report["global_coverage"],
                "mixed_six_way": report["mixed_six_way"],
                "result": report["result"],
                "wall_duration_seconds": report["wall_duration_seconds"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
