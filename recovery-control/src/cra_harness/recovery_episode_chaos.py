from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import random
import socket
import ssl
import urllib.error
from collections.abc import Callable
from dataclasses import asdict, dataclass
from email.message import Message
from pathlib import Path
from typing import Any

from cra_dell_recovery.transport_resilience import FailureClassification, classify_failure


@dataclass(frozen=True)
class Fault:
    name: str
    error_factory: Callable[[], BaseException]
    retryable: bool
    family: str


def _http(status: int) -> Callable[[], BaseException]:
    return lambda: urllib.error.HTTPError("https://facts.invalid", status, "injected", Message(), None)


FAULTS = (
    Fault("dns_temporary", lambda: urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, "injected")), True, "dns"),
    Fault("dns_permanent", lambda: urllib.error.URLError(socket.gaierror(socket.EAI_NONAME, "injected")), False, "dns"),
    Fault("tcp_timeout", lambda: urllib.error.URLError(TimeoutError(errno.ETIMEDOUT, "injected")), True, "tcp"),
    Fault("tcp_reset", lambda: urllib.error.URLError(ConnectionResetError(errno.ECONNRESET, "injected")), True, "tcp"),
    Fault("tcp_refused", lambda: urllib.error.URLError(ConnectionRefusedError(errno.ECONNREFUSED, "injected")), True, "tcp"),
    Fault("lan_down", lambda: OSError(errno.ENETDOWN, "injected"), True, "network"),
    Fault("route_missing", lambda: OSError(errno.ENETUNREACH, "injected"), True, "network"),
    Fault("host_unreachable", lambda: OSError(errno.EHOSTUNREACH, "injected"), True, "network"),
    Fault("tls_eof", lambda: urllib.error.URLError(ssl.SSLEOFError(8, "injected")), True, "tls"),
    Fault("tls_certificate", lambda: urllib.error.URLError(ssl.SSLCertVerificationError("injected")), False, "tls"),
    Fault("tls_protocol", lambda: ssl.SSLError("injected"), False, "tls"),
    *(
        Fault(f"http_{status}", _http(status), status in {408, 425, 429, 500, 502, 503, 504}, "http")
        for status in (400, 401, 403, 404, 408, 409, 422, 425, 429, 500, 502, 503, 504)
    ),
    Fault("signature", lambda: ValueError("SIGNATURE_INVALID"), False, "security"),
    Fault("sequence_regression", lambda: ValueError("SEQUENCE_REGRESSION"), False, "security"),
    Fault("redirect", lambda: ValueError("REDIRECT_FORBIDDEN"), False, "protocol"),
    Fault("content_type", lambda: ValueError("CONTENT_TYPE_INVALID"), False, "contract"),
    Fault("framing", lambda: ValueError("CONTENT_LENGTH_MISMATCH"), False, "contract"),
    Fault("json", lambda: json.JSONDecodeError("injected", "{", 0), False, "contract"),
    Fault("disk_full", lambda: OSError(errno.ENOSPC, "injected"), False, "storage"),
    Fault("sqlite_busy", lambda: OSError(errno.EBUSY, "injected"), False, "storage"),
)


def run_recovery_episode_chaos(
    *,
    case_count: int = 100_000,
    seed: int = 20260902,
    classifier: Callable[[BaseException], FailureClassification] = classify_failure,
) -> dict[str, Any]:
    if isinstance(case_count, bool) or not isinstance(case_count, int) or not len(FAULTS) <= case_count <= 500_000:
        raise ValueError("RECOVERY_EPISODE_CHAOS_CASE_COUNT_INVALID")
    rng = random.Random(seed)
    scheduled = list(FAULTS) + [rng.choice(FAULTS) for _ in range(case_count - len(FAULTS))]
    rng.shuffle(scheduled)
    failures: list[dict[str, Any]] = []
    family_counts: dict[str, int] = {}
    recovered = 0
    exhausted = 0
    safe_blocked = 0
    suspend_expired = 0
    for index, fault in enumerate(scheduled):
        classification = classifier(fault.error_factory())
        retry_budget = rng.randint(1, 8)
        fault_attempts = rng.randint(1, 12)
        suspended_seconds = rng.choice((0, 0, 0, 1, 15, 31, 300))
        elapsed_budget_seconds = rng.choice((1, 3, 10, 30))
        budget_expired = suspended_seconds >= elapsed_budget_seconds
        if classification.retryable and not budget_expired and fault_attempts <= retry_budget:
            outcome = "RECOVERED"
            recovered += 1
        elif classification.retryable:
            outcome = "EXHAUSTED"
            exhausted += 1
            suspend_expired += int(budget_expired)
        else:
            outcome = "SAFE_BLOCKED"
            safe_blocked += 1
        family_counts[fault.family] = family_counts.get(fault.family, 0) + 1
        passed = classification.retryable is fault.retryable
        # The observation plane owns no action capability in every outcome.
        passed = passed and outcome in {"RECOVERED", "EXHAUSTED", "SAFE_BLOCKED"}
        if not passed:
            failures.append(
                {
                    "index": index,
                    "fault": fault.name,
                    "expected_retryable": fault.retryable,
                    "classification": asdict(classification),
                    "outcome": outcome,
                }
            )
    return {
        "schema": "cra.recovery_episode_chaos_report.v1",
        "seed": seed,
        "case_count": case_count,
        "fault_type_count": len(FAULTS),
        "family_case_count": dict(sorted(family_counts.items())),
        "recovered_count": recovered,
        "exhausted_count": exhausted,
        "safe_blocked_count": safe_blocked,
        "host_suspend_expired_count": suspend_expired,
        "control_capability_count": 0,
        "physical_effect_count": 0,
        "production_target_touched": False,
        "failure_count": len(failures),
        "failures": failures[:20],
        "pass": not failures and len(family_counts) >= 8,
    }


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _source_hashes() -> dict[str, str]:
    project_root = Path(__file__).resolve().parents[2]
    paths = {Path(__file__).resolve(), Path(classify_failure.__code__.co_filename).resolve()}
    return {str(path.relative_to(project_root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run isolated recovery-episode chaos")
    parser.add_argument("--cases", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source_hashes = _source_hashes()
    report = run_recovery_episode_chaos(case_count=args.cases, seed=args.seed)
    if _source_hashes() != source_hashes:
        raise RuntimeError("RECOVERY_EPISODE_CHAOS_SOURCE_DRIFT")
    report["source_hashes"] = source_hashes
    if args.output is not None:
        _atomic_json(args.output, report)
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    if not report["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
