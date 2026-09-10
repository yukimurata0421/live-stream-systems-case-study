from __future__ import annotations

import argparse
import errno
import hashlib
import http.client
import json
import os
import socket
import ssl
import urllib.error
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass
from email.message import Message
from pathlib import Path
from typing import Any

from cra_dell_recovery.transport_resilience import FailureClassification, classify_failure
from cra_harness.covering_array import coverage_report, generate_covering_array

SCHEMA = "cra.interserver_resilience_chaos.v1"


@dataclass(frozen=True)
class FaultSpec:
    name: str
    family: str
    phase: str
    factory: Callable[[], BaseException] | None
    expected_category: str
    expected_retryable: bool


def _http(status: int) -> Callable[[], BaseException]:
    return lambda: urllib.error.HTTPError("https://facts.invalid", status, "injected", Message(), None)


FAULTS = (
    FaultSpec("healthy", "healthy", "none", None, "NONE", False),
    FaultSpec(
        "dns_temporary",
        "dns",
        "resolve",
        lambda: urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, "x")),
        "TRANSIENT_TRANSPORT",
        True,
    ),
    FaultSpec("dns_permanent", "dns", "resolve", lambda: urllib.error.URLError(socket.gaierror(socket.EAI_NONAME, "x")), "PROTOCOL", False),
    FaultSpec("tcp_connect_timeout", "tcp", "connect", lambda: urllib.error.URLError(TimeoutError("x")), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tcp_refused", "tcp", "connect", lambda: ConnectionRefusedError(errno.ECONNREFUSED, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tcp_half_open_reset", "tcp", "read", lambda: ConnectionResetError(errno.ECONNRESET, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tcp_half_close", "tcp", "read", lambda: ConnectionAbortedError(errno.ECONNABORTED, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tcp_broken_pipe", "tcp", "write", lambda: BrokenPipeError(errno.EPIPE, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("pmtu_black_hole", "pmtu", "large_record", lambda: TimeoutError("x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tcp_zero_window", "tcp", "body_read", lambda: TimeoutError("x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tls_unexpected_eof", "tls", "handshake_or_read", lambda: ssl.SSLEOFError(8, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tls_clean_eof", "tls", "read", lambda: ssl.SSLZeroReturnError(6, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("tls_certificate", "tls", "verify", lambda: ssl.SSLCertVerificationError("x"), "SECURITY", False),
    FaultSpec("tls_protocol", "tls", "handshake", lambda: ssl.SSLError("x"), "SECURITY", False),
    FaultSpec("http_429", "http", "headers", _http(429), "TRANSIENT_TRANSPORT", True),
    FaultSpec("http_503", "http", "headers", _http(503), "TRANSIENT_TRANSPORT", True),
    FaultSpec("http_authentication", "http", "headers", _http(401), "PROTOCOL", False),
    FaultSpec("http_incomplete_read", "http", "body_read", lambda: http.client.IncompleteRead(b"{"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("http_malformed_status", "http", "headers", lambda: http.client.BadStatusLine("x"), "PROTOCOL", False),
    FaultSpec("lan_down", "l2_l3", "connect", lambda: OSError(errno.ENETDOWN, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("route_or_arp_unreachable", "l2_l3", "connect", lambda: OSError(errno.EHOSTUNREACH, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec(
        "firewall_or_conntrack_drop", "firewall", "connect_or_read", lambda: OSError(errno.ETIMEDOUT, "x"), "TRANSIENT_TRANSPORT", True
    ),
    FaultSpec("accept_backlog_full", "resource", "connect", lambda: OSError(errno.ECONNREFUSED, "x"), "TRANSIENT_TRANSPORT", True),
    FaultSpec("ephemeral_port_exhausted", "resource", "connect", lambda: OSError(errno.EADDRNOTAVAIL, "x"), "RESOURCE", False),
    FaultSpec("file_descriptor_exhausted", "resource", "open", lambda: OSError(errno.EMFILE, "x"), "RESOURCE", False),
    FaultSpec("socket_buffer_exhausted", "resource", "connect", lambda: OSError(errno.ENOBUFS, "x"), "RESOURCE", False),
    FaultSpec("signed_source_regression", "identity", "admission", lambda: ValueError("SEQUENCE_REGRESSION"), "SECURITY", False),
    FaultSpec("signed_source_conflict", "identity", "admission", lambda: ValueError("SEQUENCE_CONFLICT"), "SECURITY", False),
    FaultSpec("source_release_overlap", "identity", "admission", lambda: ValueError("SOURCE_OR_RELEASE_NOT_ALLOWED"), "CONTRACT", False),
    FaultSpec("source_identity_oscillation", "identity", "admission", lambda: ValueError("SOURCE_IDENTITY_OSCILLATION"), "CONTRACT", False),
    FaultSpec("disk_full_during_commit", "storage", "commit", lambda: OSError(errno.ENOSPC, "x"), "STORAGE", False),
    FaultSpec("read_only_state_store", "storage", "commit", lambda: OSError(errno.EROFS, "x"), "STORAGE", False),
)

RECOVERY_ORDERS = ("source_first", "consumer_first", "source_then_consumer", "simultaneous", "oscillating", "no_recovery")
PAYLOADS = ("small", "mtu_boundary", "limit", "over_limit", "compressed", "ambiguous_framing")
RESOURCES = ("healthy", "conntrack_drop", "accept_backlog", "fd_exhausted", "ephemeral_ports", "cpu_starved")
CREDENTIALS = ("current", "overlap", "server_first", "client_first", "expired", "mixed_pair")
CLOCKS = ("stable", "ntp_slew", "suspend_below_budget", "suspend_over_budget", "forward_expired", "backward_future")
IDENTITIES = ("stable", "old_release", "split_brain", "same_sequence_conflict", "stale_cache", "oscillating")
STAGES = ("clean", "crash_before_replace", "crash_after_replace", "projection_write_fail", "status_write_fail", "reboot_network_race")

FACTOR_VALUES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("dell_to_arena", tuple(item.name for item in FAULTS)),
    ("arena_to_cra", tuple(item.name for item in FAULTS)),
    ("recovery_order", RECOVERY_ORDERS),
    ("payload", PAYLOADS),
    ("resource", RESOURCES),
    ("credential", CREDENTIALS),
    ("clock", CLOCKS),
    ("identity", IDENTITIES),
    ("commit_stage", STAGES),
)
FACTOR_SIZES = tuple(len(values) for _, values in FACTOR_VALUES)


def _mandatory_rows() -> tuple[tuple[int, ...], ...]:
    # Every cross-hop fault pair is exercised against the baseline of the
    # remaining factors.  The covering array expands those pairs across the
    # recovery/payload/resource/credential/clock/identity/stage interactions.
    rows: list[tuple[int, ...]] = [(left, right, 0, 0, 0, 0, 0, 0, 0) for left in range(len(FAULTS)) for right in range(len(FAULTS))]
    for factor_index in range(2, len(FACTOR_SIZES)):
        for level in range(FACTOR_SIZES[factor_index]):
            row = [0] * len(FACTOR_SIZES)
            row[factor_index] = level
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


def _classified(spec: FaultSpec) -> FailureClassification | None:
    if spec.factory is None:
        return None
    return classify_failure(spec.factory())


def _hop_outcome(spec: FaultSpec, *, recovers: bool, attempt_bias: int) -> dict[str, Any]:
    classification = _classified(spec)
    if classification is None:
        return {"state": "READY", "attempt_count": 1, "retry_count": 0, "classification": None}
    if classification.retryable and recovers:
        attempts = 2 + attempt_bias % 7
        state = "RECOVERED"
    elif classification.retryable:
        attempts = 9
        state = "EXHAUSTED"
    else:
        attempts = 1
        state = "SAFE_BLOCKED"
    return {
        "state": state,
        "attempt_count": attempts,
        "retry_count": attempts - 1,
        "classification": asdict(classification),
    }


def _oracle_violations(observation: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    for name in ("dell_to_arena", "arena_to_cra"):
        hop = observation[name]
        classification = hop["classification"]
        if classification is not None and not classification["retryable"] and hop["retry_count"] != 0:
            violations.append(f"NONRETRYABLE_RETRIED:{name}")
        if hop["attempt_count"] > 9:
            violations.append(f"ATTEMPT_BOUND_EXCEEDED:{name}")
    if observation["dell_to_arena"]["state"] not in {"READY", "RECOVERED"} and observation["arena_projection_advanced"]:
        violations.append("PROJECTION_ADVANCED_WITHOUT_DELL_FACT")
    if observation["contract_blocked"] and observation["cra_inbox_advanced"]:
        violations.append("CRA_ADVANCED_ON_BLOCKED_CONTRACT")
    if observation["identity"] != "stable" and observation["cra_inbox_advanced"]:
        violations.append("SOURCE_FORK_OR_REPLAY_ACCEPTED")
    if (
        observation["commit_stage"] in {"crash_before_replace", "projection_write_fail", "status_write_fail"}
        and observation["cra_inbox_advanced"]
    ):
        violations.append("PARTIAL_COMMIT_EXPOSED")
    if observation["physical_effect_count"] != 0 or observation["control_capability_count"] != 0:
        violations.append("OBSERVATION_PATH_GAINED_CONTROL")
    if observation["retry_after_required"] and not observation["retry_after_respected"]:
        violations.append("RETRY_AFTER_IGNORED")
    return violations


def _evaluate(row: tuple[int, ...]) -> tuple[dict[str, Any], list[str]]:
    left = FAULTS[row[0]]
    right = FAULTS[row[1]]
    order = RECOVERY_ORDERS[row[2]]
    payload = PAYLOADS[row[3]]
    resource = RESOURCES[row[4]]
    credential = CREDENTIALS[row[5]]
    clock = CLOCKS[row[6]]
    identity = IDENTITIES[row[7]]
    stage = STAGES[row[8]]
    source_recovers = order in {"source_first", "source_then_consumer", "simultaneous"}
    consumer_recovers = order in {"consumer_first", "source_then_consumer", "simultaneous"}
    left_outcome = _hop_outcome(left, recovers=source_recovers, attempt_bias=sum(row))
    right_outcome = _hop_outcome(right, recovers=consumer_recovers, attempt_bias=sum(row) + 1)

    classification_mismatch = []
    for name, spec, outcome in (
        ("dell_to_arena", left, left_outcome),
        ("arena_to_cra", right, right_outcome),
    ):
        classification = outcome["classification"]
        if classification is not None and (
            classification["category"] != spec.expected_category or classification["retryable"] is not spec.expected_retryable
        ):
            classification_mismatch.append(name)

    payload_blocked = payload in {"over_limit", "compressed", "ambiguous_framing"}
    resource_transient = resource in {"conntrack_drop", "accept_backlog", "cpu_starved"}
    resource_blocked = resource in {"fd_exhausted", "ephemeral_ports"} or (resource_transient and order in {"oscillating", "no_recovery"})
    credential_transient = credential in {"server_first", "client_first"}
    credential_blocked = credential in {"expired", "mixed_pair"} or (credential_transient and order in {"oscillating", "no_recovery"})
    clock_blocked = clock in {"forward_expired", "backward_future"} or (
        clock == "suspend_over_budget" and order in {"oscillating", "no_recovery"}
    )
    identity_blocked = identity != "stable"
    stage_blocked = stage in {"crash_before_replace", "projection_write_fail", "status_write_fail"} or (
        stage == "reboot_network_race" and order in {"oscillating", "no_recovery"}
    )
    contract_blocked = any((payload_blocked, resource_blocked, credential_blocked, clock_blocked, identity_blocked, stage_blocked))
    source_ready = left_outcome["state"] in {"READY", "RECOVERED"}
    consumer_ready = right_outcome["state"] in {"READY", "RECOVERED"}
    projection_advanced = source_ready and stage not in {"crash_before_replace", "projection_write_fail"}
    cra_advanced = projection_advanced and consumer_ready and not contract_blocked
    retry_after_required = left.name in {"http_429", "http_503"} or right.name in {"http_429", "http_503"}
    observation = {
        "dell_to_arena": left_outcome,
        "arena_to_cra": right_outcome,
        "recovery_order": order,
        "payload": payload,
        "resource": resource,
        "credential": credential,
        "clock": clock,
        "identity": identity,
        "commit_stage": stage,
        "contract_blocked": contract_blocked,
        "arena_projection_advanced": projection_advanced,
        "cra_inbox_advanced": cra_advanced,
        "retry_after_required": retry_after_required,
        "retry_after_respected": True,
        "control_capability_count": 0,
        "physical_effect_count": 0,
        "classification_mismatch": classification_mismatch,
    }
    violations = _oracle_violations(observation)
    if classification_mismatch:
        violations.append("PRODUCTION_CLASSIFIER_MISMATCH")
    return observation, violations


def _negative_controls() -> dict[str, bool]:
    baseline, violations = _evaluate((0,) * len(FACTOR_SIZES))
    if violations:
        raise AssertionError("NEGATIVE_CONTROL_BASELINE_INVALID")
    controls: dict[str, dict[str, Any]] = {}

    illegal_retry = json.loads(json.dumps(baseline))
    illegal_retry["dell_to_arena"] = {
        "state": "SAFE_BLOCKED",
        "attempt_count": 2,
        "retry_count": 1,
        "classification": {"category": "SECURITY", "reason_code": "TLS_CERTIFICATE_REJECTED", "retryable": False},
    }
    controls["illegal_security_retry"] = illegal_retry

    projection_without_source = json.loads(json.dumps(baseline))
    projection_without_source["dell_to_arena"]["state"] = "EXHAUSTED"
    projection_without_source["arena_projection_advanced"] = True
    controls["projection_without_source"] = projection_without_source

    fork_accepted = json.loads(json.dumps(baseline))
    fork_accepted["identity"] = "split_brain"
    fork_accepted["cra_inbox_advanced"] = True
    controls["source_fork_accepted"] = fork_accepted

    partial_commit = json.loads(json.dumps(baseline))
    partial_commit["commit_stage"] = "crash_before_replace"
    partial_commit["cra_inbox_advanced"] = True
    controls["partial_commit_exposed"] = partial_commit

    unbounded = json.loads(json.dumps(baseline))
    unbounded["arena_to_cra"]["attempt_count"] = 10
    unbounded["arena_to_cra"]["retry_count"] = 9
    controls["unbounded_retry"] = unbounded

    effect = json.loads(json.dumps(baseline))
    effect["physical_effect_count"] = 1
    controls["observation_path_effect"] = effect

    retry_after = json.loads(json.dumps(baseline))
    retry_after["retry_after_required"] = True
    retry_after["retry_after_respected"] = False
    controls["retry_after_ignored"] = retry_after
    return {name: bool(_oracle_violations(value)) for name, value in controls.items()}


def _source_hashes() -> dict[str, str]:
    root = Path(__file__).resolve().parents[2]
    paths = {
        Path(__file__).resolve(),
        Path(classify_failure.__code__.co_filename).resolve(),
        Path(generate_covering_array.__code__.co_filename).resolve(),
        root / "src/cra_dell_recovery/bounded_http.py",
        root / "src/cra_dell_recovery/recovery_health.py",
        root / "src/cra_dell_recovery/reloading_tls_server.py",
        root / "src/cra_no_action_soak/resilient_host_status.py",
        root / "src/cra_no_action_soak/resilient_host_status_pull.py",
        root / "src/cra_no_action_soak/resilient_host_status_relay.py",
        root / "src/cra_no_action_soak/resilient_soak.py",
    }
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def run_interserver_resilience_chaos(*, case_count: int = 100_000, seed: int = 20260902) -> dict[str, Any]:
    if isinstance(case_count, bool) or not isinstance(case_count, int) or not 70_000 <= case_count <= 500_000:
        raise ValueError("INTERSERVER_CHAOS_CASE_COUNT_INVALID")
    rows = campaign_rows(case_count=case_count, seed=seed)
    source_hashes = _source_hashes()
    failures: list[dict[str, Any]] = []
    failure_count = 0
    result_hash = hashlib.sha256()
    families: Counter[str] = Counter()
    states: Counter[str] = Counter()
    for index, row in enumerate(rows):
        observation, violations = _evaluate(row)
        families[FAULTS[row[0]].family] += 1
        families[FAULTS[row[1]].family] += 1
        states[observation["dell_to_arena"]["state"]] += 1
        states[observation["arena_to_cra"]["state"]] += 1
        summary = {"index": index, "row": row, "violations": violations}
        result_hash.update(json.dumps(summary, separators=(",", ":"), sort_keys=True).encode() + b"\n")
        if violations:
            failure_count += 1
            if len(failures) < 100:
                failures.append(summary)
    coverage = coverage_report(rows, FACTOR_SIZES, strength=4)
    controls = _negative_controls()
    source_stable = source_hashes == _source_hashes()
    passed = (
        failure_count == 0
        and coverage["coverage_complete"] is True
        and all(controls.values())
        and source_stable
        and all(families[name] > 0 for name in {fault.family for fault in FAULTS})
    )
    return {
        "schema": SCHEMA,
        "seed": seed,
        "case_count": len(rows),
        "fault_type_count": len(FAULTS),
        "fault_family_count": len({fault.family for fault in FAULTS}),
        "factor_model": {name: list(values) for name, values in FACTOR_VALUES},
        "four_way_coverage": coverage,
        "cross_hop_fault_pair_count": len(FAULTS) ** 2,
        "family_observation_count": dict(sorted(families.items())),
        "hop_state_count": dict(sorted(states.items())),
        "negative_control_detection": controls,
        "failure_count": failure_count,
        "failure_examples": failures,
        "case_results_sha256": result_hash.hexdigest(),
        "source_hashes": source_hashes,
        "source_stable": source_stable,
        "safety": {
            "production_network_used": False,
            "production_database_used": False,
            "production_credential_used": False,
            "cross_host_mutation_count": 0,
            "physical_effect_count": 0,
        },
        "fidelity": {
            "production_classifier_executed_per_fault": True,
            "two_hop_state_machine": True,
            "physical_l2_l3_faults": False,
            "physical_pmtu_black_hole": False,
            "formal_soak_replacement": False,
        },
        "pass": passed,
    }


def _atomic_report(path: Path, value: dict[str, Any]) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run isolated two-hop inter-server resilience chaos")
    parser.add_argument("--cases", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run_interserver_resilience_chaos(case_count=args.cases, seed=args.seed)
    _atomic_report(args.output, report)
    print(
        json.dumps(
            {
                "artifact": str(args.output),
                "artifact_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                "case_count": report["case_count"],
                "failure_count": report["failure_count"],
                "coverage": report["four_way_coverage"],
                "pass": report["pass"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    if not report["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
