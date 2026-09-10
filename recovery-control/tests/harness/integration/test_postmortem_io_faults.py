"""Postmortem-derived faults: temporary files and loopback mTLS only.

Assertions use captured HTTP calls, persisted bytes and restart sequence, not
the implementation's health labels alone. No production configuration is read.
"""

from __future__ import annotations

import errno
import io
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from datetime import timedelta
from email.message import Message
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

import pytest

from cra_dell_recovery.canonical import Signer
from cra_dell_recovery.json_input import load_object
from cra_dell_recovery.reloading_tls_server import ReloadingTLSHTTPServer
from cra_no_action_soak import recovery_soak, recovery_transport, resilient_soak
from cra_no_action_soak.recovery_facts import strict_json_object
from cra_no_action_soak.recovery_soak import Config
from tests.harness.unit.test_recovery_soak import START, packet
from tests.harness.unit.test_recovery_soak import setup as setup
from tests.integration.test_mtls_transport import mtls_contexts
from tests.recovery.test_bounded_http import _Clock, _puller, _Response

BAD_JSON = {
    "deep": b'{"x":' + b"[" * 40 + b"0" + b"]" * 40 + b"}",
    "recursion": b'{"x":' + b"[" * 1200 + b"0" + b"]" * 1200 + b"}",
    "overflow": b'{"x":1e999}',
    "surrogate": b'{"x":"\\ud800"}',
    "encoding": '{"x":1}'.encode("utf-16"),
}


@pytest.mark.parametrize("fault", BAD_JSON)
def test_recovery_parser_rejects_noncanonical_or_unbounded_input(fault: str) -> None:
    with pytest.raises(ValueError):
        strict_json_object(BAD_JSON[fault])


def test_json_parser_enforces_exact_byte_depth_and_node_boundaries() -> None:
    raw = b'{"x":"bounded"}'
    assert load_object(raw, maximum_bytes=len(raw)) == {"x": "bounded"}
    with pytest.raises(ValueError, match="TOO_LARGE"):
        load_object(raw, maximum_bytes=len(raw) - 1)

    depth_32 = b'{"x":' + b"[" * 31 + b"0" + b"]" * 31 + b"}"
    assert strict_json_object(depth_32)["x"]
    depth_33 = b'{"x":' + b"[" * 32 + b"0" + b"]" * 32 + b"}"
    with pytest.raises(ValueError, match="TOO_DEEP"):
        strict_json_object(depth_33)

    nodes_32768 = {"x": [0] * 32765}
    assert load_object(json.dumps(nodes_32768).encode(), maximum_bytes=2 * 1024 * 1024)["x"][-1] == 0
    nodes_32769 = {"x": [0] * 32766}
    with pytest.raises(ValueError, match="TOO_MANY_NODES"):
        load_object(json.dumps(nodes_32769).encode(), maximum_bytes=2 * 1024 * 1024)


@pytest.mark.parametrize("fault", ["recursion", "overflow", "surrogate"])
def test_bad_inbox_is_recorded_without_stopping_other_roles(setup: tuple[Config, dict[str, Signer]], fault: str) -> None:
    config, signers = setup
    for role in config.bindings:
        recovery_soak.atomic_write_json(Path(config.value["hosts"][role]["inbox_file"]), packet(config, signers, role, 0))
    Path(config.value["hosts"]["dell"]["inbox_file"]).write_bytes(BAD_JSON[fault])
    sample = recovery_soak.collect(config, now=START)
    assert sample["input_errors"] == {"dell": "INPUT_INVALID"}
    assert sample["inputs"]["dell"] is None
    assert sample["inputs"]["arena"]["sequence"] == 1
    assert len(Path(config.value["evidence_file"]).read_bytes().splitlines()) == 1
    result = recovery_soak.gate(config, now=START)
    assert result["eligible"] is False
    assert result["status"] != "PASS"


@pytest.mark.parametrize("fault", ["chunk_truncated", "bad_status"])
def test_protocol_failure_does_not_suppress_next_role(tmp_path: Path, fault: str) -> None:
    server_context, client_context, _ = mtls_contexts(tmp_path)
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802
            requests.append(self.path)
            if self.path == recovery_transport.ROUTES["dell"]:
                if fault == "bad_status":
                    self.wfile.write(b"BROKEN STATUS\r\n\r\n")
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    self.wfile.write(b'10\r\n{"cut":')
                self.wfile.flush()
                self.close_connection = True
                return
            body = b'{"sequence":7,"signature":"original"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ReloadingTLSHTTPServer(("127.0.0.1", 0), Handler, server_context, connection_timeout_seconds=1)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    config = {
        "sources": {
            role: {
                "url": f"https://127.0.0.1:{server.server_address[1]}" + recovery_transport.ROUTES[role],
                "output_file": str(tmp_path / (role + "-inbox.json")),
            }
            for role in ("dell", "arena")
        },
        "status_file": str(tmp_path / "transport-status.json"),
    }
    try:
        result = recovery_transport.pull(config, client_context)
        assert requests == [recovery_transport.ROUTES[r] for r in ("dell", "arena")]
        assert result["roles"] == {"dell": "UNAVAILABLE", "arena": "RECEIVED_UNTRUSTED"}
        assert not Path(config["sources"]["dell"]["output_file"]).exists()
        assert json.loads(Path(config["sources"]["arena"]["output_file"]).read_bytes())["sequence"] == 7
        assert json.loads(Path(config["status_file"]).read_bytes()) == result
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def test_two_hop_loopback_mtls_recovers_without_fabricating_missing_dell_packet(tmp_path: Path) -> None:
    server_context, client_context, _ = mtls_contexts(tmp_path)
    source_packet = b'{"sequence":19,"signature":"dell-original"}'
    arena_packet = tmp_path / "arena-original.json"
    recovery_soak.atomic_write_json(arena_packet, {"sequence": 23, "signature": "arena-original"})
    relayed_dell = tmp_path / "arena-relayed-dell.json"
    dell_requests: list[str] = []

    class DellHandler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802
            dell_requests.append(self.path)
            if len(dell_requests) == 1:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                self.wfile.write(b'10\r\n{"cut":')
                self.wfile.flush()
                self.close_connection = True
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(source_packet)))
            self.end_headers()
            self.wfile.write(source_packet)

    dell_server = ReloadingTLSHTTPServer(("127.0.0.1", 0), DellHandler, server_context, connection_timeout_seconds=1)
    arena_server = ReloadingTLSHTTPServer(
        ("127.0.0.1", 0),
        recovery_transport.handler_for({"dell": str(relayed_dell), "arena": str(arena_packet)}),
        server_context,
        connection_timeout_seconds=1,
    )
    servers = (dell_server, arena_server)
    threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
    for thread in threads:
        thread.start()
    arena_pull = {
        "sources": {
            "dell": {
                "url": f"https://127.0.0.1:{dell_server.server_address[1]}{recovery_transport.ROUTES['dell']}",
                "output_file": str(relayed_dell),
            }
        },
        "status_file": str(tmp_path / "arena-pull-status.json"),
    }
    cra_pull = {
        "sources": {
            role: {
                "url": f"https://127.0.0.1:{arena_server.server_address[1]}{recovery_transport.ROUTES[role]}",
                "output_file": str(tmp_path / f"cra-{role}.json"),
            }
            for role in ("dell", "arena")
        },
        "status_file": str(tmp_path / "cra-pull-status.json"),
    }
    try:
        assert recovery_transport.pull(arena_pull, client_context)["roles"] == {"dell": "UNAVAILABLE"}
        assert not relayed_dell.exists()
        first = recovery_transport.pull(cra_pull, client_context)
        assert first["roles"] == {"dell": "UNAVAILABLE", "arena": "RECEIVED_UNTRUSTED"}
        assert not (tmp_path / "cra-dell.json").exists()
        assert (tmp_path / "cra-arena.json").read_bytes() == arena_packet.read_bytes()

        assert recovery_transport.pull(arena_pull, client_context)["roles"] == {"dell": "RECEIVED_UNTRUSTED"}
        assert relayed_dell.read_bytes().rstrip(b"\n") == source_packet
        recovered = recovery_transport.pull(cra_pull, client_context)
        assert recovered["roles"] == {"dell": "RECEIVED_UNTRUSTED", "arena": "RECEIVED_UNTRUSTED"}
        assert (tmp_path / "cra-dell.json").read_bytes() == relayed_dell.read_bytes()
        assert dell_requests == [recovery_transport.ROUTES["dell"]] * 2
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=3)
            assert not thread.is_alive()


def test_signed_two_hop_fault_reaches_collector_recovery_and_rejects_tampering(
    setup: tuple[Config, dict[str, Signer]],
    tmp_path: Path,
) -> None:
    config, signers = setup
    server_context, client_context, _ = mtls_contexts(tmp_path)
    source = tmp_path / "producer"
    source.mkdir()
    relayed = tmp_path / "relay/dell.json"
    fault = False
    requests: list[bool] = []
    normal = recovery_transport.handler_for({"dell": str(source / "dell.json")})

    class DellHandler(normal):
        def do_GET(self) -> None:  # noqa: N802
            requests.append(fault)
            if fault:
                self.send_response(200)
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                self.wfile.write(b'10\r\n{"cut":')
                self.wfile.flush()
                self.close_connection = True
            else:
                super().do_GET()

    dell_server = ReloadingTLSHTTPServer(("127.0.0.1", 0), DellHandler, server_context, connection_timeout_seconds=1)
    arena_server = ReloadingTLSHTTPServer(
        ("127.0.0.1", 0),
        recovery_transport.handler_for({"dell": str(relayed), "arena": str(source / "arena.json")}),
        server_context,
        connection_timeout_seconds=1,
    )
    servers = (dell_server, arena_server)
    threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
    for thread in threads:
        thread.start()
    arena_pull = {
        "sources": {
            "dell": {
                "url": f"https://127.0.0.1:{dell_server.server_address[1]}{recovery_transport.ROUTES['dell']}",
                "output_file": str(relayed),
            }
        },
        "status_file": str(tmp_path / "arena-pull-status.json"),
    }
    cra_pull = {
        "sources": {
            role: {
                "url": f"https://127.0.0.1:{arena_server.server_address[1]}{recovery_transport.ROUTES[role]}",
                "output_file": config.value["hosts"][role]["inbox_file"],
            }
            for role in ("dell", "arena")
        },
        "status_file": str(tmp_path / "cra-pull-status.json"),
    }
    samples = []
    try:
        for seconds in range(0, 181, 15):
            fault = seconds == 45
            for role in config.bindings:
                changes = {"transport": {"target": "target-2"}} if role == "dell" and seconds >= 60 else {}
                if role == "arena" and fault:
                    changes = {"platform": {"state": "DOWN"}}
                value = packet(config, signers, role, seconds, **changes)
                destination = source / f"{role}.json" if role in ("dell", "arena") else Path(config.value["hosts"][role]["inbox_file"])
                recovery_soak.atomic_write_json(destination, value)
            pull = recovery_transport.pull(arena_pull, client_context)
            assert pull["roles"]["dell"] == ("UNAVAILABLE" if fault else "RECEIVED_UNTRUSTED")
            recovery_transport.pull(cra_pull, client_context)
            sample = recovery_soak.collect(config, now=START + timedelta(seconds=seconds))
            samples.append(sample)
            if fault:
                # A still-fresh prior packet is retained with its original time;
                # it is never rewritten into a new observation.
                assert sample["inputs"]["dell"]["sequence"] == 31
        result = recovery_soak.evaluate(samples, config=config, now=START + timedelta(seconds=180))
        assert requests.count(True) == 1
        assert result["harness_classification"] == "PASS", result
        assert result["live_health"] == "READY"
        assert result["recovery"]["recovered_episode_count"] == 1
        assert result["physical_attempt_delta"] == 0
        assert "RECOVERY_NOT_EXERCISED" in result["pending_reasons"]

        changed = packet(config, signers, "dell", 195)
        changed["facts"]["effects"]["physical_attempt_count"] = 100
        recovery_soak.atomic_write_json(source / "dell.json", changed)
        recovery_transport.pull(arena_pull, client_context)
        recovery_transport.pull(cra_pull, client_context)
        bad = recovery_soak.collect(config, now=START + timedelta(seconds=195))
        rejected = recovery_soak.evaluate([*samples, bad], config=config, now=START + timedelta(seconds=195))
        assert "DELL_SIGNED_EVIDENCE_INVALID" in rejected["blockers"]
        assert rejected["status"] != "PASS"
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=3)
            assert not thread.is_alive()


@pytest.mark.parametrize("ending", ["success", "exhausted", "observer_failure"])
def test_http_error_responses_are_closed_before_retry_or_exit(tmp_path: Path, ending: str) -> None:
    clock = _Clock()
    bodies: list[io.BytesIO] = []
    errors: list[urllib.error.HTTPError] = []
    calls: list[int] = []

    def opened(*_args: Any, **_kwargs: Any) -> _Response:
        assert all(body.closed for body in bodies), "previous error response retained across retry"
        calls.append(1)
        if ending == "success" and len(calls) == 3:
            return _Response()
        body = io.BytesIO(b"untrusted HTTP error body")
        bodies.append(body)
        error = urllib.error.HTTPError("https://facts.invalid", 503, "injected", Message(), body)
        errors.append(error)  # Keep references: garbage collection is not cleanup proof.
        raise error

    def observer(*_args: Any) -> None:
        if ending == "observer_failure":
            raise OSError(errno.ENOSPC, "injected observer persistence failure")

    puller = _puller(
        tmp_path,
        open_url=opened,
        monotonic=clock.now,
        wait=clock.wait,
        jitter=lambda cap: cap,
        transient_retry_delays_seconds=(0.1,),
        maximum_retry_elapsed_seconds=0.25,
        failure_observer=observer,
    )
    if ending == "success":
        assert puller._fetch_body(urllib.request.Request(puller.endpoint_url)) == (b"{}", 3)
    else:
        with pytest.raises(urllib.error.HTTPError if ending == "exhausted" else OSError):
            puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    assert bodies and all(body.closed for body in bodies)
    assert len(calls) == (1 if ending == "observer_failure" else 3)


@pytest.mark.parametrize("prior_count", [0, 1])
def test_state_replace_failure_after_append_recovers_exactly_once(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch, prior_count: int
) -> None:
    config, _ = setup
    if prior_count:
        recovery_soak.collect(config, now=START)
    original = recovery_soak.atomic_write_json
    triggered: list[int] = []

    def fail_commit(path: Path, value: Any) -> None:
        if value.get("sample_count") == prior_count + 1:
            triggered.append(1)
            raise OSError(errno.ENOSPC, "injected state replace failure")
        original(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(recovery_soak, "atomic_write_json", fail_commit)
        with pytest.raises(OSError):
            recovery_soak.collect(config, now=START + timedelta(seconds=prior_count * 15))
    assert triggered == [1]
    evidence = Path(config.value["evidence_file"])
    committed = evidence.read_bytes()
    assert len(committed.splitlines()) == prior_count + 1
    result = recovery_soak.collect(config, now=START + timedelta(seconds=(prior_count + 1) * 15))
    assert result["sample_sequence"] == prior_count + 2
    assert evidence.read_bytes().startswith(committed)
    rows = [json.loads(raw) for raw in evidence.read_bytes().splitlines()]
    assert [row["sample_sequence"] for row in rows] == list(range(1, prior_count + 3))


def test_append_retries_short_regular_file_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "samples.jsonl"
    original = os.fdopen
    writes: list[int] = []

    class ShortWriter:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.handle = original(*args, **kwargs)

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> Any:
            return self.handle.__exit__(*args)

        def fileno(self) -> int:
            return self.handle.fileno()

        def write(self, raw: bytes) -> int:
            writes.append(len(raw))
            return int(self.handle.write(raw[:7]))

    monkeypatch.setattr(resilient_soak.os, "fdopen", ShortWriter)
    resilient_soak._append_line(path, {"sequence": 1, "proof": "complete"}, maximum_bytes=1024)
    assert writes
    assert path.read_bytes().endswith(b"\n")
    assert json.loads(path.read_bytes()) == {"sequence": 1, "proof": "complete"}


@pytest.mark.parametrize("fault", ["none_write", "zero_write", "disk_full", "io_error"])
def test_incomplete_append_never_advances_committed_state(
    setup: tuple[Config, dict[str, Signer]], monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    config, _ = setup
    recovery_soak.collect(config, now=START)
    state = Path(config.value["state_file"])
    evidence = Path(config.value["evidence_file"])
    before_state, before_bytes = state.read_bytes(), evidence.read_bytes()
    original = os.fdopen
    triggered: list[int] = []

    class FaultWriter:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.handle = original(*args, **kwargs)

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *args: Any) -> Any:
            return self.handle.__exit__(*args)

        def fileno(self) -> int:
            return self.handle.fileno()

        def write(self, raw: bytes) -> int:
            triggered.append(1)
            if len(triggered) == 1:
                return int(self.handle.write(raw[:7]))
            if fault == "none_write":
                return None  # type: ignore[return-value]
            if fault == "zero_write":
                return 0
            raise OSError(errno.ENOSPC if fault == "disk_full" else errno.EIO, "injected")

    def opened(fd: int, *args: Any, **kwargs: Any) -> Any:
        if os.readlink(f"/proc/self/fd/{fd}") == str(evidence):
            return FaultWriter(fd, *args, **kwargs)
        return original(fd, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(resilient_soak.os, "fdopen", opened)
        with pytest.raises(OSError):
            recovery_soak.collect(config, now=START + timedelta(seconds=15))
    assert len(triggered) == 2
    assert state.read_bytes() == before_state
    assert evidence.read_bytes().startswith(before_bytes)
    partial = evidence.read_bytes()
    with pytest.raises(ValueError, match="PARTIAL"):
        recovery_soak.collect(config, now=START + timedelta(seconds=30))
    assert evidence.read_bytes() == partial  # Never truncate or silently bless loss.


@pytest.mark.parametrize("failed_fsync", ["file", "directory"])
def test_complete_append_fsync_failure_recovers_one_frame_without_sequence_reuse(
    setup: tuple[Config, dict[str, Signer]],
    monkeypatch: pytest.MonkeyPatch,
    failed_fsync: str,
) -> None:
    config, _ = setup
    recovery_soak.collect(config, now=START)
    state = Path(config.value["state_file"])
    evidence = Path(config.value["evidence_file"])
    committed_state = state.read_bytes()
    original_fsync = os.fsync
    calls: list[int] = []

    def fail_at_boundary(descriptor: int) -> None:
        calls.append(descriptor)
        if len(calls) == (1 if failed_fsync == "file" else 2):
            raise OSError(errno.EIO, f"injected {failed_fsync} fsync failure")
        original_fsync(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(resilient_soak.os, "fsync", fail_at_boundary)
        with pytest.raises(OSError, match="fsync failure"):
            recovery_soak.collect(config, now=START + timedelta(seconds=15))
    assert state.read_bytes() == committed_state
    appended = evidence.read_bytes()
    assert len(appended.splitlines()) == 2

    recovered = recovery_soak.collect(config, now=START + timedelta(seconds=30))
    assert recovered["sample_sequence"] == 3
    assert evidence.read_bytes().startswith(appended)
    assert [json.loads(raw)["sample_sequence"] for raw in evidence.read_bytes().splitlines()] == [1, 2, 3]


@pytest.mark.parametrize("operation", ["tail", "append"])
def test_evidence_fifo_does_not_block_before_type_check(tmp_path: Path, operation: str) -> None:
    path = tmp_path / "evidence.fifo"
    os.mkfifo(path, 0o600)
    code = "from pathlib import Path; import sys; from cra_no_action_soak.resilient_soak import _last_line, _append_line; " + (
        "_last_line(Path(sys.argv[1]), maximum_bytes=1024)"
        if operation == "tail"
        else "_append_line(Path(sys.argv[1]), {'sample':1}, maximum_bytes=1024)"
    )
    result = subprocess.run([sys.executable, "-c", code, str(path)], capture_output=True, text=True, timeout=5)
    assert result.returncode != 0
    assert "NOT_REGULAR_FILE" in result.stderr or "No such device or address" in result.stderr


def test_first_append_survives_abrupt_test_process_exit(setup: tuple[Config, dict[str, Signer]], tmp_path: Path) -> None:
    config, _ = setup
    config.value["minimum_duration_seconds"] = 604800
    path = tmp_path / "config.json"
    recovery_soak.atomic_write_json(path, config.value)
    code = """
import os, sys
from pathlib import Path
from datetime import datetime, UTC
from cra_no_action_soak import recovery_soak as module
config = module.Config.load(Path(sys.argv[1]))
write = module.atomic_write_json
def interrupted(path, value):
    if value.get('sample_count') == 1:
        os._exit(75)
    write(path, value)
module.atomic_write_json = interrupted
module.collect(config, now=datetime(2026, 9, 4, tzinfo=UTC))
"""
    result = subprocess.run([sys.executable, "-c", code, str(path)], capture_output=True, text=True, timeout=5)
    assert result.returncode == 75, result.stderr
    config = Config.load(path)
    evidence = Path(config.value["evidence_file"])
    original = evidence.read_bytes()
    recovered = recovery_soak.collect(config, now=START + timedelta(seconds=15))
    assert recovered["sample_sequence"] == 2
    assert len(evidence.read_bytes().splitlines()) == 2
    assert evidence.read_bytes().startswith(original)


def test_negative_control_detects_error_response_leak(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(urllib.error.HTTPError, "close", lambda self: None)
    with pytest.raises(AssertionError, match="retained across retry"):
        test_http_error_responses_are_closed_before_retry_or_exit(tmp_path, "success")


@pytest.mark.parametrize("fault", ["chunk_truncated", "bad_status"])
def test_negative_control_detects_missing_protocol_exception_isolation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    monkeypatch.setattr(recovery_transport, "ROLE_LOCAL_FAILURES", (OSError, ValueError))
    with pytest.raises((recovery_transport.http.client.IncompleteRead, recovery_transport.http.client.BadStatusLine)):
        test_protocol_failure_does_not_suppress_next_role(tmp_path, fault)


def test_negative_control_detects_missing_initial_committed_head(
    setup: tuple[Config, dict[str, Signer]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = recovery_soak.atomic_write_json

    def omit_empty_head(path: Path, value: Any) -> None:
        if value.get("sample_count") != 0:
            original(path, value)

    monkeypatch.setattr(recovery_soak, "atomic_write_json", omit_empty_head)
    with pytest.raises(ValueError, match="RECOVERY_SOAK_STATE_MISSING"):
        test_state_replace_failure_after_append_recovers_exactly_once(setup, monkeypatch, 0)


def test_negative_control_detects_single_short_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resilient_soak, "_write_all", lambda handle, raw: handle.write(raw))
    with pytest.raises(AssertionError):
        test_append_retries_short_regular_file_writes(tmp_path, monkeypatch)


@pytest.mark.parametrize("operation", ["tail", "append"])
def test_negative_control_detects_blocking_fifo_open(tmp_path: Path, operation: str) -> None:
    path = tmp_path / "negative-control.fifo"
    os.mkfifo(path, 0o600)
    expression = (
        "module._last_line(Path(sys.argv[1]), maximum_bytes=1024)"
        if operation == "tail"
        else "module._append_line(Path(sys.argv[1]), {'sample':1}, maximum_bytes=1024)"
    )
    code = (
        "from pathlib import Path; import os, sys; "
        "from cra_no_action_soak import resilient_soak as module; "
        "module.os.O_NONBLOCK = 0; " + expression
    )
    with pytest.raises(subprocess.TimeoutExpired):
        subprocess.run([sys.executable, "-c", code, str(path)], capture_output=True, text=True, timeout=0.5)


def test_missing_state_with_existing_evidence_is_not_silently_reinitialized(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, _ = setup
    recovery_soak.collect(config, now=START)
    evidence, state = Path(config.value["evidence_file"]), Path(config.value["state_file"])
    original = evidence.read_bytes()
    state.unlink()  # Only the pytest-owned fixture, never a live epoch.
    with pytest.raises(ValueError, match="STATE_MISSING"):
        recovery_soak.collect(config, now=START + timedelta(seconds=15))
    assert evidence.read_bytes() == original
    assert not state.exists()


def test_negative_control_detects_permissive_json(monkeypatch: pytest.MonkeyPatch) -> None:
    from cra_no_action_soak import recovery_facts

    monkeypatch.setattr(recovery_facts, "load_object", lambda raw, **kwargs: json.loads(raw))
    with pytest.raises(pytest.fail.Exception, match="DID NOT RAISE"):
        test_recovery_parser_rejects_noncanonical_or_unbounded_input("overflow")
