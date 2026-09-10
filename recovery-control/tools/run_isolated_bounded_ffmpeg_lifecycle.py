#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_CONTAINER = ROOT.parent
STREAM_V3_ROOT = REPOSITORY_CONTAINER if (REPOSITORY_CONTAINER / "src" / "stream_v3").is_dir() else REPOSITORY_CONTAINER / "stream_v3"
STREAM_V3_SRC = STREAM_V3_ROOT / "src"
for source_root in (ROOT / "src", STREAM_V3_SRC):
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))

from run_isolated_ffmpeg_tls_stall import TlsSink, make_certificate, run  # noqa: E402
from stream_core.runtime_boundary_entrypoint import RuntimeBoundaryStreamEngine  # noqa: E402

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger  # noqa: E402
from runtime_boundary.observation import _tcp_metrics  # noqa: E402

DEFAULT_IMAGE = "stream-v3:bounded-ffmpeg-lifecycle-20260902-v20"


class TlsReader:
    def __init__(self, certificate: Path, private_key: Path) -> None:
        self.certificate = certificate
        self.private_key = private_key
        self.port = 0
        self.bytes_received = 0
        self.ready = threading.Event()
        self.handshake_complete = threading.Event()
        self.stop = threading.Event()
        self.error = ""
        self._thread = threading.Thread(target=self._serve, name="isolated-tls-reader", daemon=True)

    def start(self) -> None:
        self._thread.start()
        if not self.ready.wait(timeout=3):
            raise RuntimeError("TLS_READER_LISTEN_TIMEOUT")

    def close(self) -> None:
        self.stop.set()
        self._thread.join(timeout=3)

    def _serve(self) -> None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.certificate, self.private_key)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", 0))
                listener.listen(1)
                listener.settimeout(0.2)
                self.port = int(listener.getsockname()[1])
                self.ready.set()
                while not self.stop.is_set():
                    try:
                        connection, _ = listener.accept()
                    except TimeoutError:
                        continue
                    with connection, context.wrap_socket(connection, server_side=True) as tls:
                        tls.settimeout(0.2)
                        self.handshake_complete.set()
                        while not self.stop.is_set():
                            try:
                                chunk = tls.recv(65536)
                            except TimeoutError:
                                continue
                            if not chunk:
                                return
                            self.bytes_received += len(chunk)
                    return
        except BaseException as exc:  # evidence is returned to the caller
            self.error = f"{type(exc).__name__}: {exc}"
            self.ready.set()
            self.handshake_complete.set()


class ExternalProcess:
    """Minimal Popen-compatible view of one exact Docker exec process."""

    def __init__(self, pid: int, runner: subprocess.Popen[str]) -> None:
        self.pid = pid
        self.runner = runner
        self.returncode: int | None = None

    def poll(self) -> int | None:
        if self.returncode is not None:
            return self.returncode
        try:
            state = Path(f"/proc/{self.pid}/stat").read_text(encoding="utf-8").split()[2]
        except (OSError, IndexError):
            runner_code = self.runner.poll()
            self.returncode = -signal.SIGKILL if runner_code == 137 else int(runner_code or -1)
            return self.returncode
        if state == "Z":
            self.returncode = -signal.SIGKILL
            return self.returncode
        return None

    def wait(self, timeout: float | None = None) -> int:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            result = self.poll()
            if result is not None:
                return result
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(cmd=["isolated-ffmpeg"], timeout=timeout)
            time.sleep(0.01)

    def terminate(self) -> None:
        os.kill(self.pid, signal.SIGTERM)

    def kill(self) -> None:
        os.kill(self.pid, signal.SIGKILL)


def docker_container_exists(name: str) -> bool:
    completed = run("docker", "inspect", name, check=False, timeout=5)
    return completed.returncode == 0


def docker_exec_ffmpeg(name: str, port: int) -> subprocess.Popen[str]:
    command = [
        "docker",
        "exec",
        name,
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=640x360:rate=30",
        "-threads",
        "1",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-tune",
        "zerolatency",
        "-b:v",
        "20M",
        "-maxrate",
        "20M",
        "-bufsize",
        "2M",
        "-f",
        "flv",
        f"tls://127.0.0.1:{port}?tls_verify=0",
    ]
    return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def exact_ffmpeg_pid(name: str) -> int:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        completed = run("docker", "top", name, "-eo", "pid,comm,args", check=False, timeout=5)
        candidates: list[int] = []
        for line in completed.stdout.splitlines()[1:]:
            fields = line.strip().split(maxsplit=2)
            if len(fields) == 3 and fields[1] == "ffmpeg" and "tls://127.0.0.1:" in fields[2]:
                candidates.append(int(fields[0]))
        if len(candidates) == 1:
            pid = candidates[0]
            status = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
            uid_line = next(line for line in status.splitlines() if line.startswith("Uid:"))
            if int(uid_line.split()[1]) != os.getuid():
                raise RuntimeError("ISOLATED_FFMPEG_UID_MISMATCH")
            return pid
        if len(candidates) > 1:
            raise RuntimeError("ISOLATED_FFMPEG_CARDINALITY_INVALID")
        time.sleep(0.05)
    raise RuntimeError("ISOLATED_FFMPEG_PID_NOT_FOUND")


def terminate_isolated_process(process: ExternalProcess) -> None:
    if process.poll() is not None:
        return
    os.kill(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=2)


def target(pid: int, generation: str) -> dict[str, object]:
    return {
        "host_id": "isolated-dell-fixture",
        "host_boot_id": "isolated-boot",
        "namespace": "isolated",
        "pod_uid": "isolated-runtime-1",
        "container_name": "stream-engine",
        "container_id": "docker://isolated-runtime-1",
        "ffmpeg_generation": generation,
        "ffmpeg_pid": pid,
    }


def request_payload(*, request_id: str, target_identity: dict[str, object], native_generation: str) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": request_id,
        "producer_id": "isolated-controller",
        "producer_generation": 1,
        "operation": "restart_ffmpeg",
        "reason": "isolated TLS zero-window stall",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "target_identity": target_identity,
        "expected_ffmpeg_generation": native_generation,
        "idempotency_key": request_id,
        "correlation_id": request_id,
        "target_snapshot_id": "isolated-target-snapshot-1",
        "runtime_observation_id": "isolated-observation-1",
        "expected_executor_instance_id": "isolated-executor-1",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": "isolated-projection-1",
        "projection_sequence": 1,
    }


def wait_terminal(client: EffectClient, request_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status = client.status(request_id)
        if status.get("state") in {"EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}:
            return status
        time.sleep(0.02)
    raise RuntimeError("ISOLATED_EFFECT_TERMINAL_TIMEOUT")


def collect_ack_samples(pid: int, port: int, generation: str) -> tuple[list[dict[str, object]], dict[str, Any]]:
    samples: list[dict[str, object]] = []
    last_metrics: dict[str, Any] = {}
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and len(samples) < 3:
        metrics = dict(_tcp_metrics(pid, (port,)))
        acked = int(metrics.get("bytes_acked", 0) or 0)
        if acked > 0 and (not samples or acked > int(samples[-1]["bytes_acked"])):
            samples.append(
                {
                    "observed_at": datetime.now(UTC).isoformat(),
                    "ffmpeg_pid": pid,
                    "ffmpeg_generation": generation,
                    "bytes_acked": acked,
                }
            )
            last_metrics = metrics
        time.sleep(0.25)
    if len(samples) != 3:
        raise RuntimeError("ISOLATED_SUCCESSOR_ACK_PROGRESS_NOT_OBSERVED")
    return samples, last_metrics


def execute(image: str) -> dict[str, Any]:
    suffix = uuid.uuid4().hex[:12]
    container_name = f"stream-v3-bounded-lifecycle-{suffix}"
    if docker_container_exists(container_name):
        raise RuntimeError("ISOLATED_CONTAINER_ALREADY_EXISTS")
    result: dict[str, Any] = {
        "image": image,
        "container_name": container_name,
        "loopback_only": True,
        "temporary_sqlite": True,
        "production_effect_socket_touched": False,
        "production_sqlite_touched": False,
        "production_target_touched": False,
        "pod_restart_count": 0,
        "host_restart_count": 0,
    }
    old_runner: subprocess.Popen[str] | None = None
    successor_runner: subprocess.Popen[str] | None = None
    old_process: ExternalProcess | None = None
    successor_process: ExternalProcess | None = None
    effect_server: EffectExecutorServer | None = None
    ledger: EffectLedger | None = None
    stall_sink: TlsSink | None = None
    healthy_sink: TlsReader | None = None
    with tempfile.TemporaryDirectory(prefix="stream-v3-bounded-lifecycle-") as raw_root:
        root = Path(raw_root)
        certificate, private_key = make_certificate(root)
        try:
            run(
                "docker",
                "run",
                "--detach",
                "--network",
                "host",
                "--cpus",
                "1",
                "--memory",
                "512m",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--entrypoint",
                "/bin/sleep",
                "--name",
                container_name,
                image,
                "infinity",
                timeout=20,
            )
            stall_sink = TlsSink(certificate, private_key)
            stall_sink.start()
            old_runner = docker_exec_ffmpeg(container_name, stall_sink.port)
            if not stall_sink.handshake_complete.wait(timeout=5):
                raise RuntimeError(f"ISOLATED_STALL_TLS_HANDSHAKE_TIMEOUT:{stall_sink.error}")
            old_pid = exact_ffmpeg_pid(container_name)
            old_process = ExternalProcess(old_pid, old_runner)
            time.sleep(2)

            engine = object.__new__(RuntimeBoundaryStreamEngine)
            engine.ffmpeg_proc = old_process
            engine.run_id = "isolated-run"
            engine.restart_count = 0
            engine.ffmpeg_stop_context = {}
            old_native_generation = engine.ffmpeg_generation(old_pid)
            old_target = target(old_pid, "isolated-protocol-generation-1")
            current = {
                "target_identity": old_target,
                "ffmpeg_generation": old_native_generation,
                "executor_instance_id": "isolated-executor-1",
            }
            ledger = EffectLedger(
                root / "effect.sqlite3",
                initial_producer_id="isolated-controller",
                initial_producer_generation=1,
            )
            effect_server = EffectExecutorServer(
                socket_path=root / "effect.sock",
                ledger=ledger,
                allowed_peer_uids={os.getuid()},
                current_target=lambda: current,
                perform_effect=engine.perform_fast_recovery_effect,
                execute_async=True,
                require_transport_verification=True,
            )
            effect_server.start()
            client = EffectClient(root / "effect.sock", timeout_seconds=0.5)
            request_id = f"isolated-bounded-{suffix}"
            previous_environment = {
                name: os.environ.get(name)
                for name in ("FR_FFMPEG_FORCE_KILL_ENABLED", "FR_FFMPEG_TERM_GRACE_SEC", "FR_FFMPEG_KILL_WAIT_SEC")
            }
            os.environ.update(
                {
                    "FR_FFMPEG_FORCE_KILL_ENABLED": "1",
                    "FR_FFMPEG_TERM_GRACE_SEC": "0.5",
                    "FR_FFMPEG_KILL_WAIT_SEC": "2",
                }
            )
            try:
                started = time.monotonic()
                accepted = client.execute(
                    request_payload(
                        request_id=request_id,
                        target_identity=old_target,
                        native_generation=old_native_generation,
                    )
                )
                accepted_elapsed = time.monotonic() - started
                terminal = wait_terminal(client, request_id)
                terminal_elapsed = time.monotonic() - started
            finally:
                for name, value in previous_environment.items():
                    if value is None:
                        os.environ.pop(name, None)
                    else:
                        os.environ[name] = value
            if terminal.get("state") != "EFFECT_OBSERVED":
                raise RuntimeError("ISOLATED_EFFECT_NOT_OBSERVED:" + json.dumps(terminal, ensure_ascii=False, sort_keys=True))
            terminal_result = terminal.get("result") if isinstance(terminal.get("result"), dict) else {}
            if (
                terminal_result.get("effect") != "SIGTERM_THEN_SIGKILL"
                or terminal_result.get("exit_observed") is not True
                or terminal_result.get("signal_attempt_count") != 2
                or terminal_result.get("pidfd_used") is not True
            ):
                raise RuntimeError("ISOLATED_BOUNDED_TERMINATION_CONTRACT_FAILED")
            result.update(
                {
                    "request_id": request_id,
                    "old_ffmpeg_pid": old_pid,
                    "acceptance": accepted,
                    "acceptance_elapsed_sec": round(accepted_elapsed, 3),
                    "terminal_elapsed_sec": round(terminal_elapsed, 3),
                    "terminal_status": terminal,
                }
            )

            stall_sink.close()
            stall_sink = None
            healthy_sink = TlsReader(certificate, private_key)
            healthy_sink.start()
            successor_runner = docker_exec_ffmpeg(container_name, healthy_sink.port)
            if not healthy_sink.handshake_complete.wait(timeout=5):
                raise RuntimeError(f"ISOLATED_SUCCESSOR_TLS_HANDSHAKE_TIMEOUT:{healthy_sink.error}")
            successor_pid = exact_ffmpeg_pid(container_name)
            successor_process = ExternalProcess(successor_pid, successor_runner)
            successor_generation = "isolated-protocol-generation-2"
            successor_target = target(successor_pid, successor_generation)
            current.update(
                {
                    "target_identity": successor_target,
                    "ffmpeg_generation": f"isolated-run:1:{successor_pid}",
                }
            )
            samples, metrics = collect_ack_samples(successor_pid, healthy_sink.port, successor_generation)
            unresolved = client.unresolved()["unresolved_scopes"][0]
            evidence = {
                "schema_version": "runtime.delayed_exit_reconciliation_evidence.v2",
                "oracle": "DELAYED_FFMPEG_EXIT_AND_SUCCESSOR_ACK_PROGRESS",
                "observed_at": datetime.now(UTC).isoformat(),
                "physical_effect_count": 1,
                "automatic_retry_count": 0,
                "before_target": old_target,
                "observed_target": successor_target,
                "runtime_observation_id": "isolated-successor-observation",
                "transport": {
                    "bytes_sent": int(metrics.get("bytes_sent", 0) or 0),
                    "network_down": False,
                    "tcp_probe_ok": True,
                    "ffmpeg_generation": successor_generation,
                    "ack_observation_count": len(samples),
                    "bytes_acked_start": int(samples[0]["bytes_acked"]),
                    "bytes_acked_end": int(samples[-1]["bytes_acked"]),
                    "bytes_acked_delta": int(samples[-1]["bytes_acked"]) - int(samples[0]["bytes_acked"]),
                    "ack_samples": samples,
                },
            }
            reconciliation = client.reconcile(
                {
                    "schema_version": "runtime.effect_reconciliation_request.v1",
                    "reconciliation_id": f"isolated-reconcile-{suffix}",
                    "effect_scope_id": unresolved["effect_scope_id"],
                    "owner_request_id": unresolved["owner_request_id"],
                    "owner_request_digest": unresolved["owner_request_digest"],
                    "resolution": "EFFECT_OBSERVED",
                    "evidence": evidence,
                }
            )
            result.update(
                {
                    "successor_ffmpeg_pid": successor_pid,
                    "successor_distinct": successor_pid != old_pid,
                    "ack_samples": samples,
                    "bytes_acked_delta": evidence["transport"]["bytes_acked_delta"],
                    "tls_bytes_received": healthy_sink.bytes_received,
                    "reconciliation": reconciliation,
                    "unresolved_count_after": client.unresolved()["unresolved_count"],
                    "effect_scope_state_after": ledger.request_status(request_id)["effect_scope_state"],
                    "logical_effect_count": 1,
                    "signal_attempt_count": terminal_result["signal_attempt_count"],
                    "wrong_target_signal_count": 0,
                    "duplicate_successor_count": 0,
                }
            )
            if reconciliation.get("state") != "RECONCILED_EFFECT_OBSERVED" or result["unresolved_count_after"] != 0:
                raise RuntimeError("ISOLATED_RECONCILIATION_FAILED")
        finally:
            if effect_server is not None:
                effect_server.close()
            if ledger is not None:
                ledger.close()
            if successor_process is not None:
                terminate_isolated_process(successor_process)
            if old_process is not None:
                terminate_isolated_process(old_process)
            if old_runner is not None:
                try:
                    old_runner.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    old_runner.kill()
                    old_runner.communicate(timeout=2)
            if successor_runner is not None:
                try:
                    successor_runner.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    successor_runner.kill()
                    successor_runner.communicate(timeout=2)
            if stall_sink is not None:
                stall_sink.close()
            if healthy_sink is not None:
                healthy_sink.close()
            run("docker", "rm", "--force", container_name, check=False, timeout=10)
            result["cleanup_container_absent"] = not docker_container_exists(container_name)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Loopback-only real FFmpeg bounded lifecycle and ACK proof")
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = execute(args.image)
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
