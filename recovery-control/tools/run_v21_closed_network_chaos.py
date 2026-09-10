#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_CONTAINER = ROOT.parent
STREAM_V3_ROOT = REPOSITORY_CONTAINER if (REPOSITORY_CONTAINER / "src" / "stream_v3").is_dir() else REPOSITORY_CONTAINER / "stream_v3"
STREAM_V3_SRC = STREAM_V3_ROOT / "src"
RELEASE_ROOT = ROOT / "artifacts/runtime-fence-bounded-ffmpeg-lifecycle-20260903-v21"
RELEASE_MANIFEST = RELEASE_ROOT / "release_manifest.json"
for source_root in (ROOT / "src", STREAM_V3_SRC):
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))

from stream_core.runtime_boundary_entrypoint import RuntimeBoundaryStreamEngine  # noqa: E402

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger  # noqa: E402

DEFAULT_IMAGE = "stream-v3:bounded-ffmpeg-lifecycle-20260903-v21"
FIXTURE = ROOT / "tools/closed_network_fixture.py"
RELEASE_FILES = (
    "runtime_boundary/client.py",
    "runtime_boundary/ledger.py",
    "runtime_boundary/server.py",
    "stream_core/runtime_boundary_entrypoint.py",
    "stream_core/engine/ffmpeg_args.py",
)


def run(*args: str, check: bool = True, timeout: float = 20.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=check, capture_output=True, text=True, timeout=timeout)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_release_sources(image: str) -> dict[str, Any]:
    """Bind the host-side harness imports and Docker image to the v21 artifact."""

    manifest = json.loads(RELEASE_MANIFEST.read_text(encoding="utf-8"))
    if manifest.get("release_id") != "runtime-bounded-ffmpeg-lifecycle-20260903-v21":
        raise RuntimeError("CLOSED_RELEASE_IDENTITY_INVALID")
    image_inspect = json.loads(run("docker", "image", "inspect", image).stdout)[0]
    labels = image_inspect.get("Config", {}).get("Labels") or {}
    if labels.get("org.stream-v3.effect-fence-release") != manifest["release_id"]:
        raise RuntimeError("CLOSED_IMAGE_RELEASE_LABEL_MISMATCH")

    source_hashes: dict[str, str] = {}
    artifact_root = RELEASE_ROOT / "executor-image/overlay/app/src"
    for relative_text in RELEASE_FILES:
        relative = Path(relative_text)
        source = ROOT / "src" / relative
        if relative.parts[0] == "stream_core":
            source = STREAM_V3_SRC / relative
        artifact = artifact_root / relative
        source_digest = sha256(source)
        if source_digest != sha256(artifact):
            raise RuntimeError(f"CLOSED_HOST_ARTIFACT_SOURCE_MISMATCH:{relative_text}")
        source_hashes[relative_text] = source_digest
    return {
        "release_id": manifest["release_id"],
        "release_manifest_sha256": sha256(RELEASE_MANIFEST),
        "image_id": str(image_inspect["Id"]),
        "source_sha256": source_hashes,
    }


def verify_container_sources(container: str, expected: dict[str, str]) -> None:
    paths = [f"/app/src/{relative}" for relative in RELEASE_FILES]
    completed = run("docker", "exec", container, "sha256sum", *paths)
    observed = {
        line.split(maxsplit=1)[1].removeprefix("/app/src/"): line.split(maxsplit=1)[0]
        for line in completed.stdout.splitlines()
        if len(line.split(maxsplit=1)) == 2
    }
    if observed != expected:
        raise RuntimeError("CLOSED_CONTAINER_ARTIFACT_SOURCE_MISMATCH")


def make_certificate(root: Path) -> tuple[Path, Path]:
    certificate = root / "certificate.pem"
    private_key = root / "private-key.pem"
    run(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=127.0.0.1",
        "-keyout",
        str(private_key),
        "-out",
        str(certificate),
    )
    return certificate, private_key


def wait_file(path: Path, *, timeout: float, error_path: Path | None = None) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if error_path is not None and error_path.exists():
            raise RuntimeError("CLOSED_FIXTURE_FAILED:" + error_path.read_text(encoding="utf-8"))
        time.sleep(0.02)
    raise TimeoutError(f"CLOSED_FIXTURE_TIMEOUT:{path.name}")


def start_fixture(container: str, root: Path, *, case: str, mode: str, port: int) -> subprocess.Popen[str]:
    case_root = root / case
    case_root.mkdir()
    process = subprocess.Popen(
        [
            "docker",
            "exec",
            container,
            "python3",
            "/fixture.py",
            "--mode",
            mode,
            "--certificate",
            "/evidence/certificate.pem",
            "--private-key",
            "/evidence/private-key.pem",
            "--root",
            f"/evidence/{case}",
            "--port",
            str(port),
            "--max-runtime-sec",
            "30",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    wait_file(case_root / "ready", timeout=3, error_path=case_root / "error.json")
    return process


def start_ffmpeg(container: str, *, port: int, rw_timeout_usec: int = 0) -> subprocess.Popen[str]:
    command = [
        "docker",
        "exec",
        container,
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
    ]
    if rw_timeout_usec:
        command.extend(["-rw_timeout", str(rw_timeout_usec)])
    command.extend(["-f", "flv", f"tls://127.0.0.1:{port}?tls_verify=0"])
    return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def exact_ffmpeg_pid(container: str, *, port: int) -> int:
    deadline = time.monotonic() + 5
    marker = f"tls://127.0.0.1:{port}"
    while time.monotonic() < deadline:
        completed = run("docker", "top", container, "-eo", "pid,comm,args", check=False, timeout=5)
        candidates: list[int] = []
        for line in completed.stdout.splitlines()[1:]:
            fields = line.strip().split(maxsplit=2)
            if len(fields) == 3 and fields[1] == "ffmpeg" and marker in fields[2]:
                candidates.append(int(fields[0]))
        if len(candidates) == 1:
            status = Path(f"/proc/{candidates[0]}/status").read_text(encoding="utf-8")
            uid_line = next(line for line in status.splitlines() if line.startswith("Uid:"))
            if int(uid_line.split()[1]) != os.getuid():
                raise RuntimeError("CLOSED_FFMPEG_UID_MISMATCH")
            return candidates[0]
        if len(candidates) > 1:
            raise RuntimeError("CLOSED_FFMPEG_CARDINALITY_INVALID")
        time.sleep(0.05)
    raise RuntimeError("CLOSED_FFMPEG_PID_NOT_FOUND")


class ExternalProcess:
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
            try:
                runner_code = self.runner.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                return None
            self.returncode = -signal.SIGKILL if runner_code == 137 else runner_code
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
                raise subprocess.TimeoutExpired(cmd=["closed-ffmpeg"], timeout=timeout)
            time.sleep(0.01)

    def terminate(self) -> None:
        os.kill(self.pid, signal.SIGTERM)

    def kill(self) -> None:
        os.kill(self.pid, signal.SIGKILL)


def target(pid: int, generation: str) -> dict[str, object]:
    return {
        "host_id": "closed-isolated-dell",
        "host_boot_id": "closed-isolated-boot",
        "namespace": "closed-isolated",
        "pod_uid": "closed-isolated-pod",
        "container_name": "stream-engine",
        "container_id": "docker://closed-isolated",
        "ffmpeg_generation": generation,
        "ffmpeg_pid": pid,
    }


def request_payload(
    *,
    request_id: str,
    correlation_id: str,
    identity: dict[str, object],
    native_generation: str,
) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": request_id,
        "producer_id": "closed-isolated-controller",
        "producer_generation": 1,
        "operation": "restart_ffmpeg",
        "reason": "closed TLS receive-window stall",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "target_identity": identity,
        "expected_ffmpeg_generation": native_generation,
        "idempotency_key": request_id,
        "correlation_id": correlation_id,
        "target_snapshot_id": "closed-target-snapshot-1",
        "runtime_observation_id": "closed-runtime-observation-1",
        "expected_executor_instance_id": "closed-executor-1",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": "closed-projection-1",
        "projection_sequence": 1,
    }


def wait_terminal(client: EffectClient, request_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status = client.status(request_id)
        if status.get("state") in {"EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}:
            return status
        time.sleep(0.02)
    raise TimeoutError("CLOSED_EFFECT_TERMINAL_TIMEOUT")


def stop_process(process: ExternalProcess | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=0.5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)


def finish_runner(process: subprocess.Popen[str] | None) -> tuple[int | None, str]:
    if process is None:
        return None, ""
    try:
        _, stderr = process.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate(timeout=2)
    return process.returncode, stderr[-1000:]


def stop_fixture(root: Path, case: str, process: subprocess.Popen[str] | None) -> None:
    if process is None:
        return
    (root / case / "stop").touch()
    finish_runner(process)


def collect_receiver_ack(root: Path, case: str, *, pid: int, generation: str) -> list[dict[str, object]]:
    samples: list[dict[str, object]] = []
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and len(samples) < 3:
        stats_path = root / case / "stats.json"
        if stats_path.exists():
            value = json.loads(stats_path.read_text(encoding="utf-8"))
            received = int(value.get("bytes_received", 0) or 0)
            if received > 0 and (not samples or received > int(samples[-1]["bytes_acked"])):
                samples.append(
                    {
                        "observed_at": datetime.now(UTC).isoformat(),
                        "ffmpeg_pid": pid,
                        "ffmpeg_generation": generation,
                        "bytes_acked": received,
                    }
                )
        time.sleep(0.1)
    if len(samples) != 3:
        raise RuntimeError("CLOSED_SUCCESSOR_ACK_PROGRESS_NOT_OBSERVED")
    return samples


def cleanup_container(container: str, label_value: str) -> None:
    inspected = run(
        "docker",
        "inspect",
        "--format",
        '{{index .Config.Labels "stream-recovery-control.v21-closed-chaos"}}',
        container,
        check=False,
        timeout=5,
    )
    if inspected.returncode != 0:
        return
    if inspected.stdout.strip() != label_value:
        raise RuntimeError("CLOSED_CONTAINER_LABEL_MISMATCH")
    run("docker", "rm", "--force", container, check=True, timeout=10)


def execute(image: str) -> dict[str, Any]:
    release_identity = verify_release_sources(image)
    suffix = uuid.uuid4().hex[:12]
    container = f"stream-v3-v21-closed-{suffix}"
    label_value = f"closed-{suffix}"
    ledger: EffectLedger | None = None
    server: EffectExecutorServer | None = None
    old_runner: subprocess.Popen[str] | None = None
    old_process: ExternalProcess | None = None
    successor_runner: subprocess.Popen[str] | None = None
    successor_process: ExternalProcess | None = None
    timeout_runner: subprocess.Popen[str] | None = None
    timeout_process: ExternalProcess | None = None
    stall_fixture: subprocess.Popen[str] | None = None
    healthy_fixture: subprocess.Popen[str] | None = None
    timeout_fixture: subprocess.Popen[str] | None = None
    with tempfile.TemporaryDirectory(prefix="stream-v3-v21-closed-") as raw_root:
        root = Path(raw_root)
        make_certificate(root)
        run(
            "docker",
            "run",
            "--detach",
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,size=16m",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            "64",
            "--cpus",
            "1",
            "--memory",
            "512m",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--label",
            f"stream-recovery-control.v21-closed-chaos={label_value}",
            "--volume",
            f"{root}:/evidence:rw",
            "--volume",
            f"{FIXTURE}:/fixture.py:ro",
            "--entrypoint",
            "/bin/sleep",
            "--name",
            container,
            image,
            "infinity",
            timeout=20,
        )
        result: dict[str, Any] = {
            "schema": "stream_recovery_control.v21_closed_network_chaos.v1",
            "image": image,
            "container": container,
            "release_identity": release_identity,
            "isolation": {
                "docker_network_mode": "none",
                "container_root_read_only": True,
                "capabilities_dropped": "ALL",
                "no_new_privileges": True,
                "temporary_sqlite": True,
                "production_effect_socket_touched": False,
                "production_sqlite_touched": False,
                "production_target_touched": False,
                "external_network_calls": 0,
            },
        }
        try:
            inspect = json.loads(run("docker", "inspect", container).stdout)[0]
            if inspect["HostConfig"]["NetworkMode"] != "none" or inspect["HostConfig"]["ReadonlyRootfs"] is not True:
                raise RuntimeError("CLOSED_CONTAINER_ISOLATION_INVALID")
            verify_container_sources(container, release_identity["source_sha256"])

            stall_fixture = start_fixture(container, root, case="stall", mode="stall", port=18443)
            old_runner = start_ffmpeg(container, port=18443)
            wait_file(root / "stall/handshake", timeout=5, error_path=root / "stall/error.json")
            old_pid = exact_ffmpeg_pid(container, port=18443)
            old_process = ExternalProcess(old_pid, old_runner)
            time.sleep(1.5)

            engine = object.__new__(RuntimeBoundaryStreamEngine)
            engine.ffmpeg_proc = old_process
            engine.run_id = "closed-isolated-run"
            engine.restart_count = 0
            engine.ffmpeg_stop_context = {}
            old_native_generation = engine.ffmpeg_generation(old_pid)
            old_target = target(old_pid, "closed-protocol-generation-1")
            current = {
                "target_identity": old_target,
                "ffmpeg_generation": old_native_generation,
                "executor_instance_id": "closed-executor-1",
            }
            ledger = EffectLedger(
                root / "effect.sqlite3",
                initial_producer_id="closed-isolated-controller",
                initial_producer_generation=1,
            )
            server = EffectExecutorServer(
                socket_path=root / "effect.sock",
                ledger=ledger,
                allowed_peer_uids={os.getuid()},
                current_target=lambda: current,
                perform_effect=engine.perform_fast_recovery_effect,
                execute_async=True,
                require_transport_verification=True,
            )
            server.start()
            client = EffectClient(root / "effect.sock", timeout_seconds=0.5)
            request_id = f"dell-target-closed-{suffix}"
            correlation_id = f"fra-closed-{suffix}"
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
                        correlation_id=correlation_id,
                        identity=old_target,
                        native_generation=old_native_generation,
                    )
                )
                correlation_during = client.status_by_correlation(correlation_id)
                terminal = wait_terminal(client, request_id)
                bounded_elapsed = time.monotonic() - started
            finally:
                for name, value in previous_environment.items():
                    if value is None:
                        os.environ.pop(name, None)
                    else:
                        os.environ[name] = value
            terminal_result = terminal.get("result") if isinstance(terminal.get("result"), dict) else {}
            if (
                accepted.get("state") != "EXECUTION_STARTED"
                or correlation_during.get("request_id") != request_id
                or terminal.get("state") != "EFFECT_OBSERVED"
                or terminal_result.get("effect") != "SIGTERM_THEN_SIGKILL"
                or terminal_result.get("signal_attempt_count") != 2
                or terminal_result.get("pidfd_used") is not True
                or bounded_elapsed > 4
            ):
                raise RuntimeError("CLOSED_BOUNDED_EFFECT_CONTRACT_FAILED")
            stop_fixture(root, "stall", stall_fixture)
            stall_fixture = None

            healthy_fixture = start_fixture(container, root, case="healthy", mode="read", port=18444)
            successor_runner = start_ffmpeg(container, port=18444)
            wait_file(root / "healthy/handshake", timeout=5, error_path=root / "healthy/error.json")
            successor_pid = exact_ffmpeg_pid(container, port=18444)
            successor_process = ExternalProcess(successor_pid, successor_runner)
            successor_generation = "closed-protocol-generation-2"
            successor_target = target(successor_pid, successor_generation)
            current.update(
                {
                    "target_identity": successor_target,
                    "ffmpeg_generation": f"closed-isolated-run:1:{successor_pid}",
                }
            )
            samples = collect_receiver_ack(
                root,
                "healthy",
                pid=successor_pid,
                generation=successor_generation,
            )
            unresolved = client.unresolved()["unresolved_scopes"]
            if len(unresolved) != 1:
                raise RuntimeError("CLOSED_UNRESOLVED_SCOPE_CARDINALITY_INVALID")
            evidence = {
                "schema_version": "runtime.delayed_exit_reconciliation_evidence.v2",
                "oracle": "DELAYED_FFMPEG_EXIT_AND_SUCCESSOR_ACK_PROGRESS",
                "observed_at": datetime.now(UTC).isoformat(),
                "physical_effect_count": 1,
                "automatic_retry_count": 0,
                "before_target": old_target,
                "observed_target": successor_target,
                "runtime_observation_id": "closed-successor-observation",
                "transport": {
                    "bytes_sent": int(samples[-1]["bytes_acked"]),
                    "network_down": False,
                    "tcp_probe_ok": True,
                    "ffmpeg_generation": successor_generation,
                    "ack_observation_count": 3,
                    "bytes_acked_start": int(samples[0]["bytes_acked"]),
                    "bytes_acked_end": int(samples[-1]["bytes_acked"]),
                    "bytes_acked_delta": int(samples[-1]["bytes_acked"]) - int(samples[0]["bytes_acked"]),
                    "ack_samples": samples,
                },
            }
            reconciliation = client.reconcile(
                {
                    "schema_version": "runtime.effect_reconciliation_request.v1",
                    "reconciliation_id": f"closed-reconcile-{suffix}",
                    "effect_scope_id": unresolved[0]["effect_scope_id"],
                    "owner_request_id": unresolved[0]["owner_request_id"],
                    "owner_request_digest": unresolved[0]["owner_request_digest"],
                    "resolution": "EFFECT_OBSERVED",
                    "evidence": evidence,
                }
            )
            correlation_after = client.status_by_correlation(correlation_id)
            if (
                reconciliation.get("state") != "RECONCILED_EFFECT_OBSERVED"
                or client.unresolved().get("unresolved_count") != 0
                or correlation_after.get("effect_scope_state") != "RECONCILED_EFFECT_OBSERVED"
            ):
                raise RuntimeError("CLOSED_RECONCILIATION_FAILED")

            timeout_fixture = start_fixture(container, root, case="rw_timeout", mode="stall", port=18445)
            timeout_runner = start_ffmpeg(container, port=18445, rw_timeout_usec=2_000_000)
            wait_file(root / "rw_timeout/handshake", timeout=5, error_path=root / "rw_timeout/error.json")
            timeout_pid = exact_ffmpeg_pid(container, port=18445)
            timeout_process = ExternalProcess(timeout_pid, timeout_runner)
            timeout_started = time.monotonic()
            try:
                timeout_exit = timeout_process.wait(timeout=7)
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("CLOSED_RW_TIMEOUT_DID_NOT_EXIT") from exc
            timeout_elapsed = time.monotonic() - timeout_started

            result.update(
                {
                    "result": "PASS",
                    "bounded_effect": {
                        "accepted_state": accepted.get("state"),
                        "formal_request_id_preserved": correlation_during.get("request_id") == request_id,
                        "elapsed_seconds": round(bounded_elapsed, 3),
                        "effect": terminal_result.get("effect"),
                        "signal_attempt_count": terminal_result.get("signal_attempt_count"),
                        "pidfd_used": terminal_result.get("pidfd_used"),
                        "physical_effect_count": terminal_result.get("physical_effect_count"),
                    },
                    "successor": {
                        "distinct_pid": successor_pid != old_pid,
                        "ack_observation_count": len(samples),
                        "bytes_acked_delta": int(samples[-1]["bytes_acked"]) - int(samples[0]["bytes_acked"]),
                    },
                    "reconciliation": {
                        "state": reconciliation.get("state"),
                        "unresolved_count_after": client.unresolved().get("unresolved_count"),
                        "correlation_scope_state_after": correlation_after.get("effect_scope_state"),
                    },
                    "rw_timeout": {
                        "configured_usec": 2_000_000,
                        "exit_observed": True,
                        "exit_code": timeout_exit,
                        "elapsed_seconds": round(timeout_elapsed, 3),
                    },
                }
            )
        finally:
            stop_process(timeout_process)
            stop_process(successor_process)
            stop_process(old_process)
            stop_fixture(root, "rw_timeout", timeout_fixture)
            stop_fixture(root, "healthy", healthy_fixture)
            stop_fixture(root, "stall", stall_fixture)
            finish_runner(timeout_runner)
            finish_runner(successor_runner)
            finish_runner(old_runner)
            if server is not None:
                server.close()
            if ledger is not None:
                ledger.close()
            cleanup_container(container, label_value)
            result["cleanup_container_absent"] = run("docker", "inspect", container, check=False, timeout=5).returncode != 0
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run v21 chaos in a no-network Docker namespace")
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
