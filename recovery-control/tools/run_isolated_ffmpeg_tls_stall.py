#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

DEFAULT_IMAGE = "docker.io/library/stream-v3:target-retirement-476ba5e78f32-v16"


class TlsSink:
    def __init__(self, certificate: Path, private_key: Path) -> None:
        self.certificate = certificate
        self.private_key = private_key
        self.port = 0
        self.ready = threading.Event()
        self.handshake_complete = threading.Event()
        self.stop = threading.Event()
        self.error = ""
        self._thread = threading.Thread(target=self._serve, name="isolated-tls-stall", daemon=True)

    def start(self) -> None:
        self._thread.start()
        if not self.ready.wait(timeout=3):
            raise RuntimeError("TLS_SINK_LISTEN_TIMEOUT")

    def close(self) -> None:
        self.stop.set()
        self._thread.join(timeout=3)

    def _serve(self) -> None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.certificate, self.private_key)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
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
                    with connection:
                        connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                        with context.wrap_socket(connection, server_side=True) as tls:
                            tls.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                            self.handshake_complete.set()
                            while not self.stop.wait(0.1):
                                pass
                    return
        except BaseException as exc:  # evidence is returned to the caller
            self.error = f"{type(exc).__name__}: {exc}"
            self.ready.set()
            self.handshake_complete.set()


class RtmpsStallProxy:
    """Complete a real RTMPS publish, then stop reading the publisher side."""

    def __init__(self, certificate: Path, private_key: Path, *, stall_after_bytes: int = 512 * 1024) -> None:
        self.certificate = certificate
        self.private_key = private_key
        self.stall_after_bytes = stall_after_bytes
        self.port = 0
        self.upstream_bytes = 0
        self.ready = threading.Event()
        self.handshake_complete = threading.Event()
        self.stop = threading.Event()
        self.error = ""
        self.backend_returncode: int | None = None
        self.backend_stderr_tail = ""
        self._active_sockets: list[socket.socket] = []
        self._backend_port = self._reserve_port()
        self._backend = subprocess.Popen(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-listen",
                "1",
                "-timeout",
                "10",
                "-i",
                f"rtmp://127.0.0.1:{self._backend_port}/live/key",
                "-f",
                "null",
                "-",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._thread = threading.Thread(target=self._serve, name="isolated-rtmps-stall", daemon=True)

    @staticmethod
    def _reserve_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
            candidate.bind(("127.0.0.1", 0))
            return int(candidate.getsockname()[1])

    def start(self) -> None:
        self._thread.start()
        if not self.ready.wait(timeout=3):
            raise RuntimeError("RTMPS_PROXY_LISTEN_TIMEOUT")

    def close(self) -> None:
        self.stop.set()
        for active in self._active_sockets:
            with suppress(OSError):
                active.shutdown(socket.SHUT_RDWR)
            active.close()
        self._thread.join(timeout=3)
        if self._backend.poll() is None:
            self._backend.terminate()
        try:
            _, stderr = self._backend.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            self._backend.kill()
            _, stderr = self._backend.communicate(timeout=2)
        self.backend_returncode = self._backend.returncode
        self.backend_stderr_tail = stderr[-1000:]

    def _downstream(self, backend: socket.socket, tls: ssl.SSLSocket) -> None:
        try:
            while not self.stop.is_set():
                try:
                    chunk = backend.recv(65536)
                except TimeoutError:
                    continue
                if not chunk:
                    return
                tls.sendall(chunk)
        except OSError as exc:
            if not self.stop.is_set() and not self.error:
                self.error = f"RTMPS_DOWNSTREAM_{type(exc).__name__}: {exc}"

    def _serve(self) -> None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.certificate, self.private_key)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
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
                    connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                    with connection, context.wrap_socket(connection, server_side=True) as tls:
                        tls.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                        tls.settimeout(0.2)
                        deadline = time.monotonic() + 5
                        backend: socket.socket | None = None
                        while backend is None and time.monotonic() < deadline:
                            try:
                                backend = socket.create_connection(("127.0.0.1", self._backend_port), timeout=0.2)
                            except OSError:
                                time.sleep(0.02)
                        if backend is None:
                            raise RuntimeError("RTMP_BACKEND_CONNECT_TIMEOUT")
                        self._active_sockets = [tls, backend]
                        with backend:
                            backend.settimeout(0.2)
                            downstream = threading.Thread(
                                target=self._downstream,
                                args=(backend, tls),
                                name="isolated-rtmps-downstream",
                                daemon=True,
                            )
                            downstream.start()
                            while not self.stop.is_set() and self.upstream_bytes < self.stall_after_bytes:
                                try:
                                    chunk = tls.recv(65536)
                                except TimeoutError:
                                    continue
                                if not chunk:
                                    return
                                backend.sendall(chunk)
                                self.upstream_bytes += len(chunk)
                            self.handshake_complete.set()
                            while not self.stop.wait(0.1):
                                pass
                            downstream.join(timeout=1)
                    return
        except BaseException as exc:  # evidence is returned to the caller
            self.error = f"{type(exc).__name__}: {exc}"
            self.ready.set()
            self.handshake_complete.set()


def run(*args: str, check: bool = True, timeout: float = 20.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=check, capture_output=True, text=True, timeout=timeout)


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


def task_exists(container_id: str) -> bool:
    completed = run(
        "sudo",
        "-n",
        "ctr",
        "-n",
        "k8s.io",
        "tasks",
        "list",
        "--quiet",
        check=False,
        timeout=5,
    )
    return completed.returncode == 0 and container_id in completed.stdout.splitlines()


def container_exists(container_id: str) -> bool:
    completed = run(
        "sudo",
        "-n",
        "ctr",
        "-n",
        "k8s.io",
        "containers",
        "list",
        "--quiet",
        check=False,
        timeout=5,
    )
    return completed.returncode == 0 and container_id in completed.stdout.splitlines()


def cleanup(container_id: str) -> None:
    if task_exists(container_id):
        run(
            "sudo",
            "-n",
            "ctr",
            "-n",
            "k8s.io",
            "tasks",
            "kill",
            "--signal",
            "SIGKILL",
            container_id,
            check=False,
            timeout=5,
        )
        deadline = time.monotonic() + 3
        while task_exists(container_id) and time.monotonic() < deadline:
            time.sleep(0.05)
        run(
            "sudo",
            "-n",
            "ctr",
            "-n",
            "k8s.io",
            "tasks",
            "delete",
            "--force",
            container_id,
            check=False,
            timeout=5,
        )
    if container_exists(container_id):
        run(
            "sudo",
            "-n",
            "ctr",
            "-n",
            "k8s.io",
            "containers",
            "delete",
            container_id,
            check=False,
            timeout=5,
        )


def execute_case(
    *,
    image: str,
    protocol: str,
    rw_timeout_usec: int,
    pre_term_observation_sec: float,
    observe_after_term_sec: float,
) -> dict[str, Any]:
    suffix = f"{os.getpid()}-{protocol}-{rw_timeout_usec}"
    container_id = f"stream-v3-isolated-tls-stall-{suffix}"
    if task_exists(container_id) or container_exists(container_id):
        raise RuntimeError("ISOLATED_CONTAINER_ID_ALREADY_EXISTS")
    with tempfile.TemporaryDirectory(prefix="stream-v3-ffmpeg-tls-stall-") as raw_root:
        root = Path(raw_root)
        certificate, private_key = make_certificate(root)
        sink = RtmpsStallProxy(certificate, private_key) if protocol == "rtmps" else TlsSink(certificate, private_key)
        sink.start()
        output_url = (
            f"rtmps://127.0.0.1:{sink.port}/live/key?tls_verify=0" if protocol == "rtmps" else f"tls://127.0.0.1:{sink.port}?tls_verify=0"
        )
        command = [
            "sudo",
            "-n",
            "ctr",
            "-n",
            "k8s.io",
            "run",
            "--rm",
            "--net-host",
            image,
            container_id,
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
        if rw_timeout_usec > 0:
            command.extend(["-rw_timeout", str(rw_timeout_usec)])
        command.extend(["-f", "flv", output_url])
        started = time.monotonic()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        result: dict[str, Any] = {
            "image": image,
            "container_id": container_id,
            "protocol": protocol,
            "rw_timeout_usec": rw_timeout_usec,
            "loopback_only": True,
            "production_target_touched": False,
        }
        try:
            if not sink.handshake_complete.wait(timeout=5):
                raise RuntimeError(f"TLS_HANDSHAKE_TIMEOUT: {sink.error}")
            if sink.error:
                raise RuntimeError(f"TLS_STALL_FIXTURE_FAILED: {sink.error}")
            if isinstance(sink, RtmpsStallProxy) and sink.upstream_bytes < sink.stall_after_bytes:
                raise RuntimeError("RTMPS_PUBLISH_NOT_ESTABLISHED_BEFORE_STALL")
            pre_term_deadline = time.monotonic() + pre_term_observation_sec
            while task_exists(container_id) and time.monotonic() < pre_term_deadline:
                time.sleep(0.05)
            result["exited_before_term"] = not task_exists(container_id)
            result["pre_term_observation_sec"] = round(time.monotonic() - (pre_term_deadline - pre_term_observation_sec), 3)
            if not result["exited_before_term"]:
                term_sent_at = time.monotonic()
                run(
                    "sudo",
                    "-n",
                    "ctr",
                    "-n",
                    "k8s.io",
                    "tasks",
                    "kill",
                    "--signal",
                    "SIGTERM",
                    container_id,
                    timeout=5,
                )
                deadline = term_sent_at + observe_after_term_sec
                while task_exists(container_id) and time.monotonic() < deadline:
                    time.sleep(0.05)
                result["term_sent"] = True
                result["still_running_after_term_deadline"] = task_exists(container_id)
                result["term_observation_sec"] = round(time.monotonic() - term_sent_at, 3)
            else:
                result["term_sent"] = False
                result["still_running_after_term_deadline"] = False
                result["term_observation_sec"] = 0.0
            result["elapsed_sec"] = round(time.monotonic() - started, 3)
        finally:
            cleanup(container_id)
            sink.close()
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate(timeout=2)
            result["runner_returncode"] = process.returncode
            result["stdout_tail"] = stdout[-1000:]
            result["stderr_tail"] = stderr[-2000:]
            result["cleanup_task_absent"] = not task_exists(container_id)
            result["cleanup_container_absent"] = not container_exists(container_id)
            result["tls_sink_error"] = sink.error
            if isinstance(sink, RtmpsStallProxy):
                result["rtmp_publish_bytes_before_stall"] = sink.upstream_bytes
                result["rtmp_backend_returncode"] = sink.backend_returncode
                result["rtmp_backend_stderr_tail"] = sink.backend_stderr_tail
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Bounded loopback-only FFmpeg/TLS write-stall reproduction")
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--protocol", choices=("tls", "rtmps"), default="tls")
    parser.add_argument("--rw-timeout-usec", type=int, default=0)
    parser.add_argument("--pre-term-observation-sec", type=float, default=2.0)
    parser.add_argument("--observe-after-term-sec", type=float, default=2.2)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.observe_after_term_sec <= 0 or args.observe_after_term_sec > 10:
        raise ValueError("OBSERVATION_WINDOW_OUT_OF_RANGE")
    if args.pre_term_observation_sec <= 0 or args.pre_term_observation_sec > 10:
        raise ValueError("PRE_TERM_OBSERVATION_WINDOW_OUT_OF_RANGE")
    if args.rw_timeout_usec < 0 or args.rw_timeout_usec > 300_000_000:
        raise ValueError("RW_TIMEOUT_OUT_OF_RANGE")
    required_commands = ["sudo", "ctr", "openssl"] + (["ffmpeg"] if args.protocol == "rtmps" else [])
    if any(shutil.which(command) is None for command in required_commands):
        raise RuntimeError("REQUIRED_COMMAND_UNAVAILABLE")
    report = execute_case(
        image=args.image,
        protocol=args.protocol,
        rw_timeout_usec=args.rw_timeout_usec,
        pre_term_observation_sec=args.pre_term_observation_sec,
        observe_after_term_sec=args.observe_after_term_sec,
    )
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
