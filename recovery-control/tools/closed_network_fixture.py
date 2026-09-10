#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import ssl
import time
from datetime import UTC, datetime
from pathlib import Path


def write_json(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="TLS fixture for a Docker --network none namespace")
    parser.add_argument("--mode", choices=("stall", "read"), required=True)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--private-key", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--max-runtime-sec", type=float, default=30.0)
    args = parser.parse_args()

    args.root.mkdir(parents=True, exist_ok=True)
    ready = args.root / "ready"
    handshake = args.root / "handshake"
    stop = args.root / "stop"
    stats = args.root / "stats.json"
    error = args.root / "error.json"
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.certificate, args.private_key)
    bytes_received = 0
    started = time.monotonic()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            listener.bind(("127.0.0.1", args.port))
            listener.listen(1)
            listener.settimeout(0.2)
            ready.touch()
            connection: socket.socket | None = None
            while connection is None and time.monotonic() - started < args.max_runtime_sec:
                try:
                    connection, _ = listener.accept()
                except TimeoutError:
                    if stop.exists():
                        return 0
            if connection is None:
                raise TimeoutError("CLOSED_FIXTURE_ACCEPT_TIMEOUT")
            with connection:
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                with context.wrap_socket(connection, server_side=True) as tls:
                    tls.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                    tls.settimeout(0.2)
                    handshake.touch()
                    while not stop.exists() and time.monotonic() - started < args.max_runtime_sec:
                        if args.mode == "stall":
                            time.sleep(0.05)
                            continue
                        try:
                            chunk = tls.recv(65536)
                        except TimeoutError:
                            continue
                        if not chunk:
                            break
                        bytes_received += len(chunk)
                        write_json(
                            stats,
                            {
                                "bytes_received": bytes_received,
                                "observed_at": datetime.now(UTC).isoformat(),
                            },
                        )
    except BaseException as exc:  # noqa: BLE001 - the isolated harness reads this bounded evidence
        write_json(error, {"error": f"{type(exc).__name__}: {exc}"})
        return 1
    write_json(
        stats,
        {
            "bytes_received": bytes_received,
            "observed_at": datetime.now(UTC).isoformat(),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
