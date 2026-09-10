from __future__ import annotations

import ipaddress
import socket
import subprocess
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlparse


RunCommand = Callable[..., subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class IngestEndpoint:
    host: str
    port: int


@dataclass(frozen=True)
class ConnectivityResult:
    dns_ok: bool
    tcp_ok: bool
    detail: str

    @property
    def ready(self) -> bool:
        return self.dns_ok and self.tcp_ok


def endpoint_from_rtmp_url(url: str) -> IngestEndpoint:
    parsed = urlparse(str(url or ""))
    host = str(parsed.hostname or "").strip()
    if not host:
        raise ValueError("RTMP URL does not contain a host")
    if parsed.port is not None:
        port = int(parsed.port)
    elif parsed.scheme.lower() == "rtmps":
        port = 443
    else:
        port = 1935
    return IngestEndpoint(host=host, port=port)


def _default_run_command(
    command: list[str],
    *,
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def _resolved_addresses(
    host: str,
    *,
    timeout_sec: float,
    run_command: RunCommand,
) -> tuple[list[str], str]:
    try:
        ipaddress.ip_address(host)
        return [host], "literal address"
    except ValueError:
        pass

    try:
        completed = run_command(
            ["getent", "ahosts", host],
            timeout=max(0.2, timeout_sec),
        )
    except subprocess.TimeoutExpired:
        return [], f"DNS resolution timed out after {timeout_sec:g}s"
    except Exception as exc:
        return [], f"DNS resolution failed: {type(exc).__name__}: {exc}"
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "name not found").strip()
        return [], f"DNS resolution failed: {detail[:160]}"

    addresses: list[str] = []
    for line in (completed.stdout or "").splitlines():
        raw = line.strip().split(maxsplit=1)[0] if line.strip() else ""
        try:
            address = str(ipaddress.ip_address(raw))
        except ValueError:
            continue
        if address not in addresses:
            addresses.append(address)
    if not addresses:
        return [], "DNS response did not contain an IP address"
    return addresses, f"resolved {len(addresses)} address(es)"


def probe_ingest_connectivity(
    endpoint: IngestEndpoint,
    *,
    dns_timeout_sec: float = 2.0,
    tcp_timeout_sec: float = 1.0,
    run_command: RunCommand = _default_run_command,
    socket_factory: Callable[..., socket.socket] = socket.socket,
) -> ConnectivityResult:
    addresses, dns_detail = _resolved_addresses(
        endpoint.host,
        timeout_sec=dns_timeout_sec,
        run_command=run_command,
    )
    if not addresses:
        return ConnectivityResult(dns_ok=False, tcp_ok=False, detail=dns_detail)

    failures: list[str] = []
    for address in addresses[:8]:
        family = socket.AF_INET6 if ":" in address else socket.AF_INET
        sock = socket_factory(family, socket.SOCK_STREAM)
        try:
            sock.settimeout(max(0.1, tcp_timeout_sec))
            target = (address, endpoint.port, 0, 0) if family == socket.AF_INET6 else (address, endpoint.port)
            sock.connect(target)
            return ConnectivityResult(
                dns_ok=True,
                tcp_ok=True,
                detail=f"DNS and TCP/{endpoint.port} ready",
            )
        except OSError as exc:
            failures.append(f"{type(exc).__name__}:{exc}")
        finally:
            sock.close()
    failure = failures[-1] if failures else "connection failed"
    return ConnectivityResult(
        dns_ok=True,
        tcp_ok=False,
        detail=f"TCP/{endpoint.port} unavailable after {dns_detail}: {failure[:160]}",
    )
