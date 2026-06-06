#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import socket
import ssl
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_STATE_DIR = BASE_DIR / ".state" / "wan-observer"
DEFAULT_ANCHORS = (
    "cloudflare_v4|1.1.1.1|443|cloudflare-dns.com|cloudflare-dns.com|AS13335,"
    "google_v4|8.8.8.8|443|dns.google|dns.google|AS15169,"
    "cloudflare_v6|2606:4700:4700::1111|443|cloudflare-dns.com|cloudflare-dns.com|AS13335,"
    "google_v6|2001:4860:4860::8888|443|dns.google|dns.google|AS15169"
)


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_jst(ts_utc: str) -> str:
    dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    return (dt + timedelta(hours=9)).isoformat(timespec="seconds").replace("+00:00", "+09:00")


def monotonic_ms(started: float) -> float:
    return round((time.monotonic() - started) * 1000, 1)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        f.write("\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def literal_family(address: str) -> str:
    try:
        version = ipaddress.ip_address(address.split("%", 1)[0]).version
    except ValueError:
        return ""
    return "ipv6" if version == 6 else "ipv4"


def parse_status_code(header_bytes: bytes) -> int:
    first_line = header_bytes.splitlines()[0:1]
    if not first_line:
        return 0
    parts = first_line[0].decode("iso-8859-1", errors="replace").split()
    if len(parts) < 2:
        return 0
    try:
        return int(parts[1])
    except ValueError:
        return 0


def connection_close_requested(header_bytes: bytes) -> bool:
    text = header_bytes.decode("iso-8859-1", errors="replace").lower()
    return "\nconnection: close" in text or "\r\nconnection: close" in text


def set_tcp_keepalive(sock: socket.socket, idle_sec: int, interval_sec: int, count: int, user_timeout_ms: int) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    for attr, value in (
        ("TCP_KEEPIDLE", idle_sec),
        ("TCP_KEEPINTVL", interval_sec),
        ("TCP_KEEPCNT", count),
        ("TCP_USER_TIMEOUT", user_timeout_ms),
    ):
        option = getattr(socket, attr, None)
        if option is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, option, value)
        except OSError:
            continue


@dataclass(frozen=True)
class Anchor:
    name: str
    address: str
    port: int
    server_name: str
    host_header: str
    as_hint: str

    @property
    def literal_family(self) -> str:
        return literal_family(self.address)


def parse_anchor(spec: str) -> Anchor:
    parts = [part.strip() for part in spec.split("|")]
    if len(parts) != 6:
        raise ValueError("anchor must be name|address|port|server_name|host_header|as_hint")
    name, address, port_text, server_name, host_header, as_hint = parts
    port = int(port_text)
    if not name or not address or not server_name or not host_header or not 0 < port < 65536:
        raise ValueError(f"invalid anchor: {spec}")
    return Anchor(name=name, address=address, port=port, server_name=server_name, host_header=host_header, as_hint=as_hint)


class PersistentAnchor:
    def __init__(self, anchor: Anchor, *, timeout_sec: float, keepalive_idle_sec: int, keepalive_interval_sec: int, keepalive_count: int, user_timeout_ms: int, tls_verify: bool) -> None:
        self.anchor = anchor
        self.timeout_sec = timeout_sec
        self.keepalive_idle_sec = keepalive_idle_sec
        self.keepalive_interval_sec = keepalive_interval_sec
        self.keepalive_count = keepalive_count
        self.user_timeout_ms = user_timeout_ms
        self.tls_verify = tls_verify
        self.sock: ssl.SSLSocket | None = None
        self.connection_id = 0
        self.connected_at_monotonic = 0.0
        self.connected_at_utc = ""
        self.reconnect_count = 0
        self.last_error = ""
        self.last_close_reason = ""

    def static_meta(self) -> dict[str, Any]:
        return {
            "name": self.anchor.name,
            "address": self.anchor.address,
            "port": self.anchor.port,
            "server_name": self.anchor.server_name,
            "host_header": self.anchor.host_header,
            "as_hint": self.anchor.as_hint,
            "literal_family": self.anchor.literal_family,
        }

    def close(self, reason: str) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None
        self.connected_at_monotonic = 0.0
        self.connected_at_utc = ""
        self.last_close_reason = reason

    def connect(self) -> dict[str, Any]:
        started = time.monotonic()
        raw: socket.socket | None = None
        try:
            raw = socket.create_connection((self.anchor.address, self.anchor.port), timeout=self.timeout_sec)
            raw.settimeout(self.timeout_sec)
            set_tcp_keepalive(
                raw,
                idle_sec=self.keepalive_idle_sec,
                interval_sec=self.keepalive_interval_sec,
                count=self.keepalive_count,
                user_timeout_ms=self.user_timeout_ms,
            )
            context = ssl.create_default_context()
            if not self.tls_verify:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            wrapped = context.wrap_socket(raw, server_hostname=self.anchor.server_name)
            wrapped.settimeout(self.timeout_sec)
        except OSError as exc:
            if raw is not None:
                try:
                    raw.close()
                except OSError:
                    pass
            self.last_error = f"{type(exc).__name__}: {exc}"
            return {
                **self.static_meta(),
                "ok": False,
                "phase": "connect_or_tls",
                "new_connection": False,
                "connected": False,
                "elapsed_ms": monotonic_ms(started),
                "error": self.last_error,
                "reconnect_count": self.reconnect_count,
                "last_close_reason": self.last_close_reason,
            }

        self.sock = wrapped
        self.connection_id += 1
        self.reconnect_count += 1
        self.connected_at_monotonic = time.monotonic()
        self.connected_at_utc = iso_utc_now()
        self.last_error = ""
        return {
            **self.static_meta(),
            "ok": True,
            "phase": "connect_or_tls",
            "new_connection": True,
            "connected": True,
            "connection_id": self.connection_id,
            "connected_at_utc": self.connected_at_utc,
            "connection_age_sec": 0.0,
            "elapsed_ms": monotonic_ms(started),
            "error": "",
            "reconnect_count": self.reconnect_count,
            "last_close_reason": self.last_close_reason,
        }

    def read_headers(self) -> bytes:
        if self.sock is None:
            raise OSError("socket_not_connected")
        chunks = bytearray()
        while b"\r\n\r\n" not in chunks:
            if len(chunks) > 16384:
                raise OSError("header_too_large")
            chunk = self.sock.recv(4096)
            if not chunk:
                raise OSError("remote_eof")
            chunks.extend(chunk)
        return bytes(chunks)

    def send_probe(self) -> dict[str, Any]:
        if self.sock is None:
            connect_result = self.connect()
            if not connect_result.get("ok"):
                return connect_result
            new_connection = True
        else:
            new_connection = False

        started = time.monotonic()
        request = (
            "HEAD / HTTP/1.1\r\n"
            f"Host: {self.anchor.host_header}\r\n"
            "User-Agent: stream-v3-persistent-anchor-observer/1\r\n"
            "Accept: */*\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: keep-alive\r\n"
            "\r\n"
        ).encode("ascii")

        try:
            assert self.sock is not None
            self.sock.sendall(request)
            headers = self.read_headers()
        except OSError as exc:
            failure_error = f"{type(exc).__name__}: {exc}"
            self.last_error = failure_error
            old_connection_id = self.connection_id
            old_connected_at_utc = self.connected_at_utc
            old_age = round(time.monotonic() - self.connected_at_monotonic, 1) if self.connected_at_monotonic else 0.0
            self.close(f"probe_failed:{failure_error}")
            failure_close_reason = self.last_close_reason
            reconnect_result = self.connect()
            return {
                **self.static_meta(),
                "ok": False,
                "phase": "request_on_existing_connection",
                "new_connection": new_connection,
                "connected": False,
                "connection_id": old_connection_id,
                "connected_at_utc": old_connected_at_utc,
                "connection_age_sec": old_age,
                "elapsed_ms": monotonic_ms(started),
                "error": failure_error,
                "reconnect_count": self.reconnect_count,
                "last_close_reason": failure_close_reason,
                "reconnect_after_failure_ok": bool(reconnect_result.get("ok")),
                "reconnect_after_failure": reconnect_result,
            }

        status_code = parse_status_code(headers)
        server_requested_close = connection_close_requested(headers)
        age = round(time.monotonic() - self.connected_at_monotonic, 1) if self.connected_at_monotonic else 0.0
        result = {
            **self.static_meta(),
            "ok": 100 <= status_code < 600,
            "phase": "request_on_existing_connection",
            "new_connection": new_connection,
            "connected": True,
            "connection_id": self.connection_id,
            "connected_at_utc": self.connected_at_utc,
            "connection_age_sec": age,
            "elapsed_ms": monotonic_ms(started),
            "http_status": status_code,
            "server_requested_close": server_requested_close,
            "error": "" if 100 <= status_code < 600 else "invalid_http_status",
            "reconnect_count": self.reconnect_count,
            "last_close_reason": self.last_close_reason,
        }
        if server_requested_close:
            self.close("server_requested_close")
        return result


def build_payload(flows: list[PersistentAnchor], args: argparse.Namespace) -> dict[str, Any]:
    ts_utc = iso_utc_now()
    probes = [flow.send_probe() for flow in flows]
    return {
        "schema": "stream_v3_persistent_tcp_anchor_observer/v1",
        "ts_utc": ts_utc,
        "ts_jst": iso_jst(ts_utc),
        "interval_sec": args.interval_sec,
        "timeout_sec": args.timeout_sec,
        "probes": probes,
        "ok_count": sum(1 for item in probes if item.get("ok")),
        "failed": [item.get("name", "") for item in probes if not item.get("ok")],
    }


def parse_args() -> argparse.Namespace:
    state_dir = Path(env("WAO_STATE_DIR", str(DEFAULT_STATE_DIR)))
    parser = argparse.ArgumentParser(description="Keep non-YouTube TCP/TLS anchors open and probe them for existing-flow blackholes.")
    parser.add_argument("--anchor", action="append", default=split_csv(env("WAO_PERSISTENT_ANCHORS", DEFAULT_ANCHORS)))
    parser.add_argument("--interval-sec", type=float, default=float(env("WAO_PERSISTENT_INTERVAL_SEC", "15.0") or "15.0"))
    parser.add_argument("--timeout-sec", type=float, default=float(env("WAO_PERSISTENT_TIMEOUT_SEC", "2.0") or "2.0"))
    parser.add_argument("--keepalive-idle-sec", type=int, default=int(env("WAO_PERSISTENT_KEEPALIVE_IDLE_SEC", "20") or "20"))
    parser.add_argument("--keepalive-interval-sec", type=int, default=int(env("WAO_PERSISTENT_KEEPALIVE_INTERVAL_SEC", "5") or "5"))
    parser.add_argument("--keepalive-count", type=int, default=int(env("WAO_PERSISTENT_KEEPALIVE_COUNT", "3") or "3"))
    parser.add_argument("--user-timeout-ms", type=int, default=int(env("WAO_PERSISTENT_USER_TIMEOUT_MS", "10000") or "10000"))
    parser.add_argument("--tls-no-verify", action="store_true", default=env("WAO_PERSISTENT_TLS_NO_VERIFY", "0") in {"1", "true", "yes"})
    parser.add_argument("--cycles", type=int, default=int(env("WAO_PERSISTENT_CYCLES", "0") or "0"))
    parser.add_argument("--latest-file", type=Path, default=Path(env("WAO_PERSISTENT_LATEST_FILE", str(state_dir / "persistent_tcp_anchor_observer_latest.json"))))
    parser.add_argument("--output-jsonl", type=Path, default=Path(env("WAO_PERSISTENT_OUTPUT_JSONL", str(state_dir / "logs" / "persistent_tcp_anchor_observer.jsonl"))))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    anchors = [parse_anchor(spec) for spec in args.anchor]
    flows = [
        PersistentAnchor(
            anchor,
            timeout_sec=args.timeout_sec,
            keepalive_idle_sec=args.keepalive_idle_sec,
            keepalive_interval_sec=args.keepalive_interval_sec,
            keepalive_count=args.keepalive_count,
            user_timeout_ms=args.user_timeout_ms,
            tls_verify=not args.tls_no_verify,
        )
        for anchor in anchors
    ]

    completed = 0
    try:
        while True:
            loop_started = time.monotonic()
            payload = build_payload(flows, args)
            append_jsonl(args.output_jsonl, payload)
            write_json(args.latest_file, payload)
            print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)
            completed += 1
            if args.cycles > 0 and completed >= args.cycles:
                break
            sleep_sec = args.interval_sec - (time.monotonic() - loop_started)
            time.sleep(max(0.0, sleep_sec))
    finally:
        for flow in flows:
            flow.close("shutdown")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
