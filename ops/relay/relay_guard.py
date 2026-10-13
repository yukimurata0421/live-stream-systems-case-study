#!/usr/bin/env python3
"""MediaMTXのforwardを有限leaseとfresh inputだけで許可するowner。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from relay_lease import LeaseRejected, admit


def elapsed() -> float:
    return time.clock_gettime(time.CLOCK_BOOTTIME)


class Guard:
    def __init__(self, binary: Path, runtime: Path, state: Path, credentials: Path):
        self.lock = threading.RLock()
        self.binary, self.runtime, self.state = binary, runtime, state
        self.values = json.loads((credentials / "relay.json").read_text())
        mode = self.values["operator_grant"].get("authorization_mode", "finite")
        if mode not in {"finite", "continuous"}:
            raise ValueError("RELAY_AUTHORIZATION_MODE_INVALID")
        self.continuous = mode == "continuous"
        self.key = bytes.fromhex(self.values.pop("hmac_key"))
        self.instance = hashlib.sha256((Path("/proc/sys/kernel/random/boot_id").read_text().strip()
                                       + ":" + secrets.token_hex(32)).encode()).hexdigest()
        self.db = sqlite3.connect(state / "lease.sqlite3", check_same_thread=False, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS highwater(id INTEGER PRIMARY KEY CHECK(id=1),sequence INTEGER NOT NULL)")
        self.db.execute("INSERT OR IGNORE INTO highwater VALUES(1,0)")
        self.highwater = self.db.execute("SELECT sequence FROM highwater WHERE id=1").fetchone()[0]
        self.lease = None
        self.proc = None
        self.enabled = False
        self.halted = False
        self.reason = "WAITING_FOR_LEASE"
        self.last_bytes = 0
        self.last_progress = elapsed()
        self.input_id = ""
        self.twitch_disabled = False
        self.twitch_first_attempt = None
        self.projection = []
        self.last_wall = time.time()
        self.last_elapsed = elapsed()
        self.grant_elapsed_deadline = self.last_elapsed + self.values["operator_grant"]["grant_expires_at"] - self.last_wall
        config = self.values["mediamtx"]
        if config["paths"]["relay"].get("forward") != []:
            raise ValueError("RELAY_INITIAL_FORWARD_NOT_EMPTY")
        config["rtmpServerCert"] = str(credentials / "server.crt")
        config["rtmpServerKey"] = str(credentials / "server.key")
        path = runtime / "mediamtx.json"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as fp:
            json.dump(config, fp)
        self.proc = subprocess.Popen([str(binary), str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = elapsed() + 5
        while elapsed() < deadline:
            try:
                self.api("/v3/paths/list")
                break
            except (OSError, ValueError):
                if self.proc.poll() is not None:
                    raise ValueError("MEDIAMTX_START_FAILED") from None
                time.sleep(.1)
        else:
            self.stop("MEDIAMTX_START_TIMEOUT")
            raise ValueError("MEDIAMTX_START_TIMEOUT")

    def api(self, path: str, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request("http://127.0.0.1:19998" + path, data=data,
                                     method="GET" if body is None else "PATCH",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=1) as response:
            return json.loads(response.read(65536) or b"{}")

    def forward(self, destinations: list):
        self.api("/v3/config/paths/patch/relay", {"forward": destinations})

    def stop(self, reason: str):
        # 終了は永久停止。このinstanceへ新leaseを投入して再起動しない。
        with self.lock:
            started = elapsed()
            if not self.halted:
                self.reason = reason
            self.halted, self.enabled = True, False
            if self.proc is not None and self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait(timeout=1)
            self.stop_elapsed = elapsed() - started

    def accept(self, value: dict):
        with self.lock:
            if self.halted:
                raise LeaseRejected("RELAY_HALTED")
            body = value["lease"]
            grant = self.values["operator_grant"]
            fixed = ("target", "authority_epoch") if self.continuous else ("target", "authority_epoch", "grant_expires_at")
            if any(body.get(name) != grant[name] for name in fixed):
                raise LeaseRejected("OPERATOR_GRANT_MISMATCH")
            lease = admit(value["lease"], value["signature"], key=self.key,
                          relay_instance=self.instance, highwater=self.highwater,
                          wall=time.time(), elapsed=elapsed(), previous=self.lease, continuous=self.continuous)
            # fsyncを伴うSQLite commitより前にforwardへ反映しない。
            self.db.execute("UPDATE highwater SET sequence=? WHERE id=1", (lease.body["sequence"],))
            self.highwater, self.lease = lease.body["sequence"], lease

    def status(self):
        with self.lock:
            return {"schema": "stream.relay_status.v1", "instance": self.instance,
                    "sequence": self.highwater, "enabled": self.enabled, "halted": self.halted,
                    "reason": self.reason, "input_bytes": self.last_bytes,
                    "input_age": max(0, elapsed() - self.last_progress),
                    "forward": self.projection, "twitch_disabled": self.twitch_disabled,
                    "process_exit_observed": self.proc is not None and self.proc.poll() is not None,
                    "stop_elapsed": getattr(self, "stop_elapsed", None),
                    "lease_expires_at": self.lease.body["expires_at"] if self.lease else None}

    def watchdog_status(self):
        body = {**self.status(), "heartbeat_elapsed": elapsed(),
                "lease_elapsed_deadline": self.lease.elapsed_deadline if self.lease else None}
        path = self.runtime / "guard-status.json"
        temporary = path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as out:
            json.dump(body, out, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        if body['halted']:
            # Keep credential-free failure evidence after systemd removes /run.
            record = self.state / ('stop-' + self.instance + '.json')
            if not record.exists():
                fd = os.open(record, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, 'w') as out:
                    json.dump(body, out, sort_keys=True)
                    out.flush()
                    os.fsync(out.fileno())

    def tick(self):
        with self.lock:
            if self.halted:
                return
            if self.proc is None or self.proc.poll() is not None:
                self.stop("MEDIAMTX_EXITED")
                return
            now = elapsed()
            wall = time.time()
            if wall < self.last_wall or abs((wall - self.last_wall) - (now - self.last_elapsed)) > 2:
                self.stop("RELAY_CLOCK_CHANGED")
                return
            self.last_wall, self.last_elapsed = wall, now
            if not self.continuous and now >= self.grant_elapsed_deadline:
                self.stop("OPERATOR_GRANT_ELAPSED_EXPIRED")
                return
            if not self.continuous and wall >= self.values["operator_grant"]["grant_expires_at"]:
                self.stop("OPERATOR_GRANT_EXPIRED")
                return
            if self.lease is not None and not self.lease.valid(time.time(), now):
                self.stop("LEASE_EXPIRED")
                return
            paths = self.api("/v3/paths/list").get("items", [])
            path = next((p for p in paths if p.get("name") == "relay" and p.get("online") is True), None)
            if path is not None:
                connections = self.api("/v3/rtmps/conns/list").get("items", [])
                conn = next((c for c in connections if c.get("state") == "publish" and c.get("path") == "relay"), None)
                if conn is not None:
                    identity = str(conn["id"])
                    count = int(conn.get("inboundBytes", 0))
                    if self.input_id and identity != self.input_id:
                        self.stop("INPUT_INSTANCE_CHANGED")
                        return
                    self.input_id = identity
                    if count > self.last_bytes:
                        self.last_bytes, self.last_progress = count, now
            if self.enabled and (path is None or now - self.last_progress > 4):
                self.stop("INPUT_STALE")
                return
            if not self.enabled and self.lease is not None and path is not None and self.last_bytes > 0 and now - self.last_progress < 2:
                if not self.lease.valid(time.time(), elapsed()):
                    self.stop("LEASE_EXPIRED_BEFORE_FORWARD")
                    return
                self.forward(self.values["destinations"])
                self.enabled, self.reason = True, "FORWARDING"
                self.twitch_first_attempt = elapsed()
            if self.enabled:
                items = self.api("/v3/paths/forward-dests/list?path=relay").get("items", [])
                positions = [r.get("pos") for r in items]
                if len(set(positions)) != len(positions) or any(p not in {1, 2} for p in positions):
                    self.stop("FORWARD_PROVIDER_BINDING_INVALID")
                    return
                self.projection = [{"provider": "youtube" if r["pos"] == 1 else "twitch", "state": r.get("state"),
                                    "outbound_bytes": int(r.get("outboundBytes", 0)),
                                    "inbound_bytes": int((r.get("typeSpecific") or {}).get("inboundBytes", 0)),
                                    "error_present": bool(r.get("lastError"))} for r in sorted(items, key=lambda r: r["pos"])]
                youtube = next((r for r in self.projection if r["provider"] == "youtube"), None)
                if youtube and youtube["error_present"] or elapsed() - self.twitch_first_attempt > 15 and (not youtube or youtube["state"] != "forwarding"):
                    self.stop("YOUTUBE_FORWARD_FAILED")
                    return
                twitch = next((r for r in self.projection if r["provider"] == "twitch"), None)
                # Twitch失敗はYouTube handlerを保持したまま1回で無効化。無制限retryを持たない。
                if not self.twitch_disabled and (twitch and twitch["error_present"] or elapsed() - self.twitch_first_attempt > 15 and (not twitch or twitch["state"] != "forwarding")):
                    self.forward(self.values["destinations"][:1])
                    self.twitch_disabled = True


def serve(guard: Guard):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, code, body):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path != "/status":
                self.respond(404, {"error": "NOT_FOUND"})
                return
            self.respond(200, guard.status())

        def do_POST(self):
            if self.path not in {"/lease", "/stop"}:
                self.respond(404, {"error": "NOT_FOUND"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= 8192:
                    raise ValueError()
                self.connection.settimeout(2)
                value = json.loads(self.rfile.read(length))
                if self.path == "/stop":
                    if value != {"instance": guard.instance}:
                        raise LeaseRejected("RELAY_INSTANCE_CHANGED")
                    guard.stop("OPERATOR_STOP")
                else:
                    guard.accept(value)
            except LeaseRejected as error:
                self.respond(409, {"error": str(error)})
                return
            except Exception:
                self.respond(400, {"error": "LEASE_MALFORMED"})
                return
            self.respond(200, guard.status())
    server = ThreadingHTTPServer(("127.0.0.1", 19999), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    guard = Guard(args.binary, args.runtime, args.state, Path(os.environ["CREDENTIALS_DIRECTORY"]))
    shutdown = threading.Event()
    def stopped(*_):
        shutdown.set()
    signal.signal(signal.SIGTERM, stopped)
    signal.signal(signal.SIGINT, stopped)
    server = serve(guard)
    try:
        while not shutdown.wait(.25) and not guard.halted:
            try:
                guard.tick()
            except Exception as error:
                guard.stop("CONTROL_API_UNAVAILABLE_" + type(error).__name__)
            guard.watchdog_status()
    finally:
        guard.stop("OPERATOR_STOP")
        guard.watchdog_status()
        server.shutdown()
        guard.db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
