"""承認済みの送信についてfresh mediaに束縛した有限leaseを更新する。"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
import urllib.request
from pathlib import Path


class RelayLeaseClient:
    def __init__(self, values: dict, pipeline, emit):
        self.values, self.pipeline, self.emit = values, pipeline, emit
        mode = values.get("authorization_mode", "finite")
        if mode not in {"finite", "continuous"}:
            raise ValueError("RELAY_AUTHORIZATION_MODE_INVALID")
        self.continuous = mode == "continuous"
        self.key = bytes.fromhex(values["hmac_key"])
        if len(self.key) != 32:
            raise ValueError("RELAY_LEASE_KEY_INVALID")
        self.sequence = 0
        self.last_media_sequence = 0
        self.maximum_issued_expiry = 0.0
        self.blocked = False
        self.stop_event = threading.Event()
        self.thread = None
        self.last_wall = time.time()
        self.last_elapsed = time.clock_gettime(time.CLOCK_BOOTTIME)
        self.grant_elapsed_deadline = self.last_elapsed + values["grant_expires_at"] - self.last_wall

    def request(self, path: str, body=None):
        data = None if body is None else json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        req = urllib.request.Request(self.values["control_base"] + path, data=data,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5 if path == "/stop" else 1) as response:
            raw = response.read(32769)
            if len(raw) > 32768:
                raise ValueError("RELAY_STATUS_TOO_LARGE")
            value = json.loads(raw)
        if value.get("instance") != self.values["relay_instance"]:
            raise ValueError("RELAY_INSTANCE_CHANGED")
        return value

    @staticmethod
    def process_identity(proc):
        if proc is None or proc.poll() is not None:
            raise ValueError("RELAY_MEDIA_PROCESS_NOT_RUNNING")
        raw = Path(f"/proc/{proc.pid}/stat").read_text()
        ticks = raw[raw.rfind(")") + 2:].split()[19]
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        return hashlib.sha256(f"{boot}:{proc.pid}:{ticks}".encode()).hexdigest()

    def tick(self):
        status = self.request("/status")
        if status.get("halted") is True:
            raise ValueError("RELAY_HALTED")
        state = self.pipeline.snapshot()
        if not state["encoder_running"] or state["input_age_seconds"] is None or state["input_age_seconds"] > 2:
            raise ValueError("RELAY_SOURCE_STALE")
        if state["input_sequence"] <= self.last_media_sequence:
            raise ValueError("RELAY_SOURCE_NO_PROGRESS")
        now = time.time()
        elapsed = time.clock_gettime(time.CLOCK_BOOTTIME)
        if now < self.last_wall or abs((now - self.last_wall) - (elapsed - self.last_elapsed)) > 2:
            raise ValueError("RELAY_CLIENT_CLOCK_CHANGED")
        self.last_wall, self.last_elapsed = now, elapsed
        grant_expiry = now + 10 if self.continuous else self.values["grant_expires_at"]
        if not self.continuous and (now >= grant_expiry or elapsed >= self.grant_elapsed_deadline):
            raise ValueError("RELAY_OPERATOR_GRANT_EXPIRED")
        sequence = max(self.sequence, int(status["sequence"])) + 1
        body = {"schema": "stream.relay_lease.v1", "relay_instance": self.values["relay_instance"],
                "target": self.values["target"], "encoder_instance": self.process_identity(self.pipeline.producer),
                "sender_instance": self.process_identity(self.pipeline.sender),
                "route_generation": self.values["route_generation"], "authority_epoch": self.values["grant_id"],
                "sequence": sequence, "input_sequence": state["input_sequence"], "issued_at": now,
                "expires_at": min(now + 10, grant_expiry), "grant_expires_at": grant_expiry, "route": "PRIMARY"}
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        # ACK喪失時も、発行した最長期限を保持する。
        self.maximum_issued_expiry = max(self.maximum_issued_expiry, body["expires_at"])
        signature = hmac.new(self.key, encoded, hashlib.sha256).hexdigest()
        result = self.request("/lease", {"lease": body, "signature": signature})
        if result.get("sequence") != sequence or result.get("halted") is True:
            raise ValueError("RELAY_LEASE_ACK_MISMATCH")
        self.sequence, self.last_media_sequence = sequence, state["input_sequence"]

    def start(self):
        if self.blocked:
            raise ValueError("RELAY_RENEWAL_BLOCKED")
        if self.thread is not None:
            return
        self.tick()
        def renew():
            while not self.stop_event.wait(2):
                try:
                    self.tick()
                except Exception as error:
                    self.blocked = True
                    self.emit("relay_lease_renewal_stopped", exception_type=type(error).__name__,
                              maximum_issued_expiry=self.maximum_issued_expiry)
                    # own media pipeを閉じる。独立のsignal/restart pathを作らない。
                    self.pipeline.stop_pump()
                    return
        self.thread = threading.Thread(target=renew, name="finite-relay-lease", daemon=True)
        self.thread.start()

    def close(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=3)
            if self.thread.is_alive():
                raise RuntimeError("RELAY_LEASE_THREAD_STOP_UNKNOWN")

    def stop_receipt(self):
        """更新threadを止め、同じrelay instanceのnative停止を要求する。"""
        self.close()
        result = self.request("/stop", {"instance": self.values["relay_instance"]})
        if (result.get("halted") is not True or result.get("process_exit_observed") is not True
            or result.get("enabled") is not False):
            raise RuntimeError("RELAY_STOP_OUTCOME_UNKNOWN")
        return result
