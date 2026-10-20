"""A bounded MPEG-TS buffer and a single remux sender owned by the existing engine.

The existing owner starts and stops the sender; this module adds no route policy or independent restart loop.
"""
from __future__ import annotations

import collections
import os
import selectors
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit


PACKET_SIZE = 188


def elapsed_clock() -> float:
    return time.clock_gettime(time.CLOCK_BOOTTIME)


class DeliveryBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class BufferPolicy:
    maximum_bytes: int = 2 * 1024 * 1024
    maximum_age_seconds: float = 3.0
    stale_seconds: float = 3.0

    def __post_init__(self) -> None:
        if not 188 * 64 <= self.maximum_bytes <= 8 * 1024 * 1024:
            raise ValueError("DELIVERY_BUFFER_BYTES_INVALID")
        if not 2.0 <= self.maximum_age_seconds <= 5.0:
            raise ValueError("DELIVERY_BUFFER_AGE_INVALID")
        if not 1.0 <= self.stale_seconds <= 5.0:
            raise ValueError("DELIVERY_STALE_BOUND_INVALID")


class PacketBuffer:
    def __init__(self, policy: BufferPolicy = BufferPolicy(), *, clock: Callable[[], float] = elapsed_clock):
        self.policy = policy
        self.clock = clock
        self.condition = threading.Condition()
        self.packets: collections.deque[tuple[int, float, bytes, bool]] = collections.deque()
        self.sequence = 0
        self.last_input_at: float | None = None
        self.closed = False
        self.headers: dict[int, bytes] = {}
        self.pmt_pid: int | None = None
        self.video_pid: int | None = None
        self.dropped_packets = 0
        self._partial = bytearray()

    @staticmethod
    def _payload(packet: bytes) -> bytes:
        control = (packet[3] >> 4) & 3
        if control not in (1, 3):
            return b""
        offset = 4 if control == 1 else 5 + packet[4]
        return packet[offset:] if offset <= 188 else b""

    def _tables(self, packet: bytes, pid: int) -> None:
        if pid not in (0, self.pmt_pid) or not packet[1] & 0x40:
            return
        payload = self._payload(packet)
        if not payload or payload[0] + 1 >= len(payload):
            return
        section = payload[1 + payload[0]:]
        if pid == 0 and len(section) >= 12 and section[0] == 0:
            length = ((section[1] & 15) << 8) | section[2]
            for offset in range(8, min(3 + length - 4, len(section)), 4):
                if offset + 4 <= len(section) and (section[offset] or section[offset + 1]):
                    self.pmt_pid = ((section[offset + 2] & 31) << 8) | section[offset + 3]
                    break
            self.headers[0] = packet
        elif pid == self.pmt_pid and len(section) >= 12 and section[0] == 2:
            length = ((section[1] & 15) << 8) | section[2]
            offset = 12 + ((section[10] & 15) << 8) + section[11]
            end = min(3 + length - 4, len(section))
            while offset + 5 <= end:
                stream_type = section[offset]
                stream_pid = ((section[offset + 1] & 31) << 8) | section[offset + 2]
                if stream_type == 0x1B:
                    self.video_pid = stream_pid
                offset += 5 + ((section[offset + 3] & 15) << 8) + section[offset + 4]
            self.headers[pid] = packet

    def _prune(self, now: float) -> None:
        maximum_packets = self.policy.maximum_bytes // 188
        while self.packets and (len(self.packets) > maximum_packets or now - self.packets[0][1] > self.policy.maximum_age_seconds):
            self.packets.popleft()
            self.dropped_packets += 1

    def feed(self, data: bytes) -> None:
        with self.condition:
            if len(data) > 188 * 256:
                raise DeliveryBlocked("DELIVERY_INPUT_CHUNK_TOO_LARGE")
            if self.closed:
                raise DeliveryBlocked("DELIVERY_BUFFER_CLOSED")
            self._partial.extend(data)
            while len(self._partial) >= 188:
                packet = bytes(self._partial[:188])
                del self._partial[:188]
                if packet[0] != 0x47:
                    self.closed = True
                    self.condition.notify_all()
                    raise DeliveryBlocked("DELIVERY_TS_ALIGNMENT_INVALID")
                pid = ((packet[1] & 31) << 8) | packet[2]
                self._tables(packet, pid)
                adaptation = (packet[3] >> 4) & 3
                keyframe = pid == self.video_pid and adaptation in (2, 3) and packet[4] > 0 and bool(packet[5] & 0x40)
                now = self.clock()
                self.sequence += 1
                self.last_input_at = now
                self.packets.append((self.sequence, now, packet, keyframe))
                self._prune(now)
            self.condition.notify_all()

    def read(self, cursor: int | None, *, maximum_packets: int = 128) -> tuple[int | None, bytes, bool]:
        with self.condition:
            now = self.clock()
            self._prune(now)
            if self.closed:
                raise DeliveryBlocked("DELIVERY_PRODUCER_CLOSED")
            if self.last_input_at is not None and now < self.last_input_at:
                raise DeliveryBlocked("DELIVERY_CLOCK_REGRESSION")
            if self.last_input_at is None or now - self.last_input_at > self.policy.stale_seconds:
                raise DeliveryBlocked("DELIVERY_INPUT_STALE")
            if not self.packets:
                return cursor, b"", False
            reset = cursor is None or cursor < self.packets[0][0] - 1
            header = b""
            if reset:
                keyframes = [p[0] for p in self.packets if p[3]]
                if not keyframes or self.pmt_pid not in self.headers or self.video_pid is None:
                    return None, b"", False
                cursor = keyframes[-1] - 1
                header = self.headers[0] + self.headers[self.pmt_pid]
            rows = [p for p in self.packets if p[0] > cursor][:maximum_packets]
            if not rows:
                return cursor, b"", reset
            return rows[-1][0], header + b"".join(p[2] for p in rows), reset

    def close(self) -> None:
        with self.condition:
            self.closed = True
            self.condition.notify_all()

    def snapshot(self) -> dict[str, object]:
        with self.condition:
            now = self.clock()
            self._prune(now)
            return {"buffer_bytes": len(self.packets) * 188, "dropped_packets": self.dropped_packets,
                    "input_age_seconds": None if self.last_input_at is None else max(0.0, now - self.last_input_at),
                    "input_sequence": self.sequence, "keyframe_available": any(p[3] for p in self.packets),
                    "producer_closed": self.closed}


def producer_args(encoded_args: list[str], old_output_args: list[str]) -> list[str]:
    if not old_output_args or encoded_args[-len(old_output_args):] != old_output_args:
        raise DeliveryBlocked("DELIVERY_PRODUCER_OUTPUT_NOT_EXACT")
    return encoded_args[:-len(old_output_args)] + ["-bsf:v", "h264_mp4toannexb", "-f", "mpegts",
        "-mpegts_flags", "resend_headers", "-muxdelay", "0", "-flush_packets", "1", "pipe:1"]


def sender_args(destination: str, *, ca_file: Path | None = None) -> list[str]:
    u = urlsplit(destination)
    if u.scheme != "rtmps" or not u.hostname or u.username or u.query or u.fragment:
        raise DeliveryBlocked("DELIVERY_DESTINATION_INVALID")
    try:
        port = u.port or 443
    except ValueError:
        raise DeliveryBlocked("DELIVERY_DESTINATION_INVALID") from None
    if port not in (443, 1936):
        raise DeliveryBlocked("DELIVERY_DESTINATION_PORT_INVALID")
    tls = ["-tls_verify", "1"]
    if ca_file is not None:
        tls += ["-ca_file", str(ca_file)]
    return ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "1",
        "-analyzeduration", "5000000", "-probesize", "1048576", "-f", "mpegts", "-i", "pipe:0",
        "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-bsf:a", "aac_adtstoasc",
        "-rw_timeout", "8000000", *tls, "-f", "flv", destination]


class DeliveryPipeline:
    def __init__(self, *, producer_command: list[str], destination: str, ca_file: Path | None,
                 emit: Callable[..., object], buffer_policy: BufferPolicy = BufferPolicy(),
                 producer_spawn: Callable[..., subprocess.Popen] = subprocess.Popen,
                 producer_stderr: Callable[[subprocess.Popen], object] | None = None):
        self.producer_command = producer_command
        self.destination = destination
        self.ca_file = ca_file
        self.emit = emit
        self.producer_spawn = producer_spawn
        self.producer_stderr = producer_stderr
        self.buffer = PacketBuffer(buffer_policy)
        self.producer: subprocess.Popen | None = None
        self.sender: subprocess.Popen | None = None
        self._reader: threading.Thread | None = None
        self._pump: threading.Thread | None = None
        self._pump_stop = threading.Event()

    def start_producer(self) -> None:
        if self.producer is not None:
            if self.producer.poll() is None:
                return
            if self.sender is not None and self.sender.poll() is None:
                raise DeliveryBlocked("DELIVERY_PRODUCER_EXITED_WITH_ACTIVE_SENDER")
            if self._reader is not None:
                self._reader.join(timeout=1)
                if self._reader.is_alive():
                    raise DeliveryBlocked("DELIVERY_PRODUCER_READER_STOP_UNKNOWN")
            self.emit("delivery_producer_exited", encoder_pid=self.producer.pid, exit_code=self.producer.returncode)
            self.buffer = PacketBuffer(self.buffer.policy)
        self.producer = self.producer_spawn(self.producer_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if self.producer_stderr is None:
            self.producer.terminate()
            self.producer.wait(timeout=2)
            raise DeliveryBlocked("DELIVERY_PRODUCER_STDERR_COLLECTOR_REQUIRED")
        self.producer_stderr(self.producer)
        self._reader = threading.Thread(target=self._read_producer, name="delivery-packet-buffer", daemon=True)
        self._reader.start()
        self.emit("delivery_producer_started", encoder_pid=self.producer.pid)

    def _read_producer(self) -> None:
        try:
            while self.producer is not None and self.producer.stdout is not None:
                chunk = os.read(self.producer.stdout.fileno(), 188 * 128)
                if not chunk:
                    break
                self.buffer.feed(chunk)
        except (OSError, DeliveryBlocked):
            pass
        finally:
            self.buffer.close()

    def launch_sender(self) -> subprocess.Popen:
        self.stop_pump()
        if self.sender is not None and self.sender.poll() is None:
            raise DeliveryBlocked("DELIVERY_OLD_SENDER_STILL_RUNNING")
        self.start_producer()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            s = self.buffer.snapshot()
            if s["keyframe_available"]:
                break
            if s["producer_closed"]:
                raise DeliveryBlocked("DELIVERY_PRODUCER_CLOSED")
            time.sleep(.02)
        else:
            raise DeliveryBlocked("DELIVERY_KEYFRAME_TIMEOUT")
        self.sender = subprocess.Popen(sender_args(self.destination, ca_file=self.ca_file), stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        self._pump_stop = threading.Event()
        self._pump = threading.Thread(target=self._feed_sender, args=(self.sender, self._pump_stop), name="delivery-single-sender", daemon=True)
        self._pump.start()
        self.emit("delivery_sender_started", sender_pid=self.sender.pid, encoder_pid=self.producer.pid)
        return self.sender

    def _feed_sender(self, sender: subprocess.Popen, stop: threading.Event) -> None:
        assert sender.stdin is not None
        cursor = None
        pending = b""
        pending_since = elapsed_clock()
        try:
            fd = sender.stdin.fileno()
            os.set_blocking(fd, False)
            with selectors.DefaultSelector() as selector:
                selector.register(fd, selectors.EVENT_WRITE)
                while not stop.is_set() and sender.poll() is None:
                    state = self.buffer.snapshot()
                    age = state["input_age_seconds"]
                    if state["producer_closed"] or age is None or age > self.buffer.policy.stale_seconds:
                        break
                    if not pending:
                        cursor, pending, reset = self.buffer.read(cursor)
                        pending_since = elapsed_clock()
                        if reset:
                            self.emit("delivery_buffer_keyframe_resync", sender_pid=sender.pid)
                    if not pending:
                        stop.wait(.02)
                        continue
                    if elapsed_clock() - pending_since > self.buffer.policy.maximum_age_seconds:
                        self.emit("delivery_sender_backpressure_bound_reached", sender_pid=sender.pid)
                        break
                    if not selector.select(.1):
                        continue
                    try:
                        written = os.write(fd, pending)
                        pending = pending[written:]
                    except BlockingIOError:
                        continue
        except (OSError, DeliveryBlocked, ValueError):
            pass
        finally:
            try:
                sender.stdin.close()
            except OSError:
                pass

    def stop_pump(self) -> None:
        self._pump_stop.set()
        if self._pump is not None:
            self._pump.join(timeout=1)
            if self._pump.is_alive():
                raise DeliveryBlocked("DELIVERY_PUMP_STOP_UNKNOWN")
            self._pump = None

    def snapshot(self) -> dict[str, object]:
        return {**self.buffer.snapshot(), "encoder_pid": self.producer.pid if self.producer else 0,
                "encoder_running": self.producer is not None and self.producer.poll() is None,
                "sender_pid": self.sender.pid if self.sender is not None and self.sender.poll() is None else 0}

    def close(self) -> None:
        self.stop_pump()
        for proc in (self.sender, self.producer):
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1)
        self.buffer.close()
        if self._reader is not None:
            self._reader.join(timeout=1)
