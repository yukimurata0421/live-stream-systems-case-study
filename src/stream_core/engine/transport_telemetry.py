"""Read-only process and connection generations; no recovery decisions."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import threading
import time
import weakref
from datetime import datetime, timezone

REGISTRY: weakref.WeakValueDictionary[int, TransportTelemetry] = weakref.WeakValueDictionary()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


class TransportTelemetry:
    def __init__(self, pid: int, generation: str, *, clock=time.monotonic):
        self.pid = pid
        self.generation = generation
        self.clock = clock
        self.lock = threading.RLock()
        self.connection_generation = 0
        self.connection_id = ''
        self.last_cookie = ''
        self.last_ack: int | None = None
        self.last_ack_at: float | None = None
        self.last_ack_utc = ''
        self.ack_bytes_total = 0
        self.recovery_reports = 0
        self.recovery_failures = 0
        self.queue_overflows = 0
        self.recovering = False
        self.recovery_started: float | None = None
        self.recovery_duration: float | None = None
        self.state = 'STARTING'
        self.events: list[dict] = []
        self.event_sequence = 0
        self.events_overwritten = 0
        REGISTRY[pid] = self

    def event(self, kind: str, **fields):
        self.event_sequence += 1
        value = {'schema': 'stream_v3.ffmpeg_transport_event.v1', 'sequence': self.event_sequence,
                 'observed_at': utc_now(), 'monotonic_sec': self.clock(), 'kind': kind,
                 'ffmpeg_pid': self.pid, 'ffmpeg_generation': self.generation,
                 'connection_generation': self.connection_generation, **fields}
        self.events.append(value)
        if len(self.events) > 128:
            self.events.pop(0)
            self.events_overwritten += 1

    def line(self, text: str):
        with self.lock:
            if 'Recovery successful' in text:
                self.recovery_reports += 1
                self.recovery_duration = (self.clock() - self.recovery_started
                                          if self.recovery_started is not None else None)
                self.recovering = False
                self.event('fifo_output_reopen_reported', duration_sec=self.recovery_duration,
                           start_observed=self.recovery_started is not None)
                self.recovery_started = None
            elif 'Recovery failed:' in text:
                self.recovery_failures += 1
                self._start_recovery()
                self.event('fifo_recovery_failed')
            elif 'queue overflow' in text.lower():
                self.queue_overflows += 1
                self.event('fifo_queue_overflow')
            elif ('[tls @' in text or '[rtmps @' in text or '[fifo @' in text) and any(
                token in text for token in ('IO error:', 'Error in the push function',
                                           'Broken pipe', 'Connection reset by peer')):
                self._start_recovery()

    def _start_recovery(self):
        if not self.recovering:
            self.recovering = True
            self.recovery_started = self.clock()
            self.event('fifo_output_error_observed')

    def observe(self, metrics: dict) -> dict:
        with self.lock:
            now = self.clock()
            cookie = str(metrics.get('socket_cookie') or '')
            inode = str(metrics.get('socket_inode') or '')
            if not cookie or not inode:
                new_state = 'SOCKET_UNAVAILABLE'
            else:
                identity = hashlib.sha256((self.generation + '|' + cookie + '|' + inode).encode()).hexdigest()[:24]
                ack = int(metrics['bytes_acked'])
                if identity != self.connection_id:
                    prior = self.connection_id
                    self.connection_id = identity
                    self.last_cookie = cookie
                    self.connection_generation += 1
                    self.last_ack = ack
                    self.ack_bytes_total += ack
                    self.last_ack_at = now
                    self.last_ack_utc = utc_now() if ack > 0 else ''
                    self.event('tcp_connection_observed', connection_id=identity,
                               previous_connection_id=prior, initial=not bool(prior))
                    new_state = 'TCP_ESTABLISHED'
                elif self.last_ack is not None and ack < self.last_ack:
                    new_state = 'COUNTER_REGRESSION'
                elif self.last_ack is not None and ack > self.last_ack:
                    self.ack_bytes_total += ack - self.last_ack
                    self.last_ack = ack
                    self.last_ack_at = now
                    self.last_ack_utc = utc_now()
                    new_state = 'ACK_ADVANCING'
                else:
                    pending = any(int(metrics.get(k) or 0) > 0 for k in ('send_q', 'notsent', 'unacked'))
                    new_state = ('TCP_ACK_STALLED' if pending and self.last_ack_at is not None
                                 and now - self.last_ack_at >= 4 else 'TCP_ESTABLISHED')
            if new_state != self.state:
                previous = self.state
                self.state = new_state
                self.event('transport_state_changed', previous=previous, state=new_state)
            return {
                'schema': 'stream_v3.ffmpeg_transport.v1', 'observed_at': utc_now(),
                'ffmpeg_pid': self.pid, 'ffmpeg_generation': self.generation,
                'state': self.state, 'connection_generation': self.connection_generation,
                'connection_id': self.connection_id, 'socket_cookie': self.last_cookie,
                'connections_observed_total': self.connection_generation,
                'reconnections_observed_total': max(0, self.connection_generation - 1),
                'ack_bytes_observed_total': self.ack_bytes_total,
                'last_ack_progress_at': self.last_ack_utc,
                'fifo_recovering': self.recovering, 'fifo_recovery_reports_total': self.recovery_reports,
                'fifo_recovery_failures_total': self.recovery_failures,
                'fifo_queue_overflows_total': self.queue_overflows,
                'last_fifo_recovery_duration_sec': self.recovery_duration,
                'events_overwritten_total': self.events_overwritten,
                'events': list(self.events), 'tcp_metrics': dict(metrics),
                'delivery_or_video_verified': False,
            }


def atomic_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix='.' + path.name + '.', delete=False)
    try:
        with handle:
            os.fchmod(handle.fileno(), 0o644)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(handle.name, path)
    finally:
        Path(handle.name).unlink(missing_ok=True)


class TelemetryFiles:
    def __init__(self, root: Path, *, journal_root: Path | None = None):
        self.root = root
        self.journal_root = journal_root if journal_root is not None else root
        self.last_generation = ''
        self.last_sequence = 0

    def write(self, state: dict):
        generation = state['ffmpeg_generation']
        if generation != self.last_generation:
            self.last_generation, self.last_sequence = generation, 0
        path = self.journal_root / 'ffmpeg-transport-events.jsonl'
        self.root.mkdir(parents=True, exist_ok=True)
        self.journal_root.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size >= 2 * 1024 * 1024:
            for index in range(3, 0, -1):
                source = path if index == 1 else path.with_name(path.name + f'.{index-1}')
                if source.exists():
                    source.replace(path.with_name(path.name + f'.{index}'))
        pending = [e for e in state['events'] if e['sequence'] > self.last_sequence]
        if pending:
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o644)
            with os.fdopen(fd, 'a', encoding='utf-8') as file:
                if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
                    raise OSError('transport journal must be a regular file')
                for event in pending:
                    file.write(json.dumps(event, sort_keys=True, separators=(',', ':')) + '\n')
                file.flush()
                os.fsync(file.fileno())
            self.last_sequence = pending[-1]['sequence']
        snapshot = {k: v for k, v in state.items() if k != 'events'}
        atomic_text(self.root / 'ffmpeg-transport.json', json.dumps(snapshot, sort_keys=True) + '\n')
        values = {
            'pid': state['ffmpeg_pid'], 'connection_generation': state['connection_generation'],
            'connections_total': state['connections_observed_total'],
            'reconnections_total': state['reconnections_observed_total'],
            'ack_bytes_total': state['ack_bytes_observed_total'],
            'fifo_recovery_reports_total': state['fifo_recovery_reports_total'],
            'fifo_recovery_failures_total': state['fifo_recovery_failures_total'],
            'fifo_queue_overflows_total': state['fifo_queue_overflows_total'],
            'tcp_ack_stalled': int(state['state'] == 'TCP_ACK_STALLED'),
            'fifo_recovering': int(state['fifo_recovering']),
            'observed_timestamp_seconds': time.time(),
        }
        atomic_text(self.root / 'ffmpeg-transport.prom', ''.join(
            f'# TYPE stream_v3_ffmpeg_transport_{key} '
            + ('counter' if key.endswith('_total') else 'gauge') + '\n'
            + f'stream_v3_ffmpeg_transport_{key} {value}\n' for key, value in values.items()))
