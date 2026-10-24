"""Add FIFO transport facts to an injected read-only metrics collector."""
from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess

from .transport_telemetry import REGISTRY, TelemetryFiles


def parse_sockets(text: str, pid: int, ports: tuple[int, ...]) -> dict:
    candidates = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        fields = line.split()
        if len(fields) < 5 or fields[0] != 'ESTAB' or f'pid={pid},' not in line:
            continue
        if not any(fields[4].endswith(':' + str(port)) for port in ports):
            continue
        detail = lines[index + 1] if index + 1 < len(lines) else ''
        def number(key):
            match = re.search(r'\b' + key + r':(\d+)', detail)
            return int(match.group(1)) if match else 0
        cookie = re.search(r'\bsk:([0-9a-fA-F]+)', line)
        inode = re.search(r'\bino:(\d+)', line)
        candidates.append({'send_q': int(fields[2]), 'bytes_sent': number('bytes_sent'),
            'bytes_acked': number('bytes_acked'), 'notsent': number('notsent'),
            'unacked': number('unacked'), 'lastsnd_ms': number('lastsnd'), 'rto_ms': number('rto'),
            'socket_cookie': cookie.group(1) if cookie else '',
            'socket_inode': inode.group(1) if inode else ''})
    return candidates[0] if len(candidates) == 1 else {}


class TransportMetricsCollector:
    """Callable injected by the stream runtime; no global patches or effects."""

    def __init__(self, root: Path, *, journal_root: Path | None = None):
        self.sink = TelemetryFiles(root, journal_root=journal_root)

    def __call__(self, pid: int, ports: tuple[int, ...]) -> dict:
        completed = subprocess.run(['ss', '-tinpe'], capture_output=True, text=True, timeout=2)
        metrics = parse_sockets(completed.stdout, pid, ports) if completed.returncode == 0 else {}
        capture = REGISTRY.get(pid)
        if capture is None:
            return metrics
        state = capture.observe(metrics)
        write_ok = 1
        try:
            self.sink.write(state)
        except (OSError, ValueError):
            write_ok = 0
        # Preserve missing-socket semantics for existing readers and policy.
        if metrics:
            metrics.update(transport_connection_generation=state['connection_generation'],
                transport_reconnections_total=state['reconnections_observed_total'],
                fifo_recovery_reports_total=state['fifo_recovery_reports_total'],
                fifo_recovery_failures_total=state['fifo_recovery_failures_total'],
                transport_state=state['state'], transport_telemetry_write_ok=write_ok)
        return metrics


def make_transport_collector(cfg, observation_path: Path):
    if not cfg.use_fifo_recovery or cfg.test_mode:
        return None
    journal = os.environ.get('STREAM_FIFO_TELEMETRY_JOURNAL_DIR', '')
    return TransportMetricsCollector(observation_path.parent,
        journal_root=Path(journal) if journal else None)
