#!/usr/bin/env python3
"""Sample upload counters independently, or replicate their read-only journal."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from stream_core.common import upload_samples as samples

clock = samples.clock
COUNTER_FILE = Path('/run/stream-v3-control/runtime-observation.json')


def counter_snapshot(value, now):
    """Whitelist physical telemetry; control identity is deliberately not an input."""
    if value.get('schema_version') != 'runtime.ffmpeg_observation.v1':
        raise ValueError('counter_schema_invalid')
    metrics = value.get('tcp_metrics')
    if not isinstance(metrics, dict) or value.get('ffmpeg_running') is not True:
        raise ValueError('counter_unavailable')
    if str(value.get('managed_child_cardinality')) != '1':
        raise ValueError('counter_child_ambiguous')
    pid, sequence = value.get('local_ffmpeg_pid'), value.get('sequence')
    if type(pid) is not int or pid <= 1 or type(sequence) is not int or sequence <= 0:
        raise ValueError('counter_identity_invalid')
    names = ('producer_instance_id', 'ffmpeg_generation', 'observed_at')
    if any(not isinstance(value.get(key), str) or not value[key] for key in names):
        raise ValueError('counter_identity_invalid')
    stamp = samples.epoch(value['observed_at'])
    age = now['wall'] - stamp
    if not 0 <= age <= 3:
        raise ValueError('counter_timestamp_invalid')
    for key in ('bytes_sent', 'bytes_acked'):
        if type(metrics.get(key)) is not int or metrics[key] < 0:
            raise ValueError('counter_invalid')
    cookie = metrics.get('socket_cookie', '')
    if not isinstance(cookie, str) or not cookie:
        raise ValueError('counter_connection_invalid')
    return {'pid': pid, 'sequence': sequence, 'producer': value['producer_instance_id'],
            'generation': value['ffmpeg_generation'], 'connection': cookie,
            'observed_at': value['observed_at'], 'observed': stamp,
            'bytes_sent': metrics['bytes_sent'], 'bytes_acked': metrics['bytes_acked'],
            'read_clock': now, 'counter_age': age}


class Sampler:
    def __init__(self, state, *, identity, host):
        self.identity, self.host = identity, host
        self.state = dict(state) if isinstance(state, dict) and state.get('identity') == identity else {}
        self.state['identity'] = identity

    def reset(self, current):
        self.state.update(anchor=current, seen=current, measurement_epoch=secrets.token_hex(16))

    def step(self, value, now):
        current = counter_snapshot(value, now)
        previous, anchor = self.state.get('seen'), self.state.get('anchor')
        same = lambda a, b: all(a.get(key) == b.get(key) for key in ('pid', 'producer', 'generation', 'connection'))
        if not isinstance(previous, dict) or not isinstance(anchor, dict):
            self.reset(current)
            return None
        try:
            elapsed = now['mono'] - previous['read_clock']['mono']
            wall_elapsed = now['wall'] - previous['read_clock']['wall']
            if (not same(current, previous) or not same(current, anchor)
                    or not 0 < elapsed <= samples.FRESH_SECONDS
                    or abs(wall_elapsed - elapsed) > clock.STEP_LIMIT
                    or current['bytes_sent'] < previous['bytes_sent']
                    or current['sequence'] < previous['sequence']):
                self.reset(current)
                return None
            if current['sequence'] == previous['sequence']:
                return None
            source_elapsed = current['observed'] - previous['observed']
            if source_elapsed <= 0 or abs(source_elapsed - elapsed) > 6:
                self.reset(current)
                return None
            self.state['seen'] = current
            duration = current['observed'] - anchor['observed']
            if duration < 60:
                return None
            if duration > samples.FRESH_SECONDS:
                self.reset(current)
                return None
            delta = current['bytes_sent'] - anchor['bytes_sent']
            if delta < 0:
                self.reset(current)
                return None
        except (KeyError, TypeError, ValueError):
            self.reset(current)
            return None
        identity = self.identity
        key = [identity['machine_id_sha256'], identity['boot_id'], current['producer'],
               current['generation'], current['pid'], current['connection'], current['sequence']]
        row = {'schema': samples.SCHEMA, 'kind': 'tcp_send_sample',
               'sample_id': hashlib.sha256(json.dumps(key).encode()).hexdigest(),
               'source_machine': identity['machine_id_sha256'], 'source_boot': identity['boot_id'],
               'source_host': self.host, 'source_mono': now['mono'],
               'source_counter_age_sec': current['counter_age'], 'ts_utc': current['observed_at'],
               'source_ts_utc': current['observed_at'], 'producer_instance_id': current['producer'],
               'ffmpeg_generation': current['generation'], 'ffmpeg_pid': current['pid'],
               'connection_id': current['connection'], 'measurement_epoch': self.state['measurement_epoch'],
               'sequence_start': anchor['sequence'], 'sequence_end': current['sequence'],
               'sample_start_observed_at': anchor['observed_at'], 'sample_end_observed_at': current['observed_at'],
               'bytes_sent': current['bytes_sent'], 'bytes_sent_delta': delta,
               'bytes_acked': current['bytes_acked'], 'sample_interval_sec': duration,
               'mbps': round(delta * 8 / duration / 1_000_000, 3)}
        samples.validate_sample(row, identity['machine_id_sha256'])
        self.state['anchor'] = current
        return row


def load_state(path, limit=16384):
    try:
        return samples.read_json(path, limit)
    except FileNotFoundError:
        return {}


def sample_once(directory, sampler):
    now = clock.capture()
    status = {'schema': 'stream.upload_sampler_status.v1', **sampler.identity,
              'source_host': sampler.host, 'source_mono': now['mono'], 'at_utc': samples.utc(now['wall'])}
    try:
        value = samples.read_json(COUNTER_FILE, 65536)
        row = sampler.step(value, now)
        # Commit the counter baseline first. A failed journal write loses a
        # measurement rather than allowing overlapping intervals after restart.
        samples.write_json(directory / 'sampler-state.json', sampler.state)
        if row is not None:
            samples.append_sample(directory / 'samples.jsonl', row)
        status.update(status='READY' if row else 'WAITING', measurement_epoch=sampler.state.get('measurement_epoch'))
    except (OSError, ValueError, TypeError, KeyError) as error:
        status.update(status='UNAVAILABLE', reason=type(error).__name__, measurement_epoch=sampler.state.get('measurement_epoch'))
    samples.write_json(directory / 'sampler-state.json', sampler.state)
    samples.write_json(directory / 'status.json', status)
    return status


def serve(directory, nonce):
    started = clock.capture()
    identity = clock.identity()
    status = samples.read_json(directory / 'status.json')
    if (status.get('boot_id') != identity['boot_id']
            or status.get('machine_id_sha256') != identity['machine_id_sha256']
            or not 0 <= started['mono'] - status['source_mono'] <= 20):
        raise ValueError('sampler_status_unavailable')
    rows = []
    for row in samples.journal_rows(directory):
        try:
            age = samples.age_upper(row, now=started, local_identity=identity,
                                    expected_machine=identity['machine_id_sha256'])
            if age <= samples.RECOVERY_SECONDS + samples.FRESH_SECONDS:
                rows.append(row)
        except (ValueError, TypeError, KeyError):
            continue
    packet = {'schema': 'stream.upload_measurement_batch.v1', 'status': status,
              'samples': rows[-32:], 'clock': clock.envelope(nonce, started)}
    encoded = json.dumps(packet, separators=(',', ':'), allow_nan=False)
    if len(encoded.encode()) > samples.TAIL_BYTES:
        raise ValueError('measurement_batch_too_large')
    return encoded


def accept_packet(directory, packet, nonce, sent, received, *, expected_machine, host, identity):
    if packet.get('schema') != 'stream.upload_measurement_batch.v1' or not isinstance(packet.get('samples'), list):
        raise ValueError('measurement_batch_invalid')
    status = packet['status']
    if status.get('source_host') != host or status.get('machine_id_sha256') != expected_machine:
        raise ValueError('measurement_peer_mismatch')
    envelope = packet['clock']
    clock.monotonic_source_age(envelope, nonce, sent, received, machine=expected_machine,
        boot_id=status['boot_id'], source_mono=status['source_mono'], max_age=20)
    state_path = directory / 'pull-state.json'
    state = load_state(state_path, 262144)
    seen = list(state.get('seen', []));known = set(seen)
    pending = []
    for row in packet['samples']:
        samples.validate_sample(row, expected_machine)
        if row['source_host'] != host:
            raise ValueError('measurement_peer_mismatch')
        # The clock packet cannot attest an event from another boot.
        age = clock.monotonic_source_age(envelope, nonce, sent, received, machine=expected_machine,
            boot_id=row['source_boot'], source_mono=row['source_mono'], max_age=1080)
        identifier = row['sample_id']
        if identifier in known:
            continue
        age += row['source_counter_age_sec']
        projected = dict(row, ts_utc=samples.utc(int(received['wall'] - age)),
            receipt={'clock': envelope, 'nonce': nonce, 'sent': sent, 'received': received,
                     'receiver_boot': identity['boot_id'], 'receiver_machine': identity['machine_id_sha256']})
        pending.append(projected)
        seen.append(identifier);known.add(identifier)
    for row in pending:
        samples.append_sample(directory / 'samples.jsonl', row)
    count = len(pending)
    samples.write_json(state_path, {'seen': seen[-1000:]})
    samples.write_json(directory / 'status.json', {'schema': 'stream.upload_replica_status.v1',
        'receiver_boot': identity['boot_id'], 'receiver_machine': identity['machine_id_sha256'],
        'received_mono': received['mono'], 'at_utc': samples.utc(received['wall']),
        'measurement_epoch': status.get('measurement_epoch'), 'source_status': status.get('status'),
        'source_boot': status['boot_id'], 'new_samples': count})
    return count


def pull_once(args):
    if not re.fullmatch(r'[A-Za-z0-9_-]+', args.ssh_host) or not re.fullmatch(r'[0-9a-f]{64}', args.machine):
        raise ValueError('measurement_peer_config_invalid')
    if not re.fullmatch(r'/opt/stream-v3-upload-measurement/releases/[A-Za-z0-9_-]+/ops/scripts/stream_v3_upload_measurement.py', args.remote_program):
        raise ValueError('measurement_remote_program_invalid')
    if not re.fullmatch(r'/[A-Za-z0-9_/-]+', args.remote_directory) or '..' in Path(args.remote_directory).parts:
        raise ValueError('measurement_remote_directory_invalid')
    nonce = secrets.token_hex(16);sent = clock.capture()
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
        '-o', 'ControlMaster=no', '-o', 'ControlPath=none', '-o', 'ConnectTimeout=3', args.ssh_host,
        '/usr/bin/python3', '-I', '-B', args.remote_program, 'serve', '--nonce', nonce,
        '--directory', args.remote_directory], capture_output=True, timeout=10)
    received = clock.capture()
    if result.returncode or len(result.stdout) > samples.TAIL_BYTES:
        raise ValueError('measurement_pull_unavailable')
    return accept_packet(args.directory, json.loads(result.stdout), nonce, sent, received,
        expected_machine=args.machine, host=args.source_host, identity=clock.identity())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['sample', 'serve', 'pull'])
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--source-host', default='encoder-host')
    parser.add_argument('--nonce')
    parser.add_argument('--ssh-host', default='encoder-host')
    parser.add_argument('--remote-program')
    parser.add_argument('--remote-directory')
    parser.add_argument('--machine')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if args.mode == 'serve':
        print(serve(args.directory, args.nonce))
        return 0
    args.directory.mkdir(parents=True, exist_ok=True)
    with (args.directory / '.writer.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sampler = Sampler(load_state(args.directory / 'sampler-state.json'), identity=clock.identity(), host=args.source_host) if args.mode == 'sample' else None
        while True:
            started = time.monotonic()
            try:
                status = sample_once(args.directory, sampler) if sampler else {'accepted': pull_once(args)}
                print(json.dumps({'at': samples.utc(time.time()), 'mode': args.mode, **status}), flush=True)
            except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
                print(json.dumps({'at': samples.utc(time.time()), 'mode': args.mode, 'error': type(error).__name__}), flush=True)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(max(1., (5 if sampler else 30) - (time.monotonic() - started)))


if __name__ == '__main__':
    raise SystemExit(main())
