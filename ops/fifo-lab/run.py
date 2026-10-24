#!/usr/bin/env python3
"""Compare isolated WireGuard/RTMPS links over an SSH-carried packet bridge."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import queue
import shlex
import struct
import subprocess
import threading
import time

HEADER = struct.Struct('!cI')
MAX_FRAME = 131072


def read_exact(stream, size):
    out = bytearray()
    while len(out) < size:
        chunk = stream.read(size - len(out))
        if not chunk:
            raise EOFError('node channel closed')
        out.extend(chunk)
    return bytes(out)


class Lab:
    cpu_quota = '50%'
    memory_max = '384M'
    tasks_max = 64

    def extra_properties(self, role):
        return []

    def __init__(self, args):
        self.args = args
        self.root = Path(args.output)
        self.root.mkdir(exist_ok=False)
        self.events = []
        self.queue = queue.Queue(maxsize=4096)
        self.procs = {}
        self.locks = {x: threading.Lock() for x in ('dell', 'oci')}
        self.drop = False
        self.packet_counts = {x: {'forwarded': 0, 'dropped': 0} for x in ('dell', 'oci')}
        self.clock_offset = {'dell': 0.0, 'oci': 0.0}
        self.clock_uncertainty = None
        self.logs = []
        self.results = []

    def record(self, event):
        self.events.append(event)
        with (self.root / 'events.jsonl').open('a') as file:
            file.write(json.dumps(event, separators=(',', ':')) + '\n')

    def marker(self, kind, **data):
        value = {'kind': kind, 'role': 'controller', 'mono': time.monotonic(), **data}
        self.record(value)
        return value['mono']

    def send(self, role, kind, payload):
        if len(payload) > MAX_FRAME:
            raise ValueError('oversized controller frame')
        with self.locks[role]:
            self.procs[role].stdin.write(HEADER.pack(kind, len(payload)) + payload)
            self.procs[role].stdin.flush()

    def command(self, role, **value):
        self.send(role, b'C', json.dumps(value, separators=(',', ':')).encode())

    def reader(self, role):
        try:
            stream = self.procs[role].stdout
            while True:
                kind, length = HEADER.unpack(read_exact(stream, HEADER.size))
                if length > MAX_FRAME:
                    raise ValueError('oversized node frame')
                payload = read_exact(stream, length)
                self.queue.put((role, kind, payload, time.monotonic()), timeout=3)
        except (EOFError, Exception) as exc:
            self.queue.put((role, b'X', str(exc).encode(), time.monotonic()))

    def pump(self, seconds=.05, *, tolerate_close=False):
        end = time.monotonic() + seconds
        while True:
            try:
                role, kind, payload, received = self.queue.get(timeout=max(.001, end - time.monotonic()))
            except queue.Empty:
                return
            if kind == b'P':
                if self.drop:
                    self.packet_counts[role]['dropped'] += 1
                else:
                    self.packet_counts[role]['forwarded'] += 1
                    other = 'oci' if role == 'dell' else 'dell'
                    if other in self.procs and self.procs[other].poll() is None:
                        self.send(other, b'P', payload)
            elif kind == b'E':
                event = json.loads(payload)
                # Fixture public keys and the generated certificate are needed only for setup.
                event['controller_received_mono'] = received
                self.record(event)
                if event['kind'] == 'error' and not tolerate_close:
                    raise RuntimeError(f'{role}: {event}')
            elif kind == b'X' and not tolerate_close:
                raise RuntimeError(f'{role} channel closed: {payload.decode()}')
            if time.monotonic() >= end:
                return

    def wait(self, predicate, seconds=15):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            found = next((x for x in self.events if predicate(x)), None)
            if found is not None:
                return found
            self.pump(.1)
        raise TimeoutError('lab observation deadline')

    def duration(self, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self.pump(min(.1, end - time.monotonic()))

    def start(self):
        roots = {'dell': self.args.local_root, 'oci': self.args.remote_root}
        host_ns = os.readlink('/proc/self/ns/net')
        remote_ns = subprocess.check_output(['ssh', '-o', 'BatchMode=yes', self.args.host,
                                             'readlink /proc/self/ns/net'], text=True).strip()
        for role in ('dell', 'oci'):
            root = roots[role]
            unit = f'{self.args.unit}-{role}'
            props = ['PrivateNetwork=yes', 'ProtectHome=yes', 'ProtectSystem=strict',
                     'ProtectControlGroups=yes', 'ProtectKernelModules=yes', 'ProtectKernelTunables=yes',
                     'NoNewPrivileges=yes', 'MemoryMax=' + self.memory_max, 'MemorySwapMax=0', 'CPUQuota=' + self.cpu_quota,
                     'TasksMax=' + str(self.tasks_max), 'RuntimeMaxSec=600', 'TimeoutStopSec=5', 'KillMode=control-group',
                     'Nice=19', 'DevicePolicy=closed', 'DeviceAllow=/dev/net/tun rw',
                     'CapabilityBoundingSet=CAP_NET_ADMIN CAP_NET_RAW CAP_SETUID CAP_SETGID CAP_CHOWN CAP_KILL',
                     f'BindPaths={root}/keys:/etc/wireguard',
                     f'ReadWritePaths={root} /etc/wireguard',
                     'InaccessiblePaths=-/var/lib/rancher -/var/lib/stream-recovery-control '
                     '-/var/lib/cra-agent -/run/stream-v3-control '
                     '-/etc/cra-agent -/etc/cra-central']
            props += self.extra_properties(role)
            command = ['sudo', '-n', 'systemd-run', '--quiet', '--pipe', '--wait', '--collect',
                       '--unit=' + unit]
            for prop in props:
                command += ['-p', prop]
            command += ['/usr/bin/python3', '-B', root + '/node.py', '--role', role,
                        '--root', root + '/' + role, '--ffmpeg-root',
                        root + ('/sender-runtime' if role == 'dell' else '/decoder'),
                        '--host-netns', host_ns if role == 'dell' else remote_ns]
            if role == 'oci':
                command += ['--mediamtx', self.args.mediamtx]
                command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ServerAliveInterval=10',
                           self.args.host, shlex.join(command)]
            log = (self.root / (role + '-channel.stderr')).open('wb')
            self.logs.append(log)
            self.procs[role] = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                                stderr=log, bufsize=0)
            threading.Thread(target=self.reader, args=(role,), daemon=True).start()
        ready = {r: self.wait(lambda e, r=r: e['kind'] == 'ready' and e['role'] == r, 20)
                 for r in ('dell', 'oci')}
        self.command('oci', op='peer', key=ready['dell']['public_key'])
        self.command('dell', op='peer', key=ready['oci']['public_key'],
                     certificate=ready['oci']['certificate'])
        self.wait(lambda e: e['kind'] == 'pong', 15)
        samples = []
        for i in range(5):
            sent = time.monotonic()
            self.command('oci', op='clock', token=i)
            event = self.wait(lambda e, i=i: e['kind'] == 'clock' and e.get('token') == i)
            received = event['controller_received_mono']
            samples.append({'rtt': received - sent,
                            'offset': (sent + received) / 2 - event['mono']})
        best = min(samples, key=lambda x: x['rtt'])
        self.clock_offset['oci'] = best['offset']
        self.clock_uncertainty = best['rtt'] / 2
        self.marker('setup_complete', clock_samples=samples, oci_offset=best['offset'],
                    clock_uncertainty_sec=self.clock_uncertainty)

    def local_time(self, event):
        return event['mono'] + self.clock_offset.get(event['role'], 0)

    def run_case(self, mode, condition):
        case = mode + '-' + condition
        index = len(self.events)
        self.command('dell', op='ip', address='192.0.2.2')
        self.wait(lambda e: e in self.events[index:] and e['kind'] == 'ip_changed')
        self.duration(1)
        begin = self.marker('case_begin', case=case, mode=mode, condition=condition)
        self.command('oci', op='start', case=case)
        self.wait(lambda e: e['kind'] == 'server_started' and e.get('case') == case)
        self.duration(.4)
        self.command('dell', op='start', case=case, mode=mode)
        started = self.wait(lambda e: e['kind'] == 'sender_started' and e.get('case') == case)
        deadline = time.monotonic() + 20
        first = None
        while time.monotonic() < deadline and first is None:
            first = next((e for e in self.events if e['kind'] == 'frame' and e.get('case') == case), None)
            if first is None and any(e['kind'] == 'sender_exited' and e.get('case') == case for e in self.events):
                raise RuntimeError('sender exited before its first decoded frame; startup failed')
            self.pump(.1)
        if first is None:
            raise TimeoutError('initial decoded frame deadline')
        self.duration(3)
        origin = started['origin']
        before = [e for e in self.events if e['kind'] == 'frame' and e.get('case') == case]
        if len(before) < 5 or any(b['counter'] <= a['counter'] for a, b in zip(before, before[1:])):
            raise RuntimeError('decoded counter must advance before fault')
        fault_start = self.marker('fault_start', case=case)
        outage = 0 if condition == 'ip_only' else 12
        self.drop = outage > 0
        if condition != 'outage_only':
            self.command('dell', op='ip', address='198.51.100.2')
        if outage:
            self.duration(outage)
        self.drop = False
        restored = self.marker('link_restored', case=case)
        counter_at_restore = int((restored - origin) * 5)
        pong = self.wait(lambda e: e['kind'] == 'pong' and e.get('case') == case
                         and e.get('sent_mono', 0) >= restored, 20)
        fresh = self.wait(lambda e: e['kind'] == 'frame' and e.get('case') == case
                          and e['counter'] > counter_at_restore, 35)
        self.duration(3)
        self.command('oci', op='status')
        status = self.wait(lambda e: e['kind'] == 'status' and e.get('case') == case)
        frames = [e for e in self.events if e['kind'] == 'frame' and e.get('case') == case]
        after = [e for e in frames if self.local_time(e) >= restored]
        recent = after[-5:]
        if len(recent) < 3 or any(b['counter'] <= a['counter'] for a, b in zip(recent, recent[1:])):
            raise RuntimeError('decoded counter must advance after recovery')
        starts = [e for e in self.events if e['kind'] == 'sender_started' and e.get('case') == case]
        exits = [e for e in self.events if e['kind'] == 'sender_exited' and e.get('case') == case]
        gaps = [self.local_time(b) - self.local_time(a) for a, b in zip(frames, frames[1:])]
        expected = '198.51.100.2:51823' if condition != 'outage_only' else '192.0.2.2:51823'
        if expected not in status['endpoints']:
            raise RuntimeError('OCI WireGuard peer endpoint did not follow the test IPv4')
        result = {'case': case, 'mode': mode, 'condition': condition, 'outage_seconds': outage,
                  'initial_decoded_frame_sec': self.local_time(first) - started['mono'],
                  'ping_recovery_sec': pong['mono'] - restored,
                  'fresh_decoded_frame_after_link_sec': self.local_time(fresh) - restored,
                  'fresh_frame_minus_ping_sec': self.local_time(fresh) - pong['mono'],
                  'max_decoded_frame_gap_sec': max(gaps), 'decoded_frames': len(frames),
                  'sender_generations': len(starts), 'sender_exits': [e['returncode'] for e in exits],
                  'oci_endpoint': expected, 'first_recovered_counter': fresh['counter'],
                  'counter_at_link_restore': counter_at_restore,
                  'clock_uncertainty_sec': self.clock_uncertainty,
                  'same_sender_pid': len(starts) == 1,
                  'outcome': 'OBSERVED_RECOVERY'}
        self.results.append(result)
        self.marker('case_result', **result)
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        print(json.dumps(result), flush=True)
        self.command('dell', op='stop')
        self.command('oci', op='stop')
        self.duration(1)

    def close(self):
        self.drop = False
        for role, proc in self.procs.items():
            if proc.poll() is None:
                try:
                    self.command(role, op='quit')
                except (BrokenPipeError, OSError):
                    pass
        end = time.monotonic() + 8
        while time.monotonic() < end and any(p.poll() is None for p in self.procs.values()):
            try:
                self.pump(.1, tolerate_close=True)
            except (OSError, BrokenPipeError):
                break
        cleanup = {}
        for role, proc in self.procs.items():
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
            cleanup[role] = proc.returncode
        for log in self.logs:
            log.close()
        self.marker('controller_cleanup', node_exit_codes=cleanup)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--mediamtx', required=True)
    p.add_argument('--conditions', nargs='+', default=['ip_only', 'outage_only', 'outage_and_ip'],
                   choices=['ip_only', 'outage_only', 'outage_and_ip'])
    p.add_argument('--modes', nargs='+', default=['direct', 'fifo'], choices=['direct', 'fifo'])
    args = p.parse_args()
    lab = Lab(args)
    try:
        lab.start()
        for condition in args.conditions:
            for mode in args.modes:
                lab.run_case(mode, condition)
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
