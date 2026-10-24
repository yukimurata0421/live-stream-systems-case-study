#!/usr/bin/env python3
"""Detect a silent owned TCP stall and recover only that output through FIFO."""
from __future__ import annotations

import argparse
import ipaddress
import json
import re
import signal
import subprocess
import threading
import time
from pathlib import Path

from nvenc_transport_timing import NvencTransportLab


def stall_candidate(ack_stopped_sec, sample):
    # FIFO/drop can keep notsent small even while a full flight of TCP data
    # remains unacknowledged. Both forms of pending output must be covered.
    return (ack_stopped_sec >= 4 and sample['send_q'] > 0
            and (sample['notsent'] >= 8192 or sample['unacked'] >= 64))


class HostPings:
    """Read-only route probes, separate from the fault-injection namespace."""
    interval_sec = .5
    def __init__(self, root, targets):
        if set(targets) != {'gateway', 'external', 'oci_public'}:
            raise ValueError('exactly three host-route ping targets required')
        for address in targets.values():
            ipaddress.IPv4Address(address)
        self.root, self.targets = root, targets
        self.children, self.threads = [], []
        self.samples = []

    def read(self, label, child):
        with (self.root / f'ping-{label}.jsonl').open('a') as file:
            for line in child.stdout:
                row = {'mono': time.monotonic(), 'target': label, 'line': line.strip(),
                       'reply': bool(re.search(r'bytes from .*icmp_seq=', line))}
                seq = re.search(r'icmp_seq[= ](\d+)', line)
                if seq:
                    row['sequence'] = int(seq.group(1))
                self.samples.append(row)
                file.write(json.dumps(row) + '\n')
                file.flush()

    def start(self):
        for label, address in self.targets.items():
            child = subprocess.Popen(['ping', '-n', '-D', '-O', '-i', str(self.interval_sec), '-W', '1', address],
                                     stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, bufsize=1,
                                     start_new_session=True)
            self.children.append(child)
            thread = threading.Thread(target=self.read, args=(label, child), daemon=True)
            thread.start()
            self.threads.append(thread)

    def close(self):
        for child in self.children:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
        for child in self.children:
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=2)
        for thread in self.threads:
            thread.join(timeout=2)
        (self.root / 'ping-cleanup.json').write_text(json.dumps({
            label: {'pid': child.pid, 'exited': child.poll() is not None,
                    'returncode': child.returncode}
            for label, child in zip(self.targets, self.children)}, indent=2) + '\n')


class StallFifoLab(NvencTransportLab):
    tasks_max = 128

    def __init__(self, args):
        super().__init__(args)
        self.host_pings = HostPings(self.root, json.loads(Path(args.ping_targets).read_text()))

    def command(self, role, **value):
        if value.get('op') == 'start':
            value.update(ping_interval_sec=HostPings.interval_sec, report_pings=True)
        super().command(role, **value)

    def start(self):
        super().start()
        self.host_pings.start()

    def request(self, role, kind, **value):
        sent = time.monotonic()
        self.command(role, **value)
        return self.wait(lambda e: e['kind'] == kind and e['role'] == role
                         and e['controller_received_mono'] > sent, 5)

    def ping_summary(self, begin, end):
        out = {}
        for label in self.host_pings.targets:
            replies = [e for e in self.host_pings.samples if e['target'] == label and e['reply']
                       and begin <= e['mono'] <= end]
            out[label] = {'replies': len(replies), 'configured_interval_sec': HostPings.interval_sec}
        replies = [e for e in self.events if e['kind'] == 'pong' and begin <= e['sent_mono'] <= end]
        out['oci_wg_isolated'] = {'replies': len(replies), 'configured_interval_sec': HostPings.interval_sec}
        return out

    def run_stall_case(self, repetition):
        case = f'fifo-silent-stall-{repetition:02d}'
        self.marker('stall_case_begin', case=case)
        self.command('oci', op='start', case=case)
        self.wait(lambda e: e['kind'] == 'server_started' and e.get('case') == case)
        self.duration(.3)
        self.command('dell', op='start', case=case, mode='fifo')
        started = self.wait(lambda e: e['kind'] == 'sender_started' and e.get('case') == case)
        self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                  and e['stage'] == 'rtmps_publish_ack', 15)
        self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                  and e.get('stage') == 'rtmp_publish', 15)
        self.validate_media(case, started['pid'], started['mono'])
        self.duration(1)
        blackhole = self.request('dell', 'blackhole_started', op='blackhole_old_tcp')
        original = blackhole['target']
        last_ack, last_progress = original['bytes_acked'], blackhole['mono']
        streak, samples = 0, []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            current = self.request('dell', 'tcp_snapshot', op='tcp_snapshot', original=True)
            if current['bytes_acked'] < last_ack:
                raise RuntimeError('owned TCP ACK counter regressed')
            if current['bytes_acked'] > last_ack:
                last_ack, last_progress = current['bytes_acked'], current['mono']
            age = current['mono'] - last_progress
            stalled = stall_candidate(age, current)
            streak = streak + 1 if stalled else 0
            samples.append(current)
            self.marker('stall_observation', case=case, observation_mono=current['mono'],
                        ack_stopped_sec=age, send_q=current['send_q'], notsent=current['notsent'],
                        unacked=current['unacked'], confirmed_samples=streak)
            if any(e['kind'] == 'transport_probe' and e.get('case') == case
                   and e['stage'] == 'transport_error' for e in self.events):
                raise RuntimeError('output errored before the explicit stall recovery action')
            if streak >= 2:
                detected = self.marker('stall_detected', case=case, ack_stopped_sec=age,
                                       send_q=current['send_q'], notsent=current['notsent'],
                                       unacked=current['unacked'],
                                       confirmation_samples=streak)
                break
            self.duration(.5)
        else:
            raise TimeoutError('silent TCP stall was not observed within 10 seconds')
        self.command('dell', op='cut_output_socket')
        cut = self.wait(lambda e: e['kind'] == 'output_cut_begin' and e.get('case') == case, 5)
        self.wait(lambda e: e['kind'] == 'output_cut_end' and e.get('case') == case, 5)
        error = self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                          and e['stage'] == 'transport_error' and e['mono'] >= cut['mono'], 5)
        ack = self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                        and e['stage'] == 'rtmps_publish_ack' and e['connection'] == 2, 15)
        fifo = self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                         and e.get('stage') == 'fifo_recovery_success' and e['mono'] > detected, 10)
        rules_observed = self.request('dell', 'blackhole_status', op='blackhole_status')
        self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                  and e.get('stage') == 'rtmp_publish' and self.local_time(e) > detected, 15)
        media = self.validate_media(case, started['pid'], error['mono'])
        new = self.request('dell', 'tcp_snapshot', op='tcp_snapshot')
        self.duration(1)
        fresh = self.request('dell', 'tcp_snapshot', op='tcp_snapshot')
        assert new['source_port'] != original['source_port'] and new['cookie'] != original['cookie']
        assert fresh['bytes_acked'] > new['bytes_acked']
        assert fresh['pid'] == new['pid'] == started['pid'] == ack['app_pid']
        assert len([e for e in self.events if e['kind'] == 'sender_started' and e.get('case') == case]) == 1
        assert not any(e['kind'] == 'sender_exited' and e.get('case') == case for e in self.events)
        after_rules = self.request('dell', 'blackhole_status', op='blackhole_status')
        counters = [expr['counter']['packets'] for item in rules_observed['rules']['nftables']
                    if 'rule' in item for expr in item['rule']['expr'] if 'counter' in expr]
        assert sum(counters) > 0
        stages = [e for e in self.events if e['kind'] == 'transport_probe' and e.get('case') == case
                  and e['connection'] == 2]
        stage = {e['stage']: e for e in stages}
        assert stage['tls_complete']['value'] == 0
        result = {'case': case, 'repetition': repetition, 'mode': 'fifo',
                  'blackhole_to_stall_detect_sec': detected - blackhole['mono'],
                  'stall_detect_to_cut_begin_sec': cut['mono'] - detected,
                  'stall_detect_to_transport_error_sec': error['mono'] - detected,
                  'stall_detect_to_tcp_complete_sec': stage['tcp_complete']['mono'] - detected,
                  'stall_detect_to_tls_complete_sec': stage['tls_complete']['mono'] - detected,
                  'stall_detect_to_rtmps_ack_sec': ack['mono'] - detected,
                  'stall_detect_to_fifo_success_sec': fifo['mono'] - detected,
                  'cut_begin_to_error_sec': error['mono'] - cut['mono'],
                  'error_to_rtmps_ack_sec': ack['mono'] - error['mono'],
                  'sender_pid': started['pid'], 'same_sender_pid': True, 'sender_generations': 1,
                  'old_socket_cookie': original['cookie'], 'new_socket_cookie': new['cookie'],
                  'old_source_port': original['source_port'], 'new_source_port': new['source_port'],
                  'old_ack_bytes_stopped': last_ack, 'drop_rule_packet_counts': counters,
                  'ping_during_stall': self.ping_summary(blackhole['mono'], cut['mono']),
                  'new_socket_ack_bytes_advanced': fresh['bytes_acked'] - new['bytes_acked'],
                  'old_drop_rules_still_active': bool(after_rules['rules']['nftables']),
                  'tls_session_resumed': False, 'decoder_started': False,
                  'outcome': 'SILENT_STALL_RECOVERED_SAME_FFMPEG_PID', **media}
        self.results.append(result)
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        self.marker('stall_case_result', **result)
        print(json.dumps(result), flush=True)
        self.command('dell', op='stop')
        self.command('oci', op='stop')
        self.duration(.7)

    def close(self):
        self.host_pings.close()
        super().close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--guard-observation', required=True, help='Read-only runtime observation to protect during the experiment')
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--ping-targets', required=True)
    p.add_argument('--mediamtx', required=True)
    p.add_argument('--cases', type=int, choices=range(1, 4), default=3)
    args = p.parse_args()
    lab = StallFifoLab(args)
    try:
        lab.start()
        for repetition in range(1, args.cases + 1):
            lab.run_stall_case(repetition)
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
