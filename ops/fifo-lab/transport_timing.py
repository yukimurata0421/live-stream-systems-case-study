#!/usr/bin/env python3
"""Compare TCP, TLS and RTMP publish timing after the same controlled RST."""
from __future__ import annotations

import argparse
import json
import statistics

from run import Lab


class TransportLab(Lab):
    cpu_quota = '100%'

    def validate_media(self, case, pid, after):
        return {}

    def command(self, role, **value):
        if value.get('op') == 'start':
            value.update(trace=True, restart_delay=0, sender_gop=10,
                         transport_probe=True, decode_media=False)
        super().command(role, **value)

    def run_transport_case(self, mode, repetition):
        case = f'transport-{repetition:02d}-{mode}'
        self.marker('transport_case_begin', case=case, mode=mode)
        self.command('oci', op='start', case=case)
        self.wait(lambda e: e['kind'] == 'server_started' and e.get('case') == case)
        self.duration(.3)
        self.command('dell', op='start', case=case, mode=mode)
        original = self.wait(lambda e: e['kind'] == 'sender_started' and e.get('case') == case)
        self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                  and e['stage'] == 'rtmps_publish_ack', 15)
        self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                  and e.get('stage') == 'rtmp_publish', 15)
        self.validate_media(case, original['pid'], original['mono'])
        self.duration(1)
        if mode == 'direct':
            self.command('dell', op='expect_restart')
        trigger = self.marker('connection_fault_trigger', case=case)
        self.command('oci', op='reset_connection')
        error = self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                          and e['stage'] == 'transport_error' and e['mono'] > trigger, 10)
        ack = self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                        and e['stage'] == 'rtmps_publish_ack' and e['mono'] > error['mono'], 15)
        stages = [e for e in self.events if e['kind'] == 'transport_probe' and e.get('case') == case
                  and e['mono'] >= error['mono'] and e['app_pid'] == ack['app_pid']
                  and e['connection'] == ack['connection']]

        def event(stage):
            return next(e for e in stages if e['stage'] == stage)

        tcp_begin = event('tcp_begin')
        tcp_done = event('tcp_complete')
        tls_begin = event('tls_begin')
        tls_done = event('tls_complete')
        verified = event('tls_verified')
        assert error['mono'] < tcp_begin['mono'] < tcp_done['mono'] <= tls_begin['mono']
        assert tls_begin['mono'] < tls_done['mono'] <= verified['mono'] < ack['mono']
        assert tls_done['value'] == 0, 'resumed TLS session would confound the comparison'
        assert (ack['app_pid'] == original['pid']) == (mode == 'fifo')
        # Validate renewed publishing separately; it is not the timing endpoint.
        self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                  and e.get('stage') == 'rtmp_publish' and self.local_time(e) > error['mono'], 10)
        media_evidence = self.validate_media(case, ack['app_pid'], error['mono'])
        self.duration(.5)
        starts = [e for e in self.events if e['kind'] == 'sender_started' and e.get('case') == case]
        assert len(starts) == (1 if mode == 'fifo' else 2)
        new = starts[-1]
        exits = [e for e in self.events if e['kind'] == 'sender_exited' and e.get('case') == case]
        result = {
            'case': case, 'mode': mode, 'repetition': repetition,
            'error_to_tcp_begin_sec': tcp_begin['mono'] - error['mono'],
            'tcp_connect_sec': tcp_done['mono'] - tcp_begin['mono'],
            'tcp_to_tls_begin_sec': tls_begin['mono'] - tcp_done['mono'],
            'tls_handshake_sec': tls_done['mono'] - tls_begin['mono'],
            'tls_certificate_verification_sec': verified['mono'] - tls_done['mono'],
            'tls_verified_to_rtmps_ack_sec': ack['mono'] - verified['mono'],
            'tcp_complete_to_tls_complete_sec': tls_done['mono'] - tcp_done['mono'],
            'tls_complete_to_rtmps_ack_sec': ack['mono'] - tls_done['mono'],
            'tcp_begin_to_rtmps_ack_sec': ack['mono'] - tcp_begin['mono'],
            'error_to_tcp_complete_sec': tcp_done['mono'] - error['mono'],
            'error_to_tls_complete_sec': tls_done['mono'] - error['mono'],
            'error_to_rtmps_ack_sec': ack['mono'] - error['mono'],
            'fault_trigger_to_error_sec': error['mono'] - trigger,
            'old_sender_pid': original['pid'], 'recovered_sender_pid': ack['app_pid'],
            'sender_generations': len(starts), 'tls_session_resumed': bool(tls_done['value']),
            'new_spawn_to_tcp_begin_sec': tcp_begin['mono'] - new['mono'] if mode == 'direct' else None,
            'exit_to_spawn_sec': new['mono'] - exits[0]['mono'] if mode == 'direct' else None,
            'same_sender_pid': mode == 'fifo', 'restart_delay_sec': 0,
            'decoder_started': False, 'outcome': 'RTMPS_PUBLISH_ACK_RECEIVED',
            **media_evidence,
        }
        self.results.append(result)
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        self.marker('transport_case_result', **result)
        print(json.dumps(result), flush=True)
        self.command('dell', op='stop')
        self.command('oci', op='stop')
        self.duration(.7)

    def summarize(self):
        rows = {}
        keys = [k for k, v in self.results[0].items() if k.endswith('_sec') and isinstance(v, float)]
        for mode in ('direct', 'fifo'):
            values = [r for r in self.results if r['mode'] == mode]
            rows[mode] = {'samples': len(values)}
            for key in keys:
                sample = [r[key] for r in values if r[key] is not None]
                if sample:
                    rows[mode][key] = {'median': statistics.median(sample), 'min': min(sample), 'max': max(sample)}
        (self.root / 'summary.json').write_text(json.dumps(rows, indent=2) + '\n')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--mediamtx', required=True)
    p.add_argument('--pairs', type=int, choices=range(1, 6), default=3)
    args = p.parse_args()
    lab = TransportLab(args)
    try:
        lab.start()
        for rep in range(1, args.pairs + 1):
            modes = ('direct', 'fifo') if rep % 2 else ('fifo', 'direct')
            for mode in modes:
                lab.run_transport_case(mode, rep)
        lab.summarize()
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
