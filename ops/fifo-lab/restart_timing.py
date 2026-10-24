#!/usr/bin/env python3
"""Separate isolated FFmpeg stop/start and receiver delays."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from run import Lab


class RestartLab(Lab):
    profile = 'reference'
    delay = 5
    gop = 10

    def command(self, role, **value):
        if value.get('op') == 'start':
            value.update(trace=True, reader_profile=self.profile, restart_delay=self.delay,
                         sender_gop=self.gop)
        super().command(role, **value)

    def restart_case(self, delay, profile):
        self.profile, self.delay = profile, delay
        case = f'restart-delay{delay}-{profile}' + (f'-gop{self.gop}' if self.gop != 10 else '')
        self.marker('restart_case_begin', case=case, restart_delay=delay, reader_profile=profile)
        self.command('oci', op='start', case=case)
        self.wait(lambda e: e['kind'] == 'server_started' and e.get('case') == case)
        self.duration(.4)
        self.command('dell', op='start', case=case, mode='direct')
        started = self.wait(lambda e: e['kind'] == 'sender_started' and e.get('case') == case)
        first = self.wait(lambda e: e['kind'] == 'frame' and e.get('case') == case, 20)
        self.duration(3)
        trigger = self.marker('restart_trigger', case=case)
        self.command('dell', op='restart_sender')
        exited = self.wait(lambda e: e['kind'] == 'sender_exited' and e.get('case') == case, 10)
        restarted = self.wait(lambda e: e['kind'] == 'sender_started' and e.get('case') == case
                              and e['generation'] == 2, 12)
        fresh = self.wait(lambda e: e['kind'] == 'frame' and e.get('case') == case
                          and self.local_time(e) > restarted['mono']
                          and e['counter'] >= restarted['counter_offset'] + 2, 20)
        self.duration(2)
        traces = [e for e in self.events if e['kind'] == 'trace' and e.get('case') == case]

        def first_stage(component, stage, after):
            found = [e for e in traces if e['component'] == component and e['stage'] == stage
                     and self.local_time(e) > after]
            return min((self.local_time(e) for e in found), default=None)

        publish = first_stage('server', 'rtmp_publish', restarted['mono'])
        initial_publish = first_stage('server', 'rtmp_publish', started['mono'])
        opened = first_stage('server', 'rtmp_open', restarted['mono'])
        demux = first_stage('decoder', 'demux_packet', restarted['mono'])
        decoded = first_stage('decoder', 'decoded_frame', restarted['mono'])
        if publish is None or initial_publish is None or opened is None:
            raise RuntimeError('MediaMTX connection/publish trace missing')
        result = {'case': case, 'restart_delay_setting_sec': delay, 'reader_profile': profile,
                  'sender_gop_frames': self.gop,
                  'initial_spawn_to_publish_sec': initial_publish - started['mono'],
                  'initial_spawn_to_raw_frame_sec': self.local_time(first) - started['mono'],
                  'restart_request_to_exit_sec': exited['mono'] - trigger,
                  'exit_to_spawn_sec': restarted['mono'] - exited['mono'],
                  'spawn_to_tcp_accept_sec': opened - restarted['mono'],
                  'spawn_to_publish_sec': publish - restarted['mono'],
                  'publish_to_demux_sec': demux - publish if demux is not None else None,
                  'publish_to_decoded_trace_sec': decoded - publish if decoded is not None else None,
                  'publish_to_raw_frame_sec': self.local_time(fresh) - publish,
                  'spawn_to_raw_frame_sec': self.local_time(fresh) - restarted['mono'],
                  'request_to_raw_frame_sec': self.local_time(fresh) - trigger,
                  'old_sender_pid': started['pid'], 'new_sender_pid': restarted['pid'],
                  'new_frame_counter': fresh['counter'],
                  'clock_uncertainty_sec': self.clock_uncertainty}
        self.results.append(result)
        self.marker('restart_case_result', **result)
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        print(json.dumps(result), flush=True)
        self.command('dell', op='stop')
        self.command('oci', op='stop')
        self.duration(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--local-root', required=True)
    parser.add_argument('--remote-root', required=True)
    parser.add_argument('--unit', required=True)
    parser.add_argument('--mediamtx', required=True)
    parser.add_argument('--include-fifo-outage', action='store_true')
    parser.add_argument('--case-set', choices=('base', 'probe_control', 'keyframe_control'), default='base')
    args = parser.parse_args()
    lab = RestartLab(args)
    try:
        lab.start()
        if args.case_set == 'base':
            cases = ((5, 'reference'), (0, 'reference'), (0, 'output_thread1'))
        elif args.case_set == 'probe_control':
            cases = ((0, 'preserve_probe'),)
        else:
            lab.gop = 1
            cases = ((0, 'output_thread1'),)
        for delay, profile in cases:
            lab.restart_case(delay, profile)
        if args.include_fifo_outage:
            lab.profile, lab.delay, lab.gop = 'reference', 5, 10
            lab.run_case('fifo', 'outage_and_ip')
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
