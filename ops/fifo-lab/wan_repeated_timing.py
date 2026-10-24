#!/usr/bin/env python3
"""Repeat the same WAN cases; separate retry phase from successful handshake time."""
from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path
import random
import statistics

from wan_ffmpeg_timing import WanFfmpegLab


def decompose(events, result):
    case = result['case']
    ev = [e for e in events if e.get('case') == case]
    restored = next(e['mono'] for e in ev if e['kind'] == 'wan_restored')
    down = next(e['mono'] for e in ev if e['kind'] == 'wan_down')
    probes = [e for e in ev if e['kind'] == 'transport_probe']
    ack = next(e for e in probes if e['stage'] == 'rtmps_publish_ack'
               and e['mono'] >= restored)
    key = (ack['app_pid'], ack['connection'])
    successful = {e['stage']: e for e in probes
                  if (e['app_pid'], e['connection']) == key}
    begin = successful['tcp_begin']['mono']
    tcp = successful['tcp_complete']['mono']
    tls = successful['tls_complete']['mono']
    assert tcp >= restored and begin < tcp < tls < ack['mono']
    wait = max(0, begin - restored)
    residual = tcp - max(begin, restored)
    post_tcp = ack['mono'] - tcp
    assert abs(wait + residual + post_tcp - result['restore_to_rtmps_ack_sec']) < 1e-6
    before = [e for e in probes if e['mono'] < restored]
    last_begin = next(e for e in reversed(before) if e['stage'] == 'tcp_begin')
    last_key = (last_begin['app_pid'], last_begin['connection'])
    last_events = [e for e in before if (e['app_pid'], e['connection']) == last_key]
    last_stages = {e['stage'] for e in last_events}
    phase = 'unclassified'
    if begin < restored:
        phase = 'successful_tcp_inflight'
    elif 'socket_close_begin' in last_stages:
        schedules = [e for e in ev if e['kind'] == 'restart_scheduled' and e['mono'] < restored]
        if schedules and schedules[-1]['restart_at'] > restored:
            phase = 'restart_retry_timer_wait'
        else:
            phase = 'between_closed_and_next_attempt'
    elif 'tcp_failure' in last_stages:
        phase = 'failed_tcp_pending_close'
    elif 'tcp_complete' not in last_stages:
        phase = 'earlier_unsuccessful_tcp_inflight'
    elif 'transport_error' in last_stages:
        phase = 'old_connection_error_cleanup'
    else:
        phase = 'old_connection_not_yet_failed'
    metrics = {
        'restore_phase': phase,
        'restore_to_successful_tcp_begin_wait_sec': wait,
        'successful_tcp_begin_relative_restore_sec': begin - restored,
        'successful_attempt_age_at_restore_sec': max(0, restored - begin),
        'restore_to_tcp_residual_sec': residual,
        'tcp_complete_to_tls_sec': tls - tcp,
        'tls_complete_to_publish_sec': ack['mono'] - tls,
        'tcp_complete_to_publish_sec': post_tcp,
        'successful_tcp_full_duration_sec': tcp - begin,
        'successful_tcp_was_inflight_at_restore': begin < restored,
        'first_error_after_outage_sec': next((e['mono'] - down for e in probes
            if e['stage'] == 'transport_error' and e['mono'] >= down), None),
        'original_socket_close_after_outage_sec': next((e['mono'] - down for e in probes
            if e['stage'] == 'socket_close_begin' and e['app_pid'] == result['sender_pid']
            and e['connection'] == 1 and e['mono'] >= down), None),
        'successful_tcp_attempt_key': list(key),
    }
    return metrics


def stats(values):
    if not values:
        return {'n': 0, 'mean': None, 'sample_sd': None, 'median': None, 'min': None, 'max': None}
    return {'n': len(values), 'mean': statistics.mean(values),
            'sample_sd': statistics.stdev(values) if len(values) > 1 else None,
            'median': statistics.median(values), 'min': min(values), 'max': max(values)}


def summarize(rows):
    metrics = ('restore_to_rtmps_ack_sec', 'restore_to_successful_tcp_begin_wait_sec',
               'restore_to_tcp_residual_sec', 'tcp_complete_to_publish_sec',
               'tcp_complete_to_tls_sec', 'tls_complete_to_publish_sec',
               'first_error_after_outage_sec', 'original_socket_close_after_outage_sec')
    groups = []
    for outage in (12, 30, 60):
        for mode in ('direct', 'fifo'):
            subset = [r for r in rows if r['outage_setting_sec'] == outage and r['mode'] == mode]
            if not subset:
                continue
            groups.append({'outage_sec': outage, 'mode': mode,
                'metrics': {k: stats([r[k] for r in subset if r[k] is not None]) for k in metrics},
                'by_phase': {p: stats([r['restore_to_rtmps_ack_sec'] for r in subset
                                       if r['restore_phase'] == p])
                             for p in sorted({r['restore_phase'] for r in subset})},
                'fresh_tcp_after_restore': (stats([r['successful_tcp_full_duration_sec'] for r in subset
                    if not r['successful_tcp_was_inflight_at_restore']])
                    if any(not r['successful_tcp_was_inflight_at_restore'] for r in subset) else None)})
    pairs = []
    for outage in (12, 30, 60):
        differences = []
        for repetition in sorted({r['repetition'] for r in rows}):
            modes = {r['mode']: r for r in rows if r['repetition'] == repetition
                     and r['outage_setting_sec'] == outage}
            if len(modes) == 2:
                differences.append(modes['direct']['restore_to_rtmps_ack_sec']
                                   - modes['fifo']['restore_to_rtmps_ack_sec'])
        if differences:
            pairs.append({'outage_sec': outage, 'direct_minus_fifo_sec': stats(differences),
                          'differences': differences})
    return {'groups': groups, 'paired_differences': pairs}


class RepeatedWanLab(WanFfmpegLab):
    def __init__(self, args, repetition):
        super().__init__(args)
        self.repetition = repetition
        self.pre_fault_delay = (repetition - 1) * .2

    def before_wan_fault(self):
        self.duration(self.pre_fault_delay)
        if self.sender_mode == 'direct':
            super().before_wan_fault()

    def run_wan_case(self, outage):
        super().run_wan_case(outage)
        result = self.results[-1]
        result.update(decompose(self.events, result), repetition=self.repetition,
                      pre_fault_delay_sec=self.pre_fault_delay)
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        self.marker('repeated_case_result', **result)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--guard-observation', required=True, help='Read-only runtime observation to protect during the experiment')
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--mediamtx', required=True)
    p.add_argument('--repetitions', type=int, choices=(3, 5), default=5)
    args = p.parse_args()
    root = Path(args.output)
    root.mkdir(exist_ok=False, mode=0o700)
    baseline = RepeatedWanLab.production_state()
    (root / 'campaign-baseline.json').write_text(json.dumps(baseline, indent=2) + '\n')
    schedule = []
    for rep in range(1, args.repetitions + 1):
        outages = [12, 30, 60]
        random.Random(20261024 + rep).shuffle(outages)
        for index, outage in enumerate(outages):
            modes = ('direct', 'fifo') if (rep + index) % 2 else ('fifo', 'direct')
            schedule.extend({'repetition': rep, 'outage': outage, 'mode': mode} for mode in modes)
    (root / 'schedule.json').write_text(json.dumps(schedule, indent=2) + '\n')
    rows = []
    for rep in range(1, args.repetitions + 1):
        state = RepeatedWanLab.production_state()
        if any(state[k] != baseline[k] for k in ('ffmpeg_generation', 'local_ffmpeg_pid', 'pod_uid')):
            raise RuntimeError('production identity changed between batches; no new batch')
        batch = argparse.Namespace(**vars(args))
        batch.output = str(root / f'repetition-{rep:02d}')
        batch.unit = f'{args.unit}-r{rep:02d}'
        lab = RepeatedWanLab(batch, rep)
        try:
            lab.start()
            for item in schedule:
                if item['repetition'] != rep:
                    continue
                lab.sender_mode = item['mode']
                lab.run_wan_case(item['outage'])
                rows.append(dict(lab.results[-1]))
                (root / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                (root / 'summary.json').write_text(json.dumps(summarize(rows), indent=2) + '\n')
                print(json.dumps({'progress': len(rows), 'planned': len(schedule),
                      'repetition': rep, 'mode': item['mode'], 'outage': item['outage'],
                      'rtmp_sec': rows[-1]['restore_to_rtmps_ack_sec'],
                      'phase': rows[-1]['restore_phase']}), flush=True)
        except Exception as exc:
            lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
            raise
        finally:
            lab.close()
    (root / 'complete.json').write_text(json.dumps({'cases': len(rows),
        'repetitions': args.repetitions,
        'finished_at_jst': datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).isoformat()}) + '\n')


if __name__ == '__main__':
    main()
