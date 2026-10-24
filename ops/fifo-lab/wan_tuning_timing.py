#!/usr/bin/env python3
"""Compare three FIFO tuning profiles using the same isolation and production guard."""
from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path
import random

from node import TUNING_PROFILES
from wan_fifo_timing import WanFifoLab
from wan_repeated_timing import decompose, stats


class TuningLab(WanFifoLab):
    profile = 'baseline'

    def command(self, role, **value):
        if value.get('op') == 'start':
            value.update(tuning_profile=self.profile, fifo_verbose=True)
        super().command(role, **value)

    def before_wan_fault(self):
        self.duration(self.pre_fault_delay)

    def run_wan_case(self, outage):
        super().run_wan_case(outage)
        result = self.results[-1]
        ce = [e for e in self.events if e.get('case') == result['case']]
        rest = next(e['mono'] for e in ce if e['kind'] == 'wan_restored')
        down = next(e['mono'] for e in ce if e['kind'] == 'wan_down')
        keyframes = [e for e in ce if e['kind'] == 'trace'
                     and e.get('stage') == 'fifo_keyframe_resume' and e['mono'] >= rest]
        closes = [e for e in ce if e['kind'] == 'trace' and e.get('stage') == 'rtmp_close'
                  and self.local_time(e) >= down]
        result.update(decompose(self.events, result), profile=self.profile,
                      tuning=TUNING_PROFILES[self.profile], repetition=self.repetition,
                      restore_to_fifo_keyflag_resume_sec=keyframes[0]['mono'] - rest if keyframes else None,
                      restore_to_first_server_close_sec=self.local_time(closes[0]) - rest if closes else None,
                      fifo_keyflag_is_not_proof_of_video_idr=True)
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        self.marker('tuning_case_result', **result)


def summarize(rows):
    groups = []
    for outage in (12, 30, 60):
        for profile in TUNING_PROFILES:
            subset = [r for r in rows if r['outage_setting_sec'] == outage and r['profile'] == profile]
            if not subset:
                continue
            fields = ('restore_to_rtmps_ack_sec', 'restore_to_successful_tcp_begin_wait_sec',
                      'restore_to_tcp_residual_sec', 'tcp_complete_to_publish_sec',
                      'restore_to_fifo_keyflag_resume_sec', 'restore_to_first_server_close_sec',
                      'first_error_after_outage_sec')
            groups.append({'outage_sec': outage, 'profile': profile,
                           'metrics': {k: stats([r[k] for r in subset if r[k] is not None]) for k in fields},
                           'phases': {p: sum(r['restore_phase'] == p for r in subset)
                                      for p in sorted({r['restore_phase'] for r in subset})}})
    return groups


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--guard-observation', required=True, help='Read-only runtime observation to protect during the experiment')
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--mediamtx', required=True)
    args = p.parse_args()
    root = Path(args.output)
    root.mkdir(exist_ok=False, mode=0o700)
    baseline = TuningLab.production_state()
    (root / 'production-baseline.json').write_text(json.dumps(baseline, indent=2) + '\n')
    # One outage duration per batch: all three profiles * three repetitions.
    # Each unit remains bounded to 600s; a batch ends after three sequential cases.
    schedule = []
    for rep in range(1, 4):
        outages = [12, 30, 60]
        random.Random(2026102409 + rep).shuffle(outages)
        for outage in outages:
            profiles = list(TUNING_PROFILES)
            profiles = profiles[(rep - 1):] + profiles[:(rep - 1)]
            schedule.append({'repetition': rep, 'outage': outage, 'profiles': profiles})
    (root / 'schedule.json').write_text(json.dumps(schedule, indent=2) + '\n')
    rows = []
    for index, item in enumerate(schedule, 1):
        state = TuningLab.production_state()
        if any(state[k] != baseline[k] for k in ('ffmpeg_generation', 'local_ffmpeg_pid', 'pod_uid')):
            raise RuntimeError('production changed between batches')
        batch = argparse.Namespace(**vars(args))
        batch.output = str(root / f'batch-{index:02d}')
        batch.unit = f'{args.unit}-b{index:02d}'
        lab = TuningLab(batch)
        lab.repetition = item['repetition']
        lab.pre_fault_delay = (item['repetition'] - 1) * .2
        try:
            lab.start()
            for profile in item['profiles']:
                lab.profile = profile
                # Every case needs a distinct identity within the same batch.
                lab.sender_mode = 'fifo'
                lab.case_suffix = '-' + profile
                lab.run_wan_case(item['outage'])
                rows.append(dict(lab.results[-1]))
                (root / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
                (root / 'summary.json').write_text(json.dumps(summarize(rows), indent=2) + '\n')
                print(json.dumps({'completed': len(rows), 'planned': 27, 'profile': profile,
                                  'outage': item['outage'], 'rtmp_sec': rows[-1]['restore_to_rtmps_ack_sec']}), flush=True)
        except Exception as exc:
            lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
            raise
        finally:
            lab.close()
    (root / 'complete.json').write_text(json.dumps({'cases': len(rows),
        'finished_at_jst': datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).isoformat()}) + '\n')


if __name__ == '__main__':
    main()
