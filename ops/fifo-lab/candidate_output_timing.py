#!/usr/bin/env python3
"""Check the candidate output builder and telemetry on synthetic AV, not the full production command."""
import argparse
import json

from wan_fifo_timing import WanFifoLab


class CandidateLab(WanFifoLab):
    def command(self, role, **value):
        if value.get('op') == 'start':
            value['tuning_profile'] = 'production_candidate'
            if role == 'dell':
                value['runtime_candidate'] = True
        super().command(role, **value)

    def run_wan_case(self, outage):
        super().run_wan_case(outage)
        result = self.results[-1]
        snapshots = [e for e in self.events if e['kind'] == 'tcp_snapshot'
                     and e.get('case') == result['case']]
        before, after = snapshots[0]['candidate_transport'], snapshots[-1]['candidate_transport']
        assert before['ffmpeg_pid'] == after['ffmpeg_pid']
        assert before['ffmpeg_generation'] == after['ffmpeg_generation']
        assert before['connection_generation'] == 1 and after['connection_generation'] == 2
        assert after['reconnections_observed_total'] == 1
        assert after['fifo_recovery_reports_total'] >= 1
        assert after['state'] == 'ACK_ADVANCING'
        result['candidate_telemetry_verified'] = True
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')


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
    lab = CandidateLab(args)
    try:
        lab.start()
        for outage in (12, 60):
            lab.run_wan_case(outage)
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
