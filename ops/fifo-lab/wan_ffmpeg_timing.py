#!/usr/bin/env python3
"""Measure direct-output FFmpeg exit/restart across the same isolated WAN fault."""
from __future__ import annotations

import argparse

from wan_fifo_timing import WanFifoLab


class WanFfmpegLab(WanFifoLab):
    sender_mode = 'direct'

    def before_wan_fault(self):
        self.request('dell', 'restart_armed', op='expect_restart',
                     max_restarts=20, retry_wait_sec=1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--guard-observation', required=True, help='Read-only runtime observation to protect during the experiment')
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--mediamtx', required=True)
    p.add_argument('--outages', nargs='+', type=int, choices=(12, 30, 60), default=(12, 30, 60))
    args = p.parse_args()
    if len(set(args.outages)) != len(args.outages):
        p.error('outage cases must be unique within a batch')
    lab = WanFfmpegLab(args)
    try:
        lab.start()
        for outage in args.outages:
            lab.run_wan_case(outage)
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
