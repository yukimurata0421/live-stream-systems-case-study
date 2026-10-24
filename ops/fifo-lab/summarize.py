#!/usr/bin/env python3
"""Recompute public timing summaries without network access or live state."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import statistics

METRICS = ('restore_to_rtmps_ack_sec', 'restore_to_successful_tcp_begin_wait_sec',
           'restore_to_tcp_residual_sec', 'tcp_complete_to_tls_sec',
           'tls_complete_to_publish_sec', 'tcp_complete_to_publish_sec')


def summarize(records):
    groups = defaultdict(list)
    for row in records:
        groups[(row['campaign'], row['outage_setting_sec'], row['variant'])].append(row)
    result = []
    for (campaign, outage, variant), rows in sorted(groups.items()):
        metrics = {}
        for name in METRICS:
            values = [r[name] for r in rows if r.get(name) is not None]
            if not values:
                continue
            metrics[name] = {'n':len(values), 'mean':statistics.mean(values),
                'sample_sd':statistics.stdev(values) if len(values)>1 else None,
                'median':statistics.median(values), 'min':min(values), 'max':max(values)}
        result.append({'campaign':campaign, 'outage_sec':outage, 'variant':variant,
            'metrics':metrics, 'restore_phases':dict(Counter(r['restore_phase'] for r in rows))})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    args = parser.parse_args()
    document = json.loads(args.input.read_text())
    if document['schema'] != 'stream.fifo_public_measurements.v1':
        raise ValueError('UNSUPPORTED_MEASUREMENT_SCHEMA')
    print(json.dumps(summarize(document['records']), indent=2))


if __name__ == '__main__':
    main()
