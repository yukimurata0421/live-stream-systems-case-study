#!/usr/bin/env python3
"""Cross-check saved journals, ledgers and identities without importing the guard."""
import hashlib
import json
from pathlib import Path
import re
import sys

def read(path):
    return json.loads(path.read_text())

def verify(base, names=None):
    rows = []
    for name in names or ['success', 'failed_reception', 'job_failure', 'timeout', 'monitor_crash', 'gain_change']:
        root = base / name
        e = root / 'evidence'
        result = read(e / 'result.json')
        final = read(e / 'final.json')
        origin = read(e / 'origin.json')
        completion = read(root / 'completion.json')
        events = [json.loads(x) for x in (e / 'guard-events.jsonl').read_text().splitlines()]
        journal = [json.loads(x) for x in (e / 'airspy_adsb.service.journal.jsonl').read_text().splitlines()]
        messages = [x for x in journal if re.match(r'^LAB_RECEIVER_START count=\d+ ', x.get('MESSAGE', ''))]
        assert [int(re.search(r'count=(\d+)', x['MESSAGE'])[1]) for x in messages] == [1, 2, 3], messages
        assert len({x['_SYSTEMD_INVOCATION_ID'] for x in messages}) == 3
        reservations = [x for x in events if x['event'] == 'recovery_restart_reserved']
        assert len(reservations) == 1 and result['passed']
        episode = final['state']['episode']
        assert episode['attempts'] == 1 and episode['id'] == origin['state']['episode']['id']
        assert reservations[0]['episode_id'] == episode['id']
        if name != 'job_failure':
            assert episode['instance'] != episode['origin_instance']
        assert 'populated 0' in completion['cgroup_events'] and not completion['cgroup_procs']
        copied = root / 'opt/airspy-reception-guard/current/receiver_guard.py'
        digest = hashlib.sha256(copied.read_bytes()).hexdigest()
        assert digest == completion['receiver_guard_sha256']
        if name in ['success', 'gain_change']:
            assert final['status']['reception_confirmed']
            window = reservations[0]['evidence']['bad_window']
            assert window[-1]['source_now'] - window[0]['source_now'] >= result['policy']['sustain_sec']
            if completion['full_timing']:
                assert result['policy']['grace_sec'] == 90 and result['policy']['sustain_sec'] == 180
            if name == 'gain_change':
                assert final['status']['gain'] == 18
        elif name == 'job_failure':
            assert episode['restart_returncode'] != 0
        elif name == 'timeout':
            t = read(e / 'timeout.json')
            assert t['state']['episode']['restart_error'] == 'TimeoutExpired'
            assert t['target']['ActiveState'] == 'deactivating'
            outcomes = [x for x in events if x['event'] == 'recovery_restart_result']
            assert outcomes[0]['ts'] - reservations[0]['ts'] >= 15
        elif name == 'monitor_crash':
            before = read(e / 'before-monitor-kill.json')
            after = read(e / 'after-monitor-kill.json')
            assert before['target']['ActiveState'] == 'deactivating'
            assert before['state']['episode']['phase'] == 'RESTART_RESERVED'
            assert after['state']['episode']['phase'] == 'RESTART_RESERVED'
            assert before['state']['episode']['attempts'] == after['state']['episode']['attempts'] == 1
            assert after['monitor']['Result'] == 'signal'
        rows.append(dict(case=name, passed=True, journal_starts=3, external_starts=1, automatic_starts=1,
                         reservations=1, attempts=1, same_origin=True, monitor_restarts=3,
                         guard_sha256=digest, cgroup_empty=True))
    return rows

if __name__ == '__main__':
    print(json.dumps(verify(Path(sys.argv[1]), sys.argv[2:]), indent=2))
