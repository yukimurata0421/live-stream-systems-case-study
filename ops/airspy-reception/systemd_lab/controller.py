#!/usr/bin/env python3
"""Exercise real systemd and LiveBackend only inside the owned namespaces."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

E = Path('/evidence')
S = Path('/var/lib/airspy-reception-guard')

def save(name, obj):
    (E / (name + '.json')).write_text(json.dumps(obj, indent=2) + '\n')

def ctl(*args, check=True, timeout=45):
    r = subprocess.run(['/usr/bin/systemctl', *args], capture_output=True, text=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f'systemctl {args}: {r.returncode}: {r.stderr}')
    return dict(returncode=r.returncode, stdout=r.stdout, stderr=r.stderr)

def show(unit):
    r = ctl('show', unit, '-pMainPID', '-pInvocationID', '-pActiveState', '-pSubState', '-pResult')
    return dict(x.split('=', 1) for x in r['stdout'].splitlines() if '=' in x)

def read(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}

def wait_for(fn, seconds, label):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        result = fn()
        if result:
            return result
        time.sleep(.2)
    raise TimeoutError(label)

def starts():
    try:
        return int(Path('/var/lib/lab/starts').read_text())
    except OSError:
        return 0

def state():
    return read(S / 'state.json')

def episode():
    return state().get('episode') or {}

def status():
    return read(S / 'status.json')

def readsb():
    Path('/run/readsb').mkdir(exist_ok=True)
    beginning = time.time()
    count = 0
    while True:
        healthy = Path('/run/receiver-healthy').exists() and Path('/run/receiver-healthy').read_text() == '1'
        count += 10 if healthy else 0
        now = time.time()
        for name, obj in [('stats', dict(now=now, total=dict(start=beginning, position_count_total=count))),
                          ('aircraft', dict(now=now, aircraft=[dict(lat=35, lon=139, seen_pos=1)] * 3 if healthy else []))]:
            p = Path('/run/readsb') / (name + '.json')
            tmp = p.with_suffix('.tmp')
            tmp.write_text(json.dumps(obj))
            tmp.replace(p)
        time.sleep(.2)

def run():
    host = read('/control/host.json')
    isolation = dict(pid1=Path('/proc/1/comm').read_text().strip(),
                     namespaces={n: os.readlink('/proc/1/ns/' + n) for n in host['namespaces']},
                     cgroup=Path('/proc/1/cgroup').read_text(),
                     machine_id=Path('/etc/machine-id').read_text().strip(),
                     host_bus=Path('/run/host/systemd/private').exists(),
                     usb=Path('/dev/bus/usb').exists(), home=bool(list(Path('/home').iterdir())),
                     net_devices=subprocess.check_output(['ip', '-j', 'link'], text=True))
    save('isolation', isolation)
    assert isolation['pid1'] == 'systemd'
    assert all(isolation['namespaces'][n] != v for n, v in host['namespaces'].items())
    assert isolation['cgroup'].strip() == '0::/init.scope'
    assert not any(isolation[k] for k in ['host_bus', 'usb', 'home'])
    assert [x['ifname'] for x in json.loads(isolation['net_devices'])] == ['lo']
    mode = Path('/control/mode').read_text().strip()
    if mode == 'probe':
        ctl('start', 'readsb.service', 'airspy_adsb.service')
        ctl('start', 'lab-diagnostic.service')
        save('diagnostic', read(S / 'diagnostic.json'))
        assert read(S / 'diagnostic.json')['exe'] == '/usr/local/bin/airspy_adsb'
        save('result', dict(passed=True, probe=True))
        return
    ctl('start', 'readsb.service', 'airspy_adsb.service')
    readsb_before = show('readsb.service')
    r = subprocess.run(['python3', '/opt/airspy-reception-guard/current/receiver_guard.py', 'init'], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    ctl('start', 'airspy-reception-guard.timer')
    full = Path('/control/full').exists()
    baseline_sec = 190 if full else 12
    wait_for(lambda: len(state().get('history', [])) >= 3 and status().get('state') == 'HEALTHY', baseline_sec, 'pre-start baseline')
    save('baseline', dict(state=state(), status=status(), target=show('airspy_adsb.service')))
    if mode == 'gain_change':
        cfg=Path('/etc/default/airspy_adsb')
        before=cfg.read_bytes()
        cfg.write_bytes(before.replace(b'GAIN="16"', b'GAIN="18"'))
        assert cfg.read_bytes().replace(b'GAIN="18"', b'GAIN="16"') == before
        save('external-gain-change', dict(before=before.decode(), after=cfg.read_text()))
    ctl('restart', 'airspy_adsb.service')
    wait_for(lambda: starts() == 2, 10, 'origin invocation')
    origin = wait_for(lambda: episode().get('origin_instance') == show('airspy_adsb.service')['InvocationID'], 65 if full else 8, 'origin observed')
    origin_id = episode()['id']
    save('origin', dict(state=state(), target=show('airspy_adsb.service')))
    if mode == 'monitor_crash':
        wait_for(lambda: episode().get('attempts') == 1 and show('airspy_adsb.service')['ActiveState'] == 'deactivating', 30, 'reservation plus accepted stop job')
        save('before-monitor-kill', dict(state=state(), target=show('airspy_adsb.service'), monitor=show('airspy-reception-guard.service')))
        ctl('stop', 'airspy-reception-guard.timer')
        ctl('kill', '--signal=SIGKILL', '--kill-whom=all', 'airspy-reception-guard.service')
        wait_for(lambda: show('airspy-reception-guard.service')['ActiveState'] in ['failed', 'inactive'], 5, 'monitor killed')
        save('after-monitor-kill', dict(state=state(), monitor=show('airspy-reception-guard.service')))
        ctl('reset-failed', 'airspy-reception-guard.service')
        ctl('start', 'airspy-reception-guard.timer')
    wait_for(lambda: episode().get('attempts') == 1, 420 if full else 35, 'recovery reservation')
    if mode == 'timeout':
        wait_for(lambda: episode().get('restart_error') == 'TimeoutExpired', 22, 'real 15-second systemctl timeout')
        save('timeout', dict(state=state(), status=status(), target=show('airspy_adsb.service')))
    wait_for(lambda: starts() == 3, 40, 'one recovery start')
    if mode in ['success', 'gain_change']:
        wait_for(lambda: status().get('reception_confirmed') and status().get('state') == 'HEALTHY', 140 if full else 12, 'recovered reception')
    elif mode == 'job_failure':
        wait_for(lambda: episode().get('restart_returncode') not in [None, 0], 12, 'real failed restart job')
    else:
        wait_for(lambda: status().get('state') == 'EXHAUSTED', 20, 'failed reception exhausted')
    save('after-recovery', dict(state=state(), status=status(), target=show('airspy_adsb.service')))
    # Start real oneshot monitor processes repeatedly against the same durable state.
    ctl('stop', 'airspy-reception-guard.timer')
    for _ in range(3):
        ctl('restart', 'airspy-reception-guard.service')
        time.sleep(1.1)
    ctl('start', 'airspy-reception-guard.timer')
    time.sleep(8)
    ctl('stop', 'airspy-reception-guard.timer')
    wait_for(lambda: show('airspy-reception-guard.service')['ActiveState'] != 'activating', 30, 'monitor quiescent')
    final = dict(state=state(), status=status(), target=show('airspy_adsb.service'), starts=starts(), readsb=show('readsb.service'))
    save('final', final)
    rows = [json.loads(x) for x in (S / 'events.jsonl').read_text().splitlines()]
    reservations = [x for x in rows if x['event'] == 'recovery_restart_reserved']
    assert starts() == 3, final
    assert episode()['id'] == origin_id and episode()['attempts'] == 1
    assert len(reservations) == 1, reservations
    if mode == 'gain_change':
        assert final['status']['gain'] == 18
        assert Path('/etc/default/airspy_adsb').read_text() == 'GAIN="18"\nBIAS="-b"\n'
    assert final['readsb']['InvocationID'] == readsb_before['InvocationID']
    assert final['readsb']['MainPID'] == readsb_before['MainPID']
    save('result', dict(passed=True, mode=mode, initial_starts=1, external_starts=1, automatic_starts=1,
                        reservations=1, attempts=1, same_origin=True, readsb_unchanged=True,
                        monitor_manual_restarts=3, policy=status()['policy']))

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'diagnostic':
        target=show('airspy_adsb.service')
        pid=target['MainPID']
        try:
            exe=os.readlink('/proc/'+pid+'/exe')
        except OSError:
            exe=traceback.format_exc()
        (S/'diagnostic.json').write_text(json.dumps(dict(target=target,exe=exe,proc_status=Path('/proc/self/status').read_text(),apparmor=Path('/proc/self/attr/current').read_text())))
    elif len(sys.argv) > 1 and sys.argv[1] == 'readsb':
        readsb()
    else:
        try:
            run()
        except BaseException:
            save('result', dict(passed=False, error=traceback.format_exc()))
        finally:
            for unit in ['airspy_adsb.service', 'airspy-reception-guard.service']:
                r = subprocess.run(['journalctl', '--no-pager', '-o', 'json', '-u', unit], capture_output=True, text=True)
                (E / (unit + '.journal.jsonl')).write_text(r.stdout)
            for p in S.glob('*'):
                if p.is_file():
                    (E / ('guard-' + p.name)).write_bytes(p.read_bytes())
            ctl('--no-block', 'poweroff', check=False)
