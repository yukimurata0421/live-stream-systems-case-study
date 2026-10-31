#!/usr/bin/env python3
"""Read-only kernel/NTP clock facts. No clock setting or recovery authority."""
from __future__ import annotations
import argparse
import ctypes
import importlib.util
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import threading

spec = importlib.util.spec_from_file_location('stream_clock_contract', Path(__file__).with_name('clock_contract.py'))
clock = importlib.util.module_from_spec(spec); sys.modules[spec.name] = clock; spec.loader.exec_module(clock)


class Timeval(ctypes.Structure):
    _fields_ = [('tv_sec', ctypes.c_long), ('tv_usec', ctypes.c_long)]


class Timex(ctypes.Structure):
    _fields_ = [('modes', ctypes.c_uint), ('offset', ctypes.c_long), ('freq', ctypes.c_long),
                ('maxerror', ctypes.c_long), ('esterror', ctypes.c_long), ('status', ctypes.c_int),
                ('constant', ctypes.c_long), ('precision', ctypes.c_long), ('tolerance', ctypes.c_long),
                ('time', Timeval), ('tick', ctypes.c_long), ('ppsfreq', ctypes.c_long),
                ('jitter', ctypes.c_long), ('shift', ctypes.c_int), ('stabil', ctypes.c_long),
                ('jitcnt', ctypes.c_long), ('calcnt', ctypes.c_long), ('errcnt', ctypes.c_long),
                ('stbcnt', ctypes.c_long), ('tai', ctypes.c_int), ('padding', ctypes.c_int * 11)]


def status(host):
    started = clock.capture()
    out = {'schema': 'stream.clock_status.v1', 'host_id': host, **clock.identity(), **started,
           'synchronized': False, 'ntp': {}, 'status': 'UNKNOWN'}
    # modes=0 is a read operation, requiring no CAP_SYS_TIME. Linux 64-bit ABI.
    if ctypes.sizeof(ctypes.c_long) != 8 or ctypes.sizeof(Timex) != 208:
        out['reason'] = 'kernel_clock_abi_unknown'
        return out
    value = Timex()
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.adjtimex(ctypes.byref(value))
    if result < 0:
        out['reason'] = 'kernel_clock_unavailable'
        return out
    out.update(status='OBSERVED', synchronized=(result != 5 and not value.status & 0x40),
               kernel_state=result, kernel_status=value.status,
               estimated_error_seconds=value.esterror / 1e6,
               maximum_error_seconds=value.maxerror / 1e6,
               frequency_ppm=value.freq / 65536.)
    try:
        r = subprocess.run(['chronyc', '-c', 'tracking'], capture_output=True, text=True, timeout=2,
                           env={'PATH': '/usr/sbin:/usr/bin:/sbin:/bin', 'LC_ALL': 'C'})
        parts = r.stdout.strip().split(',')
        if r.returncode == 0 and len(parts) == 14:
            out['ntp'] = {k: float(parts[i]) for k, i in {
                'reference_unixtime': 3, 'system_time_seconds': 4, 'last_offset_seconds': 5,
                'rms_offset_seconds': 6, 'root_delay_seconds': 10,
                'root_dispersion_seconds': 11, 'update_interval_seconds': 12}.items()}
            out['ntp']['synchronized'] = parts[13] == 'Normal'
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    if not out['ntp']:
        try:
            r = subprocess.run(['timedatectl','timesync-status','--no-pager'],capture_output=True,text=True,timeout=2,
                               env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LC_ALL':'C'})
            units={'s':1.,'ms':.001,'us':.000001,'µs':.000001,'ns':.000000001}
            names={'Offset':'offset_seconds','Delay':'delay_seconds','Jitter':'jitter_seconds','Root distance':'root_distance_seconds'}
            for line in r.stdout.splitlines():
                m=re.match(r'\s*(Offset|Delay|Jitter|Root distance):\s*([-+]?\d+(?:\.\d+)?)\s*(ms|us|µs|ns|s)',line)
                if m:out['ntp'][names[m[1]]]=float(m[2])*units[m[3]]
        except (OSError,ValueError,subprocess.TimeoutExpired):pass
    # Missing reference time is left unknown; file mtime is not an NTP sync.
    reference=out['ntp'].get('reference_unixtime')
    out['last_sync_age_seconds']=max(0,started['wall']-reference) if reference is not None else None
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host-id', required=True)
    parser.add_argument('--state', type=Path)
    parser.add_argument('--exchange')
    parser.add_argument('--fallback-exchange')
    args = parser.parse_args()
    started = clock.capture()
    nonce = args.exchange or args.fallback_exchange
    if nonce:
        if not clock.valid_nonce(nonce): raise SystemExit('invalid_nonce')
        if args.fallback_exchange:
            raw = Path('/run/stream-v3-fallback-controller/status.json').read_bytes()
            if len(raw) > 8192: raise SystemExit('oversized_fallback_status')
            value = json.loads(raw)
        else:
            value = status(args.host_id)
        print(json.dumps({'value': value, 'clock': clock.envelope(nonce, started)}, allow_nan=False))
        return
    if args.state is None: raise SystemExit('state_required')
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    previous = None
    steps = 0
    while not stopped.is_set():
        value = status(args.host_id)
        step = None if previous is None else (value['wall'] - previous['wall']) - (value['mono'] - previous['mono'])
        if step is not None and abs(step) > clock.STEP_LIMIT: steps += 1
        value.update(wall_step_seconds=step, wall_steps_total=steps, sample_interval_seconds=15)
        tmp = args.state.with_suffix('.tmp')
        tmp.write_text(json.dumps(value, allow_nan=False)); tmp.chmod(0o644); os.replace(tmp, args.state)
        previous = value
        stopped.wait(15)


if __name__ == '__main__': main()
