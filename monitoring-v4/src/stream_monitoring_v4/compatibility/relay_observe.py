"""Fixed read-only probe for the self-healing relay. Emits counters and resources, never URLs or errors.

The monitoring host runs it through a forced SSH command. The output schema (stream.relay_raw.v1) is
unchanged from the signed-lease relay; only the sources moved to the MediaMTX service and its keeper.
"""
from __future__ import annotations
import json
import hashlib
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import build_opener, ProxyHandler

API = 'http://127.0.0.1:9997'
UNIT = 'stream-v3-oci-mediamtx.service'
KEEPER = Path('/var/lib/private/stream-v3-oci-keeper/keeper.json')
JST = timezone(timedelta(hours=9))
# Map keeper phases onto the daily-state vocabulary the public projection already knows.
KEEPER_STATES = {'IDLE': 'TRACKING', 'WAIT_OFFLINE': 'WAIT_OFFLINE', 'VERIFY': 'VERIFYING'}


def api(path):
    with build_opener(ProxyHandler({})).open(API+path, timeout=2) as r:
        raw = r.read(65537)
    if len(raw) > 65536:
        raise ValueError('BOUNDED_RESPONSE_REQUIRED')
    return json.loads(raw)


def counters():
    values = [int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
    return sum(values), values[3]+values[4]


def provider(item):
    host = urlsplit(str((item.get('conf') or {}).get('dest') or '')).hostname or ''
    if host == 'youtube.com' or host.endswith('.youtube.com'):
        return 'youtube'
    if host.endswith('.live-video.net') or host.endswith('.twitch.tv'):
        return 'twitch'
    return None


def renewal_time(now, phase):
    """Today's 00:00 JST while a renewal is in progress, otherwise the next 00:00 JST.
    The monitor treats OFFLINE as planned only within 360 seconds of this time and only during a renewal,
    so it must not move to the next day mid-renewal."""
    local = now.astimezone(JST)
    today = local.replace(hour=0, minute=0, second=0, microsecond=0)
    due = today if phase in ('WAIT_OFFLINE', 'VERIFY') else today + timedelta(days=1)
    return due.astimezone(timezone.utc).isoformat()


def observe():
    a, ai = counters(); time.sleep(.2); b, bi = counters()
    mem = {k: int(v.split()[0])*1024 for k, v in (x.split(':', 1) for x in Path('/proc/meminfo').read_text().splitlines())}
    disk = os.statvfs('/')
    props = dict(x.split('=', 1) for x in subprocess.check_output(
        ['systemctl', 'show', UNIT, '-p', 'ActiveState', '-p', 'MainPID', '-p', 'NRestarts'], text=True, timeout=3).splitlines())
    now = datetime.now(timezone.utc)
    result = {'schema': 'stream.relay_raw.v1', 'host_id': 'relay-host',
              'machine_id_sha256': hashlib.sha256(Path('/etc/machine-id').read_bytes()).hexdigest(),
              'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
              'observed_at': now.isoformat(), 'monotonic_seconds': time.clock_gettime(time.CLOCK_BOOTTIME),
              'vm': {'cpu_percent': round(100*(1-(bi-ai)/(b-a)), 2) if b > a else None, 'cpu_count': os.cpu_count(),
                     'memory_total_bytes': mem['MemTotal'], 'memory_used_bytes': mem['MemTotal']-mem['MemAvailable'],
                     'disk_total_bytes': disk.f_blocks*disk.f_frsize, 'disk_available_bytes': disk.f_bavail*disk.f_frsize,
                     'uptime_seconds': float(Path('/proc/uptime').read_text().split()[0])},
              'relay': {'active': props['ActiveState'] == 'active', 'pid': int(props['MainPID']), 'restart_count': int(props['NRestarts'])},
              'input': {'available': False, 'ready': None, 'bytes': None, 'connection_id': None}, 'destinations': {},
              'daily': {'state': 'UNKNOWN', 'observed_at': None, 'next_renewal_at': None, 'offline_wait_seconds': None}}
    try:
        path = api('/v3/paths/get/relay')
        inbound = path.get('inboundBytes', path.get('bytesReceived'))
        result['input'] = {'available': True, 'ready': path['ready'] is True, 'bytes': inbound,
                           'connection_id': path['source']['id'] if path.get('source') else None}
        rows = api('/v3/paths/forward-dests/list?path=relay')['items']
        for name in ('youtube', 'twitch'):
            # Match by destination host, not position: positions shift when the keeper removes a forward.
            # The destination itself is never emitted.
            matches = [x for x in rows if provider(x) == name]
            if len(matches) != 1:
                continue
            x = matches[0]
            result['destinations'][name] = {'id': x['id'], 'state': x['state'] if x['state'] in ['forwarding', 'idle', 'connecting'] else 'unknown',
                                            'bytes': x['outboundBytes']}
    except Exception:
        pass
    try:
        if KEEPER.stat().st_size > 16384:
            raise ValueError()
        k = json.loads(KEEPER.read_text())
        result['daily'] = {'state': KEEPER_STATES.get(k.get('phase'), 'UNKNOWN'),
                           'observed_at': datetime.fromtimestamp(KEEPER.stat().st_mtime, timezone.utc).isoformat(),
                           'next_renewal_at': renewal_time(now, k.get('phase')), 'offline_wait_seconds': 120}
    except Exception:
        pass
    return result


if __name__ == '__main__':
    try:
        print(json.dumps(observe(), allow_nan=False, separators=(',', ':')))
    except Exception:
        print('{"schema":"stream.relay_raw.v1","error":"OBSERVATION_UNAVAILABLE"}')
