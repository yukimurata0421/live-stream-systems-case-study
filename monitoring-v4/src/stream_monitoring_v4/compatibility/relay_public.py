"""Print only the arena-sanitized public relay artifact for the Pi publisher.

Since 2026-10-20 it adds `fallback`: whether the direct YouTube fallback Deployment is running. If that
cannot be read, the fields are null and the relay artifact still prints. While the fallback runs, its upload
rate comes from the fallback FFmpeg's :443 bytes_acked, compared with the previous call (moved here on
2026-10-21 so the encoder host's controller needs no exec permission).
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import build_opener, ProxyHandler


NAMESPACE = 'stream-v3'
FALLBACK = 'stream-v3-youtube-backup'
STATE = Path(os.environ.get('RELAY_PUBLIC_STATE_DIR', str(Path.home() / '.cache' / 'stream-v4-relay-public'))) / 'fallback-sample.json'


def kubectl(*args):
    r = subprocess.run(['kubectl', '-n', NAMESPACE, *args], capture_output=True, text=True, timeout=8)
    if r.returncode:
        raise ValueError('KUBECTL_FAILED')
    return r.stdout


def upload_rate(previous, current):
    """Mbps from two samples of the same Pod; None after a Pod change, a counter reset, or an odd interval."""
    if not previous or not current or previous.get('pod') != current.get('pod'):
        return None
    seconds = current['at'] - previous['at']
    delta = current['bytes'] - previous['bytes']
    if not 5 <= seconds <= 600 or delta < 0:
        return None
    return round(delta * 8 / seconds / 1e6, 3)


def fallback_sample():
    """Bytes acknowledged on the fallback FFmpeg's :443 connection, or None."""
    try:
        pod = kubectl('get', 'pod', '-l', 'app.kubernetes.io/name=' + FALLBACK, '--field-selector=status.phase=Running',
                      '-o', 'jsonpath={.items[0].metadata.name}').strip()
        if not pod:
            return None
        out = kubectl('exec', pod, '-c', 'stream-engine', '--', 'ss', '-tni', 'state', 'established', '( dport = :443 )')
        acked = [int(x) for x in re.findall(r'bytes_acked:(\d+)', out)]
        return {'pod': pod, 'bytes': max(acked), 'at': time.time()} if acked else None
    except Exception:
        return None


def fallback_rate():
    current = fallback_sample()
    try:
        previous = json.loads(STATE.read_text())
    except Exception:
        previous = None
    if current:
        try:
            STATE.parent.mkdir(parents=True, exist_ok=True)
            tmp = STATE.with_suffix('.tmp'); tmp.write_text(json.dumps(current)); os.replace(tmp, STATE)
        except OSError:
            pass
    return upload_rate(previous, current)


def fallback():
    observed = datetime.now(timezone.utc).isoformat()
    try:
        r = subprocess.run(['kubectl', '-n', 'stream-v3', 'get', 'deploy', 'stream-v3-youtube-backup', '-o', 'json'],
                           capture_output=True, text=True, timeout=5)
        if r.returncode:
            raise ValueError()
        d = json.loads(r.stdout)
        desired, ready = int(d['spec'].get('replicas') or 0), int(d.get('status', {}).get('readyReplicas') or 0)
        out = {'active': desired >= 1, 'ready': ready >= 1, 'observed_at': observed}
        if desired >= 1:
            mbps = fallback_rate()
            out['mbps'] = mbps if mbps is not None and 0 <= mbps <= 10000 else None
        else:
            STATE.unlink(missing_ok=True)
        return out
    except Exception:
        return {'active': None, 'ready': None, 'observed_at': observed}


if __name__ == '__main__':
    try:
        with build_opener(ProxyHandler({})).open('http://127.0.0.1:9116/public', timeout=3) as r:
            raw = r.read(16385)
        if len(raw) > 16384:
            raise ValueError()
        value = json.loads(raw)
        value['fallback'] = fallback()
        sys.stdout.write(json.dumps(value, allow_nan=False))
    except Exception:
        raise SystemExit(2)
