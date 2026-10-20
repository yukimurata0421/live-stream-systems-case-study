"""Print only the sanitized public relay artifact for the Pi publisher.

Since 2026-10-20 it adds `fallback`: whether the direct YouTube fallback Deployment is running and, while
it runs, its upload rate. If that cannot be read, the fields are null and the relay artifact still prints.
"""
import json
import subprocess
import sys
from datetime import datetime, timezone
from urllib.request import build_opener, ProxyHandler


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
        # The encoder host's controller writes the fallback upload rate to this annotation while it runs.
        note = (d['metadata'].get('annotations') or {}).get('stream-v3.io/fallback-upload')
        if desired >= 1 and note:
            try:
                n = json.loads(note)
                age = datetime.now(timezone.utc) - datetime.fromisoformat(n['observed_at'])
                mbps = n.get('mbps')
                ok = isinstance(mbps, (int, float)) and 0 <= mbps <= 10000 and age.total_seconds() <= 120
                out['mbps'] = round(float(mbps), 3) if ok else None
            except Exception:
                out['mbps'] = None
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
