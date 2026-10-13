"""Print only the arena-sanitized public relay artifact for the Pi publisher."""
import sys
from urllib.request import build_opener,ProxyHandler

if __name__=='__main__':
    try:
        with build_opener(ProxyHandler({})).open('http://127.0.0.1:9116/public',timeout=3) as r:raw=r.read(16385)
        if len(raw)>16384:raise ValueError()
        sys.stdout.buffer.write(raw)
    except Exception:raise SystemExit(2)
