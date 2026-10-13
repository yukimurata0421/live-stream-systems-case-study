"""Read-only bridge: public SLI JSON only; no access to private history."""
import json
import sys
from urllib.request import build_opener, ProxyHandler

def main():
    with build_opener(ProxyHandler({})).open('http://127.0.0.1:9117/public',timeout=3) as r:
        raw=r.read(24577)
    if len(raw)>24576: raise ValueError('PUBLIC_SIZE')
    value=json.loads(raw)
    if value['schema']!='stream.twitch_sli_public.v1': raise ValueError('PUBLIC_SCHEMA')
    json.dump(value,sys.stdout,allow_nan=False);sys.stdout.write('\n')

if __name__=='__main__': main()
