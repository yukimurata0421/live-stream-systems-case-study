#!/usr/bin/env python3
"""Project only allowlisted raw facts from the existing local relay collector into observer RAM."""
import json
import math
import os
from pathlib import Path
import grp
import tempfile
import time

SOURCE=Path('/var/lib/stream-v4-relay-observer/previous.json')
CONFIG=Path('/etc/stream-v4-relay-observer/config.json')
OUTPUT=Path('/run/stream-v3-oci-monitor/relay-facts.json')


def project(raw,now,expected_machine):
    if (raw.get('schema')!='stream.relay_raw.v1' or raw.get('host_id')!='relay-host'
            or not expected_machine or raw.get('machine_id_sha256')!=expected_machine
            or not isinstance(raw.get('boot_id'),str) or not raw['boot_id']
            or not isinstance(raw.get('monotonic_seconds'),(float,int)) or not math.isfinite(raw['monotonic_seconds'])):
        raise ValueError('relay_source_contract_invalid')
    def counter(value):return value if type(value) is int and value>=0 else None
    input=raw.get('input') or {};providers=raw.get('destinations') or {};relay=raw.get('relay') or {}
    return {'schema':'stream_v3.relay_native_facts.v1','projected_at_epoch':now,'observed_at_utc':raw.get('observed_at'),
            'source_host_id':raw['host_id'],'source_boot_id':raw.get('boot_id'),'source_monotonic_seconds':raw.get('monotonic_seconds'),
            'relay_active':relay.get('active') if isinstance(relay.get('active'),bool) else None,
            'input':{'available':input.get('available'),'ready':input.get('ready'),'bytes':counter(input.get('bytes')),'connection_id':input.get('connection_id')},
            'destinations':{name:{'id':(providers.get(name) or {}).get('id'),
                                  'state':(providers.get(name) or {}).get('state') if (providers.get(name) or {}).get('state') in ('forwarding','connecting','error','idle') else 'unknown',
                                  'bytes':counter((providers.get(name) or {}).get('bytes'))} for name in ('youtube','twitch')},
            'maintenance_state':(raw.get('daily') or {}).get('state') if (raw.get('daily') or {}).get('state') in ('TRACKING','WAIT_OFFLINE','VERIFY','IDLE') else 'UNKNOWN'}


def main():
    try:
        if SOURCE.stat().st_size>262144:raise ValueError('relay_facts_too_large')
        value=project(json.loads(SOURCE.read_text()),time.time(),json.loads(CONFIG.read_text())['machine_id_sha256'])
    except (OSError,ValueError,TypeError,KeyError):value={'schema':'stream_v3.relay_native_facts.v1','available':False,'reason':'relay_native_observation_unavailable'}
    # A root pre-step never follows a user-owned symlink and never changes the source.
    fd,temporary=tempfile.mkstemp(prefix='.relay-facts-',dir=OUTPUT.parent)
    try:
        os.fchmod(fd,0o640);os.fchown(fd,0,grp.getgrnam('stream-monitor').gr_gid)
        with os.fdopen(fd,'w') as out:json.dump(value,out,separators=(',',':'));out.write('\n')
        os.replace(temporary,OUTPUT)
    finally:Path(temporary).unlink(missing_ok=True)


if __name__=='__main__':main()
