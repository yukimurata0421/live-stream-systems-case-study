"""Arena clock observations; fixed peers, no policy, authorization or effects."""
from __future__ import annotations
import argparse
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import threading
import time

spec=importlib.util.spec_from_file_location('stream_clock_contract',Path(__file__).with_name('clock_contract.py'))
clock=importlib.util.module_from_spec(spec);sys.modules[spec.name]=clock;spec.loader.exec_module(clock)


def load(path):
    with Path(path).open('rb') as handle:raw=handle.read(16385)
    if len(raw)>16384:raise ValueError('oversized_clock_facts')
    return json.loads(raw)


def probe(peer):
    nonce=secrets.token_hex(16);sent=clock.capture()
    r=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=3',
                      peer['ssh_alias'],'python3','-I','-B','/opt/stream-clock/current/clock_observer.py',
                      '--host-id',peer['host_id'],'--exchange',nonce],capture_output=True,timeout=8)
    received=clock.capture()
    if r.returncode or len(r.stdout)>16384:raise ValueError('clock_probe_unavailable')
    packet=json.loads(r.stdout);value=packet['value']
    model=clock.estimate(packet['clock'],nonce,sent,received,machine=peer['machine_id_sha256'],boot_id=value['boot_id'])
    return value,model


def peer_view(value,model,now,*,cached=False):
    if (value['schema']!='stream.clock_status.v1' or value['boot_id']!=model.boot_id
            or value['machine_id_sha256']!=model.machine or type(value['synchronized']) is not bool
            or not model.fresh(value['wall'],90,now=now)):
        raise ValueError('clock_status_expired')
    return {'status':'CACHED' if cached else 'OBSERVED','host_id':value['host_id'],
            'synchronized':value['synchronized'],'source_wall':value['wall'],
            'source_age_upper_seconds':model.age_bounds(value['wall'],now=now)[1],
            'boot_id':model.boot_id,'wall_steps_total':value.get('wall_steps_total'),
            'maximum_error_seconds':value.get('maximum_error_seconds'),
            'ntp':value.get('ntp',{}),
            'estimate':{key:getattr(model,key) for key in ('low','high','received_wall','received_mono',
                'boot_id','machine','local_boot','network_rtt')},
            **model.diagnostic(now=now)}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    config=load(args.config);stopped=threading.Event();cache={}
    allowed={'encoder-host':'encoder-host','public-host':'public-host','recovery-host':'recovery-host'}
    if (len(config['peers'])!=3 or {p['host_id']:p['ssh_alias'] for p in config['peers']}!=allowed):
        raise SystemExit('fixed_clock_peers_required')
    signal.signal(signal.SIGTERM,lambda *_:stopped.set());signal.signal(signal.SIGINT,lambda *_:stopped.set())
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        while not stopped.is_set():
            started=clock.capture();peers={}
            jobs={pool.submit(probe,p):p for p in config['peers']}
            for future,peer in jobs.items():
                key=peer['host_id'];cached=False
                try:
                    value,model=future.result();cache[key]=(value,model)
                except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError):
                    cached=True
                try:
                    value,model=cache[key]
                    peers[key]=peer_view(value,model,clock.capture(),cached=cached)
                except (KeyError,ValueError,TypeError):peers[key]={'status':'UNKNOWN'}
            try:
                value=load('/run/stream-clock-relay/oci.json')
                model=clock.Estimate(value['low'],value['high'],value['received_wall'],value['received_mono'],
                                     value['boot_id'],value['machine'],value['local_boot'],value['network_rtt_seconds'])
                if model.local_boot!=clock.identity()['boot_id']:raise ValueError('clock_boot_changed')
                peers['relay-host']=peer_view(value['clock_status'],model,clock.capture(),
                                               cached=clock.mono()-model.received_mono>75)
            except (OSError,ValueError,KeyError,TypeError):peers['relay-host']={'status':'UNKNOWN'}
            own_synchronized = None
            try:
                own=load('/run/stream-clock/status.json')
                if own['boot_id']!=clock.identity()['boot_id'] or not 0<=clock.mono()-own['mono']<=45:
                    raise ValueError('local_clock_stale')
                own_synchronized = own['synchronized']
                peers['arena-monitoring-facts']={'status':'OBSERVED','synchronized':own['synchronized'],
                    'offset_seconds':0.,'uncertainty_seconds':0.,'source_age_upper_seconds':clock.mono()-own['mono'],
                    'maximum_error_seconds':own.get('maximum_error_seconds'),'ntp':own.get('ntp',{})}
            except (OSError,ValueError,KeyError):peers['arena-monitoring-facts']={'status':'UNKNOWN'}
            out={'schema':'stream.clock_mesh.v1','wall':time.time(),'mono':clock.mono(),
                 'boot_id':clock.identity()['boot_id'],'clock_synchronized':own_synchronized,'peers':peers}
            tmp=args.output.with_suffix('.tmp');tmp.write_text(json.dumps(out,allow_nan=False));tmp.chmod(0o644);os.replace(tmp,args.output)
            stopped.wait(max(1,60-(clock.mono()-started['mono'])))


if __name__=='__main__':main()
