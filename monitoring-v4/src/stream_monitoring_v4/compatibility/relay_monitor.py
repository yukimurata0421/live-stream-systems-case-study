"""Arena-owned relay monitoring, bounded SSH observation, public GET and metrics."""
from __future__ import annotations
import argparse
import importlib.util
import secrets
import sys
import json
import math
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import build_opener, ProxyHandler

SCHEMA='stream.relay_public.v1'
LATEST={};LOCK=threading.Lock()
TWITCH=Path('/var/lib/stream-v4-twitch-api/twitch_api_evidence.json')
PHASES={'TRACKING','WAITING_CREDENTIALS','WAIT_STOP','WAIT_OFFLINE','WAIT_START','VERIFYING','CANCELLED','RENEWED','HOLD','UNKNOWN'}
DELIVERY_STATES = ('HEALTHY', 'API_UNAVAILABLE', 'PROVIDER_OFFLINE', 'OCI_UNAVAILABLE',
                   'OCI_RELAY_STOPPED', 'OCI_INPUT_STOPPED', 'TWITCH_OUTPUT_STOPPED',
                   'PATH_UNCONFIRMED', 'PLANNED_RENEWAL')
INTERRUPTED_STATES = {'OCI_RELAY_STOPPED', 'OCI_INPUT_STOPPED', 'TWITCH_OUTPUT_STOPPED'}


def utc():return datetime.now(timezone.utc).isoformat()
def stamp(v):
    if not isinstance(v,str) or not 1<=len(v)<=64:raise ValueError('timestamp required')
    parsed=datetime.fromisoformat(v.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('timezone required')
    value=parsed.timestamp()
    if not math.isfinite(value):raise ValueError('finite timestamp required')
    return value
def number(v,lower=0,upper=1e18):return type(v) in (int,float) and math.isfinite(v) and lower<=v<=upper


def twitch_delivery(out, now):
    """Combine the mandatory OCI path and provider facts without inventing reception.

    Missing counters are unknown. Only explicit stopped/stalled observations can
    establish interruption. Daily renewal suppresses only the Twitch output leg.
    """
    def fresh(value):
        try: return 0 <= now-stamp(value) <= 180
        except (AttributeError, TypeError, ValueError, OverflowError): return False
    oci = out.get('source_fresh') is True and fresh(out.get('observed_at'))
    api = out.get('twitch_api', {})
    api_ok = api.get('source_fresh') is True and fresh(api.get('observed_at')) and api.get('state') in ('LIVE', 'OFFLINE')
    aligned = oci and api_ok and abs(stamp(out['observed_at'])-stamp(api['observed_at'])) <= 90
    relay = out.get('relay', {}).get('status')
    incoming = out.get('input', {}).get('status')
    outgoing = out.get('destinations', {}).get('twitch', {}).get('status')
    daily = out.get('daily', {})
    try:
        planned = (daily.get('planned_break') is True and daily.get('state') in ('WAIT_STOP','WAIT_OFFLINE','WAIT_START','VERIFYING')
                   and 0 <= now-stamp(daily.get('next_renewal_at')) <= 360)
    except (AttributeError, TypeError, ValueError, OverflowError): planned = False
    if not oci: state = 'OCI_UNAVAILABLE'
    elif relay == 'STOPPED': state = 'OCI_RELAY_STOPPED'
    elif relay != 'RUNNING': state = 'PATH_UNCONFIRMED'
    elif incoming in ('STALLED','DISCONNECTED'): state = 'OCI_INPUT_STOPPED'
    elif incoming != 'FLOWING': state = 'PATH_UNCONFIRMED'
    elif planned: state = 'PLANNED_RENEWAL'
    elif outgoing in ('STALLED','DISCONNECTED'): state = 'TWITCH_OUTPUT_STOPPED'
    elif outgoing != 'FLOWING': state = 'PATH_UNCONFIRMED'
    elif not aligned: state = 'API_UNAVAILABLE'
    elif api['state'] == 'OFFLINE': state = 'PROVIDER_OFFLINE'
    else: state = 'HEALTHY'
    return {'state':state, 'path_interrupted':state in INTERRUPTED_STATES,
            'recovery_confirmed':state == 'HEALTHY', 'oci_observed':oci, 'api_observed':api_ok}


def empty(now):
    return {'schema':SCHEMA,'generated_at':now,'observed_at':None,'status':'UNKNOWN','source_fresh':False,
            'vm':{'status':'UNKNOWN','cpu_percent':None,'memory_percent':None,'disk_free_percent':None,'uptime_seconds':None},
            'relay':{'status':'UNKNOWN','restart_count':None},'input':{'status':'UNKNOWN','mbps':None},
            'destinations':{n:{'status':'UNKNOWN','mbps':None} for n in ['youtube','twitch']},
            'daily':{'state':'UNKNOWN','next_renewal_at':None,'planned_break':False},
            'twitch_api':{'state':'UNKNOWN','observed_at':None,'source_fresh':False},'issues':[]}


def project(raw,previous,twitch,now,machine,binding=None,clock_model=None):
    out=empty(now)
    try:
        if raw['schema']!='stream.relay_raw.v1' or raw['host_id']!='relay-host' or raw['machine_id_sha256']!=machine:raise ValueError()
        observed=stamp(raw['observed_at'])
        def age(value):
            if clock_model is None: return (now-stamp(value),)*2
            return clock_model.age_bounds(stamp(value))
        def fresh(value, limit):
            low, high = age(value)
            return high >= -.005 and high <= limit
        def normalized(value):
            if clock_model is None: return value
            return datetime.fromtimestamp(now-age(value)[1],timezone.utc).isoformat()
        if not fresh(raw['observed_at'],180) or not number(raw['monotonic_seconds']):raise ValueError()
        vm=raw['vm'];cpu=vm['cpu_percent'];total=vm['memory_total_bytes'];used=vm['memory_used_bytes'];disk=vm['disk_total_bytes'];free=vm['disk_available_bytes']
        if not number(cpu,0,100) or not number(total,1) or not number(used,0,total) or not number(disk,1) or not number(free,0,disk) or not number(vm['uptime_seconds']):raise ValueError()
        if type(raw['relay']['active']) is not bool or type(raw['input']['available']) is not bool: raise ValueError()
        out.update(observed_at=normalized(raw['observed_at']),status='OBSERVED',source_fresh=True)
        out['vm']={'status':'OBSERVED','cpu_percent':cpu,'memory_percent':round(100*used/total,2),'disk_free_percent':round(100*free/disk,2),'uptime_seconds':vm['uptime_seconds']}
        out['relay']={'status':'RUNNING' if raw['relay']['active'] is True else 'STOPPED','restart_count':raw['relay']['restart_count']}
        elapsed=None
        if previous and previous.get('boot_id')==raw['boot_id']:
            elapsed=raw['monotonic_seconds']-previous['monotonic_seconds']
            if not 10<=elapsed<=180:elapsed=None
        def rate(current,before,identity):
            key=current.get(identity)
            if elapsed is None or not before or not isinstance(key,str) or not key or key!=before.get(identity):return None
            a,b=current.get('bytes'),before.get('bytes')
            if not number(a) or not number(b) or a<b:return None
            return round((a-b)*8/elapsed/1e6,3)
        if raw['input']['available'] is True:
            if type(raw['input']['ready']) is not bool: raise ValueError()
            r=rate(raw['input'],previous.get('input') if previous else None,'connection_id')
            out['input']={'status':('FLOWING' if r and r>0 else 'STALLED' if r==0 else 'CONNECTED') if raw['input']['ready'] is True else 'DISCONNECTED','mbps':r}
        for n in ['youtube','twitch']:
            current=raw['destinations'].get(n)
            if not current:continue
            before=previous.get('destinations',{}).get(n) if previous else None
            r=rate(current,before,'id')
            out['destinations'][n]={'status':('FLOWING' if r and r>0 else 'STALLED' if r==0 else 'CONNECTED') if current['state']=='forwarding' else 'DISCONNECTED' if current['state']=='idle' else 'UNKNOWN','mbps':r}
        d=raw['daily'];daily_fresh=d.get('observed_at') and fresh(d['observed_at'],60)
        if daily_fresh and d['state'] in PHASES:
            due=d.get('next_renewal_at')
            if due is not None:stamp(due)
            # A scheduled window is bounded; it never hides a YouTube problem.
            planned=bool(due and age(due)[0]>=0 and age(due)[1]<=360 and d['state'] in {'WAIT_STOP','WAIT_OFFLINE','WAIT_START','VERIFYING'})
            out['daily']={'state':d['state'],'next_renewal_at':normalized(due) if due else None,'planned_break':planned}
        if out['relay']['status']=='STOPPED':out['issues'].append('RELAY_STOPPED')
        if out['input']['status'] in ['DISCONNECTED','STALLED']:out['issues'].append('INPUT_NOT_PROGRESSING')
        for n in ['youtube','twitch']:
            if out['destinations'][n]['status'] in ['DISCONNECTED','STALLED'] and not (n=='twitch' and out['daily']['planned_break']):out['issues'].append(n.upper()+'_NOT_PROGRESSING')
        if out['vm']['memory_percent']>=90:out['issues'].append('VM_MEMORY_HIGH')
        if out['vm']['disk_free_percent']<=10:out['issues'].append('VM_DISK_LOW')
        if out['daily']['state']=='HOLD':out['issues'].append('DAILY_RENEWAL_HELD')
    except (KeyError,ValueError,TypeError,OverflowError):out=empty(now)
    try:
        valid=(isinstance(binding,dict) and twitch['schema']=='monitoring_v4.twitch_api_evidence.v1' and twitch['source_host_id']=='arena-monitoring-facts'
               and twitch['collector_revision']==binding.get('collector_revision')
               and twitch['user_id']==binding.get('user_id') and twitch['probe_status']=='ok' and twitch['http_status']==200
               and 0<=now-stamp(twitch['collected_at'])<=180 and now<=stamp(twitch['valid_until']))
        if valid and twitch['provider_state'] in ['LIVE','OFFLINE']:
            out['twitch_api']={'state':twitch['provider_state'],'observed_at':twitch['collected_at'],'source_fresh':True}
            if twitch['provider_state']=='OFFLINE' and not out['daily']['planned_break']:out['issues'].append('TWITCH_OFFLINE')
    except (KeyError,ValueError,TypeError,OverflowError):pass
    if not out['source_fresh']:out['issues'].append('RELAY_OBSERVATION_UNAVAILABLE')
    if not out['twitch_api']['source_fresh']:out['issues'].append('TWITCH_API_UNAVAILABLE')
    out['twitch_delivery'] = twitch_delivery(out, now)
    return out


def metrics(out):
    lines=[]
    def emit(name,val):
        if val is not None:lines.append('stream_v4_'+name+' '+str(float(val)))
    emit('relay_observation_ok',out['source_fresh']);emit('relay_collected_unixtime',stamp(out['observed_at']) if out['observed_at'] else 0)
    emit('relay_service_running',out['relay']['status']=='RUNNING');emit('relay_restart_count',out['relay']['restart_count'])
    for n,k in [('cpu_percent','cpu_percent'),('memory_percent','memory_percent'),('disk_free_percent','disk_free_percent'),('uptime_seconds','uptime_seconds')]:emit('relay_vm_'+n,out['vm'][k])
    emit('relay_input_mbps',out['input']['mbps'])
    for n in ['youtube','twitch']:
        val=out['destinations'][n]
        lines.append('stream_v4_relay_destination_flowing{provider="'+n+'"} '+('1' if val['status']=='FLOWING' else '0'))
        if val['mbps'] is not None:lines.append('stream_v4_relay_destination_mbps{provider="'+n+'"} '+str(val['mbps']))
    emit('twitch_api_observation_ok',out['twitch_api']['source_fresh']);emit('twitch_api_live',out['twitch_api']['state']=='LIVE')
    emit('twitch_api_collected_unixtime',stamp(out['twitch_api']['observed_at']) if out['twitch_api']['observed_at'] else 0)
    emit('twitch_daily_planned_break',out['daily']['planned_break'])
    delivery = twitch_delivery(out, time.time())
    emit('twitch_delivery_path_interrupted', delivery['path_interrupted'])
    emit('twitch_delivery_recovery_confirmed', delivery['recovery_confirmed'])
    for state in DELIVERY_STATES:
        lines.append('stream_v4_twitch_delivery_state{state="'+state+'"} '+str(int(delivery['state'] == state)))
    try:
        import route_causality
        cause=route_causality.guard()
        lines.append('stream_v4_route_secondary_recovery_blocked '+str(int(cause['secondary_recovery_blocked'])))
        for domain in sorted(route_causality.DOMAINS):
            lines.append('stream_v4_route_assessment{assessment="'+domain+'"} '+str(int(cause['assessment']==domain)))
    except (ImportError, OSError, ValueError, KeyError, TypeError):
        lines.append('stream_v4_route_secondary_recovery_blocked 1')
    try:
        mesh=load(Path('/run/stream-clock-mesh/current.json'))
        # Monotonic TTL, bound to this boot; a stopped mesh cannot stay green.
        boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        elapsed=time.clock_gettime(time.CLOCK_BOOTTIME)-mesh['mono']
        current=mesh['boot_id']==boot and 0<=elapsed<=90
        for host in ('encoder-host','relay-host','arena-monitoring-facts','public-host','recovery-host'):
            peer=mesh['peers'].get(host,{})
            age=peer.get('source_age_upper_seconds')
            valid=(current and peer.get('status') in ('OBSERVED','CACHED')
                   and number(age) and age+elapsed<=90)
            label='{host_id="'+host+'"} '
            lines.append('stream_v4_clock_observation_ok'+label+str(int(valid)))
            if valid:
                for key in ('offset_seconds','uncertainty_seconds','maximum_error_seconds','source_age_upper_seconds','synchronized'):
                    v=peer.get(key)
                    if key=='source_age_upper_seconds' and number(v):v+=elapsed
                    if key=='uncertainty_seconds' and number(v):v+=elapsed*.001
                    if type(v) in (int,float,bool) and math.isfinite(v):lines.append('stream_v4_clock_'+key+label+str(float(v)))
    except (OSError,ValueError,KeyError,TypeError):pass
    return '\n'.join(lines)+'\n'


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        with LOCK:value=dict(LATEST)
        if not value or self.path not in ['/public','/metrics','/twitch-delivery']:self.send_error(404);return
        if time.time()-value['generated_at']>180:
            value=empty(time.time());value['issues']=['RELAY_OBSERVATION_UNAVAILABLE','TWITCH_API_UNAVAILABLE']
        value['twitch_delivery'] = twitch_delivery(value, time.time())
        if self.path == '/public': value.pop('twitch_delivery', None)
        is_json = self.path in ('/public','/twitch-delivery')
        raw=(json.dumps(value,allow_nan=False) if is_json else metrics(value)).encode()
        self.send_response(200);self.send_header('Content-Type','application/json' if is_json else 'text/plain; version=0.0.4');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def log_message(self,*args):pass


def load(path):
    if path.stat().st_size>65536:raise ValueError()
    return json.loads(path.read_text())


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True);parser.add_argument('--credentials',type=Path,required=True);parser.add_argument('--state',type=Path,required=True)
    args=parser.parse_args();config=load(args.config);previous=None;stopped=threading.Event()
    spec=importlib.util.spec_from_file_location('stream_clock_contract',Path(__file__).with_name('clock_contract.py'))
    clock=importlib.util.module_from_spec(spec);sys.modules[spec.name]=clock;spec.loader.exec_module(clock)
    signal.signal(signal.SIGTERM,lambda *_:stopped.set());signal.signal(signal.SIGINT,lambda *_:stopped.set())
    server=ThreadingHTTPServer(('127.0.0.1',9116),Handler);server.daemon_threads=True
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    while not stopped.is_set():
        started=time.monotonic();raw={};model=None;timing={'status':'UNKNOWN'}
        nonce=secrets.token_hex(16)
        argv=['/usr/bin/ssh','-F','/dev/null','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','ConnectionAttempts=1','-o','StrictHostKeyChecking=yes',
              '-o','HostKeyAlias=relay-host-observer','-o','UserKnownHostsFile='+str(args.credentials/'known-hosts'),'-i',str(args.credentials/'ssh-key'),'-l','ubuntu',config['host'],'observe',nonce]
        try:
            sent=clock.capture()
            r=subprocess.run(argv,input=nonce.encode('ascii'),capture_output=True,timeout=12)
            received=clock.capture()
            if r.returncode or len(r.stdout)>16384:raise ValueError()
            raw=json.loads(r.stdout)
            model=clock.estimate(raw.get('clock',{}),nonce,sent,received,machine=config['machine_id_sha256'],boot_id=raw['boot_id'])
            timing={'status':'VERIFIED',**model.diagnostic(),'boot_id':model.boot_id,
                    'received_wall':model.received_wall,'received_mono':model.received_mono,
                    'low':model.low,'high':model.high,'machine':model.machine,'local_boot':model.local_boot,
                    'clock_status':raw.get('clock_status')}
        except clock.ClockRejected as error:
            raw={};timing={'status':'UNKNOWN','reason':str(error)}
        except (OSError,ValueError,subprocess.TimeoutExpired):pass
        try:twitch=load(TWITCH)
        except (OSError,ValueError):twitch={}
        out=project(raw,previous,twitch,time.time(),config['machine_id_sha256'],binding=config.get('twitch_binding'),clock_model=model)
        clock_path=os.environ.get('STREAM_CLOCK_RELAY_STATE')
        if clock_path:
            p=Path(clock_path);tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(timing,allow_nan=False));tmp.chmod(0o644);os.replace(tmp,p)
        with LOCK:LATEST.clear();LATEST.update(out)
        for name,value in [('public.json',out),('previous.json',raw),('clock.json',timing)]:
            p=args.state/name;t=p.with_suffix('.tmp');t.write_text(json.dumps(value,allow_nan=False));t.chmod(0o600);os.replace(t,p)
        previous=raw if out['source_fresh'] else None
        stopped.wait(max(1,60-(time.monotonic()-started)))
    server.shutdown();server.server_close()
    return 0


if __name__=='__main__':raise SystemExit(main())
