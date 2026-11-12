"""Run concurrent API/probe checks in an isolated image before redeployment.

Mount fixture certificates at /fixture and an owned results directory at /results.
Require no network, a finite manifest-matched CPU quota, 96Mi memory and non-root
execution. This comparison does not estimate production failure frequency.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

sys.path.insert(0, '/app')
import pod_api_observer as observer

parser=argparse.ArgumentParser()
parser.add_argument('--label',required=True)
parser.add_argument('--trials',type=int,default=16)
parser.add_argument('--probes',type=int,default=2)
parser.add_argument('--probe-script',default='/app/pod_api_health.py')
parser.add_argument('--legacy-probe',action='store_true')
parser.add_argument('--manifest',required=True)
args=parser.parse_args()
container=json.loads(Path(args.manifest).read_text())['spec']['template']['spec']['containers'][0]
cpu=container['resources']['limits']['cpu']
if cpu not in ('100m','250m'):
    raise SystemExit('qualification_requires_reviewed_cpu_quota')
quota,period=Path('/sys/fs/cgroup/cpu.max').read_text().split()
if quota=='max' or int(quota)*1000!=int(cpu[:-1])*int(period):
    raise SystemExit('manifest_cpu_quota_differs_from_qualification')
memory_limit=Path('/sys/fs/cgroup/memory.max').read_text().strip()
if memory_limit=='max' or int(memory_limit)>96*1024*1024:
    raise SystemExit('qualification_requires_memory_limit_at_most_96MiB')
if container['resources']['limits']!={'cpu':cpu,'memory':'96Mi'}:
    raise SystemExit('manifest_resource_limits_differ_from_qualification')
probe_specs=[container[name] for name in ['readinessProbe','livenessProbe']]
root=Path('/ram');events=[];api_events=[];lock=threading.Lock()
payload={'iat':int(time.time()),'exp':int(time.time())+3600}
token='fixture.'+base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')+'.not-real'
(root/'token').write_text(token)
body=json.dumps({'kind':'PodList','items':[{'metadata':{'namespace':'stream-v3','name':'runtime','uid':'fixture-pod','resourceVersion':'1',
 'labels':{'app.kubernetes.io/name':'stream-v3','app.kubernetes.io/component':'runtime'}},
 'spec':{'nodeName':'fixture','padding':'x'*8192},'status':{'containerStatuses':[{'name':'stream-engine','containerID':'containerd://fixture','ready':True,'state':{'running':{'startedAt':'2026-11-03T00:00:00Z'}}}]}}]}).encode()

class API(BaseHTTPRequestHandler):
    def do_GET(self):
        started=time.monotonic();code=200 if self.headers.get('Authorization')=='Bearer '+token else 401
        self.send_response(code);self.send_header('Content-Length',str(len(body)));self.end_headers()
        try:self.wfile.write(body)
        except (OSError,ssl.SSLError):pass
        with lock:api_events.append({'started':started,'duration':time.monotonic()-started,'code':code})
    def log_message(self,*args):pass

api=ThreadingHTTPServer(('127.0.0.1',0),API)
tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain('/fixture/cert.pem','/fixture/key.pem')
api.socket=tls.wrap_socket(api.socket,server_side=True)
threading.Thread(target=api.serve_forever,daemon=True).start()
config=SimpleNamespace(socket=observer.SOCKET,client_uid=os.getuid(),api_url='https://127.0.0.1:'+str(api.server_port),token='/ram/token',ca='/fixture/cert.pem',node_name='fixture')

def cgroup():
    # Docker namespaces expose this cgroup at their mount root.
    return {n:Path('/sys/fs/cgroup/'+n).read_text() for n in ['cpu.stat','cpu.pressure','memory.peak','memory.events']}

def measured_fetch(config):
    started=time.monotonic();before=len(api_events);error=None
    try:return observer.bounded_fetch(config)
    except ValueError as exc:
        error=str(exc);raise
    finally:
        with lock:events.append({'kind':'fetch','started':started,'duration':time.monotonic()-started,'error':error,'api_responses':len(api_events)-before})

server=observer.Server(config,measured_fetch)
threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02},daemon=True).start()

def probe(index):
    started=time.monotonic()
    spec=probe_specs[index%len(probe_specs)]
    command=([sys.executable,args.probe_script,'status','--socket',config.socket,'--server-uid',str(os.getuid())]
             if args.legacy_probe else spec['exec']['command'])
    try:
        result=subprocess.run(command,capture_output=True,timeout=spec['timeoutSeconds'])
        error=None if result.returncode==0 else 'exit_'+str(result.returncode)
    except subprocess.TimeoutExpired:error='timeout'
    with lock:events.append({'kind':'probe','started':started,'duration':time.monotonic()-started,'error':error})

started=time.monotonic();stats_before=cgroup();results=[]
try:
    with ThreadPoolExecutor(max_workers=2) as pool:
        for trial in range(args.trials):
            # Reproduce a real possible collision: one fetch child and both exec probes.
            pending=[pool.submit(probe,index) for index in range(args.probes)]
            begin=time.monotonic();error=None
            try:observer.exchange(config.socket,expected_uid=os.getuid())
            except Exception as exc:error=type(exc).__name__+':'+str(exc)
            for future in pending:future.result()
            results.append({'trial':trial,'duration':time.monotonic()-begin,'error':error})
            print(json.dumps({'label':args.label,**results[-1]}),flush=True)
            time.sleep(2)
    stats_after=cgroup();summary={'label':args.label,'trials':len(results),'failures':sum(r['error'] is not None for r in results),'elapsed':time.monotonic()-started,'max_trial_seconds':max(r['duration'] for r in results),'fetch_timeouts':sum(e['kind']=='fetch' and e['error']=='POD_API_TIMEOUT' for e in events),'api_http_errors':sum(e['code']!=200 for e in api_events),'api_responses':len(api_events),'api_max_handler_seconds':max((e['duration'] for e in api_events),default=0),'events':events,'results':results,'cgroup_before':stats_before,'cgroup_after':stats_after,'observer_status':observer.exchange(config.socket,operation='status',expected_uid=os.getuid())['status']}
    summary['probe_failures']=sum(bool(e['error']) for e in events if e['kind']=='probe')
    summary['busy_rejections']=summary['observer_status'].get('busy_rejections',0)
    Path('/results/'+args.label+'.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps({k:v for k,v in summary.items() if k not in ['events','results','cgroup_before','cgroup_after','observer_status']}),flush=True)
    if not args.legacy_probe and (summary['failures'] or summary['fetch_timeouts']
            or summary['probe_failures'] or summary['busy_rejections']):
        raise SystemExit(1)
finally:
    server.shutdown();server.server_close();api.shutdown();api.server_close()
