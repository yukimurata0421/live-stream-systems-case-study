#!/usr/bin/env python3
"""Record controller clock observations for a fixed window on a configured data disk; no control actions."""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

SCHEMA = 'stream_v3.fallback_clock_recording.v1'
STATUS = Path('/run/stream-v3-fallback-controller/status.json')
NUMBERS = ('source_age_seconds', 'copy_age_seconds', 'progress_age_seconds',
           'http_elapsed_seconds', 'healthy_for_sec', 'unhealthy_for_sec',
           'healthy_resets_total', 'fallback_replicas', 'controller_pid', 'http_status',
           'clock_offset_seconds','clock_uncertainty_seconds','clock_network_rtt_seconds',
           'source_age_upper_seconds','copy_age_upper_seconds','progress_age_upper_seconds')
STAMPS = ('observed_at', 'source_observed_at', 'http_started_at', 'http_completed_at', 'last_reset_at')
CODES = ('assessment', 'health', 'reason', 'last_reset_reason', 'action', 'action_result')

# Use the same read-only probe on both hosts; exclude arbitrary messages, addresses and server names.
CLOCK_PROBE = r'''
import subprocess,json,hashlib,re,shutil
from pathlib import Path
from datetime import datetime,timezone
errors=[]
def run(args):
    try:
        r=subprocess.run(args,capture_output=True,text=True,timeout=3,env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LC_ALL':'C'})
        if r.returncode==0:return r.stdout
        errors.append(args[0]+'_failed')
    except subprocess.TimeoutExpired:errors.append(args[0]+'_timeout')
    except OSError:errors.append(args[0]+'_unavailable')
    return ''
def props(text):
    return dict(x.split('=',1) for x in text.splitlines() if '=' in x)
v={'observed_at':datetime.now(timezone.utc).isoformat(),
   'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
   'machine_id_sha256':hashlib.sha256(Path('/etc/machine-id').read_bytes()).hexdigest(),
   'sync':props(run(['timedatectl','show','-p','NTPSynchronized','-p','NTP']))}
if shutil.which('chronyc'):
    v['daemon']='chrony';unit='chrony.service';parts=run(['chronyc','-c','tracking']).strip().split(',')
    if len(parts)==14:
        v['tracking']={k:float(parts[i]) for k,i in {'stratum':2,'reference_unixtime':3,
            'system_time_seconds':4,'last_offset_seconds':5,'rms_offset_seconds':6,
            'frequency_ppm':7,'residual_frequency_ppm':8,'skew_ppm':9,'root_delay_seconds':10,
            'root_dispersion_seconds':11,'update_interval_seconds':12}.items()}
        v['tracking']['leap_status']=parts[13]
else:
    v['daemon']='systemd-timesyncd';unit='systemd-timesyncd.service';v['tracking']={}
    text=run(['timedatectl','timesync-status','--no-pager'])
    names={'Offset':'offset_seconds','Delay':'delay_seconds','Jitter':'jitter_seconds','Root distance':'root_distance_seconds'}
    units={'s':1,'ms':.001,'us':.000001,'\u00b5s':.000001,'ns':.000000001}
    for line in text.splitlines():
        m=re.match(r'\s*(Offset|Delay|Jitter|Root distance):\s*([-+]?\d+(?:\.\d+)?)\s*(ms|us|\u00b5s|ns|s)',line)
        if m:v['tracking'][names[m[1]]]=float(m[2])*units[m[3]]
v['service']=props(run(['systemctl','show',unit,'-p','ActiveState','-p','MainPID','-p','NRestarts']))
v['events']=[]
files=[];mid=Path('/etc/machine-id').read_text().strip()
for base in ('/run/log/journal','/var/log/journal'):
    directory=Path(base)/mid;active=directory/'system.journal'
    if active.exists():files.append('--file='+str(active))
    archives=sorted(directory.glob('system@*.journal'),key=lambda p:p.stat().st_mtime,reverse=True)
    if archives:files.append('--file='+str(archives[0]))
v['journal_files_read']=len(files)
if not files:errors.append('journal_source_unavailable')
text=run(['journalctl',*files,'-u',unit,'--since','-2 min','-n','20','--no-pager','-o','json',
          '--output-fields=__REALTIME_TIMESTAMP,MESSAGE']) if files else ''
for line in text.splitlines():
    try:
        row=json.loads(line);msg=row.get('MESSAGE','').lower();kind=None
        for words,label in [(('stepped','clock step'),'clock_step'),(('initial clock synchronization',),'initial_sync'),
                            (('timed out','timeout'),'timeout'),(('contacted time server','selected source'),'source_selected'),
                            (('stopping ',),'stopping'),(('started ',),'started')]:
            if any(w in msg for w in words):kind=label;break
        if kind:v['events'].append({'at':int(row['__REALTIME_TIMESTAMP'])/1e6,'kind':kind,
                                   'id':hashlib.sha256((row['__REALTIME_TIMESTAMP']+msg).encode()).hexdigest()})
    except (ValueError,KeyError,TypeError):pass
v['errors']=errors
print(json.dumps(v,allow_nan=False,separators=(',',':')))
'''


def stamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamp_without_timezone')
    return parsed.timestamp()


def utc(now=None):
    return datetime.fromtimestamp(time.time() if now is None else now, timezone.utc).isoformat()


def encode(value):
    raw = json.dumps(value, allow_nan=False, separators=(',', ':')).encode() + b'\n'
    if len(raw) > 16384:
        raise ValueError('record_too_large')
    return raw


def controller_record(value):
    if not isinstance(value, dict) or value.get('schema') != 'stream_v3.fallback_controller.v1':
        raise ValueError('controller_schema')
    result = {'kind': 'controller'}
    value = dict(value)
    causal=value.get('causality',{})
    ident=causal.get('cause_id')
    if ident is not None and (not isinstance(ident,str) or len(ident)!=32 or any(c not in '0123456789abcdef' for c in ident)):
        raise ValueError('cause_id_invalid')
    result['cause_id']=ident
    result['secondary_recovery_blocked']=causal.get('secondary_recovery_blocked')
    timing = value.get('clock')
    if isinstance(timing,dict) and timing.get('status')=='VERIFIED':
        for target,source in [('clock_offset_seconds','offset_seconds'),
                              ('clock_uncertainty_seconds','uncertainty_seconds'),
                              ('clock_network_rtt_seconds','network_rtt_seconds')]:
            value[target]=timing.get(source)
    for name in ('source','copy','progress'):
        bounds=value.get(name+'_age_bounds_seconds')
        if isinstance(bounds,list) and len(bounds)==2:
            value[name+'_age_upper_seconds']=bounds[1]
    for key in STAMPS:
        item = value.get(key)
        if item is not None:
            if not isinstance(item, str) or len(item) > 40:
                raise ValueError('timestamp_invalid')
            stamp(item)
        result[key] = item
    if result['observed_at'] is None:
        raise ValueError('observed_at_missing')
    for key in NUMBERS:
        item = value.get(key)
        if item is not None and (type(item) not in (int, float) or not math.isfinite(item)):
            raise ValueError('number_invalid')
        result[key] = item
    for key in CODES:
        item = value.get(key)
        if item is not None and (not isinstance(item, str) or not item or len(item) > 64
                                 or not all(c.isascii() and (c.isalnum() or c == '_') for c in item)):
            raise ValueError('code_invalid')
        result[key] = item
    # Bound source generation by HTTP start/completion, including asymmetric delay.
    if all(result.get(k) is not None for k in ('source_observed_at', 'http_started_at', 'http_completed_at')):
        source = stamp(result['source_observed_at'])
        result['remote_minus_local_lower_seconds'] = source - stamp(result['http_completed_at'])
        result['remote_minus_local_upper_seconds'] = source - stamp(result['http_started_at'])
        if result.get('http_elapsed_seconds') is not None:
            result['http_wall_minus_monotonic_seconds'] = (
                stamp(result['http_completed_at']) - stamp(result['http_started_at']) - result['http_elapsed_seconds'])
    return result


def probe(host, expected, ssh=None):
    command = [sys.executable, '-I', '-B', '-'] if host == 'sender' else [
        '/usr/bin/ssh', '-F', ssh['config'], '-o', 'BatchMode=yes',
        '-o', 'ConnectTimeout=3', '-o', 'ConnectionAttempts=1', '-o', 'StrictHostKeyChecking=yes',
        ssh['host'], 'python3', '-I', '-B', '-']
    try:
        result = subprocess.run(command, input=CLOCK_PROBE, capture_output=True, text=True, timeout=18)
        if result.returncode or len(result.stdout) > 16384:
            raise ValueError('probe_unavailable')
        value = json.loads(result.stdout)
        if value['machine_id_sha256'] != expected:
            raise ValueError('host_binding_mismatch')
        encode(value)
        return {'kind': 'clock', 'host': host, 'status': 'OBSERVED', **value}
    except (OSError, subprocess.TimeoutExpired, ValueError, KeyError, TypeError):
        return {'kind': 'clock', 'host': host, 'status': 'UNKNOWN', 'observed_at': utc()}


def clock_pair(job):
    return [probe(host, job['machines'][host], job['ssh']) for host in ('sender', 'relay')]


def storage_guard(job):
    mount, output = Path(job['mount']), Path(job['output'])
    if not mount.is_mount() or mount.resolve() != mount or not output.is_relative_to(mount):
        raise ValueError('hdd_mount_missing')
    result = subprocess.run(['/usr/bin/findmnt','-n','-o','UUID','-T',str(mount)],
                            capture_output=True,text=True,timeout=3,check=True)
    if result.stdout.strip() != job['filesystem_uuid'] or output.stat().st_dev != mount.stat().st_dev or output.resolve() != output:
        raise ValueError('hdd_device_mismatch')
    if shutil.disk_usage(output).free < job['min_free_bytes']:
        raise ValueError('disk_reserve')


def atomic_json(path, value):
    raw = json.dumps(value, allow_nan=False, indent=2).encode() + b'\n'
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('wb') as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    os.replace(temporary, path)


def records(output):
    for path in sorted(output.glob('records-*.jsonl')):
        with path.open('rb') as handle:
            for line in handle:
                try:
                    if len(line) > 16384 or not line.endswith(b'\n'):
                        raise ValueError('incomplete_line')
                    yield json.loads(line)
                except (ValueError, UnicodeError):
                    yield {'kind': 'incomplete_record'}


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {'count': 0}
    return {'count': len(ordered), 'min': ordered[0], 'p50': ordered[(len(ordered)-1)//2],
            'p999': ordered[max(0, math.ceil(.999 * len(ordered))-1)], 'max': ordered[-1]}


def summarize(output, job, reason, history=None):
    samples = {}; clocks = []; counts = collections.Counter(); values = collections.defaultdict(list)
    for row in records(output) if history is None else history:
        counts[row.get('kind', 'invalid')] += 1
        if row.get('kind') == 'controller':
            samples[row['observed_at']] = row
        elif row.get('kind') == 'clock':
            clocks.append(row)
    rows = sorted(samples.values(), key=lambda x: stamp(x['observed_at']))
    resets = 0; previous = None; gaps = []; future_start = None; max_future_span = 0
    for row in rows:
        for key in (*NUMBERS[:4], 'remote_minus_local_lower_seconds', 'remote_minus_local_upper_seconds',
                    'http_wall_minus_monotonic_seconds'):
            if row.get(key) is not None: values[key].append(row[key])
        if previous:
            gap = stamp(row['observed_at']) - stamp(previous['observed_at'])
            if gap > 45: gaps.append({'after': previous['observed_at'], 'seconds': gap})
            if row.get('controller_pid') == previous.get('controller_pid'):
                resets += max(0, (row.get('healthy_resets_total') or 0) - (previous.get('healthy_resets_total') or 0))
        previous = row
        if (row.get('source_age_seconds') or 0) < 0:
            future_start = row['observed_at'] if future_start is None else future_start
            max_future_span = max(max_future_span, stamp(row['observed_at'])-stamp(future_start))
        else:
            future_start = None
    clock_values = collections.defaultdict(list)
    for row in clocks:
        for key, value in row.get('tracking', {}).items():
            if type(value) in (int,float) and math.isfinite(value):
                clock_values[row['host']+'.'+key].append(value)
    return {'schema': SCHEMA, 'generated_at': utc(), 'started_at': job['started_at'],
            'deadline': job['deadline'], 'finish_reason': reason, 'controller_samples': len(rows),
            'first_sample': rows[0]['observed_at'] if rows else None,
            'last_sample': rows[-1]['observed_at'] if rows else None,
            'record_counts': dict(counts), 'distributions': {k: distribution(v) for k,v in values.items()},
            'future_samples': sum((r.get('source_age_seconds') or 0) < 0 for r in rows),
            'max_consecutive_future_observation_span_seconds': max_future_span,
            'health_counts': dict(collections.Counter(r.get('health') for r in rows)),
            'reason_counts': dict(collections.Counter(r.get('reason') for r in rows)),
            'healthy_resets_observed': resets, 'gaps_over_45_seconds': gaps,
            'clock_probe_unknown': sum(r.get('status') != 'OBSERVED' for r in clocks),
            'clock_probe_component_errors': dict(collections.Counter(r['host']+'.'+e
                                                 for r in clocks for e in r.get('errors',[]))),
            'clock_distributions': {k:distribution(v) for k,v in clock_values.items()},
            'clock_events': list({(r['host'],e['id']): {'host':r['host'],**e}
                                  for r in clocks for e in r.get('events',[])}.values()),
            'limits': 'HTTP elapsed includes server processing. Clock bounds are not a direct NTP offset; no automatic threshold change.'}


def validate_job(job):
    start, end = stamp(job['started_at']), stamp(job['deadline'])
    if not 0 < end - start <= 7*86400 or not 0 < job['max_bytes'] <= 128*1024*1024:
        raise ValueError('unbounded_job')
    if set(job['machines']) != {'sender', 'relay'} or any(len(v) != 64 for v in job['machines'].values()):
        raise ValueError('host_binding_missing')
    ssh = job.get('ssh', {})
    host = ssh.get('host', '')
    if (not isinstance(host, str) or not 1 <= len(host) <= 128 or not host[0].isalnum()
            or not all(c.isascii() and (c.isalnum() or c in '._-') for c in host)
            or not Path(ssh.get('config', '')).is_absolute()):
        raise ValueError('ssh_configuration_invalid')
    return start, end


def collect(job, *, status=STATUS, stop=lambda: False, clock_probe=clock_pair):
    start, deadline = validate_job(job)
    storage_guard(job)
    output = Path(job['output'])
    total = sum(p.stat().st_size for p in output.glob('records-*.jsonl'))
    history = list(records(output))
    previous = next((r['observed_at'] for r in reversed(history) if r.get('kind')=='controller'), None)
    reason = 'DEADLINE_REACHED' if time.time() >= deadline else 'STOPPED'
    # Also bound each invocation with monotonic time if the wall clock moves backwards.
    until = time.monotonic() + max(0, min(deadline-time.time(), deadline-start))
    future = None; next_clock = 0; next_flush = 0; last_error = None
    path = output / ('records-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8] + '.jsonl')
    def append(handle, value):
        nonlocal total
        value = {'recorded_at': utc(), 'recorded_monotonic_seconds': time.monotonic(), **value}
        raw = encode(value)
        if total + len(raw) > job['max_bytes']: raise ValueError('capacity_limit')
        storage_guard(job)
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        total += len(raw); history.append(value)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool, path.open('xb') as handle:
        os.fchmod(handle.fileno(), 0o600)
        try:
            append(handle, {'kind':'recorder_started', 'pid':os.getpid()})
            while time.time() < deadline and time.monotonic() < until and not stop():
                now = time.monotonic()
                try:
                    raw = status.read_bytes()
                    if len(raw) > 8192: raise ValueError('status_too_large')
                    row = controller_record(json.loads(raw))
                    if row['observed_at'] != previous:
                        # Do not count a retained pre-window status as a new observation.
                        if start <= stamp(row['observed_at']) < deadline:
                            append(handle, row)
                        previous = row['observed_at']
                    last_error = None
                except (OSError, ValueError, KeyError, TypeError):
                    if last_error is None or now-last_error >= 60:
                        append(handle, {'kind':'controller_unavailable'});last_error=now
                if future is not None and future.done():
                    for row in future.result(): append(handle,row)
                    future = None
                if now >= next_clock and future is None:
                    future = pool.submit(clock_probe,job);next_clock=now+60
                if now >= next_flush:
                    handle.flush();os.fsync(handle.fileno())
                    atomic_json(output/'summary.json',summarize(output,job,'RUNNING',history))
                    next_flush=now+60
                time.sleep(min(1,max(0,deadline-time.time())))
            reason = 'DEADLINE_REACHED' if time.time() >= deadline or time.monotonic() >= until else 'STOPPED'
            if future is not None:
                for row in future.result(): append(handle,row)
            append(handle,{'kind':'recorder_finished','reason':reason})
        except (OSError,ValueError) as error:
            reason = 'STORAGE_STOP_' + type(error).__name__
            print(json.dumps({'event':'recording_stopped','reason':reason}),flush=True)
        finally:
            handle.flush();os.fsync(handle.fileno())
            storage_guard(job)
            atomic_json(output/'summary.json',summarize(output,job,reason,history))
    return reason


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--job',type=Path,required=True)
    parser.add_argument('--probe-once',action='store_true');args=parser.parse_args()
    job=json.loads(args.job.read_text());validate_job(job)
    if args.probe_once:
        print(json.dumps(clock_pair(job),allow_nan=False));return
    stopped=[False]
    def finish(*_):stopped[0]=True
    signal.signal(signal.SIGTERM,finish);signal.signal(signal.SIGINT,finish)
    result=collect(job,stop=lambda:stopped[0]);print(json.dumps({'event':'recording_finished','reason':result}),flush=True)


if __name__ == '__main__':main()
