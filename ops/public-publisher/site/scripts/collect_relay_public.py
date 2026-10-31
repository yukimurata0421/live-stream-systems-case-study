"""Copy only arena's credential-free relay projection into the public tree."""
import json
import importlib.util
import secrets
import sys
import math
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from public_snapshot_time import PublicTimestampError, validate_generated_at

PUBLIC=Path(__file__).resolve().parents[1]/'public'
ISSUES={'RELAY_STOPPED','INPUT_NOT_PROGRESSING','YOUTUBE_NOT_PROGRESSING','TWITCH_NOT_PROGRESSING','VM_MEMORY_HIGH','VM_DISK_LOW','DAILY_RENEWAL_HELD','TWITCH_OFFLINE','RELAY_OBSERVATION_UNAVAILABLE','TWITCH_API_UNAVAILABLE'}
CONTROLLER_REASONS={'progress_confirmed','baseline_required','observation_unavailable','facts_invalid','schema_invalid',
    'host_binding_mismatch','timestamp_invalid','source_future','source_stale','copy_future','copy_stale',
    'progress_future','progress_stale','copy_facts_missing','copy_binding_mismatch','copy_not_flowing',
    'copy_identity_invalid','input_not_live','connection_changed','sample_gap','counter_invalid',
    'video_not_advancing','audio_not_advancing','input_not_ready','input_not_advancing','output_not_forwarding',
    'output_not_advancing','facts_http_unavailable','facts_timeout','facts_transport_error','clock_unavailable','counter_regressed','local_transport_stalled','local_fifo_recovering'}


def keys(value,expected):
    if not isinstance(value,dict) or set(value)!=set(expected):raise ValueError('UNEXPECTED_PUBLIC_FIELD')


def timestamp(v):
    if v is not None:
        if not isinstance(v,str) or len(v)>40:raise ValueError()
        t=datetime.fromisoformat(v.replace('Z','+00:00'))
        if t.tzinfo is None:raise ValueError()
        return t.timestamp()


def numeric(v,upper=1e12):
    if v is not None and (type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=upper):raise ValueError()


def validate_controller(value):
    if value is None:return
    keys(value,['schema','observed_at','health','reason','healthy_for_sec','stop_after_sec',
                'healthy_resets_total','last_reset_at','last_reset_reason'])
    if value['schema']!='stream_v3.fallback_controller_public.v1' or value['observed_at'] is None:raise ValueError()
    timestamp(value['observed_at']);timestamp(value['last_reset_at'])
    if value['health'] not in ('HEALTHY','UNHEALTHY','UNKNOWN') or value['reason'] not in CONTROLLER_REASONS:raise ValueError()
    if (value['health']=='HEALTHY')!=(value['reason']=='progress_confirmed'):raise ValueError()
    numeric(value['healthy_for_sec'])
    if value['healthy_for_sec'] is None or (value['health']!='HEALTHY' and value['healthy_for_sec']!=0):raise ValueError()
    if type(value['stop_after_sec']) is not int or value['stop_after_sec']!=300:raise ValueError()
    if type(value['healthy_resets_total']) is not int or not 0<=value['healthy_resets_total']<=1e12:raise ValueError()
    if (value['last_reset_at'] is None)!=(value['last_reset_reason'] is None):raise ValueError()
    if value['last_reset_reason'] is not None and value['last_reset_reason'] not in CONTROLLER_REASONS:raise ValueError()


def validate(v,now):
    base=['schema','generated_at','observed_at','status','source_fresh','vm','relay','input','destinations','daily','twitch_api','issues']
    # Fallback fields are optional for older producer artifacts.
    keys(v,base+(['fallback'] if isinstance(v,dict) and 'fallback' in v else []))
    if 'fallback' in v:
        fb=v['fallback'];keys(fb,['active','ready','observed_at']+[k for k in ('mbps','controller','causality') if isinstance(fb,dict) and k in fb]);timestamp(fb['observed_at'])
        if 'controller' in fb:validate_controller(fb['controller'])
        if 'causality' in fb:
            cause=fb['causality'];keys(cause,['status','assessment','cause_id','secondary_recovery_blocked'])
            if (cause['status'] not in ('VERIFIED','UNKNOWN') or cause['assessment'] not in
                    ('HEALTHY','CLOCK_FAULT','OBSERVATION_LOST','MEDIA_FAILURE','INITIALIZING')
                    or type(cause['secondary_recovery_blocked']) is not bool):raise ValueError()
            ident=cause['cause_id']
            if ident is not None and (not isinstance(ident,str) or len(ident)!=32 or any(c not in '0123456789abcdef' for c in ident)):raise ValueError()
        if 'mbps' in fb:numeric(fb['mbps'],10000)
        if any(v['fallback'][k] not in (True,False,None) for k in ('active','ready')):raise ValueError()
    if v['schema']!='stream.relay_public.v1':raise ValueError()
    validate_generated_at(v['generated_at'],now)
    timestamp(v['observed_at'])
    if v['status'] not in ['OBSERVED','UNKNOWN'] or type(v['source_fresh']) is not bool:raise ValueError()
    keys(v['vm'],['status','cpu_percent','memory_percent','disk_free_percent','uptime_seconds'])
    if v['vm']['status'] not in ['OBSERVED','UNKNOWN']:raise ValueError()
    for k in ['cpu_percent','memory_percent','disk_free_percent']:numeric(v['vm'][k],100)
    numeric(v['vm']['uptime_seconds'])
    keys(v['relay'],['status','restart_count']);numeric(v['relay']['restart_count'])
    if v['relay']['status'] not in ['RUNNING','STOPPED','UNKNOWN']:raise ValueError()
    keys(v['destinations'],['youtube','twitch'])
    for d in [v['input'],*v['destinations'].values()]:
        keys(d,['status','mbps']);numeric(d['mbps'],10000)
        if d['status'] not in ['FLOWING','STALLED','CONNECTED','DISCONNECTED','UNKNOWN']:raise ValueError()
    keys(v['daily'],['state','next_renewal_at','planned_break']);timestamp(v['daily']['next_renewal_at'])
    if v['daily']['state'] not in ['TRACKING','WAITING_CREDENTIALS','WAIT_STOP','WAIT_OFFLINE','WAIT_START','VERIFYING','CANCELLED','RENEWED','HOLD','UNKNOWN'] or type(v['daily']['planned_break']) is not bool:raise ValueError()
    keys(v['twitch_api'],['state','observed_at','source_fresh']);timestamp(v['twitch_api']['observed_at'])
    if v['twitch_api']['state'] not in ['LIVE','OFFLINE','UNKNOWN'] or type(v['twitch_api']['source_fresh']) is not bool:raise ValueError()
    if not isinstance(v['issues'],list) or len(v['issues'])>10 or any(x not in ISSUES for x in v['issues']):raise ValueError()
    return v


def clock_payload(packet, model, now):
    """Translate observation times into receiver UTC using the oldest possible age.

    Source timestamps and the uncertainty remain in a private local receipt.
    Public schema contains no machine, boot, address, or transport envelope.
    """
    value = packet['value']
    generated = value['generated_at']
    if not model.fresh(generated,180): raise ValueError('clock_payload_stale')
    value['generated_at'] = now-model.age_bounds(generated)[1]
    def shift(obj, key):
        if obj.get(key) is not None:
            source = timestamp(obj[key])
            obj[key] = datetime.fromtimestamp(now-model.age_bounds(source)[1], timezone.utc).isoformat()
    shift(value,'observed_at')
    shift(value['daily'],'next_renewal_at')
    shift(value['twitch_api'],'observed_at')
    fb=value.get('fallback')
    if fb:
        shift(fb,'observed_at')
        cp=fb.get('controller')
        if cp:
            shift(cp,'observed_at');shift(cp,'last_reset_at')
    return validate(value,now)


def main():
    path=PUBLIC/'relay-observation.json'
    try:
        spec=importlib.util.spec_from_file_location('stream_clock_contract',Path(__file__).with_name('clock_contract.py'))
        clock=importlib.util.module_from_spec(spec);sys.modules[spec.name]=clock;spec.loader.exec_module(clock)
        nonce=secrets.token_hex(16);sent=clock.capture()
        r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','arena-server','python3','-B','/opt/stream-v4-relay-public/current/relay_public.py','--clock-nonce',nonce],capture_output=True,timeout=12)
        if r.returncode or len(r.stdout)>16384:raise ValueError()
        received=clock.capture();packet=json.loads(r.stdout)
        if packet['schema']!='stream.clock_payload.v1':raise ValueError()
        machine=json.loads(Path(__file__).with_name('clock-peers.json').read_text())['arena_machine_sha256']
        model=clock.estimate(packet['clock'],nonce,sent,received,machine=machine)
        source_generated=packet['value']['generated_at']
        value=clock_payload(packet,model,time.time())
        receipt={'source_generated_at':source_generated,'clock':model.diagnostic(),'received_at':received['wall']}
        cache=Path.home()/'.cache/stream-clock-publisher';cache.mkdir(parents=True,exist_ok=True)
        temporary=cache/'receipt.tmp';temporary.write_text(json.dumps(receipt,allow_nan=False));os.replace(temporary,cache/'receipt.json')
    except (OSError,ValueError,TypeError,KeyError,subprocess.TimeoutExpired) as exc:
        reason=str(exc) if isinstance(exc,PublicTimestampError) else 'FETCH_OR_SCHEMA_INVALID'
        print(json.dumps({'relay_public_source':'unavailable','last_artifact_retained':path.exists(),'reason':reason}))
        # Retain the original timestamps: a failed fetch never rejuvenates evidence.
        return 0
    fd,name=tempfile.mkstemp(prefix='.relay-',dir=PUBLIC)
    try:
        with os.fdopen(fd,'w') as out:
            os.fchmod(out.fileno(),0o644);json.dump(value,out,allow_nan=False);out.write('\n');out.flush();os.fsync(out.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)
    return 0


if __name__=='__main__':raise SystemExit(main())
