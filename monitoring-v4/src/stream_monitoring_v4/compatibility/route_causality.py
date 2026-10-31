"""Arena-owned recovery veto. Never grants authority or rewrites raw warnings."""
from __future__ import annotations
import argparse
import dataclasses
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import threading

_spec=importlib.util.spec_from_file_location('route_guard_clock',Path(__file__).with_name('clock_contract.py'))
clock=importlib.util.module_from_spec(_spec);sys.modules[_spec.name]=clock;_spec.loader.exec_module(clock)
DEFAULT_PATH=Path('/run/stream-v4-route-causality/current.json')
DOMAINS={'HEALTHY','MEDIA_FAILURE','CLOCK_FAULT','OBSERVATION_LOST','INITIALIZING'}


def load(path):
    with Path(path).open('rb') as handle:raw=handle.read(16385)
    if len(raw)>16384:raise ValueError('oversized_cause')
    return json.loads(raw)


def valid_clock_diagnostic(value):
    return (isinstance(value,dict) and value.get('status') in ('VERIFIED','FAULT','UNAVAILABLE')
            and clock.finite(value.get('fault_for_sec')) and value['fault_for_sec']>=0
            and (value['status']!='VERIFIED' or value['fault_for_sec']==0))


def validate_source(value):
    cause=value['causality'];ident=cause['cause_id']
    if (value['schema']!='stream_v3.fallback_controller.v1' or value['assessment'] not in DOMAINS
            or cause['assessment']!=value['assessment'] or cause['reason']!=value['reason']
            or type(cause['cause_active']) is not bool
            or type(value['fallback_replicas']) is not int or value['fallback_replicas']<0
            or cause['fallback_active'] is not (value['fallback_replicas']>0)
            or (ident is not None and (not isinstance(ident,str) or len(ident)!=32
                or any(c not in '0123456789abcdef' for c in ident)))
            or cause['cause_active']!=(ident is not None)):
        raise ValueError('invalid_cause')
    # This policy belongs to arena, not to the Dell facts producer.
    result={'cause_id':ident,'assessment':value['assessment'],'reason':value['reason'],
            'cause_active':cause['cause_active'],'fallback_active':cause['fallback_active'],
            'secondary_recovery_blocked':value['assessment']!='HEALTHY' or cause['cause_active'] or cause['fallback_active']}
    # A healthy media route does not authorize other recovery actions while
    # UTC diagnostics are rejected. Keep this arena-owned veto independent.
    diagnostic=value.get('clock_diagnostic')
    if diagnostic is not None:
        if not valid_clock_diagnostic(diagnostic):
            raise ValueError('invalid_clock_diagnostic')
        result['clock_diagnostic']=diagnostic
        result['secondary_recovery_blocked'] |= diagnostic['status']!='VERIFIED'
        return result
    # Rolling upgrade: retain the old producer's hold facts until it is replaced.
    deadline=cause.get('progress_deadline');deadline=deadline if isinstance(deadline,dict) else {}
    held=deadline.get('clock_hold_for_sec',0)
    ack_age=deadline.get('last_ack_progress_age_sec')
    route_age=deadline.get('last_route_progress_age_sec')
    route_flow=(deadline.get('route_progress_confirmed') is True
                and clock.finite(route_age) and 0<=route_age<=45)
    ack_flow=(clock.finite(ack_age) and 0<=ack_age<=45
              and isinstance(cause.get('transport_witness'),dict)
              and cause['transport_witness'].get('status')=='FLOWING')
    if (clock.finite(held) and held>=0 and value['assessment']=='CLOCK_FAULT'
            and (route_flow or ack_flow)):
        result['clock_observation_hold_sec']=held
    else:result['clock_observation_hold_sec']=0.
    return result


def guard(path=None,*,now=None,boot=None,_value=None):
    try:
        value=load(path or DEFAULT_PATH) if _value is None else _value
        now=clock.mono() if now is None else now
        boot=clock.identity()['boot_id'] if boot is None else boot
        if ('clock_diagnostic' in value and not valid_clock_diagnostic(value['clock_diagnostic'])):
            raise ValueError('invalid_clock_diagnostic')
        if (value['schema']!='stream.route_causality.v1' or value['boot_id']!=boot
                or not clock.finite(value['received_mono'])
                or not clock.finite(value['source_age_upper_seconds'])
                or not 0<=value['source_age_upper_seconds']<=45
                or not 0<=now-value['received_mono']<=45-value['source_age_upper_seconds']
                or value['status']!='VERIFIED' or value['assessment'] not in DOMAINS
                or type(value['secondary_recovery_blocked']) is not bool
                or type(value['cause_active']) is not bool or type(value['fallback_active']) is not bool
                or (value['secondary_recovery_blocked'] is False and
                    (value['assessment']!='HEALTHY' or value['cause_active'] or value['fallback_active']
                     or ('clock_diagnostic' in value and value['clock_diagnostic'].get('status')!='VERIFIED')))):
            raise ValueError('cause_expired')
        return {k:value[k] for k in ('status','cause_id','assessment','reason','cause_active','fallback_active','secondary_recovery_blocked')}
    except (OSError,ValueError,KeyError,TypeError,AttributeError):
        return {'status':'UNKNOWN','cause_id':None,'assessment':'OBSERVATION_LOST','reason':'cause_context_unavailable',
                'cause_active':None,'fallback_active':None,'secondary_recovery_blocked':True}


def notification_context(path=None,*,now=None,boot=None):
    """A fresh observation fact for the existing notification owner; no action."""
    try:
        value=load(path or DEFAULT_PATH)
        context=guard(path,now=now,boot=boot,_value=value)
        held=value.get('clock_observation_hold_sec',0.)
        if context['status']!='VERIFIED' or not clock.finite(held) or held<0:
            raise ValueError('observation_context_unavailable')
        if 'clock_diagnostic' in value:
            diagnostic=value['clock_diagnostic']
            if not valid_clock_diagnostic(diagnostic):
                raise ValueError('invalid_clock_diagnostic')
            return {**context,'clock_diagnostic':diagnostic}
        return {**context,'clock_observation_hold_sec':held}
    except (OSError,KeyError,TypeError,ValueError):
        return {'status':'UNKNOWN','assessment':'OBSERVATION_LOST','clock_observation_hold_sec':0.}


def annotate(payload):
    return {**payload,'route_causality':guard(),'causal_attribution':'candidate_related_cause'}


def gate_action(decision):
    context=guard()
    if context['secondary_recovery_blocked'] and decision.action in ('restart_stream','resync_resolver'):
        return dataclasses.replace(decision,action='none',reason='route causal guard: '+context['reason'],
                    blocked_by=(*decision.blocked_by,'route_causal_guard'))
    return decision


def install_hooks(module,kind):
    """Adapter for the pinned legacy monitor, before budgets and API mutations."""
    if kind=='youtube_monitor':
        original=module.decide_action
        module.decide_action=lambda *a,**kw:gate_action(original(*a,**kw))
        force=module.force_transition_live_once
        module.force_transition_live_once=lambda *a,**kw:((False,'route causal guard')
            if guard()['secondary_recovery_blocked'] else force(*a,**kw))
        record=module.record_status
        module.record_status=lambda payload:record(annotate(payload))
        restart=module.restart_stream
        module.restart_stream=lambda *a,**kw:((False,'route causal guard')
            if guard()['secondary_recovery_blocked'] else restart(*a,**kw))
    elif kind=='stream_watchdog':
        # allow_restart is the first reservation of the restart budget.
        original=module.allow_restart
        def allowed(component,reason):
            if guard()['secondary_recovery_blocked']:
                module.log('Recovery held by route causal guard: '+str(component))
                return False
            return original(component,reason)
        module.allow_restart=allowed
        record=module.record_watchdog_stats
        def stats(*a,**kw):return record(*a,**{**kw,'route_causality':guard()})
        module.record_watchdog_stats=stats
    else:raise ValueError('unsupported_watchdog')


def probe(config):
    nonce=secrets.token_hex(16);sent=clock.capture()
    # Only this fixed read-only helper. Host identity is pinned in the config.
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=3',
        'dell','python3','-I','-B','/opt/stream-clock/current/clock_observer.py',
        '--host-id','dell-stream-runtime','--fallback-exchange',nonce],capture_output=True,timeout=8)
    received=clock.capture()
    if result.returncode or len(result.stdout)>16384:raise ValueError('cause_probe_unavailable')
    packet=json.loads(result.stdout);value=packet['value']
    age=clock.monotonic_source_age(packet['clock'],nonce,sent,received,
        machine=config['machine_id_sha256'],boot_id=value['boot_id'],source_mono=value['observed_monotonic'])
    return {**validate_source(value),'source_age_upper_seconds':age,'received_mono':received['mono']}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--once',action='store_true')
    args=parser.parse_args();config=load(args.config);stopped=threading.Event()
    previous=None;last_steps=None;hold_until=0
    signal.signal(signal.SIGTERM,lambda *_:stopped.set())
    while not stopped.is_set():
        try:
            out={**probe(config),'status':'VERIFIED'}
            own=load('/run/stream-clock/status.json');mark=clock.capture()
            if (own['boot_id']!=clock.identity()['boot_id'] or own['synchronized'] is not True
                    or not 0<=mark['mono']-own['mono']<=45):raise ValueError('arena_clock_unavailable')
            steps=own['wall_steps_total']
            if ((last_steps is not None and steps!=last_steps)
                    or (previous and abs(mark['wall']-previous['wall']-mark['mono']+previous['mono'])>clock.STEP_LIMIT)):
                hold_until=mark['mono']+180
            previous=mark;last_steps=steps
            if mark['mono']<hold_until:
                out.update(secondary_recovery_blocked=True,assessment='CLOCK_FAULT',reason='arena_clock_settling')
        except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError):
            out={'status':'UNKNOWN','assessment':'OBSERVATION_LOST','reason':'cause_context_unavailable',
                 'cause_id':None,'cause_active':None,'fallback_active':None,'secondary_recovery_blocked':True,
                 'received_mono':clock.mono(),'source_age_upper_seconds':0}
        out.update(schema='stream.route_causality.v1',boot_id=clock.identity()['boot_id'])
        tmp=args.output.with_suffix('.tmp');tmp.write_text(json.dumps(out,allow_nan=False));tmp.chmod(0o644);os.replace(tmp,args.output)
        if args.once:return
        stopped.wait(15)


if __name__=='__main__':main()
