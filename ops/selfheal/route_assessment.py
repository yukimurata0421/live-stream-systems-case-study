"""Route facts and causal identity; observation loss is not a media failure."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import uuid

DOMAINS = frozenset(('HEALTHY','MEDIA_FAILURE','CLOCK_FAULT','OBSERVATION_LOST','INITIALIZING'))


def domain_for_missing(reason):
    if reason=='baseline_required':return 'INITIALIZING'
    return 'OBSERVATION_LOST'


class TransportWitness:
    """Two fresh generations of the fixed local transport export, without UTC age.

    A timestamp/body change proves a new observation, not provider reception.
    Socket absence alone is UNKNOWN. Stalled ACKs require pending transport work.
    """
    def __init__(self):self.previous=None

    def sample(self,value,now):
        previous=self.previous;self.previous=None
        try:
            if value['schema']!='stream_v3.ffmpeg_transport.v1':raise ValueError()
            generation=value['ffmpeg_generation'];stamp=value['observed_at'];count=value['ack_bytes_observed_total']
            if (not isinstance(generation,str) or not generation or not isinstance(stamp,str)
                    or not stamp or type(count) is not int or count<0):raise ValueError()
            digest=hashlib.sha256(json.dumps(value,sort_keys=True,allow_nan=False).encode()).hexdigest()
            current={'generation':generation,'stamp':stamp,'bytes':count,'digest':digest,'at':now}
            # A same-generation ACK increase is itself a new observation, even
            # if a broken wall clock repeats the timestamp. Never subtract
            # counters across generations or trust a frozen body's state flag.
            if (previous and generation==previous['generation']
                    and 0<now-previous['at']<=45 and count>previous['bytes']):
                self.previous=current
                return {'status':'FLOWING','reason':'local_ack_advancing'}
            # Never rejuvenate an unchanged export, including repeated file copies.
            if previous and (digest==previous['digest'] or stamp==previous['stamp']):
                self.previous=previous
                return {'status':'UNKNOWN','reason':'local_transport_stale'}
            self.previous=current
            if (not previous or generation!=previous['generation'] or not 0<now-previous['at']<=45):
                return {'status':'UNKNOWN','reason':'local_transport_baseline'}
            delta=count-previous['bytes']
            if delta<0:return {'status':'UNKNOWN','reason':'local_transport_counter_regressed'}
            if delta>0:return {'status':'FLOWING','reason':'local_ack_advancing'}
            pending=any(type(value.get('tcp_metrics',{}).get(k)) is int and value['tcp_metrics'][k]>0
                        for k in ('notsent','unacked'))
            if value.get('state')=='TCP_ACK_STALLED' and pending:
                return {'status':'STOPPED','reason':'local_transport_stalled'}
            if value.get('fifo_recovering') is True:
                return {'status':'STOPPED','reason':'local_fifo_recovering'}
            return {'status':'UNKNOWN','reason':'local_transport_unconfirmed'}
        except (KeyError,TypeError,ValueError,OverflowError):
            return {'status':'UNKNOWN','reason':'local_transport_unavailable'}


class ProgressDeadline:
    """One local-boottime deadline since the last verified downstream progress.

    ACKs, generations and observation labels cannot renew YouTube evidence.
    Maintenance starts a new observation window; there is no transition grace.
    """
    AFTER = 90

    def __init__(self):
        self.started = None
        self.last_route_progress = None

    def observe(self, now, *, route_progress=False, inhibited=False):
        if self.started is None or inhibited:
            self.started = now
            self.last_route_progress = None
        if route_progress and not inhibited:self.last_route_progress = now
        since = self.last_route_progress if self.last_route_progress is not None else self.started
        age = max(0., now - since)
        return {'unconfirmed_for_sec':round(age,3),
                'route_progress_confirmed':route_progress is True and not inhibited,
                'after_sec':self.AFTER, 'expired':age >= self.AFTER}


class ClockDiagnostics:
    """UTC diagnostics only. Gaps cannot clear a known clock incident.

    Media health and the start/stop windows never read this timer. Arena keeps
    its own secondary-recovery veto and the existing notifier owns delivery.
    """
    def __init__(self):self.fault_since = None

    def observe(self, diagnostic, now, *, inhibited=False):
        status = diagnostic.get('clock',{}).get('status')
        if inhibited or status == 'VERIFIED':self.fault_since = None
        elif status == 'UNKNOWN' and self.fault_since is None:self.fault_since = now
        return {'status':'VERIFIED' if status == 'VERIFIED' else 'FAULT' if status == 'UNKNOWN' else 'UNAVAILABLE',
                'fault_for_sec':0. if self.fault_since is None else round(max(0.,now-self.fault_since),3)}


class CauseBook:
    """Small durable current-state bookmark; prior transitions go to existing logs.

    No action authorization, recovery budget or health window is persisted here.
    Missing/corrupt existing state must not create a replacement incident silently.
    """
    def __init__(self,path,boot):
        self.path=Path(path);self.boot=boot
        if self.path.exists():
            raw=self.path.read_bytes()
            if len(raw)>16384:raise ValueError('cause_state_oversized')
            self.state=json.loads(raw)
            if self.state.get('schema')!='stream.route_cause_state.v1':raise ValueError('cause_state_invalid')
            if type(self.state.get('sequence')) is not int or self.state['sequence']<0:raise ValueError('cause_state_invalid')
            current=self.state.get('current')
            if current is not None and (not isinstance(current,dict) or not isinstance(current.get('id'),str)
                    or len(current['id'])!=32 or any(c not in '0123456789abcdef' for c in current['id'])
                    or type(current.get('sequence')) is not int or current['sequence']>self.state['sequence']):
                raise ValueError('cause_state_invalid')
        else:self.state={'schema':'stream.route_cause_state.v1','sequence':0,'current':None}

    def update(self,domain,reason,replicas,now):
        if domain not in DOMAINS:raise ValueError('cause_domain_invalid')
        current=self.state.get('current')
        problem=domain not in ('HEALTHY','INITIALIZING') or replicas not in (0,None)
        event=None
        if problem:
            if current is None:
                self.state['sequence']+=1
                current={'id':uuid.uuid4().hex,'sequence':self.state['sequence'],'opened_boot':self.boot,
                         'origin':domain if domain!='HEALTHY' else 'UNATTRIBUTED_FALLBACK','origin_reason':reason,'fallback_started':False,'clear_since':None,
                         'clear_boot':None}
                self.state['current']=current;event='cause_opened'
            current['domain']=domain;current['reason']=reason;current['clear_since']=None
        elif current and domain=='HEALTHY' and replicas==0:
            # The settling window covers one existing API freshness period.
            if current.get('clear_since') is None or current.get('clear_boot')!=self.boot:
                current['clear_since']=now;current['clear_boot']=self.boot;event='cause_resolving'
            if now-current['clear_since']>=180:
                self.state['last_closed_id']=current['id'];self.state['current']=None
                event='cause_closed';current=None
        elif current:
            current['clear_since']=None
        self.save()
        return {'cause_id':current['id'] if current else None,'assessment':domain,'reason':reason,
                'cause_active':current is not None,'fallback_active':None if replicas is None else replicas>0,
                'secondary_recovery_blocked':domain!='HEALTHY' or current is not None or replicas!=0,
                'cause_event':event}

    def dispatched(self,action,result):
        current=self.state.get('current')
        if current:
            current['last_action']=action;current['last_action_result']=result
            if action=='START' and result=='SUCCEEDED':current['fallback_started']=True
        self.save()

    def save(self):
        raw=json.dumps(self.state,allow_nan=False,separators=(',',':')).encode()
        if len(raw)>16384:raise ValueError('cause_state_oversized')
        tmp=self.path.with_suffix('.tmp')
        with tmp.open('wb') as handle:
            os.fchmod(handle.fileno(),0o600);handle.write(raw);handle.flush();os.fsync(handle.fileno())
        os.replace(tmp,self.path)
        fd=os.open(self.path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
