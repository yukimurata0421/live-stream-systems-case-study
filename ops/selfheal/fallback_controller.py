#!/usr/bin/env python3
"""Control direct backup from bound progress and inhibit effects during planned maintenance."""
from __future__ import annotations

import json
import importlib.util
import secrets
import argparse
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone
from route_assessment import CauseBook, TransportWitness, ProgressDeadline, ClockDiagnostics, domain_for_missing
from maintenance_state import ROOT as MAINTENANCE_DEFAULT_ROOT, guard as maintenance_guard, session_lock

_spec = importlib.util.spec_from_file_location('stream_clock_contract', Path(__file__).with_name('clock_contract.py'))
clock = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = clock
_spec.loader.exec_module(clock)

OCI_API = os.environ.get('OCI_MEDIAMTX_API', '').rstrip('/')
NAMESPACE = 'stream-v3'
FALLBACK = os.environ.get('FALLBACK_DEPLOYMENT', 'stream-v3-youtube-backup')
INTERVAL = 15
START_AFTER = 30
STOP_AFTER = 300
FACTS_URL = os.environ.get('OCI_ROUTE_FACTS_URL', '')
EXPECTED_HOST_ID = os.environ.get('OCI_ROUTE_HOST_ID', 'relay-host')
EXPECTED_MACHINE = os.environ.get('OCI_ROUTE_MACHINE_SHA256', '')
EXPECTED_DESTINATION = os.environ.get('OCI_YOUTUBE_DESTINATION_SHA256', '')
MAX_SAMPLE_GAP = 45
MAX_FACT_AGE = 20
STATUS_PATH = Path(os.environ.get('FALLBACK_STATUS_PATH', '/run/stream-v3-fallback-controller/status.json'))
MAINTENANCE_ROOT = Path(os.environ.get('STREAM_MANUAL_MAINTENANCE_ROOT', str(MAINTENANCE_DEFAULT_ROOT)))
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


CAUSE_STATE = Path(os.environ.get('FALLBACK_CAUSE_STATE', '/var/lib/stream-v3-fallback-causality/current.json'))
TRANSPORT_STATE = Path('/run/stream-v3-transport-export/ffmpeg-transport.json')


class ObservationRejected(ValueError):
    """Keep fixed reasons without external error text or destination URLs."""


def require(condition, reason):
    if not condition:
        raise ObservationRejected(reason)


def check_timestamp(value, now, name, diagnostic, clock_model):
    require(isinstance(value, str) and len(value) <= 40, 'timestamp_invalid')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(stamp.tzinfo is not None, 'timestamp_invalid')
    if name == 'source':
        diagnostic['source_observed_at'] = value
    age = now - stamp.timestamp()
    diagnostic[name + '_age_seconds'] = round(age, 3)
    low, high = clock_model.age_bounds(stamp.timestamp())
    diagnostic[name + '_age_bounds_seconds'] = [round(low, 6), round(high, 6)]
    require(high >= -clock.EPSILON, name + '_future')
    require(high <= MAX_FACT_AGE, name + '_stale')


def counter(value):
    return type(value) is int and value >= 0


def copy_route_sample(value, *, machine, destination, received, source_age, diagnostic=None):
    """Parse boot-bound fresh counters; UTC labels never decide media health."""
    diagnostic = diagnostic if diagnostic is not None else {}
    diagnostic.update(health='UNKNOWN', reason='facts_invalid')
    try:
        negative=(value.get('relay_availability') is not None or value.get('copy_availability') is not None)
        require(type(source_age) in (int,float) and math.isfinite(source_age)
                and 0<=source_age<=MAX_FACT_AGE,'source_freshness_unavailable')
        raw,cp=value['relay'],value['youtube_copy']
        require(value['schema']=='stream_v3.route_facts.v1' and raw['schema']=='stream.relay_raw.v1','schema_invalid')
        require(raw['host_id']==EXPECTED_HOST_ID and len(machine)==64 and raw['machine_id_sha256']==machine
                and len(destination)==64,'host_binding_mismatch')
        require(isinstance(raw['observed_at'],str) and len(raw['observed_at'])<=64,'facts_invalid')
        diagnostic['source_observed_at']=raw['observed_at']
        diagnostic['source_age_bounds_seconds']=[0.,source_age]
        diagnostic['negative_evidence_timing' if negative else 'media_evidence_timing']='bound_boottime'
        availability=value.get('relay_availability')
        if availability is not None:
            require(availability=={'schema':'stream_v3.relay_availability.v1',
                                    'state':'STOPPED','reason':'unit_not_running'},'relay_availability_invalid')
            require(raw['relay']=={'active':False,'pid':0} and raw['relay']['active'] is False
                    and type(raw['relay']['pid']) is int and cp is None
                    and raw['input']=={'available':False,'ready':False,'online':False,'content_state':'OFFLINE',
                                      'connection_id':None,'bytes':0}
                    and all(raw['input'][key] is False for key in ('available','ready','online'))
                    and type(raw['input']['bytes']) is int and raw['destinations']=={}
                    and isinstance(raw['boot_id'],str) and bool(raw['boot_id'])
                    and type(raw['monotonic_seconds']) in (int,float)
                    and math.isfinite(raw['monotonic_seconds']) and raw['monotonic_seconds']>=0,
                    'relay_availability_invalid')
            diagnostic.update(health='UNHEALTHY',reason='relay_unit_not_running')
            return {'ready':False,'bytes_received':0,'source_monotonic':raw['monotonic_seconds'],
                    'received_monotonic':received,'relay_unavailable':True}
        copy_availability=value.get('copy_availability')
        if copy_availability is not None:
            require(copy_availability['schema']=='stream_v3.copy_availability.v1'
                    and copy_availability['destination_fingerprint']==destination
                    and raw['relay']['active'] is True and counter(raw['relay']['pid'])
                    and raw['relay']['pid']>0,'copy_availability_invalid')
            state=copy_availability['state'];pid=copy_availability['pid']
            if state=='STOPPED':
                require(type(pid) is int and pid==0 and cp is None
                        and copy_availability['reason']=='unit_not_running','copy_availability_invalid')
                reason='copy_unit_not_running'
            else:
                age=copy_availability['unobserved_for_sec']
                require(state=='STALLED' and type(age) in (int,float) and math.isfinite(age) and age>20
                        and copy_availability['reason']=='heartbeat_not_advancing'
                        and counter(pid) and pid>0 and isinstance(cp,dict)
                        and cp['schema']=='stream_v3.youtube_forward.v1' and type(cp['pid']) is int and cp['pid']==pid
                        and cp['boot_id']==raw['boot_id'] and cp['destination_fingerprint']==destination
                        and counter(cp['session']) and cp['session']>0
                        and type(copy_availability['session']) is int and cp['session']==copy_availability['session']
                        and cp['state'] in ('FLOWING','CONNECTING','ERROR')
                        and all(counter(cp[k]) for k in ('output_bytes','video_packets','audio_packets')),
                        'copy_availability_invalid')
                reason='copy_heartbeat_stalled'
            require(isinstance(raw['boot_id'],str) and bool(raw['boot_id'])
                    and type(raw['monotonic_seconds']) in (int,float) and math.isfinite(raw['monotonic_seconds'])
                    and raw['monotonic_seconds']>=0,'copy_availability_invalid')
            diagnostic.update(health='UNHEALTHY',reason=reason)
            return {'ready':False,'bytes_received':0,'received_monotonic':received,'copy_unavailable':reason}
        require(isinstance(cp,dict),'copy_facts_missing')
        observation=value.get('copy_observation');copy_new=False;mono_fresh=False
        if observation is not None:
            age=observation['age_seconds']
            require(observation['schema']=='stream_v3.copy_observation.v1'
                    and type(age) in (int,float) and math.isfinite(age) and age>=0
                    and type(observation['confirmed']) is bool and type(observation['new_observation']) is bool,
                    'copy_observation_invalid')
            mono_fresh=observation['confirmed'] and age+max(0,diagnostic['source_age_bounds_seconds'][1])<=MAX_FACT_AGE
            copy_new=mono_fresh and observation['new_observation']
        require(mono_fresh,'copy_freshness_unavailable')
        require(all(isinstance(cp[k],str) and len(cp[k])<=40 for k in ('updated_at','last_progress_at')),'facts_invalid')
        require(cp['schema']=='stream_v3.youtube_forward.v1' and cp['boot_id']==raw['boot_id']
                and cp['destination_fingerprint']==destination,'copy_binding_mismatch')
        require(cp['state'] in ('FLOWING','CONNECTING','ERROR'),'facts_invalid')
        require(counter(cp['pid']) and cp['pid']>0 and counter(cp['session']) and cp['session']>0,'copy_identity_invalid')
        incoming,yt=raw['input'],raw['destinations']['youtube'];ident=f"youtube-forward-{cp['pid']}-{cp['session']}"
        require(yt['id']==ident and yt['state'] in ('forwarding','unknown','idle','connecting'),'copy_binding_mismatch')
        require(isinstance(raw['boot_id'],str) and bool(raw['boot_id']) and counter(raw['relay']['pid'])
                and raw['relay']['pid']>0 and type(raw['relay']['active']) is bool,'facts_invalid')
        require(type(incoming['ready']) is bool and type(incoming['available']) is bool,'facts_invalid')
        ready=raw['relay']['active'] and incoming['ready'] and incoming['available']
        live=incoming.get('content_state','LIVE')=='LIVE' and incoming.get('online',True) is True
        connection=incoming['connection_id']
        require((isinstance(connection,str) and bool(connection)) or not ready or not live,'facts_invalid')
        require(all(counter(v) for v in (incoming['bytes'],cp['output_bytes'],cp['video_packets'],cp['audio_packets'])),'facts_invalid')
        require(type(raw['monotonic_seconds']) in (int,float) and math.isfinite(raw['monotonic_seconds'])
                and raw['monotonic_seconds']>=0,'facts_invalid')
        diagnostic.update(health='UNKNOWN',reason='baseline_required')
        return {'ready':ready and live,'bytes_received':incoming['bytes'],
                'identity':(raw['boot_id'],raw['relay']['pid'],connection,ident,destination),
                'source_monotonic':raw['monotonic_seconds'],'received_monotonic':received,
                'received_boottime':clock.mono(),
                'copy_updated_at':cp['updated_at'],'copy_observation_new':copy_new,
                'copy_observation_fresh':mono_fresh,
                'youtube':{'state':yt['state'],'error':cp['state']!='FLOWING','outbound':cp['output_bytes'],
                           'video_packets':cp['video_packets'],'audio_packets':cp['audio_packets']}}
    except ObservationRejected as error:diagnostic['reason']=str(error)
    except clock.ClockRejected as error:
        diagnostic.update(reason='clock_unavailable',clock={'status':'UNKNOWN','reason':str(error)})
    except (KeyError,ValueError,TypeError,AttributeError,OverflowError):pass
    return None


@dataclass
class Controller:
    unhealthy_since: float | None = None
    healthy_since: float | None = None
    last: dict | None = None
    domain: str = 'INITIALIZING'
    health: str = 'UNKNOWN'
    reason: str = 'baseline_required'
    healthy_resets_total: int = 0
    last_reset_at: str | None = None
    last_reset_reason: str | None = None
    route_progress: bool = False
    observation_gap: bool = False
    transport_conflict: bool = False

    def healthy(self, sample: dict | None, observation_reason='observation_unavailable') -> bool:
        prev=self.last
        resumed=self.observation_gap
        # Keep a counter baseline through brief missing samples. Its identity
        # and remote/local monotonic gap are revalidated before comparison.
        if sample is not None:self.last=sample
        self.observation_gap=sample is None
        self.route_progress=False
        self.transport_conflict=False
        self.health='UNKNOWN';self.domain=domain_for_missing(observation_reason)
        self.reason=observation_reason if sample is None else 'baseline_required'
        if sample is None:return False
        self.domain='INITIALIZING'
        if sample.get('ready') is False:
            self.health,self.domain,self.reason='UNHEALTHY','MEDIA_FAILURE',(sample.get('copy_unavailable') or ('relay_unit_not_running' if sample.get('relay_unavailable') else 'input_not_live'))
            return False
        if prev is None:return False
        yt=sample.get('youtube') or {};before=prev.get('youtube') or {}
        if 'identity' in sample:
            if sample['identity']!=prev.get('identity'):
                self.domain,self.reason='OBSERVATION_LOST','connection_changed';return False
            if not 0<sample['source_monotonic']-prev.get('source_monotonic',sample['source_monotonic'])<=MAX_SAMPLE_GAP:
                self.domain,self.reason='OBSERVATION_LOST','sample_gap';return False
            if not 0<sample.get('received_boottime',sample['received_monotonic'])-prev.get('received_boottime',prev['received_monotonic'])<=MAX_SAMPLE_GAP:
                self.domain,self.reason='OBSERVATION_LOST','sample_gap';return False
            if ('copy_updated_at' in sample and sample['copy_updated_at']==prev.get('copy_updated_at')
                    and not sample.get('copy_observation_new')
                    and not (sample.get('copy_observation_fresh')
                             and all(counter(yt.get(k)) and counter(before.get(k)) and yt[k]>before[k]
                                     for k in ('outbound','video_packets','audio_packets')))):
                self.domain,self.reason='OBSERVATION_LOST','copy_stale';return False
        checks=[(sample.get('ready') is True,'input_not_ready'),
                (counter(sample.get('bytes_received')) and counter(prev.get('bytes_received'))
                 and sample['bytes_received']>prev['bytes_received'],'input_not_advancing')]
        for key,reason in [('outbound','output_not_advancing'),('video_packets','video_not_advancing'),('audio_packets','audio_not_advancing')]:
            if key not in yt and key not in before and key in ('video_packets','audio_packets'):continue
            if not counter(yt.get(key)) or not counter(before.get(key)):
                self.domain,self.reason='OBSERVATION_LOST','counter_invalid';return False
            if yt[key]<before[key]:
                self.domain,self.reason='OBSERVATION_LOST','counter_regressed';return False
            checks.append((yt[key]>before[key],reason))
        for passed,reason in checks:
            if not passed:
                self.health,self.domain,self.reason='UNHEALTHY','MEDIA_FAILURE',reason;return False
        if yt.get('state')!='forwarding' or yt.get('error'):
            self.health,self.domain,self.reason='UNHEALTHY','MEDIA_FAILURE','output_not_forwarding';return False
        self.route_progress=True
        if resumed:
            self.domain,self.reason='INITIALIZING','observation_resumed';return False
        self.health,self.domain,self.reason='HEALTHY','HEALTHY','progress_confirmed'
        return True

    def combine_transport(self,witness):
        self.transport_conflict=witness.get('status')=='STOPPED' and self.route_progress
        # Fresh complete-route progress outweighs a contradictory local export.
        # Without downstream proof, the local stall still confirms a failure.
        if witness.get('status')=='STOPPED' and not self.route_progress:
            self.health,self.domain,self.reason='UNHEALTHY','MEDIA_FAILURE',witness['reason']
            self.route_progress=False
        return self.health=='HEALTHY'

    def decide(self, now: float, healthy: bool, fallback_replicas: int | None, *, maintenance=None, uncertainty=None) -> str | None:
        'Return START, STOP or None; unknown replicas inhibit changes.'
        if maintenance and maintenance['inhibited']:
            # Raw health remains visible; planned source loss must not start backup.
            # Do not carry a pre-maintenance stability/failure window across release.
            self.healthy_since = self.unhealthy_since = None
            return None
        if not healthy and self.domain!='MEDIA_FAILURE':
            if self.healthy_since is not None:
                self.healthy_resets_total+=1;self.last_reset_at=datetime.now(timezone.utc).isoformat();self.last_reset_reason=self.reason
            self.healthy_since=self.unhealthy_since=None
            if uncertainty and uncertainty['expired']:
                self.reason='youtube_observation_timeout'
                if fallback_replicas==0:
                    return 'START'
            return None
        if healthy:
            self.unhealthy_since = None
            self.healthy_since = self.healthy_since if self.healthy_since is not None else now
        else:
            if self.healthy_since is not None:
                self.healthy_resets_total += 1
                self.last_reset_at = datetime.now(timezone.utc).isoformat()
                self.last_reset_reason = self.reason
            self.healthy_since = None
            self.unhealthy_since = self.unhealthy_since if self.unhealthy_since is not None else now
        if fallback_replicas is None:
            return None
        if fallback_replicas == 0 and not healthy and now - self.unhealthy_since >= START_AFTER:
            return 'START'
        if fallback_replicas >= 1 and healthy and now - self.healthy_since >= STOP_AFTER:
            return 'STOP'
        return None


def get(path: str):
    return get_url(OCI_API + path)


def get_url(url):
    try:
        with _OPENER.open(url, timeout=3) as r:
            raw = r.read(16385)
    except urllib.error.HTTPError as error:
        error.close()
        raise
    if len(raw) > 16384:
        raise ValueError('oversized_facts')
    return json.loads(raw)


def provider(item: dict) -> str | None:
    """Classify the provider by hostname without returning or recording stream destinations."""
    host = urllib.parse.urlsplit(str((item.get('conf') or {}).get('dest') or '')).hostname or ''
    if host == 'youtube.com' or host.endswith('.youtube.com'):
        return 'youtube'
    if host.endswith('.live-video.net') or host.endswith('.twitch.tv'):
        return 'twitch'
    return None


def youtube_item(items: list) -> dict | None:
    """Return exactly one YouTube destination without relying on its array position.

    A temporarily removed destination can shift Twitch into the first position.
    """
    matches = [i for i in items if provider(i) == 'youtube']
    return matches[0] if len(matches) == 1 else None


def observe(diagnostic=None) -> dict | None:
    diagnostic = diagnostic if diagnostic is not None else {}
    diagnostic.clear()
    diagnostic.update(health='UNKNOWN', reason='observation_unavailable')
    try:
        if FACTS_URL:
            started = clock.capture()
            nonce = secrets.token_hex(16)
            diagnostic['http_started_at'] = datetime.now(timezone.utc).isoformat()
            try:
                value = get_url(FACTS_URL + '?nonce=' + nonce)
            finally:
                diagnostic['http_completed_at'] = datetime.now(timezone.utc).isoformat()
                completed = clock.capture()
                diagnostic['http_elapsed_seconds'] = round(completed['mono'] - started['mono'], 6)
            # UTC remains observable, but cannot accept/reject media facts.
            try:
                model = clock.estimate(value.get('clock', {}), nonce, started, completed,
                                       machine=EXPECTED_MACHINE, boot_id=value['relay']['boot_id'])
                diagnostic['clock'] = {'status':'VERIFIED', **model.diagnostic()}
                for section,field,name in (('relay','observed_at','source'),
                                          ('youtube_copy','updated_at','copy'),
                                          ('youtube_copy','last_progress_at','progress')):
                    if value.get(section) is not None:
                        check_timestamp(value[section][field],time.time(),name,diagnostic,model)
            except (clock.ClockRejected,ObservationRejected) as error:
                diagnostic['clock']={'status':'UNKNOWN','reason':str(error)}
            except (KeyError,ValueError,TypeError):
                diagnostic['clock']={'status':'UNKNOWN','reason':'clock_labels_invalid'}
            age=clock.monotonic_source_age(value['clock'],nonce,started,completed,
                machine=EXPECTED_MACHINE,boot_id=value['relay']['boot_id'],
                source_mono=value['relay']['observed_boottime_seconds'],max_age=MAX_FACT_AGE)
            return copy_route_sample(value,machine=EXPECTED_MACHINE,
                destination=EXPECTED_DESTINATION,received=clock.mono(),source_age=age,diagnostic=diagnostic)
        path = get('/v3/paths/get/relay')
        items = get('/v3/paths/forward-dests/list?path=relay').get('items', [])
    except clock.ClockRejected as error:
        diagnostic.update(reason='clock_unavailable', clock={'status': 'UNKNOWN', 'reason': str(error)})
        return None
    except urllib.error.HTTPError as error:
        diagnostic['reason'] = 'facts_http_unavailable'
        diagnostic['http_status'] = error.code
        return None
    except (TimeoutError, socket.timeout):
        diagnostic['reason'] = 'facts_timeout'
        return None
    except urllib.error.URLError as error:
        diagnostic['reason'] = 'facts_timeout' if isinstance(error.reason, TimeoutError) else 'facts_transport_error'
        return None
    except Exception:
        diagnostic['reason'] = 'facts_invalid'
        return None
    yt = youtube_item(items)
    return {'ready': path.get('ready') is True and path.get('online', True) is True
                     and bool((path.get('source') or {}).get('id')),
            'bytes_received': int(path.get('bytesReceived') or path.get('inboundBytes') or 0),
            'received_monotonic': clock.mono(), 'received_boottime':clock.mono(),
            'youtube': None if yt is None else {'state': yt.get('state'), 'error': bool(yt.get('lastError')),
                                                 'outbound': int(yt.get('outboundBytes') or 0)}}


def kubectl(*args: str) -> str:
    return subprocess.run(['kubectl', '-n', NAMESPACE, *args], capture_output=True, text=True, timeout=20, check=True).stdout


def replicas() -> int | None:
    try:
        return int(kubectl('get', 'deploy', FALLBACK, '-o', 'jsonpath={.spec.replicas}'))
    except Exception:
        return None


def log(event: str, **values):
    print(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'event': event, **values}), flush=True)


def status_snapshot(controller, diagnostic, now, current, action=None, action_result=None, *, maintenance=None):
    out = {'schema': 'stream_v3.fallback_controller.v1', 'observed_at': datetime.now(timezone.utc).isoformat(),
           'observed_monotonic':clock.mono(),'boot_id':clock.identity()['boot_id'],
           'assessment':controller.domain,'automatic_switch_eligible':controller.domain=='MEDIA_FAILURE' or controller.reason in ('observation_no_progress_timeout','youtube_observation_timeout'),
           'health': controller.health, 'reason': controller.reason,
           'transport_evidence_conflict':controller.transport_conflict,
           'fallback_replicas': current, 'healthy_for_sec': round(now - controller.healthy_since, 3) if controller.healthy_since is not None else 0,
           'unhealthy_for_sec': round(now - controller.unhealthy_since, 3) if controller.unhealthy_since is not None else 0,
           'start_after_sec': START_AFTER, 'stop_after_sec': STOP_AFTER,
           'healthy_resets_total': controller.healthy_resets_total, 'last_reset_at': controller.last_reset_at,
           'last_reset_reason': controller.last_reset_reason, 'action': action, 'action_result': action_result}
    out['maintenance_guard_supported'] = True
    out['maintenance_root'] = str(MAINTENANCE_ROOT)
    out['maintenance'] = maintenance if maintenance is not None else maintenance_guard(MAINTENANCE_ROOT)
    out['controller_pid'] = os.getpid()
    for key in ('source_observed_at', 'source_age_seconds', 'copy_age_seconds', 'progress_age_seconds', 'http_status',
                'http_started_at', 'http_completed_at', 'http_elapsed_seconds', 'clock', 'last_known',
                'source_age_bounds_seconds', 'copy_age_bounds_seconds', 'progress_age_bounds_seconds',
                'negative_evidence_timing','media_evidence_timing','clock_diagnostic'):
        out[key] = diagnostic.get(key)
    return out


def write_status(value, path=None):
    path = path if path is not None else STATUS_PATH
    raw = json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
    if len(raw) > 8192:
        raise ValueError('status_too_large')
    temporary = path.with_suffix('.tmp')
    try:
        with temporary.open('wb') as handle:
            os.fchmod(handle.fileno(), 0o644)
            handle.write(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def scale_if_allowed(action, root=None):
    """Recheck under the same lock as begin/complete, immediately before kubectl."""
    root = root if root is not None else MAINTENANCE_ROOT
    def scale():
        if maintenance_guard(root)['inhibited']:
            return 'INHIBITED'
        kubectl('scale', 'deploy', FALLBACK, '--replicas=' + ('1' if action == 'START' else '0'))
        return 'SUCCEEDED'
    try:
        if root.exists():
            with session_lock(root, shared=True):
                return scale()
        return scale()
    except (BlockingIOError, FileNotFoundError):
        return 'INHIBITED'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--observe-once', action='store_true')
    args = parser.parse_args()
    if args.observe_once:
        diagnostic = {}
        value = observe(diagnostic)
        print(json.dumps({'sample': value, 'diagnostic': diagnostic}))
        return 0 if value is not None else 1
    c = Controller()
    witness=TransportWitness()
    deadline=ProgressDeadline()
    clock_diagnostics=ClockDiagnostics()
    book=CauseBook(CAUSE_STATE,clock.identity()['boot_id'])
    log('controller_started', fallback=FALLBACK, start_after=START_AFTER, stop_after=STOP_AFTER,
        observation='copy_route_facts' if FACTS_URL else 'mediamtx_direct')
    previous_state, last_logged = None, 0
    while True:
        diagnostic = {}
        value = observe(diagnostic)
        current = replicas()
        if value is not None and clock.mono() - value.get('received_boottime',value['received_monotonic']) > 5:
            value = observe(diagnostic)
        now = clock.mono()
        ok = c.healthy(value, diagnostic['reason'])
        try:
            with TRANSPORT_STATE.open('rb') as handle:raw_transport=handle.read(16385)
            if len(raw_transport)>16384:raise ValueError('transport_oversized')
            local=witness.sample(json.loads(raw_transport),clock.mono())
        except (OSError,ValueError,TypeError):local={'status':'UNKNOWN','reason':'local_transport_unavailable'}
        ok=c.combine_transport(local)
        now=clock.mono()
        maintenance = maintenance_guard(MAINTENANCE_ROOT)
        uncertainty=deadline.observe(now,route_progress=c.route_progress,inhibited=maintenance['inhibited'])
        diagnostic['clock_diagnostic']=clock_diagnostics.observe(diagnostic,now,inhibited=maintenance['inhibited'])
        action = c.decide(now, ok, current, maintenance=maintenance,uncertainty=uncertainty)
        causal=book.update(c.domain,c.reason,current,now)
        causal['secondary_recovery_blocked'] |= diagnostic['clock_diagnostic']['status']!='VERIFIED'
        causal['transport_witness']=local
        causal['progress_deadline']=uncertainty
        if causal.get('cause_event'):log(causal['cause_event'],**{k:v for k,v in causal.items() if k!='cause_event'})
        action_result = None
        if action:
            target = '1' if action == 'START' else '0'
            try:
                action_result = scale_if_allowed(action)
                log('fallback_' + action.lower() if action_result == 'SUCCEEDED' else 'fallback_action_inhibited', replicas=target,
                    healthy_for_sec=round(now - c.healthy_since, 3) if c.healthy_since is not None else 0)
            except Exception as error:
                action_result = 'FAILED'
                log('fallback_scale_failed', action=action, error=type(error).__name__)
        if action:book.dispatched(action,action_result)
        status = status_snapshot(c, diagnostic, now, current, action, action_result, maintenance=maintenance)
        status['causality']=causal
        state = (c.health, c.reason, current, maintenance['phase'], maintenance['overdue'])
        if state != previous_state or action or now - last_logged >= 60:
            log('oci_path_health', healthy=ok, **{k: v for k, v in status.items() if k != 'schema'})
            previous_state, last_logged = state, now
        try:
            write_status(status)
        except (OSError, ValueError):
            log('controller_status_write_failed')
        time.sleep(INTERVAL)


if __name__ == '__main__':
    sys.exit(main())
