#!/usr/bin/env python3
"""固定した既存relayのTwitchだけを、有限maintenance窓で日次更新する。"""
from __future__ import annotations

import argparse
import dataclasses
import fcntl
import hashlib
import json
import os
import signal
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

from twitch_session import (Action, Binding, BroadcastFacts, BroadcastStatus, Clock, Command,
    Outcome, Phase, RenewalPolicy, RenewalState, RouteContext, Session, WriterFacts, WriterStatus,
    advance, next_daily_renewal_at, parse_get_streams)
from twitch_session import twitch_forward_update


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def pack(value):
    if dataclasses.is_dataclass(value):
        return {k:pack(v) for k,v in value.__dict__.items()}
    if isinstance(value,datetime): return value.isoformat()
    if isinstance(value,dict):return {k:pack(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [pack(v) for v in value]
    return value


def state_from(value):
    v=dict(value);v['phase']=Phase(v['phase'])
    for k in ['began_at','start_requested_at']:
        if v.get(k):v[k]=datetime.fromisoformat(v[k])
    if v.get('session'):
        v['session']=Session(v['session']['stream_id'],datetime.fromisoformat(v['session']['started_at']))
    if v.get('binding'):v['binding']=Binding(**v['binding'])
    for k in ['last_clock','stopped_clock']:
        if v.get(k):v[k]=Clock(datetime.fromisoformat(v[k]['utc']),v[k]['elapsed_seconds'],v[k]['boot_id'])
    return RenewalState(**v)


def outcome_from(value):
    return Outcome(Action(value['action']),value['request_id'],Binding(**value['binding']),
                   value['status'],datetime.fromisoformat(value['observed_at']))


class Journal:
    def __init__(self,path,approval):
        self.lock=os.open(path.parent/'owner.lock',os.O_RDWR|os.O_CREAT,0o600)
        fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.db=sqlite3.connect(path,isolation_level=None)
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL,body TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS outbox(request_id TEXT PRIMARY KEY,command TEXT NOT NULL,phase TEXT NOT NULL,attempts INTEGER NOT NULL CHECK(attempts<=1),outcome TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS approval(id INTEGER PRIMARY KEY CHECK(id=1),digest TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS writer(id INTEGER PRIMARY KEY CHECK(id=1),generation INTEGER NOT NULL,handler_id TEXT,legacy_quiesced INTEGER NOT NULL)')
        row=self.db.execute('SELECT digest FROM approval WHERE id=1').fetchone()
        if row and row[0]!=digest(approval):raise ValueError('DAILY_APPROVAL_CHANGED')
        self.db.execute('INSERT OR IGNORE INTO approval VALUES(1,?)',(digest(approval),))
        self.db.execute('INSERT OR IGNORE INTO state VALUES(1,0,?)',(canonical(pack(RenewalState())),))
        self.db.execute('INSERT OR IGNORE INTO writer VALUES(1,1,?,0)',(approval.get('twitch_handler_id'),))

    def load(self):
        return state_from(json.loads(self.db.execute('SELECT body FROM state WHERE id=1').fetchone()[0]))

    def commit(self,old,new,command=None):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            changed=self.db.execute('UPDATE state SET revision=?,body=? WHERE id=1 AND revision=?',
                (new.revision,canonical(pack(new)),old.revision)).rowcount
            if changed!=1:raise ValueError('DAILY_STATE_CAS_FAILED')
            if command:
                self.db.execute('INSERT INTO outbox VALUES(?,?,?,0,NULL)',
                    (command.request_id,canonical(pack(command)),'PENDING'))
            self.db.execute('COMMIT')
        except Exception:
            self.db.execute('ROLLBACK');raise

    def dispatch_once(self,command):
        changed=self.db.execute("UPDATE outbox SET phase='DISPATCHING',attempts=1 WHERE request_id=? AND phase='PENDING' AND attempts=0",(command.request_id,)).rowcount
        if changed!=1:raise ValueError('DAILY_EFFECT_REPLAY_BLOCKED')

    def finish(self,command,outcome,handler_id=None):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            changed=self.db.execute("UPDATE outbox SET phase='OBSERVED',outcome=? WHERE request_id=? AND phase='DISPATCHING' AND attempts=1",
                (canonical(pack(outcome)),command.request_id)).rowcount
            if changed!=1:raise ValueError('DAILY_OUTCOME_CAS_FAILED')
            if command.action==Action.START_TWITCH:
                if not handler_id:raise ValueError('DAILY_NEW_WRITER_ID_REQUIRED')
                self.db.execute('UPDATE writer SET generation=?,handler_id=? WHERE id=1',
                    (command.binding.writer_generation+1,handler_id))
            else:self.db.execute('UPDATE writer SET legacy_quiesced=1 WHERE id=1')
            self.db.execute('COMMIT')
        except Exception:
            self.db.execute('ROLLBACK');raise

    def writer(self):
        return self.db.execute('SELECT generation,handler_id,legacy_quiesced FROM writer WHERE id=1').fetchone()

    def close(self):
        self.db.close();os.close(self.lock)

    def pending(self):
        return self.db.execute("SELECT COUNT(*) FROM outbox WHERE phase!='OBSERVED'").fetchone()[0]

    def outcome(self,state):
        suffix=':stop' if state.phase==Phase.WAIT_STOP else ':start'
        row=self.db.execute('SELECT outcome FROM outbox WHERE request_id=?',(state.operation_id+suffix,)).fetchone()
        return outcome_from(json.loads(row[0])) if row and row[0] else None


class Helix:
    """Client credentialsはmemoryのみ。UNKNOWNをOFFLINEへ補完しない。"""
    def __init__(self,credentials,broadcaster):
        self.credentials,self.broadcaster=credentials,broadcaster
        self.token='';self.expires=0.;self.validated=0.

    def configured(self):
        values=json.loads(self.credentials.read_text())
        return bool(values.get('client_id') and values.get('client_secret'))

    @staticmethod
    def read(request):
        with urllib.request.urlopen(request,timeout=5) as response:
            raw=response.read(65537)
        if len(raw)>65536:raise ValueError('TWITCH_RESPONSE_TOO_LARGE')
        return json.loads(raw)

    def fetch(self,now):
        try:
            values=json.loads(self.credentials.read_text())
            if not values.get('client_id') or not values.get('client_secret'):
                return BroadcastFacts(self.broadcaster,now,BroadcastStatus.UNKNOWN),'CREDENTIALS_MISSING'
            elapsed=time.clock_gettime(time.CLOCK_BOOTTIME)
            if elapsed+60>=self.expires:
                payload=urllib.parse.urlencode({'client_id':values['client_id'],'client_secret':values['client_secret'],'grant_type':'client_credentials'}).encode()
                reply=self.read(urllib.request.Request('https://id.twitch.tv/oauth2/token',data=payload))
                self.token=reply['access_token'];self.expires=elapsed+int(reply['expires_in']);self.validated=0.
            if elapsed-self.validated>=3600 or not self.validated:
                v=self.read(urllib.request.Request('https://id.twitch.tv/oauth2/validate',headers={'Authorization':'OAuth '+self.token}))
                if v.get('client_id')!=values['client_id']:raise ValueError('TWITCH_CLIENT_IDENTITY_MISMATCH')
                self.validated=elapsed
            request=urllib.request.Request('https://api.twitch.tv/helix/streams?'+urllib.parse.urlencode({'user_id':self.broadcaster}),
                headers={'Client-ID':values['client_id'],'Authorization':'Bearer '+self.token})
            facts=parse_get_streams(self.read(request),broadcaster_id=self.broadcaster,observed_at=datetime.now(UTC))
            return facts,'HELIX_'+facts.status.value
        except urllib.error.HTTPError as error:
            if error.code==401:self.token='';self.expires=0.
            return BroadcastFacts(self.broadcaster,now,BroadcastStatus.UNKNOWN),'HELIX_HTTP_'+str(error.code)
        except (OSError,ValueError,KeyError,TypeError):
            return BroadcastFacts(self.broadcaster,now,BroadcastStatus.UNKNOWN),'HELIX_UNAVAILABLE'


class Relay:
    def __init__(self,approval,guard_status):
        self.approval,self.guard_status=approval,guard_status
        self.initial_youtube=None;self.youtube_last=0;self.last_yt_progress=0.
        self.initial_path=None
        self.tw_last=0;self.last_tw_progress=0.

    @staticmethod
    def api(path,body=None):
        data=None if body is None else canonical(body).encode()
        request=urllib.request.Request('http://127.0.0.1:19998'+path,data=data,method='GET' if body is None else 'PATCH',
            headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=1) as r:
            raw=r.read(65537)
        if len(raw)>65536:raise ValueError('MEDIAMTX_RESPONSE_TOO_LARGE')
        return json.loads(raw or b'{}')

    def bound(self,now):
        a=self.approval;g=json.loads(self.guard_status.read_text())
        # file/API読取中にheartbeatが更新されるため、読取後のclockで照合する。
        current_elapsed=time.clock_gettime(time.CLOCK_BOOTTIME)
        current_wall=time.time()
        if now.boot_id!=a['boot_id'] or g['instance']!=a['relay_instance'] or g['halted'] or not g['enabled']:
            raise ValueError('DAILY_RELAY_IDENTITY_OR_STATE_CHANGED')
        if not 0<=current_elapsed-g['heartbeat_elapsed']<=2 or g['lease_elapsed_deadline']<=current_elapsed+1:
            raise ValueError('DAILY_RELAY_LEASE_OR_HEARTBEAT_INVALID')
        if g['lease_expires_at']<=current_wall+1 or g['input_age']>2:
            raise ValueError('DAILY_INPUT_OR_WALL_LEASE_INVALID')
        proc=Path('/proc')/str(a['mediamtx_pid'])
        fields=(proc/'stat').read_text().rsplit(')',1)[1].split()
        if int(fields[19])!=a['mediamtx_start_ticks'] or Path(os.readlink(proc/'exe')).name!='mediamtx':
            raise ValueError('DAILY_MEDIAMTX_IDENTITY_CHANGED')
        return g

    def observe(self,now):
        g=self.bound(now)
        config=self.api('/v3/config/paths/get/relay')
        if config['forward'] not in [self.approval['forward'],self.approval['forward'][:1]]:
            raise ValueError('DAILY_DESTINATION_CONFIG_DRIFT')
        immutable={k:v for k,v in config.items() if k!='forward'}
        if digest(immutable)!=self.approval['path_configuration_sha256']:
            raise ValueError('DAILY_PATH_CONFIGURATION_DRIFT')
        rows=self.api('/v3/paths/forward-dests/list?path=relay')['items']
        if len({r['pos'] for r in rows})!=len(rows) or any(r['pos'] not in {1,2} for r in rows):
            raise ValueError('DAILY_FORWARD_POSITION_INVALID')
        yt=next((r for r in rows if r['pos']==1),None);tw=next((r for r in rows if r['pos']==2),None)
        if not yt or yt['state']!='forwarding' or yt.get('lastError'):
            raise ValueError('DAILY_YOUTUBE_NOT_FORWARDING')
        if yt['id']!=self.approval['youtube_handler_id'] or yt['created']!=self.approval['youtube_handler_created']:
            raise ValueError('DAILY_YOUTUBE_HANDLER_CHANGED')
        if yt['outboundBytes']<self.youtube_last:raise ValueError('DAILY_YOUTUBE_COUNTER_REGRESSED')
        if yt['outboundBytes']>self.youtube_last:self.last_yt_progress=now.elapsed_seconds
        self.youtube_last=yt['outboundBytes']
        if now.elapsed_seconds-self.last_yt_progress>4:raise ValueError('DAILY_YOUTUBE_PROGRESS_STALE')
        if tw and tw['outboundBytes']>self.tw_last:self.last_tw_progress=now.elapsed_seconds
        self.tw_last=tw['outboundBytes'] if tw else 0
        return g,yt,tw

    def change(self,command,now):
        self.observe(now)
        self.bound(now)
        # YouTubeの正本entry/indexをそのまま保持する唯一のwrite path。
        destination=twitch_forward_update(self.approval['forward'],twitch_destination=self.approval['forward'][1],action=command.action)
        # v1.21.2はmoqTransport=""のJSON入力を"quic"へ変換する。
        # RTMPSのzero-valueを明示すると同じYouTube設定でもhandlerが再生成される。
        wire=[{k:v for k,v in entry.items() if not (k=='moqTransport' and v=='')} for entry in destination]
        self.api('/v3/config/paths/patch/relay',{'forward':wire})


def status(path,value):
    raw=(canonical(value)+'\n').encode();tmp=path.with_suffix('.tmp')
    fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)


def valid_validation(grant,approval,now):
    return (grant.get('schema')=='stream.twitch_daily_validation.v1' and grant.get('approval_sha256')==digest(approval)
        and grant.get('boot_id')==now.boot_id and grant.get('relay_instance')==approval['relay_instance']
        and 0<grant['expires_at']-grant['issued_at']<=600 and grant['issued_at']<=now.utc.timestamp()<grant['expires_at']
        and 0<=now.elapsed_seconds-grant['issued_elapsed']<600)


def run(config,state_directory,credentials,exercise=False,validation_grant=None):
    approval=json.loads(config.read_text())
    assert approval['schema']=='stream.twitch_daily_approval.v1' and approval['schedule_jst']=='00:00'
    assert approval['broadcaster_id']=='1557732073'
    policy=RenewalPolicy();boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    journal=Journal(state_directory/'daily.sqlite3',approval)
    observer=Helix(credentials,approval['broadcaster_id'])
    relay=Relay(approval,Path('/run/stream-v3-oci-relay/guard-status.json'))
    stopped=False
    def stop(*_):
        nonlocal stopped
        stopped=True
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    facts=BroadcastFacts(approval['broadcaster_id'],datetime.now(UTC),BroadcastStatus.UNKNOWN)
    next_poll=0.;last_save=0.;pending_command=None;dispatch_started=0.;poll_failures=0
    # 不明なdispatchを新ID/新processで再実行しない。
    if journal.pending():
        status(state_directory/'status.json',{'state':'HOLD','reason':'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED'})
        return 2
    state=journal.load()
    if state.last_clock and state.last_clock.boot_id!=boot:
        status(state_directory/'status.json',{'state':'HOLD','reason':'RELAY_BOOT_CHANGED'})
        return 2
    validation=json.loads(validation_grant.read_text()) if exercise and validation_grant else {}
    if exercise and not valid_validation(validation,approval,Clock(datetime.now(UTC),time.clock_gettime(time.CLOCK_BOOTTIME),boot)):
        status(state_directory/'status.json',{'state':'HOLD','reason':'DAILY_VALIDATION_GRANT_INVALID'})
        return 2
    while not stopped:
        now=Clock(datetime.now(UTC),time.clock_gettime(time.CLOCK_BOOTTIME),boot)
        try:
            guard,yt,tw=relay.observe(now)
            active=state.phase not in {Phase.TRACKING,Phase.UNKNOWN,Phase.CANCELLED}
            generation,expected_handler,legacy_quiesced=journal.writer()
            due=next_daily_renewal_at(state.session.started_at) if state.session else None
            near=due is not None and (due-now.utc).total_seconds()<=120
            if (active or near) and not poll_failures and next_poll>now.elapsed_seconds+5:
                next_poll=now.elapsed_seconds
            if now.elapsed_seconds>=next_poll:
                facts,reason=observer.fetch(now.utc)
                poll_failures=min(8,poll_failures+1) if facts.status==BroadcastStatus.UNKNOWN else 0
                delay=min(900,10*2**poll_failures) if poll_failures else 5 if active or near else 1800
                if reason=='CREDENTIALS_MISSING':delay=1800
                next_poll=now.elapsed_seconds+delay
                now=Clock(datetime.now(UTC),time.clock_gettime(time.CLOCK_BOOTTIME),boot)
                guard,yt,tw=relay.observe(now)
            if state.phase==Phase.TRACKING and guard['twitch_disabled'] and not legacy_quiesced:
                raise ValueError('LEGACY_TWITCH_FAILURE_NOT_MAINTENANCE')
            if state.phase==Phase.TRACKING and not tw:
                raise ValueError('TWITCH_WRITER_MISSING_OUTSIDE_MAINTENANCE')
            if state.phase==Phase.TRACKING and tw and expected_handler and tw['id']!=expected_handler:
                raise ValueError('TWITCH_WRITER_CHANGED_OUTSIDE_MAINTENANCE')
            if state.phase==Phase.TRACKING and legacy_quiesced and now.elapsed_seconds-relay.last_tw_progress>15:
                raise ValueError('TWITCH_FORWARD_PROGRESS_STALE')
            writer_status=WriterStatus.RUNNING if tw and tw['state']=='forwarding' and not tw.get('lastError') else WriterStatus.UNKNOWN
            if not tw and guard['twitch_disabled']:writer_status=WriterStatus.STOPPED
            if tw and tw.get('lastError'):
                raise ValueError('TWITCH_FORWARD_ERROR')
            if state.binding and state.phase in {Phase.WAIT_START,Phase.VERIFYING} and tw:
                generation=state.binding.writer_generation+1
            writer=WriterFacts(now.utc,approval['relay_instance'],generation,writer_status,
                now.elapsed_seconds-relay.last_tw_progress if writer_status==WriterStatus.RUNNING else None)
            context=RouteContext(approval['broadcaster_id'],'PRIMARY',approval['route_generation'],approval['authority_epoch_id'],
                approval['relay_instance'],boot,guard['lease_elapsed_deadline'],True,True,True)
            outcome=journal.outcome(state)
            if pending_command:
                expected=WriterStatus.STOPPED if pending_command.action==Action.STOP_TWITCH else WriterStatus.RUNNING
                if writer.status==expected and (expected==WriterStatus.STOPPED or writer.progress_age_seconds<2):
                    outcome=Outcome(pending_command.action,pending_command.request_id,pending_command.binding,
                        'STOPPED' if expected==WriterStatus.STOPPED else 'STARTED',now.utc)
                    journal.finish(pending_command,outcome,tw['id'] if tw else None);pending_command=None
                elif now.elapsed_seconds-dispatch_started>15:
                    raise ValueError('TWITCH_PHYSICAL_OUTCOME_UNKNOWN')
            # exerciseはroot設定にある期限と一致する一回だけの検証承認。
            force_due=exercise and state.phase==Phase.TRACKING and valid_validation(validation,approval,now)
            if state.phase==Phase.TRACKING and (facts.status!=BroadcastStatus.LIVE or (now.utc-facts.observed_at).total_seconds()>60):
                decision=None
            else:
                decision=advance(state,now=now,context=context,broadcast=facts,writer=writer,outcome=outcome,
                                 policy=policy,validation_authorized=force_due)
            if decision:
                changed=decision.state.phase!=state.phase or decision.command is not None or decision.status=='RENEWED'
                if changed or now.elapsed_seconds-last_save>=60:
                    journal.commit(state,decision.state,decision.command);state=decision.state;last_save=now.elapsed_seconds
                if decision.command:
                    journal.dispatch_once(decision.command)
                    relay.change(decision.command,now)
                    pending_command=decision.command;dispatch_started=now.elapsed_seconds
                if decision.status=='RENEWED':
                    next_poll=0.
                    if exercise:
                        status(state_directory/'status.json',{'state':'RENEWED','reason':decision.reason,'session':pack(state.session),
                            'youtube_handler_unchanged':True,'observed_at':now.utc.isoformat()})
                        return 0
                if decision.state.phase in {Phase.UNKNOWN,Phase.CANCELLED}:
                    raise ValueError(decision.reason)
            status(state_directory/'status.json',{'state':'WAITING_CREDENTIALS' if reason=='CREDENTIALS_MISSING' else state.phase.value,
                'reason':reason,'schedule_jst':'00:00','offline_wait_seconds':120,'session':pack(state.session),
                'next_renewal_at':next_daily_renewal_at(state.session.started_at).isoformat() if state.session else None,
                'youtube_handler_unchanged':True,'observed_at':now.utc.isoformat()})
        except Exception as error:
            # raw errorやdestination/tokenをstatus/logへ投影しない。relay全体は操作しない。
            known=str(error) if isinstance(error,ValueError) and str(error).startswith(('DAILY_','TWITCH_','LEGACY_','CLOCK_','RENEWAL_','ROUTE_')) else type(error).__name__
            # legacy guardが計画停止でquiesceされた後は、このownerがTwitchの
            # failure stopも1回だけ担当する。再開/retry/全relay操作はしない。
            try:
                if known.startswith(('TWITCH_FORWARD_','TWITCH_PHYSICAL_')) and journal.writer()[2]:
                    _,_,fault_writer=relay.observe(now)
                    if fault_writer:
                        binding=Binding(approval['broadcaster_id'],approval['route_generation'],approval['authority_epoch_id'],
                            approval['relay_instance'],boot,journal.writer()[0])
                        command=Command(Action.STOP_TWITCH,'twitch-fault-stop-'+digest([approval['relay_instance'],fault_writer['id']]),binding)
                        journal.commit(state,dataclasses.replace(state,revision=state.revision+1,phase=Phase.UNKNOWN),command)
                        journal.dispatch_once(command);relay.change(command,now)
            except Exception:
                pass  # 未確定intentを保持し、再送しない。lease ownerは独立して存続する。
            status(state_directory/'status.json',{'state':'HOLD','reason':known,'observed_at':now.utc.isoformat()})
            return 2
        time.sleep(1)
    return 0


def main():
    p=argparse.ArgumentParser();p.add_argument('--config',type=Path,required=True);p.add_argument('--state',type=Path,required=True)
    p.add_argument('--credentials',type=Path,required=True);p.add_argument('--exercise',action='store_true')
    p.add_argument('--validation-grant',type=Path)
    args=p.parse_args()
    return run(args.config,args.state,args.credentials,args.exercise,args.validation_grant)


if __name__=='__main__':raise SystemExit(main())
