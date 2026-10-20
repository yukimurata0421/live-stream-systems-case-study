"""Arena-owned, durable minute observations. No provider writes or credentials."""
from __future__ import annotations
import argparse
import json
import math
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import build_opener, ProxyHandler

SCHEMA = 'stream.twitch_sli_public.v1'
REVISION = 'af3e63b2a45810ef81bd6567c5ca41ee9e54d3e437cae4b997900e044fa9a578'
SOURCE = Path('/var/lib/stream-v4-twitch-api/twitch_api_evidence.json')
WINDOWS = (('24h', 1440), ('7d', 10080))
DEFINITIONS = (
    ('provider_live', 'Twitch LIVE availability', 'Helix Get Streams: LIVE / known provider observations.', 99.0),
    ('api_measurement', 'API measurement coverage', 'Successful fresh Helix observations / all expected minutes.', 99.0),
    ('relay_flow', 'Twitch relay continuity', 'OCI forward counter progressing / known relay observations.', 99.0),
)
LATEST = {}
LOCK = threading.Lock()
SOURCE_STATUS = {}


def stamp(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise ValueError('TIMESTAMP_REQUIRED')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None: raise ValueError('TIMEZONE_REQUIRED')
    value = result.timestamp()
    if not math.isfinite(value): raise ValueError('TIMESTAMP_NOT_FINITE')
    return value


def iso(value): return datetime.fromtimestamp(value, timezone.utc).isoformat()


def api_sample(raw, now, binding=None):
    """A failed API attempt is measurable; an untrusted source is not."""
    try:
        if type(raw) is not dict or type(now) not in (int, float) or not math.isfinite(now): return None
        if (not isinstance(binding,dict) or raw['schema'] != 'monitoring_v4.twitch_api_evidence.v1'
                or raw['source_host_id'] != 'arena-monitoring-facts'
                or raw['source_boot_id'] != Path('/proc/sys/kernel/random/boot_id').read_text().strip()
                or raw['collector_revision'] != binding.get('collector_revision')
                or raw['user_id'] != binding.get('user_id')): return None
        observed = stamp(raw['collected_at'])
        if not 0 <= now-observed <= 180 or now > stamp(raw['valid_until']): return None
        ok = raw['probe_status'] == 'ok' and raw['http_status'] == 200 and raw['provider_state'] in ('LIVE', 'OFFLINE')
        return {'observed': observed, 'api': 'GOOD' if ok else 'BAD',
                'live': ('GOOD' if raw['provider_state'] == 'LIVE' else 'BAD') if ok else 'UNKNOWN',
                'stream': str(raw.get('stream_id', '')) if ok else '',
                'started': raw.get('started_at') if ok else None}
    except (KeyError, TypeError, ValueError, OSError, OverflowError): return None


def relay_sample(raw, now):
    try:
        if type(raw) is not dict or type(now) not in (int, float) or not math.isfinite(now): return None
        if raw['schema'] != 'stream.relay_public.v1' or raw['source_fresh'] is not True: return None
        observed = stamp(raw['observed_at'])
        if not 0 <= now-observed <= 180: return None
        state = raw['destinations']['twitch']['status']
        if not isinstance(state, str): return None
        return {'observed': observed, 'flow': 'GOOD' if state == 'FLOWING' else 'BAD' if state in ('STALLED', 'DISCONNECTED') else 'UNKNOWN'}
    except (KeyError, TypeError, ValueError, OverflowError): return None


class History:
    def __init__(self, path, now):
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS minutes (minute INTEGER NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
          observed REAL NOT NULL, PRIMARY KEY(minute,kind));
        CREATE TABLE IF NOT EXISTS renewals (due INTEGER PRIMARY KEY, before_id TEXT, offline_at REAL,
          result TEXT NOT NULL, completed_at REAL);
        ''')
        self.db.execute('INSERT OR IGNORE INTO meta VALUES (?,?)', ('started', str(now)))
        self.db.commit()
        self.started = float(self.db.execute("SELECT value FROM meta WHERE key='started'").fetchone()[0])

    def meta(self, key):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def record(self, kind, state, observed, now):
        minute = int(observed//60)
        # No backfill, no fresh timestamp on a cached sample, and no duplicate success.
        if observed < self.started or not 0 <= now-observed <= 180: return
        old = self.db.execute('SELECT state,observed FROM minutes WHERE minute=? AND kind=?', (minute, kind)).fetchone()
        if old and observed <= old[1]: return
        # Preserve a failure or an unknown observation within the same minute.
        rank = {'GOOD': 0, 'UNKNOWN': 1, 'BAD': 2}
        value = max((state, old[0]), key=rank.get) if old else state
        self.db.execute('INSERT OR REPLACE INTO minutes VALUES (?,?,?,?)', (minute, kind, value, observed))

    def tick(self, now, api=None, relay=None):
        # A failed cycle must not leave a partially committed observation.
        with self.db:
            self._tick(now, api, relay)

    def _tick(self, now, api=None, relay=None):
        last = self.meta('last_api')
        # 00:00 JST is 15:00 UTC. Register only renewals whose due instant was observed by this history.
        first_due = int((self.started-54000)//86400)*86400+54000
        if first_due < self.started: first_due += 86400
        due = int((now-54000)//86400)*86400+54000
        if first_due <= due:
            before = last['stream'] if last and last['live']=='GOOD' and 0 <= due-last['observed'] <= 180 else None
            self.db.execute('INSERT OR IGNORE INTO renewals VALUES (?,?,NULL,?,NULL)', (due, before, 'PENDING'))
        if api:
            self.record('provider_live', api['live'], api['observed'], now)
            self.record('api_measurement', api['api'], api['observed'], now)
        if relay: self.record('relay_flow', relay['flow'], relay['observed'], now)
        for due, before, offline, result in self.db.execute("SELECT due,before_id,offline_at,result FROM renewals WHERE result='PENDING'").fetchall():
            if api and due <= api['observed'] <= due+600:
                if api['live'] == 'BAD' and offline is None:
                    offline = api['observed']
                    self.db.execute('UPDATE renewals SET offline_at=? WHERE due=?', (offline, due))
                try: new_start = stamp(api['started']) if api['started'] else 0
                except (ValueError, TypeError): new_start = 0
                if before and offline and api['live']=='GOOD' and api['stream'] and api['stream']!=before and due <= new_start <= api['observed'] and api['observed']>offline:
                    self.db.execute("UPDATE renewals SET result='GOOD',completed_at=? WHERE due=?", (api['observed'], due))
                    continue
            if now > due+780:  # allow the collector to deliver the sample taken at the 600 s deadline
                known = self.db.execute("SELECT COUNT(*) FROM minutes WHERE kind='provider_live' AND minute>=? AND minute<? AND state IN ('GOOD','BAD')", (due//60, due//60+10)).fetchone()[0]
                outcome = 'BAD' if before and known==10 else 'UNKNOWN'
                self.db.execute('UPDATE renewals SET result=?,completed_at=? WHERE due=?', (outcome, now, due))
        if api and (not last or api['observed']>last['observed']):
            self.db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', ('last_api', json.dumps(api)))
        self.db.execute('DELETE FROM minutes WHERE minute<?', (int(now//60)-35*1440,))
        self.db.execute('DELETE FROM renewals WHERE due<?', (now-35*86400,))

    def counts(self, kind, start, end):
        found = dict(self.db.execute('SELECT state,COUNT(*) FROM minutes WHERE kind=? AND minute>=? AND minute<? GROUP BY state', (kind,start,end)))
        good, bad, unknown = (found.get(x, 0) for x in ('GOOD','BAD','UNKNOWN'))
        return good, bad, unknown, end-start-good-bad-unknown

    def public(self, now):
        end = int(now//60)  # only completed minute slots
        windows = []
        for name, length in WINDOWS:
            metrics = []
            full = now-self.started >= length*60
            for kind, label, definition, target in DEFINITIONS:
                good, bad, unknown, missing = self.counts(kind,end-length,end)
                known = good+bad
                coverage = 100*known/length
                conservative = 100*good/length
                observed = 100*good/known if known else None
                value = conservative if kind=='api_measurement' else observed
                assessment = ('COLLECTING' if not full else 'UNKNOWN' if coverage<99 else
                              'MET' if conservative>=target else 'BREACHED')
                metrics.append({'id':kind,'label':label,'definition':definition,'target_percent':target,
                    'value_percent':value,'conservative_percent':conservative,'coverage_percent':coverage,
                    'assessment':assessment,'good_minutes':good,'bad_minutes':bad,
                    'unknown_minutes':unknown,'missing_minutes':missing,'expected_minutes':length,
                    'error_budget_minutes':length*(1-target/100),'confirmed_budget_used_percent':100*bad/(length*(1-target/100))})
            start = (end-length)*60
            due_first = int((start-54000)//86400)*86400+54000
            if due_first < start: due_first += 86400
            due_list = list(range(due_first, end*60, 86400))
            events = dict(self.db.execute('SELECT due,result FROM renewals WHERE due>=? AND due<?', (start,end*60)))
            values = [events.get(d,'MISSING') for d in due_list]
            good, bad = values.count('GOOD'), values.count('BAD')
            pending = values.count('PENDING')
            unknown = len(values)-good-bad-pending
            assessment = 'NO_DUE' if not values else 'COLLECTING' if not full else 'UNKNOWN' if unknown or pending else 'MET' if bad==0 else 'BREACHED'
            metrics.append({'id':'daily_renewal','label':'Daily session renewal','definition':'OFFLINE → new LIVE stream ID and start time, within 600 s of 00:00 JST.',
                'target_percent':100.0,'value_percent':100*good/(good+bad) if good+bad else None,
                'assessment':assessment,'good_events':good,'bad_events':bad,'unknown_events':unknown,
                'pending_events':pending,'expected_events':len(values)})
            windows.append({'id':name,'start_at':iso((end-length)*60),'end_at':iso(end*60),'full_history':full,'metrics':metrics})
        timeline = []
        for i in range(24):
            a = end-1440+i*60
            good,bad,unknown,missing = self.counts('provider_live',a,a+60)
            timeline.append({'start_at':iso(a*60),'good':good,'bad':bad,'unknown':unknown,'missing':missing})
        return {'schema':SCHEMA,'generated_at':iso(now),'history_started_at':iso(self.started),
                'cadence_sec':60,'freshness_limit_sec':180,'primary_window':'7d','coverage_gate_percent':99.0,
                'planned_downtime_included':True,'windows':windows,'timeline':timeline}


def metrics(value):
    lines = ['stream_v4_twitch_sli_generated_unixtime '+str(stamp(value['generated_at']))]
    for window in value['windows']:
        for m in window['metrics']:
            labels = '{sli="'+m['id']+'",window="'+window['id']+'"}'
            for key in ('value_percent','conservative_percent','coverage_percent','target_percent','bad_minutes','unknown_minutes','missing_minutes','good_events','bad_events','unknown_events','pending_events'):
                if m.get(key) is not None: lines.append('stream_v4_twitch_sli_'+key+labels+' '+str(m[key]))
            lines.append('stream_v4_twitch_sli_assessed'+labels+' '+('1' if m['assessment'] in ('MET','BREACHED') else '0'))
    return '\n'.join(lines)+'\n'


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        with LOCK: value = dict(LATEST)
        if self.path not in ('/public','/metrics') or not value: self.send_error(404);return
        if time.time()-stamp(value['generated_at'])>180: self.send_error(503);return
        body = (json.dumps(value,allow_nan=False) if self.path=='/public' else metrics(value)).encode()
        self.send_response(200);self.send_header('Content-Type','application/json' if self.path=='/public' else 'text/plain; version=0.0.4')
        self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args): pass


def log_event(event, **fields):
    """Log fixed diagnostic fields, never source payloads or exception messages."""
    value = {'event': event, 'recorded_at_jst': datetime.now(timezone(timedelta(hours=9))).isoformat()}
    value.update(fields)
    print(json.dumps(value, allow_nan=False), file=sys.stderr, flush=True)


def source_status(source, status):
    if SOURCE_STATUS.get(source) != status:
        SOURCE_STATUS[source] = status
        log_event('source_status_changed', source=source, status=status)


def notify(message):
    """Support the service watchdog without adding delivery/control effects."""
    address = os.environ.get('NOTIFY_SOCKET')
    if not address: return False
    if address.startswith('@'): address = '\0' + address[1:]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as channel:
        channel.settimeout(1)
        channel.connect(address)
        channel.sendall(message.encode('ascii'))
    return True


def collect_once(history, opener, now, source=SOURCE, binding=None):
    api = relay = None
    try:
        with source.open('rb') as handle: raw = handle.read(65537)
        if len(raw) > 65536: raise ValueError('SOURCE_SIZE')
        api = api_sample(json.loads(raw), now, binding)
    except (OSError, ValueError): pass
    source_status('api', 'unavailable' if api is None else 'observed_failure' if api['api'] == 'BAD' else 'observed')
    try:
        with opener.open('http://127.0.0.1:9116/public', timeout=3) as response: raw = response.read(16385)
        if len(raw) > 16384: raise ValueError('SOURCE_SIZE')
        relay = relay_sample(json.loads(raw), now)
    except (OSError, ValueError): pass
    source_status('relay', 'unavailable' if relay is None else 'observed')
    history.tick(now, api, relay)
    return history.public(time.time())


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--state',type=Path,required=True);parser.add_argument('--binding',type=Path,required=True);args=parser.parse_args()
    stopped=threading.Event()
    signal.signal(signal.SIGTERM,lambda *_:stopped.set());signal.signal(signal.SIGINT,lambda *_:stopped.set())
    history = server = worker = None
    phase = 'startup'
    try:
        with args.binding.open('rb') as handle: raw = handle.read(16385)
        if len(raw) > 16384: raise ValueError('BINDING_TOO_LARGE')
        binding = json.loads(raw)
        if type(binding) is not dict or not binding.get('user_id') or not binding.get('collector_revision'): raise ValueError('BINDING_REQUIRED')
        args.state.mkdir(mode=0o700,exist_ok=True)
        history=History(args.state/'history.sqlite3',time.time())
        server=ThreadingHTTPServer(('127.0.0.1',9117),Handler);server.daemon_threads=True
        worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
        opener=build_opener(ProxyHandler({}));ready=False
        log_event('collector_started')
        while not stopped.is_set():
            phase = 'collection'
            if not worker.is_alive(): raise RuntimeError('PUBLIC_SERVER_STOPPED')
            value = collect_once(history, opener, time.time(), binding=binding)
            with LOCK: LATEST.clear();LATEST.update(value)
            phase = 'watchdog'
            notify('WATCHDOG=1' if ready else 'READY=1\nWATCHDOG=1')
            ready=True
            stopped.wait(15)
        log_event('collector_stopped')
        return 0
    except Exception as error:
        with LOCK: LATEST.clear()
        frames = [{'file': Path(frame.filename).name, 'line': frame.lineno, 'function': frame.name}
                  for frame in traceback.extract_tb(error.__traceback__)]
        log_event('collector_failed', phase=phase, exception_class=type(error).__name__, frames=frames)
        return 1
    finally:
        if server:
            if worker and worker.is_alive(): server.shutdown()
            server.server_close()
        if history: history.db.close()


if __name__=='__main__': raise SystemExit(main())
