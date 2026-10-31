#!/usr/bin/env python3
"""Serve bounded, read-only delivery facts to one peer on a private interface."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import ipaddress
import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from urllib.parse import urlsplit, parse_qs
import importlib.util as _clock_import

_spec = _clock_import.spec_from_file_location('stream_clock_contract', Path(__file__).with_name('clock_contract.py'))
clock = _clock_import.module_from_spec(_spec)
sys.modules[_spec.name] = clock
_spec.loader.exec_module(clock)

MAX_BYTES = 16384
COPY_FIELDS = ('schema', 'pid', 'boot_id', 'session', 'destination_fingerprint',
               'updated_at', 'state', 'output_bytes', 'video_packets', 'audio_packets', 'last_progress_at')
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class UnitNotRunning(ValueError):
    """The fixed unit cgroup is absent or empty; not a source-read failure."""

def stopped_relay_facts():
    identity=clock.identity()
    return {'schema':'stream_v3.route_facts.v1',
            'relay_availability':{'schema':'stream_v3.relay_availability.v1',
                                  'state':'STOPPED','reason':'unit_not_running'},
            'relay':{'schema':'stream.relay_raw.v1','host_id':'relay-host',
                     **identity,'observed_at':datetime.now(timezone.utc).isoformat(),
                     'monotonic_seconds':time.monotonic(),'observed_boottime_seconds':clock.mono(),
                     'relay':{'active':False,'pid':0},
                     'input':{'available':False,'ready':False,'online':False,'content_state':'OFFLINE',
                              'connection_id':None,'bytes':0},'destinations':{}},
            'youtube_copy':None}

def legacy_input_facts(path):
    """Preserve the operated observer contract when it lacks the newer helper."""
    return {'available':True,'ready':path['ready'],
            'connection_id':(path.get('source') or {}).get('id'),
            'bytes':path.get('bytesReceived',path.get('inboundBytes'))}


class CopyHeartbeat:
    """Observe changes in bound producer records using same-host boottime."""
    def __init__(self):
        self.identity=None;self.digest=None;self.changed_at=None;self.confirmed=False

    def observe(self,value,path,now):
        identity=(str(path),value['boot_id'],value['pid'],value['session'],value['destination_fingerprint'])
        digest=hashlib.sha256(json.dumps({k:value[k] for k in COPY_FIELDS},sort_keys=True,allow_nan=False).encode()).hexdigest()
        changed=False
        if identity!=self.identity:
            self.identity=identity;self.digest=digest;self.changed_at=now;self.confirmed=False
        elif digest!=self.digest:
            self.digest=digest;self.changed_at=now;self.confirmed=True;changed=True
        age=max(0.,now-self.changed_at)
        return {'schema':'stream_v3.copy_observation.v1','age_seconds':age,
                'confirmed':self.confirmed,'new_observation':changed}


_COPY_HEARTBEAT=CopyHeartbeat()


def bound_copy(value,pid,boot,fingerprint):
    try:
        return (value['schema']=='stream_v3.youtube_forward.v1'
                and type(value['pid']) is int and value['pid']==pid and pid>0
                and value['boot_id']==boot and value['destination_fingerprint']==fingerprint
                and len(fingerprint)==64 and all(c in '0123456789abcdef' for c in fingerprint)
                and type(value['session']) is int and value['session']>0
                and value['state'] in ('FLOWING','CONNECTING','ERROR')
                and all(type(value[k]) is int and value[k]>=0 for k in ('output_bytes','video_packets','audio_packets'))
                and all(isinstance(value[k],str) and len(value[k])<=40 for k in ('updated_at','last_progress_at')))
    except (KeyError,TypeError):return False


def unavailable_copy_facts(media_pid,fingerprint):
    value=stopped_relay_facts();value.pop('relay_availability')
    value['relay']['relay']={'active':True,'pid':media_pid}
    value['relay']['input']=None
    value['copy_availability']={'schema':'stream_v3.copy_availability.v1','state':'STOPPED',
                                'reason':'unit_not_running','pid':0,'destination_fingerprint':fingerprint}
    return value


def unit_pid(unit, comm):
    """Read the sole process in a fixed cgroup without systemd control privileges."""
    try:
        values = Path('/sys/fs/cgroup/system.slice', unit + '.service', 'cgroup.procs').read_text().split()
    except FileNotFoundError:
        raise UnitNotRunning('unit_not_running') from None
    if not values:
        raise UnitNotRunning('unit_not_running')
    if len(values) != 1:
        raise ValueError('unit_process_not_unique')
    pid = int(values[0])
    if Path('/proc', str(pid), 'comm').read_text().strip() != comm:
        raise ValueError('unit_process_mismatch')
    return pid


def api(module, path):
    try:
        with _OPENER.open(module.API + path, timeout=2) as response:
            raw = response.read(MAX_BYTES + 1)
    except urllib.error.HTTPError as error:
        error.close()
        raise
    if len(raw) > MAX_BYTES:
        raise ValueError('oversized_source')
    return json.loads(raw)


def read_json(path):
    with Path(path).open('rb') as handle:
        raw = handle.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError('oversized_facts')
    return json.loads(raw)


def collect(module,*,heartbeat=None):
    heartbeat=_COPY_HEARTBEAT if heartbeat is None else heartbeat
    try:
        media_pid = unit_pid('stream-v3-oci-mediamtx', 'mediamtx')
    except UnitNotRunning:
        value=stopped_relay_facts()
        # A start racing this read must not be described as a stopped unit.
        try:
            unit_pid('stream-v3-oci-mediamtx', 'mediamtx')
        except UnitNotRunning:
            return value
        raise ValueError('unit_process_changed')
    try:
        copy_pid = unit_pid('stream-v3-youtube-forward', 'youtube-forward')
    except UnitNotRunning:
        fingerprint=module.YOUTUBE_FINGERPRINT.read_text().strip()
        if len(fingerprint)!=64 or any(c not in '0123456789abcdef' for c in fingerprint):raise ValueError('copy_binding_mismatch')
        value=unavailable_copy_facts(media_pid,fingerprint)
        try:unit_pid('stream-v3-youtube-forward','youtube-forward')
        except UnitNotRunning:
            if unit_pid('stream-v3-oci-mediamtx','mediamtx')!=media_pid:raise ValueError('unit_process_changed')
            return value
        raise ValueError('unit_process_changed')
    path = api(module, '/v3/paths/get/relay')
    rows = api(module, '/v3/paths/forward-dests/list?path=relay')['items']
    bridges = [x for x in rows if (x.get('conf') or {}).get('dest') == module.YOUTUBE_BRIDGE]
    if len(bridges) != 1 or any(module.provider(x) == 'youtube' for x in rows):
        raise ValueError('route_binding_mismatch')
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    raw = {'schema': 'stream.relay_raw.v1', 'host_id': 'relay-host',
           'machine_id_sha256': hashlib.sha256(Path('/etc/machine-id').read_bytes()).hexdigest(),
           'boot_id': boot, 'observed_at': datetime.now(timezone.utc).isoformat(),
           'monotonic_seconds': time.monotonic(),'observed_boottime_seconds':clock.mono(),
           'relay': {'active': True, 'pid': media_pid},
           'input': getattr(module,'input_facts',legacy_input_facts)(path), 'destinations': {}}
    copy = None;observation=None;availability=None
    try:
        value = read_json(module.YOUTUBE_STATUS)
        fingerprint=module.YOUTUBE_FINGERPRINT.read_text().strip()
        if not bound_copy(value,copy_pid,boot,fingerprint):raise ValueError('copy_binding_mismatch')
        observation=heartbeat.observe(value,module.YOUTUBE_STATUS,clock.mono())
        if observation['age_seconds']>20:
            availability={'schema':'stream_v3.copy_availability.v1','state':'STALLED',
                          'reason':'heartbeat_not_advancing','pid':copy_pid,'session':value['session'],
                          'destination_fingerprint':fingerprint,'unobserved_for_sec':observation['age_seconds']}
        facts = module.youtube_forward_facts(
            value, now=time.time(), pid=copy_pid, boot_id=raw['boot_id'],
            fingerprint=fingerprint)
        # Intrinsic record changes confirm producer freshness even when its
        # UTC label is stale/future/frozen. Counter progress is checked by the
        # consumer; this does not assert YouTube reception or media health.
        if facts is None and observation['confirmed'] and observation['age_seconds']<=20:
            facts={'id':f'youtube-forward-{copy_pid}-{value["session"]}',
                   'state':'forwarding' if value['state']=='FLOWING' else 'unknown','bytes':value['output_bytes']}
        # A failed/idle bridge still has fresh, bound copy diagnostics.
        # Discarding them would hide an actual media fault as missing evidence.
        if facts is not None or availability is not None:
            if facts is None:facts={'id':f'youtube-forward-{copy_pid}-{value["session"]}','state':'unknown','bytes':value['output_bytes']}
            raw['destinations']['youtube'] = facts
            copy = {k: value[k] for k in COPY_FIELDS}
    except (OSError, KeyError, TypeError, ValueError):
        pass
    if (unit_pid('stream-v3-oci-mediamtx', 'mediamtx') != media_pid
            or unit_pid('stream-v3-youtube-forward', 'youtube-forward') != copy_pid):
        raise ValueError('unit_process_changed')
    result={'schema': 'stream_v3.route_facts.v1', 'relay': raw, 'youtube_copy': copy}
    if observation is not None:result['copy_observation']=observation
    if availability is not None:result['copy_availability']=availability
    return result


class FactsServer(HTTPServer):
    request_queue_size = 4

    def __init__(self, address, collector, allowed_peer):
        self.collector = collector
        self.allowed_peer = str(ipaddress.ip_address(allowed_peer))
        super().__init__(address, FactsHandler)

    def get_request(self):
        conn, peer = super().get_request()
        conn.settimeout(3)
        return conn, peer


class FactsHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, status, value):
        body = json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
        if len(body) > MAX_BYTES:
            raise ValueError('oversized_facts')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        started = clock.capture()
        if self.client_address[0] != self.server.allowed_peer:
            self.reply(403, {'error': 'peer_not_allowed'})
            return
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True)
        nonce = query.get('nonce', [None])[0]
        if (parsed.path != '/facts' or parsed.fragment or
                (parsed.query and (set(query) != {'nonce'} or len(query['nonce']) != 1 or not clock.valid_nonce(nonce)))):
            self.reply(404, {'error': 'not_found'})
            return
        try:
            value = self.server.collector()
            if nonce is not None:
                value['clock'] = clock.envelope(nonce, started, host_identity={
                    k: value['relay'][k] for k in ('boot_id', 'machine_id_sha256')})
            # Encode before sending a success header, including size/NaN checks.
            body = json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
            if len(body) > MAX_BYTES:
                raise ValueError('oversized_facts')
            self.reply(200, value)
        except (OSError, KeyError, ValueError, TypeError):
            self.reply(503, {'error': 'facts_unavailable'})

    def do_POST(self):
        self.reply(405, {'error': 'read_only'})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--observer-module', type=Path, required=True)
    parser.add_argument('--observer-sha256', required=True)
    parser.add_argument('--bind', required=True)
    parser.add_argument('--port', type=int, default=9998)
    parser.add_argument('--allow-peer', required=True)
    args = parser.parse_args()
    if hashlib.sha256(args.observer_module.read_bytes()).hexdigest() != args.observer_sha256:
        raise SystemExit('observer_source_mismatch')
    if not ipaddress.ip_address(args.bind).is_private:
        raise SystemExit('private_interface_required')
    spec = importlib.util.spec_from_file_location('oci_delivery_observer', args.observer_module)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    server = FactsServer((args.bind, args.port), lambda: collect(module), args.allow_peer)
    try:
        server.serve_forever(poll_interval=.5)
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
