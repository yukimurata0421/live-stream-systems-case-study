#!/usr/bin/env python3
"""Serve bounded, read-only delivery facts to one peer on a private interface."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import ipaddress
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

MAX_BYTES = 16384
COPY_FIELDS = ('schema', 'pid', 'boot_id', 'session', 'destination_fingerprint',
               'updated_at', 'state', 'output_bytes', 'video_packets', 'audio_packets', 'last_progress_at')
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def unit_pid(unit, comm):
    """Read the sole process in a fixed cgroup without systemd control privileges."""
    values = Path('/sys/fs/cgroup/system.slice', unit + '.service', 'cgroup.procs').read_text().split()
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


def collect(module):
    media_pid = unit_pid('stream-v3-oci-mediamtx', 'mediamtx')
    copy_pid = unit_pid('stream-v3-youtube-forward', 'youtube-forward')
    path = api(module, '/v3/paths/get/relay')
    rows = api(module, '/v3/paths/forward-dests/list?path=relay')['items']
    bridges = [x for x in rows if (x.get('conf') or {}).get('dest') == module.YOUTUBE_BRIDGE]
    if len(bridges) != 1 or any(module.provider(x) == 'youtube' for x in rows):
        raise ValueError('route_binding_mismatch')
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    raw = {'schema': 'stream.relay_raw.v1', 'host_id': 'relay-host',
           'machine_id_sha256': hashlib.sha256(Path('/etc/machine-id').read_bytes()).hexdigest(),
           'boot_id': boot, 'observed_at': datetime.now(timezone.utc).isoformat(),
           'monotonic_seconds': time.monotonic(),
           'relay': {'active': True, 'pid': media_pid},
           'input': {'available': True, 'ready': path['ready'],
                     'connection_id': (path.get('source') or {}).get('id'),
                     'bytes': path.get('bytesReceived', path.get('inboundBytes'))}, 'destinations': {}}
    copy = None
    try:
        value = read_json(module.YOUTUBE_STATUS)
        facts = module.youtube_forward_facts(
            value, now=time.time(), pid=copy_pid, boot_id=raw['boot_id'],
            fingerprint=module.YOUTUBE_FINGERPRINT.read_text().strip())
        if facts is not None and bridges[0]['state'] == 'forwarding' and not bridges[0].get('lastError'):
            raw['destinations']['youtube'] = facts
            copy = {k: value[k] for k in COPY_FIELDS}
    except (OSError, KeyError, TypeError, ValueError):
        pass
    if (unit_pid('stream-v3-oci-mediamtx', 'mediamtx') != media_pid
            or unit_pid('stream-v3-youtube-forward', 'youtube-forward') != copy_pid):
        raise ValueError('unit_process_changed')
    return {'schema': 'stream_v3.route_facts.v1', 'relay': raw, 'youtube_copy': copy}


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
        if self.client_address[0] != self.server.allowed_peer:
            self.reply(403, {'error': 'peer_not_allowed'})
            return
        if self.path != '/facts':
            self.reply(404, {'error': 'not_found'})
            return
        try:
            value = self.server.collector()
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
