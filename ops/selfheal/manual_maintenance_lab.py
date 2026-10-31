#!/usr/bin/env python3
"""Exercise manual maintenance through the real CLI and loopback media with OS/Kubernetes fixtures."""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import pty
import select
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
import urllib.request

from offline_slate import digest, encode, write_json
from offline_slate_lab import OwnedProcesses, free_port, api, wait_for, configuration, analyze
from maintenance_host import TIMER, SERVICE
from maintenance_state import guard, read_json
from stream_core.engine.ffmpeg_args import build_output_args

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def terminal_command(argv, owned, *, timeout=95):
    """Wait for the real PTY prompt and supply the lab-only player-confirmation fixture."""
    master, slave = pty.openpty()
    process, transcript, answered = None, bytearray(), False
    deadline = time.monotonic() + timeout
    try:
        process = subprocess.Popen(argv, stdin=slave, stderr=slave, stdout=subprocess.PIPE,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}, start_new_session=True)
        owned.items.append(process)
        os.close(slave); slave = None
        while process.poll() is None and time.monotonic() < deadline:
            if select.select([master], [], [], min(1, max(0, deadline - time.monotonic())))[0]:
                try:
                    chunk = os.read(master, 4096)
                except OSError:
                    break
                if not chunk: break
                transcript.extend(chunk)
                if len(transcript) > 16384: raise ValueError('terminal_output_too_large')
                if b'[yes/no]' in transcript:
                    os.write(master, b'yes\n')
                    answered = True
                    break
        stdout, _ = process.communicate(timeout=max(.1, deadline - time.monotonic()))
        if process.returncode == 0 and not answered:
            raise ValueError('public_confirmation_prompt_not_observed')
        return subprocess.CompletedProcess(argv, process.returncode, stdout.decode(), transcript.decode(errors='replace'))
    finally:
        if process is not None and process.poll() is None:
            owned.stop(process)
        if slave is not None: os.close(slave)
        os.close(master)


def patch(port, fields):
    request = urllib.request.Request(f'http://127.0.0.1:{port}/v3/config/paths/patch/relay',
        data=json.dumps(fields).encode(), headers={'Content-Type': 'application/json'}, method='PATCH')
    with OPENER.open(request, timeout=3) as response:
        return response.read()


class Fixture:
    def __init__(self, root, owned, ports, media):
        self.root, self.owned, self.ports, self.media = root, owned, ports, media
        self.units = {TIMER: {'active': True, 'enabled': 'enabled'}, SERVICE: {'active': False, 'enabled': 'static'}}
        self.publisher, self.publish_count = None, 0
        self.effects = []
        self.dell_boot = 'isolated-dell-boot-1'
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()

    def publish(self):
        self.publish_count += 1
        cfg = SimpleNamespace(test_mode=False, relay_primary_url='', use_fifo_recovery=True,
            fifo_max_recovery_attempts=20, fifo_recovery_wait_sec=.2, fifo_queue_size=32,
            fifo_drop_pkts_on_overflow=True, fifo_restart_with_keyframe=True,
            rtmp_url=f"rtmp://127.0.0.1:{self.ports['relay']}/relay")
        # Use the runtime's actual finite FIFO output builder, since path PATCHes
        # can disconnect the publisher before intentional Dell shutdown.
        output = build_output_args(cfg)
        self.publisher = self.owned.start('publisher-' + str(self.publish_count), [
            'ffmpeg', '-nostdin', '-v', 'error', '-threads', '1', '-re', '-stream_loop', '-1',
            '-i', str(self.root / 'source.mp4'), '-map', '0:v:0', '-map', '0:a:0',
            # MP4 stream-copy retains avc1 codec_tag through the FIFO outer muxer.
            # Production feeds an encoder into FIFO; use a single-thread encoder
            # here too, instead of weakening the production output builder.
            '-filter_threads', '1', '-c:v', 'libx264', '-preset', 'ultrafast', '-threads:v', '1',
            '-pix_fmt', 'yuv420p', '-r', '5', '-g', '10', '-bf', '0',
            '-c:a', 'aac', '-b:a', '128k', '-ar', '48000', '-ac', '2', '-threads:a', '1',
            '-flags:v', '+global_header', '-tag:v', '0', '-tag:a', '0', '-flush_packets', '1', *output])

    def snapshot(self):
        port = self.ports['relay_api']
        config = api(port, '/v3/config/paths/get/relay')
        path = api(port, '/v3/paths/get/relay')
        fields = {k: config[k] for k in ('alwaysAvailable', 'alwaysAvailableFile', 'alwaysAvailableTracks')}
        stable = {k: v for k, v in config.items() if k not in fields}
        cp = read_json(self.root / 'copy-state.json')
        destinations = api(port, '/v3/paths/forward-dests/list?path=relay').get('items', [])
        tw = next((d for d in destinations if (d.get('conf') or {}).get('dest', '').endswith('/twitch')), {})
        active = self.publisher is not None and self.publisher.poll() is None
        content = 'LIVE' if path['ready'] and path['online'] else 'MAINTENANCE_SLATE' if path['ready'] else 'UNKNOWN'
        now = time.time()
        return {'dell': {'identity': {'machine': 'a' * 64, 'boot': self.dell_boot}, 'observed_at': now,
            'units': copy.deepcopy(self.units),
            'main': {'uid': 'owned-publisher-fixture', 'replicas': int(active), 'active_pods': int(active), 'ready_pods': int(active)},
            'backup': {'uid': 'isolated-backup-fixture', 'replicas': 0, 'active_pods': 0, 'ready_pods': 0},
            'ledger': {'producer_id': 'independent-controller', 'generation': 2, 'authority_version': 1, 'unresolved': 0},
            'controller': {'observed_at': datetime.now(timezone.utc).isoformat(), 'health': 'HEALTHY',
                'maintenance_guard_supported': True, 'maintenance_root': str(self.root / 'session')}},
            'oci': {'identity': {'machine': 'b' * 64, 'boot': self.boot}, 'observed_at': now,
                'config_fingerprint': hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest(),
                'path_fields': fields, 'content': content, 'input_id': (path.get('source') or {}).get('id'),
                'tracks': path.get('tracks'), 'copy': cp,
                'twitch': {'id': tw.get('id'), 'state': tw.get('state'), 'bytes': tw.get('outboundBytes'),
                           'error': bool(tw.get('lastError'))}}}

    def request(self, value):
        if value['action'] == 'snapshot': return self.snapshot()
        action, role = value['action'], value['role']
        self.effects.append(action)
        if action == 'quiesce': self.units[TIMER] = {'active': False, 'enabled': 'disabled'}
        elif action == 'restore_units': self.units = copy.deepcopy(value['restore'])
        elif action == 'path_patch': patch(self.ports['relay_api'], value['after'])
        elif action == 'scale_main':
            if value['after'] == 0: self.owned.stop(self.publisher)
            else: self.publish()
        else: raise ValueError('unknown_fixture_action')
        return self.snapshot()[role]


def exercise(root, media, mediamtx, youtube_copy):
    root.mkdir(parents=True, exist_ok=False)
    owned, server, thread = OwnedProcesses(root), None, None
    report = {'schema': 'stream_v3.manual_maintenance_lab.v1', 'passed': False,
        'production_mutated': False, 'provider_contacted': False,
        'simulated': ['systemd', 'Kubernetes', 'fence_observation', 'operator_player_confirmation'],
        'real': ['CLI', 'durable_session', 'MediaMTX', 'youtube_copy', 'H264_AAC_decode'],
        'binary_sha256': {'mediamtx': digest(mediamtx), 'youtube_copy': digest(youtube_copy)}, 'commands': []}
    try:
        if subprocess.check_output([str(mediamtx), '--version'], text=True, timeout=5).strip() != 'v1.21.2':
            raise ValueError('mediamtx_version_mismatch')
        ports = {k: free_port() for k in ('sink', 'sink_api', 'relay', 'relay_api', 'copy')}
        sink = lambda path: f"rtmp://127.0.0.1:{ports['sink']}/{path}"
        bridge = f"rtmp://127.0.0.1:{ports['copy']}/live/youtube"
        write_json(root / 'sink.json', configuration(ports['sink'], ports['sink_api'],
            {'youtube': {'source': 'publisher'}, 'twitch': {'source': 'publisher'}}))
        write_json(root / 'relay.json', configuration(ports['relay'], ports['relay_api'],
            {'relay': {'source': 'publisher', 'forward': [{'dest': bridge}, {'dest': sink('twitch')}]}}))
        write_json(root / 'copy.json', {'destination': sink('youtube')})
        # Diagnostic logs belong only to this isolated lab, never production.
        for name in ('sink', 'relay'):
            path = root / (name + '.json')
            value = json.loads(path.read_text()); value['logLevel'] = 'info'
            path.write_text(json.dumps(value))
        ppm = root / 'source.ppm'
        ppm.write_bytes(b'P6\n1920 1080\n255\n' + bytes((20, 100, 45)) * (1920 * 1080))
        encode(ppm, root / 'source.mp4')
        owned.start('sink', [str(mediamtx), str(root / 'sink.json')])
        wait_for(lambda: api(ports['sink_api'], '/v3/paths/list'), lambda _: True)
        owned.start('copy', [str(youtube_copy), '-listen', f"127.0.0.1:{ports['copy']}",
            '-config', str(root / 'copy.json'), '-state', str(root / 'copy-state.json'), '-allow-local-output'])
        wait_for(lambda: read_json(root / 'copy-state.json'), lambda x: x['state'] == 'LISTENING')
        owned.start('relay', [str(mediamtx), str(root / 'relay.json')])
        wait_for(lambda: api(ports['relay_api'], '/v3/paths/list'), lambda _: True)
        fixture = Fixture(root, owned, ports, media)
        fixture.publish()
        wait_for(lambda: fixture.snapshot(), lambda x: x['oci']['content'] == 'LIVE' and x['oci']['copy']['state'] == 'FLOWING', seconds=20)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                try:
                    length = int(self.headers.get('Content-Length', '0'))
                    if self.path != '/' or not 0 < length <= 65536: raise ValueError('invalid_request')
                    reply = fixture.request(json.loads(self.rfile.read(length)))
                    body, code = json.dumps(reply).encode(), 200
                except Exception as error:
                    body, code = json.dumps({'fixture_error': type(error).__name__}).encode(), 503
                self.send_response(code); self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        config = {'schema': 'stream_v3.manual_maintenance_config.v1', 'oci_host': 'isolated-fixture',
            'dell_machine_sha256': 'a' * 64, 'oci_machine_sha256': 'b' * 64,
            'youtube_destination_sha256': fixture.snapshot()['oci']['copy']['destination_fingerprint'],
            'slate_sha256': digest(media), 'slate_file': str(media), 'effect_ledger_file': '/fixture-only/ledger.sqlite3'}
        config['slate_manifest_sha256'] = digest(media.with_name('manifest.json'))
        write_json(root / 'config.json', config, private=True)
        cli = [str(Path(__file__).with_name('stream-v3-maintenance')),
            '--root', str(root / 'session'), '--config', str(root / 'config.json'),
            '--lab-api', f'http://127.0.0.1:{server.server_port}', '--lab-stability-seconds', '2']

        def command(name, *args):
            completed = (terminal_command(cli + [name, *args], owned) if name in ('on', 'off') else
                subprocess.run(cli + [name, *args], capture_output=True, text=True, timeout=95,
                    env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}))
            (root / ('cli-' + name + '.log')).write_text(completed.stdout + completed.stderr)
            value = json.loads(completed.stdout)
            report['commands'].append({'command': name, 'returncode': completed.returncode, 'result': value})
            if completed.returncode != 0: raise ValueError('cli_' + name + '_failed')
            return value

        def capture(stage, seconds, expected):
            for name in ('youtube', 'twitch'):
                wait_for(lambda: api(ports['sink_api'], '/v3/paths/get/' + name),
                         lambda x: x['ready'] and x['online'] and x['bytesReceived'] > 5000, seconds=20)
            processes = []
            for name in ('youtube', 'twitch'):
                path = root / (stage + '-' + name + '.flv')
                processes.append(owned.start(stage + '-' + name, ['ffmpeg', '-nostdin', '-v', 'error',
                    '-threads', '1', '-rw_timeout', '5000000', '-analyzeduration', '5000000', '-probesize', '5000000',
                    '-i', sink(name), '-map', '0:v:0', '-map', '0:a:0', '-t', str(seconds),
                    '-c', 'copy', '-flush_packets', '1', '-n', str(path)]))
            for process in processes:
                if process.wait(timeout=25) != 0: raise ValueError('capture_failed')
            return {name: analyze(root / (stage + '-' + name + '.flv'), [expected]) for name in ('youtube', 'twitch')}

        report['before'] = capture('before', 3, 'LIVE')
        command('init')
        command('on', '--minutes', '10')
        if not guard(root / 'session', uid=os.geteuid())['inhibited']: raise ValueError('guard_not_active')
        report['maintenance'] = capture('maintenance', 9, 'OFFLINE_SLATE')
        fixture.dell_boot = 'isolated-dell-after-reboot'
        command('status')
        command('off')
        report['after'] = capture('after', 3, 'LIVE')
        if guard(root / 'session', uid=os.geteuid())['inhibited']: raise ValueError('guard_not_released')
        report['effects'] = fixture.effects
        report['passed'] = True
    except BaseException as error:
        report['failure_class'], report['failure'] = type(error).__name__, str(error)
        raise
    finally:
        if server:
            server.shutdown(); server.server_close()
        if thread: thread.join(timeout=3)
        report['owned_processes_stopped'] = owned.close()
        report['cleanup_errors'] = owned.cleanup_errors
        write_json(root / 'result.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--media', type=Path, required=True)
    parser.add_argument('--mediamtx', type=Path, required=True)
    parser.add_argument('--youtube-copy', type=Path, required=True)
    args = parser.parse_args()
    report = exercise(args.output.resolve(), args.media.resolve(), args.mediamtx.resolve(), args.youtube_copy.resolve())
    print(json.dumps({'passed': report['passed'], 'production_mutated': False, 'provider_contacted': False}))


if __name__ == '__main__':
    main()
