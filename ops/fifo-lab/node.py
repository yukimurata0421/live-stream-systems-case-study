#!/usr/bin/env python3
"""Experiment-owned WireGuard and media inside PrivateNetwork; no host-network changes."""
from __future__ import annotations

import argparse
import base64
import ctypes
import fcntl
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.request

HEADER = struct.Struct('!cI')
WIDTH, HEIGHT, FPS = 320, 180, 5
MAX_FRAME = 131072
TUNING_PROFILES = {
    'baseline': {'recovery_wait': 1, 'rw_timeout_us': 15000000, 'read_timeout': 8, 'gop': 10},
    'tuned': {'recovery_wait': .2, 'rw_timeout_us': 3000000, 'read_timeout': 4, 'gop': 10},
    'tuned_gop5': {'recovery_wait': .2, 'rw_timeout_us': 3000000, 'read_timeout': 4, 'gop': 5},
}


def framed(kind: bytes, payload: bytes) -> bytes:
    if len(payload) > MAX_FRAME:
        raise ValueError('oversized protocol frame')
    return HEADER.pack(kind, len(payload)) + payload


def decode_counter(frame: bytes) -> int:
    if len(frame) != WIDTH * HEIGHT:
        raise ValueError('incomplete decoded frame')
    return sum((frame[16 * WIDTH + bit * 20 + 10] > 128) << bit for bit in range(16))


class Node:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.root)
        self.root.mkdir(exist_ok=True)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.children = []
        self.sender = None
        self.receiver = None
        self.server = None
        self.case = None
        self.case_origin = 0.0
        self.generation = 0
        self.restart_at = None
        self.logs = []
        self.log_threads = []
        self.trace = False
        self.reader_profile = 'reference'
        self.restart_delay = 5.0
        self.sender_gop = 10
        self.transport_probe = False
        self.decode_media = True
        self.media_profile = 'light'
        self.tuning = dict(TUNING_PROFILES['baseline'])
        self.fifo_verbose = False
        self.runtime_candidate = False
        self.production_telemetry = None
        self.production_telemetry_sink = None
        self.bounded_sender = False
        self.restart_allowed = False
        self.max_restarts = 1
        self.failed_restart_delay = 0.0
        self.ping_interval = .2
        self.report_pings = False
        self.stall_target = None
        self.blackhole_active = False
        self.output_cut_done = False
        self.wan_probes_ready = False
        self.ping_targets = {'oci_wg_isolated': '198.51.100.129'}
        self.source_ip = '192.0.2.2'
        self.last_frame = None
        self.host_ns = args.host_netns
        self.node_ns = os.readlink('/proc/self/ns/net')
        if self.node_ns == self.host_ns:
            raise RuntimeError('host network namespace rejected')
        names = {name for _, name in socket.if_nameindex()}
        if names != {'lo'}:
            raise RuntimeError('namespace must initially contain loopback only')
        self.started = time.monotonic()

    def emit(self, kind, **values):
        data = {'kind': kind, 'role': self.args.role, 'mono': time.monotonic(),
                'case': self.case, **values}
        self.send(b'E', json.dumps(data, separators=(',', ':')).encode())

    def send(self, kind, data):
        with self.lock:
            sys.stdout.buffer.write(framed(kind, data))
            sys.stdout.buffer.flush()

    def run_cmd(self, values, **kwargs):
        result = subprocess.run(values, capture_output=True, timeout=10, **kwargs)
        if result.returncode:
            raise RuntimeError(f'{values[0]} exited {result.returncode}: '
                               + result.stderr.decode(errors='replace')[-1500:])
        return result

    def setup(self):
        self.tun = os.open('/dev/net/tun', os.O_RDWR | os.O_NONBLOCK)
        fcntl.ioctl(self.tun, 0x400454CA, struct.pack('16sH', b'wanlab', 0x0001 | 0x1000))
        ip = '192.0.2.2' if self.args.role == 'dell' else '192.0.2.1'
        self.run_cmd(['ip', 'addr', 'add', ip + '/32', 'dev', 'wanlab'])
        self.run_cmd(['ip', 'link', 'set', 'wanlab', 'mtu', '1380', 'up'])
        destinations = ['192.0.2.1'] if self.args.role == 'dell' else ['192.0.2.2', '198.51.100.2']
        for dest in destinations:
            self.run_cmd(['ip', 'route', 'add', dest + '/32', 'dev', 'wanlab'])
        key = self.run_cmd(['wg', 'genkey']).stdout.strip()
        # Honor the installed wg AppArmor profile. The unit binds only this
        # experiment's empty key directory here; production keys are not visible.
        if not os.path.samefile('/etc/wireguard', self.root.parent / 'keys'):
            raise RuntimeError('experiment-only WireGuard key mount is missing')
        keyfile = Path('/etc/wireguard/wg-lab.key')
        keyfile.write_bytes(key + b'\n')
        keyfile.chmod(0o600)
        public = self.run_cmd(['wg', 'pubkey'], input=key + b'\n').stdout.decode().strip()
        self.run_cmd(['ip', 'link', 'add', 'wglab', 'type', 'wireguard'])
        self.run_cmd(['wg', 'set', 'wglab', 'private-key', str(keyfile), 'listen-port', '51823'])
        inner = '198.51.100.130' if self.args.role == 'dell' else '198.51.100.129'
        self.run_cmd(['ip', 'addr', 'add', inner + '/25', 'dev', 'wglab'])
        self.run_cmd(['ip', 'link', 'set', 'wglab', 'mtu', '1280', 'up'])
        certificate = None
        if self.args.role == 'oci':
            # A prior failed setup may have left an experiment-owned key whose
            # owner was already changed to the unprivileged media process.
            (self.root / 'server.key').unlink(missing_ok=True)
            self.run_cmd(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                          '-keyout', str(self.root / 'server.key'), '-out', str(self.root / 'server.crt'),
                          '-days', '1', '-subj', '/CN=fifo-lab', '-addext', 'subjectAltName=IP:198.51.100.129'])
            (self.root / 'server.key').chmod(0o600)
            os.chown(self.root / 'server.key', 65534, 65534)
            (self.root / 'server.crt').chmod(0o644)
            certificate = base64.b64encode((self.root / 'server.crt').read_bytes()).decode()
        self.emit('ready', netns=self.node_ns, interfaces=sorted(n for _, n in socket.if_nameindex()),
                  public_key=public, certificate=certificate)

    def watch_log(self, pipe, log, component, case, pid):
        seen = set()
        try:
            for raw in iter(pipe.readline, b''):
                log.write(raw)
                text = raw.decode(errors='replace').strip()
                if self.production_telemetry is not None and self.production_telemetry.pid == pid:
                    self.production_telemetry.line(text)
                if component == 'sender' and text.startswith('TRANSPORT_PROBE '):
                    value = json.loads(text.split(' ', 1)[1])
                    if value.pop('pid') != pid:
                        raise RuntimeError('transport probe process identity mismatch')
                    self.emit('transport_probe', case=case, app_pid=pid, **value)
                    continue
                stage = None
                if component == 'server':
                    if '[RTMPS]' in text and '] opened' in text:
                        stage = 'rtmp_open'
                    elif '[RTMPS]' in text and 'is publishing to path' in text:
                        stage = 'rtmp_publish'
                    elif '[RTMPS]' in text and '] closed:' in text:
                        stage = 'rtmp_close'
                    elif '[RTSP]' in text and 'is reading from path' in text:
                        stage = 'rtsp_read'
                elif 'Recovery successful' in text:
                    stage = 'fifo_recovery_success'
                elif 'Keyframe received, recovering' in text:
                    stage = 'fifo_keyframe_resume'
                elif any(s in text for s in ('Broken pipe', 'IO error:', 'Recovery failed:', 'Failed to recover')):
                    stage = 'io_error'
                elif text.startswith('Output #'):
                    stage = 'output_ready'
                elif self.media_profile == 'production_nvenc' and 'encoder ->' in text:
                    stage = 'video_encoder_packet' if 'type:video' in text else 'audio_encoder_packet'
                elif self.media_profile == 'production_nvenc' and 'muxer <-' in text:
                    stage = 'video_mux_packet' if 'type:video' in text else 'audio_mux_packet'
                elif 'demuxer ->' in text:
                    stage = 'demux_packet'
                elif 'encoder ->' in text:
                    stage = 'encoder_packet'
                elif 'decoder ->' in text:
                    stage = 'decoded_frame'
                elif 'muxer <-' in text:
                    stage = 'mux_packet'
                if stage is None:
                    continue
                if component != 'server' and stage not in ('io_error', 'fifo_recovery_success', 'fifo_keyframe_resume'):
                    if stage in seen:
                        continue
                    seen.add(stage)
                identity = {}
                if not seen.intersection({'identity_recorded'}):
                    try:
                        identity = {line.split(':', 1)[0]: line.split(':', 1)[1].strip()
                                    for line in Path(f'/proc/{pid}/status').read_text().splitlines()
                                    if line.startswith(('Uid:', 'Gid:', 'NoNewPrivs:', 'CapEff:'))}
                        seen.add('identity_recorded')
                    except OSError:
                        pass
                self.emit('trace', component=component, stage=stage, app_pid=pid,
                          line=text[:1500], identity=identity, case=case)
        finally:
            pipe.close()

    def app(self, args, name, *, stdout=subprocess.DEVNULL, env=None, component=None):
        log = (self.root / (name + '.log')).open('ab', buffering=0)
        self.logs.append(log)
        command = ['setpriv', '--reuid=65534', '--regid=65534', '--clear-groups',
                   '--no-new-privs', *args]
        app_env = {'PATH': '/usr/bin:/usr/sbin', 'LANG': 'C', 'OMP_NUM_THREADS': '1'}
        if self.media_profile == 'production_nvenc':
            app_env['CUDA_CACHE_DISABLE'] = '1'
        if env is not None and 'LD_LIBRARY_PATH' in env:
            app_env['LD_LIBRARY_PATH'] = env['LD_LIBRARY_PATH']
        stdout_target = subprocess.PIPE if self.trace and component == 'server' else (log if stdout is None else stdout)
        stderr_target = subprocess.PIPE if self.trace and component in ('sender', 'decoder') else log
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=stdout_target,
                                 stderr=stderr_target, start_new_session=True, env=app_env)
        self.children.append(child)
        if self.trace and component:
            pipe = child.stdout if component == 'server' else child.stderr
            thread = threading.Thread(target=self.watch_log,
                                      args=(pipe, log, component, self.case, child.pid), daemon=True)
            thread.start()
            self.log_threads.append(thread)
        return child

    def ffmpeg(self):
        if self.args.role == 'dell':
            root = Path(self.args.ffmpeg_root)
            libraries = str(root / 'lib')
            if self.media_profile == 'production_nvenc':
                libraries += ':' + str(self.root.parent / 'gpu-lib')
            args = [str(root / 'lib/ld-linux-x86-64.so.2'), '--inhibit-cache',
                    '--library-path', libraries]
            if self.transport_probe:
                probe = self.root.parent / 'transport_probe.so'
                if not probe.is_file():
                    raise RuntimeError('transport probe missing')
                args += ['--preload', str(probe)]
            return [*args, str(root / 'ffmpeg')]
        return [str(Path(self.args.ffmpeg_root) / 'usr/bin/ffmpeg')]

    def server_start(self):
        config = {'logLevel': 'info', 'rtmp': True, 'rtmpEncryption': 'strict',
                  'rtmpsAddress': '198.51.100.129:1936',
                  'rtmpServerKey': str(self.root / 'server.key'),
                  'rtmpServerCert': str(self.root / 'server.crt'),
                  'rtsp': True, 'rtspAddress': '127.0.0.1:18554', 'rtspTransports': ['tcp'],
                  'hls': False, 'webrtc': False, 'srt': False, 'moq': False,
                  'api': self.media_profile == 'production_nvenc', 'apiAddress': '127.0.0.1:19997',
                  'readTimeout': str(self.tuning['read_timeout']) + 's',
                  'writeTimeout': '8s', 'writeQueueSize': 128,
                  'authInternalUsers': [{'user': 'any', 'ips': ['198.51.100.130/32', '127.0.0.1/32'],
                                         'permissions': [{'action': 'publish'}, {'action': 'read'}]
                                         + ([{'action': 'api'}] if self.media_profile == 'production_nvenc' else [])}],
                  'paths': {'lab': {'source': 'publisher'}}}
        file = self.root / 'server.json'
        file.write_text(json.dumps(config))
        file.chmod(0o644)
        self.server = self.app([self.args.mediamtx, str(file)], 'server', stdout=None, component='server')
        self.emit('server_started', pid=self.server.pid, tuning=self.tuning)

    def receiver_loop(self, case):
        env = {'LD_LIBRARY_PATH': ':'.join(sorted({str(p.parent) for p in
               (Path(self.args.ffmpeg_root) / 'usr/lib').rglob('*.so*') if p.is_file()}))}
        attempt = 0
        while self.case == case and not self.stop.is_set():
            attempt += 1
            args = [*self.ffmpeg(), '-nostdin', '-hide_banner', '-loglevel', 'verbose' if self.trace else 'error',
                    '-threads', '1', '-filter_threads', '1', '-rtsp_transport', 'tcp',
                    '-timeout', '2000000', '-analyzeduration', '0', '-probesize', '32',
                    '-fflags', 'nobuffer', '-flags', 'low_delay',
                    '-i', 'rtsp://127.0.0.1:18554/lab', '-map', '0:v:0', '-an',
                    '-pix_fmt', 'gray', '-fps_mode', 'passthrough', '-f', 'rawvideo', 'pipe:1']
            if self.trace:
                args[1:1] = ['-debug_ts', '-nostats']
            if self.reader_profile in ('output_thread1', 'preserve_probe'):
                args[-1:-1] = ['-threads:v', '1']
            if self.reader_profile == 'preserve_probe':
                index = args.index('-fflags')
                del args[index:index + 2]
            child = self.app(args, f'{case}-receiver-{attempt}', stdout=subprocess.PIPE, env=env,
                             component='decoder')
            self.receiver = child
            if self.trace:
                self.emit('receiver_started', pid=child.pid, attempt=attempt, profile=self.reader_profile,
                          case=case)
            try:
                while self.case == case and not self.stop.is_set():
                    frame = child.stdout.read(WIDTH * HEIGHT)
                    if len(frame) != WIDTH * HEIGHT:
                        break
                    counter = decode_counter(frame)
                    self.last_frame = counter
                    self.emit('frame', counter=counter, decoder_pid=child.pid)
            finally:
                self.terminate(child)
                child.stdout.close()
            self.stop.wait(.2)

    def sender_start(self):
        self.generation += 1
        counter = int((time.monotonic() - self.case_origin) * FPS)
        expression = f"if(lt(Y,32),if(mod(floor((N+{counter})/pow(2,floor(X/20))),2),235,16),lum(X,Y))"
        source = f'testsrc2=s={WIDTH}x{HEIGHT}:r={FPS},geq=lum=\'{expression}\''
        args = [*self.ffmpeg(), '-nostdin', '-hide_banner', '-loglevel', 'info',
                '-threads', '1', '-filter_threads', '1', '-re', '-f', 'lavfi', '-i', source,
                '-map', '0:v:0', '-an', '-c:v', 'libx264', '-threads:v', '1', '-preset', 'ultrafast', '-tune', 'zerolatency',
                '-pix_fmt', 'yuv420p', '-flags:v', '+global_header',
                '-r', '5', '-g', str(self.sender_gop), '-keyint_min', str(self.sender_gop), '-sc_threshold', '0',
                '-b:v', '128k', '-maxrate', '128k', '-bufsize', '256k']
        if self.media_profile == 'production_nvenc':
            args = [*self.ffmpeg(), '-nostdin', '-hide_banner', '-loglevel', 'info',
                    '-fflags', '+genpts', '-threads', '1', '-filter_threads', '1',
                    '-filter_complex_threads', '1',
                    '-re', '-f', 'lavfi', '-i', 'testsrc2=size=1920x1080:rate=5',
                    '-re', '-f', 'lavfi', '-i', 'sine=frequency=997:sample_rate=48000',
                    '-filter_complex', '[0:v:0]scale=1920:1080:flags=lanczos,setsar=1[vout]',
                    '-af', 'aresample=async=1:min_hard_comp=0.030000:first_pts=0,volume=0.540680',
                    '-map', '[vout]', '-map', '1:a:0',
                    '-c:v', 'h264_nvenc', '-preset', 'p4', '-rc', 'cbr',
                    '-pix_fmt', 'yuv420p', '-flags:v', '+global_header',
                    '-r', '5', '-g', str(self.sender_gop), '-keyint_min', str(self.sender_gop), '-sc_threshold', '0',
                    '-b:v', '4000k', '-maxrate', '4000k', '-bufsize', '8000k',
                    '-c:a', 'aac', '-b:a', '192k', '-ar', '48000', '-ac', '2']
        if self.mode == 'fifo':
            args += ['-f', 'fifo', '-fifo_format', 'flv', '-attempt_recovery', '1',
                     '-recover_any_error', '1', '-recovery_wait_time', str(self.tuning['recovery_wait']),
                     '-max_recovery_attempts', '20', '-queue_size', '32',
                     '-drop_pkts_on_overflow', '1', '-restart_with_keyframe', '1',
                     '-format_opts', 'rw_timeout=' + str(self.tuning['rw_timeout_us']) + ':tls_verify=1:ca_file='
                     + str(self.root / 'server.crt')]
        else:
            args += ['-tls_verify', '1', '-ca_file', str(self.root / 'server.crt'),
                     '-rw_timeout', '15000000', '-f', 'flv']
        args += ['rtmps://198.51.100.129:1936/lab']
        if self.runtime_candidate:
            sys.path.insert(0, str(self.root.parent / 'candidate'))
            from stream_core.engine.ffmpeg_args import build_output_args
            from types import SimpleNamespace
            cfg = SimpleNamespace(test_mode=False, use_fifo_recovery=True,
                fifo_recovery_wait_sec=.2, fifo_max_recovery_attempts=20, fifo_queue_size=32,
                fifo_drop_pkts_on_overflow=True, fifo_restart_with_keyframe=True,
                rtmp_url='rtmps://198.51.100.129:1936/lab')
            os.environ['FFMPEG_RW_TIMEOUT_ENABLED'] = '1'
            os.environ['FFMPEG_RW_TIMEOUT_USEC'] = '3000000'
            output = build_output_args(cfg)
            # Only the isolated fixture CA differs from the production protocol options.
            output[output.index('-format_opts') + 1] += ':tls_verify=1:ca_file=' + str(self.root / 'server.crt')
            args = args[:args.index('-f', args.index('-ac') + 2)] + output
        if self.fifo_verbose:
            args[args.index('-loglevel') + 1] = 'verbose'
        if self.trace:
            args[len(self.ffmpeg()):len(self.ffmpeg())] = ['-debug_ts', '-nostats']
        self.sender = self.app(args, f'{self.case}-sender-{self.generation}', component='sender')
        if self.runtime_candidate:
            from stream_core.engine.transport_telemetry import TransportTelemetry, TelemetryFiles
            self.production_telemetry = TransportTelemetry(self.sender.pid, self.case)
            self.production_telemetry_sink = TelemetryFiles(self.root / 'candidate-telemetry')
        self.emit('sender_started', pid=self.sender.pid, generation=self.generation,
                  origin=self.case_origin, counter_offset=counter, mode=self.mode,
                  media_profile=self.media_profile, tuning=self.tuning)

    def owned_tcp(self, *, original=False):
        if self.args.role != 'dell' or self.sender is None or self.sender.poll() is not None:
            raise RuntimeError('owned sender is not running')
        cmd = ['ss', '-ntieOH', 'dst', '198.51.100.129', 'dport', '=', ':1936']
        if original:
            if self.stall_target is None:
                raise RuntimeError('no owned stall target')
            cmd += ['sport', '=', ':' + str(self.stall_target['source_port'])]
        raw_all = self.run_cmd(cmd).stdout.decode().strip()
        all_rows = raw_all.splitlines()
        rows = [row for row in all_rows if row.split() and row.split()[0] == 'ESTAB']
        if len(rows) != 1:
            raise RuntimeError('expected exactly one established owned RTMPS socket: ' + raw_all[:3000])
        raw = rows[0]
        fields = rows[0].split()
        if fields[0] != 'ESTAB' or fields[3].rsplit(':', 1)[0] != '198.51.100.130':
            raise RuntimeError('unexpected socket identity')
        values = dict(re.findall(r'\b([a-z_]+):([^\s]+)', raw))
        if values.get('uid') != '65534' or not values.get('ino') or not values.get('sk'):
            raise RuntimeError('owned unprivileged socket identity missing')
        return {'pid': self.sender.pid, 'generation': self.generation,
                'source_port': int(fields[3].rsplit(':', 1)[1]),
                'inode': int(values['ino']), 'cookie': values['sk'],
                'send_q': int(fields[2]), 'bytes_acked': int(values.get('bytes_acked', 0)),
                'bytes_sent': int(values.get('bytes_sent', 0)),
                'notsent': int(values.get('notsent', 0)),
                'unacked': int(values.get('unacked', 0)), 'raw': raw,
                'other_socket_states': [row.split()[0] for row in all_rows if row not in rows]}

    def blackhole_old_tcp(self):
        if self.media_profile != 'production_nvenc' or self.mode != 'fifo' or self.generation != 1:
            raise RuntimeError('silent stall requires the one owned NVENC/FIFO sender')
        if self.blackhole_active or self.stall_target is not None:
            raise RuntimeError('silent stall already armed')
        target = self.owned_tcp()
        port = target['source_port']
        rules = ("table inet fifo_lab_stall {\n"
                 " chain output { type filter hook output priority 0; policy accept;\n"
                 f"  ip saddr 198.51.100.130 ip daddr 198.51.100.129 tcp sport {port} tcp dport 1936 counter drop\n"
                 " }\n"
                 " chain input { type filter hook input priority 0; policy accept;\n"
                 f"  ip saddr 198.51.100.129 ip daddr 198.51.100.130 tcp sport 1936 tcp dport {port} counter drop\n"
                 " }\n}\n")
        if os.readlink('/proc/self/ns/net') != self.node_ns or self.node_ns == self.host_ns:
            raise RuntimeError('namespace drift before isolated firewall mutation')
        self.run_cmd(['/usr/sbin/nft', '-f', '-'], input=rules.encode())
        self.stall_target = target
        self.blackhole_active = True
        self.output_cut_done = False
        self.emit('blackhole_started', target=target)

    def cut_owned_output(self):
        if not self.blackhole_active or self.output_cut_done:
            raise RuntimeError('output cut is not armed or already consumed')
        current = self.owned_tcp(original=True)
        for key in ('pid', 'generation', 'inode', 'cookie', 'source_port'):
            if current[key] != self.stall_target[key]:
                raise RuntimeError('owned socket identity changed before cut')
        if os.readlink('/proc/self/ns/net') != self.node_ns or self.node_ns == self.host_ns:
            raise RuntimeError('namespace drift before isolated socket cut')
        # Consume the one effect before invoking the kernel; never automatically resend it.
        self.output_cut_done = True
        self.emit('output_cut_begin', target=current)
        result = self.run_cmd(['ss', '-K', 'src', '198.51.100.130', 'sport', '=',
                               ':' + str(current['source_port']), 'dst', '198.51.100.129',
                               'dport', '=', ':1936'])
        self.emit('output_cut_end', diagnostic=result.stdout.decode()[:2000])

    @staticmethod
    def terminate(child):
        if child is None or child.poll() is not None:
            return
        try:
            os.killpg(child.pid, signal.SIGTERM)
            child.wait(timeout=2)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=2)

    def case_stop(self):
        old = self.case
        self.case = None
        self.restart_at = None
        for child in (self.sender, self.receiver, self.server):
            self.terminate(child)
        if self.blackhole_active:
            self.run_cmd(['/usr/sbin/nft', 'delete', 'table', 'inet', 'fifo_lab_stall'])
        self.blackhole_active = False
        self.stall_target = None
        self.output_cut_done = False
        self.sender = self.receiver = self.server = None
        self.emit('case_stopped', stopped_case=old)

    def command(self, value):
        op = value['op']
        if op == 'peer':
            args = ['wg', 'set', 'wglab', 'peer', value['key'], 'allowed-ips',
                    '198.51.100.129/32' if self.args.role == 'dell' else '198.51.100.130/32',
                    'persistent-keepalive', '1']
            if self.args.role == 'dell':
                args += ['endpoint', '192.0.2.1:51823']
                cert = self.root / 'server.crt'
                cert.write_bytes(base64.b64decode(value['certificate']))
                cert.chmod(0o644)
            self.run_cmd(args)
            self.emit('peer_ready')
        elif op == 'ip':
            new = value['address']
            if self.args.role != 'dell' or new not in ('192.0.2.2', '198.51.100.2'):
                raise ValueError('unexpected address change')
            if new != self.source_ip:
                self.run_cmd(['ip', 'addr', 'add', new + '/32', 'dev', 'wanlab'])
                self.run_cmd(['ip', 'route', 'replace', '192.0.2.1/32', 'dev', 'wanlab', 'src', new])
                self.run_cmd(['ip', 'addr', 'del', self.source_ip + '/32', 'dev', 'wanlab'])
                self.source_ip = new
            self.emit('ip_changed', address=new)
        elif op == 'configure_wan_probes':
            if self.wan_probes_ready or self.case is not None:
                raise RuntimeError('WAN probes must be configured once before media start')
            if os.readlink('/proc/self/ns/net') != self.node_ns or self.node_ns == self.host_ns:
                raise RuntimeError('namespace drift before isolated probe setup')
            if self.args.role == 'oci':
                self.run_cmd(['ip', 'addr', 'add', '203.0.113.1/32', 'dev', 'lo'])
            else:
                self.run_cmd(['ip', 'addr', 'add', '203.0.113.254/32', 'dev', 'lo'])
                self.run_cmd(['ip', 'route', 'add', '203.0.113.1/32', 'dev', 'wanlab'])
                self.ping_targets = {'lan_gateway': '203.0.113.254',
                                     'external_isolated': '203.0.113.1',
                                     'oci_outer': '192.0.2.1',
                                     'oci_wg': '198.51.100.129'}
            self.wan_probes_ready = True
            self.ping_interval = .5
            self.report_pings = True
            self.emit('wan_probes_configured', targets=self.ping_targets,
                      ping_interval_sec=self.ping_interval)
        elif op == 'start':
            self.case_stop()
            self.case = value['case']
            self.trace = bool(value.get('trace', False))
            self.reader_profile = value.get('reader_profile', 'reference')
            if self.reader_profile not in ('reference', 'output_thread1', 'preserve_probe'):
                raise ValueError('unexpected reader profile')
            self.restart_delay = float(value.get('restart_delay', 5))
            if self.restart_delay not in (0, 5):
                raise ValueError('unexpected restart delay')
            self.sender_gop = int(value.get('sender_gop', 10))
            if self.sender_gop not in (1, 10):
                raise ValueError('unexpected experiment GOP')
            self.transport_probe = bool(value.get('transport_probe', False))
            self.decode_media = bool(value.get('decode_media', True))
            self.media_profile = value.get('media_profile', 'light')
            if self.media_profile not in ('light', 'production_nvenc'):
                raise ValueError('unexpected media profile')
            profile = value.get('tuning_profile')
            if profile is not None and (profile not in (*TUNING_PROFILES, 'production_candidate')
                                        or self.media_profile != 'production_nvenc'):
                raise ValueError('unexpected tuning profile')
            self.tuning = ({'recovery_wait': .2, 'rw_timeout_us': 3000000, 'read_timeout': 8, 'gop': 10}
                           if profile == 'production_candidate' else dict(TUNING_PROFILES[profile or 'baseline']))
            self.runtime_candidate = bool(value.get('runtime_candidate', False))
            self.production_telemetry = None
            if self.runtime_candidate and (profile != 'production_candidate' or self.args.role != 'dell'):
                raise ValueError('unexpected candidate runtime use')
            self.fifo_verbose = bool(value.get('fifo_verbose', False))
            if profile is not None:
                self.sender_gop = self.tuning['gop']
            self.bounded_sender = bool(value.get('bounded_sender', False))
            self.restart_allowed = False
            self.max_restarts = 1
            self.failed_restart_delay = 0.0
            self.ping_interval = float(value.get('ping_interval_sec', .2))
            if self.ping_interval not in (.2, .5, 1):
                raise ValueError('unexpected ping interval')
            self.report_pings = bool(value.get('report_pings', False))
            if self.transport_probe and not self.trace:
                raise ValueError('transport probe requires trace')
            self.last_frame = None
            if self.args.role == 'dell':
                self.case_origin = time.monotonic()
                self.mode = value['mode']
                self.generation = 0
                self.sender_start()
            else:
                self.server_start()
                if self.decode_media:
                    threading.Thread(target=self.receiver_loop, args=(self.case,), daemon=True).start()
        elif op == 'reset_connection':
            if self.args.role != 'oci' or self.server is None or self.server.poll() is not None:
                raise RuntimeError('owned media server is not running')
            self.emit('connection_reset_begin')
            self.run_cmd(['ss', '-K', 'dst', '198.51.100.130', 'sport', '=', ':1936'])
            self.emit('connection_reset_end')
        elif op == 'expect_restart':
            if (self.args.role != 'dell' or self.sender is None or self.mode != 'direct'
                    or self.generation != 1 or self.restart_allowed):
                raise RuntimeError('unexpected restart arming')
            maximum = value.get('max_restarts', 1)
            retry_delay = value.get('retry_wait_sec', 0)
            if type(maximum) is not int or maximum not in range(1, 21) or retry_delay not in (0, 1):
                raise ValueError('unexpected bounded restart policy')
            self.max_restarts = maximum
            self.failed_restart_delay = float(retry_delay)
            self.restart_allowed = True
            self.emit('restart_armed', max_restarts=maximum, retry_wait_sec=retry_delay,
                      initial_restart_wait_sec=self.restart_delay)
        elif op == 'blackhole_old_tcp':
            self.blackhole_old_tcp()
        elif op == 'tcp_snapshot':
            snapshot = self.owned_tcp(original=bool(value.get('original', False)))
            if self.production_telemetry is not None:
                telemetry = self.production_telemetry.observe({**snapshot,
                    'socket_cookie': snapshot['cookie'], 'socket_inode': snapshot['inode']})
                self.production_telemetry_sink.write(telemetry)
                snapshot['candidate_transport'] = telemetry
            self.emit('tcp_snapshot', **snapshot)
        elif op == 'cut_output_socket':
            self.cut_owned_output()
        elif op == 'blackhole_status':
            if not self.blackhole_active:
                raise RuntimeError('no owned blackhole')
            value = json.loads(self.run_cmd(['/usr/sbin/nft', '-j', 'list', 'table', 'inet',
                                            'fifo_lab_stall']).stdout)
            self.emit('blackhole_status', rules=value)
        elif op == 'media_status':
            if self.args.role != 'oci' or self.media_profile != 'production_nvenc':
                raise RuntimeError('media status requires isolated NVENC receiver')
            with urllib.request.urlopen('http://127.0.0.1:19997/v3/paths/get/lab', timeout=2) as response:
                value = json.load(response)
            self.emit('media_status', ready=value.get('ready'), tracks=value.get('tracks'),
                      bytes_received=value.get('bytesReceived'))
        elif op == 'stop':
            self.case_stop()
        elif op == 'restart_sender':
            if self.args.role != 'dell' or self.sender is None or self.sender.poll() is not None:
                raise RuntimeError('owned sender is not running')
            self.emit('restart_requested', pid=self.sender.pid, restart_delay=self.restart_delay)
            os.killpg(self.sender.pid, signal.SIGTERM)
        elif op == 'clock':
            self.emit('clock', token=value['token'])
        elif op == 'status':
            peers = self.run_cmd(['wg', 'show', 'wglab', 'endpoints']).stdout.decode().splitlines()
            endpoints = [line.split()[1] for line in peers]
            self.emit('status', endpoints=endpoints, last_frame=self.last_frame,
                      sender_pid=self.sender.pid if self.sender and self.sender.poll() is None else None)
        elif op == 'quit':
            self.stop.set()
        else:
            raise ValueError('unknown operation')

    def ping_setup(self):
        if self.args.role != 'dell':
            return
        self.icmp = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
        self.icmp.setblocking(False)
        self.ping_id = os.getpid() & 0xffff
        self.ping_seq = 0
        self.pings = {}

    def ping_send(self):
        if self.args.role != 'dell':
            return
        for label, address in self.ping_targets.items():
            self.ping_seq = (self.ping_seq + 1) & 0xffff
            data = struct.pack('!BBHHH', 8, 0, 0, self.ping_id, self.ping_seq) + b'fifo-wan-lab-icmp!'
            words = struct.unpack('!' + 'H' * (len(data) // 2), data)
            checksum = sum(words)
            checksum = (checksum & 0xffff) + (checksum >> 16)
            checksum = (checksum & 0xffff) + (checksum >> 16)
            data = data[:2] + struct.pack('!H', (~checksum) & 0xffff) + data[4:]
            sent = time.monotonic()
            self.pings[self.ping_seq] = (sent, label, address)
            try:
                self.icmp.sendto(data, (address, 0))
                if self.report_pings:
                    self.emit('ping_sent', target=label, sequence=self.ping_seq, sent_mono=sent)
            except OSError:
                pass
            if len(self.pings) > 512:
                self.pings.pop(next(iter(self.pings)))

    def ping_read(self):
        raw = self.icmp.recv(4096)
        n = (raw[0] & 15) * 4
        if len(raw) < n + 8:
            return
        kind, code, _, identifier, sequence = struct.unpack('!BBHHH', raw[n:n + 8])
        if kind == 0 and code == 0 and identifier == self.ping_id:
            record = self.pings.get(sequence)
            if record is not None:
                sent, label, address = record
                if socket.inet_ntoa(raw[12:16]) != address:
                    return
                del self.pings[sequence]
                self.emit('pong', target=label, sequence=sequence, sent_mono=sent,
                          rtt_sec=time.monotonic() - sent)

    def schedule_restart(self, now):
        delay = self.restart_delay
        if self.bounded_sender:
            if not self.restart_allowed or self.mode != 'direct' or self.generation > self.max_restarts:
                raise RuntimeError('owned sender exited outside its bounded expected recovery')
            if self.generation > 1:
                delay = self.failed_restart_delay
            if self.generation == self.max_restarts:
                self.restart_allowed = False
        self.restart_at = now + delay
        self.emit('restart_scheduled', exited_generation=self.generation,
                  restart_wait_sec=delay, restart_at=self.restart_at,
                  max_restarts=self.max_restarts if self.bounded_sender else None)

    def loop(self):
        self.setup()
        self.ping_setup()
        sel = selectors.DefaultSelector()
        sel.register(sys.stdin.buffer, selectors.EVENT_READ, 'input')
        sel.register(self.tun, selectors.EVENT_READ, 'tun')
        if self.args.role == 'dell':
            sel.register(self.icmp, selectors.EVENT_READ, 'icmp')
        pending = bytearray()
        ping_at = 0
        while not self.stop.is_set():
            now = time.monotonic()
            if now - self.started > self.args.max_seconds:
                self.emit('deadline')
                break
            if now >= ping_at:
                self.ping_send()
                ping_at = now + self.ping_interval
            if self.sender is not None and self.sender.poll() is not None:
                self.emit('sender_exited', pid=self.sender.pid, returncode=self.sender.returncode,
                          generation=self.generation)
                self.sender = None
                self.schedule_restart(now)
            if self.restart_at is not None and now >= self.restart_at:
                self.restart_at = None
                self.sender_start()
            for key, _ in sel.select(.05):
                if key.data == 'tun':
                    try:
                        self.send(b'P', os.read(self.tun, 65536))
                    except BlockingIOError:
                        pass
                elif key.data == 'icmp':
                    self.ping_read()
                else:
                    chunk = os.read(sys.stdin.fileno(), 65536)
                    if not chunk:
                        self.stop.set()
                        break
                    pending.extend(chunk)
                    while len(pending) >= HEADER.size:
                        kind, length = HEADER.unpack(pending[:HEADER.size])
                        if length > MAX_FRAME:
                            raise ValueError('oversized incoming frame')
                        if len(pending) < HEADER.size + length:
                            break
                        payload = bytes(pending[HEADER.size:HEADER.size + length])
                        del pending[:HEADER.size + length]
                        if kind == b'P':
                            try:
                                os.write(self.tun, payload)
                            except BlockingIOError:
                                self.emit('tun_backpressure')
                        elif kind == b'C':
                            self.command(json.loads(payload))
                        else:
                            raise ValueError('unexpected incoming kind')
        sel.close()

    def close(self):
        self.case_stop()
        for child in self.children:
            self.terminate(child)
        for thread in self.log_threads:
            thread.join(timeout=2)
        for file in self.logs:
            file.close()
        group = Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1]
        cgroot = Path('/sys/fs/cgroup') / group.lstrip('/')
        resources = {}
        for name in ('memory.peak', 'memory.events', 'cpu.stat', 'pids.peak', 'pids.events'):
            try:
                resources[name] = (cgroot / name).read_text().strip()
            except OSError:
                resources[name] = 'unavailable'
        self.emit('cleanup', all_children_exited=all(c.poll() is not None for c in self.children),
                  resources=resources)
        if hasattr(self, 'tun'):
            os.close(self.tun)
        if hasattr(self, 'icmp'):
            self.icmp.close()
        # Experiment-only private keys are not retained as evidence.
        (self.root / 'server.key').unlink(missing_ok=True)
        Path('/etc/wireguard/wg-lab.key').unlink(missing_ok=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--role', choices=('dell', 'oci'), required=True)
    p.add_argument('--root', required=True)
    p.add_argument('--ffmpeg-root', required=True)
    p.add_argument('--mediamtx', default='')
    p.add_argument('--host-netns', required=True)
    p.add_argument('--max-seconds', type=int, default=600)
    args = p.parse_args()
    node = Node(args)
    signal.signal(signal.SIGTERM, lambda *_: node.stop.set())
    signal.signal(signal.SIGINT, lambda *_: node.stop.set())
    try:
        node.loop()
    except Exception as exc:
        node.emit('error', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        node.close()


if __name__ == '__main__':
    main()
