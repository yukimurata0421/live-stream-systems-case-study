#!/usr/bin/env python3
"""Check slate, live input, loss and reconnection using real loopback media and the existing copy bridge."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request

from offline_slate import candidate_config, digest, encode, write_json
import importlib.util
_observer_path = Path(__file__).resolve().parents[2] / 'monitoring-v4/src/stream_monitoring_v4/compatibility/relay_observe.py'
_observer_spec = importlib.util.spec_from_file_location('public_slate_relay_observe', _observer_path)
_observer = importlib.util.module_from_spec(_observer_spec)
_observer_spec.loader.exec_module(_observer)
input_facts = _observer.input_facts

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def api(port, path):
    with OPENER.open(f'http://127.0.0.1:{port}{path}', timeout=2) as response:
        return json.load(response)


def wait_for(read, predicate, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = read()
            if predicate(value):
                return value
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(.1)
    raise TimeoutError('local_observation_deadline')


class OwnedProcesses:
    def __init__(self, root):
        self.root, self.items = root, []

    def start(self, name, args):
        # No control credentials, inherited provider environment or home mounts.
        env = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'GOMAXPROCS': '1', 'GOMEMLIMIT': '64MiB'}
        with (self.root / (name + '.stderr')).open('xb') as log:
            process = subprocess.Popen(args, cwd=self.root, env=env, stdin=subprocess.DEVNULL,
                stdout=log, stderr=log, start_new_session=True)
        self.items.append(process)
        return process

    @staticmethod
    def stop(process):
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGINT)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=3)

    def close(self):
        self.cleanup_errors = []
        for process in reversed(self.items):
            try:
                self.stop(process)
            except (OSError, subprocess.TimeoutExpired) as error:
                self.cleanup_errors.append(type(error).__name__)
        return all(process.poll() is not None for process in self.items)


def configuration(rtmp, api_port, paths):
    return {'logLevel': 'warn', 'rtsp': False, 'hls': False, 'webrtc': False, 'srt': False, 'moq': False,
            'rtmp': True, 'rtmpAddress': f'127.0.0.1:{rtmp}', 'readTimeout': '8s',
            'api': True, 'apiAddress': f'127.0.0.1:{api_port}', 'paths': paths}


def analyze(path, expected=None):
    probe = subprocess.run(['ffprobe', '-v', 'error', '-show_streams', '-show_packets',
        '-show_entries', 'stream=index,codec_name:packet=stream_index,dts_time', '-of', 'json', str(path)],
        check=True, capture_output=True, timeout=15)
    data = json.loads(probe.stdout)
    if sorted(s['codec_name'] for s in data['streams']) != ['aac', 'h264']:
        raise ValueError('captured_tracks_invalid')
    last = {}
    for packet in data['packets']:
        track, dts = packet['stream_index'], float(packet['dts_time'])
        if track in last and dts < last[track]:
            raise ValueError('captured_timestamp_regression')
        last[track] = dts
    raw = subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-threads', '1', '-filter_threads', '1',
        '-i', str(path), '-map', '0:v:0', '-vf', 'fps=2,scale=192:108', '-pix_fmt', 'rgb24',
        '-f', 'rawvideo', '-threads', '1', 'pipe:1'], check=True, capture_output=True, timeout=40).stdout
    size, sequence = 192 * 108 * 3, []
    for offset in range(0, len(raw), size):
        frame = raw[offset:offset + size]
        if len(frame) != size:
            raise ValueError('incomplete_decoded_frame')
        green = sum(g > 60 and g > r * 1.5 and g > b * 1.5
                    for r, g, b in zip(frame[0::3], frame[1::3], frame[2::3]))
        state = 'LIVE' if green / (192 * 108) > .9 else 'OFFLINE_SLATE'
        if not sequence or sequence[-1] != state:
            sequence.append(state)
    expected = expected if expected is not None else ['OFFLINE_SLATE', 'LIVE', 'OFFLINE_SLATE', 'LIVE']
    if sequence[:len(expected)] != expected:
        raise ValueError('decoded_source_sequence_invalid: ' + repr(sequence))
    # Decode the complete AAC track as well; packet presence alone is insufficient.
    subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-threads', '1', '-i', str(path),
        '-map', '0:a:0', '-f', 'null', '-'], check=True, capture_output=True, timeout=20)
    return {'decoded_sequence': sequence, 'video_frames_sampled': len(raw) // size,
            'audio_decoded': True, 'timestamps_monotonic': True}


def exercise(root, media, mediamtx, youtube_copy):
    root.mkdir(parents=True, exist_ok=False)
    owned = OwnedProcesses(root)
    report = {'schema': 'stream_v3.offline_slate_lab.v1', 'passed': False,
              'production_mutated': False, 'provider_contacted': False, 'observations': [],
              'binary_sha256': {'mediamtx': digest(mediamtx), 'youtube_copy': digest(youtube_copy)}}
    try:
        version = subprocess.check_output([str(mediamtx), '--version'], timeout=5, text=True).strip()
        if version != 'v1.21.2':
            raise ValueError('mediamtx_version_mismatch')
        ports = {k: free_port() for k in ('sink', 'sink_api', 'relay', 'relay_api', 'copy')}
        sink = lambda path: f"rtmp://127.0.0.1:{ports['sink']}/{path}"
        bridge = f"rtmp://127.0.0.1:{ports['copy']}/live/youtube"
        relay_url = f"rtmp://127.0.0.1:{ports['relay']}/relay"
        write_json(root / 'sink.json', configuration(ports['sink'], ports['sink_api'],
                    {'youtube': {'source': 'publisher'}, 'twitch': {'source': 'publisher'}}))
        base = configuration(ports['relay'], ports['relay_api'],
                    {'relay': {'source': 'publisher', 'forward': [{'dest': bridge}, {'dest': sink('twitch')}]}})
        write_json(root / 'relay.json', candidate_config(base, str(media)))
        write_json(root / 'copy.json', {'destination': sink('youtube')})
        # Build before starting any real-time streams, within the same CPU limit.
        ppm = root / 'source.ppm'
        ppm.write_bytes(b'P6\n1920 1080\n255\n' + bytes((20, 100, 45)) * (1920 * 1080))
        encode(ppm, root / 'source.mp4')
        owned.start('sink', [str(mediamtx), str(root / 'sink.json')])
        wait_for(lambda: api(ports['sink_api'], '/v3/paths/list'), lambda _: True)
        copy_state = root / 'copy-state.json'
        owned.start('copy', [str(youtube_copy), '-listen', f"127.0.0.1:{ports['copy']}",
            '-config', str(root / 'copy.json'), '-state', str(copy_state), '-allow-local-output'])
        wait_for(lambda: json.loads(copy_state.read_text()), lambda x: x['state'] == 'LISTENING')
        owned.start('relay', [str(mediamtx), str(root / 'relay.json')])
        path = lambda: api(ports['relay_api'], '/v3/paths/get/relay')
        wait_for(path, lambda x: x['ready'] and not x['online'])
        captures = []
        ids = {}
        for name in ('youtube', 'twitch'):
            value = wait_for(lambda: api(ports['sink_api'], '/v3/paths/get/' + name), lambda x: x['ready'])
            ids[name] = value['source']['id']
            captures.append(owned.start('capture-' + name, ['ffmpeg', '-nostdin', '-v', 'error',
                '-threads', '1', '-rw_timeout', '4000000', '-analyzeduration', '5000000',
                '-probesize', '5000000', '-i', sink(name), '-map', '0:v:0', '-map', '0:a:0',
                '-c', 'copy', '-flush_packets', '1', '-n', str(root / (name + '.flv'))]))
        for name in ids:
            capture = root / (name + '.flv')
            wait_for(lambda: capture.stat().st_size, lambda n: n > 0, seconds=15)
        state = wait_for(lambda: json.loads(copy_state.read_text()),
                         lambda x: x['state'] == 'FLOWING' and x['audio_packets'] > 0)
        session = state['session']

        def observe(expected):
            raw = path()
            row = input_facts(raw)
            if row['content_state'] != expected or row['ready'] != (expected == 'LIVE'):
                raise ValueError('input_observation_mismatch')
            current = json.loads(copy_state.read_text())
            if current['session'] != session or current['state'] != 'FLOWING':
                raise ValueError('copy_connection_changed')
            for name in ids:
                if api(ports['sink_api'], '/v3/paths/get/' + name)['source']['id'] != ids[name]:
                    raise ValueError('destination_connection_changed')
            if any(p.poll() is not None for p in captures):
                raise ValueError('continuous_capture_ended')
            report['observations'].append({'content_state': expected, 'input_ready': row['ready'],
                'copy_session': current['session'], 'video_packets': current['video_packets'],
                'audio_packets': current['audio_packets']})

        time.sleep(2)
        observe('OFFLINE_SLATE')
        def publish(name):
            return owned.start(name, ['ffmpeg', '-nostdin', '-v', 'error', '-threads', '1', '-re',
                '-stream_loop', '-1', '-i', str(root / 'source.mp4'), '-map', '0:v:0', '-map', '0:a:0',
                '-c', 'copy', '-flush_packets', '1', '-flvflags', 'no_duration_filesize', '-f', 'flv', relay_url])
        publisher = publish('publisher1')
        wait_for(path, lambda x: x['online'])
        time.sleep(3)
        observe('LIVE')
        owned.stop(publisher)
        wait_for(path, lambda x: not x['online'])
        time.sleep(9)  # More than two complete four-second offline loops.
        observe('OFFLINE_SLATE')
        publisher = publish('publisher2')
        wait_for(path, lambda x: x['online'])
        time.sleep(3)
        observe('LIVE')
        for process in captures:
            owned.stop(process)
        owned.stop(publisher)
        report['destinations'] = {name: analyze(root / (name + '.flv')) for name in ids}
        report['passed'] = True
    except BaseException as error:
        report['failure_class'] = type(error).__name__
        report['failure'] = str(error)
        raise
    finally:
        report['owned_processes_stopped'] = owned.close()
        report['cleanup_errors'] = owned.cleanup_errors
        write_json(root / 'result.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--media', required=True, type=Path)
    parser.add_argument('--mediamtx', required=True, type=Path)
    parser.add_argument('--youtube-copy', required=True, type=Path)
    args = parser.parse_args()
    report = exercise(args.output.resolve(), args.media.resolve(), args.mediamtx.resolve(), args.youtube_copy.resolve())
    print(json.dumps({'passed': report['passed'], 'owned_processes_stopped': report['owned_processes_stopped']}))


if __name__ == '__main__':
    main()
