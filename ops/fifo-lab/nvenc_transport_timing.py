#!/usr/bin/env python3
"""Compare synthetic AV with the recorded production encoding profile in isolated processes."""
from __future__ import annotations

import argparse
import datetime
import json
import subprocess
import threading
import time

from transport_timing import TransportLab


class NvencTransportLab(TransportLab):
    memory_max = '768M'

    def __init__(self, args):
        super().__init__(args)
        self.guard_stop = threading.Event()
        self.guard_error = None
        self.guard_thread = None

    def extra_properties(self, role):
        if role != 'dell':
            return []
        return ['DeviceAllow=' + p + ' rw' for p in
                ('/dev/nvidia0', '/dev/nvidiactl', '/dev/nvidia-uvm')]

    def command(self, role, **value):
        if value.get('op') == 'start':
            value['media_profile'] = 'production_nvenc'
            value['bounded_sender'] = True
        super().command(role, **value)

    def production_state(self):
        raw = subprocess.check_output(['sudo', '-n', 'cat',
                                       self.args.guard_observation], timeout=3)
        return json.loads(raw)

    def guard(self, baseline):
        last_bytes = baseline['tcp_metrics']['bytes_acked']
        last_progress = time.monotonic()
        try:
            while not self.guard_stop.is_set():
                state = self.production_state()
                now = time.monotonic()
                for key in ('ffmpeg_generation', 'local_ffmpeg_pid', 'pod_uid'):
                    if state[key] != baseline[key]:
                        raise RuntimeError('production identity changed: ' + key)
                if not state['ffmpeg_running']:
                    raise RuntimeError('production FFmpeg stopped')
                age = time.time() - datetime.datetime.fromisoformat(
                    state['observed_at'].replace('Z', '+00:00')).timestamp()
                if age > 10:
                    raise RuntimeError('production observation became stale')
                acked = state['tcp_metrics']['bytes_acked']
                if acked < last_bytes:
                    raise RuntimeError('production transport counter regressed')
                if acked > last_bytes:
                    last_progress, last_bytes = now, acked
                if now - last_progress > 6:
                    raise RuntimeError('production ACK stopped advancing for 6 seconds')
                line = subprocess.check_output([
                    'nvidia-smi', '--query-gpu=memory.used,utilization.gpu,utilization.encoder,'
                    'temperature.gpu,encoder.stats.sessionCount',
                    '--format=csv,noheader,nounits'], text=True, timeout=3).strip()
                memory, gpu, encoder, temp, sessions = map(float, line.split(','))
                if memory > 2048 or gpu > 50 or encoder > 50 or temp >= 75 or sessions > 2:
                    raise RuntimeError('shared GPU stop threshold reached')
                sample = {'mono': now, 'observed_at': state['observed_at'], 'ack_bytes': acked,
                          'production_pid': state['local_ffmpeg_pid'], 'gpu_memory_mib': memory,
                          'gpu_util_percent': gpu, 'encoder_util_percent': encoder,
                          'gpu_temperature_c': temp, 'encoder_sessions': sessions}
                with (self.root / 'production-guard.jsonl').open('a') as file:
                    file.write(json.dumps(sample) + '\n')
                self.guard_stop.wait(1)
        except Exception as exc:
            self.guard_error = f'{type(exc).__name__}: {exc}'
            (self.root / 'guard-stop.json').write_text(json.dumps({'error': self.guard_error}) + '\n')

    def start(self):
        baseline = self.production_state()
        (self.root / 'guard-baseline.json').write_text(json.dumps(baseline, indent=2) + '\n')
        self.guard_thread = threading.Thread(target=self.guard, args=(baseline,), daemon=True)
        self.guard_thread.start()
        super().start()

    def pump(self, seconds=.05, *, tolerate_close=False):
        if self.guard_error and not tolerate_close:
            raise RuntimeError('stopping owned experiment: ' + self.guard_error)
        return super().pump(seconds, tolerate_close=tolerate_close)

    def validate_media(self, case, pid, after):
        video = self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                          and e.get('app_pid') == pid and e.get('stage') == 'video_mux_packet', 15)
        audio = self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                          and e.get('app_pid') == pid and e.get('stage') == 'audio_mux_packet', 15)
        observations = []
        for _ in range(2):
            sent = time.monotonic()
            self.command('oci', op='media_status')
            status = self.wait(lambda e: e['kind'] == 'media_status' and e.get('case') == case
                               and self.local_time(e) > sent, 5)
            assert status['ready'] and len(status['tracks']) == 2, status
            assert 'H264' in status['tracks'], status
            observations.append(status)
            self.duration(.5)
        assert observations[1]['bytes_received'] > observations[0]['bytes_received']
        return {'received_tracks': observations[-1]['tracks'], 'receiver_bytes_advanced': True,
                'first_video_mux_after_error_sec': video['mono'] - after if video['mono'] > after else None,
                'first_audio_mux_after_error_sec': audio['mono'] - after if audio['mono'] > after else None,
                'media_profile': 'production_nvenc',
                'receiver_rate_mbps': (observations[1]['bytes_received'] - observations[0]['bytes_received'])
                    * 8 / (observations[1]['mono'] - observations[0]['mono']) / 1e6}

    def close(self):
        try:
            super().close()
        finally:
            self.guard_stop.set()
            if self.guard_thread:
                self.guard_thread.join(timeout=4)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--guard-observation', required=True, help='Read-only runtime observation to protect during the experiment')
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--mediamtx', required=True)
    p.add_argument('--pairs', type=int, choices=range(1, 4), default=3)
    args = p.parse_args()
    lab = NvencTransportLab(args)
    try:
        lab.start()
        for rep in range(1, args.pairs + 1):
            for mode in (('direct', 'fifo') if rep % 2 else ('fifo', 'direct')):
                lab.run_transport_case(mode, rep)
        lab.summarize()
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
