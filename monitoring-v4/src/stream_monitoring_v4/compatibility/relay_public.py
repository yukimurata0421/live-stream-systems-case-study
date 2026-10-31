"""Print only the arena-sanitized public relay artifact for the Pi publisher.

Since 2026-10-20 it adds `fallback`: whether the direct YouTube fallback Deployment is running. If that
cannot be read, the fields are null and the relay artifact still prints. While the fallback runs, its upload
rate comes from the fallback FFmpeg's :443 bytes_acked, compared with the previous call (moved here on
2026-10-21 so the encoder host's controller needs no exec permission).
"""
import json
import importlib.util
import secrets
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import build_opener, ProxyHandler


NAMESPACE = 'stream-v3'
FALLBACK = 'stream-v3-youtube-backup'
STATE = Path(os.environ.get('RELAY_PUBLIC_STATE_DIR', str(Path.home() / '.cache' / 'stream-v4-relay-public'))) / 'fallback-sample.json'
CONTROLLER_FIELDS = ('observed_at', 'health', 'reason', 'healthy_for_sec', 'stop_after_sec',
                     'healthy_resets_total', 'last_reset_at', 'last_reset_reason')
CONTROLLER_REASONS = frozenset(('progress_confirmed', 'baseline_required', 'observation_unavailable',
    'facts_invalid', 'schema_invalid', 'host_binding_mismatch', 'timestamp_invalid',
    'source_future', 'source_stale', 'copy_future', 'copy_stale', 'progress_future', 'progress_stale',
    'copy_facts_missing', 'copy_binding_mismatch', 'copy_not_flowing', 'copy_identity_invalid',
    'input_not_live', 'connection_changed', 'sample_gap', 'counter_invalid', 'video_not_advancing',
    'audio_not_advancing', 'input_not_ready', 'input_not_advancing', 'output_not_forwarding',
    'output_not_advancing', 'facts_http_unavailable', 'facts_timeout', 'facts_transport_error', 'clock_unavailable', 'counter_regressed', 'local_transport_stalled', 'local_fifo_recovering'))


def public_controller(value, now):
    """Project only fixed vocabulary; stale facts never become current."""
    try:
        if value['schema'] != 'stream_v3.fallback_controller.v1':
            return None
        out = {key: value[key] for key in CONTROLLER_FIELDS}
        observed = datetime.fromisoformat(out['observed_at'].replace('Z', '+00:00'))
        if observed.tzinfo is None or not -1 <= now - observed.timestamp() <= 90:
            return None
        if out['health'] not in ('HEALTHY', 'UNHEALTHY', 'UNKNOWN') or out['reason'] not in CONTROLLER_REASONS:
            return None
        if (out['health'] == 'HEALTHY') != (out['reason'] == 'progress_confirmed'):
            return None
        duration = out['healthy_for_sec']
        if type(duration) not in (int, float) or not math.isfinite(duration) or not 0 <= duration <= 1e12:
            return None
        if out['health'] != 'HEALTHY' and duration != 0:
            return None
        if type(out['stop_after_sec']) is not int or out['stop_after_sec'] != 300:
            return None
        if type(out['healthy_resets_total']) is not int or not 0 <= out['healthy_resets_total'] <= 1e12:
            return None
        reset, reason = out['last_reset_at'], out['last_reset_reason']
        if (reset is None) != (reason is None):
            return None
        if reason is not None:
            parsed = datetime.fromisoformat(reset.replace('Z', '+00:00'))
            if parsed.tzinfo is None or parsed.timestamp() > observed.timestamp() + 1 or reason not in CONTROLLER_REASONS:
                return None
        return {'schema': 'stream_v3.fallback_controller_public.v1', **out}
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        return None


def load_clock():
    spec = importlib.util.spec_from_file_location('stream_clock_contract', Path(__file__).with_name('clock_contract.py'))
    clock = importlib.util.module_from_spec(spec); sys.modules[spec.name] = clock; spec.loader.exec_module(clock)
    return clock


def controller_status():
    try:
        clock = load_clock(); nonce = secrets.token_hex(16); sent = clock.capture()
        result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'ConnectTimeout=2', 'encoder-host', 'python3', '-I', '-B',
            '/opt/stream-clock/current/clock_observer.py', '--host-id', 'encoder-host',
            '--fallback-exchange', nonce], capture_output=True, timeout=3, check=True)
        received = clock.capture()
        if len(result.stdout) > 16384: return None
        packet = json.loads(result.stdout)
        machine = json.loads(Path(__file__).with_name('clock-peers.json').read_text())['encoder_machine_sha256']
        model = clock.estimate(packet['clock'], nonce, sent, received, machine=machine)
        value = packet['value']
        for key in ('observed_at', 'last_reset_at'):
            if value.get(key) is not None:
                original = datetime.fromisoformat(value[key].replace('Z', '+00:00')).timestamp()
                age = model.age_bounds(original)[1]
                if key == 'observed_at' and not model.fresh(original, 90): return None
                value[key] = datetime.fromtimestamp(time.time()-age, timezone.utc).isoformat()
        return public_controller(value, time.time())
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        return None


def kubectl(*args):
    r = subprocess.run(['kubectl', '-n', NAMESPACE, *args], capture_output=True, text=True, timeout=8)
    if r.returncode:
        raise ValueError('KUBECTL_FAILED')
    return r.stdout


def upload_rate(previous, current):
    """Compute Mbps across two samples of the same Pod; reject resets or invalid gaps."""
    if not previous or not current or previous.get('pod') != current.get('pod'):
        return None
    seconds = current['at'] - previous['at']
    delta = current['bytes'] - previous['bytes']
    if not 5 <= seconds <= 600 or delta < 0:
        return None
    return round(delta * 8 / seconds / 1e6, 3)


def fallback_sample():
    """Read acknowledged :443 bytes from fallback FFmpeg, or return None."""
    try:
        pod = kubectl('get', 'pod', '-l', 'app.kubernetes.io/name=' + FALLBACK, '--field-selector=status.phase=Running',
                      '-o', 'jsonpath={.items[0].metadata.name}').strip()
        if not pod:
            return None
        out = kubectl('exec', pod, '-c', 'stream-engine', '--', 'ss', '-tni', 'state', 'established', '( dport = :443 )')
        acked = [int(x) for x in re.findall(r'bytes_acked:(\d+)', out)]
        return {'pod': pod, 'bytes': max(acked), 'at': time.time()} if acked else None
    except Exception:
        return None


def fallback_rate():
    current = fallback_sample()
    try:
        previous = json.loads(STATE.read_text())
    except Exception:
        previous = None
    if current:
        try:
            STATE.parent.mkdir(parents=True, exist_ok=True)
            tmp = STATE.with_suffix('.tmp'); tmp.write_text(json.dumps(current)); os.replace(tmp, STATE)
        except OSError:
            pass
    return upload_rate(previous, current)


def fallback():
    observed = datetime.now(timezone.utc).isoformat()
    try:
        r = subprocess.run(['kubectl', '-n', 'stream-v3', 'get', 'deploy', 'stream-v3-youtube-backup', '-o', 'json'],
                           capture_output=True, text=True, timeout=5)
        if r.returncode:
            raise ValueError()
        d = json.loads(r.stdout)
        desired, ready = int(d['spec'].get('replicas') or 0), int(d.get('status', {}).get('readyReplicas') or 0)
        out = {'active': desired >= 1, 'ready': ready >= 1, 'observed_at': observed}
        if desired >= 1:
            mbps = fallback_rate()
            out['mbps'] = mbps if mbps is not None and 0 <= mbps <= 10000 else None
        else:
            STATE.unlink(missing_ok=True)
        return out
    except Exception:
        return {'active': None, 'ready': None, 'observed_at': observed}


if __name__ == '__main__':
    try:
        clock = load_clock(); started = clock.capture()
        nonce = sys.argv[2] if len(sys.argv) == 3 and sys.argv[1] == '--clock-nonce' else None
        if nonce is not None and not clock.valid_nonce(nonce): raise ValueError()
        with build_opener(ProxyHandler({})).open('http://127.0.0.1:9116/public', timeout=3) as r:
            raw = r.read(16385)
        if len(raw) > 16384:
            raise ValueError()
        value = json.loads(raw)
        value['fallback'] = fallback()
        value['fallback']['controller'] = controller_status()
        try:
            import route_causality
            causal=route_causality.guard()
            value['fallback']['causality']={k:causal[k] for k in ('status','assessment','cause_id','secondary_recovery_blocked')}
        except (ImportError,KeyError,ValueError):pass
        if nonce is not None:
            value = {'schema':'stream.clock_payload.v1','value':value,'clock':clock.envelope(nonce,started)}
        sys.stdout.write(json.dumps(value, allow_nan=False))
    except Exception:
        raise SystemExit(2)
