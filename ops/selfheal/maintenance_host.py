#!/usr/bin/env python3
"""Host adapter for manual maintenance: fixed targets and read-only fence checks."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import time
import urllib.parse
import urllib.request

TIMER = 'stream-v3-fast-recovery-controller.timer'
SERVICE = 'stream-v3-fast-recovery-controller.service'
FIELDS = ('alwaysAvailable', 'alwaysAvailableFile', 'alwaysAvailableTracks')
API = 'http://127.0.0.1:9997'
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def run(argv, *, check=True):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=25,
                            env={'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin', 'LANG': 'C.UTF-8'})
    require(not check or result.returncode == 0, 'host_command_failed')
    return result.stdout.strip()


def bounded_json(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= 65536, 'invalid_observation_file')
        raw = handle.read(65537)
    require(len(raw) <= 65536, 'oversized_observation')
    return json.loads(raw)


def identity():
    machine = Path('/etc/machine-id').read_text().strip()
    return {'machine': hashlib.sha256(machine.encode()).hexdigest(),
            'boot': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def api(path, payload=None):
    request = urllib.request.Request(API + path, data=None if payload is None else json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json'}, method='GET' if payload is None else 'PATCH')
    with OPENER.open(request, timeout=5) as response:
        raw = response.read(65537)
    require(len(raw) <= 65536, 'oversized_mediamtx_response')
    return json.loads(raw) if raw else {}


def cloud_snapshot():
    config = api('/v3/config/paths/get/relay')
    require(config.get('source') == 'publisher' and len(config.get('forward', [])) == 2, 'unexpected_relay_layout')
    stable = {k: v for k, v in config.items() if k not in FIELDS}
    path = api('/v3/paths/get/relay')
    destinations = api('/v3/paths/forward-dests/list?path=relay').get('items', [])
    twitch = [d for d in destinations if (urllib.parse.urlsplit((d.get('conf') or {}).get('dest', '')).hostname or '').endswith(
        ('.live-video.net', '.twitch.tv'))]
    tw = twitch[0] if len(twitch) == 1 else {}
    copy = bounded_json('/run/stream-v3-youtube-forward/observation.json')
    content = ('LIVE' if path.get('ready') and path.get('online') is True and (path.get('source') or {}).get('id')
               else 'MAINTENANCE_SLATE' if path.get('ready') and path.get('online') is False else 'UNKNOWN')
    return {'identity': identity(), 'observed_at': time.time(),
            'path_fields': {k: config[k] for k in FIELDS},
            'config_fingerprint': hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest(),
            'content': content, 'input_id': (path.get('source') or {}).get('id'),
            'tracks': path.get('tracks'),
            'twitch': {'id': tw.get('id'), 'state': tw.get('state'), 'bytes': tw.get('outboundBytes'),
                       'error': bool(tw.get('lastError'))},
            'copy': {k: copy.get(k) for k in ('schema', 'boot_id', 'pid', 'session', 'state', 'updated_at',
                'last_progress_at', 'destination_fingerprint', 'output_bytes', 'video_packets', 'audio_packets')}}


def units():
    result = {}
    for unit in (TIMER, SERVICE):
        values = dict(line.split('=', 1) for line in run(['systemctl', 'show', unit,
            '-p', 'LoadState', '-p', 'ActiveState', '-p', 'UnitFileState']).splitlines() if '=' in line)
        require(values.get('LoadState') == 'loaded', 'recovery_unit_not_loaded')
        require(values.get('ActiveState') in ('active', 'inactive', 'failed'), 'recovery_unit_transitioning')
        require(values.get('UnitFileState') in ('enabled', 'disabled', 'static'), 'unsupported_unit_enablement')
        result[unit] = {'active': values['ActiveState'] == 'active', 'enabled': values['UnitFileState']}
    return result


def kube(*args):
    return json.loads(run(['k3s', 'kubectl', '-n', 'stream-v3', *args, '-o', 'json']))


def workload(name):
    value = kube('get', 'deployment', name)
    labels = value['spec']['selector']['matchLabels']
    selector = ','.join(k + '=' + v for k, v in sorted(labels.items()))
    replicasets = kube('get', 'replicasets', '-l', selector)['items']
    owned = {r['metadata']['uid'] for r in replicasets if any(
        o['uid'] == value['metadata']['uid'] for o in r['metadata'].get('ownerReferences', []))}
    pods = kube('get', 'pods', '-l', selector)['items']
    require(all(any(o['uid'] in owned for o in p['metadata'].get('ownerReferences', [])) for p in pods),
            'unowned_pod_matches_selector')
    active = [p for p in pods if p['status'].get('phase') not in ('Succeeded', 'Failed')]
    ready = [p for p in active if not p['metadata'].get('deletionTimestamp') and any(
        c.get('type') == 'Ready' and c.get('status') == 'True' for c in p['status'].get('conditions', []))]
    return {'uid': value['metadata']['uid'], 'replicas': value['spec'].get('replicas', 1),
            'active_pods': len(active), 'ready_pods': len(ready)}


def ledger(path):
    # Read-only compatibility with the existing target-wide fence; never initialize,
    # retire, release, reconcile or switch authority here.
    path = Path(path)
    require(path.is_absolute() and path.is_file() and not path.is_symlink(), 'effect_ledger_missing')
    connection = sqlite3.connect('file:' + urllib.parse.quote(str(path), safe='/') + '?mode=ro', uri=True, timeout=2)
    try:
        connection.execute('PRAGMA query_only=ON')
        row = connection.execute('SELECT producer_id,producer_generation,authority_version FROM runtime_authority WHERE singleton=1').fetchone()
        require(row is not None, 'authority_missing')
        fences = connection.execute("SELECT count(*) FROM effect_scope_fences WHERE state IN "
            "('ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED','OUTCOME_UNKNOWN','EFFECT_OBSERVED_AWAITING_VERIFICATION')").fetchone()[0]
        pending = 0
        for table in ('effect_requests', 'typed_effect_requests'):
            pending += connection.execute('SELECT count(*) FROM ' + table + " r LEFT JOIN effect_scope_fences f ON f.owner_request_id=r.request_id "
                "WHERE r.state IN ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN') AND coalesce(f.state,'') NOT IN "
                "('RECONCILED_EFFECT_OBSERVED','RELEASED_NO_EFFECT','RETIRED_TARGET_OUTCOME_UNKNOWN')").fetchone()[0]
        return {'producer_id': row[0], 'generation': row[1], 'authority_version': row[2],
                'unresolved': fences + pending}
    finally:
        connection.close()


def local_snapshot(config):
    status = bounded_json('/run/stream-v3-fallback-controller/status.json')
    return {'identity': identity(), 'observed_at': time.time(), 'units': units(),
            'main': workload('stream-v3-runtime'), 'backup': workload('stream-v3-youtube-backup'),
            'ledger': ledger(config['effect_ledger_file']),
            'controller': {k: status.get(k) for k in ('observed_at', 'health', 'maintenance_guard_supported', 'maintenance_root')}}


def verify_asset(path, expected, manifest_expected, *, trusted_uid=0):
    path = Path(path)
    require(path.is_absolute() and path.resolve() == path and path.suffix == '.mp4', 'invalid_slate_path')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == trusted_uid and not info.st_mode & 0o022
                and info.st_mode & 0o004 and 0 < info.st_size <= 16 * 1024 * 1024, 'untrusted_slate')
        require(hashlib.file_digest(handle, 'sha256').hexdigest() == expected, 'slate_hash_mismatch')
    # Probe the bundle during build and decode it in validation before install.
    # Pin both
    # exact media and its manifest so the relay needs no multimedia packages.
    manifest_path = path.with_name('manifest.json')
    fd = os.open(manifest_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == trusted_uid and not info.st_mode & 0o022
                and 0 < info.st_size <= 65536, 'untrusted_slate_manifest')
        raw = handle.read(65537)
    require(hashlib.sha256(raw).hexdigest() == manifest_expected, 'slate_manifest_hash_mismatch')
    manifest = json.loads(raw)
    require(manifest.get('schema') == 'stream_v3.offline_slate_bundle.v1'
            and manifest.get('content_state') == 'MAINTENANCE_SLATE' and manifest.get('language') == 'en'
            and manifest.get('files', {}).get('offline.mp4') == expected, 'slate_manifest_binding_invalid')
    media = manifest.get('media', {})
    required = {'video': 'H264', 'audio': 'AAC', 'width': 1920, 'height': 1080, 'fps': 5,
                'sample_rate': 48000, 'channels': 2}
    require(all(media.get(k) == v for k, v in required.items())
            and type(media.get('duration_seconds')) in (int, float)
            and 3.9 <= media['duration_seconds'] <= 4.2, 'slate_media_contract_invalid')


def handle(request):
    require(os.geteuid() == 0, 'root_required')
    config, role, action = request['config'], request['role'], request['action']
    current = identity()
    require(role in ('dell', 'oci') and current['machine'] == config[role + '_machine_sha256'], 'physical_host_mismatch')
    if request.get('expected_boot'):
        require(current['boot'] == request['expected_boot'], 'host_rebooted_during_effect')
    if role == 'oci':
        if action in ('snapshot', 'preflight'):
            if action == 'preflight':
                verify_asset(config['slate_file'], config['slate_sha256'], config['slate_manifest_sha256'])
            return cloud_snapshot()
        require(action == 'path_patch', 'unknown_cloud_action')
        verify_asset(config['slate_file'], config['slate_sha256'], config['slate_manifest_sha256'])
        before = cloud_snapshot()
        require(before['config_fingerprint'] == request['config_fingerprint'], 'relay_config_drift')
        require(before['path_fields'] == request['before'], 'relay_path_fields_drift')
        patch = request['after']
        require(set(patch) == set(FIELDS), 'unexpected_patch_fields')
        api('/v3/config/paths/patch/relay', patch)
        return cloud_snapshot()
    if action in ('snapshot', 'preflight'):
        return local_snapshot(config)
    require(action in ('quiesce', 'restore_units', 'scale_main'), 'unknown_local_action')
    found = ledger(config['effect_ledger_file'])
    require(found == request['ledger'] and found['unresolved'] == 0
            and found['producer_id'] == 'independent-controller', 'authority_or_fence_not_quiescent')
    if action == 'quiesce':
        require(units() == request['before'], 'recovery_units_drift')
        run(['systemctl', 'disable', TIMER])
        run(['systemctl', 'stop', TIMER])
        run(['systemctl', 'stop', SERVICE])
    elif action == 'restore_units':
        before = request['restore']
        run(['systemctl', 'enable' if before[TIMER]['enabled'] == 'enabled' else 'disable', TIMER])
        if before[TIMER]['active']:
            run(['systemctl', 'start', TIMER])
        # The oneshot service is resumed by its original timer, never launched here.
    else:
        before = workload('stream-v3-runtime')
        require(before['uid'] == request['uid'] and before['replicas'] == request['before'], 'main_workload_drift')
        require(request['after'] in (0, 1), 'replica_count_not_allowed')
        patch = [{'op': 'test', 'path': '/metadata/uid', 'value': before['uid']},
                 {'op': 'test', 'path': '/spec/replicas', 'value': before['replicas']},
                 {'op': 'replace', 'path': '/spec/replicas', 'value': request['after']}]
        run(['k3s', 'kubectl', '-n', 'stream-v3', 'patch', 'deployment', 'stream-v3-runtime',
             '--type=json', '-p', json.dumps(patch)])
    return local_snapshot(config)


def main():
    try:
        raw = sys.stdin.buffer.read(65537)
        require(len(raw) <= 65536, 'oversized_request')
        print(json.dumps({'ok': True, 'value': handle(json.loads(raw))}, allow_nan=False))
    except Exception as error:
        # No command stderr, raw config, destination URL or credentials.
        reason = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(json.dumps({'ok': False, 'reason': reason[:100]}))
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
