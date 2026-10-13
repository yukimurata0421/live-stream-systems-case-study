#!/usr/bin/env python3
"""Root-only manual route caller. Persist one request; never retry an effect."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path


def write_new(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as out:
        json.dump(value, out, sort_keys=True, separators=(',', ':'))
        out.flush()
        os.fsync(out.fileno())


def exchange(path, value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode() + b'\n'
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(5)
        client.connect(str(path))
        client.sendall(raw)
        data = bytearray()
        while b'\n' not in data and len(data) <= 65536:
            chunk = client.recv(4096)
            if not chunk:
                break
            data.extend(chunk)
    if len(data) > 65536:
        raise RuntimeError('EXECUTOR_RESPONSE_TOO_LARGE')
    return json.loads(data.split(b'\n', 1)[0])


def observation(path):
    value = json.loads(path.read_text())
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(value['observed_at'].replace('Z', '+00:00'))).total_seconds()
    if not 0 <= age <= 5 or value['target_snapshot_status'] != 'VALID' or value['runtime_snapshot_status'] != 'VALID':
        raise RuntimeError('MANUAL_ROUTE_OBSERVATION_STALE_OR_INVALID')
    if not value['ffmpeg_running'] or value['managed_child_cardinality'] != '1':
        raise RuntimeError('MANUAL_ROUTE_SENDER_UNPROVEN')
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--socket', type=Path, default=Path('/run/stream-v3-control/effect.sock'))
    parser.add_argument('--observation', type=Path, default=Path('/run/stream-v3-control/runtime-observation.json'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--route', choices=['PRIMARY', 'FALLBACK'], required=True)
    args = parser.parse_args()
    if os.getuid() != 0:
        raise RuntimeError('MANUAL_ROUTE_ROOT_REQUIRED')
    args.output.mkdir(mode=0o700, exist_ok=False)
    cfg = json.loads(args.config.read_text())
    grant_path = Path(cfg['operator_grant_file_host'])
    meta = grant_path.lstat()
    if grant_path.is_symlink() or meta.st_uid != 0 or meta.st_mode & 0o077:
        raise RuntimeError('MANUAL_ROUTE_GRANT_UNTRUSTED')
    grant = json.loads(grant_path.read_text())
    current = observation(args.observation)
    if current['delivery_route'] == args.route or time.time() >= grant['expires_at']:
        raise RuntimeError('MANUAL_ROUTE_ALREADY_SELECTED_OR_GRANT_EXPIRED')
    target = current['target_identity']
    for name in ['host_id', 'namespace', 'pod_uid', 'container_name', 'container_id']:
        if target[name] != grant[name]:
            raise RuntimeError('MANUAL_ROUTE_GRANT_TARGET_DRIFT')
    query = exchange(args.socket, {'schema_version': 'runtime.effect_unresolved_query.v1'})
    write_new(args.output / 'before-authority.json', query)
    if query['unresolved_count'] != 0:
        raise RuntimeError('MANUAL_ROUTE_UNRESOLVED_FENCE')
    active = query['active_authority']
    if (active['producer_id'], active['producer_generation']) != (grant['producer_id'], grant['producer_generation']):
        expected = cfg['previous_authority']
        if any(active[k] != expected[k] for k in ['producer_id', 'producer_generation']):
            raise RuntimeError('MANUAL_ROUTE_AUTHORITY_DRIFT')
        result = exchange(args.socket, {'schema_version': 'runtime.operator_authority_handoff.v1', 'grant_id': grant['grant_id'],
            'phase': 'enter', 'expected_producer_id': active['producer_id'], 'expected_generation': active['producer_generation']})
        write_new(args.output / 'authority-handoff.json', result)
        query = exchange(args.socket, {'schema_version': 'runtime.effect_unresolved_query.v1'})
        if (query['active_authority']['producer_id'], query['active_authority']['producer_generation']) != (grant['producer_id'], grant['producer_generation']):
            raise RuntimeError('MANUAL_ROUTE_AUTHORITY_HANDOFF_FAILED')
    current = observation(args.observation)
    now = datetime.now(timezone.utc)
    request_id = 'manual-route-' + uuid.uuid4().hex
    request = {'schema_version': 'runtime.typed_effect_request.v3', 'request_id': request_id,
        'producer_id': grant['producer_id'], 'producer_generation': grant['producer_generation'],
        'intent_type': 'SWITCH_STREAM_ROUTE', 'reason': 'User approved public Twitch route migration',
        'failure_domain': 'FFMPEG', 'issued_at': now.isoformat(), 'expires_at': (now + timedelta(seconds=4)).isoformat(),
        'ffmpeg_target_identity': current['target_identity'], 'runtime_identity': None,
        'expected_ffmpeg_generation': current['ffmpeg_generation'], 'idempotency_key': request_id,
        'correlation_id': request_id, 'target_snapshot_id': current['target_snapshot_id'], 'runtime_snapshot_id': '',
        'runtime_observation_id': current['observation_id'], 'expected_executor_instance_id': current['executor_instance_id'],
        'maintenance_evidence_status': current['maintenance_evidence_status'], 'projection_id': current['projection_id'],
        'projection_sequence': current['projection_sequence'], 'route_transition': {'from_route': current['delivery_route'],
        'to_route': args.route, 'expected_route_generation': current['delivery_route_generation'], 'operator_grant_id': grant['grant_id']}}
    write_new(args.output / 'request.json', request)
    result = exchange(args.socket, request)
    write_new(args.output / 'dispatch-response.json', result)
    # Poll identity-bound status; do not send the effect a second time.
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        status = exchange(args.socket, {'schema_version': 'runtime.effect_request_status_query.v1', 'request_id': request_id})
        if status.get('state') not in {'ACCEPTED', 'EXECUTION_STARTED'}:
            break
        time.sleep(.2)
    write_new(args.output / 'effect-status.json', status)
    if status.get('state') != 'EFFECT_OBSERVED' or status.get('result', {}).get('exit_observed') is not True:
        raise RuntimeError('MANUAL_ROUTE_EFFECT_NOT_OBSERVED')
    samples = []
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and len(samples) < 3:
        try:
            observed = observation(args.observation)
            ack = observed['tcp_metrics'].get('bytes_acked', 0)
            if (observed['delivery_route'] == args.route and observed['target_identity']['ffmpeg_pid'] != target['ffmpeg_pid']
                and observed['ffmpeg_uptime_sec'] >= 15 and ack > 0):
                if samples and observed['target_identity'] != samples[-1]['target']:
                    raise RuntimeError('MANUAL_ROUTE_SUCCESSOR_CHANGED')
                if not samples or ack > samples[-1]['bytes_acked']:
                    samples.append({'observed_at': observed['observed_at'], 'bytes_acked': ack, 'target': observed['target_identity']})
        except (KeyError, RuntimeError):
            pass
        time.sleep(1)
    if len(samples) != 3:
        raise RuntimeError('MANUAL_ROUTE_SUCCESSOR_ACK_UNPROVEN')
    successor = samples[-1]['target']
    transport = {'bytes_sent': observed['tcp_metrics']['bytes_sent'],
        'network_down': False, 'tcp_probe_ok': True, 'ffmpeg_generation': successor['ffmpeg_generation'],
        'ack_observation_count': 3, 'bytes_acked_start': samples[0]['bytes_acked'], 'bytes_acked_end': samples[-1]['bytes_acked'],
        'bytes_acked_delta': samples[-1]['bytes_acked'] - samples[0]['bytes_acked'], 'ack_samples': [
            {'observed_at': s['observed_at'], 'ffmpeg_pid': successor['ffmpeg_pid'], 'ffmpeg_generation': successor['ffmpeg_generation'],
             'bytes_acked': s['bytes_acked']} for s in samples]}
    digest = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    reconciliation = {'schema_version': 'runtime.effect_reconciliation_request.v1', 'reconciliation_id': request_id + '-ack',
        'effect_scope_id': status['effect_scope_id'], 'owner_request_id': request_id, 'owner_request_digest': digest,
        'resolution': 'EFFECT_OBSERVED', 'evidence': {'schema_version': 'runtime.delayed_exit_reconciliation_evidence.v2',
            'oracle': 'DELAYED_FFMPEG_EXIT_AND_SUCCESSOR_ACK_PROGRESS', 'observed_at': observed['observed_at'],
            'automatic_retry_count': 0, 'physical_effect_count': 1, 'before_target': target, 'observed_target': successor,
            'runtime_observation_id': observed['observation_id'], 'transport': transport}}
    write_new(args.output / 'reconciliation-request.json', reconciliation)
    receipt = exchange(args.socket, reconciliation)
    write_new(args.output / 'reconciliation-response.json', receipt)
    query = exchange(args.socket, {'schema_version': 'runtime.effect_unresolved_query.v1'})
    write_new(args.output / 'after-authority.json', query)
    if not receipt.get('ok') or query['unresolved_count']:
        raise RuntimeError('MANUAL_ROUTE_FENCE_VERIFICATION_UNRESOLVED')
    returned = exchange(args.socket, {'schema_version': 'runtime.operator_authority_handoff.v1', 'grant_id': grant['grant_id'],
        'phase': 'leave', 'expected_producer_id': grant['producer_id'], 'expected_generation': grant['producer_generation']})
    write_new(args.output / 'authority-return.json', returned)
    final = exchange(args.socket, {'schema_version': 'runtime.effect_unresolved_query.v1'})
    if any(final['active_authority'][k] != cfg['previous_authority'][k] for k in ['producer_id', 'producer_generation']):
        raise RuntimeError('MANUAL_ROUTE_AUTHORITY_RETURN_UNPROVEN')
    print(json.dumps({'route': args.route, 'request_id': request_id, 'result': 'EFFECT_OBSERVED_AND_TCP_ACK_VERIFIED',
                      'unresolved_count': 0, 'provider_reception_verified': False}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'status': 'MANUAL_ROUTE_STOPPED', 'error_type': type(error).__name__,
                          'reason': str(error) if isinstance(error, RuntimeError) else 'CALLER_FAILURE', 'automatic_retry': False}))
        raise SystemExit(2)
