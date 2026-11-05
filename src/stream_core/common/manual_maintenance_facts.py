"""Read-only, allowlisted facts from the delivery host's root-owned maintenance session.

This module is also sent over the existing host-key-verified SSH observation
transport. It has no control entrypoint, credentials or effect dispatch.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import stat
import time

MAINTENANCE_FACT_SCHEMA = 'stream_v3.manual_maintenance_facts.v1'
MAINTENANCE_PHASES = {'NORMAL', 'PREPARING', 'AWAITING_SLATE_CONFIRMATION',
                      'MAINTENANCE', 'RESUMING', 'VERIFYING', 'RESTORING'}


def _maintenance_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def project_manual_maintenance(value, *, boot, machine_sha256, observed_epoch):
    phase, generation = value['phase'], value['generation']
    if (value.get('schema') != 'stream_v3.manual_maintenance.v1'
            or phase not in MAINTENANCE_PHASES or type(generation) is not int or generation < 0):
        raise ValueError('maintenance_session_invalid')
    result = {'schema': MAINTENANCE_FACT_SCHEMA, 'installed': True, 'available': True,
              'source_host_id': 'delivery-host', 'source_boot_id': boot,
              'observed_epoch': observed_epoch, 'phase': phase, 'generation': generation,
              'session_id': value.get('session_id'), 'expected_main_stopped': False,
              'overdue': False}
    if phase == 'NORMAL':
        # A new notification reader can start after maintenance has completed.
        # Export the validated historical stop interval for context only; NORMAL
        # never grants admission to suppress a current incident.
        try:
            history = project_manual_maintenance({**value, 'phase':'VERIFYING'},
                boot=boot, machine_sha256=machine_sha256, observed_epoch=observed_epoch)
            if 'source_resumed_at' in history:
                result.update({k:history[k] for k in ('source_stopped_at','source_resumed_at')})
        except (ValueError, TypeError, KeyError, StopIteration, AttributeError):
            pass
        return result
    session, deadline = value['session_id'], value['deadline']
    before = value['before']
    effects = value['effects']
    if (not isinstance(session, str) or not 1 <= len(session) <= 128
            or not _maintenance_number(deadline) or deadline <= 0
            or before['dell']['identity']['machine'] != machine_sha256
            or not isinstance(before['dell']['identity']['boot'], str)
            or not 1 <= len(before['dell']['identity']['boot']) <= 160
            or not isinstance(effects, list) or len(effects) > 16
            or any(e.get('result') != 'APPLIED' for e in effects)):
        raise ValueError('maintenance_session_unresolved_or_unbound')
    result.update(deadline=deadline, overdue=observed_epoch >= deadline,
                  session_source_boot_id=before['dell']['identity']['boot'],
                  main_deployment_uid=before['dell']['main']['uid'],
                  oci_boot_id=before['oci']['identity']['boot'])
    if any(not isinstance(result[key], str) or not 1 <= len(result[key]) <= 160
           for key in ('main_deployment_uid', 'oci_boot_id')):
        raise ValueError('maintenance_target_identity_invalid')
    stopped = next((e for e in effects if e.get('name') == 'source_stop'), None)
    resumed = next((e for e in effects if e.get('name') == 'source_resume'), None)
    if stopped is not None:
        if (stopped.get('role') != 'dell' or stopped.get('action') != 'scale_main'
                or stopped.get('request', {}).get('uid') != result['main_deployment_uid']
                or stopped.get('request', {}).get('after') != 0
                or not _maintenance_number(stopped.get('at'))
                or not 0 < deadline - stopped['at'] <= 86400
                or stopped['at'] > observed_epoch + 1):
            raise ValueError('maintenance_stop_not_bound')
        result['source_stopped_at'] = stopped['at']
    if resumed is not None:
        if (stopped is None or resumed.get('role') != 'dell'
                or resumed.get('action') != 'scale_main'
                or resumed.get('request', {}).get('uid') != result['main_deployment_uid']
                or resumed.get('request', {}).get('after') != 1
                or not _maintenance_number(resumed.get('at'))
                or not stopped['at'] <= resumed['at'] <= observed_epoch + 1):
            raise ValueError('maintenance_resume_not_bound')
        result['source_resumed_at'] = resumed['at']
    if phase in {'AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE'}:
        stopped = next(e for e in effects if e['name'] == 'source_stop')
        armed = next(e for e in effects if e['name'] == 'arm_slate')
        request = stopped['request']
        if (stopped['role'] != 'dell' or stopped['action'] != 'scale_main'
                or request['uid'] != result['main_deployment_uid'] or request['after'] != 0
                or armed['role'] != 'oci' or armed['action'] != 'path_patch'
                or armed['request']['after'] != value['armed_fields']
                or value['armed_fields'].get('alwaysAvailable') is not True
                or not _maintenance_number(stopped['at']) or stopped['at'] > observed_epoch + 1
                or not 0 < deadline - stopped['at'] <= 86400):
            raise ValueError('maintenance_stop_not_bound')
        result.update(expected_main_stopped=True, source_stopped_at=stopped['at'])
    return result


def read_manual_maintenance(root=Path('/var/lib/stream-v3-manual-maintenance'), *, uid=0):
    now = time.time()
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    unavailable = {'schema': MAINTENANCE_FACT_SCHEMA, 'installed': True, 'available': False,
                   'source_host_id': 'delivery-host', 'source_boot_id': boot,
                   'observed_epoch': now, 'reason': 'maintenance_state_unverifiable'}
    try:
        info = root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != uid or info.st_mode & 0o022:
            return unavailable
        fd = os.open(root/'session.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as file:
            info = os.fstat(file.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != uid
                    or info.st_mode & 0o022 or info.st_size > 65536):
                return unavailable
            raw = file.read(65537)
        if len(raw) > 65536:
            return unavailable
        value = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        machine = hashlib.sha256(Path('/etc/machine-id').read_text().strip().encode()).hexdigest()
        result = project_manual_maintenance(value, boot=boot, machine_sha256=machine, observed_epoch=now)
        result['state_sha256'] = hashlib.sha256(raw).hexdigest()
        result['notification_stops'] = notification_stops(root, value, boot=boot,
            machine_sha256=machine, observed_epoch=now, uid=uid)
        return result
    except FileNotFoundError:
        return {**unavailable, 'installed': root.exists(), 'reason': 'maintenance_not_installed_or_missing'}
    except (OSError, ValueError, TypeError, KeyError, StopIteration, AttributeError):
        return unavailable


def notification_stops(root, current, *, boot, machine_sha256, observed_epoch, uid=0):
    """Bounded root history; only armed, operator-confirmed, applied stop/resume."""
    values = [current]
    history = root/'history'
    try:
        info = history.lstat()
        if stat.S_ISDIR(info.st_mode) and info.st_uid == uid and not info.st_mode & 0o022:
            paths = sorted(history.glob('*.json'), key=lambda p:p.lstat().st_mtime, reverse=True)[:16]
            for path in paths:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, 'rb') as file:
                    info = os.fstat(file.fileno())
                    if (not stat.S_ISREG(info.st_mode) or info.st_uid != uid
                            or info.st_mode & 0o022 or info.st_size > 65536):
                        continue
                    raw = file.read(65537)
                if len(raw) <= 65536:
                    values.append(json.loads(raw))
    except (OSError, ValueError, TypeError):
        pass  # Unverifiable history never creates an exclusion.
    stops = {}
    for value in values:
        try:
            if value.get('phase') not in {'NORMAL','RESUMING','VERIFYING','RESTORING'}:
                continue
            f = project_manual_maintenance({**value, 'phase':'MAINTENANCE'},
                boot=boot, machine_sha256=machine_sha256, observed_epoch=observed_epoch)
            resumed = f['source_resumed_at']
            confirmation = value['slate_player_confirmation']
            if (confirmation.get('kind') != 'operator_attestation'
                    or confirmation.get('youtube') is not True or confirmation.get('twitch') is not True
                    or not _maintenance_number(confirmation.get('at'))
                    or not f['source_stopped_at'] <= confirmation['at'] <= resumed
                    or not 0 <= observed_epoch-resumed <= 21900):
                continue
            row = {k:f[k] for k in ('session_id','generation','source_stopped_at','source_resumed_at')}
            stops[f['session_id']] = row
        except (ValueError, TypeError, KeyError, StopIteration, AttributeError):
            continue
    return sorted(stops.values(), key=lambda r:r['source_stopped_at'])


def remote_reader_source():
    """Use this exact reader in the existing fixed SSH program, without a copy."""
    return Path(__file__).read_text() + '\n'
