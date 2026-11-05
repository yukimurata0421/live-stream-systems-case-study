"""Bounded notification admission for an observed, intentional main stop."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat

from stream_core.common import observation_time as timing
from stream_core.common.manual_maintenance_facts import MAINTENANCE_FACT_SCHEMA
from stream_core.common.timeutil import jst_text

DEFERRED_IDS = frozenset({'reliability:upload_ceiling_coverage_or_freshness',
                          'reliability:upload_history_replication'})
SOURCE_TTL = 35.
OUTPUT_WAIT = 90.


def _read_bundle(root):
    try:
        fd = os.open(Path(root)/'oci_monitor/observation.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as file:
            if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
                return {}
            raw = file.read(65537)
        return json.loads(raw) if len(raw) <= 65536 else {}
    except (OSError, ValueError):
        return {}


def _alert(kind, now):
    return {'id':'manual_maintenance:'+kind, 'severity':'critical',
            'component':'manual_maintenance', 'summary':kind,
            'evidence':'manual maintenance observation requires attention',
            'recovery_type':'human_maintenance_reconciliation',
            'follow_up':'Check slate outputs, the maintenance deadline and authoritative state',
            'observed_ts':int(now.wall)}


def owns_notifications(root, state):
    """An active manual session must not enter the legacy blanket-mute branch."""
    bundle = _read_bundle(root)
    facts = bundle.get('manual_maintenance') if isinstance(bundle, dict) else None
    slot = state.get('manual_maintenance_notifications', {})
    return (isinstance(slot, dict) and slot.get('phase') not in (None, 'NORMAL')
            or isinstance(facts, dict) and facts.get('installed') is True
            and (facts.get('available') is not True or facts.get('phase') != 'NORMAL'))


def _output(bundle, facts, age):
    native = bundle.get('native_delivery', {})
    if native.get('source_boot_id') != facts.get('oci_boot_id'):
        return 'BAD'
    observed = timing.epoch(native.get('observed_at_utc'))
    stopped = facts.get('source_stopped_at')
    if (not timing.finite(stopped) or observed is None or observed < stopped-1
            or native.get('fresh') is not True
            or not timing.finite(native.get('source_age_seconds'))
            or not 0 <= native['source_age_seconds']+age <= 90):
        return 'PENDING'
    for target in ('youtube', 'twitch'):
        row = native.get('destinations', {}).get(target, {})
        if row.get('classification') == 'target_bad' or row.get('flowing') is False:
            return 'BAD'
        if row.get('state') != 'forwarding' or row.get('flowing') is not True:
            return 'PENDING'
    contract = bundle.get('observer_contract', {})
    row = bundle.get('state', {}).get('targets', {}).get('oci_relay', {})
    captured = timing.epoch(row.get('captured_at_utc'))
    if (contract.get('runtime_attested') is not True or contract.get('compatible') is not True
            or row.get('source_binding_ok') is not True or row.get('capture_ok') is not True
            or captured is None or not -1 <= facts['observed_epoch']-captured+age <= 30):
        return 'PENDING'
    return 'GOOD'


def assess(bundle, state, *, now):
    """Return eligible IDs and independent maintenance alarms; never recovery effects."""
    slot = state.setdefault('manual_maintenance_notifications', {})
    if not isinstance(slot, dict):
        slot = state['manual_maintenance_notifications'] = {}
    facts = bundle.get('manual_maintenance') if isinstance(bundle, dict) else None
    prior_active = slot.get('phase') not in (None, 'NORMAL')
    if facts is None or isinstance(facts, dict) and facts.get('installed') is False:
        return set(), [_alert('observation_unavailable', now)] if prior_active else []
    try:
        sample = timing.qualify(facts.get('observation_time'), now=now,
                                max_age=SOURCE_TTL, source='manual_maintenance')
        if (not sample.valid or facts.get('available') is not True
                or facts.get('schema') != MAINTENANCE_FACT_SCHEMA
                or facts.get('source_host_id') != 'delivery-host'
                or facts.get('source_boot_id') != bundle.get('state', {}).get('source_boot_id')
                or type(facts.get('generation')) is not int or facts['generation'] < 0
                or facts['generation'] < slot.get('generation', 0)):
            raise ValueError()
        phase = facts['phase']
        if phase not in {'NORMAL', 'PREPARING', 'AWAITING_SLATE_CONFIRMATION',
                         'MAINTENANCE', 'RESUMING', 'VERIFYING', 'RESTORING'}:
            raise ValueError()
        # A delivery-host reboot can be intentional maintenance. Current boot still
        # has to match the fresh observer; the durable session and lease do not
        # restart with it. The source reader binds the persistent machine ID.
        identity = [facts['source_host_id'], facts['generation'], facts.get('session_id')]
        old_identity = slot.get('identity')
        if (isinstance(old_identity, list) and facts['generation'] == slot.get('generation')
                and identity[1:] != old_identity[1:]):
            raise ValueError()
        same = identity == slot.get('identity')
        if phase == 'NORMAL':
            slot.update(identity=identity, generation=facts['generation'], phase=phase,
                        deferred_ids=[], output='NOT_REQUIRED')
            stopped, resumed = facts.get('source_stopped_at'), facts.get('source_resumed_at')
            if (timing.finite(stopped) and timing.finite(resumed)
                    and stopped <= resumed <= facts.get('observed_epoch', 0)+1
                    and 0 <= now.wall-resumed <= 21600):
                slot['stop_window'] = {'stopped':stopped,'resumed':resumed}
            return set(), []
        if not timing.finite(facts.get('deadline')) or not timing.finite(facts.get('observed_epoch')):
            raise ValueError()
        remaining = facts['deadline']-facts['observed_epoch']-sample.age_upper
        if remaining > 86400 or same and facts['deadline'] != slot.get('deadline'):
            raise ValueError()
        same_boot = slot.get('receiver_boot') == now.boot
        expires = now.mono+remaining
        if same and same_boot and timing.finite(slot.get('expires_mono')):
            expires = min(expires, slot['expires_mono'])  # no lease extension by polling or UTC steps
        since = slot.get('active_since_mono') if same and same_boot else now.mono
        if not timing.finite(since) or not 0 <= since <= now.mono:
            raise ValueError()
        slot.update(identity=identity, generation=facts['generation'], phase=phase,
                    receiver_boot=now.boot, deadline=facts['deadline'], expires_mono=expires,
                    active_since_mono=since, deferred_ids=[])
        stopped, resumed = facts.get('source_stopped_at'), facts.get('source_resumed_at')
        if timing.finite(stopped) and 0 <= facts['observed_epoch']-stopped <= 86400:
            slot['stop_window'] = {'stopped':stopped,
                'resumed':resumed if timing.finite(resumed) and stopped <= resumed <= facts['observed_epoch']+1 else None}
        if facts.get('overdue') is True or now.mono >= expires:
            return set(), [_alert('deadline_exceeded', now)]
        if phase not in {'AWAITING_SLATE_CONFIRMATION', 'MAINTENANCE'}:
            return set(), []
        main = facts.get('main_status', {})
        if (facts.get('expected_main_stopped') is not True
                or main.get('uid') != facts.get('main_deployment_uid')
                or type(main.get('replicas')) is not int or main['replicas'] != 0
                or type(main.get('active_pods')) is not int or main['active_pods'] != 0):
            return set(), [_alert('main_stop_not_confirmed', now)]
        output = _output(bundle, facts, sample.age_upper)
        was_good = slot.get('output') == 'GOOD'
        slot['output'] = output
        if output == 'BAD' or output != 'GOOD' and (was_good or now.mono-since >= OUTPUT_WAIT):
            return set(), [_alert('slate_output_unavailable', now)]
        if output != 'GOOD':
            return set(), []
        return set(DEFERRED_IDS), []
    except (ValueError, TypeError, KeyError, AttributeError):
        return set(), [_alert('observation_unavailable', now)]


def apply(items, active, state, *, root, now=None):
    bundle = _read_bundle(root)
    # The publisher can replace the file while other incidents are collected.
    # Capture the receiver clock after reading to avoid rejecting a new local
    # sample as future-dated. Explicit readings are for deterministic replay.
    now = timing.capture() if now is None else now
    deferred, alerts = assess(bundle, state, now=now)
    slot = state['manual_maintenance_notifications']
    if deferred:  # Fresh bound session, main absent, and both slate outputs verified.
        for item in items:
            ident = item.get('id')
            evidence = item.get('evidence', '')
            if ident == 'map:delivery_critical' and isinstance(evidence, str):
                reasons = evidence.partition('reasons=')[2].split(',')
                if set(reasons) <= {'deployment_ready', 'pod_topology_ready',
                                    'runtime_readiness', 'render_heartbeat', 'browser_contract'}:
                    deferred.add(ident)
            if (ident == 'stream:current_fail' and isinstance(evidence, str)
                    and set(evidence.split()) == {'youtube_status=ok', 'youtube_judgment=ok',
                                                 'youtube_stats_stale=True', 'pulse_pass=True'}):
                deferred.add(ident)
    deferred -= set(active)  # an existing incident still requires its original recovery proof
    slot['deferred_ids'] = sorted(deferred)
    window = slot.get('stop_window', {})
    stopped, resumed = window.get('stopped'), window.get('resumed')
    context = None
    if (timing.finite(stopped) and timing.finite(resumed)
            and 0 <= now.wall-resumed <= 21600):
        context = f"Planned main stop history={jst_text(int(stopped))} → {jst_text(int(resumed))}; historical values and recovery conditions are preserved"
    elif (slot.get('phase') not in (None, 'NORMAL') and timing.finite(slot.get('expires_mono'))
          and slot.get('receiver_boot') == now.boot and now.mono < slot['expires_mono']):
        context = f"Planned maintenance phase={slot['phase']}; independent faults and historical measurements remain monitored"
    result = []
    for item in items:
        if item.get('id') in deferred:
            continue
        if context and (str(item.get('id', '')).startswith('reliability:')
                        or item.get('id') in {'map:delivery_critical', 'stream:current_fail'}):
            item = {**item, 'diagnostic_context':(str(item.get('diagnostic_context', ''))+' '+context).strip()}
        result.append(item)
    return [*result, *alerts], deferred


def notification_plan(root):
    """Facts for notification-only arithmetic; no incident or formal SLI writes."""
    bundle = _read_bundle(root)
    now = timing.capture()
    state = {}
    deferred, alerts = assess(bundle, state, now=now)
    facts = bundle.get('manual_maintenance', {})
    if alerts or state.get('manual_maintenance_notifications', {}).get('phase') is None:
        return None
    stops = facts.get('notification_stops')
    if not isinstance(stops, list) or len(stops) > 17:
        return None
    if deferred:
        stops = [*stops, {'session_id':facts['session_id'], 'generation':facts['generation'],
            'source_stopped_at':facts['source_stopped_at'], 'source_resumed_at':None}]
    return {'schema':'stream.notification_maintenance_plan.v1', 'stops':stops,
        'active_verified':bool(deferred), 'observed_epoch':facts['observed_epoch'],
        'qualified_epoch':now.wall,
        'source_sample_id':facts['observation_time']['sample_id']}
