"""Admission of measurement warnings; source/SLO qualification stays unchanged."""
from __future__ import annotations

import math

BAD_SECONDS = 180.
WINDOW_SECONDS = 900.
STABLE_SECONDS = 180.
MAX_GAP_SECONDS = 120.
MAX_POINTS = 128


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def reconcile(items, active, state, *, now, conditions=None):
    """Ignore isolated loss; detect recurring loss without crossing host clocks.

    Checks carry two independent facts: whether the source is fresh now and
    whether the original recovery window is complete. Warmup alone never opens
    a warning; an already open episode closes only on its full recovery proof.
    """
    slots = state.setdefault('measurement_continuity', {})
    if not isinstance(slots,dict):
        slots = state['measurement_continuity'] = {}
    result = []
    seen = set()
    if conditions is not None:
        items = [{**item, '_measurement_condition':conditions[item['id']]}
                 if item.get('id') in conditions else item for item in items]
        present = {item.get('id') for item in items}
        items += [{'id':ident, '_measurement_condition':value, '_no_incident':True}
                  for ident,value in conditions.items() if ident not in present]
    for item in items:
        condition = item.get('_measurement_condition')
        if not isinstance(condition, dict):
            result.append(item)
            continue
        ident = item['id']; seen.add(ident)
        fresh = condition.get('fresh') is True
        recovered = condition.get('recovered') is True
        old = slots.get(ident, {})
        if not isinstance(old,dict):old={}
        points = old.get('points', []) if old.get('boot') == now.boot else []
        if (not isinstance(points, list) or len(points) > MAX_POINTS or any(
                not isinstance(p, list) or len(p) != 2 or not number(p[0])
                or type(p[1]) is not bool or p[0] > now.mono for p in points)
                or any(a[0] >= b[0] for a,b in zip(points,points[1:]))):
            points = []
        if points and now.mono-points[-1][0] > MAX_GAP_SECONDS:
            points = []
        # Re-running a decision at one monotonic instant earns no duration.
        if not points or now.mono > points[-1][0]:
            points.append([now.mono, not fresh])
        cutoff = now.mono-WINDOW_SECONDS
        points = [p for i,p in enumerate(points) if p[0]>=cutoff or i+1<len(points) and points[i+1][0]>cutoff][-MAX_POINTS:]
        bad_seconds = sum(max(0., b[0]-max(a[0],cutoff)) for a,b in zip(points,points[1:]) if a[1])
        good_since = now.mono
        if fresh:
            for stamp,bad in reversed(points):
                if bad:break
                good_since = stamp
            if now.mono-good_since >= STABLE_SECONDS:
                points = [[good_since, False], [now.mono, False]]
                bad_seconds = 0.
        slots[ident] = {'boot':now.boot,'points':points,'bad_seconds':bad_seconds}
        if ident in active:
            if recovered:
                continue
            previous = active[ident]
            # A recovery wait does not renew the last observed failure time.
            if fresh or item.get('_no_incident'):
                item = {**previous.get('last_incident',item),
                        'observed_ts':previous.get('last_bad_ts',item.get('observed_ts')),
                        '_observation_pending':True}
            result.append(item)
        elif not fresh and bad_seconds >= BAD_SECONDS and not item.get('_no_incident'):
            result.append(item)
    # Missing owner output is not proof of recovery for an existing episode.
    for ident, previous in active.items():
        if ident in slots and ident not in seen and previous.get('last_incident'):
            result.append({**previous['last_incident'], '_observation_pending':True,
                           'observed_ts':previous.get('last_bad_ts')})
    return result


def apply(items, active, state, *, root, now):
    from stream_core.common import upload_prometheus_source
    from .upload_source import current_upload_source
    try:
        if upload_prometheus_source.selected(root) is None:
            return items  # Only an explicit legacy selection retains old policy.
    except (OSError, ValueError):
        pass  # Missing selection is an observation failure, never legacy mode.
    assessment = current_upload_source(root, int(now.wall))
    conditions = {}
    for ident, key in [('reliability:upload_ceiling_coverage_or_freshness','measurement'),
                       ('reliability:upload_history_replication','history')]:
        fact = assessment.get(key, {})
        conditions[ident] = {'fresh':fact.get('fresh') is True,
                             'recovered':fact.get('sustained') is True}
    return reconcile(items, active, state, now=now, conditions=conditions)
