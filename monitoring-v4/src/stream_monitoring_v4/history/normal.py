from __future__ import annotations

import json
import hashlib
import re
from typing import Any

from stream_contracts.monitoring_v4.ids import canonical_json
from stream_contracts.monitoring_v4.time import unix_ts, utc_text
from stream_monitoring_v4.adapters.factory import SHADOW_DOMAINS
from stream_monitoring_v4.reporting.policy import SOURCE_CADENCES
from stream_monitoring_v4.runtime.source_revision import UNKNOWN_SOURCE_REVISION

from .rolling import DEFINITION, WINDOWS, record_slot, read_windows, series_identity
from .snapshot import read_snapshot

NORMAL_REPORT_SCHEMA = 'monitoring_v4.normal_report.v1'
DEFAULT_TARGET = 'stream-v3'


def stage_normal_cycle(connection, *, result, cycle_id: str, completed_ts: int,
                       build_revision: str, source_revision: str, projection_status: dict,
                       target: str = DEFAULT_TARGET) -> dict[str, Any]:
    sampled = unix_ts(result.observer.started_at)
    observer = result.observer.to_dict()
    adapters = {item['source']: item for item in observer['source_results']}
    collector_states = {
        name: 'good' if adapters.get(name, {}).get('outcome') == 'observed' else 'unknown'
        for name in adapters
    }
    facts = [event for item in observer['source_results'] for event in item['observation_events']]
    source_states = {}
    configured_sources = set(SOURCE_CADENCES)
    if 'youtube_api_direct_state' not in adapters:
        configured_sources -= {'youtube_api_direct_lifecycle', 'youtube_input_quality_api_direct'}
    for source in sorted(configured_sources):
        events = [e for e in facts if e['source'] == source]
        fresh = [e for e in events if 0 <= completed_ts - unix_ts(e['observed_at']) <= e['freshness_limit_sec']]
        source_states[source] = 'good' if fresh else 'unknown'
    currents = {item.domain: item.state for item in result.currents}
    deadlines = [unix_ts(item.valid_until) for item in result.currents]
    for source in configured_sources:
        valid_events = [e for e in facts if e['source'] == source
                        and 0 <= completed_ts - unix_ts(e['observed_at']) <= e['freshness_limit_sec']]
        if valid_events:
            deadlines.append(max(unix_ts(e['observed_at']) + e['freshness_limit_sec'] for e in valid_events))
    facts_deadline = min(deadlines, default=completed_ts)
    health = {
        'collectors_complete': (bool(collector_states) and len(collector_states) == observer['adapters']
                                and all(s == 'good' for s in collector_states.values())),
        'sources_fresh': all(s == 'good' for s in source_states.values()),
        'no_rejections': observer['inserted_rejections'] == 0,
        'no_timeouts': not observer['timed_out_sources'],
        'currents_complete': set(currents) == set(SHADOW_DOMAINS),
        'currents_known': all(s in ('good', 'bad') for s in currents.values()),
        'currents_fresh': facts_deadline >= completed_ts,
        'projection_complete': projection_status['complete'] is True,
    }
    metrics = {'monitoring.cycle': 'good' if all(health.values()) else 'unknown'}
    metrics.update({f'collector.{k}': v for k, v in collector_states.items()})
    metrics.update({f'facts.{k}': v for k, v in source_states.items()})
    metrics.update({f'current.{k}': v for k, v in currents.items()})
    definitions = {metric: DEFINITION for metric in metrics}
    for item in result.currents:
        definitions[f'current.{item.domain}'] = f'current-minute.v1:{item.policy_revision}:{item.reducer_revision}'
    definitions['monitoring.cycle'] = DEFINITION + ':' + hashlib.sha256(
        canonical_json(definitions).encode()).hexdigest()[:16]
    active_series = []
    for metric, status in sorted(metrics.items()):
        record_slot(connection, metric=metric, target=target, sample_ts=sampled,
                    event_id=cycle_id, status=status, now_ts=completed_ts,
                    definition_revision=definitions[metric])
        active_series.append(series_identity(metric, target, definitions[metric]))
    payload = {
        'cycle_id': cycle_id, 'started_at': result.observer.started_at,
        'completed_at': utc_text(completed_ts), 'build_revision': build_revision,
        'source_revision': source_revision, 'health_checks': health,
        'active_series_ids': active_series, 'facts_valid_until_ts': facts_deadline,
        'current_states': currents, 'collector_states': collector_states,
        'source_freshness': source_states, 'projection_integrity': projection_status,
        'safety_boundary': {'real_delivery_enabled': False, 'runtime_mutation_enabled': False,
                            'raspberry_pi_dependency': False},
    }
    connection.execute(
        '''INSERT INTO normal_cycle_current(target, cycle_id, completed_ts, payload_json)
           VALUES (?, ?, ?, ?) ON CONFLICT(target) DO UPDATE SET cycle_id=excluded.cycle_id,
           completed_ts=excluded.completed_ts, payload_json=excluded.payload_json
           WHERE excluded.completed_ts >= normal_cycle_current.completed_ts''',
        (target, cycle_id, completed_ts, canonical_json(payload)),
    )
    return payload


def build_normal_report(repository, *, now_ts: int, build_revision: str,
                        source_revision: str, target: str = DEFAULT_TARGET,
                        max_cycle_age_sec: int = 180) -> dict[str, Any]:
    # Never inspect raw shadow_cycles / observation JSON or the qualification report.
    with read_snapshot(repository) as connection:
        row = connection.execute('SELECT * FROM normal_cycle_current WHERE target=?', (target,)).fetchone()
        windows = read_windows(connection, target=target)
    latest = json.loads(row['payload_json']) if row else {}
    active = latest.get('active_series_ids', [])
    windows = [w for w in windows if w['series_id'] in active]
    aggregates_present = (bool(active) and len(active) == len(set(active))
                          and {(w['series_id'], w['window_sec']) for w in windows}
                          == {(series, size) for series in active for size in WINDOWS})
    completed_ts = int(row['completed_ts']) if row else None
    age = now_ts - completed_ts if completed_ts is not None else None
    checks = {
        'cycle_present': bool(row),
        'cycle_fresh': age is not None and 0 <= age <= max_cycle_age_sec,
        'cycle_build_matches': latest.get('build_revision') == build_revision,
        'cycle_source_matches': latest.get('source_revision') == source_revision,
        'facts_currently_valid': isinstance(latest.get('facts_valid_until_ts'), int) and now_ts <= latest['facts_valid_until_ts'],
        'normal_pipeline_clean': bool(latest.get('health_checks')) and all(latest.get('health_checks', {}).values()),
        'aggregates_present': aggregates_present,
        'aggregates_fresh': bool(windows) and all(
            0 <= now_ts // 60 * 60 - w['end_ts'] <= max_cycle_age_sec for w in windows
        ),
    }
    return {
        'schema': NORMAL_REPORT_SCHEMA, 'generated_at': utc_text(now_ts),
        'build_revision': build_revision,
        'build_revision_immutable': bool(re.fullmatch('[0-9a-f]{40}', build_revision)),
        'source_revision_known': bool(source_revision and source_revision != UNKNOWN_SOURCE_REVISION),
        'source_revision': source_revision, 'target': target,
        'health_checks': checks, 'healthy': all(checks.values()),
        'latest_cycle': latest, 'rolling': windows,
        'rolling_basis': 'completed received-cycle minutes; not API/OCR event counts or formal SLI',
        'qualification': {'required_for_normal_health': False, 'status': 'separate_explicit_campaign'},
        'safety_boundary': latest.get('safety_boundary', {}),
    }
