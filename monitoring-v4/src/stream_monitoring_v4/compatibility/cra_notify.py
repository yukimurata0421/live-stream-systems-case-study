#!/usr/bin/env python3
"""Deliver CRA alerts to both routes through the shared gateway; never retry UNKNOWN."""
from __future__ import annotations

import argparse
from datetime import datetime
from http.client import HTTPException
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
from zoneinfo import ZoneInfo

if __package__:
    from .cra_monitoring_io import append, atomic, get_json, utc
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from cra_monitoring_io import append, atomic, get_json, utc

NAMES = frozenset(('CRAObservationUnavailable', 'CRAAgentUnavailable',
    'CRATargetObservationUnavailable', 'CRACentralVerificationFailed',
    'CRACentralVerificationStale', 'CRAAuditReplicationLag', 'CRAUnresolvedExecution',
    'CRAExecutorAuthorityMismatch', 'CRAObserverTimeouts', 'CRALogIngestionUnavailable'))


def send(route, body, socket_path):
    # This is the shared client/credential boundary, not another webhook implementation.
    from notification_gateway_client import Connection
    c = Connection(socket_path, 8)
    try:
        raw = json.dumps({'route': route, 'content': body, 'username': 'CRA Monitoring'}, ensure_ascii=False).encode()
        c.request('POST', '/deliver', raw, {'Content-Type': 'application/json'})
        response = c.getresponse()
        raw = response.read(4097)
        if len(raw) > 4096:
            return 'UNKNOWN', 0
        value = json.loads(raw)
        if value.get('route') != route:
            return 'UNKNOWN', 0
        state = str(value.get('status', '')).upper()
        code = value.get('provider_status', 0)
        if response.status == 200 and state == 'DELIVERED' and value.get('delivered') is True:
            return 'DELIVERED', code
        if state == 'CAPACITY' or state == 'RETRY' and (code == 429 or code >= 500):
            return 'PENDING', code
        if state == 'REJECTED' or state == 'RETRY' and 400 <= code < 500:
            return 'REJECTED', code
        return 'UNKNOWN', code
    except (OSError, ValueError, TypeError, HTTPException):
        return 'UNKNOWN', 0
    finally:
        c.close()


def rules_from(value, now):
    if value.get('status') != 'success':
        raise ValueError('prometheus_rules_unavailable')
    found = {}
    for group in value['data']['groups']:
        for rule in group['rules']:
            name = rule.get('name')
            if name not in NAMES:
                continue
            at = datetime.fromisoformat(rule['lastEvaluation'].replace('Z', '+00:00')).timestamp()
            if name in found or rule.get('health') != 'ok' or not -5 <= now-at <= 120:
                raise ValueError('cra_rule_unconfirmed')
            found[name] = rule
    if set(found) != NAMES:
        raise ValueError('cra_rules_missing')
    return found


def connect(path):
    c = sqlite3.connect(path, timeout=2)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA synchronous=FULL')
    c.executescript('''
        CREATE TABLE IF NOT EXISTS episodes (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, opened REAL NOT NULL, closed REAL);
        CREATE TABLE IF NOT EXISTS deliveries (
          id TEXT PRIMARY KEY, episode TEXT NOT NULL, phase TEXT NOT NULL, route TEXT NOT NULL,
          body TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
          next_at REAL NOT NULL DEFAULT 0, code INTEGER NOT NULL DEFAULT 0);
    ''')
    # A crash after send admission is not a new attempt after daemon restart.
    with c:
        c.execute("UPDATE deliveries SET state='UNKNOWN' WHERE state='SENDING'")
    return c


def cycle(c, rules, now, *, sender, log_path, test_label=''):
    if len(list(c.execute('SELECT 1 FROM episodes LIMIT 4097'))) > 4096:
        raise ValueError('cra_notification_capacity')
    prefix = f'[CRA migration test {test_label}] This is a test alert.\n' if test_label else ''
    def enroll(episode, phase, body):
        for route in ('discord', 'slack'):
            identifier = f'{episode}/{phase}/{route}'
            c.execute('INSERT OR IGNORE INTO deliveries(id,episode,phase,route,body,state) VALUES(?,?,?,?,?,?)',
                      (identifier, episode, phase, route, body, 'PENDING'))
    with c:
        for name, rule in rules.items():
            for alert in rule.get('alerts', []):
                if alert.get('state') != 'firing':
                    continue
                identity = json.dumps({'labels': alert['labels'], 'activeAt': alert['activeAt']}, sort_keys=True)
                episode = hashlib.sha256(identity.encode()).hexdigest()
                opened = datetime.fromisoformat(alert['activeAt'].replace('Z', '+00:00')).timestamp()
                c.execute('INSERT OR IGNORE INTO episodes VALUES(?,?,?,NULL)', (episode, name, opened))
                title = rule.get('annotations', {}).get('summary', name)
                at = datetime.fromtimestamp(opened, ZoneInfo('Asia/Tokyo')).strftime('%m/%d %H:%M:%S JST')
                body = prefix+f'CRA monitoring alert: {name}\n{title}\nStarted: {at}\nRecovery observation or audit needs attention; this does not establish a stream outage.\nID: {episode[:16]}'
                enroll(episode, 'firing', body)
        for row in c.execute('SELECT * FROM episodes WHERE closed IS NULL').fetchall():
            rule = rules.get(row['name'])
            # Missing/broken Prometheus or a missing rule cannot resolve an episode.
            if rule is not None and rule['state'] == 'inactive':
                body = prefix+f"CRA monitoring alert resolved: {row['name']}\nPrometheus has a fresh normal evaluation. This does not prove stream recovery.\nID: {row['id'][:16]}"
                enroll(row['id'], 'resolved', body)
                c.execute('UPDATE episodes SET closed=? WHERE id=?', (now, row['id']))
    for row in c.execute("SELECT * FROM deliveries WHERE state='PENDING' AND next_at<=? ORDER BY rowid LIMIT 20", (now,)).fetchall():
        # Preserve firing -> resolved order for each route even during provider failure.
        if row['phase'] == 'resolved' and c.execute("SELECT 1 FROM deliveries WHERE episode=? AND route=? AND phase='firing' AND state='PENDING'", (row['episode'], row['route'])).fetchone():
            continue
        attempts = row['attempts']+1
        with c:
            c.execute("UPDATE deliveries SET state='SENDING',attempts=? WHERE id=?", (attempts, row['id']))
        state, code = sender(row['route'], row['body'])
        if state not in ('DELIVERED', 'PENDING', 'UNKNOWN', 'REJECTED'):
            state = 'UNKNOWN'
        if state == 'PENDING' and attempts >= 5:
            state = 'EXHAUSTED'
        with c:
            c.execute('UPDATE deliveries SET state=?,code=?,next_at=? WHERE id=?',
                      (state, code, now+60*2**min(attempts-1, 4), row['id']))
        append(log_path, [{'schema': 'cra.monitoring_event.v1', 'ts_utc': utc(now),
                          'component': 'cra_notify', 'kind': 'notification_delivery',
                          'event_id': row['id'], 'route': row['route'], 'phase': row['phase'],
                          'state': state, 'provider_code': code, 'test': bool(test_label)}])
    return dict(c.execute('SELECT state,count(*) FROM deliveries GROUP BY state'))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--once', action='store_true')
    p.add_argument('--test-label', default='')
    args = p.parse_args()
    cfg = json.loads(args.config.read_text())
    c = connect(str(Path(cfg['state_dir'])/'deliveries.sqlite3'))
    while True:
        now = time.time()
        try:
            rules = rules_from(get_json(cfg['prometheus']+'/api/v1/rules?type=alert'), now)
            counts = cycle(c, rules, now, sender=lambda route, body: send(route, body, cfg['gateway_socket']),
                           log_path=cfg['log_path'], test_label=args.test_label)
            status = {'checked_at': now, 'available': True, 'deliveries': counts}
        except (OSError, ValueError, KeyError, TypeError):
            status = {'checked_at': now, 'available': False}
        atomic(Path(cfg['state_dir'])/'notify-status.json', status)
        if args.once:
            return 0 if status['available'] else 1
        time.sleep(15)


if __name__ == '__main__':
    raise SystemExit(main())
