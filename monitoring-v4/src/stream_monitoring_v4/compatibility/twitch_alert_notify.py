"""Deliver arena's Twitch Prometheus alerts to the existing Discord webhook.

Prometheus already evaluates the Twitch alert rules, but no Alertmanager or other consumer delivers them.
This loop reads firing alerts from the local Prometheus API, sends one message when an alert starts, repeats
while it stays firing, and sends one message when it resolves. It never changes delivery or recovery state.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROMETHEUS = 'http://127.0.0.1:9090/api/v1/alerts'
JST = timezone(timedelta(hours=9))
# Alert name -> (title, what it means). Only these alerts are delivered here.
ALERTS = {
    'TwitchUnexpectedOffline': ('Twitch is OFFLINE unexpectedly', 'OFFLINE for 3 minutes or more outside the 00:00 JST renewal.'),
    'TwitchRelayNotProgressing': ('Relay to Twitch has stopped', 'Bytes sent from the relay to Twitch have not advanced for 3 minutes.'),
    'TwitchObservationUnavailable': ('Twitch state is unavailable', 'No Twitch API (Helix) observation for 3 minutes.'),
    'StreamV4TwitchSliMeasurementUnavailable': ('Twitch SLI measurement has stopped', 'The Twitch SLI has not been updated for 3 minutes.'),
    'StreamV4TwitchSloBreached': ('Twitch SLO is below target', 'The 7-day result is below its target.'),
}


def key(alert: dict) -> str:
    labels = alert.get('labels') or {}
    extra = ','.join(f'{k}={labels[k]}' for k in sorted(labels) if k in ('sli', 'window', 'provider'))
    return labels.get('alertname', '') + (f'[{extra}]' if extra else '')


def jst(ts: float) -> str:
    return datetime.fromtimestamp(ts, JST).strftime('%m/%d %H:%M JST')


def duration(sec: float) -> str:
    sec = max(0, int(sec))
    return f'{sec // 3600}h {sec % 3600 // 60}m' if sec >= 3600 else f'{sec // 60}m {sec % 60}s'


def decide(alerts: list[dict], state: dict, now: float, *, repeat_sec: int) -> tuple[list[str], dict]:
    """Return messages to send and the next state. state = {key: {'since': ts, 'last_sent': ts, 'name': alertname}}."""
    messages, current = [], {}
    for alert in alerts:
        name = (alert.get('labels') or {}).get('alertname')
        if name not in ALERTS or alert.get('state') != 'firing':
            continue
        k = key(alert)
        try:
            since = datetime.fromisoformat(str(alert.get('activeAt')).replace('Z', '+00:00')).timestamp()
        except ValueError:
            since = now
        prev = state.get(k)
        title, meaning = ALERTS[name]
        labels = alert.get('labels') or {}
        detail = f" ({labels['sli']})" if labels.get('sli') else ''
        if prev is None:
            messages.append(f'🔴 {title}{detail}\n{meaning}\nStarted: {jst(since)}')
            current[k] = {'since': since, 'last_sent': now, 'name': name}
        elif now - prev['last_sent'] >= repeat_sec:
            messages.append(f'🔴 Still firing: {title}{detail}\nFiring for {duration(now - prev["since"])}.')
            current[k] = {**prev, 'last_sent': now}
        else:
            current[k] = prev
    for k, prev in state.items():
        if k not in current:
            title = ALERTS.get(prev.get('name'), (k, ''))[0]
            messages.append(f'✅ Resolved: {title}\nDuration {duration(now - prev["since"])}')
    return messages, current


def fetch_alerts() -> list[dict]:
    with urllib.request.urlopen(PROMETHEUS, timeout=5) as response:
        return json.load(response)['data']['alerts']


def send(webhook: str, content: str, username: str) -> bool:
    body = json.dumps({'content': content, 'username': username}, ensure_ascii=False).encode()
    req = urllib.request.Request(webhook, data=body, method='POST',
                                 headers={'Content-Type': 'application/json', 'User-Agent': 'stream-v4-twitch-alert-notify/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return 200 <= response.status < 300
    except Exception:
        return False


def log(event: str, **fields):
    print(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'event': event, **fields}, ensure_ascii=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--interval-sec', type=int, default=30)
    parser.add_argument('--repeat-sec', type=int, default=3600)
    args = parser.parse_args()
    webhook = os.environ.get('STREAM_NOTIFY_DISCORD_WEBHOOK_URL', '').strip()
    username = os.environ.get('STREAM_NOTIFY_USERNAME', '').strip() or 'ADS-B Stream Watchdog'
    state = json.loads(args.state.read_text()) if args.state.exists() else {}
    log('started', webhook_configured=bool(webhook), tracked=len(state))
    while True:
        try:
            alerts = fetch_alerts()
        except Exception as error:
            # Prometheus unavailable: keep the current state; do not report a resolution we did not observe.
            log('prometheus_unavailable', error=type(error).__name__)
            time.sleep(args.interval_sec)
            continue
        messages, next_state = decide(alerts, state, time.time(), repeat_sec=args.repeat_sec)
        delivered = True
        for message in messages:
            ok = bool(webhook) and send(webhook, message, username)
            delivered = delivered and ok
            log('notify', delivered=ok, first_line=message.splitlines()[0])
        if delivered:
            state = next_state
            tmp = args.state.with_suffix('.tmp'); tmp.write_text(json.dumps(state)); os.replace(tmp, args.state)
        time.sleep(args.interval_sec)


if __name__ == '__main__':
    raise SystemExit(main())
