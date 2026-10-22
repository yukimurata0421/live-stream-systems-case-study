"""Deliver OCI-aware Twitch alerts: critical and recovery to Slack, others to Discord.

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
DELIVERY = 'http://127.0.0.1:9116/twitch-delivery'
JST = timezone(timedelta(hours=9))
# Alert name -> (title, what it means). Only these alerts are delivered here.
ALERTS = {
    'TwitchUnexpectedOffline': ('Twitch API and OCI output disagree', 'OCI input and Twitch output progress, but the API reports unplanned OFFLINE.'),
    'TwitchRelayNotProgressing': ('Twitch OCI delivery path is interrupted', 'The required OCI relay, input, or Twitch output has been interrupted for at least three minutes.'),
    'TwitchObservationUnavailable': ('OCI output progresses but Twitch API observation is unavailable', 'OCI input and Twitch output progress. API observations have been unavailable for at least three minutes.'),
    'TwitchDeliveryObservationUnavailable': ('Twitch OCI delivery path cannot be assessed', 'OCI observations are unavailable or input/output counter deltas are not established.'),
    'StreamV4TwitchSliMeasurementUnavailable': ('Twitch SLI collection has stopped', 'Twitch SLI collection has not updated for at least three minutes.'),
    'StreamV4TwitchSloBreached': ('Twitch SLO is below target', 'The seven-day measurement is below its target.'),
}
PATH_ALERTS = {'TwitchRelayNotProgressing', 'TwitchUnexpectedOffline'}
SLI_TITLES = {
    'api_measurement': ('Twitch API observation success is below target', 'Successful observations are measured against all expected minute slots, including failed API probes and missing collection minutes. Check LIVE and transport progress for delivery state.'),
    'provider_live': ('Twitch LIVE observation rate is below target', 'The fraction of API observations reporting LIVE is below target.'),
    'relay_flow': ('Twitch forwarding continuity is below target', 'The fraction of OCI observations showing Twitch output progress is below target.'),
    'daily_renewal': ('Twitch daily renewal confirmation is below target', 'The fraction of scheduled daily renewals confirmed before the deadline is below target.'),
}


def alert_copy(name, labels):
    if name == 'StreamV4TwitchSloBreached':
        return SLI_TITLES.get(labels.get('sli'), ALERTS[name])
    return ALERTS[name]


def key(alert: dict) -> str:
    labels = alert.get('labels') or {}
    extra = ','.join(f'{k}={labels[k]}' for k in sorted(labels) if k in ('sli', 'window', 'provider'))
    return labels.get('alertname', '') + (f'[{extra}]' if extra else '')


def jst(ts: float) -> str:
    return datetime.fromtimestamp(ts, JST).strftime('%m/%d %H:%M JST')


def duration(sec: float) -> str:
    sec = max(0, int(sec))
    return f'{sec // 3600}h {sec % 3600 // 60}m' if sec >= 3600 else f'{sec // 60}m {sec % 60}s'


def delivery_facts(value, now):
    """Validate the arena projection at notification time, including recovery proof."""
    try:
        def fresh(ts):
            age = now-datetime.fromisoformat(str(ts).replace('Z','+00:00')).timestamp()
            return 0 <= age <= 180
        if (value['schema'] != 'stream.relay_public.v1' or type(value['generated_at']) not in (int,float)
                or not 0 <= now-value['generated_at'] <= 180): return None
        result = dict(value['twitch_delivery'])
        if result.get('state') not in ('HEALTHY','API_UNAVAILABLE','PROVIDER_OFFLINE','OCI_UNAVAILABLE',
                                      'OCI_RELAY_STOPPED','OCI_INPUT_STOPPED','TWITCH_OUTPUT_STOPPED',
                                      'PATH_UNCONFIRMED','PLANNED_RENEWAL'): return None
        result['oci_observed'] = value['source_fresh'] is True and fresh(value['observed_at'])
        api = value['twitch_api']
        result['api_observed'] = api['source_fresh'] is True and api['state'] in ('LIVE','OFFLINE') and fresh(api['observed_at'])
        result['recovery_confirmed'] = (
            result.get('state') == 'HEALTHY' and result['oci_observed'] and result['api_observed']
            and value['relay']['status'] == 'RUNNING' and value['input']['status'] == 'FLOWING'
            and value['destinations']['twitch']['status'] == 'FLOWING' and api['state'] == 'LIVE'
            and abs(datetime.fromisoformat(value['observed_at']).timestamp()-datetime.fromisoformat(api['observed_at']).timestamp()) <= 90)
        return result
    except (KeyError, TypeError, ValueError, OverflowError, OSError): return None


def context_text(value, now):
    facts = delivery_facts(value, now)
    if not facts or not facts['oci_observed']: return 'OCI path: observation unavailable. Current delivery state cannot be assessed.'
    names = {'RUNNING':'running', 'STOPPED':'stopped', 'FLOWING':'progressing', 'STALLED':'stalled',
             'DISCONNECTED':'disconnected', 'CONNECTED':'connected, progress unconfirmed', 'UNKNOWN':'unknown'}
    relay = names.get(value['relay']['status'], 'unknown')
    incoming = names.get(value['input']['status'], 'unknown')
    outgoing = names.get(value['destinations']['twitch']['status'], 'unknown')
    api = value['twitch_api']['state'] if facts['api_observed'] else 'UNKNOWN'
    return f'OCI relay: {relay} / Input: {incoming} / Twitch output: {outgoing} / API: {api}'


def plan(alerts: list[dict], state: dict, now: float, *, repeat_sec: int, delivery=None, slo_repeat_sec: int = 86400):
    """Plan channel-bound messages; retain open episodes until recovery is observed."""
    messages, current = [], {}
    context = context_text(delivery, now)
    facts = delivery_facts(delivery, now)
    def enqueue(k, channel, content):
        messages.append({'key':k, 'channel':channel, 'content':content+'\n'+context})
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
        labels = alert.get('labels') or {}
        title, meaning = alert_copy(name, labels)
        channel = 'slack' if labels.get('severity') == 'critical' or (prev and prev.get('channel') == 'slack') else 'discord'
        icon = '\U0001f534' if labels.get('severity') == 'critical' else '\U0001f7e0' if labels.get('severity', 'warning') == 'warning' else '\u2139\ufe0f'
        detail = f"\uff08{labels['sli']}\uff09" if labels.get('sli') else ''
        historical_slo = name == 'StreamV4TwitchSloBreached'
        # Compare semantic states, not changing counters, timestamps or SLI values.
        status = {'severity': labels.get('severity', 'warning'),
                  'delivery_state': facts['state'] if facts else 'UNKNOWN'} if historical_slo else None
        # Older state files acquire a baseline without replaying an acknowledged alert.
        changed = prev is not None and status is not None and prev.get('notification_status', status) != status
        reminder_sec = slo_repeat_sec if historical_slo else repeat_sec
        if prev is None or (channel == 'slack' and prev.get('channel', 'discord') != 'slack'):
            enqueue(k, channel, f'{icon} {title}{detail}\n{meaning}\nStarted: {jst(since)}')
            current[k] = {'since': prev['since'] if prev else since, 'last_sent': now, 'name': name, 'title': title, 'channel':channel}
        elif changed or now - prev['last_sent'] >= reminder_sec:
            prefix = 'State changed' if changed else 'Still firing'
            enqueue(k, channel, f'{icon} {prefix}: {title}{detail}\n{meaning}\nOpen for {duration(now - prev["since"])}.')
            current[k] = {**prev, 'last_sent': now, 'title': title, 'channel':channel}
        else:
            current[k] = {**prev, 'title': title, 'channel':channel}
        current[k].pop('recovery_candidate_at', None)
        if status is not None:
            current[k]['notification_status'] = status
    for k, prev in state.items():
        if k not in current:
            name = prev.get('name')
            if name in PATH_ALERTS:
                # A disappearing Prometheus series or planned renewal is not recovery.
                if not facts or not facts['recovery_confirmed']:
                    current[k] = {kk:vv for kk,vv in prev.items() if kk != 'recovery_candidate_at'}
                    continue
                observed = datetime.fromisoformat(delivery['observed_at']).timestamp()
                candidate = prev.get('recovery_candidate_at')
                if candidate is None or observed-candidate < 45:
                    current[k] = {**prev, 'recovery_candidate_at':observed if candidate is None else candidate}
                    continue
            elif name == 'TwitchObservationUnavailable' and (not facts or not facts['api_observed']):
                current[k] = prev
                continue
            elif name == 'TwitchDeliveryObservationUnavailable' and (not facts or not facts['oci_observed'] or facts['state'] == 'PATH_UNCONFIRMED'):
                current[k] = prev
                continue
            title = prev.get('title')
            if not title:
                labels = dict(part.split('=', 1) for part in k.partition('[')[2].rstrip(']').split(',') if '=' in part)
                title = alert_copy(prev['name'], labels)[0] if prev.get('name') in ALERTS else k
            enqueue(k, prev.get('channel', 'discord'), f'\u2705 Resolved: {title}\nDuration {duration(now - prev["since"])}')
    return messages, current


def decide(alerts, state, now, *, repeat_sec, delivery=None, slo_repeat_sec=86400):
    messages, current = plan(alerts, state, now, repeat_sec=repeat_sec, delivery=delivery, slo_repeat_sec=slo_repeat_sec)
    return [m['content'] for m in messages], current


def fetch_alerts() -> list[dict]:
    with urllib.request.urlopen(PROMETHEUS, timeout=5) as response:
        return json.load(response)['data']['alerts']


def fetch_delivery():
    try:
        with urllib.request.urlopen(DELIVERY, timeout=3) as response:
            raw = response.read(16385)
        return json.loads(raw) if len(raw) <= 16384 else None
    except (OSError, ValueError): return None


def send(webhook: str, content: str, username: str, channel: str = 'discord') -> bool:
    body = json.dumps({'text':content} if channel == 'slack' else {'content': content, 'username': username}, ensure_ascii=False).encode()
    req = urllib.request.Request(webhook, data=body, method='POST',
                                 headers={'Content-Type': 'application/json', 'User-Agent': 'stream-v4-twitch-alert-notify/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return 200 <= response.status < 300
    except Exception:
        return False


def deliver_plan(messages, next_state, previous, sender):
    """Commit each alert's state only after its own channel acknowledges delivery."""
    result = dict(next_state)
    for message in messages:
        if not sender(message):
            k = message['key']
            if k in previous: result[k] = previous[k]
            else: result.pop(k, None)
    return result


def log(event: str, **fields):
    print(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'event': event, **fields}, ensure_ascii=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--interval-sec', type=int, default=30)
    parser.add_argument('--repeat-sec', type=int, default=3600)
    parser.add_argument('--slo-repeat-sec', type=int, default=86400,
                        help='Repeat historical SLO warnings after this many seconds; current-failure alerts use --repeat-sec.')
    args = parser.parse_args()
    webhooks = {'discord':os.environ.get('STREAM_NOTIFY_DISCORD_WEBHOOK_URL', '').strip(),
                'slack':os.environ.get('STREAM_NOTIFY_SLACK_WEBHOOK_URL', '').strip()}
    username = os.environ.get('STREAM_NOTIFY_USERNAME', '').strip() or 'ADS-B Stream Watchdog'
    state = json.loads(args.state.read_text()) if args.state.exists() else {}
    log('started', routes_configured={k:bool(v) for k,v in webhooks.items()}, tracked=len(state),
        repeat_sec=args.repeat_sec, slo_repeat_sec=args.slo_repeat_sec)
    while True:
        try:
            alerts = fetch_alerts()
        except Exception as error:
            # Prometheus unavailable: keep the current state; do not report a resolution we did not observe.
            log('prometheus_unavailable', error=type(error).__name__)
            time.sleep(args.interval_sec)
            continue
        messages, next_state = plan(alerts, state, time.time(), repeat_sec=args.repeat_sec,
                                    slo_repeat_sec=args.slo_repeat_sec, delivery=fetch_delivery())
        def sender(message):
            channel = message['channel']
            ok = bool(webhooks[channel]) and send(webhooks[channel], message['content'], username, channel)
            log('notify', delivered=ok, channel=channel, first_line=message['content'].splitlines()[0])
            return ok
        state = deliver_plan(messages, next_state, state, sender)
        tmp = args.state.with_suffix('.tmp'); tmp.write_text(json.dumps(state)); os.replace(tmp, args.state)
        time.sleep(args.interval_sec)


if __name__ == '__main__':
    raise SystemExit(main())
