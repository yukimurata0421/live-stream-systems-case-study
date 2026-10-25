#!/usr/bin/env python3
"""Fallback controller on the encoder host (self-healing fan-out).

Optional OCI_ROUTE_FACTS_URL binds actual copy-output counters to the configured relay machine and destination.
Normally the encoder host uploads one feed to the relay. Only while the relay path is unhealthy does
it start a fallback Deployment that sends directly to the YouTube backup ingest.
- Every 15 seconds it reads the relay's MediaMTX API over the private tunnel. Healthy means the input
  path is ready and its received bytes advance, and the YouTube forward is forwarding without error
  with advancing outbound bytes. An unreachable API or unknown observation counts as unhealthy.
- After 30 seconds of continuous failure, the fallback is scaled 0 -> 1.
- While the fallback runs, 300 seconds of continuous relay health scale it 1 -> 0 (back to one upload).
It does not depend on the monitoring host. The fallback stops only after continuous health.
Its only Kubernetes permissions are getting and scaling the fallback Deployment (since 2026-10-21); the
fallback upload rate is sampled by the public relay projection on the monitoring host instead.
"""
from __future__ import annotations

import json
import argparse
import math
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone

OCI_API = os.environ.get('OCI_MEDIAMTX_API', '').rstrip('/')
NAMESPACE = 'stream-v3'
FALLBACK = os.environ.get('FALLBACK_DEPLOYMENT', 'stream-v3-youtube-backup')
INTERVAL = 15
START_AFTER = 30
STOP_AFTER = 300
FACTS_URL = os.environ.get('OCI_ROUTE_FACTS_URL', '')
EXPECTED_MACHINE = os.environ.get('OCI_ROUTE_MACHINE_SHA256', '')
EXPECTED_DESTINATION = os.environ.get('OCI_YOUTUBE_DESTINATION_SHA256', '')
MAX_SAMPLE_GAP = 45
MAX_FACT_AGE = 20
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def counter(value):
    return type(value) is int and value >= 0


def fresh(value, now):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return stamp.tzinfo is not None and 0 <= now - stamp.timestamp() <= MAX_FACT_AGE


def copy_route_sample(value, *, now, machine, destination, received):
    """Bind both tracks and actual output bytes to one relay and copy session."""
    try:
        raw, cp = value['relay'], value['youtube_copy']
        if (value['schema'] != 'stream_v3.route_facts.v1' or raw['schema'] != 'stream.relay_raw.v1'
                or raw['host_id'] != 'relay-host' or len(machine) != 64
                or raw['machine_id_sha256'] != machine or len(destination) != 64
                or not fresh(raw['observed_at'], now) or not fresh(cp['updated_at'], now)
                or not fresh(cp['last_progress_at'], now)
                or cp['schema'] != 'stream_v3.youtube_forward.v1'
                or cp['boot_id'] != raw['boot_id'] or cp['destination_fingerprint'] != destination
                or cp['state'] != 'FLOWING' or not counter(cp['pid']) or cp['pid'] == 0
                or not counter(cp['session']) or cp['session'] == 0):
            return None
        incoming, yt = raw['input'], raw['destinations']['youtube']
        ident = f"youtube-forward-{cp['pid']}-{cp['session']}"
        if (yt['id'] != ident or yt['state'] != 'forwarding'
                or type(raw['relay']['active']) is not bool or type(incoming['ready']) is not bool
                or type(incoming['available']) is not bool
                or not isinstance(incoming['connection_id'], str) or not incoming['connection_id']
                or not isinstance(raw['boot_id'], str) or not raw['boot_id']
                or not counter(raw['relay']['pid']) or raw['relay']['pid'] == 0
                or not all(counter(v) for v in (incoming['bytes'], cp['output_bytes'], cp['video_packets'], cp['audio_packets']))
                or not cp['video_packets'] or not cp['audio_packets']
                or type(raw['monotonic_seconds']) not in (int, float)
                or not math.isfinite(raw['monotonic_seconds']) or raw['monotonic_seconds'] < 0):
            return None
        return {'ready': raw['relay']['active'] and incoming['available'] and incoming['ready'],
                'bytes_received': incoming['bytes'],
                'identity': (raw['boot_id'], raw['relay']['pid'], incoming['connection_id'], ident, destination),
                'source_monotonic': raw['monotonic_seconds'], 'received_monotonic': received,
                'youtube': {'state': yt['state'], 'error': False, 'outbound': cp['output_bytes'],
                            'video_packets': cp['video_packets'], 'audio_packets': cp['audio_packets']}}
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        return None


@dataclass
class Controller:
    unhealthy_since: float | None = None
    healthy_since: float | None = None
    last: dict | None = None   # Previous observation used to check increasing input and output counters.

    def healthy(self, sample: dict | None) -> bool:
        """sample = {'ready': bool, 'bytes_received': int, 'youtube': {'state', 'error', 'outbound'}} or None"""
        prev, self.last = self.last, sample
        if sample is None or prev is None:
            return False
        yt = sample.get('youtube') or {}
        prev_yt = prev.get('youtube') or {}
        if 'identity' in sample:
            if (sample['identity'] != prev.get('identity')
                    or not 0 < sample['source_monotonic'] - prev.get('source_monotonic', sample['source_monotonic']) <= MAX_SAMPLE_GAP
                    or any(not counter(yt.get(k)) or not counter(prev_yt.get(k))
                           or yt[k] <= prev_yt[k] for k in ('video_packets', 'audio_packets'))):
                return False
        return (sample.get('ready') is True
                and counter(sample.get('bytes_received')) and counter(prev.get('bytes_received'))
                and sample.get('bytes_received', 0) > prev.get('bytes_received', 0)
                and yt.get('state') == 'forwarding' and not yt.get('error')
                and counter(yt.get('outbound')) and counter(prev_yt.get('outbound'))
                and yt.get('outbound', 0) > prev_yt.get('outbound', 0))

    def decide(self, now: float, healthy: bool, fallback_replicas: int | None) -> str | None:
        """Return 'START', 'STOP', or None. Do nothing while the replica count is unknown."""
        if healthy:
            self.unhealthy_since = None
            self.healthy_since = self.healthy_since if self.healthy_since is not None else now
        else:
            self.healthy_since = None
            self.unhealthy_since = self.unhealthy_since if self.unhealthy_since is not None else now
        if fallback_replicas is None:
            return None
        if fallback_replicas == 0 and not healthy and now - self.unhealthy_since >= START_AFTER:
            return 'START'
        if fallback_replicas >= 1 and healthy and now - self.healthy_since >= STOP_AFTER:
            return 'STOP'
        return None


def get(path: str):
    return get_url(OCI_API + path)


def get_url(url):
    try:
        with _OPENER.open(url, timeout=3) as r:
            raw = r.read(16385)
    except urllib.error.HTTPError as error:
        error.close()
        raise
    if len(raw) > 16384:
        raise ValueError('oversized_facts')
    return json.loads(raw)


def provider(item: dict) -> str | None:
    """Provider of a forward item from its destination host. The destination (with its key) is never returned or logged."""
    host = urllib.parse.urlsplit(str((item.get('conf') or {}).get('dest') or '')).hostname or ''
    if host == 'youtube.com' or host.endswith('.youtube.com'):
        return 'youtube'
    if host.endswith('.live-video.net') or host.endswith('.twitch.tv'):
        return 'twitch'
    return None


def youtube_item(items: list) -> dict | None:
    """The YouTube forward if exactly one exists. List position is not used.

    When the keeper removes a failing forward, the remaining one shifts to pos 1.
    """
    matches = [i for i in items if provider(i) == 'youtube']
    return matches[0] if len(matches) == 1 else None


def observe() -> dict | None:
    try:
        if FACTS_URL:
            value = get_url(FACTS_URL)
            return copy_route_sample(value, now=time.time(), machine=EXPECTED_MACHINE,
                                     destination=EXPECTED_DESTINATION, received=time.monotonic())
        path = get('/v3/paths/get/relay')
        items = get('/v3/paths/forward-dests/list?path=relay').get('items', [])
    except Exception:
        return None
    yt = youtube_item(items)
    return {'ready': bool(path.get('ready')), 'bytes_received': int(path.get('bytesReceived') or path.get('inboundBytes') or 0),
            'received_monotonic': time.monotonic(),
            'youtube': None if yt is None else {'state': yt.get('state'), 'error': bool(yt.get('lastError')),
                                                 'outbound': int(yt.get('outboundBytes') or 0)}}


def kubectl(*args: str) -> str:
    return subprocess.run(['kubectl', '-n', NAMESPACE, *args], capture_output=True, text=True, timeout=20, check=True).stdout


def replicas() -> int | None:
    try:
        return int(kubectl('get', 'deploy', FALLBACK, '-o', 'jsonpath={.spec.replicas}'))
    except Exception:
        return None


def log(event: str, **values):
    print(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'event': event, **values}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--observe-once', action='store_true')
    args = parser.parse_args()
    if not OCI_API and not FACTS_URL:
        raise SystemExit('OCI_MEDIAMTX_API or OCI_ROUTE_FACTS_URL is required')
    if args.observe_once:
        value = observe()
        print(json.dumps({'sample': value}))
        return 0 if value is not None else 1
    c = Controller()
    log('controller_started', fallback=FALLBACK, start_after=START_AFTER, stop_after=STOP_AFTER,
        observation='copy_route_facts' if FACTS_URL else 'mediamtx_direct')
    previous_health = None
    while True:
        value = observe()
        current = replicas()
        if value is not None and time.monotonic() - value['received_monotonic'] > 5:
            value = observe()
        now = time.monotonic()
        ok = c.healthy(value)
        action = c.decide(now, ok, current)
        if ok != previous_health:
            log('oci_path_health', healthy=ok, fallback_replicas=current)
            previous_health = ok
        if action:
            target = '1' if action == 'START' else '0'
            try:
                kubectl('scale', 'deploy', FALLBACK, '--replicas=' + target)
                log('fallback_' + action.lower(), replicas=target,
                    healthy_for_sec=round(now - c.healthy_since, 3) if c.healthy_since is not None else 0)
            except Exception as error:
                log('fallback_scale_failed', action=action, error=type(error).__name__)
        time.sleep(INTERVAL)


if __name__ == '__main__':
    sys.exit(main())
