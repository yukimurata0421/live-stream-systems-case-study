#!/usr/bin/env python3
"""Fallback controller on the encoder host (self-healing fan-out).

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
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

OCI_API = os.environ.get('OCI_MEDIAMTX_API', '').rstrip('/')
NAMESPACE = 'stream-v3'
FALLBACK = os.environ.get('FALLBACK_DEPLOYMENT', 'stream-v3-youtube-backup')
INTERVAL = 15
START_AFTER = 30
STOP_AFTER = 300


@dataclass
class Controller:
    unhealthy_since: float | None = None
    healthy_since: float | None = None
    last: dict | None = None   # previous sample, used to require advancing byte counters

    def healthy(self, sample: dict | None) -> bool:
        """sample = {'ready': bool, 'bytes_received': int, 'youtube': {'state', 'error', 'outbound'}} or None"""
        prev, self.last = self.last, sample
        if sample is None or prev is None:
            return False
        yt = sample.get('youtube') or {}
        prev_yt = prev.get('youtube') or {}
        return (sample.get('ready') is True
                and sample.get('bytes_received', 0) > prev.get('bytes_received', 0)
                and yt.get('state') == 'forwarding' and not yt.get('error')
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
    with urllib.request.urlopen(OCI_API + path, timeout=3) as r:
        return json.load(r)


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
        path = get('/v3/paths/get/relay')
        items = get('/v3/paths/forward-dests/list?path=relay').get('items', [])
    except Exception:
        return None
    yt = youtube_item(items)
    return {'ready': bool(path.get('ready')), 'bytes_received': int(path.get('bytesReceived') or path.get('inboundBytes') or 0),
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
    if not OCI_API:
        raise SystemExit('OCI_MEDIAMTX_API is required')
    c = Controller()
    log('controller_started', fallback=FALLBACK, start_after=START_AFTER, stop_after=STOP_AFTER)
    previous_health = None
    while True:
        now = time.monotonic()
        ok = c.healthy(observe())
        current = replicas()
        action = c.decide(now, ok, current)
        if ok != previous_health:
            log('oci_path_health', healthy=ok, fallback_replicas=current)
            previous_health = ok
        if action:
            target = '1' if action == 'START' else '0'
            try:
                kubectl('scale', 'deploy', FALLBACK, '--replicas=' + target)
                log('fallback_' + action.lower(), replicas=target)
            except Exception as error:
                log('fallback_scale_failed', action=action, error=type(error).__name__)
        time.sleep(INTERVAL)


if __name__ == '__main__':
    sys.exit(main())
