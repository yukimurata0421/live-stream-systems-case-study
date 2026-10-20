#!/usr/bin/env python3
"""Keep the relay's MediaMTX forwards healthy and renew the Twitch session daily.

- Forward healing: while input is present, a destination that is not forwarding, or reports an
  error, for 30 seconds is removed and restored on the next cycle (at most once per 60 seconds
  per destination).
- Twitch renewal: at 00:00 JST, or 46 hours after the Twitch session started, only the Twitch
  forward is removed. It is restored after OFFLINE is observed (at least 120 seconds), or after
  600 seconds even if OFFLINE is never observed, so Twitch is never held down for long.
- The YouTube forward is never touched. State is persisted so a restart does not renew twice on
  the same JST date.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

JST = timezone(timedelta(hours=9))
API = os.environ.get('MEDIAMTX_API', 'http://127.0.0.1:9997')
PATH = 'relay'
BAD_SECONDS = 30
HEAL_INTERVAL = 60
OFFLINE_MIN = 120
OFFLINE_MAX = 600
VERIFY_MAX = 180
AGE_LIMIT = 46 * 3600


def is_twitch(dest: str) -> bool:
    return 'live-video.net' in dest or 'twitch' in dest


@dataclass
class Keeper:
    destinations: list            # every configured forward, [{'dest': ...}, ...]
    bad_since: dict = field(default_factory=dict)
    last_heal: dict = field(default_factory=dict)
    removed: str | None = None    # destination temporarily removed for healing
    phase: str = 'IDLE'           # IDLE / WAIT_OFFLINE / VERIFY
    phase_at: float = 0.0
    last_renewal_date: str = ''   # JST date, so the 00:00 renewal runs once per day
    old_stream_id: str = ''
    current: list = field(default_factory=list)   # forward list last applied to MediaMTX

    def __post_init__(self):
        self.current = list(self.destinations)

    def apply(self, value: list) -> list:
        self.current = list(value)
        return value

    def desired(self) -> list:
        keep = []
        for d in self.destinations:
            if self.phase == 'WAIT_OFFLINE' and is_twitch(d['dest']):
                continue
            if d['dest'] == self.removed:
                continue
            keep.append(d)
        return keep

    def heal(self, now: float, ready: bool, items: list) -> list | None:
        """Decide whether to re-create a forward. Returns a new forward list, or None."""
        if self.removed is not None:
            # Restore what the previous cycle removed.
            self.removed = None
            return self.apply(self.desired())
        if not ready:
            self.bad_since.clear()
            return None
        for item in items:
            # forward-dests items carry no URL; map them to the applied list by 1-based pos.
            pos = item.get('pos')
            if not isinstance(pos, int) or not 1 <= pos <= len(self.current):
                continue
            dest = self.current[pos - 1]['dest']
            ok = item.get('state') == 'forwarding' and not item.get('lastError')
            if ok:
                self.bad_since.pop(dest, None)
                continue
            since = self.bad_since.setdefault(dest, now)
            if now - since >= BAD_SECONDS and now - self.last_heal.get(dest, -1e9) >= HEAL_INTERVAL:
                self.last_heal[dest] = now
                self.bad_since.pop(dest, None)
                self.removed = dest
                return self.apply(self.desired())
        return None

    def renewal(self, now: float, wall: datetime, ready: bool, twitch: dict) -> list | None:
        """Advance the daily Twitch renewal. Returns a forward list when it must change.
        twitch = {'status': 'LIVE'|'OFFLINE'|'UNKNOWN', 'stream_id': str, 'started_at': datetime|None}
        """
        today = wall.astimezone(JST).date().isoformat()
        if self.phase == 'IDLE':
            if not ready or twitch['status'] != 'LIVE':
                return None
            age = (wall - twitch['started_at']).total_seconds() if twitch.get('started_at') else 0
            due_daily = self.last_renewal_date and today != self.last_renewal_date
            if not self.last_renewal_date:
                self.last_renewal_date = today   # the first start counts as today's renewal
            if not (due_daily or age >= AGE_LIMIT):
                return None
            self.phase, self.phase_at = 'WAIT_OFFLINE', now
            self.old_stream_id = twitch.get('stream_id', '')
            self.last_renewal_date = today
            return self.apply(self.desired())
        if self.phase == 'WAIT_OFFLINE':
            waited = now - self.phase_at
            if (twitch['status'] == 'OFFLINE' and waited >= OFFLINE_MIN) or waited >= OFFLINE_MAX:
                self.phase, self.phase_at = 'VERIFY', now
                return self.apply(self.desired())
            return None
        if self.phase == 'VERIFY':
            new_live = twitch['status'] == 'LIVE' and twitch.get('stream_id') not in ('', self.old_stream_id)
            if new_live or now - self.phase_at >= VERIFY_MAX or not ready:
                self.phase = 'IDLE'
            return None
        self.phase = 'IDLE'
        return None

    def to_state(self) -> dict:
        return {'phase': self.phase, 'last_renewal_date': self.last_renewal_date, 'old_stream_id': self.old_stream_id}


def api(path: str, body: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(API + path, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=3) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


class Helix:
    def __init__(self, credentials: dict, user_id: str):
        self.c, self.user_id, self.token, self.expires = credentials, user_id, '', 0.0

    def status(self) -> dict:
        try:
            if time.monotonic() >= self.expires - 60:
                payload = urllib.parse.urlencode({'client_id': self.c['client_id'], 'client_secret': self.c['client_secret'],
                                                  'grant_type': 'client_credentials'}).encode()
                with urllib.request.urlopen(urllib.request.Request('https://id.twitch.tv/oauth2/token', data=payload), timeout=5) as r:
                    reply = json.load(r)
                self.token, self.expires = reply['access_token'], time.monotonic() + int(reply['expires_in'])
            req = urllib.request.Request('https://api.twitch.tv/helix/streams?user_id=' + self.user_id,
                                         headers={'Client-Id': self.c['client_id'], 'Authorization': 'Bearer ' + self.token})
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.load(r).get('data', [])
            if not data:
                return {'status': 'OFFLINE', 'stream_id': '', 'started_at': None}
            s = data[0]
            return {'status': 'LIVE', 'stream_id': s['id'],
                    'started_at': datetime.fromisoformat(s['started_at'].replace('Z', '+00:00'))}
        except Exception as error:  # an unknown observation never starts a renewal
            return {'status': 'UNKNOWN', 'stream_id': '', 'started_at': None, 'error': type(error).__name__}


def log(event: str, **values):
    print(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'event': event, **values}), flush=True)


def main():
    user_id = os.environ.get('TWITCH_USER_ID', '').strip()
    if not user_id:
        raise SystemExit('TWITCH_USER_ID is required')
    creds = Path(os.environ['CREDENTIALS_DIRECTORY'])
    config = json.loads((creds / 'mediamtx.yml').read_text())
    destinations = config['paths'][PATH]['forward']
    helix = Helix(json.loads((creds / 'api.json').read_text()), user_id)
    state_file = Path(os.environ.get('STATE_DIRECTORY', '/tmp')) / 'keeper.json'
    keeper = Keeper(destinations)
    if state_file.exists():
        saved = json.loads(state_file.read_text())
        keeper.last_renewal_date = saved.get('last_renewal_date', '')
        if saved.get('phase') in ('WAIT_OFFLINE', 'VERIFY'):
            # Restarted mid-renewal: put Twitch back and verify from there.
            keeper.phase, keeper.phase_at, keeper.old_stream_id = 'VERIFY', time.monotonic(), saved.get('old_stream_id', '')
            api(f'/v3/config/paths/patch/{PATH}', {'forward': keeper.apply(keeper.desired())})
    log('keeper_started', forwards=len(destinations), last_renewal_date=keeper.last_renewal_date)
    next_helix, twitch = 0.0, {'status': 'UNKNOWN', 'stream_id': '', 'started_at': None}
    while True:
        now = time.monotonic()
        try:
            path = api(f'/v3/paths/get/{PATH}')
            ready = bool(path.get('ready'))
            items = api(f'/v3/paths/forward-dests/list?path={PATH}').get('items', [])
        except Exception as error:
            log('mediamtx_unreachable', error=type(error).__name__)
            time.sleep(5)
            continue
        if now >= next_helix:
            twitch = helix.status()
            next_helix = now + (10 if keeper.phase != 'IDLE' else 60)
        change = keeper.renewal(now, datetime.now(timezone.utc), ready, twitch)
        if change is None and keeper.phase == 'IDLE':
            change = keeper.heal(now, ready, items)
        if change is not None:
            try:
                api(f'/v3/config/paths/patch/{PATH}', {'forward': change})
                log('forward_patched', phase=keeper.phase, count=len(change), healed=keeper.removed is not None,
                    twitch=twitch['status'], stream_id=twitch.get('stream_id', ''))
            except Exception as error:
                log('forward_patch_failed', error=type(error).__name__)
        state_file.write_text(json.dumps(keeper.to_state()))
        time.sleep(5)


if __name__ == '__main__':
    sys.exit(main())
