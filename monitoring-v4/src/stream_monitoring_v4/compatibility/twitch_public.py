"""Project a fixed Twitch observer source to credential-free public facts."""
from __future__ import annotations
import json
import argparse
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path

SOURCE = Path('/var/lib/stream-v4-twitch-api/twitch_api_evidence.json')
RECEPTION = Path('/opt/stream-v4-twitch-public/current/reception.json')
REVISION = 'af3e63b2a45810ef81bd6567c5ca41ee9e54d3e437cae4b997900e044fa9a578'
SCHEMA = 'stream-v3-twitch-public.v1'


def timestamp(value):
    if not isinstance(value, str) or len(value) > 40: raise ValueError('invalid time')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None: raise ValueError('naive time')
    return parsed.timestamp()


def project(raw, reception, now, binding=None):
    result = {'schema': SCHEMA, 'generated_at': now,
              'channel': {'name': 'TokyoFlightTracker', 'url': 'https://www.twitch.tv/tokyoflighttracker'},
              'observation': {'provider_state': 'UNKNOWN', 'status': 'UNAVAILABLE', 'observed_at': None, 'source_fresh': False},
              'session': {'id': None, 'started_at': None, 'viewer_count': None},
              'reception': {'verified_at': None, 'video': None, 'audio': None, 'non_silent': False}}
    try:
        if not isinstance(binding,dict) or raw['schema'] != 'monitoring_v4.twitch_api_evidence.v1' or raw['source_host_id'] != 'arena-monitoring-facts' or raw['collector_revision'] != binding.get('collector_revision') or raw['user_id'] != binding.get('user_id') or raw['user_login'] != binding.get('user_login'): raise ValueError()
        observed = timestamp(raw['collected_at']); until = timestamp(raw['valid_until'])
        ttl = raw['freshness_ttl_sec']
        if type(ttl) is not int or not 180 <= ttl <= 900 or not 0 <= observed <= now or not 0 < until-observed <= ttl: raise ValueError()
        fresh = observed <= now <= until and now-observed <= ttl
        result['observation'].update(observed_at=raw['collected_at'], source_fresh=fresh)
        if raw['probe_status'] == 'unknown':
            if raw['provider_state'] != 'UNKNOWN': raise ValueError()
            result['observation']['status'] = 'NOT_CONFIGURED' if raw['error_reason'] == 'credentials_missing' else 'UNAVAILABLE'
        elif raw['probe_status'] == 'ok' and raw['http_status'] == 200 and raw['error_reason'] == '' and raw['provider_state'] in ('LIVE','OFFLINE'):
            if fresh:
                result['observation'].update(status='OBSERVED', provider_state=raw['provider_state'])
                if raw['provider_state'] == 'LIVE':
                    if not isinstance(raw['stream_id'],str) or not re.fullmatch(r'[0-9]{1,64}',raw['stream_id']) or timestamp(raw['started_at']) > now+30 or type(raw['viewer_count']) is not int or not 0 <= raw['viewer_count'] <= 1000000000: raise ValueError()
                    result['session'] = {'id':raw['stream_id'],'started_at':raw['started_at'],'viewer_count':raw['viewer_count']}
            else: result['observation']['status'] = 'STALE'
        else: raise ValueError()
    except (KeyError,TypeError,ValueError,OverflowError):
        result['observation'] = {'provider_state':'UNKNOWN','status':'UNAVAILABLE','observed_at':None,'source_fresh':False}
        result['session'] = {'id':None,'started_at':None,'viewer_count':None}
    try:
        if timestamp(reception['verified_at']) <= now and reception['video']=='1080p H.264' and reception['audio']=='48 kHz stereo AAC' and reception['non_silent'] is True:
            result['reception'] = {k:reception[k] for k in ['verified_at','video','audio','non_silent']}
    except (KeyError,TypeError,ValueError,OverflowError): pass
    return result


def bounded(path):
    try:
        if path.stat().st_size > 16384: return {}
        value=json.loads(path.read_text())
        return value if isinstance(value,dict) else {}
    except (OSError,ValueError): return {}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binding',type=Path);args=parser.parse_args()
    binding=bounded(args.binding) if args.binding else None
    print(json.dumps(project(bounded(SOURCE),bounded(RECEPTION),time.time(),binding),allow_nan=False))
