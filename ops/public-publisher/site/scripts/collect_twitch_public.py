"""Fetch only the arena-produced public Twitch artifact; no provider credentials."""
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

PUBLIC = Path(__file__).resolve().parents[1]/'public'
SCHEMA = 'stream-v3-twitch-public.v1'


def validate(value):
    if not isinstance(value,dict) or set(value)!={'schema','generated_at','channel','observation','session','reception'} or value['schema']!=SCHEMA: raise ValueError()
    if value['channel']!={'name':'TokyoFlightTracker','url':'https://www.twitch.tv/tokyoflighttracker'}: raise ValueError()
    if set(value['observation'])!={'provider_state','status','observed_at','source_fresh'} or value['observation']['provider_state'] not in ('LIVE','OFFLINE','UNKNOWN') or value['observation']['status'] not in ('OBSERVED','NOT_CONFIGURED','STALE','UNAVAILABLE') or type(value['observation']['source_fresh']) is not bool: raise ValueError()
    if set(value['session'])!={'id','started_at','viewer_count'} or set(value['reception'])!={'verified_at','video','audio','non_silent'}: raise ValueError()
    for stamp in [value['observation']['observed_at'],value['session']['started_at'],value['reception']['verified_at']]:
        if stamp is not None and (not isinstance(stamp,str) or not re.fullmatch(r'[0-9T:.+Z-]{10,40}',stamp)): raise ValueError()
    sid=value['session']['id']; viewers=value['session']['viewer_count']
    if sid is not None and (not isinstance(sid,str) or not re.fullmatch(r'[0-9]{1,64}',sid)): raise ValueError()
    if viewers is not None and (type(viewers) is not int or not 0<=viewers<=1000000000): raise ValueError()
    if value['reception']['video'] not in (None,'1080p H.264') or value['reception']['audio'] not in (None,'48 kHz stereo AAC') or type(value['reception']['non_silent']) is not bool: raise ValueError()
    generated=value['generated_at']
    if type(generated) not in (int,float) or not 0<=time.time()-generated<=180: raise ValueError()
    return value


def main():
    destination=PUBLIC/'twitch-observation.json'
    try:
        result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','arena-server','python3','/opt/stream-v4-twitch-public/current/twitch_public.py'],capture_output=True,timeout=15)
        if result.returncode or len(result.stdout)>16384: raise ValueError()
        value=validate(json.loads(result.stdout))
    except (OSError,ValueError,TypeError,KeyError,subprocess.TimeoutExpired):
        print(json.dumps({'twitch_public_source':'unavailable','last_artifact_retained':destination.exists()}))
        if destination.exists(): return 0
        value={'schema':SCHEMA,'generated_at':time.time(),'channel':{'name':'TokyoFlightTracker','url':'https://www.twitch.tv/tokyoflighttracker'},'observation':{'provider_state':'UNKNOWN','status':'UNAVAILABLE','observed_at':None,'source_fresh':False},'session':{'id':None,'started_at':None,'viewer_count':None},'reception':{'verified_at':None,'video':None,'audio':None,'non_silent':False}}
    PUBLIC.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(prefix='.twitch-',dir=PUBLIC)
    try:
        with os.fdopen(fd,'w') as out:
            os.fchmod(out.fileno(),0o644);json.dump(value,out,allow_nan=False);out.write('\n')
        os.replace(name,destination)
    finally:
        if os.path.exists(name): os.unlink(name)
    return 0


if __name__=='__main__': raise SystemExit(main())
