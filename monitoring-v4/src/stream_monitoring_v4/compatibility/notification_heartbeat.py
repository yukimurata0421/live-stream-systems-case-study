"""Small role-bound heartbeats shared by explicit read-only file mounts."""
import fcntl
import json
import os
import stat
from pathlib import Path

_LAST_VALID = {}

def write_heartbeat(path: Path, producer: str, value: dict) -> None:
    data=json.dumps({**value,'schema_version':1,'producer':producer}).encode()
    if len(data)>4096:raise ValueError('heartbeat too large')
    fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o644)
    with os.fdopen(fd,'r+b') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):raise ValueError('heartbeat must be regular')
        fcntl.flock(stream,fcntl.LOCK_EX)
        # Keep the inode: a read-only bind mount must see subsequent writes.
        stream.seek(0);stream.write(data);stream.truncate();stream.flush();os.fsync(stream.fileno())


def read_heartbeat(path: Path, producer: str):
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(fd,'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):return None
            fcntl.flock(stream,fcntl.LOCK_SH|fcntl.LOCK_NB)
            raw=stream.read(4097)
        if len(raw)>4096:return None
        value=json.loads(raw)
        if value.get('schema_version')!=1 or value.get('producer')!=producer:return None
        _LAST_VALID[(str(path),producer)]=value
        return value
    except BlockingIOError:
        # A concurrent publisher write may hold the lock briefly. Preserve the
        # last complete sample's original timestamp; callers still enforce age.
        return _LAST_VALID.get((str(path),producer))
    except (OSError,ValueError,AttributeError,TypeError):return None
