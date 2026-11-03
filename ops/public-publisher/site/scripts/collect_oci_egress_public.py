"""Copy arena's egress-only OCI data transfer projection (Always Free 10 TB/month) into the public tree."""
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path
from public_snapshot_time import PublicTimestampError, validate_generated_at

PUBLIC=Path(__file__).resolve().parents[1]/'public'
COUNTS=['quota_bytes','month_egress_bytes','month_projected_egress_bytes','month_projected_egress_bytes_24h_rate','trailing_24h_egress_bytes','trailing_7d_avg_daily_egress_bytes']
STAMPS=['observed_at','latest_data_at','month_start','month_end']


def timestamp(v):
    if not isinstance(v,str) or len(v)>40:raise ValueError()
    t=datetime.fromisoformat(v.replace('Z','+00:00'))
    if t.tzinfo is None:raise ValueError()
    return t.timestamp()


def count(v):
    if type(v) is not int or not 0<=v<=10**15:raise ValueError()


def validate(v,now):
    if not isinstance(v,dict) or set(v)!={'schema','generated_at','month','days',*COUNTS,*STAMPS}:raise ValueError('UNEXPECTED_PUBLIC_FIELD')
    if v['schema']!='stream.oci_egress_public.v1':raise ValueError()
    validate_generated_at(v['generated_at'],now)
    for k in STAMPS:timestamp(v[k])
    for k in COUNTS:count(v[k])
    if not v['quota_bytes'] or not re.fullmatch(r'\d{4}-\d{2}',str(v['month'])) or not v['month_start'].startswith(v['month']):raise ValueError()
    if not isinstance(v['days'],list) or len(v['days'])>31:raise ValueError()
    for d in v['days']:
        if not isinstance(d,dict) or set(d)!={'date','egress_bytes'} or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',str(d['date'])) or not d['date'].startswith(v['month']):raise ValueError()
        count(d['egress_bytes'])
    return v


def main():
    path=PUBLIC/'oci-egress.json'
    try:
        r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',os.environ.get('STREAM_V3_EGRESS_SSH_TARGET','stream-monitor'),'curl','-sf','--max-time','5','http://127.0.0.1:9119/public.json'],capture_output=True,timeout=12)
        if r.returncode or len(r.stdout)>16384:raise ValueError()
        value=validate(json.loads(r.stdout),time.time())
    except (OSError,ValueError,TypeError,KeyError,subprocess.TimeoutExpired) as exc:
        reason=str(exc) if isinstance(exc,PublicTimestampError) else 'FETCH_OR_SCHEMA_INVALID'
        print(json.dumps({'oci_egress_public_source':'unavailable','last_artifact_retained':path.exists(),'reason':reason}))
        # Retain the original timestamps: a failed fetch never rejuvenates evidence.
        return 0
    fd,name=tempfile.mkstemp(prefix='.oci-egress-',dir=PUBLIC)
    try:
        with os.fdopen(fd,'w') as out:
            os.fchmod(out.fileno(),0o644);json.dump(value,out,allow_nan=False);out.write('\n');out.flush();os.fsync(out.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):os.unlink(name)
    return 0


if __name__=='__main__':raise SystemExit(main())
