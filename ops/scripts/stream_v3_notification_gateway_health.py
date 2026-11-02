#!/usr/bin/env python3
"""Extend the existing independent deadman with shared-delivery observations."""
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import sys
import time


def conditions(value,*,boot,mono):
    stamp=value.get('mono')
    receipts=value.get('receipts')
    valid=(value.get('schema')=='stream.notification_gateway.v1' and value.get('boot_id')==boot
           and type(stamp)in (int,float) and math.isfinite(stamp) and 0<=mono-stamp<=30
           and isinstance(receipts,dict) and set(receipts)<={'UNKNOWN','SENDING','DELIVERED','RETRY','REJECTED'}
           and all(type(n)is int and 0<=n<=10000 for n in receipts.values()))
    pending=sum(receipts.get(k,0) for k in ('UNKNOWN','SENDING')) if valid else 0
    return {'NotificationGatewayUnavailable':(not valid,180,'critical'),
            'NotificationGatewayUnknownOutcome':(valid and pending>0,0,'critical')}


def reduce_own(values,previous,*,boot,mono,wall):
    from datetime import datetime,timezone
    result={};alerts=[]
    for name,(bad,hold,severity)in values.items():
        if not bad:continue
        old=previous.get(name,{})
        same=old.get('boot_id')==boot and type(old.get('last_mono'))in (int,float) and 0<=mono-old['last_mono']<=180
        origin=old.get('since_mono')if same else mono
        if type(origin)not in(int,float)or not math.isfinite(origin)or not 0<=origin<=mono:origin=mono
        result[name]={'boot_id':boot,'since_mono':origin,'last_mono':mono,'since':old.get('since',wall)if same else wall}
        if mono-origin>=hold:alerts.append({'labels':{'alertname':name,'severity':severity},'state':'firing',
            'activeAt':datetime.fromtimestamp(result[name]['since'],timezone.utc).isoformat()})
    return alerts,result


def main():
    path=Path(os.environ['NOTIFICATION_HEALTH_BASELINE'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=os.environ['NOTIFICATION_HEALTH_BASELINE_SHA256']:raise ValueError('health_baseline_changed')
    sys.path.insert(0,str(path.parent))
    spec=importlib.util.spec_from_file_location('health_baseline',path);base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
    base.notifier.ALERTS.update({
      'NotificationGatewayUnavailable':('Shared notification delivery is unavailable','Sent by the independent notification watchdog. This is not a stream outage.'),
      'NotificationGatewayUnknownOutcome':('Notification delivery outcome is unknown','Automatic retries are held to avoid duplicate delivery. Reconcile the delivery receipt.')})
    original_collect=base.collect;original_reduce=base.reduce_conditions
    def collect(*args,**kwargs):
        value=original_collect(*args,**kwargs)
        try:raw=base.read_json(Path('/run/stream-notification-gateway/status.json'),4096)
        except (OSError,ValueError):raw={}
        value.update(conditions(raw,boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),mono=time.clock_gettime(time.CLOCK_BOOTTIME)))
        return value
    def reduce(values,previous,wall):
        own={k:v for k,v in values.items()if k.startswith('NotificationGateway')}
        others={k:v for k,v in values.items()if k not in own}
        alerts,tracked=original_reduce(others,previous,wall)
        extra,state=reduce_own(own,previous,boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),mono=time.clock_gettime(time.CLOCK_BOOTTIME),wall=wall)
        return alerts+extra,{**tracked,**state}
    base.collect=collect;base.reduce_conditions=reduce
    return base.main()

if __name__=='__main__':raise SystemExit(main())
