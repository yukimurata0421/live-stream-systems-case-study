"""Add prolonged route-observation uncertainty to the existing critical route.

No provider, credential, recovery effect or independent delivery queue is owned
here. The original notification loop retains routing, retries and receipts.
"""
from __future__ import annotations
import dataclasses
import hashlib
import importlib.util
import math
import os
from pathlib import Path
import sys
import time

IDENT='route:youtube_observation_unconfirmed'
AFTER=300


def pinned_reader():
    path=Path(os.environ['STREAM_ROUTE_OBSERVATION_GUARD_MODULE'])
    expected=os.environ['STREAM_ROUTE_OBSERVATION_GUARD_SHA256']
    if hashlib.sha256(path.read_bytes()).hexdigest()!=expected:raise ValueError('route_observation_guard_changed')
    spec=importlib.util.spec_from_file_location('notification_route_facts',path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    return module.notification_context


def incident(context,prior,now):
    verified=context.get('status')=='VERIFIED'
    diagnostic=context.get('clock_diagnostic') if verified else None
    if isinstance(diagnostic,dict):
        if diagnostic.get('status')=='VERIFIED':return None
        age=diagnostic.get('fault_for_sec')
        due=(diagnostic.get('status')=='FAULT' and type(age) in (int,float)
             and math.isfinite(age) and age>=AFTER)
    else:
        # Old producer during a rolling upgrade; preserve pending incidents.
        if verified and context.get('assessment')=='HEALTHY':return None
        held=context.get('clock_observation_hold_sec',0.) if verified else 0.
        due=(verified and context.get('assessment')=='CLOCK_FAULT'
             and type(held) in (int,float) and math.isfinite(held) and held>=AFTER)
    if not due and not prior:return None
    return {'id':IDENT,'component':'route_observation','severity':'critical','observed_ts':now,
            'summary':'Route UTC diagnostics remain unconfirmed',
            'detail':'UTC diagnostics have failed for at least five minutes. Media health and backup start/stop use independently verified boottime freshness. Check clock synchronization and timestamp producers. Viewer reception is unverified.',
            'source':'route_causality','evidence':'fresh_clock_diagnostic' if due else 'previous_alert_recovery_unconfirmed'}


def wrap_context(ctx,read_context):
    view={}
    def collect(*args,**kwargs):
        nonlocal view
        rows=ctx.collect_incidents(*args,**kwargs)
        try:view=read_context()
        except (OSError,ValueError,KeyError,TypeError):view={'status':'UNKNOWN'}
        if not isinstance(view,dict):view={'status':'UNKNOWN'}
        state=ctx.load_state();active=state.get('active');prior=active.get(IDENT) if isinstance(active,dict) else None
        value=incident(view,prior,int(kwargs.get('now_ts',time.time())))
        return [*rows,*([value] if value else [])]
    def recovery(ident,now):
        if ident==IDENT:
            return (now,'Fresh verified UTC diagnostics returned; media health and viewer recovery are evaluated separately.')
        return ctx.recovery_observation_for_incident(ident,now)
    return dataclasses.replace(ctx,collect_incidents=collect,recovery_observation_for_incident=recovery)
