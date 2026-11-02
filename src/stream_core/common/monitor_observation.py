"""Read existing owner-qualified samples through the common observation schema.

No counter or service verdict is inferred from file mtime or scrape success.
The original producer clock qualification and source-specific limits remain.
"""
from __future__ import annotations
from pathlib import Path
from . import observation_time as timing, upload_samples
from .observation_contract import Observation, Validity, TargetState, Activity, finite
from .adsb_timing import adsb_source_timing

SOURCES={
 'youtube_watchdog':('youtube_watchdog_stats.json',300),
 'map_runtime':('map_runtime_status.json',360),
 'viewer_synthetic':('viewer_synthetic_status.json',600),
 'reliability_rollup':('operational_reliability_status.json',7200),
 'reliability_burn':('operational_reliability_burn_status.json',7200),
 'external_blackbox':('external_blackbox_status.json',900),
 'adsb_overlay_probe':('watchdog/adsb_freshness_state.json',180),
 'subsystems':('subsystems_status.json',180),
}


def sample(source,payload,*,now,max_age):
    scope='adsb_source_heartbeat' if source=='adsb_overlay_probe' else source+'_observation'
    q=timing.qualify(payload.get('observation_time'),now=now,source=source,max_age=max_age)
    if not q.valid:
        return Observation(source,scope,Validity.UNAVAILABLE,'source_unverifiable',max_age_seconds=max_age)
    state=TargetState.UNKNOWN
    if source=='adsb_overlay_probe':
        detail=adsb_source_timing(payload,now=now.wall,reading=now)
        if not detail['available']:
            return Observation(source,scope,Validity.UNAVAILABLE,'upstream_unverifiable',max_age_seconds=max_age)
        state=TargetState.OK if detail['heartbeat_fresh'] else TargetState.FAIL
    # Other adapters expose observation validity. A fresh JSON is not proof of
    # overall delivery, formal SLO compliance or readiness to execute recovery.
    return Observation.measured(source=source,scope=scope,age_upper_seconds=q.age_upper,
        max_age_seconds=max_age,target_state=state)


def mesh(payload,*,now):
    source='clock_mesh';scope='host_clock_synchronization'
    stamp=payload.get('mono');sync=payload.get('clock_synchronized')
    if payload.get('boot_id')!=now.boot or not finite(stamp) or not 0<=now.mono-stamp or type(sync)is not bool:
        return Observation(source,scope,Validity.UNAVAILABLE,'clock_mesh_unverifiable',max_age_seconds=90)
    return Observation.measured(source=source,scope=scope,age_upper_seconds=now.mono-stamp,max_age_seconds=90,
        target_state=TargetState.OK if sync else TargetState.FAIL)


def read(root:Path,mesh_path:Path,*,now=None):
    now=timing.capture() if now is None else now
    result=[]
    for name,(relative,limit)in SOURCES.items():
        try:payload=upload_samples.read_json(root/relative,2*1024*1024)
        except (OSError,ValueError):payload={}
        result.append(sample(name,payload,now=now,max_age=limit))
    try:payload=upload_samples.read_json(mesh_path,256*1024)
    except (OSError,ValueError):payload={}
    result.append(mesh(payload,now=now))
    return result
