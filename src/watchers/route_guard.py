"""Optional adapter to the arena-owned, installed Monitoring v4 veto."""
import importlib.util
import os
from pathlib import Path
import sys

_module=None


def context():
    global _module
    path=os.environ.get('STREAM_ROUTE_CAUSALITY_MODULE','')
    if not path:return {'secondary_recovery_blocked':False,'status':'DISABLED'}
    try:
        if _module is None:
            spec=importlib.util.spec_from_file_location('installed_route_causality',Path(path))
            _module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=_module;spec.loader.exec_module(_module)
        return _module.guard()
    except Exception:return {'secondary_recovery_blocked':True,'status':'UNKNOWN','reason':'cause_context_unavailable'}


def veto():return context()['secondary_recovery_blocked']
