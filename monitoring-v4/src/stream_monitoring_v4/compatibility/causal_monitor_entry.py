"""Pin the existing monitor and its two observers; add only causal admission."""
import dataclasses
import hashlib
import importlib.util
import json
from pathlib import Path
import sys


def main():
    root=Path(__file__).parent
    manifest=json.loads((root/'monitor-pins.json').read_text())
    def verify(key):
        item=manifest[key];path=Path(item['path'])
        if hashlib.sha256(path.read_bytes()).hexdigest()!=item['sha256']:raise RuntimeError('monitor_source_drift')
        return path
    original_entry=verify('monitor_entry');sys.path.insert(0,str(root))
    if '--check-imports' in sys.argv:
        import route_causality
        for kind in ('stream_watchdog','youtube_monitor'):
            target=verify(kind)
            spec=importlib.util.spec_from_file_location('causal_legacy_'+kind,target)
            module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
            route_causality.install_hooks(module,kind)
            if kind=='stream_watchdog':assert callable(module.allow_restart)
            else:assert callable(module.decide_action) and callable(module.force_transition_live_once)
        print(json.dumps({'imports':'PASS','pinned_watchdogs':2,'effects_executed':0}))
        return 0
    if len(sys.argv)>1 and sys.argv[1] in ('stream_watchdog','youtube_monitor'):
        kind=sys.argv.pop(1);target=verify(kind)
        spec=importlib.util.spec_from_file_location('causal_legacy_watchdog',target)
        module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
        import route_causality
        route_causality.install_hooks(module,kind)
        return module.main(force_live_once=module.parse_args().force_live_once) if kind=='youtube_monitor' else module.main()
    # Loading this entry preserves the original baseline, flags and task set.
    spec=importlib.util.spec_from_file_location('causal_legacy_monitor',original_entry)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    original=module.control_loop.monitor_tasks
    def tasks(source):
        values=original(source)
        for kind in ('stream_watchdog','youtube_monitor'):
            if sum(t.name==kind for t in values)!=1:raise RuntimeError('monitor_task_drift')
        return [dataclasses.replace(t,command=(t.command[0],str(Path(__file__).resolve()),t.name))
                if t.name in ('stream_watchdog','youtube_monitor') else t for t in values]
    module.control_loop.monitor_tasks=tasks
    return module.control_loop.main()


if __name__=='__main__':raise SystemExit(main())
