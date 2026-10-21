"""Fresh monitor facts must not wait for the expensive legacy cache to expire."""
import importlib.util
import json
import time
from pathlib import Path
from stream_v3.viewer_clock import utc_timestamp

spec=importlib.util.spec_from_file_location('clock_exporter_cache',Path(__file__).parents[1]/'ops/scripts/stream_v3_prometheus_exporter.py')
exporter=importlib.util.module_from_spec(spec);spec.loader.exec_module(exporter)


def test_clock_metrics_refresh_inside_live_legacy_cache(tmp_path):
    cache=exporter.MetricsCache(repo_root=tmp_path,state_root=tmp_path,ttl_sec=60,timeout_sec=1,
        health_snapshot_file=None,max_health_snapshot_age_sec=1,objective_snapshot_file=None,max_objective_snapshot_age_sec=1)
    cache._payload='legacy_metric 7\n';cache._updated=time.monotonic();cache._last_success_updated=cache._updated
    directory=tmp_path/'oci_monitor';directory.mkdir();file=directory/'observation.json';now=time.time()
    row={'checked_at_utc':utc_timestamp(now),'captured_at_utc':utc_timestamp(now),'clock_state':'ADVANCING_CONFIRMED',
         'capture_ok':True,'ocr_verified':True,'clock_pair_consistent':True,'source_binding_ok':True}
    document={'schema':'stream_v3.oci_monitor_bundle.v1','observer_contract':{'compatible':True,'runtime_attested':True},'state':{'targets':{'oci_relay':row}}}
    file.write_text(json.dumps(document));first,_=cache.get()
    assert 'legacy_metric 7' in first and 'stream_v3_oci_monitor_clock_advancing 1.0' in first
    row['clock_state']='OCR_UNCERTAIN';file.write_text(json.dumps(document));second,_=cache.get()
    assert 'legacy_metric 7' in second and 'stream_v3_oci_monitor_clock_advancing 0.0' in second
    assert second.count('# TYPE stream_v3_oci_monitor_clock_advancing ')==1
