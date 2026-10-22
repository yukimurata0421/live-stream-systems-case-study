#!/usr/bin/env python3
"""Retain the existing exporter baseline and append independent observation metrics."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time


def main():
    baseline = Path(os.environ["OCI_MONITOR_EXPORTER_BASELINE"])
    if hashlib.sha256(baseline.read_bytes()).hexdigest() != os.environ["OCI_MONITOR_EXPORTER_BASELINE_SHA256"]:
        raise ValueError("exporter_baseline_changed")
    spec = importlib.util.spec_from_file_location("branch_exporter_baseline", baseline)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    metrics_path = Path(__file__).with_name("oci_monitor_metrics.py")
    if not metrics_path.is_file():
        metrics_path = Path(__file__).resolve().parents[2] / "src" / "stream_v3" / "oci_monitor_metrics.py"
    metrics_spec = importlib.util.spec_from_file_location("oci_monitor_metrics", metrics_path)
    metrics = importlib.util.module_from_spec(metrics_spec)
    metrics_spec.loader.exec_module(metrics)
    write_metrics = metrics.write_metrics
    thumbnail_writer=None
    if os.environ.get('TWITCH_THUMBNAIL_OBSERVATION_FILE'):
        thumbnail_path=Path(__file__).with_name('twitch_thumbnail_metrics.py')
        if not thumbnail_path.is_file():thumbnail_path=Path(__file__).resolve().parents[2]/'src/stream_v3/twitch_thumbnail_metrics.py'
        thumbnail_spec=importlib.util.spec_from_file_location('twitch_thumbnail_metrics',thumbnail_path)
        thumbnail_module=importlib.util.module_from_spec(thumbnail_spec);thumbnail_spec.loader.exec_module(thumbnail_module)
        thumbnail_writer=thumbnail_module.write_metrics
    original = module.MetricsCache.get
    thumbnail_publication=thumbnail_module.PublicationReader(started_at=time.time()) if thumbnail_writer else None

    def combined(cache):
        payload,error = original(cache)
        try:
            path = Path(os.environ["OCI_MONITOR_OBSERVATION_FILE"])
            if path.stat().st_size > 65536:
                raise ValueError("observation_too_large")
            observation = json.loads(path.read_text())
        except (OSError, ValueError):
            observation = {}
        writer=module.MetricWriter()
        write_metrics(writer, observation, now=time.time())
        if thumbnail_writer:
            now=time.time()
            thumbnail=thumbnail_publication.read(os.environ['TWITCH_THUMBNAIL_OBSERVATION_FILE'],now=now)
            thumbnail_writer(writer,thumbnail,now=now,last_cycle_epoch=thumbnail_publication.last_cycle_epoch)
        prefixes=('stream_v3_oci_monitor_','stream_v3_youtube_fallback_monitor_','stream_v3_monitor_probe_','stream_v3_twitch_thumbnail_')
        # Keep the existing collector cache; refresh only this small RAM-state projection.
        lines=[line for line in payload.splitlines() if not line.removeprefix('# HELP ').removeprefix('# TYPE ').startswith(prefixes)]
        return '\n'.join(lines)+'\n'+writer.render(),error

    module.MetricsCache.get = combined
    return module.main()


if __name__ == "__main__":
    raise SystemExit(main())
