"""Attach only the independent upload reader to an existing exporter release."""
import importlib.util
from pathlib import Path
import sys


def install(module):
    source = Path(__file__).resolve().parents[2] / 'src/stream_core/common/upload_samples.py'
    spec = importlib.util.spec_from_file_location('independent_upload_records', source)
    records = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = records
    spec.loader.exec_module(records)
    history, latest = module.tcp_send_rows, module.latest_tcp_send_observation

    def history_with_measurements(state_root, *, now):
        return records.exporter_rows(state_root, history(state_root, now=now), now)

    def latest_with_measurements(state_root, *, now):
        selected = records.latest(state_root, now)
        return latest(state_root, now=now) if selected is None else selected

    module.tcp_send_rows = history_with_measurements
    module.latest_tcp_send_observation = latest_with_measurements
