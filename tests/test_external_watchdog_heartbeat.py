import importlib.util
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('external_heartbeat', Path(__file__).resolve().parents[1] / 'ops/scripts/stream_v3_external_watchdog_heartbeat.py')
heartbeat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(heartbeat)


def test_only_fresh_same_boot_owner_evidence_allows_heartbeat():
    gateway = {'schema': 'stream.notification_gateway.v1', 'boot_id': 'boot', 'mono': 290}
    state = {'notification_clock': {'boot_id': 'boot', 'mono': 100}}
    assert heartbeat.ready(gateway, state, boot='boot', mono=300, prometheus_ready=True)
    assert not heartbeat.ready(gateway, state, boot='boot', mono=321, prometheus_ready=True)
    assert not heartbeat.ready(gateway, state, boot='reboot', mono=300, prometheus_ready=True)
    assert not heartbeat.ready(gateway, state, boot='boot', mono=300, prometheus_ready=False)
    state['notification_clock']['mono'] = float('nan')
    assert not heartbeat.ready(gateway, state, boot='boot', mono=300, prometheus_ready=True)


def test_policy_requires_an_explicit_project_bound_channel():
    for channels in [[], ['projects/other/notificationChannels/12'], ['https://example.invalid']]:
        with pytest.raises(ValueError):
            heartbeat.policy('example-project', 'observer', channels)
    value = heartbeat.policy('example-project', 'observer', ['projects/example-project/notificationChannels/12'])
    assert value['conditions'][0]['conditionAbsent']['duration'] == '300s'
    assert value['notificationChannels'] == ['projects/example-project/notificationChannels/12']
    point = heartbeat.point('example-project', 'observer', 1800000000)
    assert point['timeSeries'][0]['metric']['type'] in value['conditions'][0]['conditionAbsent']['filter']
