import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT/'ops/fifo-lab'
spec = importlib.util.spec_from_file_location('fifo_summary',LAB/'summarize.py')
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


def test_curated_timings_reproduce_every_published_group():
    data=json.loads((ROOT/'examples/fifo-recovery/measurements-20261024.json').read_text())
    expected=json.loads((ROOT/'examples/fifo-recovery/summary-20261024.json').read_text())
    assert len(data['records']) == 57
    assert summary.summarize(data['records']) == expected
    for row in data['records']:
        assert row['restore_to_rtmps_ack_sec'] == pytest.approx(
            row['restore_to_successful_tcp_begin_wait_sec'] + row['restore_to_tcp_residual_sec']
            + row['tcp_complete_to_publish_sec'],abs=1e-8)
    assert data['ping_interval_during_measurement_sec']==1
    assert data['current_tool_ping_interval_sec']==.5


def test_missing_stage_remains_missing_and_single_sample_has_no_sd():
    rows=[{'campaign':'fixture','outage_setting_sec':12,'variant':'fifo',
           'restore_phase':'unknown','restore_to_rtmps_ack_sec':2,
           'tcp_complete_to_tls_sec':None}]
    result=summary.summarize(rows)[0]
    assert result['metrics']['restore_to_rtmps_ack_sec']['sample_sd'] is None
    assert 'tcp_complete_to_tls_sec' not in result['metrics']


@pytest.mark.parametrize('tool',[p.name for p in LAB.glob('*.py')if p.name!='summarize.py'])
def test_tool_help_imports_without_starting_a_host(tool):
    process=subprocess.run([sys.executable,'-B',str(LAB/tool),'--help'],
        capture_output=True,text=True,timeout=8)
    assert process.returncode==0,process.stderr
    if tool!='node.py':
        assert '--host' in process.stdout and '--mediamtx' in process.stdout


def test_nvenc_guard_uses_explicit_observation_path(tmp_path):
    sys.path.insert(0,str(LAB))
    try:
        from nvenc_transport_timing import NvencTransportLab
        lab=object.__new__(NvencTransportLab)
        path=tmp_path/'observation.json';path.write_text('{"fixture":true}')
        lab.args=SimpleNamespace(guard_observation=str(path))
        def cat(command,**kwargs):
            assert command==['sudo','-n','cat',str(path)]
            return path.read_bytes()
        with patch('nvenc_transport_timing.subprocess.check_output',side_effect=cat):
            assert lab.production_state()=={'fixture':True}
    finally:
        sys.path.remove(str(LAB))


@pytest.mark.skipif(shutil.which('cc') is None,reason='Optional C compiler check')
def test_sender_protocol_probe_builds_without_runtime_injection(tmp_path):
    process=subprocess.run(['cc','-std=c11','-shared','-fPIC','-O2','-Wall','-Wextra','-Werror',
        '-o',str(tmp_path/'transport_probe.so'),str(LAB/'transport_probe.c'),'-ldl'],
        capture_output=True,text=True,timeout=15)
    assert process.returncode==0,process.stderr
