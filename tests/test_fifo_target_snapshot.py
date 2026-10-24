from pathlib import Path
from types import SimpleNamespace

import pytest

from dell_recovery_agent import delivery_command as module
from dell_recovery_agent.target_snapshot import AtomicKubernetesTargetSnapshotProducer


def raw(*args):return b'\0'.join(a.encode() for a in args)+b'\0'


@pytest.mark.parametrize('args,accepted',[
    (('ffmpeg','-f','pulse','-i','sink','-f','flv','rtmps://example.invalid/relay'),True),
    (('ffmpeg','-f','pulse','-i','sink','-f','fifo','-fifo_format','flv','rtmps://example.invalid/relay'),True),
    (('ffmpeg','-f','fifo','-fifo_format','mpegts','rtmps://example.invalid/relay'),False),
    (('ffmpeg','-f','fifo','rtmps://example.invalid/relay'),False),
    (('ffmpeg','-f','flv','-i','rtmps://example.invalid/relay','-f','null','-'),False),
    (('ffmpeg','-f','flv','rtmps://example.invalid/a','rtmps://example.invalid/b'),False),
    (('ffmpeg','-f','tee','rtmps://example.invalid/a'),False),
    (('python3','-f','flv','rtmps://example.invalid/a'),False),
])
def test_real_argv_classifier(args,accepted):
    assert module.is_delivery_command(raw(*args)) is accepted


def test_cgroup_unique_pid_and_start_ticks_still_required(tmp_path):
    def process(pid,container='owned'):
        p=tmp_path/str(pid);p.mkdir()
        (p/'cgroup').write_text('0::/kube/'+container)
        (p/'cmdline').write_bytes(raw('ffmpeg','-f','fifo','-fifo_format','flv','rtmps://example.invalid/relay'))
        (p/'stat').write_text('ticks-'+str(pid))
    process(11,'foreign')
    process(12)
    producer=SimpleNamespace(proc_root=tmp_path,_start_ticks=lambda value:value)
    assert AtomicKubernetesTargetSnapshotProducer._ffmpeg(producer,'containerd://owned')==(12,'ticks-12')
    with pytest.raises(ValueError,match='CARDINALITY'):
        AtomicKubernetesTargetSnapshotProducer._ffmpeg(producer,'containerd://missing')
    process(13)
    with pytest.raises(ValueError,match='CARDINALITY'):
        AtomicKubernetesTargetSnapshotProducer._ffmpeg(producer,'containerd://owned')
