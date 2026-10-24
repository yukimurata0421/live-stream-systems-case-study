from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import json
import shutil
import subprocess

import pytest

from stream_core.engine.fifo_runtime import TransportMetricsCollector, make_transport_collector
from stream_core.engine.transport_telemetry import TransportTelemetry
from stream_core.engine.ffmpeg_args import build_ffmpeg_args
from stream_core.engine.config import load_config
from runtime_boundary.observation import RuntimeObservationPublisher


def socket(cookie, ack):
    return (f'ESTAB 0 64 192.0.2.2:30000 192.0.2.1:1936 '
            f'users:(("ffmpeg",pid=77,fd=3)) ino:{cookie} sk:{cookie}\n'
            f' cubic rto:250 bytes_sent:{ack+64} bytes_acked:{ack} notsent:64 unacked:2\n')


def test_collector_is_wired_into_real_runtime_publisher_and_retains_missing_socket(tmp_path):
    transport = TransportTelemetry(77, 'engine:1:77')
    collector = TransportMetricsCollector(tmp_path/'run', journal_root=tmp_path/'journal')
    publisher = RuntimeObservationPublisher(path=tmp_path/'runtime.json',
        projection_path=tmp_path/'projection.json', target_snapshot_path=tmp_path/'target.json',
        process_supplier=lambda: {'local_ffmpeg_pid':77,'ffmpeg_generation':'engine:1:77'},
        rtmp_ports=(1936,), tcp_metrics_supplier=collector)
    def read(text):
        with patch('stream_core.engine.fifo_runtime.subprocess.run',
                   return_value=SimpleNamespace(returncode=0,stdout=text)):
            return publisher.publish()
    first = read(socket('1001', 8_000_000))
    transport.line('[fifo @ fixture] Recovery successful')
    second = read(socket('1002', 200))
    third = read(socket('1002', 500))
    assert first['local_ffmpeg_pid'] == second['local_ffmpeg_pid'] == 77
    assert second['tcp_metrics']['transport_connection_generation'] == 2
    assert third['tcp_metrics']['transport_reconnections_total'] == 1
    assert third['tcp_metrics']['transport_state'] == 'ACK_ADVANCING'
    assert json.loads((tmp_path/'run/ffmpeg-transport.json').read_text())['delivery_or_video_verified'] is False
    assert (tmp_path/'journal/ffmpeg-transport-events.jsonl').exists()
    assert read('')['tcp_metrics'] == {}
    assert json.loads((tmp_path/'run/ffmpeg-transport.json').read_text())['state'] == 'SOCKET_UNAVAILABLE'


def test_factory_preserves_non_fifo_and_test_modes(monkeypatch, tmp_path):
    assert make_transport_collector(SimpleNamespace(use_fifo_recovery=False,test_mode=False),tmp_path/'run/state.json') is None
    assert make_transport_collector(SimpleNamespace(use_fifo_recovery=True,test_mode=True),tmp_path/'run/state.json') is None
    monkeypatch.setenv('STREAM_FIFO_TELEMETRY_JOURNAL_DIR',str(tmp_path/'persistent'))
    collector=make_transport_collector(SimpleNamespace(use_fifo_recovery=True,test_mode=False),tmp_path/'run/state.json')
    assert collector.sink.journal_root == tmp_path/'persistent'


@pytest.mark.skipif(shutil.which('ffmpeg') is None or shutil.which('ffprobe') is None,
                    reason='Optional local FFmpeg/ffprobe AV output check')
def test_full_fifo_builder_keeps_both_av_streams_in_real_flv(tmp_path):
    cfg = load_config()
    cfg.use_fifo_recovery = True
    cfg.test_mode = False
    cfg.fifo_max_recovery_attempts = 20
    cfg.video_encoder = 'libx264'
    cfg.video_preset = 'ultrafast'
    cfg.output_size = '160x90'
    cfg.rtmp_url = str(tmp_path/'av.flv')
    args = build_ffmpeg_args(cfg,x11_input='fixture',pulse_source='fixture',encoder_profile={})
    # Replace capture devices only. Keep all final filters, maps, codecs and FIFO args.
    output = args[args.index('-filter_complex'):]
    output[-1:-1] = ['-t','0.4','-threads:v','1']
    command = ['ffmpeg','-nostdin','-y','-hide_banner','-loglevel','error',
        '-filter_complex_threads','1','-f','lavfi','-i','testsrc2=size=160x90:rate=5',
        '-f','lavfi','-i','sine=frequency=997:sample_rate=48000',*output]
    process = subprocess.run(command,capture_output=True,text=True,timeout=15)
    assert process.returncode == 0,process.stderr
    streams=json.loads(subprocess.check_output(['ffprobe','-v','error','-show_streams',
        '-of','json',str(tmp_path/'av.flv')],text=True,timeout=5))['streams']
    assert [(s['codec_type'],s['codec_name'])for s in streams]==[('video','h264'),('audio','aac')]
    assert all(s.get('extradata_size',0)>0 for s in streams)
