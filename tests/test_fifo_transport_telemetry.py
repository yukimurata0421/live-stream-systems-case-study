from pathlib import Path
from types import SimpleNamespace
import json

import pytest

from stream_core.engine.fifo_runtime import parse_sockets
from stream_core.engine.transport_telemetry import TelemetryFiles, TransportTelemetry
from stream_core.engine.ffmpeg_stderr import FFmpegStderrCapture, redact_stderr_line
from stream_core.engine.ffmpeg_args import build_output_args


def socket(cookie='a1', ack=100, *, pid=77, state='ESTAB', inode=1001):
    return (f'{state} 0 64 192.0.2.2:30000 192.0.2.1:1936 '
            f'users:(("ffmpeg",pid={pid},fd=3)) uid:0 ino:{inode} sk:{cookie}\n'
            f' cubic rto:250 bytes_sent:{ack+64} bytes_acked:{ack} notsent:64 unacked:2\n')


def test_pid_ownership_and_single_established_socket():
    text = socket('dead', pid=77, state='FIN-WAIT-1') + socket('beef', ack=222)
    value = parse_sockets(text, 77, (1936,))
    assert value['socket_cookie'] == 'beef' and value['bytes_acked'] == 222
    assert parse_sockets(text, 78, (1936,)) == {}
    assert parse_sockets(text, 77, (443,)) == {}
    assert parse_sockets(socket('a') + socket('b'), 77, (1936,)) == {}


def test_same_pid_new_connection_resets_socket_counter_not_process_generation(tmp_path):
    clock = [10.0]
    state = TransportTelemetry(77, 'engine:1:77', clock=lambda: clock[0])
    first = state.observe(parse_sockets(socket(ack=8_000_000), 77, (1936,)))
    assert first['connection_generation'] == 1
    clock[0] += 1
    state.observe(parse_sockets(socket(ack=8_000_500), 77, (1936,)))
    state.line('[tls @ 0x1] IO error: Connection reset by peer')
    clock[0] += 1
    state.observe({})
    state.line('[fifo @ 0x1] Recovery successful')
    second = state.observe(parse_sockets(socket('b2', ack=2000, inode=1002), 77, (1936,)))
    assert second['ffmpeg_generation'] == first['ffmpeg_generation']
    assert second['connection_generation'] == 2 and second['reconnections_observed_total'] == 1
    assert second['fifo_recovery_reports_total'] == 1
    assert second['last_fifo_recovery_duration_sec'] == 1
    assert second['ack_bytes_observed_total'] == 8_002_500
    assert second['delivery_or_video_verified'] is False
    sink = TelemetryFiles(tmp_path)
    sink.write(second)
    sink.write(second)
    events = [json.loads(x) for x in (tmp_path/'ffmpeg-transport-events.jsonl').read_text().splitlines()]
    assert len(events) == len(second['events'])
    assert json.loads((tmp_path/'ffmpeg-transport.json').read_text())['connection_generation'] == 2
    assert 'stream_v3_ffmpeg_transport_reconnections_total 1\n' in (tmp_path/'ffmpeg-transport.prom').read_text()


def test_counter_regression_is_unknown_and_ack_stall_requires_pending_data():
    clock = [0.0]
    state = TransportTelemetry(77, 'g', clock=lambda: clock[0])
    state.observe(parse_sockets(socket(ack=100), 77, (1936,)))
    clock[0] = 5
    assert state.observe(parse_sockets(socket(ack=100), 77, (1936,)))['state'] == 'TCP_ACK_STALLED'
    assert state.observe(parse_sockets(socket(ack=50), 77, (1936,)))['state'] == 'COUNTER_REGRESSION'
    assert state.observe(parse_sockets(socket(ack=200), 77, (1936,)))['state'] == 'ACK_ADVANCING'


def test_unobserved_error_has_no_invented_recovery_duration():
    state = TransportTelemetry(77, 'g')
    state.line('[fifo @ 0x1] Recovery successful')
    value = state.observe({})
    assert value['last_fifo_recovery_duration_sec'] is None
    assert value['fifo_recovery_reports_total'] == 1


def test_event_journal_can_live_on_persistent_storage_separate_from_snapshot(tmp_path):
    state = TransportTelemetry(77, 'g')
    value = state.observe(parse_sockets(socket(), 77, (1936,)))
    TelemetryFiles(tmp_path/'run', journal_root=tmp_path/'persistent').write(value)
    assert (tmp_path/'persistent/ffmpeg-transport-events.jsonl').exists()
    assert not (tmp_path/'run/ffmpeg-transport-events.jsonl').exists()
    assert (tmp_path/'run/ffmpeg-transport.prom').exists()


def test_inner_timeout_and_finite_retries(monkeypatch):
    monkeypatch.setenv('FFMPEG_RW_TIMEOUT_ENABLED', '1')
    monkeypatch.setenv('FFMPEG_RW_TIMEOUT_USEC', '3000000')
    cfg = SimpleNamespace(test_mode=False, relay_primary_url='', use_fifo_recovery=True,
        fifo_recovery_wait_sec=.2, fifo_max_recovery_attempts=20, fifo_queue_size=32,
        fifo_drop_pkts_on_overflow=True, fifo_restart_with_keyframe=True,
        rtmp_url='rtmps://example.invalid/relay')
    args = build_output_args(cfg)
    assert '-rw_timeout' not in args
    assert args[args.index('-format_opts')+1] == 'rw_timeout=3000000'
    assert args[args.index('-recovery_wait_time')+1] == '0.2'
    cfg.fifo_max_recovery_attempts = 0
    with pytest.raises(ValueError, match='FINITE'):
        build_output_args(cfg)


def test_stderr_capture_redacts_all_rtmp_destinations_and_updates_facts(tmp_path):
    import io
    value = ('[fifo @ x] Recovery successful\n'
             "Output to rtmps://user:password@example.invalid:443/app/stream-secret?token=secret\n")
    capture = FFmpegStderrCapture(path=tmp_path/'stderr.jsonl', max_bytes=4096,
        backup_count=1, run_id='r', ffmpeg_pid=77, stream_generation='g')
    capture.start(io.BytesIO(value.encode()))
    result = capture.stop()
    assert not result['capture_error']
    stored = (tmp_path/'stderr.jsonl').read_text()
    assert 'password' not in stored and 'stream-secret' not in stored and 'token=secret' not in stored
    assert capture.transport.observe({})['fifo_recovery_reports_total'] == 1
    assert redact_stderr_line('rtmp://host/live2/key') == 'rtmp://host/live2/***'
