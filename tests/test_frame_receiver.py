import shutil
import time
import os
import pytest
from stream_v3.frame_receiver import FrameReceiver,FrameTiming,error_code,relay_args


def test_continuous_reader_reuses_process_stamps_arrival_and_reaps():
    if not shutil.which('ffmpeg'):pytest.skip('ffmpeg unavailable')
    reader=FrameReceiver()
    args=['ffmpeg','-hide_banner','-nostdin','-loglevel','info','-re','-f','lavfi','-i','color=c=blue:s=160x90:r=5','-t','4',
          '-vf',"select='isnan(prev_selected_t)+gte(t-prev_selected_t,1)',showinfo=checksum=0",'-vsync','0','-threads','1','-c:v','ppm','-f','image2pipe','pipe:1']
    reader.start(args)
    try:
        frames=[reader.take(deadline=time.monotonic()+3) for _ in range(3)]
        pid=reader.process.pid
        assert reader.process.pid==pid and frames[2].timing_valid
        assert [x.sequence for x in frames]==[0,1,2]
        assert 1.5<frames[2].received_monotonic-frames[0].received_monotonic<3
        assert abs(frames[-1].received_epoch-time.time())<1
    finally:reader.close()
    assert reader.process.poll() is not None
    with pytest.raises(ProcessLookupError):os.kill(pid,0)


@pytest.mark.parametrize('message,code',[(b'TLS handshake failed','capture_tls_failed'),(b'Connection refused','capture_connect_failed'),(b'Invalid data found','capture_decode_failed'),(b'No one is publishing','capture_source_unavailable')])
def test_fixed_capture_reason_codes(message,code):assert error_code(message)==code


@pytest.mark.parametrize('text',[b'Could not find codec parameters for stream 0',b'Output file #0 does not contain any stream'])
def test_input_probe_failure_has_its_own_category(text):
    assert error_code(text)=='capture_probe_failed'


def test_session_cumulative_drift_rejects_slow_decoder_and_wall_steps():
    normal=FrameTiming();slow=FrameTiming();step=FrameTiming()
    for i in range(61):
        good,_=normal.observe(i,100+i,1000+i)
        bad,reason=slow.observe(i,100+i*1.03,1000+i*1.03)
        stepped,wall_reason=step.observe(i,100+i,1000+i+(.03*i))
    assert good
    assert not bad and reason=='receiver_cumulative_timing_drift'
    assert not stepped and wall_reason=='observer_wall_clock_step'


def test_initial_probe_burst_warms_up_once_without_hiding_later_drift():
    timing=FrameTiming()
    for i in range(5):timing.observe(i,100+i*.01,1000+i*.01)
    for i in range(5,10):valid,reason=timing.observe(i,100+i-4,1000+i-4)
    assert valid and reason=='consistent'
    origin=timing.origin
    for i in range(10,70):valid,reason=timing.observe(i,100+i-4+(i-9)*.03,1000+i-4+(i-9)*.03)
    assert timing.origin==origin and not valid and reason=='receiver_cumulative_timing_drift'


def test_probe_budget_covers_the_production_gop():
    args=relay_args({'source_url':'rtmp://127.0.0.1/test'})
    assert args[args.index('-analyzeduration')+1]=='5000000'
    assert args[args.index('-probesize')+1]=='5000000'


def test_pending_reconnect_can_be_cancelled_without_a_child(monkeypatch):
    import stream_v3.frame_receiver as module
    reader=FrameReceiver();reader.starts=1
    monkeypatch.setattr(module.random,'uniform',lambda a,b:2.)
    launched=[]
    monkeypatch.setattr(module.subprocess,'Popen',lambda *_a,**_k:launched.append(True))
    reader.start(['unused']);assert reader.running
    reader.close()
    assert not reader.running and not launched


def test_recovered_decode_warning_does_not_mask_eof():
    import sys
    code="import sys,time;sys.stderr.write('Error while decoding\\n');sys.stderr.flush();sys.stdout.buffer.write(b'P6\\n8 4\\n255\\n'+b'x'*96);sys.stdout.flush();sys.stderr.write('n: 0 pts_time: 0\\n');sys.stderr.flush();time.sleep(.1)"
    reader=FrameReceiver();reader.start([sys.executable,'-c',code])
    try:
        reader.take(deadline=time.monotonic()+2)
        reader.thread.join(timeout=2)
        assert reader.error=='capture_stream_ended'
    finally:reader.close()
