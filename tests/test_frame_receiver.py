import shutil
import time
import os
import pytest
from stream_v3.frame_receiver import FrameReceiver,error_code


def test_continuous_reader_reuses_process_stamps_arrival_and_reaps():
    if not shutil.which('ffmpeg'):pytest.skip('ffmpeg unavailable')
    reader=FrameReceiver()
    args=['ffmpeg','-hide_banner','-nostdin','-loglevel','info','-re','-f','lavfi','-i','color=c=blue:s=160x90:r=5','-t','4',
          '-vf',"select='isnan(prev_selected_t)+gte(t-prev_selected_t,1)',showinfo=checksum=0",'-vsync','0','-threads','1','-c:v','ppm','-f','image2pipe','pipe:1']
    reader.start(args);pid=reader.process.pid
    try:
        frames=[reader.take(deadline=time.monotonic()+3) for _ in range(3)]
        assert reader.process.pid==pid and frames[2].timing_valid
        assert [x.sequence for x in frames]==[0,1,2]
        assert 1.5<frames[2].received_monotonic-frames[0].received_monotonic<3
        assert abs(frames[-1].received_epoch-time.time())<1
    finally:reader.close()
    assert reader.process.poll() is not None
    with pytest.raises(ProcessLookupError):os.kill(pid,0)


@pytest.mark.parametrize('message,code',[(b'TLS handshake failed','capture_tls_failed'),(b'Connection refused','capture_connect_failed'),(b'Invalid data found','capture_decode_failed'),(b'No one is publishing','capture_source_unavailable')])
def test_fixed_capture_reason_codes(message,code):assert error_code(message)==code
