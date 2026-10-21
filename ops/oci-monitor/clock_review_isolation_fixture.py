#!/usr/bin/env python3
"""Use the backup argument builder and matching FFmpeg to isolate stopped and blocked monitor reception."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
from types import SimpleNamespace
from urllib.request import urlopen

from stream_v3.fallback_monitor import add_monitor_output
from stream_v3.frame_receiver import ObservedTransportBuffer,FrameReceiver
from stream_v3.verified_clock import VerifiedClockAnalyzer,VerifiedClockConfig
from stream_v3.oci_monitor_probe import MonitorWorker


def main():
    root=Path('/ram/fixture');root.mkdir()
    stop=threading.Event();children=[];logs=[];samples=[];stages=[];receiver=None
    def spawn(args,name):
        log=(root/(name+'.log')).open('wb');logs.append(log)
        child=subprocess.Popen(args,stdout=log,stderr=log,start_new_session=True);children.append(child);return child
    conf={'logLevel':'warn','rtsp':False,'hls':False,'webrtc':False,'srt':False,'moq':False,
          'rtmp':True,'rtmpAddress':'127.0.0.1:11935','api':True,'apiAddress':'127.0.0.1:19997',
          'authInternalUsers':[{'user':'any','ips':['127.0.0.1/32'],
                                'permissions':[{'action':'publish'},{'action':'read'},{'action':'api'}]}],
          'paths':{'all_others':{'source':'publisher'}}}
    path=root/'server.json';path.write_text(json.dumps(conf))
    def observe():
        with urlopen('http://127.0.0.1:19997/v3/paths/list',timeout=2) as r:d=json.load(r)
        row=next(x for x in d['items'] if x['name']=='direct' and x['ready'])
        value=row.get('inboundBytes',row.get('bytesReceived'))
        samples.append({'elapsed':round(time.monotonic()-started,3),'primary_bytes':value,
                        'buffer_bytes':receiver.size if receiver else 0})
        return value
    def phase(name,seconds=8):
        before=observe()
        for _ in range(seconds):
            time.sleep(1)
            if sender.poll() is not None or server.poll() is not None:raise RuntimeError('primary_process_exited')
            observe()
        delta=samples[-1]['primary_bytes']-before
        if delta<=0:raise RuntimeError('primary_stopped:'+name)
        stages.append({'stage':name,'seconds':seconds,'primary_delta':delta})
        print(json.dumps({'event':'phase_complete',**stages[-1]}),flush=True)
    started=time.monotonic()
    try:
        server=spawn(['/opt/mediamtx',str(path)],'primary')
        for _ in range(40):
            try:
                with urlopen('http://127.0.0.1:19997/v3/info',timeout=1):break
            except Exception:time.sleep(.1)
        else:raise RuntimeError('primary_receiver_start_failed')
        clock=root/'clock.txt';clock.write_text('12:00:00');utc=root/'utc.txt';utc.write_text('03:00:00')
        def tick():
            while not stop.is_set():
                clock.write_text(time.strftime('%H:%M:%S',time.gmtime(time.time()+9*3600)));utc.write_text(time.strftime('%H:%M:%S',time.gmtime()));stop.wait(.2)
        threading.Thread(target=tick,daemon=True).start()
        spec=importlib.util.spec_from_file_location('actual_backup_builder','/fixture/ffmpeg_args.py')
        builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)
        cfg=SimpleNamespace(test_mode=False,use_fifo_recovery=False,rtmp_url='rtmp://127.0.0.1:11935/direct',
                            video_bitrate='3400k',video_maxrate='3400k',video_bufsize='6800k',audio_bitrate='128k',
                            output_size='1920x1080',draw_mouse=0,frame_rate=5,video_size='1920x1080',
                            audio_queue_size=32,audio_filter='anull',video_encoder='libx264',video_preset='ultrafast',audio_sample_rate=48000)
        values=builder.build_ffmpeg_args(cfg,x11_input=':fixture',pulse_source='fixture',encoder_profile={})
        i=values.index('-thread_queue_size');j=values.index('-filter_complex')
        # Replace only the physical input devices; retain actual encoder/output options.
        values[i:j]=['-re','-f','lavfi','-i','testsrc2=s=1920x1080:r=5','-f','lavfi','-i','sine=frequency=440:sample_rate=48000']
        i=values.index('-filter_complex')+1
        values[i]+=f',drawbox=x=30:y=1040:w=100:h=35:color=black:t=fill,drawtext=fontfile=/fixture/font.ttf:textfile={clock}:reload=1:x=40:y=1047:fontsize=14:fontcolor=white:box=1:boxcolor=black'
        values[i]+=f',drawbox=x=150:y=1040:w=100:h=35:color=black:t=fill,drawtext=fontfile=/fixture/font.ttf:textfile={utc}:reload=1:x=160:y=1047:fontsize=14:fontcolor=white:box=1:boxcolor=black'
        values=[*values[:-3],'-threads:v','1','-flvflags','aac_seq_header_detect',*values[-3:]]
        values=add_monitor_output(values,'udp://127.0.0.1:8767?pkt_size=1316&connect=0&buffer_size=65536')
        sender=spawn(values,'sender')
        for _ in range(80):
            try:observe();break
            except Exception:
                if sender.poll() is not None:raise RuntimeError('sender_start_failed:'+(root/'sender.log').read_text()[-2500:])
                time.sleep(.1)
        else:raise RuntimeError('primary_input_unready')
        sender_pid=sender.pid
        phase('monitor_absent')
        receiver=ObservedTransportBuffer();receiver.start(host='127.0.0.1');phase('monitor_running')
        key=root/'key';key.write_bytes(b'x'*32)
        worker=MonitorWorker({'source_identity':'fixture','source_url':'rtmp://127.0.0.1:11935/direct','ca_file':'/no-ca',
                              'key_file':str(key),'roi':[.019,.968,.04,.022]},root/'worker')
        worker.fallback=receiver
        from stream_v3.viewer_clock_worker import EvidenceStore
        worker.analyzers['youtube_fallback']=VerifiedClockAnalyzer('youtube_fallback',VerifiedClockConfig())
        worker.readers['youtube_fallback']=FrameReceiver()
        worker.evidence=EvidenceStore(root/'worker/evidence',max_bytes=8*1024*1024,platforms=('oci_relay','youtube_fallback'))
        first=worker.run_cycle()
        if not first['targets']['youtube_fallback']['capture_ok']:raise RuntimeError('fallback_decode_failed:'+json.dumps(first))
        print(json.dumps({'event':'initial_capture','targets':first['targets']}),flush=True)
        phase('continuous_both_readers',10)
        quality=[]
        for _ in range(4):
            phase('continuous_clock_comparison',5)
            row=worker.run_cycle();quality.append(row)
        for target in ('oci_relay','youtube_fallback'):
            if sum(x['targets'][target]['clock_state']=='ADVANCING_CONFIRMED' for x in quality)<2:
                raise RuntimeError('paired_clock_quality_failed:'+json.dumps([x['targets'][target] for x in quality]))
        reader_pids={k:r.process.pid for k,r in worker.readers.items()}
        # Leave the UDP socket open but stop reading it. Kernel receive queue fills.
        receiver.stop.set();receiver.thread.join(timeout=1)
        phase('monitor_socket_unread')
        receiver.close();receiver=None
        for reader in worker.readers.values():reader.close()
        phase('monitor_stopped')
        receiver=ObservedTransportBuffer();receiver.start(host='127.0.0.1');phase('monitor_restarted')
        worker.fallback=receiver
        warmup=worker.run_cycle()
        phase('continuous_recovered',10)
        second=worker.run_cycle()
        if not second['targets']['youtube_fallback']['capture_ok']:raise RuntimeError('fallback_not_recovered')
        # Verify that primary FLV carries decodable video and audio after all failures.
        result=subprocess.run(['ffmpeg','-nostdin','-v','error','-threads','1','-rw_timeout','5000000','-i','rtmp://127.0.0.1:11935/direct',
                               '-t','1','-map','0:v:0','-map','0:a:0','-threads','1','-f','null','-'],capture_output=True,timeout=15)
        if result.returncode:raise RuntimeError('primary_decode_failed:'+result.stderr.decode()[-1000:])
        clock_row=second['targets']['youtube_fallback']
        if clock_row['clock_state']!='ADVANCING_CONFIRMED':
            print(json.dumps({'event':'clock_verification_failed','observation':clock_row}),flush=True)
            raise RuntimeError('fallback_clock_not_confirmed')
        print(json.dumps({'result':'PASS','scope':'isolated_actual_backup_builder_ffmpeg_5_1_9',
                          'stages':stages,'seconds':round(time.monotonic()-started,3),'samples':samples,
                          'sender_pid_preserved':sender.pid==sender_pid and sender.poll() is None,
                          'primary_video_audio_decode':True,'paired_ocr_each_sample':True,'quality_states':{target:[x['targets'][target]['clock_state'] for x in quality] for target in ('oci_relay','youtube_fallback')},'continuous_reader_pids':reader_pids,
                          'fallback_clock':{k:clock_row.get(k) for k in ('clock_state','ocr_previous','ocr_current','ocr_confidence_previous','ocr_confidence_current','clock_delta_seconds')},
                          'buffer_limit_bytes':receiver.max_bytes,'buffer_bytes':receiver.size}),flush=True)
    finally:
        stop.set()
        if receiver:receiver.close()
        if 'worker' in locals():
            for reader in worker.readers.values():reader.close()
        for child in children:
            try:os.killpg(child.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        for child in children:child.wait(timeout=5)
        for log in logs:log.close()
        print(json.dumps({'cleanup_all_children_reaped':all(c.poll() is not None for c in children)}),flush=True)


if __name__=='__main__':main()
