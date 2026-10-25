package main

import (
	"context"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
)

func TestCommonOriginAndKeyframe(t *testing.T) {
	var r timeline
	if _, ok, _ := r.audio(20 * time.Hour); ok {
		t.Fatal("audio before first keyframe")
	}
	if _, _, ok, _ := r.video(20*time.Hour, 20*time.Hour, false); ok {
		t.Fatal("non-IDR before keyframe")
	}
	p, d, ok, e := r.video(20*time.Hour+80*time.Millisecond, 20*time.Hour, true)
	if e != nil || !ok || p != 80*time.Millisecond || d != 0 {
		t.Fatal(p, d, ok, e)
	}
	if _, ok, _ := r.audio(20*time.Hour - time.Millisecond); ok {
		t.Fatal("pre-keyframe audio")
	}
	a, ok, e := r.audio(20*time.Hour + 21*time.Millisecond)
	if e != nil || !ok || a != 21*time.Millisecond {
		t.Fatal(a, ok, e)
	}
	p, d, ok, e = r.video(20*time.Hour+120*time.Millisecond, 20*time.Hour+40*time.Millisecond, false)
	if e != nil || !ok || p-d != 80*time.Millisecond {
		t.Fatal("composition offset changed", p, d, e)
	}
}

func TestRegressionFailsAndConnectionResetsOrigin(t *testing.T) {
	var r timeline
	r.video(10*time.Hour, 10*time.Hour, true)
	r.video(10*time.Hour+time.Second, 10*time.Hour+time.Second, false)
	if _, _, _, e := r.video(10*time.Hour, 10*time.Hour, false); e == nil {
		t.Fatal("video regression accepted")
	}
	r.audio(10*time.Hour + time.Second)
	if _, _, e := r.audio(10 * time.Hour); e == nil {
		t.Fatal("audio regression accepted")
	}
	r = timeline{}
	_, d, ok, e := r.video(30*time.Hour, 30*time.Hour, true)
	if e != nil || !ok || d != 0 {
		t.Fatal("new connection retained origin")
	}
}

func TestRealRTMPConnectionRebasesBothTracks(t *testing.T) {
	// Both ends use real RTMP handshakes and readers/writers over loopback.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	receiver, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	inlet, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inlet.Close()
	result := make(chan error, 1)
	got := make(chan [3]time.Duration, 1)
	go func() {
		conn, e := receiver.Accept()
		if e != nil {
			result <- e
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(8 * time.Second))
		sc := &gortmplib.ServerConn{RW: conn}
		if e = sc.Initialize(); e != nil {
			result <- e
			return
		}
		if e = sc.AcceptConn(); e != nil {
			result <- e
			return
		}
		r := &gortmplib.Reader{Conn: sc}
		if e = r.Initialize(); e != nil {
			result <- e
			return
		}
		var vpts, vdts, apts time.Duration
		var haveVideo, haveAudio bool
		for _, track := range r.Tracks() {
			switch track.Codec.(type) {
			case *codecs.H264:
				r.OnDataH264(track, func(p, d time.Duration, au [][]byte) {
					if !haveVideo {
						vpts, vdts = p, d
						haveVideo = true
					}
				})
			case *codecs.MPEG4Audio:
				r.OnDataMPEG4Audio(track, func(p time.Duration, au []byte) {
					if !haveAudio {
						apts = p
						haveAudio = true
					}
				})
			}
		}
		for !haveVideo || !haveAudio {
			if e = r.Read(); e != nil {
				result <- e
				return
			}
		}
		got <- [3]time.Duration{vpts, vdts, apts}
	}()
	go func() {
		conn, e := inlet.Accept()
		if e != nil {
			result <- e
			return
		}
		d, _ := url.Parse("rtmp://" + receiver.Addr().String() + "/live/test")
		report := &reporter{path: filepath.Join(t.TempDir(), "state.json")}
		_ = copySession(ctx, conn, d, report)
	}()
	u, _ := url.Parse("rtmp://" + inlet.Addr().String() + "/live/test")
	c := &gortmplib.Client{URL: u, Publish: true}
	if err = c.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	v := &gortmplib.Track{Codec: &codecs.H264{SPS: []byte{0x67, 0x42, 0xc0, 0x28, 0xd9, 0x00, 0x78, 0x02, 0x27, 0xe5, 0x84, 0x00, 0x00, 0x03, 0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xf0, 0x3c, 0x60, 0xc9, 0x20}, PPS: []byte{0x68, 0xce, 0x3c, 0x80}}}
	a := &gortmplib.Track{Codec: &codecs.MPEG4Audio{Config: &mpeg4audio.AudioSpecificConfig{Type: 2, SampleRate: 48000, ChannelCount: 2}}}
	w := &gortmplib.Writer{Conn: c, Tracks: []*gortmplib.Track{v, a}}
	if err = w.Initialize(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 125; i++ {
		stamp := 20*time.Hour + time.Duration(i)*20*time.Millisecond
		if i%5 == 0 {
			nalu := []byte{0x41, 0x88, 0x84}
			if i%50 == 0 {
				nalu[0] = 0x65
			}
			if err = w.WriteH264(v, stamp, stamp, [][]byte{nalu}); err != nil {
				t.Fatal(err)
			}
		}
		if err = w.WriteMPEG4Audio(a, stamp+21*time.Millisecond, []byte{0x21, 0x10, 0x04, 0x60}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case e := <-result:
		t.Fatal(e)
	case x := <-got:
		if x[0] != 0 || x[1] != 0 || x[2] != 21*time.Millisecond {
			t.Fatal("wire timestamps", x)
		}
	case <-ctx.Done():
		t.Fatal("RTMP bridge timed out")
	}
}
