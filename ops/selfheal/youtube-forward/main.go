// youtube-forward copies H264/AAC to a fresh RTMPS connection with a common
// timestamp origin. Encoding and the source publisher remain independent.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
)

type timeline struct {
	started              bool
	origin               time.Duration
	lastVideo, lastAudio time.Duration
	videoSeen, audioSeen bool
}

func (t *timeline) video(pts, dts time.Duration, idr bool) (time.Duration, time.Duration, bool, error) {
	if !t.started {
		if !idr {
			return 0, 0, false, nil
		}
		t.started, t.origin = true, dts
	}
	p, d := pts-t.origin, dts-t.origin
	if p < 0 || d < 0 || (t.videoSeen && d < t.lastVideo) {
		return 0, 0, false, errors.New("video_timeline_regressed")
	}
	t.videoSeen, t.lastVideo = true, d
	return p, d, true, nil
}

func (t *timeline) audio(pts time.Duration) (time.Duration, bool, error) {
	if !t.started || pts < t.origin {
		return 0, false, nil
	}
	p := pts - t.origin
	if t.audioSeen && p < t.lastAudio {
		return 0, false, errors.New("audio_timeline_regressed")
	}
	t.audioSeen, t.lastAudio = true, p
	return p, true, nil
}

type observation struct {
	Schema                 string  `json:"schema"`
	PID                    int     `json:"pid"`
	BootID                 string  `json:"boot_id"`
	UpdatedAt              string  `json:"updated_at"`
	State                  string  `json:"state"`
	Session                uint64  `json:"session"`
	DestinationFingerprint string  `json:"destination_fingerprint"`
	OutputBytes            uint64  `json:"output_bytes"`
	VideoPackets           uint64  `json:"video_packets"`
	AudioPackets           uint64  `json:"audio_packets"`
	OriginSeconds          float64 `json:"origin_seconds"`
	LastVideoDTS           float64 `json:"last_video_dts_seconds"`
	LastAudioPTS           float64 `json:"last_audio_pts_seconds"`
	LastProgressAt         string  `json:"last_progress_at"`
	ErrorClass             string  `json:"error_class"`
}

type reporter struct {
	mu    sync.Mutex
	value observation
	path  string
}

func (r *reporter) update(fn func(*observation)) { r.mu.Lock(); defer r.mu.Unlock(); fn(&r.value) }
func (r *reporter) save() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.value
	v.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err = os.WriteFile(r.path+".tmp", b, 0644); err != nil {
		return err
	}
	return os.Rename(r.path+".tmp", r.path)
}

func errorClass(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, io.EOF) {
		return "EOF"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return fmt.Sprintf("%T", err)
}

func copySession(ctx context.Context, input net.Conn, dest *url.URL, report *reporter) error {
	defer input.Close()
	input.SetDeadline(time.Now().Add(10 * time.Second))
	sc := &gortmplib.ServerConn{RW: input}
	if err := sc.Initialize(); err != nil {
		return err
	}
	if err := sc.AcceptConn(); err != nil {
		return err
	}
	if !sc.Publish {
		return errors.New("publisher_required")
	}
	r := &gortmplib.Reader{Conn: sc}
	if err := r.Initialize(); err != nil {
		return err
	}
	tracks := r.Tracks()
	var video, audio *gortmplib.Track
	for _, t := range tracks {
		switch t.Codec.(type) {
		case *codecs.H264:
			if video != nil {
				return errors.New("duplicate_video")
			}
			video = t
		case *codecs.MPEG4Audio:
			if audio != nil {
				return errors.New("duplicate_audio")
			}
			audio = t
		default:
			return errors.New("unsupported_codec")
		}
	}
	if video == nil || audio == nil || len(tracks) != 2 {
		return errors.New("h264_aac_required")
	}
	cc := &gortmplib.Client{URL: dest, Publish: true}
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := cc.Initialize(connectCtx)
	cancel()
	if err != nil {
		return err
	}
	defer cc.Close()
	w := &gortmplib.Writer{Conn: cc, Tracks: tracks}
	if err = w.Initialize(); err != nil {
		return err
	}
	cc.NetConn().SetReadDeadline(time.Time{})
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			input.Close()
			cc.Close()
		case <-stop:
		}
	}()
	go func() {
		select {
		case <-stop:
			return
		default:
		}
		_ = w.Wait()
		input.Close()
	}()
	input.SetDeadline(time.Time{})
	tm := timeline{}
	var writeErr error
	report.update(func(o *observation) {
		o.State = "WAIT_KEYFRAME"
		o.OutputBytes = 0
		o.VideoPackets = 0
		o.AudioPackets = 0
		o.LastProgressAt = ""
		o.ErrorClass = ""
	})
	r.OnDataH264(video, func(pts, dts time.Duration, au [][]byte) {
		if writeErr != nil {
			return
		}
		idr := false
		for _, n := range au {
			if len(n) > 0 && n[0]&31 == 5 {
				idr = true
			}
		}
		p, d, emit, e := tm.video(pts, dts, idr)
		if e != nil {
			writeErr = e
			return
		}
		if !emit {
			return
		}
		cc.NetConn().SetWriteDeadline(time.Now().Add(8 * time.Second))
		if e = w.WriteH264(video, p, d, au); e != nil {
			writeErr = e
			return
		}
		report.update(func(o *observation) {
			o.State = "FLOWING"
			o.VideoPackets++
			o.OutputBytes = cc.BytesSent()
			o.OriginSeconds = tm.origin.Seconds()
			o.LastVideoDTS = d.Seconds()
			o.LastProgressAt = time.Now().UTC().Format(time.RFC3339Nano)
		})
	})
	r.OnDataMPEG4Audio(audio, func(pts time.Duration, au []byte) {
		if writeErr != nil {
			return
		}
		p, emit, e := tm.audio(pts)
		if e != nil {
			writeErr = e
			return
		}
		if !emit {
			return
		}
		cc.NetConn().SetWriteDeadline(time.Now().Add(8 * time.Second))
		if e = w.WriteMPEG4Audio(audio, p, au); e != nil {
			writeErr = e
			return
		}
		report.update(func(o *observation) {
			o.AudioPackets++
			o.OutputBytes = cc.BytesSent()
			o.LastAudioPTS = p.Seconds()
			o.LastProgressAt = time.Now().UTC().Format(time.RFC3339Nano)
		})
	})
	for {
		input.SetReadDeadline(time.Now().Add(10 * time.Second))
		if err = r.Read(); err != nil {
			return err
		}
		if writeErr != nil {
			return writeErr
		}
	}
}

func run() error {
	listen := flag.String("listen", "127.0.0.1:19350", "loopback publisher input")
	config := flag.String("config", "", "JSON containing destination")
	state := flag.String("state", "", "atomic observation path")
	local := flag.Bool("allow-local-output", false, "allow a loopback lab receiver")
	flag.Parse()
	addr, e := net.ResolveTCPAddr("tcp", *listen)
	if e != nil || addr.IP == nil || !addr.IP.IsLoopback() {
		return errors.New("loopback_listener_required")
	}
	b, e := os.ReadFile(*config)
	if e != nil {
		return e
	}
	var c struct {
		Destination string `json:"destination"`
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return e
	}
	dest, e := url.Parse(c.Destination)
	if e != nil {
		return errors.New("invalid_destination")
	}
	if !(dest.Scheme == "rtmps" && dest.Hostname() == "a.rtmps.youtube.com") && !(*local && dest.Scheme == "rtmp" && net.ParseIP(dest.Hostname()) != nil && net.ParseIP(dest.Hostname()).IsLoopback()) {
		return errors.New("unexpected_destination")
	}
	if *state == "" {
		return errors.New("state_path_required")
	}
	if e = os.MkdirAll(filepath.Dir(*state), 0755); e != nil {
		return e
	}
	boot, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	fp := sha256.Sum256([]byte(c.Destination))
	r := &reporter{path: *state, value: observation{Schema: "stream_v3.youtube_forward.v1", PID: os.Getpid(), BootID: string(boot[:len(boot)-1]), State: "LISTENING", DestinationFingerprint: hex.EncodeToString(fp[:])}}
	if e = r.save(); e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	ln, e := net.ListenTCP("tcp", addr)
	if e != nil {
		return e
	}
	defer ln.Close()
	go func() { <-ctx.Done(); ln.Close() }()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.save(); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		r.update(func(o *observation) { o.Session++; o.State = "CONNECTING" })
		err = copySession(ctx, conn, dest, r)
		r.update(func(o *observation) { o.State = "ERROR"; o.ErrorClass = errorClass(err) })
		if e = r.save(); e != nil {
			return e
		}
		// Retry is driven by the single existing MediaMTX destination. No second writer.
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, errorClass(err))
		os.Exit(1)
	}
}
