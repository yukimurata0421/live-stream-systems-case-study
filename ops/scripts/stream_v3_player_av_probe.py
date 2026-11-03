#!/usr/bin/env python3
"""Bounded anonymous public-player observation; never a recovery/control input."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import math
from pathlib import Path
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlsplit

MIN_CONTINUOUS_SECONDS = 60.0
CONTENT_CLOCK_MAX_AGE_SECONDS = 60.0

SAMPLE = r"""async () => {
 const v = document.querySelector('video');
 if (!v) return {present:false};
 let audioError = null;
 try {
   if (!window.__av || window.__av.video !== v) {
     const ctx = new AudioContext(); const node = ctx.createMediaElementSource(v);
     const analyser = ctx.createAnalyser(); analyser.fftSize = 2048;
     node.connect(analyser); analyser.connect(ctx.destination);
     window.__av = {video:v,ctx,analyser};
   }
   await window.__av.ctx.resume();
 } catch(e) { audioError = e.name; }
 let rms = null;
 if (window.__av && window.__av.video === v && !audioError) {
   const a = new Float32Array(2048); window.__av.analyser.getFloatTimeDomainData(a);
   rms = Math.sqrt(a.reduce((s,x) => s+x*x,0)/a.length);
 }
 const q = v.getVideoPlaybackQuality();
 const player = document.querySelector('#movie_player');
 let videoId = null;
 try { videoId = player.getVideoData().video_id; } catch(e) {}
 const seekableEnd = v.seekable.length ? v.seekable.end(v.seekable.length-1) : null;
 return {present:true, media_time:v.currentTime, paused:v.paused, ended:v.ended,
   ready:v.readyState, error:v.error ? v.error.code : null,
   frames:q.totalVideoFrames-q.droppedVideoFrames, dropped:q.droppedVideoFrames, width:v.videoWidth,
   height:v.videoHeight, audio_bytes:v.webkitAudioDecodedByteCount || 0,
   rms, audio_error:audioError, muted:v.muted,
   seekable_end:seekableEnd,
   seekable_delay_sec:seekableEnd === null ? null : seekableEnd-v.currentTime,
   ad:!!document.querySelector('.ad-showing'), video_id:videoId};
}"""


def failure_code(value):
    return value if isinstance(value, str) and re.fullmatch(r"net::[A-Z0-9_]{1,80}", value) else "unclassified"


def diagnostics(report):
    """Separate viewer evidence from browser/provider access failures."""
    if report.get("error"):
        return "probe_execution_incomplete"
    if report.get("av_status") == "PASS" and report.get("displayed_clock", {}).get("status") != "PASS" and report.get("verify_displayed_clock"):
        return "displayed_clock_unconfirmed"
    if report.get("status") == "PASS":
        return "continuous_av_observed"
    rejected = any(row.get("kind") == "media" and row.get("status") in (401, 403, 429)
                   for row in report.get("network", []))
    return "media_access_rejected" if rejected else "continuous_av_unconfirmed"


def embed_html(video_id, origin):
    """A real local origin for the official embed, with no credential headers."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
        raise ValueError("invalid_video_id")
    if not re.fullmatch(r"http://127\.0\.0\.1:[0-9]{1,5}", origin):
        raise ValueError("invalid_embed_origin")
    return ('<!doctype html><meta name="referrer" content="strict-origin-when-cross-origin">'
            '<style>body{margin:0}iframe{border:0;width:100vw;height:100vh}</style>'
            '<iframe id="player" width="960" height="540" allow="autoplay; fullscreen" '
            'referrerpolicy="strict-origin-when-cross-origin" '
            f'src="https://www.youtube.com/embed/{video_id}?enablejsapi=1&amp;autoplay=1'
            f'&amp;playsinline=1&amp;origin={origin}"></iframe>'
            '<script>function onYouTubeIframeAPIReady(){window.player=new YT.Player("player",'
            '{events:{onReady:()=>{window.playerReady=true;}}});}</script>'
            '<script src="https://www.youtube.com/iframe_api"></script>').encode()


async def embed_server(video_id):
    """Serve only generated player HTML; owned by this bounded observation."""
    async def handle(reader, writer):
        try:
            await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
            body = embed_html(video_id, origin)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                         b"Cache-Control: no-store\r\nConnection: close\r\nContent-Length: "
                         + str(len(body)).encode() + b"\r\n\r\n" + body)
            await asyncio.wait_for(writer.drain(), timeout=2)
        except (TimeoutError, ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_server(handle, "127.0.0.1", 0, limit=8192)
    origin = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    return server, origin


def assess(samples, video_id, minimum=MIN_CONTINUOUS_SECONDS):
    """Require one continuous, target-bound interval; no isolated frames as PASS."""
    start = None
    last = None
    audible = 0
    longest = 0.0
    span = 0.0
    for s in samples:
        valid = (s.get("present") and s.get("video_id") == video_id and not s.get("ad")
                 and s.get("ready", 0) >= 3 and not s.get("paused") and not s.get("ended")
                 and not s.get("error") and s.get("width", 0) > 0 and s.get("height", 0) > 0)
        progressing = (last is not None and valid and 0 < s["elapsed"] - last["elapsed"] <= 5
                       and s["frames"] > last.get("frames", 0)
                       and s["media_time"] > last.get("media_time", 0)
                       and s["audio_bytes"] > last.get("audio_bytes", 0))
        if not valid:
            start = last = None
            audible = 0
            span = 0.0
            continue
        if not progressing:
            start = s["elapsed"]
            audible = 0
        if s.get("rms") is not None and s["rms"] > 0.00001 and not s.get("audio_error") and not s.get("muted"):
            audible += 1
        span = s["elapsed"] - start
        longest = max(longest, span)
        last = s
    if span >= minimum and audible >= 3:
        return {"status": "PASS", "target_state": "OK", "verified_seconds": span,
                "video_progress_verified": True, "audio_decoded_verified": True,
                "interval_start_elapsed": start, "interval_end_elapsed": last["elapsed"],
                "reason": "target_video_and_decoded_audio_advanced"}
    return {"status": "UNKNOWN", "target_state": "UNKNOWN", "verified_seconds": longest,
            "video_progress_verified": False, "audio_decoded_verified": False,
            "reason": "public_player_av_not_qualified"}


def assess_displayed_clock(rows, av):
    """Require fresh, advancing, paired clocks overlapping the same AV interval."""
    first = last = None
    ages = []
    for row in rows:
        age = row.get("display_age_seconds")
        stamp = row.get("elapsed")
        valid = (row.get("ocr_verified") is True and row.get("clock_pair_consistent") is True
                 and row.get("observer_timing_valid") is True
                 and row.get("clock_state") in ("WARMUP", "ADVANCING_CONFIRMED")
                 and type(age) in (int, float) and math.isfinite(age)
                 and -1 <= age and age+1 <= CONTENT_CLOCK_MAX_AGE_SECONDS
                 and type(stamp) in (int, float) and math.isfinite(stamp))
        if not valid:
            first = last = None
            ages = []
            continue
        if last is None or not 0 < stamp-last <= 15 or row["clock_state"] == "WARMUP":
            first = stamp
            ages = []
        last = stamp
        ages.append(max(0., age+1))
    span = 0.
    current_age = None
    if last is not None and av.get("status") == "PASS":
        current_age = ages[-1] + max(0., av["interval_end_elapsed"]-last)
        span = max(0., min(last, av["interval_end_elapsed"])
                   - max(first, av["interval_start_elapsed"]))
        if av["interval_end_elapsed"]-last > 15 or current_age > CONTENT_CLOCK_MAX_AGE_SECONDS:
            span = 0.
    return {"status": "PASS" if span >= MIN_CONTINUOUS_SECONDS else "UNKNOWN",
            "verified_seconds": span, "maximum_age_upper_seconds": max(ages) if ages else None,
            "current_age_upper_seconds": current_age,
            "scope": "displayed UTC clock, paired with JST; independent of ingest selection"}


class DisplayedClock:
    """Reuse the existing bounded OCR and paired-clock observation contract."""
    def __init__(self, args):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
        from stream_v3.viewer_clock_capture import ProcessRunner, TesseractOCR
        from stream_v3.verified_clock import VerifiedClockAnalyzer, VerifiedClockConfig
        from stream_core.common import clock_contract
        self.clock = clock_contract
        self.runner = ProcessRunner()
        self.ocr = TesseractOCR(self.runner, binary=args.tesseract, tessdata_dir=args.tessdata_dir)
        self.analyzer = VerifiedClockAnalyzer("youtube", VerifiedClockConfig())
        self.anchor = self.clock.capture()
        self.timing_fault = False
        self.ntp_synchronized = self.synchronized()

    def synchronized(self):
        try:
            result = self.runner.run(["/usr/bin/timedatectl", "show", "-p", "NTPSynchronized", "--value"],
                                     deadline=time.monotonic()+2, max_stdout=128)
            return result.returncode == 0 and result.stdout.strip() == b"yes"
        except Exception:
            return False

    def read(self, content, video_id, elapsed):
        from PIL import Image
        from stream_v3.viewer_clock import Capture, GrayImage
        if len(content) > 8*1024*1024:
            raise ValueError("player_screenshot_too_large")
        with Image.open(io.BytesIO(content)) as image:
            if image.size not in ((960, 540), (1920, 1080)):
                raise ValueError("player_screenshot_geometry_changed")
            image = image.convert("L").resize((1920, 1080))
            frame = GrayImage(image.width, image.height, image.tobytes())
        current = self.clock.capture()
        wall, mono = current["wall"], current["mono"]
        self.timing_fault |= abs((wall-self.anchor["wall"])-(mono-self.anchor["mono"])) > self.clock.STEP_LIMIT
        self.anchor = current
        timing_valid = self.ntp_synchronized and not self.timing_fault
        deadline = time.monotonic()+2
        row, _ = self.analyzer.analyze(Capture(frame, "youtube:"+video_id, wall, mono),
            lambda image: self.ocr.read(image, deadline=deadline), now=wall, timing_valid=timing_valid)
        row.update(elapsed=elapsed, observer_timing_valid=timing_valid)
        return row


async def observe(a):
    from playwright.async_api import async_playwright
    output = a.output
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema": "stream.player_av_observation.v1", "video_id": a.video_id,
              "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "scope": "anonymous public YouTube player; no ingest selection or failover proof",
              "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "requested_seconds": a.seconds, "required_continuous_seconds": MIN_CONTINUOUS_SECONDS,
              "verify_displayed_clock": a.verify_displayed_clock,
              "content_clock_max_age_seconds": CONTENT_CLOCK_MAX_AGE_SECONDS,
              "live_edge_verified": False,
              "surface": a.surface, "samples": [], "clock_samples": [], "error": None}
    responses = Counter()
    failures = Counter()
    server = None
    clock = None
    try:
        if a.verify_displayed_clock:
            clock = DisplayedClock(a)
        async with async_playwright() as p:
            browser = await p.chromium.launch(executable_path=a.chromium, headless=not a.headed,
                chromium_sandbox=not a.confined_no_sandbox, timeout=20000,
                args=["--disable-gpu", "--disable-dev-shm-usage", "--disable-background-networking",
                      "--autoplay-policy=no-user-gesture-required", "--disk-cache-size=0", "--media-cache-size=0"],
                ignore_default_args=["--mute-audio"])
            report["browser_version"] = browser.version
            try:
                page = await browser.new_page(
                    viewport={"width": 1280, "height": 720} if a.verify_displayed_clock else {"width": 960, "height": 540},
                    device_scale_factor=1.5 if a.verify_displayed_clock else 1)
                def response(row):
                    # Signed media URLs, headers and browser credentials are never retained.
                    kind = "media" if "videoplayback" in urlsplit(row.url).path else row.request.resource_type
                    responses[(kind, row.status)] += 1
                page.on("response", response)
                page.on("requestfailed", lambda row: failures.update([failure_code(row.failure)]))
                target = page
                if a.surface == "embed":
                    server, origin = await embed_server(a.video_id)
                    await page.goto(origin, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_function("window.playerReady === true", timeout=20000)
                    target = await page.locator("iframe").element_handle()
                    target = await target.content_frame()
                else:
                    await page.goto("https://www.youtube.com/watch?v=" + a.video_id,
                                    wait_until="domcontentloaded", timeout=30000)
                await target.wait_for_selector("video", timeout=20000)
                # This affects only the anonymous viewing browser.
                await target.evaluate("""async () => {const v=document.querySelector('video');
                    v.muted=false;v.volume=1;await v.play();} """)
                start = time.monotonic()
                next_clock = 0.
                while time.monotonic() - start < a.seconds:
                    sample = await target.evaluate(SAMPLE)
                    if a.surface == "embed":
                        info = await page.evaluate("""() => ({
                            video_id:new URL(window.player.getVideoUrl()).searchParams.get('v'),
                            player_time:window.player.getCurrentTime(),
                            live_duration:window.player.getDuration(),
                            player_state:window.player.getPlayerState()})""")
                        sample["inner_video_id"] = sample.get("video_id")
                        sample.update(info)
                        sample["api_duration_minus_position_seconds"] = info["live_duration"]-info["player_time"]
                    sample["elapsed"] = time.monotonic() - start
                    report["samples"].append(sample)
                    if clock is not None and sample["elapsed"] >= next_clock:
                        content = await page.screenshot(timeout=5000)
                        report["clock_samples"].append(clock.read(content, a.video_id, sample["elapsed"]))
                        if next_clock == 0:
                            (output / "first-clock.png").write_bytes(content)
                        next_clock = time.monotonic()-start+10
                    await asyncio.sleep(2)
                (output / "final.png").write_bytes(await page.screenshot(timeout=5000))
            finally:
                await browser.close()
    except Exception as e:
        # Do not store browser exception messages: they may contain signed media URLs.
        report["error"] = type(e).__name__
    finally:
        if server is not None:
            server.close()
            await server.wait_closed()
    av = assess(report["samples"], a.video_id)
    report.update(av)
    report["av_status"] = av["status"]
    report["displayed_clock"] = assess_displayed_clock(report["clock_samples"], av)
    if clock is not None:
        report["observer_ntp_synchronized"] = clock.ntp_synchronized and clock.synchronized()
        if not report["observer_ntp_synchronized"]:
            report["displayed_clock"]["status"] = "UNKNOWN"
        if report["displayed_clock"]["status"] != "PASS":
            report.update(status="UNKNOWN", target_state="UNKNOWN", reason="displayed_clock_unconfirmed")
    report["network"] = [{"kind": key[0], "status": key[1], "count": value} for key, value in sorted(responses.items())]
    report["request_failures"] = dict(failures)
    report["browser_mode"] = "headed" if a.headed else "headless"
    if report["error"]:
        report.update(status="UNKNOWN", target_state="UNKNOWN", reason="probe_execution_incomplete")
    report["diagnosis"] = diagnostics(report)
    report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    (output / "player.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k not in ("samples", "clock_samples")}))
    return 0 if report["status"] == "PASS" else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--video-id", required=True)
    p.add_argument("--chromium", required=True)
    p.add_argument("--seconds", type=int, default=75)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--headed", action="store_true", help="use a separately confined display server")
    p.add_argument("--surface", choices=("watch", "embed"), default="watch")
    p.add_argument("--verify-displayed-clock", action="store_true",
                   help="qualify current content with the existing paired JST/UTC OCR contract")
    p.add_argument("--tesseract", default="tesseract")
    p.add_argument("--tessdata-dir")
    p.add_argument("--confined-no-sandbox", action="store_true",
                   help="only for an independently confined, unprivileged one-shot process")
    a = p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", a.video_id) or not 65 <= a.seconds <= 120:
        p.error("valid video ID and 65..120 seconds required")
    if a.verify_displayed_clock and a.surface != "embed":
        p.error("displayed-clock qualification requires --surface embed")
    raise SystemExit(asyncio.run(observe(a)))


if __name__ == "__main__":
    main()
