#!/usr/bin/env python3
"""Bounded anonymous public-player observation; never a recovery/control input."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import re
import time
from collections import Counter
from urllib.parse import urlsplit


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
 return {present:true, media_time:v.currentTime, paused:v.paused, ended:v.ended,
   ready:v.readyState, error:v.error ? v.error.code : null,
   frames:q.totalVideoFrames-q.droppedVideoFrames, dropped:q.droppedVideoFrames, width:v.videoWidth,
   height:v.videoHeight, audio_bytes:v.webkitAudioDecodedByteCount || 0,
   rms, audio_error:audioError, muted:v.muted,
   ad:!!document.querySelector('.ad-showing'), video_id:videoId};
}"""


def failure_code(value):
    return value if isinstance(value, str) and re.fullmatch(r"net::[A-Z0-9_]{1,80}", value) else "unclassified"


def assess(samples, video_id, minimum=60.0):
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
                "reason": "target_video_and_decoded_audio_advanced"}
    return {"status": "UNKNOWN", "target_state": "UNKNOWN", "verified_seconds": longest,
            "video_progress_verified": False, "audio_decoded_verified": False,
            "reason": "public_player_av_not_qualified"}


async def observe(a):
    from playwright.async_api import async_playwright
    output = a.output
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema": "stream.player_av_observation.v1", "video_id": a.video_id,
              "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "scope": "anonymous public YouTube player; no ingest selection or failover proof",
              "samples": [], "error": None}
    responses = Counter()
    failures = Counter()
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(executable_path=a.chromium, headless=not a.headed,
                chromium_sandbox=not a.confined_no_sandbox, timeout=20000,
                args=["--disable-gpu", "--disable-dev-shm-usage", "--disable-background-networking",
                      "--autoplay-policy=no-user-gesture-required", "--disk-cache-size=0", "--media-cache-size=0"],
                ignore_default_args=["--mute-audio"])
            try:
                page = await browser.new_page(viewport={"width": 960, "height": 540})
                def response(row):
                    # Signed media URLs, headers and browser credentials are never retained.
                    kind = "media" if "videoplayback" in urlsplit(row.url).path else row.request.resource_type
                    responses[(kind, row.status)] += 1
                page.on("response", response)
                page.on("requestfailed", lambda row: failures.update([failure_code(row.failure)]))
                await page.goto("https://www.youtube.com/watch?v=" + a.video_id,
                                wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector("video", timeout=20000)
                # This affects only the anonymous viewing browser.
                await page.evaluate("""async () => {const v=document.querySelector('video');
                    v.muted=false;v.volume=1;await v.play();} """)
                start = time.monotonic()
                while time.monotonic() - start < a.seconds:
                    sample = await page.evaluate(SAMPLE)
                    sample["elapsed"] = time.monotonic() - start
                    report["samples"].append(sample)
                    await asyncio.sleep(2)
                await page.screenshot(path=str(output / "final.png"))
            finally:
                await browser.close()
    except Exception as e:
        # Do not store browser exception messages: they may contain signed media URLs.
        report["error"] = type(e).__name__
    report.update(assess(report["samples"], a.video_id))
    report["network"] = [{"kind": key[0], "status": key[1], "count": value} for key, value in sorted(responses.items())]
    report["request_failures"] = dict(failures)
    report["browser_mode"] = "headed" if a.headed else "headless"
    if report["error"]:
        report.update(status="UNKNOWN", target_state="UNKNOWN", reason="probe_execution_incomplete")
    (output / "player.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "samples"}))
    return 0 if report["status"] == "PASS" else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--video-id", required=True)
    p.add_argument("--chromium", required=True)
    p.add_argument("--seconds", type=int, default=75)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--headed", action="store_true", help="use a separately confined display server")
    p.add_argument("--confined-no-sandbox", action="store_true",
                   help="only for an independently confined, unprivileged one-shot process")
    a = p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", a.video_id) or not 65 <= a.seconds <= 120:
        p.error("valid video ID and 65..120 seconds required")
    raise SystemExit(asyncio.run(observe(a)))


if __name__ == "__main__":
    main()
