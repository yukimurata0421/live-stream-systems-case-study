# Clock Observation Case Study

This case study records how video-progress observation changed between
2026-10-21 and 2026-10-22 JST: from occasional pixel comparison to paired clock
reading on a continuous return feed, through several hours in which that reader
was blind without any warning, to a separate provider-thumbnail layer. All times
are JST.

This is retained historical evidence, not a current health report. The current
contracts are the [return-feed clock monitor](relay-clock-monitor.md) and the
[thumbnail contract](twitch-thumbnail-monitor.md);
[observability](../observability.md#video-observation-layers) defines how far
each layer reaches. Exact measurements and source selection are in the dated
records listed at the end.

## Starting Point

The stream overlay shows a clock in JST and UTC. The goal was to check that this
clock keeps advancing after the video leaves the delivery host, without
restarting anything on the result.

The first monitor added a third read branch on the OCI relay. Every 60 seconds
it opened a new connection, decoded one frame and compared the clock region's
pixels with the previous sample. OCR ran only on every tenth comparison or after
a suspected stall. A changed region was reported as probable progress, and the
exporter treated probable and OCR-confirmed progress the same way.

## What The Review Found

- **Changed pixels are not an advancing clock.** In the retained pre-change
  history of 127 rows, 110 were probable progress and 14 were OCR-confirmed.
  Most "advancing" results therefore rested on pixel change alone.
- **Wide tolerance hid uncertain reads.** Any forward step of up to about two
  minutes was accepted. One audit read a 55-second step where neighbouring
  audits read 60; the image was not retained, so a misread could not be
  confirmed or excluded.
- **Delay was available but unused.** The difference between the displayed
  clock and capture time is a display age. The one-shot design measured it
  after FFmpeg exited, so the roughly ten-second connection setup was included.
- **Scope was easy to overstate.** A moving clock at the relay output says
  nothing about YouTube or Twitch reception, and a forward could fail while the
  relay stayed healthy.
- **Failures collapsed into one state.** Missing publisher, TLS failure and
  decoder failure were all `UNKNOWN`, and a capture failure reset the stall
  counter, so an alternating stall and failure could never confirm.

## Correction

The monitor moved to one persistent reader per source and reads both JST and UTC
on every ten-second sample. A reading is accepted only when both clocks pass the
confidence threshold and agree modulo nine hours. Progress must match monotonic
elapsed time within five seconds; 5-30 seconds is recorded as drift, and larger
errors stay uncertain. Probable progress no longer counts as advancing. Display
age is taken when a complete decoded frame arrives. Failure reasons, native
forward facts and freeze coverage are reported separately.

In a 90-second operated window at 01:42-01:44, all ten unique cycles were
confirmed and valid display ages were 3.46-3.72 seconds. That window was short
and normal; it did not exercise failure.

## The Reader Went Blind

On the same morning the corrected reader stopped producing frames. The last
valid observation was at 06:31:36. The retained history from 07:10:58 to
09:17:30 contained 760 consecutive `capture_starting` / `capture_stream_ended`
rows; the gap before the retained window is not assumed to have been continuous.
Throughout, native relay facts showed a ready publisher and progressing YouTube
and Twitch forwards. Sender-side flow continued; the frame observation was lost.

The reader's FFmpeg 5.1.9 was given a one-second, 1 MiB input probe. It often
ended the probe before receiving a keyframe and exited with "Could not find codec
parameters" and "does not contain any stream". In repeated starts inside the
monitor Pod, that probe succeeded in three of six attempts, while five seconds
and 5 MB succeeded in six of six. A newer FFmpeg outside the Pod succeeded six
of six times with the short probe, so the result depends on the decoder version. The run of
failures was much longer than a 50% success rate would suggest. One explanation
is that the fixed ten-second reconnect kept landing on the same GOP phase; that
was not demonstrated. The reader started working again at about 09:17, shortly
after diagnostic commands loaded the Pod, and that timing does not establish why.

No warning was raised for these hours. The observer's heartbeat stayed healthy,
the notifying Prometheus had no rule on clock observations, and a recorded
disagreement between the frame result and native facts had no consumer.

The repair raised the probe to five seconds and 5 MB, added 0.1-2 seconds of
reconnect jitter, and gave missing codec parameters their own reason code. The
isolated test was rerun with the production 5 fps / GOP 10 settings. Two warnings
were added to the existing notification loop: a required clock observation
missing for more than 300 seconds, and observer or quality metrics missing for
five minutes. Replaying the 760 retained rows offline, every row met the new gap
condition. From 09:44 to 09:47, 18 of 18 cycles after runtime binding were
confirmed.

## A Provider Layer For Twitch

The return feed cannot show what a provider did with the video. Automating the
public player was not adopted. Twitch's Get Streams API returns a
`thumbnail_url`, an image from a frame in the last five minutes, and the
existing API collector already fetched that response. A worker on the monitoring
host now reads the clock in that image.

From 10:29 to 10:59 the worker recorded 30 observations, five distinct clock
values and four progress confirmations, with new images exactly six minutes
apart. On one image, 720p gave UTC confidence 45.4 against 96.0/95.2 at 1080p,
so 1080p is requested.

Review of that first version found three problems:

- The thumbnail URL belongs to the channel, not the stream. After a session
  renewal, the old ETag would have produced 304 responses and the old stream's
  last image, which the worker would have counted as OCR failures. Waiting for
  the first image of the new stream would likely have exceeded the 300-second
  warning every day. The ETag is now cleared on a stream change, the old image
  is an explicit waiting state, and the first clock of a stream gets up to 720
  seconds.
- A stale warning at 600 seconds would fire after a single missed six-minute
  refresh. It was moved to 840 seconds.
- If the exporter could not read the worker's state, the age metric disappeared
  and no warning could fire. The exporter now keeps the last publication time,
  and a rule covers the metric disappearing.

From about 11:06, UTC confidence on real thumbnails dropped below 70 while JST
and UTC still agreed, and the observation warning fired. On a retained clock
region, the original UTC confidence was about 57.9; a binary threshold of 128
kept both readings unchanged with confidences 92.9 and 94.1, while a threshold
of 96 produced misreads. One binary retry was added, accepted only when both
readings are unchanged and both meet the original threshold. Counters for OCR
observations and retries were deployed at 11:51; by 12:12 they showed three
observations and two retries. At 11:22 the live image had passed with the
ordinary read, so the retry was not shown to be the cause of that recovery.

The return-feed analyzer also raised a UTC-confidence warning around noon while
frames and forward bytes continued. It had no binary retry and was not changed
that day.

## Evidence Before A Wider Read

On 2026-10-23 the return feed's unknown picture-freshness minutes were traced
to UTC confidence: in all 40 failing images of one 24-hour window both clocks
parsed and agreed, and only UTC fell below 70. The thumbnail had already gained
from a UTC region widened three pixels to the left, and downscaled JPEGs
suggested the same for the return feed. But the analyzer kept only JST pixels
on failure and did not record the contrast retry, so the cause could not be
tested on the production input.

The reader first started keeping native JST and UTC regions, a wider UTC
context and every read stage, plus occasional normal controls. Within about 40
minutes that produced 39 natural low-confidence rejections and 31 controls.
Widening rescued all 39 but rejected one control that the current region read
at confidence 92.1, which dropped to 51.5 when widened. Widening was better on
average and worse for that image.

So the region was not changed. The current ROI stays authoritative, and the
wider UTC region is read once, only after both current passes fail, against a
confident JST and without changing any value already read. On the retained
images this keeps all 31 controls untouched and rescues all 39 rejections. In
its first half hour in production every sample passed on the current region,
so no rescue has been observed yet.

## Lessons

- A healthy heartbeat shows the worker is running, not that it observes
  anything. Observation gaps need their own warning.
- Prefer a reading that can be wrong in a visible way (paired clocks, a cadence
  check) over a signal that is merely correlated with progress (changed pixels).
- Disagreement between independent layers is useful only if something reads it.
- A threshold derived from one short window should be written as provisional
  and tied to the refresh behaviour it assumes.
- Keep the input that failed before tuning the reader. A downscaled copy can
  suggest a cause but cannot confirm it.
- An improvement in the median can still break individual cases. Add an
  alternative after the working path fails rather than replacing the path.

## Not Established

- Viewer playback, audio, or video CDN health on either platform.
- Twenty-four-hour OCR quality, retry rate, or thumbnail refresh distribution.
- Thumbnail behaviour across a real session renewal. The 2026-10-22 renewal
  completed at 00:02, before the thumbnail worker started.
- Delivery of the new warnings to the notification channel.
- The cause of the long run of probe failures beyond the short probe itself.

## Dated Records

- [Return-feed monitor source port](public-source-port-2026-10-22.md)
- [Thumbnail initial source port](public-source-port-2026-10-22-thumbnail.md)
- [Thumbnail review follow-up](public-source-port-2026-10-22-thumbnail-review.md)
- [Operated OCR counter rollout](thumbnail-ocr-rollout-2026-10-22.md)
- [Return-feed evidence and UTC fallback read](public-source-port-2026-10-23-relay-clock.md)
