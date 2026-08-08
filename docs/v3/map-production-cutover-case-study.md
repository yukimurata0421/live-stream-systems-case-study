# Map Renderer Production Cutover Case Study

This case study records the August 2-4, 2026 JST transition from the previous
viewer map to the track-free custom MapLibre renderer. It preserves the
failure, rollback, repair, soak, and production-acceptance sequence without
publishing raw runtime state, private logs, local ports, Pod identifiers, or
captured media.

This is retained historical evidence, not a current health report. The current
renderer and monitoring contracts are documented in
[`map-rendering-and-monitoring.md`](map-rendering-and-monitoring.md).

## Change Under Review

The candidate changed the viewer-facing rendering path while keeping YouTube
lifecycle authority and encoder ownership outside the map itself. The intended
result was:

- current-position aircraft icons without retained flight tracks or labels;
- 24-hour LOS-aware reception coverage and 50/100/150 NM range rings;
- analysis-only JMA precipitation with bounded last-known-good behavior;
- explicit map-tile and current-aircraft render readiness;
- the existing audio, NVIDIA NVENC, and RTMPS delivery contracts.

The test sequence did not treat a healthy HTTP endpoint, a Running Pod, or one
good screenshot as proof that the renderer was ready for production.

## Confidence Gates

| Stage | Window | Authority | Exit evidence |
| --- | --- | --- | --- |
| Local no-track soak | 30 minutes | Isolated browser and map only | 346 five-second samples, `7 / 7` actual frames, no restart, final render ready and precipitation current. |
| Clean renderer soak | 24 hours | Isolated browser and map only | `17,280 / 17,280` samples, `289 / 289` actual frames, no restart, remediation, memory guard, or descriptor guard. |
| NVENC soak | 1 hour | Isolated map plus local video encode | `18,000 / 18,000` frames, no drop or duplicate, `13 / 13` visual checks, no encoder restart. |
| Production cutover observation | 1 hour | Live delivery runtime | `13 / 13` five-minute health checks and actual frames, no Pod, container, or FFmpeg restart, NVENC and RTMPS ready. |

Any candidate or harness repair invalidated the active confidence window. A
fresh window started from zero after the repair; partial time from superseded
runs was not added to the accepted result.

## Failures And Root Repairs

### WebGL2 Blocklist During Local Preflight

The first 1920x1080 preflight frame had a blank map canvas while ADS-B and
precipitation inputs remained available. Chromium reported that WebGL2 was
blocklisted. The isolated browser received explicit ANGLE and SwiftShader
arguments, was restarted, and then completed a fresh 30-minute clean window.

This early repair applied to the soak harness. A later production attempt
proved that the same arguments also had to be part of the real browser startup
path rather than only the test supervisor.

### Response Ownership And Last-Known-Good Validation

The first attempted 24-hour run failed after about 2 hours 51 minutes. One
frame showed an aircraft JSON fragment in the music title, valid precipitation
was presented as unavailable, and the render-ready heartbeat stopped updating.
The aircraft map itself remained visible and no flight tracks returned, but
the frame was not accepted.

The parent overlay was repaired to validate response content type, payload
shape, and schema before changing music or precipitation state. Invalid
responses now retain the last-known-good presentation. The failed run remained
in the operational history as `37` good frames and `1` failed frame; it was not
counted toward the final 24 hours.

### Render-Ready Response Body And Descriptor Growth

The next 24-hour attempt was stopped after about 2 hours 52 minutes. All `43`
captured frames looked correct, but the render-ready report became stale and a
browser remediation occurred. Chromium shared-memory descriptors also rose
over time.

A controlled 15-minute comparison isolated the cause: the browser checked the
HTTP status from the render-ready POST but did not consume and validate its
JSON response body. The unconsumed-response variant rose by approximately
`5.648` shared-memory descriptors per minute; the response-consuming variant
measured `0.000` per minute.

The browser was changed to parse the body and require `accepted === true`.
A 20-minute verification then completed 240 samples and `5 / 5` frames with no
restart or remediation before a new 24-hour window was started.

### Harness Process Classification

A short preflight after the descriptor repair found that the process parser
could classify the Snap Chromium renderer as the browser process. Total
descriptor counts were valid, but per-process trend attribution was not. The
parser was repaired and the preflight was superseded rather than treated as
part of the clean soak. This was observability-harness failure, not renderer
failure.

### Remote Recovery Collision During Production Cutover

The first production attempt failed during its initial gate. An existing
monitoring-plane recovery timer changed the Deployment while the controlled
cutover monitor was validating the candidate. The candidate Pod temporarily
disappeared, which the monitor conservatively classified as candidate identity
drift and rolled back.

The repair kept monitoring active but suspended that mutating recovery timer
for the bounded observation window. An independent timed fallback restored the
timer if the interactive client or cutover monitor disappeared. This separated
observation from competing mutation authority.

### WebGL2 Blocklist In The Production Browser Path

The second production attempt reached the real Snap Chromium startup path and
again failed to establish MapLibre render readiness because WebGL2 was
blocklisted. The prepared rollback completed before the next attempt.

The browser startup implementation was repaired to include the ANGLE and
SwiftShader arguments in the production path. This was a browser-rendering
change only: FFmpeg remained on `h264_nvenc`; it was not switched to CPU
encoding. Contract tests were added for the browser arguments.

### Render-Ready Idle Starvation

The third production attempt produced `13 / 13` visually correct frames and
kept NVENC, audio, RTMPS, precipitation, Pod health, and resources acceptable.
The final gate still failed because render-report age reached `60.271` seconds,
exceeding the 30-second expiry.

Readiness republishing depended on MapLibre's `idle` event. A valid display
could therefore stop sending heartbeats when no new idle event arrived. The
monitor correctly rejected the final gate and completed rollback even though
the frame still looked healthy.

The repaired contract separates initial readiness from ongoing liveness:

- initial readiness still requires loaded map sources and a recent aircraft
  sample;
- after initial readiness, an independent five-second timer republishes the
  heartbeat;
- server-side expiry remains 30 seconds;
- WebGL context loss stops the heartbeat rather than reporting false readiness.

An isolated production-image preflight then remained ready before the final
one-hour production attempt began.

## Rollback Decision Contract

A fixed previous image and configuration, a serialized rollback procedure, and
the candidate identity were recorded before each production attempt. The
cutover monitor used these thresholds:

| Evidence | Decision |
| --- | --- |
| Candidate restart, image or source identity drift, or WebGL fatal evidence | Immediate rollback. |
| Initial gate failure or final gate failure | Immediate rollback. |
| Other delivery-health failure | Rollback after two consecutive failed checks. |
| Precipitation failure by itself | Degrade weather evidence; do not roll back delivery. |
| Interactive-client loss | Continue under the user service and retain events; do not abandon the observation silently. |

The rollback had two scopes. A renderer-only rollback returned the viewer to
the previous rendering configuration. A deployment rollback returned the
previous image, configuration, and container set. Weather state remained
separate from ADS-B state so a visual rollback did not overwrite the primary
aircraft source state.

This was not general-purpose automatic fallback. It was bounded authority for
one identified candidate and one prepared previous state. It never started a
second publisher, and it did not grant the monitoring plane unrestricted
Deployment mutation.

## Accepted Soak Evidence

### Clean 24-Hour Renderer Window

The accepted isolated renderer window ran from August 3 05:59 to August 4
05:59 JST:

- `17,280 / 17,280` five-second samples with full coverage;
- `289 / 289` scheduled 1920x1080 frame checks;
- zero unexpected exits, component restarts, service restarts, remediations,
  memory guards, descriptor guards, or `EMFILE` evidence;
- zero post-warmup render failures or weather failures;
- final render ready and precipitation current;
- browser PSS from approximately 721 to 871 MiB, with a fitted slope of about
  `+1.010 MiB/hour`, but no runaway growth, swap use, OOM, or restart.

This was a renderer and browser soak. It did not publish video and did not
modify the production Deployment. An independent production `tcp_stall`
recovery changed the live Deployment generation during the window; that was
retained as a boundary warning rather than attributed to the isolated
candidate.

### One-Hour NVENC Window

The accepted encoder window used the production video contract:

```text
h264_nvenc p4 / CBR
1920x1080 / 5 fps
3400k bitrate / 3400k maxrate / 6800k buffer
```

It produced `18,000 / 18,000` frames over approximately one hour, with zero
drop, duplicate, encoder error, encoder restart, or map guard event. All
`13 / 13` visual checks were valid and contained no flight tracks.

The output used a local null muxer and was video-only. It did not include
PulseAudio, RTMPS upload, or YouTube delivery, so it was an encoder confidence
gate rather than production acceptance. A separate live-runtime `tcp_stall`
recovery occurred during this isolated test; the test encoder continued, and
the event was not claimed as a candidate-caused failure.

## Authoritative Production Result

After the three superseded production attempts and their repairs, the final
candidate ran in production from August 4 14:07 to 15:07 JST.

| Result | Evidence |
| --- | --- |
| Scheduled health checks | `13 / 13` passed at five-minute intervals. |
| Actual frames | `13 / 13` distinct 1920x1080 frames passed; no flight tracks or layout corruption. |
| Runtime continuity | No Pod replacement, container restart, FFmpeg PID change, or fast-recovery intervention. |
| Encoder and delivery | `h264_nvenc`, 5 fps, 3400k CBR, AAC 192k, and an established RTMPS socket. |
| Rendering | Current aircraft samples, map sources ready, no WebGL fatal evidence, and fresh independent heartbeats. |
| Weather | Analysis-only precipitation remained current and advanced through 12 source generations. |
| Resources | Stream cgroup memory remained approximately 777-983 MiB with no monotonic growth, swap consumption, guard trigger, OOM, or restart. |
| Final decision | Keep the candidate in production; no rollback in the accepted window. |

The one-hour window supports the live renderer, audio, NVENC, RTMPS send path,
actual source frame, and short-window resource stability. It does not prove
indefinite memory stability or viewer-visible correctness for every frame.

## Evidence Classification Lessons

The rollout retained three different classes instead of flattening every red
signal into a renderer incident:

- **Candidate failure:** WebGL startup failure and stale render heartbeats
  directly failed candidate gates and required rollback.
- **Independent historical event:** a production `tcp_stall` recovery during
  isolated soak changed production state but did not interrupt the candidate.
- **Observability noise:** one monitor summary reported rollback failure after
  seeing the rollback lock, while the rollback event ledger, completion marker,
  and restored Deployment agreed that rollback succeeded.

This distinction prevented a correct rollback from being reported as a failed
recovery and prevented unrelated production movement from invalidating an
otherwise isolated test without explanation.

## Post-Cutover Boundary

The production cutover did not itself add the later 60-second map-runtime
probe, 300-second public-viewer synthetic probe, public dashboard fields, or
Discord/Slack alert routing. Those were follow-up observability changes and are
documented as the current contract in
[`map-rendering-and-monitoring.md`](map-rendering-and-monitoring.md).

The later time-of-day base-map palette is also a current renderer feature, not
evidence that was present in the August 2-4 cutover window.

Public tests preserve the repaired contracts in
`tests/test_adsb_map_contract.py`, `tests/test_runtime_bootstrap_contracts.py`,
`tests/test_overlay_precipitation_state.py`, and related runtime-readiness
tests. Public CI validates those deterministic contracts but does not replay
the private production rollout.

## Claims And Limits

- Raw JSONL, screenshots, state directories, local ports, image identifiers,
  Pod identifiers, and production credentials remain outside this repository.
- The 24-hour accepted window was isolated renderer evidence, not a 24-hour
  live YouTube smoke test.
- The NVENC window used a local null muxer, not RTMPS or YouTube.
- The production acceptance window was one hour; normal long-window SLI review
  remains a separate requirement.
- OpenFreeMap, terrain, and the JMA website interface do not provide a service
  availability guarantee to this project.
- This case study records a historical decision and must not be read as proof
  that the stream is healthy now.
