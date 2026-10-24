# Operational Case Studies

These are short accounts of real operational problems and one failed test
campaign. They explain the engineering decisions without requiring familiarity
with the whole system. The October cases summarize retained records from
2026-10-13 to 2026-10-24 JST; they are historical observations, not a current
health report.

The useful result is sometimes restored delivery, sometimes a corrected warning,
and sometimes a decision that the evidence is insufficient. Those results are
kept separate.

## 1. Diagnose A Network Stall Before Changing The Broadcast

**Problem.** The outgoing video connection repeatedly stopped making progress
around a morning window. Restarting the encoder or replacing the YouTube URL
could conceal the symptom while leaving the underlying cause unresolved.

**Investigation and response.** The diagnosis compared the video connection
with independent Cloudflare and Google connections, new connection attempts,
and changes in network identity. Both existing and new independent connections
failed during the recurring events. The evidence favored a home-network or
carrier-session problem over a YouTube-only problem. Recovery therefore
preserved the existing broadcast identity instead of creating a new URL.

**Result and limit.** The pattern repeated across three JST days and was followed
by renewed transport progress. It did not establish whether the router or the
carrier initiated the interruption. Historical local transport recovery had a
90-second median and a much slower tail; this is not direct viewer recovery time.

**What it demonstrates.** Test competing explanations with independent evidence,
then choose an action appropriate to the failing layer.

[Detailed TCP investigation](v3/tcp-stall-case-study.md) ·
[Higher-resolution evidence](v3/tcp-stall-resolution-depth.md)

## 2. An Old Watchdog Assumption Disrupted The New Cloud Route

**Problem.** The OCI route could deliver playable Twitch media, but repeated
workload restarts stopped it. The relay correctly stopped forwarding when its
input disappeared. A healthy provider response alone could not explain the
local restart.

**Investigation and response.** The retained watchdog logs matched three restart
times. Its old ingest check recognized TCP port 443 but the new local relay
connection used 1936. It classified the pipeline as unhealthy despite fresh
provider health. The authorized repair changed the existing encrypted relay
connection to port 443. The separate proposal to change the monitoring host's
restart policy was not applied as part of that repair.

**Result and limit.** After the 09:52 JST change, the recorded checkpoints showed
one sender, advancing relay/provider counters, a fresh successful watchdog
result with no restart action, and public Twitch media reception. A roughly
ten-minute closing window supported recovery in that window, not an entire
grant period or a completed failure qualification. Earlier uncertain route
attempts were retained rather than replayed or labeled successful.

**What it demonstrates.** Trace who performed an action and revisit monitoring
assumptions when the delivery architecture changes.

[OCI design at the time](v3/oci-relay-and-twitch.md) ·
[Related runtime-boundary code](../src/stream_core/runtime_readiness.py)

## 3. Twitch Renewal Initially Interrupted YouTube Forwarding

**Problem.** The daily-session mechanism was intended to stop and restart only
Twitch. Its first manual exercise at 11:38 JST unexpectedly recreated the
YouTube forwarding handler. The owner detected the identity change, entered a
hold, and did not dispatch the Twitch start.

**Investigation and response.** MediaMTX returned an empty `moqTransport` field
that changed meaning when the complete configuration was sent back. The adapter
was corrected to omit that empty field while preserving the YouTube entry and
other settings. The failed command was not resent; the corrected exercise used
a separate operation identity.

**Result and limit.** The corrected 11:50–11:53 JST exercise preserved the YouTube
handler and progressing bytes, observed Twitch OFFLINE for at least 120 seconds,
then verified a new Twitch session. A guest sample contained 1080p video and
decoded, non-silent audio. This verified one bounded exercise, not every later
midnight renewal or continuous reception over 48 hours.

**What it demonstrates.** Check the unaffected component explicitly. Successful
recovery does not erase the first failed attempt.

[Session renewal and exact evidence boundaries](v3/twitch-session-renewal.md) ·
[Reducer and adapter tests](../tests/test_twitch_daily.py)

## 4. A Current Warning Outlived Its Measurement Failure

**Problem.** An upload-measurement warning stayed active even after the current
source was producing fresh samples. The previous 24-hour window still contained
50 stale points out of 1,441, so closing a current warning could not justify
claiming that the historical measurement had been repaired.

**Investigation and response.** The correction required agreement between recent
raw transport samples and a matching 15-minute monitoring series. Only sustained
freshness on both sources could close that particular current warning. Other
coverage, disagreement, and error-budget reasons remained active when applicable.

**Result and limit.** The retained checkpoints showed current-source recovery and
zero active incidents after the correction. Historical source freshness remained
96.53%; the formal measurement/compliance result remained UNKNOWN. Neither the
past gaps nor the historical SLI value was rewritten.

**What it demonstrates.** Separate present service health from the quality of a
historical measurement window.

[Current-source classifier](../src/stream_core/notifications/upload_source.py) ·
[Regression cases](../tests/test_upload_source_recovery.py)

## 5. A Public Page And Cache Produced Misleading Failure Signals

**Problem.** Two presentation issues affected external evidence. A page-heading
change removed the exact string required by the external checker even though
the page returned HTTP 200. Later, a fixed origin query returned an old cached
status while the publisher and private monitoring already had newer data.

**Investigation and response.** The checker configuration was read without
changing its criteria; the required page text was restored. For the stale
snapshot, the investigation compared source files, upload cycles, response
timestamps, and cache metadata. The installed uploader was corrected to set
the intended cache policy during upload, closing the interval in which an
object could be cached with a longer default lifetime.

**Result and limit.** Three external regions passed after the text correction.
Once the old cached response expired, the public snapshot followed fresh
21:06, 21:09, and 21:12 JST generations; the last recorded age was about 15
seconds. This was publication and observation recovery. It did not repair
historical measurement gaps or prove every viewer's playback quality.

**What it demonstrates.** HTTP success, fresh public data, and successful media
delivery require different checks.

[External evidence contract](v3/operational-reliability-and-external-evidence.md) ·
[Publisher boundary](v3/public-publisher-boundary.md)

## 6. The Qualification Harness Did Not Produce A Valid Result

**Problem.** The October 13 native qualification campaign failed before it could establish
the intended safety result. The first reserved attempt could not start its
container because of a logging configuration error. The second reached an
isolated container, but its temporary-filesystem artifacts could not be
recovered before timeout.

**Response.** The failed reservations and available diagnostics were retained.
The harness was subsequently removed at the operator's request; no additional
attempt was started. The record did not infer a product defect or a pass from
the container's existence or its timeout.

**Result and limit.** The expected JUnit, summary, baseline, and negative-control
results were unavailable. That campaign's native qualification remained unverified. Later
successful live reception and ordinary source tests did not supply those
missing results retroactively.

**What it demonstrates.** Treat test infrastructure as something that can fail,
and preserve the difference between missing evidence and a successful check.

[Qualification boundary](v3/public-source-port-2026-10-13.md) ·
[Test strategy](test-strategy-and-safety-boundary.md)

## 7. An Unfinished Migration And Recovery Design Was Replaced

**Problem.** The goal was to add Twitch without sacrificing YouTube or running
two permanent home uploads. After the October 13 relay delivered media, the
implementation grew into a different migration and recovery system: signed
leases, a central route authority, typed commands between three hosts, and
native qualification before production steps. The complete path remained
unfinished; it was not simply awaiting production activation. By the evening
of October 19 there had been 104 qualification runs, 58 failed and 46 passed.
Repeated fixes to fixtures, isolation mounts, and preflight checks added work
without completing the operator's delivery and recovery goal.

**Trigger.** At 06:31 JST on October 20 a short network interruption ended the
one-shot backup sender started through that process, and it did not reconnect.
The main sender recovered by itself in five seconds. The safety component had
weaker recovery than the component it protected.

**Response.** The operator returned the work to the original priority: preserve
YouTube while adding another platform within the home uplink. The route first
returned to the earlier relay, then moved to a smaller design:
MediaMTX with `Restart=always`, a keeper that re-creates failed forwards and
renews Twitch, and one controller that starts a direct YouTube backup upload
after 30 seconds of relay failure and stops it after 300 seconds of health.

**Result and limit.** The new route went live at 08:43 JST with one upload.
The previous migration and recovery system had not achieved the intended
behavior. Its replacement removed lease fencing and central authorization for
this route, with the trade-offs recorded in
[the decision record](v3/oci-route-simplification-decision.md).

**What it demonstrates.** Reconsider the structure when repeated patches add
states and verification work without completing the main path. Partial
operation of an earlier component does not establish the replacement system.

## 8. Stopping The Relay On Purpose

**Problem.** Automatic fallback exists only on paper until the relay actually
fails. The drill stopped the relay service during a live broadcast.

**Result.** The controller started the fallback 52 seconds after the stop, and
YouTube reported the broadcast live and healthy while the fallback sent. The
relay was started again after about two minutes; the main sender reconnected
within seven seconds, Twitch came back with a new session, and the fallback
stopped itself after 300 seconds of relay health. The broadcast did not end,
and the recorded gap in data reaching YouTube was about 65 seconds. The operator
stopped and restarted the relay; fallback control and sender reconnection were
automatic. Frame-level continuity was not measured.

**What it found.** The YouTube watchdog on the monitoring host counted only the
main sender and confirmed an incident while the fallback was sending. Because
it could create a replacement broadcast, a longer outage could have replaced the
live one. Replacement creation was disabled the same morning. That afternoon the
watchdog was changed to count an established fallback connection as local
delivery; it has not yet been observed during a real fallback. Only a stopped
relay service was exercised.

**What it demonstrates.** Exercise recovery deliberately, and check what every
other automation does during the failure, not only the component under test.

[Self-healing route and drill](v3/self-healing-oci-fanout.md)

## 9. The Video Monitor Was Blind For Hours And Nothing Said So

**Problem.** A reader on the relay output checks that the stream's overlay clock
keeps advancing. On 2026-10-22 its last valid observation was at 06:31 JST. The
retained history from 07:10 to 09:17 JST held 760 consecutive start or
stream-ended failures, while the relay's own counters showed the publisher and
both platform forwards progressing. Sender-side flow continued; observation did not.

**Investigation and response.** The reader gave FFmpeg 5.1.9 a one-second input
probe, which often ended before a keyframe arrived. Inside the monitor Pod that
probe started successfully three times in six, against six in six with five
seconds. Why the failures ran unbroken for hours was not established. The probe
was lengthened, reconnects were jittered, and the failure got its own reason
code. No warning had fired because the worker's heartbeat stayed healthy and no
rule watched clock observations, so a 300-second observation-gap warning and an
observer-unavailable warning were added to the existing notification loop.

**Result and limit.** Replaying the 760 retained rows offline, every row met the
new gap condition, and 18 of 18 cycles after the fix were confirmed. Delivery of
the new warning to the notification channel has not yet been observed, and the
clock layers still do not establish viewer playback.

**What it demonstrates.** A healthy heartbeat shows that a worker runs, not that
it observes anything. An observation gap needs its own warning.

[Clock observation case study](v3/clock-observation-case-study.md) ·
[Observation layers](observability.md#video-observation-layers)

## 10. Keep The Encoder, But Verify Both Media Tracks

**Problem.** Restarting FFmpeg replaces the encoder as well as its failed output
connection. An internal FIFO can reopen only the output, but successful
reconnection does not establish complete media recovery.

**Investigation and response.** Isolated NVENC/AAC comparisons measured TCP,
TLS and publish responses for restarts and FIFO. Repeated 12/30/60-second
outages showed that retry phase could matter more than the replacement method.
The first production rollout then exposed a missing test: synthetic trials
explicitly mapped audio, while the complete production command did not. The
relay lost audio and the FLV-only target classifier stopped recognizing the
process. The original template was restored, stream mapping was corrected,
and the full input/output command and observer were checked before reapplying.

**Result and limit.** The corrected 12-second fault kept the same process and
Pod, changed connection generation 1 to 2, and resumed observed ACK progress
about 1.95 seconds after planned restoration. Returned video/audio and both
forward counters were checked. The initial audio gap, about 12:06-12:18 JST,
remains a rollout failure; provider-player impact is unknown. The full
retry-exhaustion/process-replacement cascade and real ISP reconnect were not
exercised.

**What it demonstrates.** Separate process, connection and media evidence.
Test the complete invocation rather than transferring a component result to
an untested input-selection path.

[Measurements and reproducible summaries](v3/ffmpeg-fifo-measurements.md) ·
[Runtime and telemetry contract](v3/ffmpeg-fifo-recovery.md)

## Evidence And Reading Boundaries

The October accounts were curated from retained deployment, renewal,
qualification-failure, and warning/cache-recovery records. Private logs,
addresses, credentials, process identities, and raw artifacts are excluded.
[Source provenance](../publication/source-map.json) identifies the public files;
[local validation](../publication/validation.json) reports source checks.

For measurement dates and limitations, use the [scorecard](operational-scorecard.md).
For one deeper code review, use the [implementation map](implementation-review-map.md).
