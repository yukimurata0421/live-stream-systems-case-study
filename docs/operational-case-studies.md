# Operational Case Studies

These are short accounts of real operational problems and one failed test
campaign. They explain the engineering decisions without requiring familiarity
with the whole system. The October cases summarize retained records from
2026-10-13 to 2026-10-20 JST; they are historical observations, not a current
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

**Problem.** A native qualification campaign failed before it could establish
the intended safety result. The first reserved attempt could not start its
container because of a logging configuration error. The second reached an
isolated container, but its temporary-filesystem artifacts could not be
recovered before timeout.

**Response.** The failed reservations and available diagnostics were retained.
The harness was subsequently removed at the operator's request; no additional
attempt was started. The record did not infer a product defect or a pass from
the container's existence or its timeout.

**Result and limit.** The expected JUnit, summary, baseline, and negative-control
results were unavailable. Native qualification remained unverified. Later
successful live reception and ordinary source tests did not supply those
missing results retroactively.

**What it demonstrates.** Treat test infrastructure as something that can fail,
and preserve the difference between missing evidence and a successful check.

[Qualification boundary](v3/public-source-port-2026-10-13.md) ·
[Test strategy](test-strategy-and-safety-boundary.md)

## 7. A Safer Design That Never Reached Production Was Retired

**Problem.** After the October 13 relay worked, the next goal was to make the
route change itself fully automated and provably safe: signed leases, a
central route authority, typed commands between three hosts, and native
qualification before each production step. Six days later the route was still
not enabled automatically. By the evening of October 19 there had been 104
qualification runs, 58 of them failed, and many failures were in fixtures,
isolation mounts, and preflight checks rather than in forwarding media.

**Trigger.** At 06:31 JST on October 20 a short network interruption ended the
one-shot backup sender started through that process, and it did not reconnect.
The main sender recovered by itself in five seconds. The safety component had
weaker recovery than the component it protected.

**Response.** The operator restated the goal as unattended, self-recovering
delivery within a narrow home uplink. The route first returned to the October 13
relay that had already run in production, then moved to a smaller design:
MediaMTX with `Restart=always`, a keeper that re-creates failed forwards and
renews Twitch, and one controller that starts a direct YouTube backup upload
after 30 seconds of relay failure and stops it after 300 seconds of health.

**Result and limit.** The new route went live at 08:43 JST with one upload. The
decision gave up lease fencing and central authorization for this route; those
properties were not shown to be wrong, only too costly for this system. See
[the decision record](v3/oci-route-simplification-decision.md).

**What it demonstrates.** Notice when the verification apparatus grows faster
than delivery, and choose the simplest mechanism that meets the actual goal.

## 8. Stopping The Relay On Purpose

**Problem.** Automatic fallback exists only on paper until the relay actually
fails. The drill stopped the relay service during a live broadcast.

**Result.** The controller started the fallback 52 seconds after the stop, and
YouTube reported the broadcast live and healthy while the fallback sent. The
relay was started again after about two minutes; the main sender reconnected
within seven seconds, Twitch came back with a new session, and the fallback
stopped itself after 300 seconds of relay health. The broadcast did not end,
and data stopped reaching YouTube for about 65 seconds. No operator action was
needed after the stop and start.

**What it found.** The YouTube watchdog on the monitoring host counted only the
main sender and confirmed an incident while the fallback was sending. Because
it could create a replacement broadcast, a longer outage could have replaced the
live one. Replacement creation was disabled; teaching the watchdog about the
fallback remains open. Only a stopped relay service was exercised.

**What it demonstrates.** Exercise recovery deliberately, and check what every
other automation does during the failure, not only the component under test.

[Self-healing route and drill](v3/self-healing-oci-fanout.md)

## Evidence And Reading Boundaries

The October accounts were curated from retained deployment, renewal,
qualification-failure, and warning/cache-recovery records. Private logs,
addresses, credentials, process identities, and raw artifacts are excluded.
[Source provenance](../publication/source-map.json) identifies the public files;
[local validation](../publication/validation.json) reports source checks.

For measurement dates and limitations, use the [scorecard](operational-scorecard.md).
For one deeper code review, use the [implementation map](implementation-review-map.md).
