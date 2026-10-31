# Moving Public Snapshots To R2 And Detecting Stale Evidence

This is a dated record of the operated system on 2026-10-27 JST. It describes
the storage migration, its verification, and a later clock-skew failure.
The R2 migration itself is documented here; its uploader/Worker implementation,
private operations records, generated snapshots, cloud identities and archive
contents are not included. The snapshot timestamp repair is now also present
in the public Twitch/SLI collectors, as noted in the October 31 source update below.
The public publisher source still represents its earlier GCS implementation.

## Storage Migration And Verification

The public delivery path changed to:

```text
sanitized publisher output
  -> private R2 bucket
  -> Cloudflare Worker binding
  -> public browser
```

The publisher's measurement generation, rolling windows, historical failures,
and SLO definitions were retained. The Worker reads the published files; it
does not recalculate reliability or control media delivery. Its browser cache
lifetime is 60 seconds for JSON and 300 seconds for static files.

Verification covered matching bytes at the publisher, object store, and
public URL; progressing JSON timestamps; the rendered page; and the actual
deployed Worker. During the first cutover, a cache-hit response unexpectedly
advertised a four-hour lifetime. The Cache API path was removed and repeated
GET/HEAD checks confirmed the intended lifetimes before completion.

The old site's 38 objects and an older status area's 18 objects were copied
to a separate private R2 archive. Original object metadata and generations
were recorded. All 56 bodies were read back and checked before conditional
GCS deletion, and the archived index was retrieved with its original digest.
The archive has no public domain or Worker binding.

At 09:47 JST, GCS had zero live objects, public delivery continued from R2,
and the former GCS publishing entrypoints had been retired. Soft-deleted
generations still remained: approximately 170 MB, with the last scheduled
expiry at 2026-11-03 09:26 JST. This was not an observation of their future
permanent deletion or a claim that accrued cloud charges had disappeared.

## Freshness And External Observation

The operated page and Worker share a contract for eight JSON feeds. Missing
or malformed data, snapshots older than 180 seconds, and timestamps more
than 30 seconds ahead are excluded from the current display. The page shows
unknown status and stopped-update times instead of retained LIVE badges or
old successful percentages. Original measurements and timestamps remain intact.
Fetches have an eight-second timeout, and displayed age is reevaluated every
15 seconds even without a successful new response.

A publication-health endpoint checks the index and all eight feeds. It
returns HTTP 200 for fresh publication and 503 otherwise, always with
`Cache-Control: no-store`. This observes snapshot delivery, not decoded
viewer reception or formal availability. The existing external check uses
this endpoint every minute. A separate check preserves HTTPS and page-content
verification through the site's other hostname.

The external-result importer also needed correction. Reading only the first
Monitoring API page could report one observed region when all three had
passed. The reader now retrieves subsequent pages within a bounded deadline;
regional quorum, stale limits, incident rules, and notification routes were
retained. Both external targets had three fresh passing regions in the
subsequent natural timer cycle at 09:39 JST.

Checks included 16 Worker tests against the deployed bundle, 14 publisher
tests, nine importer-related tests, eight isolated browser scenarios, and
four isolated archive/deletion-guard scenarios. Failure cases were exercised
locally; production was not deliberately broken to test notification delivery.
These checks do not establish uninterrupted media or long-term stability.

## Follow-Up: A Small Clock Difference Stopped Two Feeds

After the successful migration checkpoint, Twitch observation and transfer
data stopped advancing at about 10:01 JST while the other snapshots continued.
The page correctly reported those two feeds as stale, alongside a recent
timestamp for the separately updated monitoring snapshot.

Diagnosis at 10:19-10:21 JST found current source data and successful SSH
responses. Both older publisher-side collectors required a generated timestamp
to be no later than the receiving host's clock, with zero future tolerance.
The source timestamps were approximately 47-50 milliseconds ahead at receipt.
That rejected otherwise valid JSON, and the collectors retained their last
good files. Upload success therefore coexisted with two stale feed bodies.

For each feed, a diagnostic in-memory copy with only the generated timestamp
placed one second in the past passed the same validator. The source Twitch
observation was LIVE and the transfer observation was also advancing. Both
hosts reported NTP synchronization; the exact reason the relative offset
crossed the strict bound was not established. Production data and host clocks
were not changed by this diagnosis.

At 10:40 JST, both operated collectors received a shared one-second future
tolerance. The 180-second age limit and original timestamps were retained;
invalid numeric timestamps, larger future offsets, and stale data still fail.
Rejected data retains the last good file and reports a fixed reason code.
Boundary and file-output tests covered both collectors, including the observed
50 ms offset, nonfinite values, rejection, and preservation of prior bytes.

Both feeds resumed in the first publisher cycle. Publisher, R2, and public
response bodies matched; the health endpoint returned 200 with eight fresh
feeds. The browser showed LIVE again, subsequent timer output advanced, and
all three publication-check regions had passing samples at 10:42 JST. Existing
historical failures and formal reliability judgments were retained.

That first checkpoint was incomplete. The clock-observation collector also
used a zero-tolerance check and had stopped at 10:39 JST; its file had not yet
exceeded the display's age limit during the first recovery check. The health
endpoint later returned 503 again, and the operator reported the clock warning.
A direct fetch succeeded but its timestamp was approximately 28 ms ahead and
was rejected. The first repair had missed this other use of the same assumption.

At 10:47 JST, clock, relay, and Twitch SLI acquisition checks were connected
to the same bounded one-second tolerance. The clock collector kept its stricter
120-second age limit; the other collectors retained 180 seconds. The captured
clock response that failed the old validator passed the candidate unchanged.
At 10:48 JST, the clock file advanced again, matched R2 and public responses,
and all eight feeds were fresh. The collector tests now cover 28 cases across
these paths, including unchanged measurement windows and retained failure data.
The earlier partial repair and temporary recovery remain part of this record.
Publication checks had passing samples in all three regions at 10:48 JST.
The next natural importer cycle at 10:50 JST also returned three passing
regions for both its public-status and YouTube targets, without resetting
their failure history or restarting the monitoring core.
From 10:48:46 to 10:51:50 JST, seven observations remained healthy and all
eight generated timestamps advanced by 180-194 seconds. This covered multiple
natural publication cycles; it is a bounded recovery observation, not a
long-term availability guarantee.

The saved pre-migration and current builder were byte-identical, and the GCS
uploader invoked that same builder before storage. Retained GCS-era logs also
contained three transfer-feed acquisition failures on the previous day, but
did not record their causes. They cannot establish the same clock-skew fault.
The old Twitch display trusted its retained source-fresh flag without checking
the envelope age, and transfer freshness used a one-hour observation limit.
Thus the old page could conceal a stopped acquisition. The stricter new checks
exposed this failure; changing object storage does not itself explain the
relative host-clock offset. Its onset remains unproven.

A broader journal comparison confirmed a real increase in failed acquisition
attempts: three entries across 1,922 completed GCS-era cycles in roughly
32 hours, compared with 103 entries during the later 10:00 JST hour. Most
were repeated attempts during the same persistent failures, rather than
independent outages. The earlier three errors did not justify treating the
increase as only a change in visibility. The clock-offset onset and any
indirect connection to the migration remain unproven.

## Response-Time Follow-Up

A later operator-side sample at the Tokyo edge measured median TTFB of about
234 ms for HTML and 177-279 ms for the three principal JSON files, with four
samples per path. One cold headless-browser run reached the populated
dashboard at about 1.34 seconds. It showed an additional module fetch before
JSON loading, and a full age-timer redraw occupied the main thread for about
189 ms in that run.

There was no matching pre-migration timing capture. These observations identify
work to investigate, but do not establish an exact GCS-versus-R2 slowdown or
an end-user latency guarantee. Performance optimization remained separate from
the clock-skew diagnosis at this checkpoint.

## Separating Live Status From Observation Gaps

At 11:32 JST, a further operated repair separated the three feeds needed for
live-status confirmation (monitoring, relay and Twitch observation) from five
supporting observation feeds. Missing supporting data is still excluded from
its card and explicitly listed in a separate data-updates panel; it no longer
alone makes live status unconfirmed. Missing required observations remain
unconfirmed, and observed provider OFFLINE remains degraded. The monitoring
timestamp is now labelled as monitoring data rather than a page-wide update.

The primary publication-health endpoint requires the page and those three
feeds. A separate strict observation-health endpoint still requires all eight.
Both return every feed's state, observation completeness and issue list with
no caching. Existing regional checks and the result importer were retained;
supporting-feed gaps remain visible without being classified as whole-site
failure.

The publisher now permits at most two simultaneous requests. It omits an
unnecessary pre-read only for changed objects verified by the preceding
successful cycle. Readback after PUT, reconciliation of uncertain outcomes,
and the prohibition on duplicate uncertain PUTs remain. Failure returns wait
for in-flight requests to finish. Private status records build and publication
durations without clearing accumulated failures.

The browser entrypoint bundles its shared contract, removing a module-fetch
waterfall. Age-only ticks retain chart nodes and expanded details; an age
boundary still triggers reevaluation and removal of stale values. Four natural
publication cycles had a median duration of about 27.3 seconds versus 42.6
seconds in the preceding 15 cycles. Seven freshness observations remained
healthy and all eight timestamps advanced. A single cold-browser comparison
reached the populated page in about 1.13 seconds versus 1.45 seconds before;
the former 193 ms redraw after 15 seconds was absent. Initial chart rendering
still occupied about 278 ms. These are bounded measurements, not a latency or
long-term availability guarantee.

Validation covered 19 publisher tests on both development and publisher hosts,
18 tests against the deployed Worker bundle, 14 isolated browser cases, and
three client-only fault/recovery cases through the deployed page. No server
fault was injected. The natural external-result import remained healthy with
three passing regions per target. The uploader and page changes in
this section are documented only; their implementation is not ported here. The
collector timestamp repair described below is ported.

At 11:56 JST, the persistent data-updates summary was removed following review
of its usefulness to readers. Normal operation now adds no source-count panel;
missing or stale data is still identified in its affected section. The live-
status decision and both internal health contracts remain unchanged. The new
versioned browser entrypoint was verified with normal, missing-data and recovery
cases; this remains a documentation-only public update.

For the public source boundary and its earlier portable implementation, see
[publisher boundary](public-publisher-boundary.md). The
[documentation record](../../publication/r2-publication-docs-20261027.json)
distinguishes this local documentation addition from the earlier operated work.


## Public source alignment (2026-10-31)

The public Twitch and SLI collectors now use `public_snapshot_time.py`, matching
the operated one-second future tolerance and 180-second freshness limit in this
case study. Invalid values, excessive future time and stale source data fail
closed; a failed fetch preserves the previous artifact bytes and timestamps.
SLI history start is ordered against source generation, not the receiver's
possibly different clock. This source repair is separate from the excluded R2
uploader implementation and does not deploy a publisher.

The [October 31 clock contract](cross-host-observation-time.md) subsequently
replaced the relay route-facts controller's provisional one-second rule with a
request-bound age interval. That controller policy and the public Twitch/SLI
scalar tolerance serve different boundaries; the latter was not removed.
