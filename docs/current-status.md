# Current Status

Last updated: 2026-11-01 JST.

This is the only page that records state expected to change soon: what was
changed recently, what has not been observed yet, and what is deliberately
disabled. Other pages describe designs and dated evidence and link here instead
of repeating these items.

## Notification Observation Time (2026-11-01 JST)

The [notification evidence contract](v3/notification-observation-time.md) binds
freshness and recovery confirmation to producer host, boot and sample identity.
Producers were upgraded before consumers.

| Item | Observed checkpoint |
| --- | --- |
| Applied | Producers at 20:12 JST and consumers at 20:20 JST. A consumer-only SLO classification correction followed at 20:49 JST. |
| Checks | The actual notifier UID and mount accepted the producer facts. The correction has isolated CLI coverage and one witnessed natural CLI cycle. The 20:24:52 JST Discord test received HTTP 204 with no pending delivery. |
| Production | Native monitoring had no active incidents during the initial observation window. Media Pod/container identities and FFmpeg were preserved. |
| Limits | Failure cases remain isolated evidence. Human message visibility and public-player continuity were not observed. |
| Pending source correction | Compatibility continuity now uses each source's TTL; observation-loss warnings wait for the greater of that TTL and 300 seconds. Source-restoration confirmation survives a duplicate read in the same decision cycle. Development and public source are synchronized and tested, but this correction has not been deployed or sent a production notification. See the [source correction record](../publication/notification-source-timing-fix-20261101.json). |

## Thumbnail Collector on K3s (2026-11-01 JST)

The [migration record](v3/thumbnail-k3s-migration-2026-11-01.md) documents the
Service boundaries and preserved state. Helix/relay observation and the exporter
remain host services; Dell's media runtime and local CRA processing stay in place.

| Item | Observed checkpoint |
| --- | --- |
| Applied | `arena-thumbnail/twitch-thumbnail`, manifest digest `053b9cbfcd73642e02cafb66260eca4651d05e12a4348b2176849246e2368673`; old worker inactive/disabled. |
| Checks | 146 private / 139 public tests; state-file hashes identical across transfer; native image OCR passed. At 19:44 JST, fourteen of fifteen cycles verified three image clocks, with one initial DNS UNKNOWN and zero Pod restarts. |
| Public result | The same open page recovered from UNKNOWN to FRESH and advanced Thumbnail age through 19:40 JST. The ten-minute maximum remained distinct from current picture age. |
| Protected | Main/observer Pod/container identities, FFmpeg generation, Dell CRA/fallback process/release and ongoing native forward progress retained. |
| Remaining | Existing host-observer and single-node dependencies; no production rollback/node-loss exercise, long-term qualification or public-player AV continuity claim. |

## Public Page Wiring (2026-11-01 JST)

The public checkout now includes the clock-projection collector, and the page,
build and upload allowlist reference the current renderer. Retired public
systemd/file-input thumbnail examples were removed; host rollback files and
dated evidence remain intact. This is a source-tree change only: it does not
redeploy the live publisher or change delivery or CRA.

## Thumbnail Transfer Recovery (2026-11-01 JST)

The [dated correction](v3/thumbnail-transfer-recovery-2026-11-01.md) separates
acquisition from OCR and stops the age label from reusing an old bucket.

| Item | Observed checkpoint |
| --- | --- |
| Applied | Thumbnail worker `worker-aeb8bae90961` at 18:32:58 JST; public renderer SHA-256 `691f352818056913feb7e8b29b1170d6ec550c67c7cbdb1e62ce01ce9711918a`. |
| Checks | 94 candidate tests, 95 public tests and eleven Chromium fixture checks passed. Nineteen natural worker cycles accepted four distinct image clocks; longest cycle 38.599 s. |
| Public result | Fresh public JSON and an already-open page changed from UNKNOWN to AGING/FRESH. The age graph gained 18:40 and 18:50 JST buckets; missing historical data stayed missing. |
| Delivery protection | Main/observer Pod and container identities retained; FFmpeg PID730 predates this repair and runtime restart count remains zero. Existing monitoring parent and exporter retained. OCI forwarding and Twitch LIVE remained observed. |
| Limits | Long-term CDN behavior, a future session renewal under this version and uninterrupted public-player video/audio were not qualified. The age graph is a ten-minute maximum, not an instantaneous value. |

## Fallback Decision Rule (2026-10-31 JST)

The [boottime simplification](v3/boottime-media-health.md) uses one media-freshness
path and keeps UTC diagnostics separate. The
[fallback decision contract](v3/fallback-decision-contract.md) is the current
rule; the [revision history](v3/fallback-policy-revisions.md) preserves older policies.

| Item | State at this checkpoint |
| --- | --- |
| Applied | Dell controller `b38eb215dec46486`; OCI facts producer unchanged; arena read-only projection `545156b88e99d6d4` and existing-notifier adapter `dbe6f9cd39b4e025`. |
| Production | Main Pod/container, FFmpeg generation/connection and OCI relay/copy/keeper identities preserved. ACKs advancing, controller HEALTHY, backup zero. Existing admission hooks, disabled restart flags and notification routing/state/outbox retained. |
| Checked | 22 successful native conditions; two aborted setup launches, one readiness failure and two sibling cancellations retained separately. 210 private tests / 93 subtests; 212 public tests / 97 subtests. Actual-notifier subset: 19 tests with fake senders. Projection: eight tests in each checkout. |
| Representative result | Relay-stop start 47.5 s; copy-hang start 78.6 s; facts-only loss start 94.3 s. A healthy backup stopped after 303.0 s despite UTC rejection. |
| Policy evidence | Fresh bound downstream media can earn health and retirement credit independently of UTC. Every missing sample resets healthy time. A single downstream progress deadline replaces transition grace; local STOPPED does not override contradictory verified downstream progress. |
| Not observed | Production fault outcome, YouTube backup selection, uninterrupted public playback, actual alert delivery, long-term false-start rate. |

## Host Clock and Freshness Contract (2026-10-31 JST)

The October 30 one-second tolerance was provisional. The first October 31
revision replaced it with UTC offset intervals; the current media consumer uses
nonce/machine/boot-bound boottime freshness instead. UTC intervals remain
diagnostic and keep arena's independent secondary-recovery veto. A UTC fault
alone neither starts a backup nor prevents retiring a healthy one. The current
rule and timing bounds are in the decision contract.

Five hosts retain read-only clock observers. Their timing envelopes do not
extend CRA authorization or lease/command expiry. New consumers require the
boottime-aware producer: upgrade producers first and roll back consumers first.
The public Twitch/SLI snapshot readers keep their separate scalar policy; see
the [cross-host clock contract](v3/cross-host-observation-time.md).

The fixed HDD recording retains its October 31 21:12 JST deadline and original
capacity. Its full-window completion and final coverage remain unobserved at
this checkpoint. See the [dated recording case](v3/fallback-clock-recording.md).

## Manual Maintenance Command (2026-10-30 JST)

The [manual-maintenance module](v3/manual-maintenance.md) was ported with an
English interface, a map-backed slate, durable fallback inhibition and explicit
return gates. The command and controller guard are installed on the operated
sender, and a pinned inactive bundle is placed on the relay. The installed
`preflight`, actual UID/mount reads and shared-lock access passed. State remains
`NORMAL`, main replicas remain one and backup replicas zero; no stream switch
was requested. The primary-copy process/session and both output counters were
checked before and after enablement.

`stream-v3-maintenance on` combines entry and an in-terminal public-player
confirmation. `stream-v3-maintenance off` combines return, 300 seconds of stable
live progress and an in-terminal public video/audio/data confirmation. Both
commands continue their saved stages after interruption, retain inhibition on
failed checks and refuse to resend unknown effects. The return command passed
the real executable/PTY and isolated media pipeline; an actual maintenance return
on the operated stream remains untested. Only readiness observations wait briefly for a naturally activating
recovery oneshot; effects are never automatically retried. The first controller
verification expired before its second sample and rolled back; separate startup
and observation windows completed the next attempt. Failed evidence is retained.
This installation does not enable unattended source-loss slates or establish
provider selection, broadcast retention or public playback during maintenance.

## Airspy Startup Reception (2026-10-30 JST)

A receiver-local quality guard was added to the operated installation after
an Airspy-only restart with unchanged settings restored zero-position
reception. The exact device/software initialization or Bias-T/LNA cause is
still unresolved. The existing gain/power policy and media delivery remain
separate from its one-extra-restart budget.

The [public module](../ops/airspy-reception/README.md) ships as an optional,
observation-only example; this source port does not install a second guard.
Portable tests and the isolated real-systemd cases are tracked in the
[dated record](v3/airspy-reception-guard.md). **Unobserved:** a production
fault-triggered automatic restart, physical RF/power failure recovery and the
10% threshold's general false-positive rate. Synthetic service-manager tests
are kept distinct from those outcomes.

## Transport Recording (2026-10-29 JST)

The route observer's learned throughput baseline was retired at 10:13:57 JST.
Its relay-side rates had stopped on 2026-10-14 and its local rate on
2026-10-20; only ping probes continued. The retained data could not confirm
that a learned rate baseline discriminates faults on the constant-bitrate
sender. Its database is kept; it does not restart automatically.

The [transport flight recorder](v3/transport-flight-recorder.md) has run on the
Dell sender since 09:59 JST. It saves FIFO transport evidence around events and
has no action path. **Not yet observed:** an event window on a real fault, a
configured clock input, boot or power loss, and the storage limit. The first
two weeks evaluate classification value; a precursor result is likely to stay
open. See the [decision and revisit conditions](v3/route-baseline-retirement-decision.md).

## Public Snapshot Delivery (2026-10-27 JST)

The operated site now serves its snapshots through a Worker bound to private
R2. The 09:47 JST checkpoint confirmed continued publication, the previous
56 GCS objects archived and checked in private R2, and zero live objects in
the old GCS bucket. Deleted generations retain their existing expiry.

At 11:32 JST, the operated page separated required live-status observations
from supporting-data gaps, and the publisher began using at most two parallel
requests while retaining readback and uncertain-outcome reconciliation. The
primary health check and a strict eight-feed check expose both views. Four
natural cycles had median duration 27.3 seconds versus 42.6 seconds in the
preceding 15 cycles; all eight feeds advanced, both checks remained healthy,
and the next natural external-result import retained three passing regions.

Earlier that morning, an acquisition failure left two feeds at approximately 10:01 JST.
Diagnosis found successful source responses rejected by the older collectors'
zero-tolerance future-timestamp check with a 47-50 ms clock difference. Both
collectors received a one-second tolerance at 10:40 JST, preserving original
timestamps and the 180-second age limit. Publication resumed, the browser
showed LIVE again, subsequent timer output advanced, and all three publication
check regions passed at 10:42 JST. A recent monitoring snapshot timestamp does
not by itself make the other feed bodies current. The first repair missed the
clock collector's equivalent check: its warning appeared after the initial
freshness window expired. At 10:47 JST the same bounded tolerance was applied
to clock, relay and Twitch SLI acquisition, preserving their existing age
limits. At 10:48 JST the clock file resumed and all eight feeds were fresh.
Seven observations through 10:51:50 JST remained healthy, and all eight
timestamps advanced across multiple natural publication cycles. The 10:50 JST
external-result import was also healthy with three passing regions per target.

The [dated record](v3/r2-publication-and-freshness-case-study.md) separates the
operated changes, subsequent diagnosis, status separation and response-time observations.
This public update adds the record only; it does not transfer the matching R2
code or deploy the public example.

## Production Route

Since 2026-10-20 08:43 JST, Dell uploads one feed to the OCI relay, which
forwards it to YouTube and Twitch. A Dell controller starts a direct YouTube
fallback upload when the relay route to YouTube fails and stops it after
300 seconds of continuous relay health. The [route record](v3/self-healing-oci-fanout.md)
describes the components; the [decision contract](v3/fallback-decision-contract.md)
defines the thresholds. The table below keeps the October 20 rule as it was.

## Changes Since The Route Went Live (2026-10-20 JST)

| Time | Change | Verified |
| --- | --- | --- |
| 08:55-09:03 | Historical relay-stop drill under the former policy. | Fallback started after 52 s and stopped after 300 s of relay health; the broadcast did not end. |
| 09:10 | Replacement-broadcast creation disabled in the YouTube watchdog. A broadcast that really ends is recreated by hand. | Configuration applied and monitor restarted. |
| 11:33 | arena-server observes the relay through the private tunnel instead of the home public address. | Public status relay values returned to measured data. |
| 11:56-12:21 | Main sender switched to RTMPS on the relay; readiness and TCP-sampling port lists updated. | Upload samples of 3.57 and 3.63 Mbps returned. |
| 14:16 | The YouTube watchdog counts an established fallback-Pod connection as local delivery while the main Pod is not sending. | After restart, `local_ok=true` and no incident with the fallback at zero replicas. Not yet observed during a real fallback. The public source includes its credential-free connection tests. |

## Twitch Copyrighted-Audio Notice (2026-10-23 to 2026-10-24 JST)

Twitch's automated audio detection identified three NCS tracks in segments of a
VOD created on 2026-10-23: *Get Out Here* (Wiguez, Josh Levoid ft. MaryQueen),
*Apollo* (Malik Bash) and *Flames* (Vena Cava, Project Veresen ft. Raya). Twitch
muted those segments, deleted corresponding clips and changed the channel's
default so new VODs are stored unpublished for review. It was not a DMCA
notification, and no copyright strike was issued.

Review found a separate gap: from the Twitch addition on 2026-10-20 until
2026-10-24 16:00 JST, the Twitch channel carried no NCS credit. The music boundary named only the YouTube
livestream description as the attribution location, and adding Twitch did not
trigger the re-review that boundary already required.

| Time | Change | Verified |
| --- | --- | --- |
| 2026-10-24 15:50:41 | The three tracks were removed from AutoDJ playback candidates, and a classification override keeps them out of later regenerations. Source files and play history were kept. | The tracks were absent from the running image's candidate list; no other track's classification changed; the AutoDJ process and the FFmpeg process and connection continued; the next natural track change came at 15:52:54. |
| 2026-10-24 16:00 | NCS credit added on the Twitch channel. | Operator-applied. |

The two actions address different problems. Removing the tracks responds to
platform detection; adding credit meets the NCS usage-policy condition. Neither
restores the muted VOD segments or proves that other tracks will not be
detected. The operator found no YouTube claim for the same tracks as of
2026-10-24. Seven VODs were stored unpublished on 2026-10-24; automatic VOD
publication stays off by choice, so no review before republication is planned.

The public `configs/` copy includes the same three confirmed per-track
exclusions, so regenerating the time buckets keeps these tracks out of playback
candidates. The updated rule is in the
[music licensing boundary](compliance-and-licensing-boundary.md#music-licensing-and-credit-boundary).

## FIFO Connection Recovery (2026-10-24 JST)

The operated main sender now uses bounded FFmpeg FIFO output recovery, retaining
NVENC/AAC, GOP 10 and the relay's existing timeout. The corrected 12-second
WireGuard-underlay blackout kept the same process and Pod while the observed
connection generation changed from 1 to 2. New-connection ACK progress was
observed about 1.95 s after planned restoration; returned video/audio and both
provider forward counters were checked separately.

The first rollout lost its relay audio track from about 12:06 to 12:18 JST and
made the old FLV-only target classifier unavailable. It was rolled back and
corrected before the successful exercise. Viewer impact was not measured.
The [dated case](v3/ffmpeg-fifo-measurements.md) retains both outcomes.

The public source now includes the FIFO builder, injected transport observer,
FIFO-aware target classification, regression checks and sanitized timing tools.
This source transfer does not deploy them. Since 2026-10-25, a read-only export
on Dell and an observer on arena-server feed the operated observation into
Prometheus and an internal dashboard, and its reconnect events into Loki. The
public page counts output reconnects per day from those events, so FIFO
recoveries no longer disappear from the restart record. The event journal
covers every reconnect since FIFO recovery was enabled at 12:05 JST on
2026-10-24: three by 2026-10-25, one of them inside the first rollout that was
later rolled back. The observer is not yet in the public source, and no CRA
database fields were added. The full retry-exhaustion to
process-replacement cascade, a real ISP/public-IP change and long-term behavior
under this version remain untested. Current lab pings use 0.5 s; retained
measurements used 1 s.

## YouTube Timestamp Origin (2026-10-25 JST)

After the 06:31 uplink interruption, FIFO reconnected the main sender in the
same process, but the relay's YouTube connection kept closing until 09:22
because it received about 20 hours of continuing media timestamps. The YouTube
backup carried YouTube meanwhile; Twitch recovered on its own. A copy process on
the relay now gives each YouTube connection its own timestamp origin, the
keeper requires byte progress, and the fallback controller reads the copy
process's output facts. A manual re-creation at 08:53 also reconnected the
Twitch forward twice.

| Time | Change | Verified |
| --- | --- | --- |
| 09:22 | YouTube forward routed through the copy process. | One session sending with both tracks; no new `EOF`. |
| 09:56-10:02 | Fallback controller reads the new output facts. | Backup stopped after 300 s of continuous health. |
| 10:31-10:58 | Private YouTube comparison, timestamp origin only. | 5/5/6 unplanned reconnects without it, 0/0/0 with it. |
| 15:43 | Private player, connection cut during recording. | Same FFmpeg process; continuous video and test tone across 487 s. |

YouTube's internal reason for closing the connection, the long-term behavior of
the copy process, and Twitch player continuity during the 08:53 re-creation are
not observed. The [case study](v3/fifo-timestamp-continuity-case-study.md) has
the details and limits.

## Video Observation (2026-10-22 to 2026-10-23 JST)

The operated system now runs two clock layers beside the native forward counters
and provider APIs. [Observability](observability.md#video-observation-layers)
defines what each layer can show; the
[clock observation case study](v3/clock-observation-case-study.md) explains how
they were reached.

| Layer | Operated state | Last checked | Open item |
| --- | --- | --- | --- |
| Return-feed clock | Continuous reader, paired JST/UTC OCR every 10 s. Since 2026-10-23 18:27 JST, stage records, bounded native clock evidence, and one wider UTC read only after both current-region passes fail. | 18:27-18:59 on 2026-10-23: 194 of 195 cycles accepted on the current region, one startup wait; no wider read attempted. | No production rescue yet, so the fallback's coverage gain is unmeasured; rolling 24-hour picture-freshness coverage was 92.64-93.26%. |
| Fallback-sender clock | Deployed; idle because the fallback was not running. | Standby only. | Not observed during a real fallback. |
| Observation warnings | Gap and observer-unavailable warnings on the existing notification loop; no Critical or recovery threshold. | Rule tests and offline replay of the blind period. | Delivery to the notification channel not yet observed. |
| Twitch thumbnail clock | Conservative interpolation, compared UTC ROI, bounded distinct-image evidence and method/rejection diagnostics. | 17:12-17:13: a new image confirmed by the original read, then cached without recounting; earlier deployment at 17:09:42 JST. | Twenty-four-hour quality and first real session renewal under this version remain unobserved. |

None of these layers is public-player E2E evidence. Dated measurements are in the
[return-feed port](v3/public-source-port-2026-10-22.md), the
[thumbnail port](v3/public-source-port-2026-10-22-thumbnail.md) and its
[review follow-up](v3/public-source-port-2026-10-22-thumbnail-review.md), and the
[operated OCR counter rollout](v3/thumbnail-ocr-rollout-2026-10-22.md),
[OCR/evidence follow-up](v3/public-source-port-2026-10-22-thumbnail-evidence.md), and the
[return-feed evidence and UTC fallback record](v3/public-source-port-2026-10-23-relay-clock.md). The public
examples were not deployed; OCI restore assets and monitor images were retained
privately.

## Not Yet Observed

The [October 23 source reconciliation](v3/public-source-port-2026-10-23-observability.md)
adds thumbnail CDN timing, current input-quality diagnostics, notification
delivery safeguards and Monitoring v4 normal history to the public checkout.
This source transfer performs no deployment and does not close the operated
system's unobserved items below.

- YouTube continuity across a keeper-driven Twitch renewal. The first renewal,
  at 2026-10-21 00:00 JST, did nothing: the keeper sent its forward change as
  POST, which MediaMTX's config API rejects with 404, and it recorded the date
  anyway. The keeper now sends PATCH. At the 2026-10-22 renewal it removed the
  Twitch forward at 00:00:01 JST and restored it at 00:02:02 JST, and the Twitch
  API reported a new stream from 00:02:05 JST. YouTube delivery during that
  window was not separately checked.
- Delivery of a real Twitch alert to Discord.
- A prolonged relay-host stop that releases the cloud host allocation, and
  provider-side failures. A stopped relay service, tunnel loss, and a
  relay-host reset were exercised on 2026-10-20 and 2026-10-21.
- Whether MediaMTX reconnects a dropped forward without the keeper.
- Why the relayed stream drew YouTube `bitrateLow` warnings at an unchanged
  media rate. Captures on 2026-10-21 showed media of about 3.6 Mbps on both
  routes (the earlier 4.88 Mbps included about 34% TLS framing). Raising the
  video bitrate to 4000k stopped the warnings: 11 in the 24 hours before, 0 in
  the 24 hours after.
- Long-term soak of the current route.
- Twenty-four-hour clock/OCR quality, worst-case two-input performance within
  the observer limit, and the updated observer during a real fallback.
- A production rescue by the return feed's wider UTC read, and the resulting
  coverage gain. Replayed on 70 retained native images it accepted all 39
  low-confidence rejections without changing any normal reading, but no live
  sample has needed it yet. Behavior at the hours where failures had clustered
  (UTC 02 and 04) is also unobserved.
- The cause of two `runtime_attestation_unavailable` minutes on 2026-10-22. The
  public picture-freshness row labels every unknown minute as unread OCR.
- Twenty-four-hour thumbnail freshness, OCR quality and binary-retry rate, the
  thumbnail worker across a real session renewal, and actual delivery of the
  new observation-quality warnings.
- A new-VM bootstrap and live relay recovery from the saved OCI bundle.
- Public-player frame continuity or end-to-end platform delay from returned
  relay/fallback clocks. Public-player automated acquisition remains on hold
  pending applicable terms or permission. The selected operated update connects
  observation-quality warnings to the existing notification route; it adds no
  clock-derived Critical or recovery threshold. Thumbnail checks do not require
  automated player acquisition.
- Critical and recovery delivery through the OCI-aware Twitch notifier. Its
  public source tests use synthetic inputs and mocked webhooks.

## Twitch Observation Source Update (2026-10-21 JST)

The public Monitoring v4 example now includes fixed-cadence API collection,
failure/quota diagnostics, and notification decisions that combine OCI input,
Twitch output, and API observations. Critical path failures use Slack; observation
warnings use Discord. Recovery waits for two distinct fresh path/LIVE
observations. Historical API SLO failures and missing minutes remain intact.
See the [implementation notes](../monitoring-v4/docs/relay-provider-evidence.md)
and [source-port validation record](../publication/twitch-observation-port-20261021.json).
This port applies no public example to production and sends no notifications.

The [2026-10-27 transport update](../publication/twitch-api-deadline-20261027.json)
adds a whole-request API deadline, a bounded curl child in place of repeated
Python TLS setup, and phase-specific diagnostics. Local HTTPS and process tests
cover stalled DNS/TCP/TLS/header/body processing and child cleanup. Explicit
public account/revision bindings, SLO history and notification decisions remain
unchanged; this source port performs no deployment or provider experiment.

## CRA Public Layout (2026-10-26 JST)

The current [CRA package](../recovery-control/README.md) contains only the local
reflex implementation: 17 Python files and 2,535 lines, with its tests, tools,
service examples and current docs. Required Stream Executor and observation
modules moved unchanged to the top-level `src/`; boundary tests and schemas moved
with them. The previous three-host authority, soak and general harness remain
at a fixed [historical revision](../recovery-control/docs/history.md). This is a
source-only reorganization; it does not deploy code or enable effects. Older
suite totals and dated records remain historical.

## CRA Local Reflex (2026-10-22 JST)

Since 15:17 JST, a CRA agent on Dell evaluates the managed FFmpeg child every
two seconds under a `dry_run` policy signed by cra-01. It records confirmed
stalls and would-restart decisions but has no path to the Effect Executor.
cra-01 collects the audit log and checks it hourly. No stall has been confirmed
yet. See the [design record](../recovery-control/docs/local-reflex-authority.md).

At about 23:30 JST the agent switched to judging stalls by acknowledgement
progress, after a replay of recorded samples showed that the earlier
`bytes_sent` criterion missed the 06:31 JST stall. The production fast-recovery
controller still uses a `bytes_sent` criterion of the same kind, so it can miss
an acknowledgement stall until the agent takes over in `enforce`.

On 2026-10-23, most recorded stalls were traced to the daily WAN address
renumbering around 06:30 JST, after which the stream engine recovers by itself.
The agent now waits 90 s (signed policy v2, still `dry_run`) before a stall can
become a restart candidate. A relay output recorder keeps each daily
interruption; the first entry, at 06:31 JST, left 41 s of relay output unseen.
The [records](../recovery-control/docs/local-reflex-records.md) keep the dated
analysis, and [operations](../recovery-control/docs/local-reflex-operations.md)
lists what remains before `enforce`.

## Deliberately Disabled

- CRA production authority (`production_behavior_change_authorized=false`).
  The local reflex agent runs only in `dry_run`.
- Automatic replacement-broadcast creation by the YouTube watchdog.
- Automatic Twitch VOD publication. Twitch turned it off after the 2026-10-23
  audio notice; it is kept off because the 24/7 stream does not need public
  archives.
- Runtime and recovery authority in Monitoring v4. Its relay observer and
  public projection run in production as read-only components.

## Source Port Status (2026-10-23 JST)

Implementation, Git history and operated-system evidence are separate:

- **Implementation:** the capabilities below are present in the public working
  tree and have credential-free source tests.
- **Commit checkpoint:** the October 23 source-transfer record starts from
  `0356966` and records local preparation without a new commit or push. Later
  commits are recorded in Git history; the baseline is not the implementation
  status of the working tree.
- **Deployment evidence:** this source transfer performed no deployment or new
  runtime verification. Operated-system claims retain their own dated
  checkpoints above; source tests do not establish runtime version parity.

| Area | Public implementation | Contract and checks |
| --- | --- | --- |
| Twitch thumbnail age | Bounded CDN `Age`, persisted age at first sight and origin-side estimate. | [Thumbnail timing](v3/twitch-thumbnail-monitor.md#provider-and-cache-timing); HTTP, restart and delayed-OCR regressions. |
| YouTube input quality | `current_input_quality_crosscheck` alongside retained rolling gates. | [Current and historical quality](v3/sli-and-dashboard.md#current-input-quality-and-historical-gates); latest-state agreement and disagreement regressions. |
| YouTube watchdog | Recognition of an established FFmpeg connection in a ready fallback workload. | [Source record](v3/public-source-port-2026-10-23-observability.md); mocked connection and exact-port regressions. |
| Notifications | Actual-route receipts, retry ordering and durable overflow. | [Delivery rules](v3/notification-and-auto-recovery.md#delivery-and-current-state-rules); mocked delivery and persisted-state regressions. |
| Monitoring v4 | Schema-v7 normal aggregates, explicit campaigns and verified history copies. | [Normal monitoring and history](../monitoring-v4/docs/normal-monitoring-history.md); temporary SQLite and cursor-contract tests. |
| ADS-B source coverage | Time-based evidence coverage and compatible event counts. The development-source copy still lacks this change at this checkpoint. | [Coverage contract](v3/adsb-source-coverage.md); synthetic-event regressions. |

The [source-transfer record](../publication/observability-port-20261023.json)
binds the selected files and validation results. These are selected ports;
private operational material and public configuration adjustments keep other
differences intentional.

## Latest Evidence Checkpoints

| Evidence | Latest checkpoint | Record |
| --- | --- | --- |
| Same YouTube URL | 2026-10-14 00:04:57 JST, `140 / 140` ledger dates | [Continuity evidence](same-url-continuity-evidence.md) |
| Automatic fallback and return in production | 2026-10-20 drill, earlier rule | [Route record](v3/self-healing-oci-fanout.md#drill-stopping-the-relay-2026-10-20-0855-to-0903-jst) |
| Current fallback rule, isolated faults | 2026-10-31, 22 conditions | [Evidence status](v3/fallback-decision-contract.md#evidence-status) |

## Longer-Term Gaps

These are known gaps, not scheduled or authorized work:

- Keep Monitoring v4 runtime/recovery authority and CRA production authority
  disabled until a host, immutable revision, target, rollback, formal soak, and
  authorization review is completed outside public CI. Monitoring v4's read-only
  relay observation and public projection are already running in production.
- Complete the Monitoring v4 phases still marked unimplemented in its
  [status record](../monitoring-v4/docs/status.md).
- Measure viewer buffering and frame continuity before claiming more than the
  same-URL evidence supports.
- Audit the full YouTube broadcast inventory before strengthening the
  zero-replacement claim beyond ledger evidence.
- Add a portable Kind or k3d validation path for contributors without k3s.
- Publish more dashboard examples only when labels and generated evidence can
  be sanitized.
