# Producer Time and Notification Evidence

Updated: 2026-11-01 JST. This is the current notification-evidence contract;
delivery-path failover remains governed by the [fallback decision contract](fallback-decision-contract.md).

## Problem and decision

Restarting hosts, correcting UTC and rereading a cached file can change what a
notification consumer sees without proving that the media failed or recovered.
The notification consumer now distinguishes a new producer observation from a
new read of the same observation.

Existing NTP observers supply synchronization and clock diagnostics. Producers
record UTC for correlation, plus a machine identity, boot ID, sample ID and
`CLOCK_BOOTTIME` at collection. Same-host age and notification elapsed time use
boottime. Remote UTC is interpreted only through a pinned clock estimate or an
authenticated nonce exchange; monotonic epochs from different hosts are never
subtracted directly.

## Decision contract

| Evidence | Notification behavior |
| --- | --- |
| Fresh, verified abnormal observation | Keep the owner's existing detection and routing rules; no blanket extra delay. |
| Missing, expired or rejected observation | Retain an already announced incident without calling it currently confirmed abnormal. Absence alone cannot announce recovery. |
| Distinct verified normal observations | Confirm recovery after at least two observations spanning 45 seconds on both producer and consumer clocks. |
| Persistent observation loss | Emit a separate observation warning after the greater of 300 seconds and the source's existing TTL. It does not assert a media outage. |
| UTC correction with valid local boottime evidence | Keep the causal freshness result and report clock quality separately. A wall-clock jump does not complete reminder or escalation timers. |

Duplicate samples, counter rollback, boot changes and delayed old samples cannot
complete normal continuity. Missing evidence breaks that continuity. OAuth
evidence retains its original identity while cached, even when its parent
watchdog publishes a new result. The raw quality cross-check uses the OAuth
identity rather than the aggregate report's newer timestamp.

The maximum interval between normal observations follows the source's TTL.
A slow producer can therefore confirm recovery at its normal cadence. Repeated
reads still do not add observations or renew the observation-loss deadline.

Viewer capture and visual health are separate facts. The shipped viewer producer
uses `frame_ok`, failure counters and black/frozen flags; it does not require a
`probe_ok` field. Both consumers share a checked predicate for that schema.
A captured black or frozen frame remains a visual failure. Missing capture or
incomplete visual evidence cannot confirm visual recovery. A verified failed
measurement is still a measurement, so it does not become a missing-source event.

ADS-B source-clock lag and snapshot retention are separate measurements.
`STREAM_ADSB_SOURCE_CLOCK_HOST` identifies the host that generated the original
`aircraft.json.now`, not a convenient proxy hostname. A lag interval crossing
the threshold remains unknown. Local PulseAudio observation is optional on a
Kubernetes monitor where that producer has never existed; after registration,
loss of the producer is still tracked durably.

## Implementation and rollout

The canonical implementation is `stream_core.common.observation_time`; the
Monitoring compatibility slice contains a checked copy. Producers stamp samples,
the native collector retains their provenance, and both notification consumers
recheck freshness at the decision boundary. The executable recovery plan is
unchanged: notifications read a separate facts-only derivative.

A subsequent isolated review found that the compatibility consumer was still
using the common helper's default observation interval and loss deadline.
The source correction applies each source's TTL to both continuity checks and
the loss deadline. It also preserves source-restoration confirmation through a
duplicate read in the same decision cycle, even if the media incident remains
open. A real-user preflight then exposed the viewer schema mismatch, which was
corrected in both consumers before application. The follow-up is now deployed;
the [current status](../current-status.md) separates its evidence from the earlier
operated rollout.

The public Python package also carries the canonical clock contract as a checked
package module. An installed wheel does not need an operator's checkout or an
`ops/` directory at runtime. `sync_observation_clock.py` verifies that copy
alongside the existing dependencies.

Dedicated Twitch/relay recovery, thumbnail, notifier/exporter liveness and
backlog owners keep their existing evidence and routes. Legacy metric inputs
remain compatible; they do not become verified observations merely by being
read. SLO targets, windows, recovery authorization and media processes are
outside this change.

Formal SLO coverage and current-source reconciliation also keep their existing
owner. A valid measurement is not proof that the SLO is healthy. An active burn
in a valid measurement must still notify; the normal-continuity check must not
veto it. This distinction was caught during the rollout audit and corrected in
the native consumer. A real CLI in isolation retained that burn through missing
observations. The correction changed no delivery function and did not send an
additional production test message.

Upload sampling now has an [independent read-only owner](upload-measurement-source.md).
A recovery safety rejection cannot suppress physical measurement collection.
Its current-source confirmation still leaves historical coverage and SLO values
unchanged; the operated checkpoint is recorded in current status.

The operated rollout first warmed producers, then enabled consumers. Effective
systemd commands, process-visible files and the real service user's read-only
mount were checked. A later drop-in and an environment file initially selected
older producer code; two refresh jobs failed before consumer activation. Their
failed evidence was preserved, the effective entries were corrected, and the
jobs then succeeded. This is why file placement alone is insufficient evidence
of deployment.

The monitoring parent, clock mesh and standalone notifier were restarted;
the media runtime and fallback controller were not restarted. Consumers are
rolled back before producers, with ownership checks and unresolved-delivery
checks. Notification state, outbox, delivery audit and failed attempts must not
be restored from an old snapshot.

## Verification and limits

Isolated tests cover UTC steps, source/receiver boot changes, duplicate and
regressing samples, nonce binding, uncertainty, alternating missing samples,
outbox behavior and recovery continuity. The real CLI ran under a DynamicUser,
read-only source, resource limits and network isolation with delivery disabled.
The deployed notifier also read real producer evidence under its actual service
UID and mount namespace.

The production notification test at **20:24:52 JST on 2026-11-01** received
Discord HTTP 204. Exactly one test message was sent, with no pending delivery.
Provider acceptance does not prove that a person saw the message. Production
failure injection was not performed; failure and recovery cases were exercised
in isolation. The [current status](../current-status.md) records the latest
operated checkpoint.

The later correction also received Discord HTTP 204 for one labelled test using
the standalone notifier's service UID and unchanged delivery function. It did
not create a production incident. Two real viewer samples at the producer's
normal cadence completed recovery in a separate, nondelivery planner state;
rereading the first sample did not add a vote. Production failure injection was
not used to establish that result.

The day's 02:00–06:00 JST reboot interval was excluded from the historical
notification analysis, not configured as a recurring mute. The remaining two
historical ADS-B warnings cannot be assigned a clock root cause without their
original snapshots.

Optional diagnostics use a bounded rotating log and must be explicitly enabled
on an operator-selected HDD path. A size bound does not promise a week of
retention. That optional exporter logger was not activated during this rollout;
private deployment evidence was retained on HDD. No raw logs, credentials,
machine IDs or private deployment configuration are part of this public port.
