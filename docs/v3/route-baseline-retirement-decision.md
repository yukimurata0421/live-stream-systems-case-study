# Retiring The Learned Throughput Baseline

Status: accepted
Decision date: 2026-10-29 JST
Scope: the route observer that learned per-plane throughput baselines, and how
transport degradation is recorded instead.
Retired design: the baseline contract in
[route network observation](route-network-observation.md).
Replacement: the [transport flight recorder](transport-flight-recorder.md).

## Context

On 2026-10-13 a route observer started on the Dell sender. Every 15 seconds it
differentiated byte counters on four planes (`local_ack`, `oci_input`,
`youtube`, `twitch`) into Mbps, bound each sample to a measurement regime
(route, encoder profile, relay source, WAN identity), and kept one-day and
seven-day windows with median, MAD and lower percentiles. The intent was to
learn what normal progress looks like, then connect thresholds and statistics
to observability and, after separate review, to decisions. It never held
action authority.

Its rate planes stopped in two steps. The last valid OCI read, and the last
`oci_input`, `youtube` and `twitch` rates, were at 2026-10-14 18:18 JST. The
reader expected the signed-lease relay and its guard status file, which the
current relay host no longer has. The last `local_ack` rate was at
2026-10-20 08:43 JST, during the
[route change](self-healing-oci-fanout.md); the local reader also required a
controller file that the new route no longer refreshes. After that the
observer kept running and writing samples with every rate plane
`SOURCE_UNAVAILABLE`, while its ping probes continued until it was stopped.
No alert covered an observer that was running without producing any rate. The
gap was found on 2026-10-28 during a manual status review.

Repairing the readers was possible. The question reviewed first was whether the
learned baseline was worth repairing at all.

## What The Retained Data Showed

The 2026-10-13 to 2026-10-20 database was read back, read-only, before the
decision.

**Training-gate labels did not show up in the rate distribution.** Each rate
sample was marked eligible for training only when every gate held (fresh
sources, reachability, DNS, no queued output, no provider error). A failed gate
means that one of those conditions was missing or unobserved. It is **not** a
fault label, and the rows below combine several regimes.

| Plane | Training gate | Samples | Median Mbps | MAD | P1 | P5 | Samples below 0.1 Mbps |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `local_ack` | passed | 22,153 | 4.86 | 0.118 | 3.44 | 3.60 | 0 |
| `local_ack` | failed | 13,775 | 4.87 | 0.102 | 3.61 | 4.62 | 0 |
| `youtube` | passed | 3,028 | 3.61 | 0.078 | 3.39 | 3.44 | 0 |
| `youtube` | failed | 128 | 3.55 | 0.055 | 3.38 | 3.41 | 0 |

What this supports, and what it does not:

- The two gate outcomes had nearly the same rate distribution. Because the
  gate is not a fault label, this does not show that rate cannot detect
  faults. It shows that this data **could not confirm** that a learned rate
  baseline discriminates faults.
- No sample below 0.1 Mbps was recorded. The observer refuses to compute a
  rate across a counter reset, connection change, process change or long gap.
  Interruptions therefore left missing samples and identity changes rather
  than low rates, and the lower tail the statistics watched stayed empty.
- The level is set by the encoder. The sender is constant-bitrate H.264
  (NVENC CBR), and the medians match the per-route profile (about 3.6 Mbps on
  the relay route, about 4.9 Mbps on the direct route) without learning.

**The baseline never became ready.** Seventeen regimes were opened in seven
days as routes, profiles and relay sources changed. The best one-day window
reached about 67% eligible normal samples against the 90% readiness rule.

**Queue metrics moved, but after the fault started.** For about 40 continuity
breaks, the four samples before each break were inspected.

| Break type | Count | Queue behavior before the break |
| --- | --- | --- |
| Daily 06:31 JST WAN interruption | 6 | `notsent` rose to 0.5-2.1 MB and `lastsnd_ms` to 1.3-7.1 s within the preceding 15-30 s |
| Other breaks | About 35 | `notsent` and `unacked` stayed in their normal range |

At about 4.9 Mbps, 2.1 MB is roughly 3.5 seconds of output, so these values
are data that queued after delivery stopped. They are early **detection** of an
interruption that has already begun, not a precursor. With 15-second sampling,
the other breaks could not be separated into real transport faults and
observer input failures. No gradual degradation (queue growing over minutes
before a stall) appeared in the week.

## Decision

1. **Stop the baseline observer and keep its history.** It was stopped and
   disabled on 2026-10-29 at 10:13:57 JST, its last sample. The database, WAL,
   last snapshot, releases and deployment receipts were kept unchanged. No
   automatic restart was configured.
2. **Do not learn a baseline for a constant-bitrate single stream.** Expected
   throughput comes from the encoder profile, and the retained data did not
   show the learned baseline adding discrimination. Interruptions appeared as
   stopped progress, missing observations and connection changes, which
   existing checks already observe.
3. **Record transport detail around events instead.** The
   [transport flight recorder](transport-flight-recorder.md) reads the existing
   one-second FIFO telemetry, keeps 120 seconds in memory, and saves the
   window around each event to SQLite. It has no action path.
4. **Decide on any precursor-based action only after the records show one.**
   Recording triggers are not fault thresholds and are not promoted to
   recovery conditions automatically.

## Alternatives Considered

| Alternative | Why not now |
| --- | --- |
| Repair the readers for the new route and restart learning | The data above gives no evidence that the result would differ: a narrow, profile-set distribution that interruptions leave as gaps rather than low rates. A new regime would also need another week before `ready`. |
| Move learned baselines into Prometheus (`quantile_over_time`, MAD rules) | This suits demand-driven, seasonal signals whose normal value is unknown. Neither condition holds for this sender. Plain `rate()` with profile-derived thresholds remains available. |
| Act on queue metrics immediately (for example, start the backup sender early) | No precursor has been observed. The six daily interruptions showed no queue change before onset at 15-second resolution, and in the 2026-10-24 injected outage the existing stall state was observed about 4.4 seconds after the block began. |
| Keep the observer running unchanged | It consumed disk and made a privileged relay read every 15 seconds while no rate plane produced data. |

## What Was Kept

- The fast-recovery controller's ACK-difference measurement
  (`ack_delivery_measurement_v1`, measurement only, no restart threshold).
- FIFO [transport telemetry](ffmpeg-fifo-recovery.md#observing-recovery-without-a-new-pid)
  and its Prometheus counts on the monitoring host.
- The [fallback controller](self-healing-oci-fanout.md), which judges relay
  health from input, output and packet counters advancing.

The retired observer's multi-probe diagnosis (pings, TCP anchors, OCI
counters) is not replaced by the recorder, which observes only the main
sender's transport.

## Revisit Conditions

The first two weeks of recording (from 2026-10-29) are evaluated on three
questions:

- **Classification:** were events that were previously unknown separated with
  evidence (transport stall, observer failure, relay side, provider side)?
- **Detection:** did queue signals catch a real degradation earlier than the
  existing stall state, and how many candidates were false alarms?
- **Precursor:** separately from early detection, was any change usable
  before the fault began?

The recorder stays if it helps classification even without a precursor. It is
removed if it adds nothing to classification either. A period without faults
is not evidence of detection performance. Because the observed faults are
mostly the abrupt daily interruption, the precursor question is likely to stay
open after two weeks.

Precursor-based action would be reconsidered only if the records show that a
queue or RTT signal leads a real stall by several seconds with a measured false
alarm rate. Even then it would start as a low-cost action such as starting
the backup sender early, under a separate review.

## Limits

The data above covers one week on the pre-2026-10-20 routes at 15-second
resolution, with training-gate outcomes rather than labeled faults. It did not
confirm that a learned baseline is useful for this sender. It does not prove
the approach invalid, and it says nothing about variable-bitrate, multi-stream
or capacity-planning uses. The production database is not published.
