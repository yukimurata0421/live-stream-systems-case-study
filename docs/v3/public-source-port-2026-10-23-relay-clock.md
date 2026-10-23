# Public Source Port: Relay Clock Evidence And UTC Fallback Read

This record follows the return-feed reader from an unexplained loss of picture
freshness coverage to a deployed, narrowly scoped fix. The stable behavior is in
the [relay clock monitor contract](relay-clock-monitor.md);
[current status](../current-status.md) owns what is still unobserved. Earlier
dated records remain intact.

## Source And Scope

Only selected source, tests and documentation are ported. Source selection,
digests and checks are in
[the port record](../../publication/relay-clock-port-20261023.json). The change
touches the paired-clock analyzer, the relay worker, a new bounded clock-evidence
store and the monitoring-host importer. It does not change the clock ROIs, the
confidence threshold of 70, the nine-hour pair rule, progress and freeze
judgement, the public coverage gate of 90%, SLI windows or targets, delivery,
fallback, notification or recovery authority.

The YouTube fallback keeps its existing reading and acceptance rules; it also
receives the shared stage diagnostics and native abnormal-evidence storage.
The wider UTC retry and periodic normal controls are enabled only for the relay.
The Twitch thumbnail reader is unchanged. This port performs no deployment.

## Cause

For the 24 hours ending 16:25 JST on 2026-10-23, the minute-by-minute
classification used by the public collector had 1,358 judged-good minutes, no
bad minutes and 82 unknown minutes: 94.306% coverage, 100% of judged minutes
good. Eighty unknown minutes were `clock_pair_unreliable` and two were
`runtime_attestation_unavailable`, a separate binding problem.

The 80 OCR minutes mapped to 40 scrapes and 40 retained abnormal images. In
every one, both clocks parsed and agreed modulo nine hours and JST confidence
passed; only UTC confidence was below 70 (median about 48.9). This is
observation-quality loss, not a frozen or missing picture.

Across 538 retained `clock_pair_unreliable` archives in that window:

| First-read result | Archives |
| --- | ---: |
| Pair valid, UTC confidence alone below 70 | 523 |
| UTC time format invalid | 10 |
| Pair valid, JST confidence alone below 70 | 4 |
| Pair valid, both confidences below 70 | 1 |

These are retained abnormal examples, not a failure rate over all samples.
Replaying ten downscaled display JPEGs suggested that the UTC region's left
margin was too tight: widening it restored leading zeros in several readings.
The production input is 1080p, so this was a hypothesis, not proof.

The investigation also found three observation gaps:

- On a rejected pair the analyzer kept only the previous and current JST
  regions, so the native UTC pixels that failed were never retained.
- The contrast retry's own strings, confidences and errors were not recorded.
- The public collector evaluates a two-minute scrape job once per minute; a
  30-minute unknown run in that view did not mean every ten-second sample
  failed for 30 minutes. A 30-second job showed at most 15 consecutive unknown
  points over the same span.

## Evidence First

Before changing any reading rule, the worker started keeping what was missing:

- **Stage records.** `ocr_pair_stages` stores the `original` and `contrast`
  JST/UTC strings, confidences, bounded error codes and pair acceptance. The
  acceptance rule, value selection and OCR count are unchanged.
- **Native clock regions.** Abnormal evidence adds `jst.pgm`, `utc.pgm` and a
  `utc_context.pgm` with eight extra pixels left and right and four above and
  below. At 1080p these are 77x24, 76x24 and 92x32 pixels. Metadata binds the
  sample, capture time, display-JPEG hash, source resolution, ROI settings and
  pixel bounds.
- **Normal controls.** To compare against readings that succeed, the relay
  target also stores these three regions from an advancing sample at most once
  every 60 seconds, marked `clock_evidence_kind=periodic_roi`. This cadence is
  not an SLI denominator.

The evidence travels in the existing signed bundle, and the monitoring-host
importer verifies sample binding, member names and count, PGM format, regions of
at most 320x96 pixels and 256 KiB per archive. Each host keeps at most 8 MiB of
allocated space for 24 hours and skips new evidence when full. Broken evidence
is recorded as a storage status and never fails the clock observation. Ordinary
full frames are still not persisted.

## Widening Was Not Monotonic

By 17:54 JST, 70 native-resolution images had been retained: 39 low-confidence
rejections and 31 normal controls. All 39 rejections had failed both the
original and contrast reads only on UTC confidence, with no timeout or process
error. Replaying them with the same OCR binary, data and reader:

| Retained images | Accepted, current ROI | Accepted, UTC widened 3 px left |
| --- | ---: | ---: |
| Low-confidence rejections | 0/39 | 39/39 |
| Normal controls | 31/31 | 30/31 |

Every replay reproduced the recorded production reads exactly, and widening
never changed a reading. One normal image, however, fell from UTC confidence
92.1 to 51.5 when widened (56.2 with contrast), reproducibly. A wider region
helps on average but is not better for every image, so a uniform ROI change was
rejected.

## Current ROI First

The adopted rule keeps the current ROI authoritative and uses the wider UTC
region only after it has failed:

- If the original or contrast read is accepted, that reading and confidence are
  returned unchanged and no wider region is cut or read.
- Only when both are rejected, and the original JST reading has confidence of at
  least 70, is that JST fixed and UTC read once more with three extra pixels on
  the left.
- No wider read is attempted after a parseable time conflict, a nine-hour
  mismatch, an unreliable JST anchor, a process failure or a timeout. Skip
  reasons are fixed codes.
- A UTC value that was already parseable may not change, and the wider reading
  must itself pass confidence 70 and the nine-hour rule. There is no voting,
  time substitution or string repair.
- At most five OCR calls per relay sample: two original, two contrast and one
  wider UTC read. The fallback retains at most four calls per sample. Both
  targets share the unchanged eight-second cycle deadline, with at most one
  second per call, and no process starts after the deadline.
- Progress, freeze, jump, day-boundary and delay judgement are unchanged. A
  stopped clock that becomes readable is still stopped.
- Only the relay target enables it. Images rescued this way are retained as
  `clock_evidence_kind=roi_retry`; `utc.pgm` keeps the current-ROI pixels and
  `expanded_utc_bounds` records the region actually read.

On the same 70 images, all 31 normal controls were accepted with no additional
read and identical readings and confidences; all 39 rejections were accepted
by the wider read; 16 missing-clock and mismatched-time negatives stayed
rejected. Pair processing took at most 1.964 seconds. In an isolated,
network-free, non-root container limited to 0.5 CPU and 256 MiB, the candidate
worker walked a rescued warm-up, a rescued advancing sample and a normal
sample through storage and import with a longest cycle of 2.107 seconds. The
fixture used real clock pixels but is not evidence of current production video.

## Operated Rollout

The monitoring image was updated at 18:27:32 JST with only these three modules
changed, and the monitoring-host importer was updated for the next timer run.
Delivery identities, the encoder process and forward rates of about 4.2 Mbps to
YouTube and Twitch were unchanged before and after. These are operated-system
observations, not deployment of this public checkout.

Up to 18:59 JST the new worker ran 195 cycles: 194 accepted on the current ROI
and one startup wait. No wider read was attempted, so no rescue was observed.
All 30 one-minute evaluation points from 18:30 were judged in both the
two-minute and 30-second jobs, but the current ROI alone also judged all of
them: the confirmed coverage contribution of the fallback is zero points so
far. Rolling 24-hour coverage stayed at 92.64% (two-minute job) and 93.26%
(30-second job); past unknown minutes are not rewritten.

A rescue will count toward coverage only when a one-minute evaluation point is
judged good or bad and maps to a sample that the current ROI rejected in both
stages and the wider read accepted. Startup waits, runtime-binding failures and
samples with ambiguous timing are not counted.

## Validation And Limits

Credential-free regressions cover acceptance without a wider read, contrast
acceptance, the anchor, conflict, confidence, timeout and value-change
rejections, freeze and jump detection, and storage of rescued evidence. The
development candidate passed 128 related tests.

Not established by this port:

- A production rescue, and therefore any measured coverage improvement.
- Behavior at the hours where failures had clustered (UTC 02 and 04), and
  24-hour OCR quality.
- The cause of the two `runtime_attestation_unavailable` minutes. The public
  picture-freshness row still labels every unknown minute as unread OCR.
- Public-player video or audio reception.
